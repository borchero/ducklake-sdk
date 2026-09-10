from __future__ import annotations

import re
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import polars.datatypes as pld
import polars.selectors as cs

from ducklake.table import Table
from ducklake.view import View

if TYPE_CHECKING:
    from ducklake import typedefs

DROP_COLUMN_PREFIX = "__ducklake_drop__"

_POLARS_VERSION = tuple(int(part) for part in re.findall(r"\d+", pl.__version__)[:2])


def scan_ducklake(
    table: Table, *, include_file_paths: str | None = None, time_zone: str | None = None
) -> pl.LazyFrame:
    cache_path = Path(tempfile.mkdtemp())

    # 1) First, we read all relevant data from the table. We first scan, then get the
    #    schema because this ensures that the schema is up-to-date.
    scan_result = table.scan()
    schema = table.schema

    # 2) Then, we have to build all the inputs for the scan
    # 2.1) Deletion files: DuckLake's deletion files are the same as the ones used by Iceberg.
    #      For inlined deletions, we need to currently create separate files
    iceberg_position_deletes: defaultdict[int, list[str]] = defaultdict(list)
    inline_delete_count = 0
    for i, data_file in enumerate(scan_result.data_files):
        if data_file.delete_files:
            iceberg_position_deletes[i].extend([file.path for file in data_file.delete_files])
        if data_file.inline_deletes is not None:
            # TODO: Writing to a temp directory here is an ugly workaround. Instead, we should be
            #  able to apply these inline deletes in-memory
            df = pl.DataFrame({"file_path": data_file.path, "pos": data_file.inline_deletes})
            write_path = cache_path / f"inline_deletes_{i}.parquet"
            df.write_parquet(write_path)
            iceberg_position_deletes[i].append(str(write_path))
            inline_delete_count += df.height

    if _POLARS_VERSION >= (1, 44):
        deletion_files = ("iceberg", (dict(iceberg_position_deletes), {}))
    else:
        deletion_files = ("iceberg-position-delete", dict(iceberg_position_deletes))

    # 2.2) Row counts
    physical_rows = sum(data_file.statistics.num_rows for data_file in scan_result.data_files)
    deleted_rows = (
        sum(
            sum(delete_file.num_deletes for delete_file in data_file.delete_files)
            for data_file in scan_result.data_files
        )
        + inline_delete_count
    )

    # 2.3) Schema and defaults. DuckLake's column-level defaults map to Iceberg's per-column
    #      `initial-default`, which is applied wherever a column is missing from a data file.
    target_schema = pl.Schema(schema)
    columns_with_defaults = [
        col
        for col in schema.columns
        if col.initial_default is not None and col.field_id is not None
    ]
    if _POLARS_VERSION >= (1, 43):
        # polars 1.43 introduced a dedicated slot for per-column initial defaults, passed as a
        # 2-tuple of `(identity_transformed_values, initial_defaults)`.
        default_values = (
            {},
            {
                col.field_id: pl.Series([col.initial_default], dtype=target_schema[col.name])
                for col in columns_with_defaults
            },
        )
    else:
        # Older polars only exposes the per-file `identity_transformed_values` slot, so the
        # constant default has to be repeated once per data file.
        default_values = {
            col.field_id: pl.repeat(
                col.initial_default,
                len(scan_result.data_files),
                dtype=target_schema[col.name],
                eager=True,
            )
            for col in columns_with_defaults
        }

    # 2.4) Statistics
    stat_len = pl.Series(
        [file.statistics.num_rows for file in scan_result.data_files], dtype=pl.get_index_type()
    )
    stat_min = {
        f"{col.name}_min": pl.Series(
            [
                col_stats.min_value
                if (col_stats := file.statistics.column_stats.get(col.field_id)) is not None
                else None
                for file in scan_result.data_files
            ],
            dtype=target_schema[col.name],
        )
        for col in schema.columns
        if col.field_id is not None
    }
    stat_max = {
        f"{col.name}_max": pl.Series(
            [
                col_stats.max_value
                if (col_stats := file.statistics.column_stats.get(col.field_id)) is not None
                else None
                for file in scan_result.data_files
            ],
            dtype=target_schema[col.name],
        )
        for col in schema.columns
        if col.field_id is not None
    }
    stat_null_count = {
        f"{col.name}_nc": pl.Series(
            [
                col_stats.null_count
                if (col_stats := file.statistics.column_stats.get(col.field_id)) is not None
                else None
                for file in scan_result.data_files
            ],
            dtype=pl.get_index_type(),
        )
        for col in schema.columns
        if col.field_id is not None
    }
    table_statistics = pl.DataFrame({"len": stat_len, **stat_min, **stat_max, **stat_null_count})

    # 3) Then, we create the lazy frame by scanning all data files
    result = pl.scan_parquet(
        # --- Files ---
        [data_file.path for data_file in scan_result.data_files],
        glob=False,
        hive_partitioning=False,
        include_file_paths=include_file_paths,
        storage_options=table._storage_options.to_dict(),
        # --- Schema ---
        schema=target_schema,
        missing_columns="insert",
        extra_columns="ignore",
        cast_options=pl.ScanCastOptions(
            integer_cast="upcast",
            float_cast="upcast",
            missing_struct_fields="insert",
            extra_struct_fields="ignore",
            categorical_to_string="allow",
        ),
        # --- Optimization ---
        _column_mapping=("iceberg-column-mapping", schema),
        _deletion_files=deletion_files,  # ty: ignore[invalid-argument-type]
        _default_values=("iceberg", default_values),  # ty: ignore[invalid-argument-type]
        _table_statistics=table_statistics,
        _row_count=(physical_rows, deleted_rows),
    )

    # 4) If we had any inline data, we also want to include that in the scan result
    if scan_result.inline_data:
        for inline_data in scan_result.inline_data:
            inline_lf = pl.LazyFrame(inline_data)
            if include_file_paths is not None:
                inline_lf = inline_lf.with_columns(
                    pl.lit(None, dtype=pl.String).alias(include_file_paths)
                )
            result = pl.concat([result, inline_lf])

    # 5) Represent timezone-aware timestamps in the requested connection or per-read time zone.
    result_time_zone = table._time_zone if time_zone is None else time_zone
    result = result.with_columns(
        pl.col(name).cast(_convert_datetime_time_zone(dtype, result_time_zone))
        for name, dtype in target_schema.items()
    )

    return result


def read_ducklake(
    table: Table, *, include_file_paths: str | None = None, time_zone: str | None = None
) -> pl.DataFrame:
    return scan_ducklake(
        table, include_file_paths=include_file_paths, time_zone=time_zone
    ).collect(optimizations=pl.QueryOptFlags._eager())


# -------------------------------------------- VIEWS -------------------------------------------- #


def scan_view(view: View) -> pl.LazyFrame:
    """Lazily evaluate a view's query against its referenced DuckLake relations."""
    return _scan_view(view, frozenset())


def _scan_view(view: View, ancestors: frozenset[typedefs.TableName]) -> pl.LazyFrame:
    name = view.name
    if name in ancestors:
        raise ValueError(f"cyclic view reference involving {name}")

    sql, pytables, pyviews = view._pyview.polars_query()
    frames = {
        alias: Table._from_pytable(
            pytable, view._duckdb_connection_fn, view._storage_options, view._time_zone
        ).scan_polars()
        for alias, pytable in pytables
    }
    for alias, pyview in pyviews:
        nested_view = View._from_pyview(
            pyview,
            view._ducklake,
            view._duckdb_connection_fn,
            view._storage_options,
            view._time_zone,
        )
        frames[alias] = _scan_view(nested_view, ancestors | {name})

    ctx = pl.SQLContext(frames=frames, eager=False)
    result = ctx.execute(sql)
    if aliases := view.column_aliases:
        renamed = [pl.nth(index).alias(alias) for index, alias in enumerate(aliases)]
        remaining = cs.all() - cs.by_index(*range(len(aliases)))
        return result.select(*renamed, remaining)
    return result


def read_view(view: View) -> pl.DataFrame:
    return scan_view(view).collect(optimizations=pl.QueryOptFlags._eager())


# -------------------------------------------- UTILS -------------------------------------------- #


def _convert_datetime_time_zone(
    dtype: pl.DataType | pld.DataTypeClass, time_zone: str
) -> pl.DataType | pld.DataTypeClass:
    match dtype:
        case pl.Datetime(time_unit=time_unit, time_zone=current_time_zone) if (
            current_time_zone is not None
        ):
            return pl.Datetime(time_unit, time_zone)
        case pl.Struct(fields=fields):
            return pl.Struct(
                [
                    pl.Field(field.name, _convert_datetime_time_zone(field.dtype, time_zone))
                    for field in fields
                ]
            )
        case pl.List(inner=inner):
            return pl.List(_convert_datetime_time_zone(inner, time_zone))
        case _:
            return dtype
