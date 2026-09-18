from __future__ import annotations

import re
import tempfile
from bisect import bisect_left, bisect_right
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, cast

import polars as pl
import polars.datatypes as pld
import polars.selectors as cs

from ducklake import typedefs
from ducklake._native import arrow_schema_field_ids
from ducklake.table import Table
from ducklake.view import View

if TYPE_CHECKING:
    from ducklake.typedefs import Column, ScanDataFile, Schema

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
    statistics = {"len": stat_len}
    for column in schema.columns:
        if column.field_id is None:
            continue
        minimum, maximum, null_count = _column_statistics(
            column, target_schema[column.name], scan_result.data_files
        )
        statistics[f"{column.name}_min"] = minimum
        statistics[f"{column.name}_max"] = maximum
        statistics[f"{column.name}_nc"] = null_count
    table_statistics = pl.DataFrame(statistics)

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
            inline_lf = (
                pl.LazyFrame(inline_data)
                .pipe(
                    _align_schema,
                    ducklake_schema=schema,
                    polars_schema=target_schema,
                    field_id_mapping=arrow_schema_field_ids(inline_data),
                )
                .match_to_schema(target_schema, integer_cast="upcast", float_cast="upcast")
            )
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


def _column_statistics(
    column: Column, dtype: pl.DataType | pld.DataTypeClass, files: list[ScanDataFile]
) -> tuple[pl.Series, pl.Series, pl.Series]:
    if isinstance(column.data_type, typedefs.Struct):
        # Catalog statistics are keyed by leaf field ID. Polars expects min/max and null
        # counts to follow the struct's field paths, with each enum leaf normalized first.
        field_dtypes = cast(pl.Struct, dtype).to_schema()
        children = {
            field.name: _column_statistics(field, field_dtypes[field.name], files)
            for field in column.data_type.fields
        }
        minimum, maximum, null_count = (
            pl.DataFrame({name: stats[index] for name, stats in children.items()}).to_struct()
            for index in range(3)
        )
        return minimum, maximum, null_count

    stats = [
        file.statistics.column_stats.get(column.field_id) if column.field_id is not None else None
        for file in files
    ]
    bounds_dtype = pl.String if isinstance(dtype, pl.Enum) else dtype
    minimum = pl.Series([stat.min_value if stat else None for stat in stats], dtype=bounds_dtype)
    maximum = pl.Series([stat.max_value if stat else None for stat in stats], dtype=bounds_dtype)
    if isinstance(dtype, pl.Enum):
        minimum, maximum = _enum_statistics(minimum, maximum, dtype)
    null_count = pl.Series(
        [stat.null_count if stat else None for stat in stats], dtype=pl.get_index_type()
    )
    return minimum, maximum, null_count


def _enum_statistics(
    minimum: pl.Series, maximum: pl.Series, dtype: pl.Enum
) -> tuple[pl.Series, pl.Series]:
    # DuckLake/Parquet bounds use lexical string order, whereas Polars enums use category
    # order. Every category in the lexical interval may occur in the file, including ones
    # whose enum codes fall outside the codes of the two endpoints. Keep the persisted
    # statistics lexical for other readers; only convert the bounds passed to Polars.
    # Polars can still reject equality with a nonmember string when evaluating its ordered
    # skip predicate, as it does for ordinary Enum statistics.
    categories = dtype.categories.to_list()
    enum_order = {category: index for index, category in enumerate(categories)}
    lexical_categories = sorted(categories)

    def bounds(lower: str | None, upper: str | None) -> tuple[str | None, str | None]:
        if lower is None or upper is None:
            return None, None
        start = bisect_left(lexical_categories, lower)
        stop = bisect_right(lexical_categories, upper)
        candidates = lexical_categories[start:stop]
        if not candidates:
            return None, None
        return (
            min(candidates, key=enum_order.__getitem__),
            max(candidates, key=enum_order.__getitem__),
        )

    converted = [bounds(lower, upper) for lower, upper in zip(minimum, maximum, strict=True)]
    return (
        pl.Series(minimum.name, [lower for lower, _ in converted], dtype=dtype),
        pl.Series(maximum.name, [upper for _, upper in converted], dtype=dtype),
    )


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


def _align_schema(
    lf: pl.LazyFrame,
    ducklake_schema: Schema,
    polars_schema: pl.Schema,
    field_id_mapping: dict[int, str],
) -> pl.LazyFrame:
    projections = _derive_projections(ducklake_schema.columns, polars_schema, field_id_mapping)
    return lf.select(projections)


def _derive_projections(
    columns: list[Column], target_schema: pl.Schema, field_id_mapping: dict[int, str]
) -> list[pl.Expr]:
    projections: list[pl.Expr] = []
    for column in columns:
        field_id = cast(int, column.field_id)
        existing_column_name = field_id_mapping.get(field_id)
        target_dtype = target_schema[column.name]
        if existing_column_name is not None:
            # "Existing column" -> reference it and reshape to match the target dtype,
            # recursively descending into nested types (renames, inserted fields, etc.).
            projection = _reshape_existing(
                pl.col(existing_column_name), column, target_dtype, field_id_mapping
            )
        else:
            # "Missing column" -> create new expression with (possibly nested) defaults.
            projection = _new_column_expression(column, target_dtype)

        # Select the expression and apply the alias to apply renames
        projections.append(projection.alias(column.name))

    return projections


def _reshape_existing(
    base: pl.Expr,
    column: Column,
    target_dtype: pl.DataType | pld.DataTypeClass,
    field_id_mapping: dict[int, str],
) -> pl.Expr:
    """Reshape `base` (an expression producing a value whose source shape corresponds to `column`)
    so that it matches `target_dtype`.

    This recursively handles nested renames and inserted fields for Struct/List types.
    `base` may be any expression: `pl.col(name)` at the top level, `pl.element()` inside
    a `list.eval`, or `<parent>.struct.field(name)` inside a struct.
    """
    if isinstance(column.data_type, typedefs.Struct):
        struct_dtype = cast(pl.Struct, target_dtype)
        target_fields = {field.name: field.dtype for field in struct_dtype.fields}
        rebuilt: list[pl.Expr] = []
        for field in column.data_type.fields:
            sub_target = target_fields[field.name]
            existing_name = field_id_mapping.get(cast(int, field.field_id))
            if existing_name is None:
                rebuilt.append(_new_column_expression(field, sub_target).alias(field.name))
            else:
                sub_base = base.struct.field(existing_name)
                rebuilt.append(
                    _reshape_existing(sub_base, field, sub_target, field_id_mapping).alias(
                        field.name
                    )
                )
        return pl.struct(rebuilt)

    if isinstance(column.data_type, typedefs.List):
        inner_target = cast(pl.List, target_dtype).inner
        inner_column = column.data_type.inner
        existing_name = field_id_mapping.get(cast(int, inner_column.field_id))
        if existing_name is None:
            # The inner element itself was replaced with a new field. Fall back to a
            # default expression per element.
            inner_expr = _new_column_expression(inner_column, inner_target)
        else:
            inner_expr = _reshape_existing(
                pl.element(), inner_column, inner_target, field_id_mapping
            )
        return base.list.eval(inner_expr)

    # Leaf scalar: nothing to reshape.
    return base


def _new_column_expression(column: Column, dtype: pl.DataType | pld.DataTypeClass) -> pl.Expr:
    if isinstance(column.data_type, typedefs.Struct):
        if column.initial_default is not None:
            raise NotImplementedError("Initial defaults for struct columns are not supported")
        struct_schema = cast(pl.Struct, dtype).to_schema()
        return pl.struct(
            _new_column_expression(field, struct_schema[field.name]).alias(field.name)
            for field in column.data_type.fields
        )
    if isinstance(column.data_type, typedefs.List):
        if column.initial_default is not None:
            raise NotImplementedError("Initial defaults for list columns are not supported")
        inner_dtype = cast(pl.List, dtype).inner
        inner_expr = _new_column_expression(column.data_type.inner, inner_dtype)
        return pl.concat_list([inner_expr]).alias(column.name)

    if column.initial_default is None:
        return pl.lit(None, dtype=dtype).alias(column.name)
    return pl.lit(column.initial_default, dtype=dtype).alias(column.name)
