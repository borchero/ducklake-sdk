import tempfile
from collections import defaultdict
from pathlib import Path
from typing import cast

import polars as pl
import polars.datatypes as pld

from ducklake import typedefs
from ducklake._native import arrow_schema_field_ids
from ducklake.table import Table
from ducklake.typedefs import Column, Schema

DROP_COLUMN_PREFIX = "__ducklake_drop__"


def scan_ducklake(table: Table, *, include_file_paths: str | None = None) -> pl.LazyFrame:
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

    # 2.2) Row counts
    physical_rows = sum(data_file.statistics.num_rows for data_file in scan_result.data_files)
    deleted_rows = (
        sum(
            sum(delete_file.num_deletes for delete_file in data_file.delete_files)
            for data_file in scan_result.data_files
        )
        + inline_delete_count
    )

    # 2.3) Schema and defaults
    target_schema = pl.Schema(schema)
    defaults: dict[int, pl.Series | str] = {
        col.field_id: pl.repeat(
            col.initial_default,
            len(scan_result.data_files),
            dtype=target_schema[col.name],
            eager=True,
        )
        for col in schema.columns
        if col.initial_default is not None and col.field_id is not None
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
        _deletion_files=("iceberg-position-delete", dict(iceberg_position_deletes)),
        _default_values=("iceberg", defaults),
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

    return result


def read_ducklake(table: Table, *, include_file_paths: str | None = None) -> pl.DataFrame:
    return cast(
        pl.DataFrame,  # Remove once https://github.com/astral-sh/ty/issues/2278 is fixed
        scan_ducklake(table, include_file_paths=include_file_paths).collect(
            optimizations=pl.QueryOptFlags._eager()
        ),
    )


# -------------------------------------------- UTILS -------------------------------------------- #


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
