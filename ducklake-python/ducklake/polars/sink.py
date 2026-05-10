from functools import partial
from typing import Literal, cast, overload

import polars as pl
from polars._typing import EngineType
from polars.io.partition import FileProviderArgs, SinkedPathsCallbackArgs
from polars.lazyframe.opt_flags import DEFAULT_QUERY_OPT_FLAGS

from ducklake import typedefs
from ducklake._native import PyDataFilePathGenerator
from ducklake.table import Table
from ducklake.transaction import TransactionTable
from ducklake.typedefs import Column, Partitioning, WriteDataFile

PARTITION_COLUMN_PREFIX = "__ducklake_partition__"


@overload
def sink_ducklake(
    lf: pl.LazyFrame,
    table: Table | TransactionTable,
    *,
    engine: EngineType = "auto",
    optimizations: pl.QueryOptFlags | None = None,
    lazy: Literal[False] = False,
) -> None: ...


@overload
def sink_ducklake(
    lf: pl.LazyFrame,
    table: Table | TransactionTable,
    *,
    engine: EngineType = "auto",
    optimizations: pl.QueryOptFlags | None = None,
    lazy: Literal[True],
) -> pl.LazyFrame: ...


def sink_ducklake(
    lf: pl.LazyFrame,
    table: Table | TransactionTable,
    *,
    engine: EngineType = "auto",
    optimizations: pl.QueryOptFlags | None = None,
    lazy: bool = False,
) -> pl.LazyFrame | None:
    # 1) First, we need to read metadata information about the table to know how to write it. This
    #    also makes sure that the table metadata is up-to-date.
    table_metadata, file_generator = table._get_write_info()

    # 2) Then, we prepare the lazy frame contents
    lf = lf.pipe(_prepare_frame, table)

    # 4) Then, we need to derive partitions from the table. If a partition applies a transform,
    #    we need to apply the transform to the lazyframe. In order to not include those transformed
    #    columns in the output, we set `include_key=False` below and simply add a new column for
    #    the partitioning for ALL partition columns, regardless of whether they have a transform.
    partition_columns: list[str] | None = None
    if table.partitioning is not None:
        lf, partition_columns = lf.pipe(_prepare_partitions, table.partitioning)

    # 5) Afterwards, we create the partitioning object for the sink. Note that we need to keep a
    #    mapping from the generated file paths to partition values as we cannot read the partition
    #    values in the sink callback but need them to write to DuckLake.
    partition_value_cache: dict[str, pl.DataFrame] = {}
    target = pl.PartitionBy(
        base_path=file_generator.base_path,
        file_path_provider=partial(
            _file_path_provider_callback, partition_value_cache, file_generator
        ),
        key=partition_columns,
        include_key=False if partition_columns else None,
        approximate_bytes_per_file=table_metadata["target_file_size"],
    )

    # 6) Eventually, we can actually write the data. The callback will take care of actually
    #    committing the new data files to the Ducklake. This allows to perform the entire
    #    operation lazily if requested.
    return lf.sink_parquet(
        target,
        storage_options=table._storage_options.to_dict(),
        mkdir=True,
        compression=table_metadata["parquet_compression"],
        compression_level=table_metadata["parquet_compression_level"],
        row_group_size=table_metadata["parquet_row_group_size"],
        statistics=True,
        engine=engine,
        optimizations=optimizations or DEFAULT_QUERY_OPT_FLAGS,
        lazy=lazy,
        arrow_schema=table.schema,
        _sinked_paths_callback=partial(
            _sinked_paths_callback, table, file_generator.base_path, partition_value_cache
        ),
    )


# -------------------------------------------- EAGER -------------------------------------------- #


def write_ducklake(df: pl.DataFrame, table: Table | TransactionTable) -> None:
    table_metadata, _ = table._get_write_info()
    if df.height <= table_metadata["data_inlining_row_limit"]:
        # Inline the data
        df = cast(
            pl.DataFrame,  # Remove once https://github.com/astral-sh/ty/issues/2278 is fixed
            df.lazy().pipe(_prepare_frame, table).collect(optimizations=pl.QueryOptFlags._eager()),
        )
        table._write_inline_data(df)
    else:
        # Write data files
        df.lazy().pipe(
            sink_ducklake,
            table,
            optimizations=pl.QueryOptFlags._eager(),
            lazy=False,
        )


# ----------------------------------------------------------------------------------------------- #
#                                              UTILS                                              #
# ----------------------------------------------------------------------------------------------- #


def _prepare_frame(lf: pl.LazyFrame, table: Table | TransactionTable) -> pl.LazyFrame:
    # Ensure that the provided lazy frame aligns with the current schema of the table
    lf = lf.match_to_schema(pl.Schema(table.schema))

    # Make sure that we apply the current defaults if there are any
    default_exprs = [
        pl.col(col.name).pipe(_apply_default_value, col).alias(col.name)
        for col in table.schema.columns
        if _has_default_expression(col)
    ]
    if default_exprs:
        return lf.with_columns(default_exprs)
    return lf


# ------------------------------------------- DEFAULTS ------------------------------------------ #


def _has_default_expression(column: Column) -> bool:
    # Check column itself
    if column.default_value is not None:
        if isinstance(column.default_value, tuple):
            raise ValueError("Default expressions are currently not supported via polars")
        return True

    # Check nested types; omit Map since it's not supported via polars anyway
    if isinstance(column.data_type, typedefs.Struct):
        if column.default_value is not None:
            raise NotImplementedError("Default values for struct columns are not supported")
        return any(_has_default_expression(field) for field in column.data_type.fields)
    elif isinstance(column.data_type, typedefs.List):
        if column.default_value is not None:
            raise NotImplementedError("Default values for list columns are not supported")
        return _has_default_expression(column.data_type.inner)

    return False


def _apply_default_value(expr: pl.Expr, column: Column) -> pl.Expr:
    if isinstance(column.data_type, typedefs.Struct):
        return pl.struct(
            expr.struct[field.name].pipe(_apply_default_value, field).alias(field.name)
            for field in column.data_type.fields
        )
    if isinstance(column.data_type, typedefs.List):
        return expr.list.eval(pl.element().pipe(_apply_default_value, column.data_type.inner))

    # Assume primitive type here.
    # NOTE: While there is some expression in the "root expression" that needs filling defaults,
    #  this particular expression might not. We therefore need to check again if the default is
    #  actually set.
    if column.default_value is not None:
        return expr.fill_null(column.default_value)
    return expr


# ------------------------------------------ PARTITIONS ----------------------------------------- #


def _prepare_partitions(
    lf: pl.LazyFrame, partitioning: Partitioning
) -> tuple[pl.LazyFrame, list[str]]:
    result: list[str] = []

    for partition_col in partitioning.columns:
        partition_name = f"{PARTITION_COLUMN_PREFIX}{partition_col.name}"
        result.append(partition_name)

        if partition_col.transform == "bucket":
            raise NotImplementedError("Bucket transforms are currently not supported via polars")
        elif partition_col.transform == "year":
            lf = lf.with_columns(pl.col(partition_col.name).dt.year().alias(partition_name))
        elif partition_col.transform == "month":
            lf = lf.with_columns(pl.col(partition_col.name).dt.month().alias(partition_name))
        elif partition_col.transform == "day":
            lf = lf.with_columns(pl.col(partition_col.name).dt.day().alias(partition_name))
        elif partition_col.transform == "hour":
            lf = lf.with_columns(pl.col(partition_col.name).dt.hour().alias(partition_name))
        else:
            lf = lf.with_columns(pl.col(partition_col.name).alias(partition_name))

    return lf, result


# ------------------------------------------ CALLBACKS ------------------------------------------ #


def _file_path_provider_callback(
    partition_value_cache: dict[str, pl.DataFrame],
    file_generator: PyDataFilePathGenerator,
    args: FileProviderArgs,
) -> str:
    path = file_generator.generate_relative(
        [
            (name.removeprefix(PARTITION_COLUMN_PREFIX), value)
            for name, value in args.partition_keys.row(0, named=True).items()
        ]
    )
    partition_value_cache[path] = args.partition_keys
    return path


def _sinked_paths_callback(
    table: Table | TransactionTable,
    base_path: str,
    partition_value_cache: dict[str, pl.DataFrame],
    args: SinkedPathsCallbackArgs,
) -> None:
    new_data_files: list[WriteDataFile] = []
    for path in args.paths:
        # TODO: Currently, polars does not directly provide statistics about the written files, so
        #  we derive the statistics by reading the file again. This is obviously not ideal but the
        #  best we can do for now. This should be changed once the appropriate change has been made
        #  in polars. See also: https://github.com/pola-rs/polars/issues/27226
        relative_path = path.removeprefix(base_path)
        partitions = partition_value_cache[relative_path]
        data_file = WriteDataFile(
            path=relative_path,
            partition_values={
                name.removeprefix(PARTITION_COLUMN_PREFIX): value
                for name, value in partitions.row(0, named=True).items()
            },
        )
        new_data_files.append(data_file)

    table._write_data_files(new_data_files)
