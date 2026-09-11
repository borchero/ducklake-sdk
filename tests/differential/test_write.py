import datetime as dt
from collections.abc import Callable

import duckdb
import polars as pl
import pytest
from _testutils import assert_ducklake_catalogs_equal

import ducklake as dl


@pytest.mark.differential
def test_match_reference_write_parquet(
    ducklake: dl.Ducklake,
    catalog_url: str,
    reference_catalog_url: str,
    reference_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    # Act
    ducklake.create_table("test", {"x": dl.Int64()})
    table = ducklake.table("test")
    table.write_polars(pl.DataFrame({"x": range(100)}))

    reference_duckdb_connection.execute("CREATE TABLE test (x BIGINT)")
    reference_duckdb_connection.execute(
        "INSERT INTO test VALUES " + ", ".join(f"({i})" for i in range(100))
    )

    # Assert
    assert_ducklake_catalogs_equal(
        reference_catalog_url,
        catalog_url,
        # TODO: Properly compute `value_count`
        extra_ignored_columns={"ducklake_file_column_stats": ["value_count"]},
    )


# A single bulk insert into a bucket-partitioned table produces one data file per populated
# bucket. `data_file_id` and `row_id_start` are assigned in whatever order each engine happens to
# materialize/finalize those files, which isn't guaranteed to agree between `ducklake-sdk` and the
# reference extension. The values that matter for correctness (`record_count`, per-file min/max
# stats, and `partition_value`) are still compared.
_BUCKET_PARTITION_IGNORED_COLUMNS = {
    "ducklake_data_file": ["data_file_id", "row_id_start"],
    "ducklake_file_partition_value": ["data_file_id"],
    # TODO: Properly compute `value_count`
    "ducklake_file_column_stats": ["value_count", "data_file_id"],
}


@pytest.mark.differential
@pytest.mark.parametrize(
    ("ducklake_dtype", "sql_type", "values", "to_sql_literal", "extra_ignored_columns"),
    [
        pytest.param(dl.Int64(), "BIGINT", list(range(100)), str, {}, id="int64"),
        pytest.param(
            dl.Varchar(),
            "VARCHAR",
            [f"item-{i}" for i in range(100)],
            lambda v: f"'{v}'",
            {},
            id="varchar",
        ),
        pytest.param(
            dl.Boolean(),
            "BOOLEAN",
            [i % 2 == 0 for i in range(100)],
            lambda v: "TRUE" if v else "FALSE",
            {},
            id="boolean",
        ),
        pytest.param(
            dl.Date(),
            "DATE",
            [dt.date(2020, 1, 1) + dt.timedelta(days=i) for i in range(100)],
            lambda v: f"DATE '{v.isoformat()}'",
            {},
            id="date",
        ),
        pytest.param(
            dl.Timestamp("microseconds"),
            "TIMESTAMP",
            [dt.datetime(2020, 1, 1) + dt.timedelta(minutes=i) for i in range(100)],
            lambda v: f"TIMESTAMP '{v.isoformat(sep=' ')}'",
            # TODO: `ducklake-sdk` formats `Timestamp` stats as RFC3339 (with a `+00:00` offset),
            #  while the reference extension formats them without an offset (`'2020-01-01
            #  00:00:00'`).
            {
                "ducklake_table_column_stats": ["min_value", "max_value"],
                "ducklake_file_column_stats": ["min_value", "max_value"],
            },
            id="timestamp",
        ),
        pytest.param(
            dl.Float64(),
            "DOUBLE",
            [i + 0.5 for i in range(100)],
            repr,
            # TODO: `contains_nan` is never actually computed by `ducklake-sdk` (always `None`),
            #  in both the in-memory-Arrow and read-Parquet-footer statistics paths.
            {
                "ducklake_table_column_stats": ["contains_nan"],
                "ducklake_file_column_stats": ["contains_nan"],
            },
            id="float64",
        ),
    ],
)
def test_match_reference_write_bucket_partition(
    ducklake: dl.Ducklake,
    catalog_url: str,
    reference_catalog_url: str,
    reference_duckdb_connection: duckdb.DuckDBPyConnection,
    ducklake_dtype: dl.DataType,
    sql_type: str,
    values: list,
    to_sql_literal: Callable[[object], str],
    extra_ignored_columns: dict[str, list[str]],
) -> None:
    # Arrange
    # No hand-picked bucket ids here: both sides hash the *same* values with their own
    # implementation, so any mismatch in `ducklake-sdk`'s bucket hash shows up as a difference in
    # the resulting partition values/file layout, without us needing to precompute anything.
    table = ducklake.create_table("test", {"x": ducklake_dtype})
    table.update_partitioning(
        dl.Partitioning(dl.PartitionColumn("x", transform="bucket", num_buckets=8))
    )
    reference_duckdb_connection.execute(f"CREATE TABLE test (x {sql_type})")
    reference_duckdb_connection.execute("ALTER TABLE test SET PARTITIONED BY (bucket(8, x))")

    # Act
    pl_dtype = pl.Schema(dl.Schema({"x": ducklake_dtype}))["x"]
    table.sink_polars(pl.LazyFrame({"x": values}, schema={"x": pl_dtype}))
    reference_duckdb_connection.execute(
        "INSERT INTO test VALUES " + ", ".join(f"({to_sql_literal(v)})" for v in values)
    )

    # Assert
    ignored_columns = {k: list(v) for k, v in _BUCKET_PARTITION_IGNORED_COLUMNS.items()}
    for table_name, columns in extra_ignored_columns.items():
        ignored_columns.setdefault(table_name, []).extend(columns)
    assert_ducklake_catalogs_equal(
        reference_catalog_url, catalog_url, extra_ignored_columns=ignored_columns
    )


@pytest.mark.differential
def test_match_reference_write_inline(
    ducklake: dl.Ducklake,
    catalog_url: str,
    reference_catalog_url: str,
    reference_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    # Act
    ducklake.create_table("test", {"x": dl.Int64()})
    table = ducklake.table("test")
    table.write_polars(pl.DataFrame({"x": [1, 2, 3]}))

    reference_duckdb_connection.execute("CREATE TABLE test (x BIGINT)")
    reference_duckdb_connection.execute("INSERT INTO test VALUES (1), (2), (3)")

    # Assert
    assert_ducklake_catalogs_equal(reference_catalog_url, catalog_url)
