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
    table = ducklake.get_table("test")
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


@pytest.mark.differential
def test_match_reference_write_inline(
    ducklake: dl.Ducklake,
    catalog_url: str,
    reference_catalog_url: str,
    reference_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    # Act
    ducklake.create_table("test", {"x": dl.Int64()})
    table = ducklake.get_table("test")
    table.write_polars(pl.DataFrame({"x": [1, 2, 3]}))

    reference_duckdb_connection.execute("CREATE TABLE test (x BIGINT)")
    reference_duckdb_connection.execute("INSERT INTO test VALUES (1), (2), (3)")

    # Assert
    assert_ducklake_catalogs_equal(reference_catalog_url, catalog_url)
