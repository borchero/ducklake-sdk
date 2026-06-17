import duckdb
import polars as pl
import pytest
from _testutils import assert_ducklake_catalogs_equal

import ducklake as dl


@pytest.mark.differential
def test_match_reference_expire_snapshot_versions(
    ducklake: dl.Ducklake,
    catalog_url: str,
    reference_catalog_url: str,
    reference_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    # Arrange
    first_table = ducklake.create_table("first", schema={"x": dl.Int64()})  # snapshot 1
    second_table = ducklake.create_table("second", schema={"x": dl.Int64()})  # snapshot 2
    first_table.sink_polars(pl.LazyFrame({"x": range(100)}))  # snapshot 3
    second_table.delete()  # snapshot 4
    first_table.sink_polars(pl.LazyFrame({"x": range(100)}))  # snapshot 5
    first_table.delete()  # snapshot 6
    ducklake.create_table("third", schema={"x": dl.Int64()})  # snapshot 7

    reference_duckdb_connection.execute("CREATE TABLE first (x BIGINT)")
    reference_duckdb_connection.execute("CREATE TABLE second (x BIGINT)")
    reference_duckdb_connection.execute("INSERT INTO first SELECT * FROM range(100)")
    reference_duckdb_connection.execute("DROP TABLE second")
    reference_duckdb_connection.execute("INSERT INTO first SELECT * FROM range(100)")
    reference_duckdb_connection.execute("DROP TABLE first")
    reference_duckdb_connection.execute("CREATE TABLE third (x BIGINT)")

    # Act
    ducklake.expire_snapshots(versions=[2, 3, 4, 5])
    reference_duckdb_connection.execute(
        "CALL ducklake_expire_snapshots('my_ducklake', versions => [2, 3, 4, 5])"
    )

    # Assert
    assert_ducklake_catalogs_equal(reference_catalog_url, catalog_url)


@pytest.mark.differential
def test_match_reference_cleanup_old_files(
    ducklake: dl.Ducklake,
    catalog_url: str,
    reference_catalog_url: str,
    reference_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    # Arrange
    first_table = ducklake.create_table("first", schema={"x": dl.Int64()})  # snapshot 1
    first_table.sink_polars(pl.LazyFrame({"x": range(100)}))  # snapshot 2
    first_table.delete()  # snapshot 3

    reference_duckdb_connection.execute("CREATE TABLE first (x BIGINT)")
    reference_duckdb_connection.execute("INSERT INTO first SELECT * FROM range(100)")
    reference_duckdb_connection.execute("DROP TABLE first")

    # Act
    ducklake.expire_snapshots(versions=[0, 1, 2])
    ducklake.cleanup_old_files(cleanup_all=True)

    reference_duckdb_connection.execute(
        "CALL ducklake_expire_snapshots('my_ducklake', versions => [0, 1, 2])"
    )
    reference_duckdb_connection.execute(
        "CALL ducklake_cleanup_old_files('my_ducklake', cleanup_all => true)"
    )

    # Assert
    assert_ducklake_catalogs_equal(reference_catalog_url, catalog_url)
