import duckdb
import pytest
from _testutils import assert_ducklake_catalogs_equal

import ducklake as dl


@pytest.mark.differential
def test_match_reference_table_creation(
    ducklake: dl.Ducklake,
    catalog_url: str,
    reference_catalog_url: str,
    reference_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    # Act
    ducklake.create_table("test", {"x": dl.Int64()})
    reference_duckdb_connection.execute("CREATE TABLE test (x BIGINT)")

    # Assert
    assert_ducklake_catalogs_equal(reference_catalog_url, catalog_url)


@pytest.mark.differential
def test_match_reference_comment(
    ducklake: dl.Ducklake,
    catalog_url: str,
    reference_catalog_url: str,
    reference_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    # Act
    table = ducklake.create_table("test", {"x": dl.Int64()})
    table.add_tag("comment", "test")
    table.update_partitioning(dl.Partitioning(["x"]))
    table.rename("test_rename")

    reference_duckdb_connection.execute("CREATE TABLE test (x BIGINT)")
    reference_duckdb_connection.execute("COMMENT ON TABLE test IS 'test'")
    reference_duckdb_connection.execute("ALTER TABLE test SET PARTITIONED BY (x)")
    reference_duckdb_connection.execute("ALTER TABLE test RENAME TO test_rename")

    # Assert
    assert_ducklake_catalogs_equal(reference_catalog_url, catalog_url)


@pytest.mark.differential
def test_match_reference_nested_types(
    ducklake: dl.Ducklake,
    catalog_url: str,
    reference_catalog_url: str,
    reference_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    # Act
    ducklake.create_table(
        "test",
        {
            "l": dl.List(dl.Struct({"a": dl.Int64(), "b": dl.Varchar()})),
            "s": dl.Map(dl.Varchar(), dl.Int64()),
        },
    )
    reference_duckdb_connection.execute("""
        CREATE TABLE test (
            l STRUCT(a BIGINT, b VARCHAR)[],
            s MAP(VARCHAR, BIGINT)
        )
    """)

    # Assert
    assert_ducklake_catalogs_equal(reference_catalog_url, catalog_url)


@pytest.mark.differential
def test_match_reference_table_alter(
    ducklake: dl.Ducklake,
    catalog_url: str,
    reference_catalog_url: str,
    reference_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    # Arrange
    table = ducklake.create_table("test", {"x": dl.Int32()})
    reference_duckdb_connection.execute("CREATE TABLE test (x INTEGER)")

    # Act & Assert
    # Round 1: Update column dtype
    table.update_column_dtype("x", dl.Int64())
    reference_duckdb_connection.execute("ALTER TABLE test ALTER COLUMN x TYPE BIGINT")
    assert_ducklake_catalogs_equal(reference_catalog_url, catalog_url)

    # Round 2: Add another column
    table.add_column(dl.Column("y", dl.Varchar()))
    reference_duckdb_connection.execute("ALTER TABLE test ADD COLUMN y VARCHAR")
    assert_ducklake_catalogs_equal(reference_catalog_url, catalog_url)

    # Round 3: Drop the first column
    table.remove_column("x")
    reference_duckdb_connection.execute("ALTER TABLE test DROP COLUMN x")
    assert_ducklake_catalogs_equal(reference_catalog_url, catalog_url)
