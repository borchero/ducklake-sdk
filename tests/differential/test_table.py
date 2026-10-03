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


@pytest.fixture(
    params=[
        (dl.Variant(), "VARIANT", "42::VARIANT"),
        (dl.Geometry(), "GEOMETRY", "'POINT (1 2)'::GEOMETRY"),
        (
            dl.List(dl.Column("element", dl.Geometry(), field_id=2)),
            "GEOMETRY[]",
            "['POINT (1 2)'::GEOMETRY]",
        ),
        (
            dl.Struct([dl.Column("g", dl.Geometry(), field_id=2)]),
            "STRUCT(g GEOMETRY)",
            "{'g': 'POINT (1 2)'::GEOMETRY}",
        ),
        (
            dl.Map(
                dl.Column("key", dl.Varchar(), field_id=2),
                dl.Column("value", dl.Geometry(), field_id=3),
            ),
            "MAP(VARCHAR, GEOMETRY)",
            "MAP {'g': 'POINT (1 2)'::GEOMETRY}",
        ),
    ],
    ids=["variant", "geometry", "geometry_list", "geometry_struct", "geometry_map"],
)
def metadata_type(request: pytest.FixtureRequest) -> tuple[dl.DataType, str, str]:
    return request.param


@pytest.mark.differential
def test_match_reference_metadata_type_table_creation(
    ducklake: dl.Ducklake,
    catalog_url: str,
    reference_catalog_url: str,
    reference_duckdb_connection: duckdb.DuckDBPyConnection,
    metadata_type: tuple[dl.DataType, str, str],
) -> None:
    # Arrange
    dtype, sql_type, _ = metadata_type

    # Act
    ducklake.create_table("test", {"payload": dtype})
    reference_duckdb_connection.execute(f"CREATE TABLE test (payload {sql_type})")

    # Assert
    assert_ducklake_catalogs_equal(reference_catalog_url, catalog_url)


@pytest.mark.differential
def test_parse_reference_metadata_type_catalog(
    reference_catalog_url: str,
    reference_duckdb_connection: duckdb.DuckDBPyConnection,
    metadata_type: tuple[dl.DataType, str, str],
) -> None:
    # Arrange
    dtype, sql_type, sql_value = metadata_type
    reference_duckdb_connection.execute(f"CREATE TABLE test (payload {sql_type})")
    reference_duckdb_connection.execute(f"INSERT INTO test VALUES ({sql_value})")

    # Act
    with dl.connect(reference_catalog_url) as reference_ducklake:
        table = reference_ducklake.table("test")
        columns = table.schema.columns
        data_files = table.scan().data_files

    # Assert
    assert columns == [dl.Column("payload", dtype, field_id=1)]
    assert len(data_files) == 1


@pytest.mark.differential
def test_read_reference_sort_info(
    reference_catalog_url: str,
    reference_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    # Arrange
    reference_duckdb_connection.execute("CREATE TABLE test (x BIGINT, y BIGINT)")
    reference_duckdb_connection.execute(
        "ALTER TABLE test SET SORTED BY (x DESC NULLS FIRST, y ASC NULLS LAST)"
    )

    # Act
    with dl.connect(reference_catalog_url) as lake:
        sort_info = lake.table("test").sort_info

    # Assert
    assert sort_info is not None
    assert [(c.expression, c.direction, c.null_order) for c in sort_info.columns] == [
        ("x", "descending", "nulls_first"),
        ("y", "ascending", "nulls_last"),
    ]


@pytest.mark.differential
def test_match_reference_sort_info(
    ducklake: dl.Ducklake,
    catalog_url: str,
    reference_catalog_url: str,
    reference_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    # Arrange
    table = ducklake.create_table("test", {"x": dl.Int64(), "y": dl.Int64()})
    reference_duckdb_connection.execute("CREATE TABLE test (x BIGINT, y BIGINT)")

    # Act
    table.update_sort_info(dl.SortInfo([dl.SortColumn("x", direction="descending"), "y"]))
    reference_duckdb_connection.execute(
        "ALTER TABLE test SET SORTED BY (x DESC NULLS LAST, y ASC NULLS LAST)"
    )

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
