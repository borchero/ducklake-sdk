import polars as pl
import pytest

import ducklake as dl

pytestmark = pytest.mark.skip_config(
    catalog="mysql", reason="The DuckDB MySQL connector is unreliable."
)


def test_merge_adjacent_files(ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.set_metadata(data_inlining_row_limit=0)
    for i in range(3):
        table.write_polars(pl.DataFrame({"x": [i]}))
    assert len(table.scan().data_files) == 3

    # Act
    result = table.merge_adjacent_files(min_file_size=0)

    # Assert
    assert result == [
        {
            "table_name": dl.TableName("main", random_table_name),
            "files_processed": 3,
            "files_created": 1,
        }
    ]
    assert len(table.scan().data_files) == 1


def test_merge_adjacent_files_in_schema(
    ducklake: dl.Ducklake, random_schema_name: str, random_table_name: str
) -> None:
    # Arrange
    ducklake.create_schema(random_schema_name)
    table = ducklake.create_table((random_schema_name, random_table_name), {"x": dl.Int64()})
    sibling = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.set_metadata(data_inlining_row_limit=0)
    sibling.set_metadata(data_inlining_row_limit=0)
    for i in range(2):
        table.write_polars(pl.DataFrame({"x": [i]}))
        sibling.write_polars(pl.DataFrame({"x": [i]}))
    assert len(table.scan().data_files) == 2
    assert len(sibling.scan().data_files) == 2

    # Act
    result = table.merge_adjacent_files(min_file_size=0)

    # Assert
    assert result == [
        {
            "table_name": dl.TableName(random_schema_name, random_table_name),
            "files_processed": 2,
            "files_created": 1,
        }
    ]
    assert len(table.scan().data_files) == 1
    assert len(sibling.scan().data_files) == 2


def test_rewrite_data_files(ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange: write a file then delete most rows so its delete ratio is high
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.set_metadata(data_inlining_row_limit=0)
    table.write_polars(pl.DataFrame({"x": list(range(10))}))
    ducklake._duckdb_connection.execute(f"DELETE FROM {table.name} WHERE x < 9")
    files_before = table.scan().data_files
    assert len(files_before) == 1
    assert files_before[0].delete_files is not None and len(files_before[0].delete_files) == 1

    # Act
    result = table.rewrite_data_files(delete_threshold=0.5)

    # Assert
    assert result == [
        {
            "table_name": dl.TableName("main", random_table_name),
            "files_processed": 1,
            "files_created": 1,
        }
    ]
    files_after = table.scan().data_files
    assert len(files_after) == 1
    assert files_after[0].path != files_before[0].path
    assert not files_after[0].delete_files
    assert pl.read_parquet(
        files_after[0].path, storage_options=ducklake._storage_options.to_dict()
    )["x"].to_list() == [9]
