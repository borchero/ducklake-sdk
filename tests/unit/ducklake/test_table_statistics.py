import polars as pl
import pytest

import ducklake as dl


@pytest.mark.parametrize("second_file_size", [200, None])
def test_statistics_file_totals(ducklake: dl.Ducklake, second_file_size: int | None) -> None:
    # Arrange
    ducklake.create_table("empty", {"x": dl.Int64()})
    table = ducklake.create_table("files", {"x": dl.Int64()})
    table.write_data_files(
        [
            dl.WriteDataFile(
                "first.parquet", statistics=dl.DataFileStatistics(5, file_size_bytes=100)
            ),
            dl.WriteDataFile(
                "second.parquet",
                statistics=dl.DataFileStatistics(3, file_size_bytes=second_file_size),
            ),
        ]
    )

    # Act
    results = {
        table.name.name: stats for table, stats in ducklake.list_tables("main", statistics=True)
    }

    # Assert
    assert set(results) == {"empty", "files"}
    files = results["files"]
    assert files.num_rows == 8
    assert files.num_data_files == 2
    assert files.total_file_size_bytes == (300 if second_file_size is not None else None)
    assert files.min_file_size_bytes == 100
    assert files.max_file_size_bytes == (second_file_size or 100)
    empty = results["empty"]
    assert empty.num_rows == empty.num_data_files == empty.total_file_size_bytes == 0
    assert empty.min_file_size_bytes is empty.max_file_size_bytes is None


@pytest.mark.skip_config(catalog="mysql", reason="Data inlining is not supported for MySQL.")
def test_statistics_inline_rows_at_snapshot(ducklake: dl.Ducklake) -> None:
    # Arrange
    table = ducklake.create_table("inline", {"x": dl.Int64()})
    table.write_polars(pl.DataFrame({"x": [1, 2]}))
    snapshot = ducklake.get_latest_snapshot().id
    table.add_column(dl.Column("y", dl.Varchar()))
    table.write_polars(pl.DataFrame({"x": [3], "y": ["new"]}))

    # Act
    current = ducklake.list_tables(statistics=True)[0][1]
    historical = ducklake.at(snapshot).list_tables(statistics=True)[0][1]

    # Assert
    assert current.num_rows == current.num_inline_rows == 3
    assert historical.num_rows == historical.num_inline_rows == 2
    assert current.num_data_files == historical.num_data_files == 0


@pytest.mark.skip_config(catalog="mysql", reason="The DuckDB MySQL connector is unreliable.")
def test_statistics_deleted_rows(ducklake: dl.Ducklake) -> None:
    # Arrange
    table = ducklake.create_table("files", {"x": dl.Int64()})
    table.set_metadata(data_inlining_row_limit=0)
    table.write_polars(pl.DataFrame({"x": [1, 2, 3]}))
    snapshot = ducklake.get_latest_snapshot().id
    ducklake._duckdb_connection.execute(f"DELETE FROM {table.name} WHERE x = 1")

    # Act
    current = ducklake.list_tables(statistics=True)[0][1]
    historical = ducklake.at(snapshot).list_tables(statistics=True)[0][1]

    # Assert
    assert current.num_rows == 2
    assert current.num_deleted_rows == 1
    assert historical.num_rows == 3
    assert historical.num_deleted_rows == 0
