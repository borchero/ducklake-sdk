import os

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ducklake as dl

pytestmark = pytest.mark.skip_config(
    storage="s3", reason="Local Parquet files are written to the storage path."
)


def _write_parquet(table: dl.Table, name: str, data: dict) -> str:
    _, generator = table._get_write_info()
    path = os.path.join(generator.base_path, name)
    pl.DataFrame(data).write_parquet(
        path,
        arrow_schema=table.schema,
        storage_options=table._storage_options.to_dict(),
        mkdir=True,
    )
    return name


def test_write_data_files_from_paths(ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    path = _write_parquet(table, "file.parquet", {"x": [1, 2, 3]})

    # Act
    table.write_data_files([path])

    # Assert
    assert table.scan().data_files[0].statistics.num_rows == 3
    assert_frame_equal(
        table.scan_polars().sort("x"),
        pl.LazyFrame({"x": [1, 2, 3]}, schema={"x": pl.Int64}),
    )


def test_write_data_files_with_statistics(ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    path = _write_parquet(table, "file.parquet", {"x": [1, 2, 3]})

    # Act
    table.write_data_files(
        [
            dl.WriteDataFile(
                path,
                statistics=dl.DataFileStatistics(
                    num_rows=3,
                    column_stats={1: dl.ColumnStats(min_value=1, max_value=3, null_count=0)},
                ),
            )
        ]
    )

    # Assert
    stats = table.scan().data_files[0].statistics
    assert stats.num_rows == 3
    assert stats.column_stats[1].min_value == 1
    assert stats.column_stats[1].max_value == 3


def test_write_data_files_mixed(ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    path_a = _write_parquet(table, "a.parquet", {"x": [1, 2, 3]})
    path_b = _write_parquet(table, "b.parquet", {"x": [4, 5, 6]})

    # Act
    table.write_data_files([path_a, dl.WriteDataFile(path_b)])

    # Assert
    assert_frame_equal(
        table.scan_polars().sort("x"),
        pl.LazyFrame({"x": [1, 2, 3, 4, 5, 6]}, schema={"x": pl.Int64}),
    )


def test_write_data_files_in_transaction(ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    path = _write_parquet(table, "file.parquet", {"x": [1, 2, 3]})

    # Act
    with ducklake.transaction() as tx:
        tx.table(random_table_name).write_data_files([path])

    # Assert
    assert_frame_equal(
        ducklake.get_table(random_table_name).scan_polars().sort("x"),
        pl.LazyFrame({"x": [1, 2, 3]}, schema={"x": pl.Int64}),
    )
