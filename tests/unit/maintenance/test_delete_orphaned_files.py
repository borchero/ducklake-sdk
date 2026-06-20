import datetime as dt
import os

import polars as pl
from _testutils import storage_file_exists

import ducklake as dl


def test_delete_orphaned_files(
    ducklake: dl.Ducklake, random_table_name: str, storage_path: str
) -> None:
    # Arrange
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.sink_polars(pl.LazyFrame({"x": [1]}))
    live_files = {file.path for file in table.scan().data_files}

    orphan_file = os.path.join(storage_path, "test.parquet")
    pl.LazyFrame({"x": [2]}).sink_parquet(
        orphan_file, mkdir=True, storage_options=ducklake._storage_options.to_dict()
    )

    # Act / Assert
    dry_run_result = ducklake.delete_orphaned_files(cleanup_all=True, dry_run=True)
    assert storage_file_exists(ducklake, orphan_file)

    result = ducklake.delete_orphaned_files(cleanup_all=True)
    assert not storage_file_exists(ducklake, orphan_file)

    # Assert
    assert orphan_file in dry_run_result
    assert orphan_file in result
    assert len(table.scan().data_files) == 1
    assert all(storage_file_exists(ducklake, path) for path in live_files)


def test_delete_orphaned_files_keeps_scheduled_for_deletion(
    ducklake: dl.Ducklake, random_table_name: str, storage_path: str
) -> None:
    # Arrange
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.sink_polars(pl.LazyFrame({"x": [1]}))
    live_files = {file.path for file in table.scan().data_files}

    table.delete()
    ducklake.expire_snapshots(versions=[0, 1, 2])

    # Act
    result = ducklake.delete_orphaned_files(cleanup_all=True)

    # Assert
    assert result == []
    assert all(storage_file_exists(ducklake, path) for path in live_files)


def test_delete_orphaned_files_respects_older_than(
    ducklake: dl.Ducklake, storage_path: str
) -> None:
    # Arrange
    orphan_file = os.path.join(storage_path, "test.parquet")
    pl.LazyFrame({"x": [2]}).sink_parquet(
        orphan_file, mkdir=True, storage_options=ducklake._storage_options.to_dict()
    )

    # Act
    result_default = ducklake.delete_orphaned_files()
    result_explicit = ducklake.delete_orphaned_files(
        older_than=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)
    )

    # Assert
    assert result_default == []
    assert result_explicit == []
    assert storage_file_exists(ducklake, orphan_file)
