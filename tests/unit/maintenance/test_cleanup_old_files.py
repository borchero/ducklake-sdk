import datetime as dt

import polars as pl
import pytest
from _testutils import storage_file_exists

import ducklake as dl


@pytest.fixture()
def ducklake_with_scheduled_files(
    ducklake: dl.Ducklake, random_table_name: str
) -> tuple[dl.Ducklake, set[str]]:
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.sink_polars(pl.LazyFrame({"x": [1]}))
    file_paths = {file.path for file in table.scan().data_files}
    table.delete()
    ducklake.expire_snapshots(versions=[0, 1, 2])
    return ducklake, file_paths


# -------------------------------------------- TESTS -------------------------------------------- #


def test_cleanup_old_files_dry_run(
    ducklake_with_scheduled_files: tuple[dl.Ducklake, set[str]],
) -> None:
    # Arrange
    ducklake, file_paths = ducklake_with_scheduled_files

    # Act
    dry_run = ducklake.cleanup_old_files(cleanup_all=True, dry_run=True)
    actual = ducklake.cleanup_old_files(cleanup_all=True)

    # Assert
    assert set(dry_run) == file_paths
    assert set(actual) == file_paths


def test_cleanup_old_files(ducklake_with_scheduled_files: tuple[dl.Ducklake, set[str]]) -> None:
    # Arrange
    ducklake, file_paths = ducklake_with_scheduled_files

    # Act
    result = ducklake.cleanup_old_files(cleanup_all=True)

    # Assert: the files are deleted and the rows are removed from the catalog
    assert set(result) == file_paths
    assert ducklake.cleanup_old_files(cleanup_all=True) == []
    assert all(not storage_file_exists(ducklake, path) for path in result)


@pytest.mark.parametrize(("offset_days", "deletes_files"), [(-1, False), (1, True)])
def test_cleanup_old_files_older_than(
    ducklake_with_scheduled_files: tuple[dl.Ducklake, set[str]],
    offset_days: int,
    deletes_files: bool,
) -> None:
    # Arrange
    ducklake, _ = ducklake_with_scheduled_files
    older_than = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=offset_days)

    # Act
    result = ducklake.cleanup_old_files(older_than=older_than)

    # Assert
    assert (len(result) > 0) == deletes_files
    assert all(not storage_file_exists(ducklake, path) for path in result)


def test_cleanup_old_files_default_keeps_recent(
    ducklake_with_scheduled_files: tuple[dl.Ducklake, set[str]],
) -> None:
    # Arrange: the default `delete_older_than` is two days, so freshly scheduled files are kept
    ducklake, _ = ducklake_with_scheduled_files

    # Act
    result = ducklake.cleanup_old_files()

    # Assert
    assert result == []
