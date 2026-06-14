import datetime as dt
from pathlib import Path

import polars as pl
import pytest

import ducklake as dl


@pytest.fixture()
def ducklake_with_scheduled_files(ducklake: dl.Ducklake, random_table_name: str) -> dl.Ducklake:
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.set_metadata(data_inlining_row_limit=0)
    table.write_polars(pl.DataFrame({"x": [1]}))
    table.delete()
    ducklake.expire_snapshots(older_than=dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1))
    return ducklake


def test_cleanup_old_files(ducklake_with_scheduled_files: dl.Ducklake) -> None:
    # Arrange
    ducklake = ducklake_with_scheduled_files

    # Act
    dry_run_result = ducklake.cleanup_old_files(cleanup_all=True, dry_run=True)
    result = ducklake.cleanup_old_files(cleanup_all=True)

    # Assert
    assert dry_run_result
    assert set(result) == set(dry_run_result)
    assert ducklake.cleanup_old_files(cleanup_all=True) == []


def test_cleanup_old_files_older_than(ducklake_with_scheduled_files: dl.Ducklake) -> None:
    # Arrange
    ducklake = ducklake_with_scheduled_files

    # Act
    too_old = ducklake.cleanup_old_files(
        older_than=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)
    )
    result = ducklake.cleanup_old_files(
        older_than=dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)
    )

    # Assert
    assert too_old == []
    assert result
    assert ducklake.cleanup_old_files(cleanup_all=True) == []


def test_cleanup_old_files_default_keeps_recent(
    ducklake_with_scheduled_files: dl.Ducklake,
) -> None:
    # Arrange: the default `delete_older_than` is two days, so freshly scheduled files are kept
    ducklake = ducklake_with_scheduled_files

    # Act
    result = ducklake.cleanup_old_files()

    # Assert
    assert result == []
    assert ducklake.cleanup_old_files(cleanup_all=True)


@pytest.mark.skip_config(
    storage="s3", reason="Physical file inspection requires direct filesystem access."
)
@pytest.mark.skip_config(
    storage="gcs", reason="Physical file inspection requires direct filesystem access."
)
@pytest.mark.skip_config(
    storage="azure", reason="Physical file inspection requires direct filesystem access."
)
def test_cleanup_old_files_deletes_from_storage(
    ducklake: dl.Ducklake, random_table_name: str, storage_path: str
) -> None:
    # Arrange
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.set_metadata(data_inlining_row_limit=0)
    table.write_polars(pl.DataFrame({"x": [1]}))
    table_dir = Path(storage_path) / "main" / random_table_name
    assert list(table_dir.glob("*.parquet"))
    table.delete()
    ducklake.expire_snapshots(older_than=dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1))

    # Act
    result = ducklake.cleanup_old_files(cleanup_all=True)

    # Assert
    assert result
    assert list(table_dir.glob("*.parquet")) == []
