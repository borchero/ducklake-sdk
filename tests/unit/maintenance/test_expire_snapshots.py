import datetime as dt
import time

import polars as pl

import ducklake as dl


def test_expire_snapshots_dry_run(ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.sink_polars(pl.LazyFrame({"x": [1]}))
    table.sink_polars(pl.LazyFrame({"x": [2]}))
    snapshots_before = len(ducklake.list_snapshots())
    latest_snapshot_before = ducklake.get_latest_snapshot().id

    # Act
    result = ducklake.expire_snapshots(
        older_than=dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1), dry_run=True
    )

    # Assert
    assert len(result) == snapshots_before - 1
    assert latest_snapshot_before not in [snapshot.id for snapshot in result]
    assert len(ducklake.list_snapshots()) == snapshots_before


def test_expire_snapshots(ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.sink_polars(pl.LazyFrame({"x": [1]}))
    table.sink_polars(pl.LazyFrame({"x": [2]}))
    snapshots_before = ducklake.list_snapshots()
    latest_snapshot_before = ducklake.get_latest_snapshot().id
    assert len(snapshots_before) > 1

    # Act
    result = ducklake.expire_snapshots(
        older_than=dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)
    )

    # Assert
    assert len(result) == len(snapshots_before) - 1
    assert latest_snapshot_before not in [snapshot.id for snapshot in result]
    assert len(ducklake.list_snapshots()) == 1
    assert ducklake.get_latest_snapshot().id == latest_snapshot_before


def test_no_expire_latest_snapshot(ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    ducklake.create_table(random_table_name, {"x": dl.Int64()})
    assert len(ducklake.list_snapshots()) == 2
    assert ducklake.get_latest_snapshot().id == 1

    # Act
    expired = ducklake.expire_snapshots(versions=[0, 1])

    # Assert
    assert len(expired) == 1
    assert len(ducklake.list_snapshots()) == 1
    assert ducklake.get_latest_snapshot().id == 1


def test_expire_global_metadata(ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    ducklake.set_metadata(expire_older_than="00:00:00.001")
    time.sleep(0.01)
    ducklake.create_table(random_table_name, {"x": dl.Int64()})
    assert len(ducklake.list_snapshots()) == 2
    assert ducklake.get_latest_snapshot().id == 1

    # Act
    expired = ducklake.expire_snapshots()

    # Assert
    assert len(expired) == 1
    assert len(ducklake.list_snapshots()) == 1
    assert ducklake.get_latest_snapshot().id == 1


def test_no_expire_global_metadata(ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    ducklake.set_metadata(expire_older_than="1 day")
    ducklake.create_table(random_table_name, {"x": dl.Int64()})
    assert len(ducklake.list_snapshots()) == 2
    assert ducklake.get_latest_snapshot().id == 1

    # Act
    expired = ducklake.expire_snapshots()

    # Assert
    assert len(expired) == 0
    assert len(ducklake.list_snapshots()) == 2
    assert ducklake.get_latest_snapshot().id == 1
