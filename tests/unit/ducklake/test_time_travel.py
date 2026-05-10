import datetime as dt

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ducklake as dl
import ducklake.exceptions as dlexc


def test_time_travel_by_snapshot_id(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    lf = pl.LazyFrame({"x": [1, 2, 3]})
    table.sink_polars(lf)
    snapshot_id = shared_ducklake.get_latest_snapshot().id
    table.sink_polars(lf)

    # Act
    time_traveled_table = shared_ducklake.at(snapshot_id).get_table(random_table_name)

    # Assert
    assert_frame_equal(pl.concat([lf, lf]), table.scan_polars())
    assert_frame_equal(lf, time_traveled_table.scan_polars())


@pytest.mark.skip_config(
    catalog="mysql", reason="MySQL uses second-level precision for timestamps."
)
@pytest.mark.skip_config(
    catalog="sqlite", reason="SQLite on Linux seems to have issues with timestamp precision."
)
def test_time_travel_by_fixed_timestamp(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    lf = pl.LazyFrame({"x": [1, 2, 3]})
    table.sink_polars(lf)
    snapshot_ts = shared_ducklake.get_latest_snapshot().timestamp
    table.sink_polars(lf)

    # Act
    time_traveled_table = shared_ducklake.at(snapshot_ts).get_table(random_table_name)

    # Assert
    assert_frame_equal(pl.concat([lf, lf]), table.scan_polars())
    assert_frame_equal(lf, time_traveled_table.scan_polars())


@pytest.mark.skip_config(
    catalog="mysql", reason="MySQL uses second-level precision for timestamps."
)
def test_time_travel_by_fuzzy_timestamp(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    lf = pl.LazyFrame({"x": [1, 2, 3]})
    table.sink_polars(lf)
    snapshot_ts = dt.datetime.now(dt.timezone.utc)
    table.sink_polars(lf)

    # Act
    time_traveled_table = shared_ducklake.at(snapshot_ts).get_table(random_table_name)

    # Assert
    assert_frame_equal(pl.concat([lf, lf]), table.scan_polars())
    assert_frame_equal(lf, time_traveled_table.scan_polars())


def test_time_travel_no_transaction(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    snapshot_id = shared_ducklake.get_latest_snapshot().id
    time_traveled_table = shared_ducklake.at(snapshot_id).get_table(random_table_name)

    # Act
    with pytest.raises(dlexc.ImmutableDucklakeError):
        time_traveled_table.add_tag("foo", "bar")


def test_time_travel_list_snapshots_returns_only_traveled(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.sink_polars(pl.LazyFrame({"x": [1, 2, 3]}))
    snapshot_id = shared_ducklake.get_latest_snapshot().id
    table.sink_polars(pl.LazyFrame({"x": [4, 5, 6]}))

    # Act
    traveled_snapshots = shared_ducklake.at(snapshot_id).list_snapshots()

    # Assert
    assert len(traveled_snapshots) == 1
    assert traveled_snapshots[0].id == snapshot_id


def test_time_travel_connect_at(
    catalog_url: str, storage_path: str, random_table_name: str
) -> None:
    # Arrange
    with dl.create(catalog_url, data_path=storage_path) as ducklake:
        table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
        table.sink_polars(pl.LazyFrame({"x": [1, 2, 3]}))
        snapshot_id = ducklake.get_latest_snapshot().id
        table.sink_polars(pl.LazyFrame({"x": [4, 5, 6]}))

    # Act
    with dl.connect(catalog_url, at=snapshot_id) as ducklake:
        traveled_table = ducklake.get_table(random_table_name)

        # Assert
        assert traveled_table.read_polars().height == 3
