import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ducklake as dl
import ducklake.exceptions as dlexc


def test_readonly_reads_follow_head(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    lf = pl.LazyFrame({"x": [1, 2, 3]})
    table.sink_polars(lf)

    readonly = shared_ducklake.readonly()

    # Act: commit more data through the writable handle *after* deriving the read-only view.
    table.sink_polars(lf)

    # Assert: unlike time travel, the read-only view sees the new commit (it follows head).
    assert_frame_equal(pl.concat([lf, lf]), readonly.get_table(random_table_name).scan_polars())


def test_readonly_get_latest_snapshot_works(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act & Assert: unlike time travel, this must not raise and must reflect the latest snapshot.
    assert (
        shared_ducklake.readonly().get_latest_snapshot().id
        == shared_ducklake.get_latest_snapshot().id
    )


def test_readonly_list_snapshots_returns_all(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.sink_polars(pl.LazyFrame({"x": [1, 2, 3]}))

    # Act & Assert: read-only follows head, so it lists all snapshots (not just one, as time
    # travel does).
    assert len(shared_ducklake.readonly().list_snapshots()) == len(
        shared_ducklake.list_snapshots()
    )


def test_readonly_is_idempotent(shared_ducklake: dl.Ducklake) -> None:
    # A read-only handle can itself be made read-only again without raising (the naive
    # `.at(get_latest_snapshot().id)` shim would raise here).
    assert shared_ducklake.readonly().readonly().get_latest_snapshot() is not None


def test_readonly_blocks_transaction(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    readonly = shared_ducklake.readonly()
    with pytest.raises(dlexc.ReadonlyDucklakeError):
        readonly.create_table(random_table_name, {"x": dl.Int64()})


def test_readonly_blocks_table_write(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    readonly_table = shared_ducklake.readonly().get_table(random_table_name)

    # Act & Assert
    with pytest.raises(dlexc.ReadonlyDucklakeError):
        readonly_table.sink_polars(pl.LazyFrame({"x": [1, 2, 3]}))


def test_readonly_blocks_table_metadata(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    readonly_table = shared_ducklake.readonly().get_table(random_table_name)

    # Act & Assert
    with pytest.raises(dlexc.ReadonlyDucklakeError):
        readonly_table.add_tag("foo", "bar")


def test_readonly_blocks_global_metadata(shared_ducklake: dl.Ducklake) -> None:
    with pytest.raises(dlexc.ReadonlyDucklakeError):
        shared_ducklake.readonly().set_metadata(target_file_size=1024)


def test_readonly_allows_reads(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    readonly = shared_ducklake.readonly()

    # Act & Assert: read paths must not be affected.
    assert random_table_name in [table.name[1] for table in readonly.list_tables()]


def test_connect_readonly(catalog_url: str, storage_path: str, random_table_name: str) -> None:
    # Arrange
    with dl.create(catalog_url, data_path=storage_path) as ducklake:
        table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
        table.sink_polars(pl.LazyFrame({"x": [1, 2, 3]}))

    # Act & Assert
    with dl.connect(catalog_url, readonly=True) as ducklake:
        # Reads work.
        assert ducklake.get_table(random_table_name).read_polars().height == 3
        # Writes are rejected.
        with pytest.raises(dlexc.ReadonlyDucklakeError):
            ducklake.create_table("other", {"x": dl.Int64()})
