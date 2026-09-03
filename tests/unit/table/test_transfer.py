from collections.abc import Iterator
from pathlib import Path

import polars as pl
import pytest
from _testutils import make_catalog_url, make_storage_path
from polars.testing import assert_frame_equal

import ducklake as dl
import ducklake.exceptions as dlexc

pytestmark = pytest.mark.skip_config(
    catalog="mysql", reason="The DuckDB MySQL connector is unreliable."
)


@pytest.fixture()
def transfer_target(catalog: str, storage: str, tmp_path: Path) -> Iterator[dl.Ducklake]:
    with (
        make_catalog_url(catalog, tmp_path) as catalog_url,
        make_storage_path(storage, tmp_path) as storage_path,
        dl.create(catalog_url, data_path=storage_path) as ducklake,
    ):
        yield ducklake


def test_copy_table_to_another_ducklake(
    ducklake: dl.Ducklake, transfer_target: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    source = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    transfer_target.create_table("existing", {"other": dl.Int64()})
    data = pl.LazyFrame({"x": range(source.metadata["data_inlining_row_limit"] + 1)})
    source.sink_polars(data)
    source_paths = {file.path for file in source.scan().data_files}

    # Act
    copied = source.copy_to(transfer_target)

    # Assert
    target_paths = {file.path for file in copied.scan().data_files}
    assert source_paths.isdisjoint(target_paths)
    assert_frame_equal(copied.read_polars(), data.collect())


def test_move_transfers_file_ownership(
    ducklake: dl.Ducklake, transfer_target: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    source = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    data = pl.LazyFrame({"x": range(source.metadata["data_inlining_row_limit"] + 1)})
    source.sink_polars(data)
    source_paths = {file.path for file in source.scan().data_files}

    # Act
    moved = source.move_to(transfer_target)
    ducklake.expire_snapshots(versions=[snapshot.id for snapshot in ducklake.list_snapshots()])
    deleted_paths = ducklake.cleanup_old_files(cleanup_all=True)

    # Assert
    assert not ducklake.has_table(random_table_name)
    assert {file.path for file in moved.scan().data_files} == source_paths
    assert deleted_paths == []
    assert_frame_equal(moved.read_polars(), data.collect())


@pytest.mark.parametrize("operation", ["copy_tables_from", "move_tables_from"])
def test_transfer_multiple_tables(
    ducklake: dl.Ducklake, transfer_target: dl.Ducklake, operation: str
) -> None:
    # Arrange
    first = ducklake.create_table("first", {"x": dl.Int64()})
    second = ducklake.create_table("second", {"y": dl.Varchar()})
    first.write_polars(pl.DataFrame({"x": [1, 2, 3]}))
    second.write_polars(pl.DataFrame({"y": ["a", "b"]}))

    # Act
    transferred = getattr(transfer_target, operation)([first, second])

    # Assert
    assert [table.name for table in transferred] == [("main", "first"), ("main", "second")]
    assert_frame_equal(transferred[0].read_polars(), pl.DataFrame({"x": [1, 2, 3]}))
    assert_frame_equal(transferred[1].read_polars(), pl.DataFrame({"y": ["a", "b"]}))


def test_transfer_multiple_tables_with_names(
    ducklake: dl.Ducklake, transfer_target: dl.Ducklake
) -> None:
    # Arrange
    first = ducklake.create_table("first", {"x": dl.Int64()})
    second = ducklake.create_table("second", {"y": dl.Varchar()})

    # Act
    transferred = transfer_target.copy_tables_from(
        [first, second], ["renamed_first", "renamed_second"]
    )

    # Assert
    assert [table.name for table in transferred] == [
        ("main", "renamed_first"),
        ("main", "renamed_second"),
    ]


def test_transfer_empty_sources(transfer_target: dl.Ducklake) -> None:
    # Act
    transferred = transfer_target.copy_tables_from([])

    # Assert
    assert transferred == []


def test_transfer_mixed_sources_rejected(
    ducklake: dl.Ducklake, transfer_target: dl.Ducklake, tmp_path: Path
) -> None:
    # Arrange
    first = ducklake.create_table("first", {"x": dl.Int64()})
    with (
        make_catalog_url("sqlite", tmp_path / "other") as catalog_url,
        make_storage_path("local", tmp_path / "other") as storage_path,
        dl.create(catalog_url, data_path=storage_path) as other_source,
    ):
        second = other_source.create_table("second", {"y": dl.Varchar()})

        # Act / Assert
        with pytest.raises(ValueError, match="same DuckLake"):
            transfer_target.copy_tables_from([first, second])


def test_transfer_duplicate_target_names_rejected(
    ducklake: dl.Ducklake, transfer_target: dl.Ducklake
) -> None:
    # Arrange
    first = ducklake.create_table("first", {"x": dl.Int64()})
    second = ducklake.create_table("second", {"y": dl.Varchar()})

    # Act / Assert
    with pytest.raises(ValueError, match="same name"):
        transfer_target.copy_tables_from([first, second], ["clash", "clash"])


def test_transfer_name_count_mismatch_rejected(
    ducklake: dl.Ducklake, transfer_target: dl.Ducklake
) -> None:
    # Arrange
    first = ducklake.create_table("first", {"x": dl.Int64()})
    second = ducklake.create_table("second", {"y": dl.Varchar()})

    # Act / Assert
    with pytest.raises(ValueError, match="target name"):
        transfer_target.copy_tables_from([first, second], ["only_one"])


def test_transfer_existing_target_rejected(
    ducklake: dl.Ducklake, transfer_target: dl.Ducklake
) -> None:
    # Arrange
    source = ducklake.create_table("source", {"x": dl.Int64()})
    transfer_target.create_table("source", {"x": dl.Int64()})

    # Act / Assert
    with pytest.raises(dlexc.AlreadyExistsError):
        transfer_target.copy_tables_from([source])


@pytest.mark.parametrize("operation", ["copy_to", "move_to"])
def test_transfer_preserves_delete_files(
    ducklake: dl.Ducklake,
    transfer_target: dl.Ducklake,
    random_table_name: str,
    operation: str,
) -> None:
    # Arrange
    source = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    source.set_metadata(data_inlining_row_limit=0)
    source.write_polars(pl.DataFrame({"x": list(range(10))}))
    ducklake._duckdb_connection.execute(f"DELETE FROM {source.name} WHERE x < 9")
    assert len(source.scan().data_files[0].delete_files) == 1

    # Act
    transferred = getattr(source, operation)(transfer_target)

    # Assert
    assert len(transferred.scan().data_files[0].delete_files) == 1
    assert_frame_equal(transferred.read_polars(), pl.DataFrame({"x": [9]}))
