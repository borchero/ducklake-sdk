from collections.abc import Iterator
from pathlib import Path

import polars as pl
import pytest
import sqlalchemy as sa
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


@pytest.fixture()
def berlin_transfer_target(catalog: str, storage: str, tmp_path: Path) -> Iterator[dl.Ducklake]:
    with (
        make_catalog_url(catalog, tmp_path) as catalog_url,
        make_storage_path(storage, tmp_path) as storage_path,
        dl.create(catalog_url, data_path=storage_path, time_zone="Europe/Berlin") as ducklake,
    ):
        yield ducklake


@pytest.fixture()
def source_tables(ducklake: dl.Ducklake) -> tuple[dl.Table, dl.Table]:
    first = ducklake.create_table("first", {"x": dl.Int64()})
    second = ducklake.create_table("second", {"y": dl.Varchar()})
    return first, second


def read_partition_values(ducklake: dl.Ducklake) -> list[tuple[int, str | None]]:
    url = ducklake._connection_args.render_as_string(hide_password=False)
    engine = sa.create_engine(url)
    try:
        with engine.connect() as connection:
            rows = connection.execute(
                sa.text(
                    "SELECT partition_key_index, partition_value "
                    "FROM ducklake_file_partition_value "
                    "ORDER BY data_file_id, partition_key_index"
                )
            )
            return [(row.partition_key_index, row.partition_value) for row in rows]
    finally:
        engine.dispose()


def read_active_file_partition_ids(ducklake: dl.Ducklake) -> list[int | None]:
    url = ducklake._connection_args.render_as_string(hide_password=False)
    engine = sa.create_engine(url)
    try:
        with engine.connect() as connection:
            rows = connection.execute(
                sa.text(
                    "SELECT partition_id FROM ducklake_data_file "
                    "WHERE end_snapshot IS NULL ORDER BY data_file_id"
                )
            )
            return [row.partition_id for row in rows]
    finally:
        engine.dispose()


def add_inline_deletes(ducklake: dl.Ducklake, table_name: str, row_ids: list[int]) -> None:
    url = ducklake._connection_args.render_as_string(hide_password=False)
    engine = sa.create_engine(url)
    try:
        with engine.begin() as connection:
            table_id = connection.scalar(
                sa.text(
                    "SELECT table_id FROM ducklake_table "
                    "WHERE table_name = :table_name AND end_snapshot IS NULL"
                ),
                {"table_name": table_name},
            )
            assert isinstance(table_id, int)
            file_id = connection.scalar(
                sa.text(
                    "SELECT data_file_id FROM ducklake_data_file "
                    "WHERE table_id = :table_id AND end_snapshot IS NULL"
                ),
                {"table_id": table_id},
            )
            assert isinstance(file_id, int)
            snapshot_id = connection.scalar(
                sa.text("SELECT MAX(snapshot_id) FROM ducklake_snapshot")
            )
            assert isinstance(snapshot_id, int)
            connection.execute(
                sa.text(
                    f"CREATE TABLE ducklake_inlined_delete_{table_id} ("
                    "file_id BIGINT, row_id BIGINT, begin_snapshot BIGINT)"
                )
            )
            connection.execute(
                sa.text(
                    f"INSERT INTO ducklake_inlined_delete_{table_id} "
                    "(file_id, row_id, begin_snapshot) "
                    "VALUES (:file_id, :row_id, :snapshot_id)"
                ),
                [
                    {"file_id": file_id, "row_id": row_id, "snapshot_id": snapshot_id}
                    for row_id in row_ids
                ],
            )
    finally:
        engine.dispose()


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
    source_tables: tuple[dl.Table, dl.Table],
    transfer_target: dl.Ducklake,
    operation: str,
) -> None:
    # Arrange
    first, second = source_tables
    first.write_polars(pl.DataFrame({"x": [1, 2, 3]}))
    second.write_polars(pl.DataFrame({"y": ["a", "b"]}))

    # Act
    transferred = getattr(transfer_target, operation)([first, second])

    # Assert
    assert [table.name for table in transferred] == [("main", "first"), ("main", "second")]
    assert_frame_equal(transferred[0].read_polars(), pl.DataFrame({"x": [1, 2, 3]}))
    assert_frame_equal(transferred[1].read_polars(), pl.DataFrame({"y": ["a", "b"]}))


def test_transfer_multiple_tables_with_names(
    source_tables: tuple[dl.Table, dl.Table], transfer_target: dl.Ducklake
) -> None:
    # Arrange
    first, second = source_tables

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
    # Arrange
    sources: list[dl.Table] = []

    # Act
    transferred = transfer_target.copy_tables_from(sources)

    # Assert
    assert transferred == []


def test_transfer_empty_sources_with_names_rejected(transfer_target: dl.Ducklake) -> None:
    # Arrange
    sources: list[dl.Table] = []

    # Act
    with pytest.raises(ValueError) as error:
        transfer_target.copy_tables_from(sources, ["unexpected"])

    # Assert
    assert "expected 0 target name(s)" in str(error.value)


def test_transfer_mixed_sources_rejected(
    ducklake: dl.Ducklake, transfer_target: dl.Ducklake, tmp_path: Path
) -> None:
    # Arrange
    first = ducklake.create_table("first", {"x": dl.Int64()})
    other_path = tmp_path / "other"
    other_path.mkdir()
    with (
        make_catalog_url("sqlite", other_path) as catalog_url,
        make_storage_path("local", other_path) as storage_path,
        dl.create(catalog_url, data_path=storage_path) as other_source,
    ):
        second = other_source.create_table("second", {"y": dl.Varchar()})

        # Act
        with pytest.raises(ValueError) as error:
            transfer_target.copy_tables_from([first, second])

        # Assert
        assert "same DuckLake" in str(error.value)


def test_transfer_duplicate_target_names_rejected(
    source_tables: tuple[dl.Table, dl.Table], transfer_target: dl.Ducklake
) -> None:
    # Arrange
    first, second = source_tables

    # Act
    with pytest.raises(ValueError) as error:
        transfer_target.copy_tables_from([first, second], ["clash", "clash"])

    # Assert
    assert "same name" in str(error.value)


def test_transfer_name_count_mismatch_rejected(
    source_tables: tuple[dl.Table, dl.Table], transfer_target: dl.Ducklake
) -> None:
    # Arrange
    first, second = source_tables

    # Act
    with pytest.raises(ValueError) as error:
        transfer_target.copy_tables_from([first, second], ["only_one"])

    # Assert
    assert "target name" in str(error.value)


def test_transfer_existing_target_rejected(
    ducklake: dl.Ducklake, transfer_target: dl.Ducklake
) -> None:
    # Arrange
    source = ducklake.create_table("source", {"x": dl.Int64()})
    transfer_target.create_table("source", {"x": dl.Int64()})

    # Act
    with pytest.raises(dlexc.AlreadyExistsError) as error:
        transfer_target.copy_tables_from([source])

    # Assert
    assert "already exists" in str(error.value)


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


@pytest.mark.parametrize("operation", ["copy_to", "move_to"])
def test_transfer_preserves_inline_deletes(
    ducklake: dl.Ducklake,
    transfer_target: dl.Ducklake,
    random_table_name: str,
    operation: str,
) -> None:
    # Arrange
    source = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    values = list(range(source.metadata["data_inlining_row_limit"] + 1))
    source.sink_polars(pl.LazyFrame({"x": values}))
    deleted_row_ids = [1, 3]
    add_inline_deletes(ducklake, random_table_name, deleted_row_ids)
    existing = transfer_target.create_table("existing", {"x": dl.Int64()})
    existing.sink_polars(pl.LazyFrame({"x": values}))
    expected = pl.DataFrame({"x": [value for value in values if value not in deleted_row_ids]})

    # Act
    transferred = getattr(source, operation)(transfer_target)

    # Assert
    inline_deletes = transferred.scan().data_files[0].inline_deletes
    assert inline_deletes is not None
    assert pl.Series(inline_deletes).to_list() == deleted_row_ids
    assert_frame_equal(transferred.read_polars(), expected)


@pytest.mark.parametrize("operation", ["copy_to", "move_to"])
def test_transfer_preserves_partition_values(
    ducklake: dl.Ducklake,
    transfer_target: dl.Ducklake,
    random_table_name: str,
    operation: str,
) -> None:
    # Arrange
    source = ducklake.create_table(
        random_table_name,
        {"x": dl.Int64(), "region": dl.Varchar()},
        partition_by="region",
    )
    source.sink_polars(pl.LazyFrame({"x": [1, 2], "region": ["east", "west"]}))
    expected = read_partition_values(ducklake)

    # Act
    getattr(source, operation)(transfer_target)

    # Assert
    assert sorted(expected) == [(0, "east"), (0, "west")]
    assert sorted(read_partition_values(transfer_target)) == sorted(expected)


@pytest.mark.parametrize("operation", ["copy_to", "move_to"])
def test_transfer_does_not_apply_historical_partition_values_to_current_spec(
    ducklake: dl.Ducklake,
    transfer_target: dl.Ducklake,
    random_table_name: str,
    operation: str,
) -> None:
    # Arrange
    source = ducklake.create_table(
        random_table_name,
        {"x": dl.Int64(), "y": dl.Varchar()},
        partition_by="x",
    )
    source.sink_polars(pl.LazyFrame({"x": [1], "y": ["old"]}))
    source.update_partitioning(dl.Partitioning("y"))
    source.sink_polars(pl.LazyFrame({"x": [2], "y": ["new"]}))

    # Act
    transferred = getattr(source, operation)(transfer_target)
    old_rows = transferred.scan_polars().filter(pl.col("y") == "old").collect()

    # Assert
    assert read_active_file_partition_ids(transfer_target).count(None) == 1
    assert read_partition_values(transfer_target) == [(0, "new")]
    assert_frame_equal(old_rows, pl.DataFrame({"x": [1], "y": ["old"]}))


@pytest.mark.parametrize("operation", ["copy_to", "move_to"])
def test_single_table_transfer_uses_target_time_zone(
    ducklake: dl.Ducklake,
    berlin_transfer_target: dl.Ducklake,
    random_table_name: str,
    operation: str,
) -> None:
    # Arrange
    source = ducklake.create_table(random_table_name, {"x": dl.TimestampTz()})

    # Act
    transferred = getattr(source, operation)(berlin_transfer_target)

    # Assert
    assert transferred._time_zone == "Europe/Berlin"


@pytest.mark.parametrize("operation", ["copy_to", "move_to"])
def test_transfer_to_same_connection_rejected_without_changes(
    ducklake: dl.Ducklake, random_table_name: str, operation: str
) -> None:
    # Arrange
    source = ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    with pytest.raises(ValueError) as error:
        getattr(source, operation)(ducklake, "renamed")

    # Assert
    assert "must be different" in str(error.value)
    assert ducklake.has_table(random_table_name)
    assert not ducklake.has_table("renamed")


def test_move_to_second_connection_for_same_catalog_rejected_without_changes(
    ducklake: dl.Ducklake,
    catalog_url: str,
    random_table_name: str,
) -> None:
    # Arrange
    source = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    with dl.connect(catalog_url) as second_connection:
        # Act
        with pytest.raises(ValueError) as error:
            source.move_to(second_connection, "renamed")

        # Assert
        assert "must be different" in str(error.value)
        assert ducklake.has_table(random_table_name)
        assert not ducklake.has_table("renamed")


def test_move_rejects_duplicate_source_without_changes(
    ducklake: dl.Ducklake, transfer_target: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    source = ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    with pytest.raises(ValueError) as error:
        transfer_target.move_tables_from([source, source], ["first", "second"])

    # Assert
    assert "more than once" in str(error.value)
    assert ducklake.has_table(random_table_name)
    assert not transfer_target.has_table("first")
    assert not transfer_target.has_table("second")
