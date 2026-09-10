from collections.abc import Iterator
from pathlib import Path
from typing import cast

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
def transfer_target_url(catalog: str, tmp_path: Path) -> Iterator[str]:
    with make_catalog_url(catalog, tmp_path) as catalog_url:
        yield catalog_url


@pytest.fixture()
def transfer_target(
    transfer_target_url: str, storage: str, tmp_path: Path
) -> Iterator[dl.Ducklake]:
    with (
        make_storage_path(storage, tmp_path) as storage_path,
        dl.create(transfer_target_url, data_path=storage_path) as ducklake,
    ):
        yield ducklake


@pytest.fixture()
def source_tables(ducklake: dl.Ducklake) -> tuple[dl.Table, dl.Table]:
    first = ducklake.create_table("first", {"x": dl.Int64()})
    second = ducklake.create_table("second", {"y": dl.Varchar()})
    first.write_polars(pl.DataFrame({"x": [1, 2, 3]}))
    second.write_polars(pl.DataFrame({"y": ["a", "b"]}))
    return first, second


@pytest.mark.parametrize(
    ("operation", "source_remains", "files_are_copied"),
    [("copy_tables", True, True), ("move_tables", False, False)],
)
@pytest.mark.parametrize("rename", [False, True])
def test_transfer_table(
    ducklake: dl.Ducklake,
    transfer_target: dl.Ducklake,
    random_table_name: str,
    operation: str,
    source_remains: bool,
    files_are_copied: bool,
    rename: bool,
) -> None:
    # Arrange
    source = ducklake.create_table(
        random_table_name,
        {"x": dl.Int64(), "region": dl.Varchar()},
        partition_by="region",
    )
    source.set_metadata(data_inlining_row_limit=0)
    data = pl.DataFrame({"x": range(12), "region": ["east", "west"] * 6})
    source.write_polars(data)
    ducklake._duckdb_connection.execute(f"DELETE FROM {source.name} WHERE x < 2")
    source_paths = {file.path for file in source.scan().data_files}
    tables = {"renamed": source} if rename else [source]

    # Act
    transferred = getattr(ducklake, operation)(tables, transfer_target)[0]
    deleted_paths = None
    if not source_remains:
        snapshots = [snapshot.id for snapshot in ducklake.list_snapshots()]
        ducklake.expire_snapshots(versions=snapshots)
        deleted_paths = ducklake.cleanup_old_files(cleanup_all=True)

    # Assert
    target_scan = transferred.scan()
    target_paths = {file.path for file in target_scan.data_files}
    assert ducklake.has_table(random_table_name) is source_remains
    assert deleted_paths == ([] if not source_remains else None)
    assert source_paths.isdisjoint(target_paths) is files_are_copied
    assert any(file.delete_files for file in target_scan.data_files)
    assert transferred.name == ("main", "renamed" if rename else random_table_name)
    assert_frame_equal(transferred.read_polars().sort("x"), data.filter(pl.col("x") >= 2))


@pytest.mark.parametrize("relative_path", [False, True])
def test_copy_to_local_data_path(
    ducklake: dl.Ducklake,
    transfer_target_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    relative_path: bool,
) -> None:
    # Arrange
    source = ducklake.create_table("source", {"x": dl.Int64()})
    source.set_metadata(data_inlining_row_limit=0)
    source.write_polars(pl.DataFrame({"x": [1, 2, 3]}))
    ducklake._duckdb_connection.execute(f"DELETE FROM {source.name} WHERE x = 1")
    monkeypatch.chdir(tmp_path)
    data_path = "target_data" if relative_path else str(tmp_path / "target_data")
    with dl.create(transfer_target_url, data_path=data_path) as target:
        # Act
        copied = ducklake.copy_tables([source], target)[0]

        # Assert
        assert_frame_equal(copied.read_polars().sort("x"), pl.DataFrame({"x": [2, 3]}))
        assert any(file.delete_files for file in copied.scan().data_files)
        assert len(list((tmp_path / "target_data").rglob("*.parquet"))) == 2


@pytest.mark.parametrize(
    ("operation", "source_remains"),
    [("copy_tables", True), ("move_tables", False)],
)
def test_transfer_multiple_tables(
    ducklake: dl.Ducklake,
    source_tables: tuple[dl.Table, dl.Table],
    transfer_target: dl.Ducklake,
    operation: str,
    source_remains: bool,
) -> None:
    # Arrange
    first, second = source_tables

    # Act
    transferred = getattr(ducklake, operation)(
        {("target_schema", "renamed_first"): first, "renamed_second": second}, transfer_target
    )

    # Assert
    assert [table.name for table in transferred] == [
        ("target_schema", "renamed_first"),
        ("main", "renamed_second"),
    ]
    assert ducklake.has_table("first") is source_remains
    assert ducklake.has_table("second") is source_remains
    assert_frame_equal(transferred[0].read_polars(), pl.DataFrame({"x": [1, 2, 3]}))
    assert_frame_equal(transferred[1].read_polars(), pl.DataFrame({"y": ["a", "b"]}))


def test_transfer_is_atomic_when_target_exists(
    ducklake: dl.Ducklake,
    source_tables: tuple[dl.Table, dl.Table],
    transfer_target: dl.Ducklake,
) -> None:
    # Arrange
    first, second = source_tables
    transfer_target.create_table("existing", {"x": dl.Int64()})

    # Act
    with pytest.raises(dlexc.AlreadyExistsError):
        ducklake.copy_tables({"new": first, "existing": second}, transfer_target)

    # Assert
    assert not transfer_target.has_table("new")
    assert transfer_target.has_table("existing")


def test_transfer_rejects_tables_from_different_catalogs(
    ducklake: dl.Ducklake, transfer_target: dl.Ducklake, tmp_path: Path
) -> None:
    # Arrange
    first = ducklake.create_table("first", {"x": dl.Int64()})
    other_path = tmp_path / "other"
    other_path.mkdir()
    with (
        make_catalog_url("sqlite", other_path) as catalog_url,
        make_storage_path("local", other_path) as storage_path,
        dl.create(catalog_url, data_path=storage_path) as other,
    ):
        second = other.create_table("second", {"y": dl.Varchar()})

        # Act
        with pytest.raises(ValueError, match="source DuckLake"):
            ducklake.copy_tables([first, second], transfer_target)

        # Assert
        assert not transfer_target.has_table("first")
        assert not transfer_target.has_table("second")


@pytest.mark.parametrize("operation", ["copy_tables", "move_tables"])
def test_transfer_rejects_same_catalog(
    ducklake: dl.Ducklake, catalog_url: str, random_table_name: str, operation: str
) -> None:
    # Arrange
    source = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    second_connection = dl.connect(catalog_url)

    # Act
    with second_connection, pytest.raises(ValueError, match="must be different"):
        getattr(ducklake, operation)({"renamed": source}, second_connection)

    # Assert
    assert ducklake.has_table(random_table_name)
    assert not ducklake.has_table("renamed")


# --------------------------------------- SCHEMA EVOLUTION -------------------------------------- #


@pytest.fixture(params=[False, True], ids=["flat", "nested"])
def evolved_source(
    ducklake: dl.Ducklake, request: pytest.FixtureRequest
) -> tuple[dl.Table, pl.DataFrame, int]:
    payload_type = dl.Struct({"values": dl.List(dl.Int64())}) if request.param else dl.Int64()
    payload = [{"values": [10, 20]}, {"values": [30]}] if request.param else [10, 20]
    source = ducklake.create_table(
        "evolved",
        {"discarded": dl.Int64(), "key": dl.Int64(), "payload": payload_type},
    )
    source.set_metadata(data_inlining_row_limit=0)
    original = pl.DataFrame({"discarded": [100, 200], "key": [1, 2], "payload": payload})
    source.write_polars(original)
    source.remove_column("discarded")
    source.rename_column("key", "renamed")
    source.add_column(dl.Column("added", dl.Varchar(), initial_default="old"))
    source.add_column(dl.Column("retired", dl.Int64()))
    retired_id = source.schema.columns[-1].field_id
    assert retired_id is not None
    later = (
        original.drop("discarded")
        .rename({"key": "renamed"})
        .with_columns(
            pl.col("renamed") + 2,
            pl.lit("new").alias("added"),
            pl.lit(999, dtype=pl.Int64).alias("retired"),
        )
    )
    source.write_polars(later)
    source.remove_column("retired")
    expected = pl.concat(
        [
            original.drop("discarded")
            .rename({"key": "renamed"})
            .with_columns(pl.lit("old").alias("added")),
            later.drop("retired"),
        ]
    )
    return source, expected, retired_id


@pytest.mark.parametrize("operation", ["copy_tables", "move_tables"])
def test_transfer_preserves_evolved_column_ids(
    ducklake: dl.Ducklake,
    evolved_source: tuple[dl.Table, pl.DataFrame, int],
    transfer_target: dl.Ducklake,
    operation: str,
) -> None:
    # Arrange
    source, expected, _ = evolved_source
    source_columns = source.schema.columns
    source_stats = [
        {key: vars(stats) for key, stats in file.statistics.column_stats.items()}
        for file in source.scan().data_files
    ]

    # Act
    transferred = getattr(ducklake, operation)([source], transfer_target)[0]

    # Assert
    assert transferred.schema.columns == source_columns
    assert [
        {key: vars(stats) for key, stats in file.statistics.column_stats.items()}
        for file in transferred.scan().data_files
    ] == source_stats
    assert_frame_equal(transferred.read_polars().sort("renamed"), expected)
    # read_arrow uses DuckDB, providing an independent check of the file-to-column mapping.
    duckdb_data = cast(pl.DataFrame, pl.from_arrow(transferred.read_arrow()))
    assert_frame_equal(duckdb_data.sort("renamed"), expected)


@pytest.mark.parametrize("operation", ["copy_tables", "move_tables"])
@pytest.mark.parametrize("reconnect", [False, True])
def test_transfer_reserves_dropped_column_ids(
    ducklake: dl.Ducklake,
    evolved_source: tuple[dl.Table, pl.DataFrame, int],
    transfer_target: dl.Ducklake,
    transfer_target_url: str,
    operation: str,
    reconnect: bool,
) -> None:
    # Arrange
    source, expected, retired_id = evolved_source
    transferred = getattr(ducklake, operation)([source], transfer_target)[0]
    expected = expected.with_columns(pl.lit(123, dtype=pl.Int64).alias("fresh"))

    with dl.connect(transfer_target_url) as reopened:
        table = reopened.table("evolved") if reconnect else transferred

        # Act
        table.add_column(dl.Column("fresh", dl.Int64(), initial_default=123))

        # Assert
        assert table.schema.columns[-1].field_id == retired_id + 1
        assert_frame_equal(table.read_polars().sort("renamed"), expected)
