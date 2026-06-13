import datetime as dt
from pathlib import Path

import polars as pl
import pytest

import ducklake as dl

pytestmark = pytest.mark.skip_config(
    catalog="mysql", reason="The DuckDB MySQL connector is unreliable."
)


def test_merge_adjacent_files(ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.set_metadata(data_inlining_row_limit=0)
    for i in range(3):
        table.write_polars(pl.DataFrame({"x": [i]}))
    assert len(table.scan().data_files) == 3

    # Act
    result = ducklake.merge_adjacent_files(min_file_size=0)

    # Assert
    assert result == [
        {
            "table_name": dl.TableName("main", random_table_name),
            "files_processed": 3,
            "files_created": 1,
        }
    ]
    assert len(table.scan().data_files) == 1


def test_merge_adjacent_files_skipped(ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.set_metadata(data_inlining_row_limit=0)
    for i in range(3):
        table.write_polars(pl.DataFrame({"x": [i]}))

    # Act
    result = ducklake.merge_adjacent_files(min_file_size=10**9)

    # Assert
    assert result == []
    assert len(table.scan().data_files) == 3


def test_expire_snapshots_dry_run(ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.write_polars(pl.DataFrame({"x": [1]}))
    table.write_polars(pl.DataFrame({"x": [2]}))
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
    table.write_polars(pl.DataFrame({"x": [1]}))
    table.write_polars(pl.DataFrame({"x": [2]}))
    snapshots_before = len(ducklake.list_snapshots())
    latest_snapshot_before = ducklake.get_latest_snapshot().id
    assert snapshots_before > 1

    # Act
    result = ducklake.expire_snapshots(
        older_than=dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)
    )

    # Assert
    assert len(result) == snapshots_before - 1
    assert latest_snapshot_before not in [snapshot.id for snapshot in result]
    assert len(ducklake.list_snapshots()) == 1
    assert ducklake.get_latest_snapshot().id == latest_snapshot_before


def test_cleanup_old_files(ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.set_metadata(data_inlining_row_limit=0)
    for i in range(3):
        table.write_polars(pl.DataFrame({"x": [i]}))
    ducklake.merge_adjacent_files(min_file_size=0)
    ducklake.expire_snapshots(older_than=dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1))
    assert len(table.scan().data_files) == 1  # the compacted file is the only live one

    # Act
    dry_run_result = ducklake.cleanup_old_files(cleanup_all=True, dry_run=True)
    result = ducklake.cleanup_old_files(cleanup_all=True)

    # Assert
    assert dry_run_result
    assert set(result) == set(dry_run_result)
    assert table.read_arrow().num_rows == 3
    assert len(table.scan().data_files) == 1


@pytest.mark.skip_config(
    storage="s3", reason="Orphaned-file cleanup test requires direct filesystem access."
)
@pytest.mark.skip_config(
    storage="gcs", reason="Orphaned-file cleanup test requires direct filesystem access."
)
@pytest.mark.skip_config(
    storage="azure", reason="Orphaned-file cleanup test requires direct filesystem access."
)
def test_delete_orphaned_files(
    ducklake: dl.Ducklake, random_table_name: str, storage_path: str
) -> None:
    # Arrange: write a stray parquet file into the table directory that DuckLake doesn't know about
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.set_metadata(data_inlining_row_limit=0)
    table.write_polars(pl.DataFrame({"x": [1]}))
    orphan = Path(storage_path) / "main" / random_table_name / "orphan.parquet"
    orphan.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"x": [42]}).write_parquet(str(orphan))
    assert orphan.exists()

    # Act
    dry_run_result = ducklake.delete_orphaned_files(cleanup_all=True, dry_run=True)
    assert orphan.exists()
    result = ducklake.delete_orphaned_files(cleanup_all=True)

    # Assert
    assert orphan in {Path(path) for path in dry_run_result}
    assert orphan in {Path(path) for path in result}
    assert not orphan.exists()


def test_rewrite_data_files(ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.set_metadata(data_inlining_row_limit=0)
    table.write_polars(pl.DataFrame({"x": list(range(10))}))
    ducklake._duckdb_connection.execute(f"DELETE FROM {table.name} WHERE x < 9")
    files_before = table.scan().data_files
    assert len(files_before) == 1
    assert files_before[0].delete_files is not None and len(files_before[0].delete_files) == 1

    # Act
    result = ducklake.rewrite_data_files(delete_threshold=0.5)

    # Assert
    assert result == [
        {
            "table_name": dl.TableName("main", random_table_name),
            "files_processed": 1,
            "files_created": 1,
        }
    ]
    files_after = table.scan().data_files
    assert len(files_after) == 1
    assert files_after[0].path != files_before[0].path
    assert not files_after[0].delete_files
    assert pl.read_parquet(
        files_after[0].path, storage_options=ducklake._storage_options.to_dict()
    )["x"].to_list() == [9]


def test_checkpoint(ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.set_metadata(data_inlining_row_limit=0)
    for i in range(3):
        table.write_polars(pl.DataFrame({"x": [i]}))
    assert len(table.scan().data_files) == 3

    # Act
    ducklake.checkpoint()

    # Assert: files were merged and data is intact
    assert len(table.scan().data_files) == 1
    assert table.read_arrow().num_rows == 3


def test_scan_after_expire_with_orphan_schema_versions(
    ducklake: dl.Ducklake,
    catalog_url: str,
    random_table_name: str,
) -> None:
    # Arrange
    table = ducklake.create_table(random_table_name, {"a": dl.Int64()})
    table.write_polars(pl.DataFrame({"a": [1]}))
    table.add_column(dl.Column("b", dl.Varchar()))
    table.write_polars(pl.DataFrame({"a": [2], "b": "two"}))
    ducklake.expire_snapshots(older_than=dt.datetime.now(dt.timezone.utc))

    # Act: re-connect on same catalog to go around the cache
    with dl.connect(catalog_url) as reader:
        result = reader.get_table(random_table_name).scan()

    # Assert
    assert len(result.inline_data) == 2
    row_counts = sorted(pl.DataFrame(arr).height for arr in result.inline_data)
    assert row_counts == [1, 1]
