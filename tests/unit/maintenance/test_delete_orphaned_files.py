import datetime as dt
from pathlib import Path

import polars as pl
import pytest

import ducklake as dl

# Creating an orphaned file requires writing directly into the table directory, which the test
# only does for local storage.
pytestmark = [
    pytest.mark.skip_config(catalog="mysql", reason="The DuckDB MySQL connector is unreliable."),
    pytest.mark.skip_config(
        storage="s3", reason="Orphaned-file cleanup test requires direct filesystem access."
    ),
    pytest.mark.skip_config(
        storage="gcs", reason="Orphaned-file cleanup test requires direct filesystem access."
    ),
    pytest.mark.skip_config(
        storage="azure", reason="Orphaned-file cleanup test requires direct filesystem access."
    ),
]


@pytest.fixture()
def orphan_file(ducklake: dl.Ducklake, random_table_name: str, storage_path: str) -> Path:
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.set_metadata(data_inlining_row_limit=0)
    table.write_polars(pl.DataFrame({"x": [1]}))
    orphan = Path(storage_path) / "main" / random_table_name / "orphan.parquet"
    orphan.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"x": [42]}).write_parquet(str(orphan))
    return orphan


def test_delete_orphaned_files(ducklake: dl.Ducklake, orphan_file: Path) -> None:
    # Arrange
    live_files = {Path(file.path) for file in ducklake.list_tables()[0].scan().data_files}

    # Act
    dry_run_result = ducklake.delete_orphaned_files(cleanup_all=True, dry_run=True)
    result = ducklake.delete_orphaned_files(cleanup_all=True)

    # Assert
    assert orphan_file in {Path(path) for path in dry_run_result}
    assert orphan_file in {Path(path) for path in result}
    assert not orphan_file.exists()
    assert live_files.isdisjoint({Path(path) for path in result})


def test_delete_orphaned_files_dry_run_keeps_files(
    ducklake: dl.Ducklake, orphan_file: Path
) -> None:
    # Act
    result = ducklake.delete_orphaned_files(cleanup_all=True, dry_run=True)

    # Assert
    assert orphan_file in {Path(path) for path in result}
    assert orphan_file.exists()


def test_delete_orphaned_files_respects_older_than(
    ducklake: dl.Ducklake, orphan_file: Path
) -> None:
    # Act: only files modified before yesterday are eligible, so the fresh orphan is retained
    result = ducklake.delete_orphaned_files(
        older_than=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)
    )

    # Assert
    assert result == []
    assert orphan_file.exists()
