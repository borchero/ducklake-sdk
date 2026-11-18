from collections.abc import Callable
from pathlib import Path

import pytest
import sqlalchemy as sa

import ducklake as dl
import ducklake.exceptions as dlexc
from ducklake.migrations import Migrator

CREATE_EVENTS = """
def up(tx: dl.Transaction) -> None:
    tx.create_table("events", {"x": dl.Int64()})
"""


def test_run_applies_migrations_in_order(
    ducklake: dl.Ducklake,
    migrations_dir: Path,
    write_migration: Callable[[str, str], Path],
) -> None:
    # Arrange
    write_migration(
        "2026_01_02_000000_add_column",
        """
        def up(tx: dl.Transaction) -> None:
            tx.table("events").add_column(dl.Column("y", dl.Varchar()))
        """,
    )
    write_migration("2026_01_01_000000_create_events", CREATE_EVENTS)

    # Act
    applied = Migrator(ducklake, migrations_dir).run()

    # Assert
    assert applied == ["2026_01_01_000000_create_events", "2026_01_02_000000_add_column"]
    assert [column.name for column in ducklake.table("events").schema.columns] == ["x", "y"]


def test_run_uses_one_batch_per_run(
    ducklake: dl.Ducklake,
    migrations_dir: Path,
    write_migration: Callable[[str, str], Path],
) -> None:
    # Arrange
    migrator = Migrator(ducklake, migrations_dir)
    write_migration("2026_01_01_000000_create_events", CREATE_EVENTS)
    migrator.run()
    write_migration("2026_01_02_000000_noop", "def up(tx: dl.Transaction) -> None: ...\n")

    # Act
    second = migrator.run()
    third = migrator.run()

    # Assert
    assert second == ["2026_01_02_000000_noop"]
    assert third == []
    assert [status.batch for status in migrator.status()] == [1, 2]


@pytest.mark.parametrize(
    ("name", "body"),
    [
        pytest.param("create_table", "def up(tx: dl.Transaction) -> None: ...\n", id="name"),
        pytest.param("2026_01_02_000000_broken", "def up(tx)\n", id="syntax"),
        pytest.param("2026_01_02_000000_broken", "def down(tx): ...\n", id="missing-up"),
        pytest.param("2026_01_02_000000_broken", "raise RuntimeError()\n", id="import"),
    ],
)
def test_run_invalid_migration_applies_nothing(
    ducklake: dl.Ducklake,
    migrations_dir: Path,
    write_migration: Callable[[str, str], Path],
    name: str,
    body: str,
) -> None:
    # Arrange
    write_migration("2026_01_01_000000_create_events", CREATE_EVENTS)
    write_migration(name, body)

    # Act & Assert
    with pytest.raises(dlexc.MigrationError):
        Migrator(ducklake, migrations_dir).run()
    assert not ducklake.has_table("events")
    assert ducklake._pyducklake.applied_migrations() == []


def test_run_failing_migration_stops_run(
    ducklake: dl.Ducklake,
    migrations_dir: Path,
    write_migration: Callable[[str, str], Path],
) -> None:
    # Arrange
    migrator = Migrator(ducklake, migrations_dir)
    write_migration("2026_01_01_000000_create_events", CREATE_EVENTS)
    write_migration(
        "2026_01_02_000000_fail",
        """
        def up(tx: dl.Transaction) -> None:
            tx.create_table("other", {"x": dl.Int64()})
            raise ValueError("boom")
        """,
    )
    write_migration("2026_01_03_000000_noop", "def up(tx: dl.Transaction) -> None: ...\n")

    # Act
    with pytest.raises(dlexc.MigrationError, match="2026_01_02_000000_fail") as exc_info:
        migrator.run()

    # Assert
    assert isinstance(exc_info.value.__cause__, ValueError)
    assert ducklake.has_table("events")
    assert not ducklake.has_table("other")
    assert migrator.pending() == ["2026_01_02_000000_fail", "2026_01_03_000000_noop"]


def test_status(
    ducklake: dl.Ducklake,
    migrations_dir: Path,
    write_migration: Callable[[str, str], Path],
) -> None:
    # Arrange
    migrator = Migrator(ducklake, migrations_dir)
    file = write_migration("2026_01_01_000000_create_events", CREATE_EVENTS)
    migrator.run()
    file.unlink()
    write_migration("2026_01_02_000000_noop", "def up(tx: dl.Transaction) -> None: ...\n")

    # Act
    status = migrator.status()

    # Assert
    assert [(s.name, s.batch, s.file_exists, s.pending) for s in status] == [
        ("2026_01_01_000000_create_events", 1, False, False),
        ("2026_01_02_000000_noop", None, True, True),
    ]
    assert status[0].applied_at is not None
    assert migrator.pending() == ["2026_01_02_000000_noop"]


@pytest.mark.parametrize(
    "make_readonly",
    [
        pytest.param(lambda lake: lake.readonly(), id="readonly"),
        pytest.param(lambda lake: lake.at(lake.get_latest_snapshot().id), id="time-travel"),
    ],
)
def test_run_rejects_readonly_ducklake(
    ducklake: dl.Ducklake,
    catalog_engine: sa.Engine,
    migrations_dir: Path,
    write_migration: Callable[[str, str], Path],
    make_readonly: Callable[[dl.Ducklake], dl.Ducklake],
) -> None:
    # Arrange
    write_migration("2026_01_01_000000_create_events", CREATE_EVENTS)
    migrator = Migrator(make_readonly(ducklake), migrations_dir)

    # Act & Assert
    with pytest.raises(dlexc.ReadonlyDucklakeError):
        migrator.run()
    assert not sa.inspect(catalog_engine).has_table("_ducklake_sdk_migrations")
    assert migrator.pending() == ["2026_01_01_000000_create_events"]


def test_ignored_files_are_skipped(
    ducklake: dl.Ducklake,
    migrations_dir: Path,
    write_migration: Callable[[str, str], Path],
) -> None:
    # Arrange
    write_migration("2026_01_01_000000_create_events", CREATE_EVENTS)
    write_migration("_helpers", "raise RuntimeError()\n")
    (migrations_dir / "README.md").write_text("# Migrations\n")
    (migrations_dir / "2026_01_02_000000_directory.py").mkdir()

    # Act
    applied = Migrator(ducklake, migrations_dir).run()

    # Assert
    assert applied == ["2026_01_01_000000_create_events"]
