import re
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

import ducklake as dl
from ducklake.cli import main


@pytest.mark.parametrize(
    "name",
    ["create_events_table", "CreateEventsTable", "create-events-table", "create events table"],
)
def test_migrate_new(tmp_path: Path, capsys: pytest.CaptureFixture[str], name: str) -> None:
    # Arrange
    path = tmp_path / "migrations"

    # Act
    exit_code = main(["migrate", "new", name, "--path", str(path)])

    # Assert
    assert exit_code == 0
    (file,) = path.iterdir()
    assert re.fullmatch(r"\d{4}_\d{2}_\d{2}_\d{6}_create_events_table\.py", file.name)
    assert "def up(tx: dl.Transaction) -> None:" in file.read_text()
    assert capsys.readouterr().out == f"Created migration: {file}\n"


def test_migrate_new_rejects_empty_name(
    migrations_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Act
    exit_code = main(["migrate", "new", "!!!", "--path", str(migrations_dir)])

    # Assert
    assert exit_code == 1
    assert "at least one letter or digit" in capsys.readouterr().err
    assert list(migrations_dir.iterdir()) == []


def test_migrate_new_rejects_duplicate_name(
    migrations_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Arrange
    (migrations_dir / "2026_01_01_000000_create_events.py").touch()

    # Act
    exit_code = main(["migrate", "new", "create_events", "--path", str(migrations_dir)])

    # Assert
    assert exit_code == 1
    assert "already exists" in capsys.readouterr().err
    assert len(list(migrations_dir.iterdir())) == 1


@pytest.mark.parametrize("use_env", [False, True])
def test_migrate_up_and_status(
    ducklake: dl.Ducklake,
    catalog_url: str,
    migrations_dir: Path,
    write_migration: Callable[[str, str], Path],
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    use_env: bool,
) -> None:
    # Arrange
    write_migration(
        "2026_01_01_000000_create_events",
        """
        def up(tx: dl.Transaction) -> None:
            tx.create_table("events", {"x": dl.Int64()})
        """,
    )
    args = ["--path", str(migrations_dir)]
    if use_env:
        monkeypatch.setenv("DUCKLAKE_CATALOG_URL", catalog_url)
    else:
        args += ["--catalog-url", catalog_url]

    # Act
    up_exit_code = main(["migrate", "up", *args])
    up_output = capsys.readouterr().out
    status_exit_code = main(["migrate", "status", *args])
    status_output = capsys.readouterr().out

    # Assert
    assert up_exit_code == 0
    assert up_output == "Applied 2026_01_01_000000_create_events\n"
    assert status_exit_code == 0
    assert status_output == "2026_01_01_000000_create_events  batch 1\n"
    assert ducklake.has_table("events")


def test_migrate_up_failing_migration(
    ducklake: dl.Ducklake,
    catalog_url: str,
    migrations_dir: Path,
    write_migration: Callable[[str, str], Path],
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Arrange
    write_migration(
        "2026_01_01_000000_fail",
        """
        def up(tx: dl.Transaction) -> None:
            raise ValueError("boom")
        """,
    )

    # Act
    exit_code = main(
        ["migrate", "up", "--path", str(migrations_dir), "--catalog-url", catalog_url]
    )

    # Assert
    assert exit_code == 1
    assert "migration '2026_01_01_000000_fail' failed: boom" in capsys.readouterr().err


@pytest.mark.parametrize("command", ["up", "status"])
def test_migrate_requires_catalog_url(monkeypatch: pytest.MonkeyPatch, command: str) -> None:
    # Arrange
    monkeypatch.delenv("DUCKLAKE_CATALOG_URL", raising=False)

    # Act & Assert
    with pytest.raises(SystemExit) as exc_info:
        main(["migrate", command])
    assert exc_info.value.code == 2


def test_module_entry_point() -> None:
    # Act
    result = subprocess.run(
        [sys.executable, "-m", "ducklake", "--help"], capture_output=True, text=True
    )

    # Assert
    assert result.returncode == 0
    assert "migrate" in result.stdout
