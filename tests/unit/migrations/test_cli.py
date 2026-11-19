import re
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest
from typer.testing import CliRunner

import ducklake as dl
from ducklake.cli import app


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.mark.parametrize(
    "name",
    ["create_events_table", "CreateEventsTable", "create-events-table", "create events table"],
)
def test_migrate_new(runner: CliRunner, tmp_path: Path, name: str) -> None:
    # Arrange
    path = tmp_path / "migrations"

    # Act
    result = runner.invoke(app, ["migrate", "new", name, "--path", str(path)])

    # Assert
    assert result.exit_code == 0
    (file,) = path.iterdir()
    assert re.fullmatch(r"\d{4}_\d{2}_\d{2}_\d{6}_create_events_table\.py", file.name)
    assert "def up(tx: dl.Transaction) -> None:" in file.read_text()
    assert result.stdout == f"Created migration: {file}\n"


@pytest.mark.parametrize(
    ("name", "existing", "message"),
    [
        pytest.param("!!!", [], "at least one letter or digit", id="empty"),
        pytest.param(
            "create_events",
            ["2026_01_01_000000_create_events.py"],
            "already exists",
            id="duplicate",
        ),
    ],
)
def test_migrate_new_rejects_invalid_name(
    runner: CliRunner,
    migrations_dir: Path,
    name: str,
    existing: list[str],
    message: str,
) -> None:
    # Arrange
    for file in existing:
        (migrations_dir / file).touch()

    # Act
    result = runner.invoke(app, ["migrate", "new", name, "--path", str(migrations_dir)])

    # Assert
    assert result.exit_code == 1
    assert message in result.stderr
    assert sorted(file.name for file in migrations_dir.iterdir()) == existing


@pytest.mark.parametrize("use_env", [False, True])
def test_migrate_up_and_status(
    runner: CliRunner,
    ducklake: dl.Ducklake,
    catalog_url: str,
    migrations_dir: Path,
    write_migration: Callable[[str, str], Path],
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
    env = {}
    if use_env:
        env["DUCKLAKE_CATALOG_URL"] = catalog_url
    else:
        args += ["--catalog-url", catalog_url]

    # Act
    up = runner.invoke(app, ["migrate", "up", *args], env=env)
    status = runner.invoke(app, ["migrate", "status", *args], env=env)

    # Assert
    assert up.exit_code == 0
    assert up.stdout == "Applied 2026_01_01_000000_create_events\n"
    assert status.exit_code == 0
    assert status.stdout == "2026_01_01_000000_create_events  batch 1\n"
    assert ducklake.has_table("events")


def test_migrate_up_failing_migration(
    runner: CliRunner,
    ducklake: dl.Ducklake,
    catalog_url: str,
    migrations_dir: Path,
    write_migration: Callable[[str, str], Path],
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
    result = runner.invoke(
        app, ["migrate", "up", "--path", str(migrations_dir), "--catalog-url", catalog_url]
    )

    # Assert
    assert result.exit_code == 1
    assert "migration '2026_01_01_000000_fail' failed: boom" in result.stderr


@pytest.mark.parametrize("command", ["up", "status"])
def test_migrate_requires_catalog_url(runner: CliRunner, command: str) -> None:
    # Act
    result = runner.invoke(app, ["migrate", command], env={"DUCKLAKE_CATALOG_URL": None})

    # Assert
    assert result.exit_code == 2


def test_module_entry_point() -> None:
    # Act
    result = subprocess.run(
        [sys.executable, "-m", "ducklake", "--help"], capture_output=True, text=True
    )

    # Assert
    assert result.returncode == 0
    assert "migrate" in result.stdout
