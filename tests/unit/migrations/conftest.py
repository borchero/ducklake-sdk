import textwrap
from collections.abc import Callable
from pathlib import Path

import pytest


@pytest.fixture()
def migrations_dir(tmp_path: Path) -> Path:
    path = tmp_path / "migrations"
    path.mkdir()
    return path


@pytest.fixture()
def write_migration(migrations_dir: Path) -> Callable[[str, str], Path]:
    def write(name: str, body: str) -> Path:
        file = migrations_dir / f"{name}.py"
        file.write_text("import ducklake as dl\n\n\n" + textwrap.dedent(body))
        return file

    return write
