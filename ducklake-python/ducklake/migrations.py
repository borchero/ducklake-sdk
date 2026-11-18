from __future__ import annotations

import importlib.util
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from .exceptions import MigrationError, ReadonlyDucklakeError

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Callable

    from .ducklake import Ducklake
    from .transaction import Transaction


@dataclass(frozen=True)
class MigrationStatus:
    """The status of a single migration."""

    #: The name of the migration, i.e., the name of its file without the extension.
    name: str
    #: The batch in which the migration was applied, or `None` if it is still pending.
    batch: int | None
    #: The time at which the migration was applied, or `None` if it is still pending.
    applied_at: dt.datetime | None
    #: Whether the migration file exists. This is only `False` for migrations that were applied
    #: but whose file has been deleted since.
    file_exists: bool

    @property
    def pending(self) -> bool:
        """Whether the migration has not been applied yet."""
        return self.batch is None


@dataclass(frozen=True)
class Migrator:
    """Applies migrations from a directory to a DuckLake.

    Migrations are Python files named ``YYYY_MM_DD_HHMMSS_<name>.py`` which define a function
    ``up(tx)``. The function receives a :class:`~ducklake.Transaction` and makes all changes
    through it. Migrations are applied in the order of their file names. Which migrations have
    been applied is recorded in the catalog database.
    """

    #: The DuckLake to apply the migrations to.
    dl: Ducklake
    #: The directory containing the migration files.
    path: str | Path = "migrations"

    def pending(self) -> list[str]:
        """The names of all migrations which have not been applied yet, in the order of application.

        This never modifies the catalog database.

        Raises:
            FileNotFoundError: If the directory does not exist.
            MigrationError: If the directory contains a file with an invalid migration name.
        """
        applied = {name for name, _, _ in self.dl._pyducklake.applied_migrations()}
        return [name for name in self._collect_migrations() if name not in applied]

    def status(self) -> list[MigrationStatus]:
        """The status of all migrations, ordered by name.

        This includes migrations which have been applied but whose file does not exist (anymore).
        It never modifies the catalog database.

        Raises:
            FileNotFoundError: If the directory does not exist.
            MigrationError: If the directory contains a file with an invalid migration name.
        """
        files = self._collect_migrations()
        applied = {
            name: (batch, applied_at)
            for name, batch, applied_at in self.dl._pyducklake.applied_migrations()
        }
        return [
            MigrationStatus(
                name=name,
                batch=applied[name][0] if name in applied else None,
                applied_at=applied[name][1] if name in applied else None,
                file_exists=name in files,
            )
            for name in sorted(files.keys() | applied.keys())
        ]

    def run(self) -> list[str]:
        """Apply all pending migrations.

        Migrations are applied one after the other in the order of their names. Each migration is
        committed in its own transaction, together with the record that marks it as applied. All
        migrations applied by one call share the same batch number.

        All pending migrations are loaded before the first one is applied, i.e., a migration file
        which cannot be loaded prevents any migration from being applied. If a migration fails
        while it is being applied, the run stops. Migrations applied before stay applied.

        Returns:
            The names of the migrations applied by this call, in the order of application.

        Raises:
            ReadonlyDucklakeError: If the DuckLake is read-only or time traveled.
            FileNotFoundError: If the directory does not exist.
            MigrationError: If a migration cannot be loaded or applied. The message names the
                migration and the original exception is available as the ``__cause__``.
        """
        if self.dl._pyducklake.is_readonly():
            raise ReadonlyDucklakeError("migrations cannot be applied to a read-only DuckLake")

        files = self._collect_migrations()

        self.dl._pyducklake.ensure_migrations_table()
        applied = self.dl._pyducklake.applied_migrations()
        applied_names = {name for name, _, _ in applied}
        batch = max((batch for _, batch, _ in applied), default=0) + 1

        migrations = {
            name: _load_up(name, file) for name, file in files.items() if name not in applied_names
        }
        for name, migration in migrations.items():
            try:
                tx = self.dl._transaction_for_migration(
                    name,
                    batch,
                    author="ducklake-sdk",
                    message=name,
                    extra_info=json.dumps({"migration": name, "batch": batch}),
                )
                migration(tx)
                tx.commit()
            except Exception as e:
                raise MigrationError(f"migration '{name}' failed: {e}") from e
        return list(migrations)

    def _collect_migrations(self) -> dict[str, Path]:
        """Find all migration files, ordered by name."""
        files: dict[str, Path] = {}
        for file in sorted(Path(self.path).iterdir()):
            if file.suffix != ".py" or file.name.startswith("_") or not file.is_file():
                continue
            _validate_name(file.stem)
            files[file.stem] = file
        return files


def _validate_name(name: str) -> None:
    """Ensure that the name of a migration is valid.

    Raises:
        MigrationError: If the name is invalid.
    """
    # NOTE: The maximum length matches the length of the column that stores the name in the
    #  catalog database.
    if not re.fullmatch(r"\d{4}_\d{2}_\d{2}_\d{6}_[a-z0-9_]+", name) or len(name) > 255:
        raise MigrationError(
            f"migration '{name}' has an invalid name: the name must have the form "
            "YYYY_MM_DD_HHMMSS_<name> where <name> consists of lowercase letters, digits, and "
            "underscores, and it must not exceed 255 characters"
        )


def _load_up(name: str, file: Path) -> Callable[[Transaction], None]:
    """Load the migration file and return its ``up`` function."""
    # NOTE: The names of migration files start with digits, hence they cannot be imported
    #  regularly. The module is registered in `sys.modules` as certain libraries (e.g.,
    #  `dataclasses`) expect to find the module of the classes defined in it.
    module_name = f"_ducklake_migration_{name}"
    spec = importlib.util.spec_from_file_location(module_name, file)
    if spec is None or spec.loader is None:
        raise MigrationError(f"migration '{name}' could not be loaded from '{file}'")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        del sys.modules[module_name]
        raise MigrationError(f"migration '{name}' could not be loaded: {e}") from e

    up = getattr(module, "up", None)
    if not callable(up):
        raise MigrationError(f"migration '{name}' does not define a function `up`")
    return up
