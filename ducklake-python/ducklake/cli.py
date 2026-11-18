from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import sys
import traceback
from pathlib import Path
from typing import TYPE_CHECKING

from .connect import connect
from .exceptions import MigrationError
from .migrations import Migrator, _validate_name

if TYPE_CHECKING:
    from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    """Run the ``ducklake`` command line interface.

    Args:
        argv: The command line arguments, excluding the program name. If not provided, the
            arguments are read from :data:`sys.argv`.

    Returns:
        The exit code of the command.
    """
    parser = argparse.ArgumentParser(prog="ducklake", description="Interact with a DuckLake.")
    commands = parser.add_subparsers(dest="command", required=True)

    migrate = commands.add_parser(
        "migrate", help="Manage migrations.", description="Manage migrations."
    )
    migrate_commands = migrate.add_subparsers(dest="migrate_command", required=True)

    new = migrate_commands.add_parser(
        "new", help="Create a new migration file.", description="Create a new migration file."
    )
    new.add_argument("name", help="The name of the migration, e.g. 'create_events_table'.")
    _add_path_argument(new)

    up = migrate_commands.add_parser(
        "up", help="Apply all pending migrations.", description="Apply all pending migrations."
    )
    _add_path_argument(up)
    _add_catalog_url_argument(up)

    status = migrate_commands.add_parser(
        "status",
        help="Show the status of all migrations.",
        description="Show the status of all migrations.",
    )
    _add_path_argument(status)
    _add_catalog_url_argument(status)

    args = parser.parse_args(argv)
    if args.migrate_command == "new":
        return _migrate_new(args.name, Path(args.path))

    subparser = up if args.migrate_command == "up" else status
    catalog_url = args.catalog_url or os.environ.get("DUCKLAKE_CATALOG_URL")
    if not catalog_url:
        subparser.error(
            "the catalog URL must be provided via --catalog-url or DUCKLAKE_CATALOG_URL"
        )

    try:
        with connect(catalog_url) as lake:
            migrator = Migrator(lake, args.path)
            if args.migrate_command == "up":
                return _migrate_up(migrator)
            return _migrate_status(migrator)
    except (MigrationError, FileNotFoundError) as e:
        _write_error(e)
        return 1


# ------------------------------------------ COMMANDS ------------------------------------------- #


def _migrate_new(name: str, directory: Path) -> int:
    name = _to_snake_case(name)
    if not name:
        sys.stderr.write("error: the migration name must contain at least one letter or digit\n")
        return 1
    stem = f"{dt.datetime.now(dt.UTC).strftime('%Y_%m_%d_%H%M%S')}_{name}"
    try:
        _validate_name(stem)
    except MigrationError as e:
        _write_error(e)
        return 1

    if directory.is_dir() and any(
        file.stem.split("_", 4)[-1] == name for file in directory.glob("*.py")
    ):
        sys.stderr.write(f"error: a migration named '{name}' already exists in '{directory}'\n")
        return 1

    directory.mkdir(parents=True, exist_ok=True)
    file = directory / f"{stem}.py"
    file.write_text("import ducklake as dl\n\n\ndef up(tx: dl.Transaction) -> None:\n    ...\n")
    sys.stdout.write(f"Created migration: {file}\n")
    return 0


def _migrate_up(migrator: Migrator) -> int:
    applied = migrator.run()
    if not applied:
        sys.stdout.write("Nothing to migrate.\n")
    for name in applied:
        sys.stdout.write(f"Applied {name}\n")
    return 0


def _migrate_status(migrator: Migrator) -> int:
    migrations = migrator.status()
    if not migrations:
        sys.stdout.write("No migrations found.\n")
    width = max((len(migration.name) for migration in migrations), default=0)
    for migration in migrations:
        state = "pending" if migration.batch is None else f"batch {migration.batch}"
        missing = "" if migration.file_exists else " (file missing)"
        sys.stdout.write(f"{migration.name.ljust(width)}  {state}{missing}\n")
    return 0


# ------------------------------------------- UTILS --------------------------------------------- #


def _add_path_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--path",
        default="migrations",
        help="The directory containing the migration files (default: %(default)s).",
    )


def _add_catalog_url_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--catalog-url",
        help="The URL of the catalog database. Defaults to the DUCKLAKE_CATALOG_URL environment "
        "variable.",
    )


def _to_snake_case(name: str) -> str:
    # Insert underscores at camel case boundaries, e.g. "CreateHTTPTable" -> "Create_HTTP_Table"
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _write_error(error: Exception) -> None:
    if error.__cause__ is not None:
        sys.stderr.write("".join(traceback.format_exception(error.__cause__)))
    sys.stderr.write(f"error: {error}\n")
