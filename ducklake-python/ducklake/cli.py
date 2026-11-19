import datetime as dt
import re
import sys
import traceback
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated, NoReturn

from .connect import connect
from .exceptions import MigrationError
from .migrations import Migrator, _validate_name

try:
    import typer
except ImportError as e:  # pragma: no cover
    raise ImportError(
        "the `ducklake` command requires typer, install it via `pip install 'ducklake-sdk[cli]'`"
    ) from e

app = typer.Typer(
    name="ducklake",
    help="Interact with a DuckLake.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)
migrate_app = typer.Typer(help="Manage migrations.", no_args_is_help=True)
app.add_typer(migrate_app, name="migrate")

PathOption = Annotated[Path, typer.Option(help="The directory containing the migration files.")]
CatalogUrlOption = Annotated[
    str, typer.Option(envvar="DUCKLAKE_CATALOG_URL", help="The URL of the catalog database.")
]


@migrate_app.command()
def new(
    name: Annotated[str, typer.Argument(help="The name of the migration, e.g. create_events.")],
    path: PathOption = Path("migrations"),
) -> None:
    """Create a new migration file."""
    name = _to_snake_case(name)
    if not name:
        _fail("the migration name must contain at least one letter or digit")
    stem = f"{dt.datetime.now(dt.UTC).strftime('%Y_%m_%d_%H%M%S')}_{name}"
    try:
        _validate_name(stem)
    except MigrationError as e:
        _fail(e)
    if path.is_dir() and any(file.stem.split("_", 4)[-1] == name for file in path.glob("*.py")):
        _fail(f"a migration named '{name}' already exists in '{path}'")

    path.mkdir(parents=True, exist_ok=True)
    file = path / f"{stem}.py"
    file.write_text("import ducklake as dl\n\n\ndef up(tx: dl.Transaction) -> None:\n    ...\n")
    typer.echo(f"Created migration: {file}")


@migrate_app.command()
def up(catalog_url: CatalogUrlOption, path: PathOption = Path("migrations")) -> None:
    """Apply all pending migrations."""
    with _migrator(catalog_url, path) as migrator:
        applied = migrator.run()
    typer.echo("\n".join(f"Applied {name}" for name in applied) or "Nothing to migrate.")


@migrate_app.command()
def status(catalog_url: CatalogUrlOption, path: PathOption = Path("migrations")) -> None:
    """Show the status of all migrations."""
    with _migrator(catalog_url, path) as migrator:
        migrations = migrator.status()
    width = max((len(migration.name) for migration in migrations), default=0)
    for migration in migrations:
        state = "pending" if migration.pending else f"batch {migration.batch}"
        missing = "" if migration.file_exists else " (file missing)"
        typer.echo(f"{migration.name.ljust(width)}  {state}{missing}")
    if not migrations:
        typer.echo("No migrations found.")


# ------------------------------------------- UTILS --------------------------------------------- #


@contextmanager
def _migrator(catalog_url: str, path: Path) -> Generator[Migrator]:
    try:
        with connect(catalog_url) as lake:
            yield Migrator(lake, path)
    except (MigrationError, FileNotFoundError) as e:
        _fail(e)


def _fail(error: Exception | str) -> NoReturn:
    if isinstance(error, Exception) and error.__cause__ is not None:
        sys.stderr.write("".join(traceback.format_exception(error.__cause__)))
    typer.echo(f"error: {error}", err=True)
    raise typer.Exit(1)


def _to_snake_case(name: str) -> str:
    # Insert underscores at camel case boundaries, e.g. "CreateHTTPTable" -> "Create_HTTP_Table"
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
