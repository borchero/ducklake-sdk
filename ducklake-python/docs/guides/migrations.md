# Migrations

This guide explains how to evolve a DuckLake over time with versioned, user-defined migrations.

## What Are Migrations?

Migrations are Python files that describe changes to a DuckLake, e.g. creating a schema, creating a table, or adding a
column. They are stored in a directory alongside your code and applied in order. The DuckLake remembers which migrations
have already been applied such that every migration is applied exactly once.

Migrations are not to be confused with _catalog upgrades_. A catalog upgrade migrates the catalog database to a newer
version of the DuckLake specification and is performed by passing `upgrade=True` to {func}`ducklake.connect`.
Migrations, in contrast, change the _contents_ of the DuckLake and are written by you.

## Writing Migrations

Migrations live in a directory, by default `migrations/`. Each migration is a Python file named
`YYYY_MM_DD_HHMMSS_<name>.py` where `<name>` consists of lowercase letters, digits, and underscores. The file name
without the extension must not exceed 255 characters. Migrations are applied in the order of their file names, i.e.,
in the order in which they were created.

Each migration defines a function `up` which receives a {class}`~ducklake.Transaction` and makes all changes through
it:

```python
# migrations/2026_01_01_120000_create_events_table.py
import ducklake as dl


def up(tx: dl.Transaction) -> None:
    tx.create_schema("analytics")
    tx.create_table("analytics.events", {"id": dl.Int64(), "name": dl.Varchar()})
```

A later migration can build upon the changes of earlier ones:

```python
# migrations/2026_02_01_090000_add_event_timestamp.py
import ducklake as dl


def up(tx: dl.Transaction) -> None:
    tx.table("analytics.events").add_column(dl.Column("created_at", dl.TimestampTz()))
```

Files that do not end in `.py` and files whose names start with an underscore (e.g., `_helpers.py`) are ignored. Any
other Python file in the directory must follow the naming scheme above.

## Using the Command Line

The `ducklake` command is installed together with the SDK. It can also be run via `python -m ducklake`.

To create a new migration, run:

```bash
ducklake migrate new create_events_table
```

This creates a file such as `migrations/2026_01_01_120000_create_events_table.py` with an empty `up` function. The
name is converted to snake case, i.e., `CreateEventsTable` and `create-events-table` result in the same file name.
Use `--path` to place migrations in a different directory.

To apply all pending migrations and to show which migrations have been applied, run:

```bash
export DUCKLAKE_CATALOG_URL="postgresql://user:password@localhost:5432/catalog"
ducklake migrate up
ducklake migrate status
```

Instead of setting the `DUCKLAKE_CATALOG_URL` environment variable, you may also pass the URL via `--catalog-url`.
Storage options are inferred from environment variables, just like in {func}`ducklake.connect`.

## Using the Python API

Migrations can also be applied from Python via {class}`~ducklake.migrations.Migrator`:

```python
import ducklake as dl
from ducklake.migrations import Migrator

with dl.connect("sqlite:///metadata.sqlite") as ducklake:
    migrator = Migrator(ducklake, "migrations")
    print(migrator.pending())
    applied = migrator.run()
```

{meth}`~ducklake.migrations.Migrator.status` additionally reports the batch and time at which each migration was
applied as well as migrations which were applied but whose file no longer exists.

## Guarantees

- Each migration is applied in its own transaction. The changes of the migration and the record marking it as applied
  are committed atomically.
- All migrations applied by one run share the same _batch_ number.
- All pending migration files are loaded and validated before the first one is applied. A file which cannot be loaded,
  e.g., due to a syntax error or a missing `up` function, prevents any migration from being applied.
- If a migration fails, the run stops and raises a {class}`~ducklake.exceptions.MigrationError` which names the
  migration. Migrations applied before in the same run stay applied.
- Concurrent runs cannot apply the same migration twice. The run that loses the race fails with a
  {class}`~ducklake.exceptions.MigrationError`. Its cause is a {class}`~ducklake.exceptions.TransactionConflictError`
  if the changes of both runs conflict (e.g., both create the same table) and a
  {class}`~ducklake.exceptions.MigrationConflictError` otherwise. Running the migrations again is safe.
- Read-only and time-traveled DuckLakes are rejected with a {class}`~ducklake.exceptions.ReadonlyDucklakeError`.
  Inspecting pending migrations and their status works nonetheless.

## Limitations

- Only "up" migrations are supported. To undo a migration, write a new migration.
- Migrations can only make changes that {class}`~ducklake.Transaction` supports. For example, rows cannot be updated
  or deleted.
- Anything a migration does outside of the provided transaction, e.g., calling
  {meth}`~ducklake.Ducklake.execute_sql`, is not part of the atomic commit.
- Applied migrations are recorded in the `_ducklake_sdk_migrations` table of the catalog database. This table is not
  part of the DuckLake specification and is unknown to other DuckLake clients.
