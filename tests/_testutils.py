from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import boto3
import sqlalchemy as sa
from sqlalchemy_utils import create_database, database_exists, drop_database

# ----------------------------------------------------------------------------------------------- #
#                                              SETUP                                              #
# ----------------------------------------------------------------------------------------------- #


@contextmanager
def make_catalog_url(catalog: str, tmp_path: Path) -> Iterator[str]:
    database_name = uuid.uuid4()
    match catalog:
        case "sqlite":
            yield f"sqlite:///{tmp_path}/{database_name}.db"
        case "postgres":
            url = f"postgresql://postgres:postgres@localhost:5432/{database_name}"
            create_database(url)
            try:
                yield url
            finally:
                if database_exists(url):
                    # In some cases, Postgres connections are not properly dropped by the server.
                    # Hence, we force-drop the database.
                    url = sa.make_url(url).set(database="postgres")
                    admin_engine = sa.create_engine(url, isolation_level="AUTOCOMMIT")
                    with admin_engine.connect() as conn:
                        conn.execute(sa.text(f'DROP DATABASE "{database_name}" WITH (FORCE);'))
                    admin_engine.dispose()
        case "mysql":
            url = f"mysql://root:root@127.0.0.1:3306/{database_name}"
            create_database(url)
            try:
                yield url
            finally:
                if database_exists(url):
                    drop_database(url)
        case _:
            raise NotImplementedError


@contextmanager
def make_storage_path(storage: str, tmp_path: Path) -> Iterator[str]:
    match storage:
        case "local":
            yield str(tmp_path / str(uuid.uuid4()))
        case "s3":
            bucket = str(uuid.uuid4())
            s3 = boto3.resource("s3")
            s3.create_bucket(Bucket=bucket)
            try:
                yield f"s3://{bucket}"
            finally:
                s3.Bucket(bucket).objects.delete()
                s3.Bucket(bucket).delete()
        case _:
            raise NotImplementedError


# ----------------------------------------------------------------------------------------------- #
#                                            ASSERTION                                            #
# ----------------------------------------------------------------------------------------------- #

_IGNORED_COLUMNS: dict[str, list[str]] = {
    "ducklake_snapshot": ["snapshot_time"],
    "ducklake_schema": ["schema_uuid"],
    "ducklake_table": ["table_uuid"],
    "ducklake_data_file": ["path", "file_order", "file_size_bytes", "footer_size"],
    "ducklake_file_column_stats": ["column_size_bytes"],
    "ducklake_table_stats": ["file_size_bytes"],
}
_CONDITIONAL_IGNORED_COLUMNS: dict[str, dict[tuple[str, Any], list[str]]] = {
    "ducklake_metadata": {
        ("key", "created_by"): ["value"],
        ("key", "data_path"): ["value"],
    }
}


def assert_ducklake_catalogs_equal(
    expected: sa.Engine | str,
    actual: sa.Engine | str,
    *,
    extra_ignored_columns: dict[str, list[str]] | None = None,
) -> None:
    """Assert that the catalog databases of two DuckLakes are equivalent.

    Verifies that both catalogs:
    - Contain the same tables.
    - Have matching schemas for each table (column name, type, order, nullability) and the same
      primary key columns.
    - Contain the same rows in each table.
    """
    with _get_engine(expected) as expected_engine, _get_engine(actual) as actual_engine:
        expected_metadata = sa.MetaData()
        expected_metadata.reflect(bind=expected_engine)
        actual_metadata = sa.MetaData()
        actual_metadata.reflect(bind=actual_engine)

        expected_tables = set(expected_metadata.tables)
        actual_tables = set(actual_metadata.tables)
        assert expected_tables == actual_tables, (
            f"Table sets differ: expected {expected_tables}, got {actual_tables}"
        )

        for table_name in expected_tables:
            expected_table = expected_metadata.tables[table_name]
            actual_table = actual_metadata.tables[table_name]
            _assert_table_schemas_equal(expected_table, actual_table)
            _assert_table_contents_equal(
                expected_engine,
                actual_engine,
                expected_table,
                actual_table,
                ignored_columns=(
                    _IGNORED_COLUMNS.get(table_name, [])
                    + (extra_ignored_columns or {}).get(table_name, [])
                ),
                conditional_ignored_columns=_CONDITIONAL_IGNORED_COLUMNS.get(table_name, {}),
            )


@contextmanager
def _get_engine(conn: sa.Engine | str) -> Iterator[sa.Engine]:
    if isinstance(conn, sa.Engine):
        yield conn
    else:
        engine = sa.create_engine(conn)
        try:
            yield engine
        finally:
            engine.dispose()


def _assert_table_schemas_equal(expected: sa.Table, actual: sa.Table) -> None:
    """Assert that two tables have the same column schema and primary key."""
    expected_cols = list(expected.columns)
    actual_cols = list(actual.columns)
    assert len(expected_cols) == len(actual_cols), (
        f"Column count differs for table '{expected.name}': "
        f"expected {len(expected_cols)}, got {len(actual_cols)}. "
        f"Extra: {set(c.name for c in actual.columns) - set(c.name for c in expected.columns)}, "
        f"Missing: {set(c.name for c in expected.columns) - set(c.name for c in actual.columns)}"
    )
    for expected_col, actual_col in zip(expected_cols, actual_cols, strict=True):
        assert expected_col.name == actual_col.name, (
            f"Column name mismatch in table '{expected.name}': "
            f"expected '{expected_col.name}', got '{actual_col.name}'"
        )
        assert expected_col.nullable == actual_col.nullable, (
            f"Nullability mismatch for column '{expected_col.name}' in table '{expected.name}': "
            f"expected {expected_col.nullable}, got {actual_col.nullable}"
        )
        # Compare types via their generic Python representation to tolerate dialect-specific
        # type subclasses.
        expected_type = expected_col.type.as_generic()
        actual_type = actual_col.type.as_generic()
        assert type(expected_type) is type(actual_type), (
            f"Type mismatch for column '{expected_col.name}' in table '{expected.name}': "
            f"expected {expected_type!r}, got {actual_type!r}"
        )

    expected_pk = [c.name for c in expected.primary_key.columns]
    actual_pk = [c.name for c in actual.primary_key.columns]
    assert expected_pk == actual_pk, (
        f"Primary key mismatch for table '{expected.name}': "
        f"expected {expected_pk}, got {actual_pk}"
    )


def _assert_table_contents_equal(
    expected_engine: sa.Engine,
    actual_engine: sa.Engine,
    expected: sa.Table,
    actual: sa.Table,
    ignored_columns: list[str],
    conditional_ignored_columns: dict[tuple[str, Any], list[str]],
) -> None:
    """Assert that two tables contain the same rows (order-independent).

    Columns listed in `ignored_columns` are excluded from row comparison.

    `conditional_ignored_columns` allows per-row value masking: for each
    `(match_column, match_value) -> [columns]` entry, rows whose `match_column` equals
    `match_value` have their values in `[columns]` replaced with a nullability sentinel.
    Such rows are still asserted to be present, but the listed columns are not compared.
    """
    column_names = [c.name for c in expected.columns if c.name not in ignored_columns]
    with expected_engine.connect() as expected_conn, actual_engine.connect() as actual_conn:
        expected_rows = expected_conn.execute(
            sa.select(*[expected.c[name] for name in column_names])
        ).all()
        actual_rows = actual_conn.execute(
            sa.select(*[actual.c[name] for name in column_names])
        ).all()

    def _project(row: sa.Row) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for name, value in zip(column_names, row, strict=True):
            result[name] = value
        # Apply conditional row-level masking.
        for (match_column, match_value), masked_columns in conditional_ignored_columns.items():
            if result.get(match_column) == match_value:
                for masked in masked_columns:
                    result.pop(masked, None)
        return result

    expected_projected = [_project(r) for r in expected_rows]
    actual_projected = [_project(r) for r in actual_rows]
    expected_sorted = sorted(expected_projected, key=_row_sort_key)
    actual_sorted = sorted(actual_projected, key=_row_sort_key)
    assert len(expected_sorted) == len(actual_sorted), (
        f"Row count differs for table '{expected.name}': "
        f"expected {len(expected_sorted)}, got {len(actual_sorted)}"
    )
    for expected_row, actual_row in zip(expected_sorted, actual_sorted, strict=True):
        assert expected_row == actual_row, (
            f"Row contents differ for table '{expected.name}': "
            f"expected {expected_row}, got {actual_row}"
        )


def _row_sort_key(row: dict[str, Any]) -> tuple[tuple[int, str], ...]:
    # Build a sort key that tolerates None values and heterogeneous types by tagging each value
    # with a discriminator and using its repr so comparisons never raise. Sort by column name to
    # produce a stable ordering across rows.
    return tuple(
        (0, "") if value is None else (1, repr(value)) for _, value in sorted(row.items())
    )
