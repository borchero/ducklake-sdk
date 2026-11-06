import datetime as dt

import pytest
import sqlalchemy as sa
from _testutils import MIGRATIONS_TABLE

import ducklake as dl
import ducklake.exceptions as dlexc

LedgerRow = tuple[str, int, dt.datetime]


@pytest.fixture()
def ledger_rows() -> list[LedgerRow]:
    utc = dt.timezone.utc
    return [
        ("2026_11_03_110000_first", 1, dt.datetime(2026, 11, 3, 11, 0, 5, tzinfo=utc)),
        ("2026_11_03_120000_second", 1, dt.datetime(2026, 11, 3, 12, 0, 5, tzinfo=utc)),
        ("2026_11_04_090000_third", 2, dt.datetime(2026, 11, 4, 9, 0, 5, tzinfo=utc)),
    ]


@pytest.fixture()
def populated_ledger(
    ducklake: dl.Ducklake, catalog_engine: sa.Engine, ledger_rows: list[LedgerRow]
) -> list[LedgerRow]:
    ducklake._pyducklake.ensure_migrations_table()
    with catalog_engine.begin() as conn:
        # NOTE: Rows are inserted in reverse order to ensure that the order is not incidental
        conn.execute(
            MIGRATIONS_TABLE.insert(),
            [
                {"name": name, "batch": batch, "applied_at": applied_at}
                for name, batch, applied_at in reversed(ledger_rows)
            ],
        )
    return ledger_rows


def test_ensure_migrations_table_creates_table(
    ducklake: dl.Ducklake, catalog_engine: sa.Engine
) -> None:
    # Act
    ducklake._pyducklake.ensure_migrations_table()

    # Assert
    inspector = sa.inspect(catalog_engine)
    columns = inspector.get_columns(MIGRATIONS_TABLE.name)
    assert [column["name"] for column in columns] == ["name", "batch", "applied_at"]
    assert all(not column["nullable"] for column in columns)
    primary_key = inspector.get_pk_constraint(MIGRATIONS_TABLE.name)
    assert primary_key["constrained_columns"] == ["name"]


def test_ensure_migrations_table_is_idempotent(
    ducklake: dl.Ducklake, populated_ledger: list[LedgerRow]
) -> None:
    # Act
    ducklake._pyducklake.ensure_migrations_table()

    # Assert
    assert ducklake._pyducklake.applied_migrations() == populated_ledger


def test_ensure_migrations_table_fails_when_readonly(
    ducklake: dl.Ducklake, catalog_engine: sa.Engine
) -> None:
    # Arrange
    readonly = ducklake.readonly()

    # Act & Assert
    with pytest.raises(dlexc.ReadonlyDucklakeError):
        readonly._pyducklake.ensure_migrations_table()
    assert not sa.inspect(catalog_engine).has_table(MIGRATIONS_TABLE.name)


@pytest.mark.parametrize("table_exists", [False, True])
def test_applied_migrations_empty(
    ducklake: dl.Ducklake, catalog_engine: sa.Engine, table_exists: bool
) -> None:
    # Arrange
    if table_exists:
        ducklake._pyducklake.ensure_migrations_table()

    # Act
    result = ducklake._pyducklake.applied_migrations()

    # Assert
    assert result == []
    assert sa.inspect(catalog_engine).has_table(MIGRATIONS_TABLE.name) == table_exists


@pytest.mark.parametrize("readonly", [False, True])
def test_applied_migrations_ordered_by_batch_and_name(
    ducklake: dl.Ducklake, populated_ledger: list[LedgerRow], readonly: bool
) -> None:
    # Arrange
    connection = ducklake.readonly() if readonly else ducklake

    # Act
    result = connection._pyducklake.applied_migrations()

    # Assert
    assert result == populated_ledger


@pytest.mark.skip_config(catalog="mysql", reason="The DuckDB MySQL connector is unreliable.")
def test_duckdb_operations_leave_migrations_table_intact(
    ducklake: dl.Ducklake, populated_ledger: list[LedgerRow], random_table_name: str
) -> None:
    # Act
    ducklake.execute_sql(f"CREATE TABLE {random_table_name} (x INTEGER)")
    ducklake.execute_sql(f"INSERT INTO {random_table_name} VALUES (1)")
    ducklake.checkpoint()

    # Assert
    assert ducklake.table(random_table_name).scan_duckdb().fetchall() == [(1,)]
    assert ducklake._pyducklake.applied_migrations() == populated_ledger
