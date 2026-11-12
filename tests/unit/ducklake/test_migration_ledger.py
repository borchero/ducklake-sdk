import pytest
import sqlalchemy as sa

import ducklake as dl

TABLE = "_ducklake_sdk_migrations"

LedgerRow = tuple[str, int]


@pytest.fixture()
def ledger_rows() -> list[LedgerRow]:
    return [
        ("2026_11_03_110000_first", 1),
        ("2026_11_03_120000_second", 1),
        ("2026_11_04_090000_third", 2),
    ]


@pytest.fixture()
def populated_ledger(ducklake: dl.Ducklake, ledger_rows: list[LedgerRow]) -> list[LedgerRow]:
    ducklake._pyducklake.ensure_migrations_table()
    # NOTE: Migrations are recorded in reverse order to ensure that the order is not incidental
    for name, batch in reversed(ledger_rows):
        with ducklake._transaction_for_migration(name, batch):
            pass
    return ledger_rows


def _applied(lake: dl.Ducklake) -> list[LedgerRow]:
    return [(name, batch) for name, batch, _ in lake._pyducklake.applied_migrations()]


def test_ensure_migrations_table_creates_table(
    ducklake: dl.Ducklake, catalog_engine: sa.Engine
) -> None:
    # Act
    ducklake._pyducklake.ensure_migrations_table()

    # Assert
    inspector = sa.inspect(catalog_engine)
    columns = inspector.get_columns(TABLE)
    assert [column["name"] for column in columns] == ["name", "batch", "applied_at"]
    assert all(not column["nullable"] for column in columns)
    assert inspector.get_pk_constraint(TABLE)["constrained_columns"] == ["name"]


def test_ensure_migrations_table_is_idempotent(
    ducklake: dl.Ducklake, populated_ledger: list[LedgerRow]
) -> None:
    # Act
    ducklake._pyducklake.ensure_migrations_table()

    # Assert
    assert _applied(ducklake) == populated_ledger


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
    assert sa.inspect(catalog_engine).has_table(TABLE) == table_exists


def test_applied_migrations_ordered_by_batch_and_name(
    ducklake: dl.Ducklake, populated_ledger: list[LedgerRow]
) -> None:
    # Act
    result = _applied(ducklake)

    # Assert
    assert result == populated_ledger
