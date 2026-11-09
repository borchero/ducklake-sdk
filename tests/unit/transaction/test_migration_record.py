import datetime as dt

import pytest
import sqlalchemy as sa

import ducklake as dl
import ducklake.exceptions as dlexc

MIGRATION = "2026_11_03_110000_create_events_table"
OTHER_MIGRATION = "2026_11_04_090000_create_users_table"


@pytest.fixture()
def lake(ducklake: dl.Ducklake) -> dl.Ducklake:
    ducklake._pyducklake.ensure_migrations_table()
    return ducklake


def _applied_names(lake: dl.Ducklake) -> list[str]:
    return [name for name, _, _ in lake._pyducklake.applied_migrations()]


def test_record_migration_commits_ledger_row_and_snapshot(
    lake: dl.Ducklake, catalog_url: str, catalog_engine: sa.Engine, random_table_name: str
) -> None:
    # Arrange
    snapshot_id = lake.get_latest_snapshot().id

    # Act
    with lake._transaction_for_migration(
        MIGRATION, 3, author="ducklake-sdk", message=MIGRATION, extra_info="{}"
    ) as tx:
        tx.create_table(random_table_name, {"x": dl.Int64()})

    # Assert
    [(name, batch, applied_at)] = lake._pyducklake.applied_migrations()
    assert (name, batch) == (MIGRATION, 3)
    assert abs(dt.datetime.now(dt.timezone.utc) - applied_at) < dt.timedelta(minutes=5)
    assert lake.has_table(random_table_name)
    assert lake.get_latest_snapshot().id == snapshot_id + 1
    # NOTE: A new connection reads the snapshot time from the database, just like the ledger
    with dl.connect(catalog_url) as fresh_lake:
        assert fresh_lake.get_latest_snapshot().timestamp == applied_at
    with catalog_engine.connect() as conn:
        author_info = conn.execute(
            sa.text(
                "SELECT author, commit_message, commit_extra_info FROM ducklake_snapshot_changes"
                " ORDER BY snapshot_id DESC LIMIT 1"
            )
        ).one()
    assert tuple(author_info) == ("ducklake-sdk", MIGRATION, "{}")


def test_record_migration_without_other_changes(lake: dl.Ducklake) -> None:
    # Act
    with lake._transaction_for_migration(MIGRATION, 1):
        pass

    # Assert
    assert _applied_names(lake) == [MIGRATION]


def test_record_migration_not_recorded_if_commit_fails(
    lake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    lake.create_table(random_table_name, {"x": dl.Int64()})
    tx = lake._transaction_for_migration(MIGRATION, 1)
    tx.table(random_table_name).add_column(dl.Column("y", dl.Int64()))
    with lake.transaction() as concurrent_tx:
        concurrent_tx.table(random_table_name).add_column(dl.Column("z", dl.Int64()))

    # Act
    with pytest.raises(dlexc.TransactionConflictError):
        tx.commit()

    # Assert
    assert _applied_names(lake) == []


@pytest.mark.parametrize("open_before_commit", [True, False])
def test_record_migration_fails_if_already_recorded(
    lake: dl.Ducklake, random_table_name: str, open_before_commit: bool
) -> None:
    # Arrange
    other_table_name = random_table_name + "_other"
    tx1 = lake._transaction_for_migration(MIGRATION, 1)
    tx1.create_table(random_table_name, {"x": dl.Int64()})
    if open_before_commit:
        tx2 = lake._transaction_for_migration(MIGRATION, 2)
    tx1.commit()
    if not open_before_commit:
        tx2 = lake._transaction_for_migration(MIGRATION, 2)
    tx2.create_table(other_table_name, {"x": dl.Int64()})

    # Act
    with pytest.raises(dlexc.MigrationConflictError):
        tx2.commit()

    # Assert
    assert [(name, batch) for name, batch, _ in lake._pyducklake.applied_migrations()] == [
        (MIGRATION, 1)
    ]
    assert lake.has_table(random_table_name)
    assert not lake.has_table(other_table_name)


def test_record_different_migrations_concurrently(
    lake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    tx1 = lake._transaction_for_migration(MIGRATION, 1)
    tx1.create_table(random_table_name, {"x": dl.Int64()})
    tx2 = lake._transaction_for_migration(OTHER_MIGRATION, 1)
    tx2.create_table(random_table_name + "_other", {"x": dl.Int64()})

    # Act
    tx1.commit()
    tx2.commit()

    # Assert
    assert _applied_names(lake) == sorted([MIGRATION, OTHER_MIGRATION])
    assert lake.has_table(random_table_name)
    assert lake.has_table(random_table_name + "_other")


@pytest.mark.parametrize(
    ("name", "fails"),
    [
        pytest.param("", True, id="empty"),
        pytest.param("m", False, id="shortest"),
        pytest.param("m" * 255, False, id="longest"),
        pytest.param("m" * 256, True, id="too-long"),
    ],
)
def test_record_migration_validates_name(lake: dl.Ducklake, name: str, fails: bool) -> None:
    # Act & Assert
    if fails:
        with pytest.raises(ValueError, match="between 1 and 255 characters"):
            lake._transaction_for_migration(name, 1)
    else:
        with lake._transaction_for_migration(name, 1):
            pass
        assert _applied_names(lake) == [name]


@pytest.mark.skip_config(catalog="mysql", reason="The DuckDB MySQL connector is unreliable.")
def test_duckdb_reads_lake_with_recorded_migrations(
    lake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    with lake._transaction_for_migration(MIGRATION, 1):
        pass
    with lake._transaction_for_migration(OTHER_MIGRATION, 1) as tx:
        tx.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    lake.execute_sql(f"INSERT INTO {random_table_name} VALUES (1)")
    lake.checkpoint()

    # Assert
    assert lake.table(random_table_name).scan_duckdb().fetchall() == [(1,)]
    assert _applied_names(lake) == sorted([MIGRATION, OTHER_MIGRATION])
