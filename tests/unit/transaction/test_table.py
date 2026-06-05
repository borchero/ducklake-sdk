import uuid

import pytest

import ducklake as dl
import ducklake.exceptions as dlexc


def test_create_table(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Act
    with shared_ducklake.transaction() as tx:
        tx.create_table(random_table_name, {"x": dl.Int64()})

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.name == ("main", random_table_name)
    assert table.schema.columns == [dl.Column("x", dl.Int64(), field_id=1)]
    assert table.partitioning is None
    assert table.tags == {}


def test_create_delete_table_does_nothing(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    snapshot = shared_ducklake.get_latest_snapshot()

    # Act
    with shared_ducklake.transaction() as tx:
        tx.create_table(random_table_name, {"x": dl.Int64()})
        tx.table(random_table_name).delete()

    # Assert
    assert shared_ducklake.get_latest_snapshot().id == snapshot.id


def test_delete_create_table(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).delete()
        tx.create_table(random_table_name, {"y": dl.Int64()})

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.schema.columns == [dl.Column("y", dl.Int64(), field_id=1)]


def test_double_rename_keeps_last(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    new_table_name = "table_" + str(uuid.uuid4()).replace("-", "")

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).rename(new_table_name + "_tmp")
        tx.table(new_table_name + "_tmp").rename(new_table_name)

    # Assert
    assert table.name.name == new_table_name


def test_explicit_commit(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Act
    tx = shared_ducklake.transaction()
    tx.create_table(random_table_name, {"x": dl.Int64()})
    tx.commit()

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.name == ("main", random_table_name)


def test_transaction_aborts_on_exception(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Act
    with pytest.raises(RuntimeError):
        with shared_ducklake.transaction() as tx:
            tx.create_table(random_table_name, {"x": dl.Int64()})
            raise RuntimeError()

    # Assert: the table was never committed and is therefore not visible
    with pytest.raises(dlexc.NotFoundError):
        shared_ducklake.get_table(random_table_name)


def test_create_table_with_partitioning_and_tags(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Act
    with shared_ducklake.transaction() as tx:
        tx.create_table(
            random_table_name,
            {"x": dl.Int64(), "y": dl.Varchar()},
            partition_by="x",
            tags={"env": "prod"},
        )

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.partitioning is not None
    assert [c.name for c in table.partitioning.columns] == ["x"]
    assert table.tags == {"env": "prod"}


def test_delete_table_in_transaction(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).delete()

    # Assert
    with pytest.raises(dlexc.NotFoundError):
        shared_ducklake.get_table(random_table_name)
