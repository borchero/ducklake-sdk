import pytest

import ducklake as dl
import ducklake.exceptions as dlexc


def test_delete_missing_schema_raises(
    shared_ducklake: dl.Ducklake, random_schema_name: str
) -> None:
    # Act & Assert
    with shared_ducklake.transaction() as tx:
        with pytest.raises(dlexc.NotFoundError):
            tx.delete_schema(random_schema_name)


def test_delete_missing_schema_skip(
    shared_ducklake: dl.Ducklake, random_schema_name: str
) -> None:
    # Arrange
    snapshot = shared_ducklake.get_latest_snapshot()

    # Act
    with shared_ducklake.transaction() as tx:
        tx.delete_schema(random_schema_name, if_not_exists="skip")

    # Assert
    assert random_schema_name not in shared_ducklake.list_schemas()
    assert shared_ducklake.get_latest_snapshot().id == snapshot.id


def test_delete_existing_schema_skip(
    shared_ducklake: dl.Ducklake, random_schema_name: str
) -> None:
    # Arrange
    shared_ducklake.create_schema(random_schema_name)

    # Act
    with shared_ducklake.transaction() as tx:
        tx.delete_schema(random_schema_name, if_not_exists="skip")

    # Assert
    assert random_schema_name not in shared_ducklake.list_schemas()


def test_create_delete_schema_does_nothing(
    shared_ducklake: dl.Ducklake, random_schema_name: str
) -> None:
    # Arrange
    snapshot = shared_ducklake.get_latest_snapshot()

    # Act
    with shared_ducklake.transaction() as tx:
        tx.create_schema(random_schema_name)
        tx.delete_schema(random_schema_name)

    # Assert
    assert shared_ducklake.get_latest_snapshot().id == snapshot.id


def test_delete_create_schema(shared_ducklake: dl.Ducklake, random_schema_name: str) -> None:
    # Arrange
    shared_ducklake.create_schema(random_schema_name)

    # Act
    with shared_ducklake.transaction() as tx:
        tx.delete_schema(random_schema_name)
        tx.create_schema(random_schema_name)

    # Assert
    assert random_schema_name in shared_ducklake.list_schemas()


def test_list_schemas_reflects_transaction_changes(
    shared_ducklake: dl.Ducklake, random_schema_name: str
) -> None:
    # Act
    with shared_ducklake.transaction() as tx:
        tx.create_schema(random_schema_name)
        schemas_after_create = tx.list_schemas()
        tx.delete_schema(random_schema_name)
        schemas_after_delete = tx.list_schemas()

    # Assert
    assert random_schema_name in schemas_after_create
    assert random_schema_name not in schemas_after_delete
