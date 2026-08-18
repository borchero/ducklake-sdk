import pytest

import ducklake as dl
import ducklake.exceptions as dlexc


def test_create_delete_schema(shared_ducklake: dl.Ducklake, random_schema_name: str) -> None:
    # Act
    shared_ducklake.create_schema(random_schema_name)

    # Assert
    assert random_schema_name in shared_ducklake.list_schemas()

    # Act
    shared_ducklake.delete_schema(random_schema_name)

    # Assert
    assert random_schema_name not in shared_ducklake.list_schemas()


def test_list_schemas_includes_default(shared_ducklake: dl.Ducklake) -> None:
    # Act
    schemas = shared_ducklake.list_schemas()

    # Assert
    assert "main" in schemas


def test_create_existing_schema_raises(
    shared_ducklake: dl.Ducklake, random_schema_name: str
) -> None:
    # Arrange
    shared_ducklake.create_schema(random_schema_name)

    # Act & Assert
    with pytest.raises(dlexc.AlreadyExistsError):
        shared_ducklake.create_schema(random_schema_name)


def test_create_existing_schema_skip(
    shared_ducklake: dl.Ducklake, random_schema_name: str
) -> None:
    # Arrange
    shared_ducklake.create_schema(random_schema_name)

    # Act
    shared_ducklake.create_schema(random_schema_name, if_exists="skip")

    # Assert
    assert random_schema_name in shared_ducklake.list_schemas()


def test_create_schema_skip_when_missing(
    shared_ducklake: dl.Ducklake, random_schema_name: str
) -> None:
    # Act
    shared_ducklake.create_schema(random_schema_name, if_exists="skip")

    # Assert
    assert random_schema_name in shared_ducklake.list_schemas()


def test_delete_missing_schema_raises(
    shared_ducklake: dl.Ducklake, random_schema_name: str
) -> None:
    # Act & Assert
    with pytest.raises(dlexc.NotFoundError):
        shared_ducklake.delete_schema(random_schema_name)


def test_delete_nonempty_schema_without_cascade_raises(
    shared_ducklake: dl.Ducklake, random_schema_name: str, random_table_name: str
) -> None:
    # Arrange
    shared_ducklake.create_schema(random_schema_name)
    shared_ducklake.create_table((random_schema_name, random_table_name), {"x": dl.Int64()})

    # Act
    with pytest.raises(ValueError, match="not empty"):
        shared_ducklake.delete_schema(random_schema_name)

    # Assert
    assert random_schema_name in shared_ducklake.list_schemas()
    assert shared_ducklake.has_table((random_schema_name, random_table_name))


@pytest.mark.parametrize("use_transaction", [False, True])
def test_delete_schema_cascade(
    shared_ducklake: dl.Ducklake,
    random_schema_name: str,
    random_table_name: str,
    use_transaction: bool,
) -> None:
    # Arrange
    table_names = [random_table_name, random_table_name + "_other"]
    shared_ducklake.create_schema(random_schema_name)
    for table_name in table_names:
        shared_ducklake.create_table((random_schema_name, table_name), {"x": dl.Int64()})
    snapshot = shared_ducklake.get_latest_snapshot()

    # Act
    if use_transaction:
        with shared_ducklake.transaction() as tx:
            tx.delete_schema(random_schema_name, cascade=True)
    else:
        shared_ducklake.delete_schema(random_schema_name, cascade=True)

    # Assert
    assert shared_ducklake.get_latest_snapshot().id == snapshot.id + 1
    assert random_schema_name not in shared_ducklake.list_schemas()
    assert all(
        not shared_ducklake.has_table((random_schema_name, table_name))
        for table_name in table_names
    )


def test_create_schema_with_data_path(
    ducklake: dl.Ducklake,
    random_schema_name: str,
    random_table_name: str,
    storage_path: str,
) -> None:
    # Arrange
    schema_data_path = storage_path.rstrip("/") + "/custom_schema/"

    # Act
    ducklake.create_schema(random_schema_name, data_path=schema_data_path)
    ducklake.create_table(
        (random_schema_name, random_table_name),
        {"x": dl.Int64()},
    )

    # Assert
    assert random_schema_name in ducklake.list_schemas()
