import ducklake as dl


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
