import pytest

import ducklake as dl
import ducklake.exceptions as dlexc


def test_create_table(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Act
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Assert
    assert table.name == ("main", random_table_name)
    assert table.schema.columns == [dl.Column("x", dl.Int64(), field_id=1)]
    assert table.partitioning is None
    assert table.tags == {}


def test_delete_table(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    table.delete()

    # Assert
    assert not any(t.name == ("main", random_table_name) for t in shared_ducklake.list_tables())


def test_table_repr(
    ducklake: dl.Ducklake, random_schema_name: str, random_table_name: str
) -> None:
    # Arrange
    ducklake.create_schema(random_schema_name)
    table = ducklake.create_table((random_schema_name, random_table_name), {"x": dl.Int64()})

    # Act
    actual = repr(table)

    # Assert
    assert actual == f"Table(schema='{random_schema_name}', name='{random_table_name}')"


def test_create_table_with_tags(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Act
    table = shared_ducklake.create_table(
        random_table_name, {"x": dl.Int64()}, tags={"env": "prod", "owner": "team-a"}
    )

    # Assert
    assert table.tags == {"env": "prod", "owner": "team-a"}


def test_create_table_with_partitioning(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Act
    table = shared_ducklake.create_table(
        random_table_name,
        {"x": dl.Int64(), "y": dl.Varchar()},
        partition_by=dl.Partitioning(["x"]),
    )

    # Assert
    assert table.partitioning is not None
    assert [c.name for c in table.partitioning.columns] == ["x"]


def test_create_table_in_schema_via_tuple(
    ducklake: dl.Ducklake, random_schema_name: str, random_table_name: str
) -> None:
    # Arrange
    ducklake.create_schema(random_schema_name)

    # Act
    table = ducklake.create_table((random_schema_name, random_table_name), {"x": dl.Int64()})

    # Assert
    assert table.name == (random_schema_name, random_table_name)


def test_create_existing_table_raises(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act & Assert
    with pytest.raises(dlexc.AlreadyExistsError):
        shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
