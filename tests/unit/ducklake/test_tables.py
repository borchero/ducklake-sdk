import pytest

import ducklake as dl
import ducklake.exceptions as dlexc


def test_list_tables(ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    ducklake.create_table(random_table_name, {"x": dl.Int64()})
    ducklake.create_table(random_table_name + "_other", {"y": dl.Varchar()})

    # Act
    tables = ducklake.list_tables()

    # Assert
    table_names = {table.name.name for table in tables}
    assert random_table_name in table_names
    assert random_table_name + "_other" in table_names


def test_list_tables_filtered_by_schema(
    ducklake: dl.Ducklake, random_schema_name: str, random_table_name: str
) -> None:
    # Arrange
    ducklake.create_schema(random_schema_name)
    ducklake.create_table((random_schema_name, random_table_name), {"x": dl.Int64()})
    ducklake.create_table(random_table_name + "_main", {"x": dl.Int64()})

    # Act
    tables_in_schema = ducklake.list_tables(schema=random_schema_name)
    tables_in_main = ducklake.list_tables(schema="main")

    # Assert
    schema_names = {(t.name.schema, t.name.name) for t in tables_in_schema}
    main_names = {(t.name.schema, t.name.name) for t in tables_in_main}
    assert (random_schema_name, random_table_name) in schema_names
    assert all(t.name.schema == random_schema_name for t in tables_in_schema)
    assert ("main", random_table_name + "_main") in main_names
    assert all(t.name.schema == "main" for t in tables_in_main)


def test_get_table_not_found(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Act & Assert
    with pytest.raises(dlexc.NotFoundError):
        shared_ducklake.get_table(random_table_name)


def test_get_table_by_tuple(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    table = shared_ducklake.get_table(("main", random_table_name))

    # Assert
    assert table.name == ("main", random_table_name)


def test_get_table_by_tablename(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    table = shared_ducklake.get_table(dl.TableName("main", random_table_name))

    # Assert
    assert table.name == ("main", random_table_name)
