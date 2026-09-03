import pytest

import ducklake as dl
import ducklake.exceptions as dlexc


def test_create_view(
    shared_ducklake: dl.Ducklake, random_table_name: str, random_view_name: str
) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    view = shared_ducklake.create_view(random_view_name, f"SELECT x FROM {random_table_name}")

    # Assert
    assert view.name == ("main", random_view_name)
    assert view.sql == f"SELECT\n  x\nFROM\n  {random_table_name}"
    assert view.tags == {}


def test_create_view_with_column_aliases(
    shared_ducklake: dl.Ducklake, random_table_name: str, random_view_name: str
) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act: use aliases that require quoting to exercise the serialization roundtrip.
    aliases = ["renamed x", 'weird"quote']
    shared_ducklake.create_view(
        random_view_name,
        f"SELECT x, x + 1 FROM {random_table_name}",
        column_aliases=aliases,
    )

    # Assert: the aliases roundtrip through the catalog verbatim (without quoting artifacts).
    view = shared_ducklake.get_view(random_view_name)
    assert view.column_aliases == aliases


def test_create_view_without_column_aliases(
    shared_ducklake: dl.Ducklake, random_table_name: str, random_view_name: str
) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    shared_ducklake.create_view(random_view_name, f"SELECT x FROM {random_table_name}")

    # Assert: no aliases were provided, so none are reported.
    assert shared_ducklake.get_view(random_view_name).column_aliases is None


def test_create_view_with_tags(
    shared_ducklake: dl.Ducklake, random_table_name: str, random_view_name: str
) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    view = shared_ducklake.create_view(
        random_view_name,
        f"SELECT x FROM {random_table_name}",
        tags={"env": "prod"},
    )

    # Assert
    assert view.tags == {"env": "prod"}


def test_create_view_rejects_non_select(
    shared_ducklake: dl.Ducklake, random_view_name: str
) -> None:
    # Act & Assert
    with pytest.raises(ValueError):
        shared_ducklake.create_view(random_view_name, "CREATE VIEW v AS SELECT 1")


def test_delete_view(
    shared_ducklake: dl.Ducklake, random_table_name: str, random_view_name: str
) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    view = shared_ducklake.create_view(random_view_name, f"SELECT x FROM {random_table_name}")

    # Act
    view.delete()

    # Assert
    assert not any(v.name == ("main", random_view_name) for v in shared_ducklake.list_views())


def test_create_existing_view_raises(
    shared_ducklake: dl.Ducklake, random_table_name: str, random_view_name: str
) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    shared_ducklake.create_view(random_view_name, f"SELECT x FROM {random_table_name}")

    # Act & Assert
    with pytest.raises(dlexc.AlreadyExistsError):
        shared_ducklake.create_view(random_view_name, f"SELECT x FROM {random_table_name}")


def test_create_existing_view_skip(
    shared_ducklake: dl.Ducklake, random_table_name: str, random_view_name: str
) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    shared_ducklake.create_view(random_view_name, f"SELECT x FROM {random_table_name}")

    # Act
    view = shared_ducklake.create_view(
        random_view_name,
        f"SELECT x AS y FROM {random_table_name}",
        if_exists="skip",
    )

    # Assert: the existing view is returned unchanged.
    assert view.sql == f"SELECT\n  x\nFROM\n  {random_table_name}"


@pytest.mark.parametrize("existing_kind", ["table", "view"])
def test_create_relation_with_conflicting_name_raises(
    shared_ducklake: dl.Ducklake, random_table_name: str, existing_kind: str
) -> None:
    # Arrange
    if existing_kind == "table":
        shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    else:
        shared_ducklake.create_view(random_table_name, "SELECT 1 AS x")

    # Act
    with pytest.raises(dlexc.AlreadyExistsError) as exc_info:
        if existing_kind == "table":
            shared_ducklake.create_view(random_table_name, "SELECT 1 AS x")
        else:
            shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Assert
    assert "already exists" in str(exc_info.value)


def test_rename_table_to_existing_view_raises(
    shared_ducklake: dl.Ducklake, random_table_name: str, random_view_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    shared_ducklake.create_view(random_view_name, "SELECT 1 AS x")

    # Act
    with pytest.raises(dlexc.AlreadyExistsError) as exc_info:
        table.rename(random_view_name)

    # Assert
    assert "already exists" in str(exc_info.value)


def test_get_view(
    shared_ducklake: dl.Ducklake, random_table_name: str, random_view_name: str
) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    shared_ducklake.create_view(random_view_name, f"SELECT x FROM {random_table_name}")

    # Act
    view = shared_ducklake.get_view(random_view_name)

    # Assert
    assert view.name == ("main", random_view_name)


def test_get_missing_view_raises(shared_ducklake: dl.Ducklake, random_view_name: str) -> None:
    # Act & Assert
    with pytest.raises(dlexc.NotFoundError):
        shared_ducklake.get_view(random_view_name)


def test_view_repr(ducklake: dl.Ducklake, random_table_name: str, random_view_name: str) -> None:
    # Arrange
    ducklake.create_table(random_table_name, {"x": dl.Int64()})
    view = ducklake.create_view(random_view_name, f"SELECT x FROM {random_table_name}")

    # Act
    actual = repr(view)

    # Assert
    assert actual == f"View(schema='main', name='{random_view_name}')"
