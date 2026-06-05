import pytest

import ducklake as dl
import ducklake.exceptions as dlexc

# ----------------------------------------- RENAME TABLE ---------------------------------------- #


def test_rename_table(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    new_table_name = random_table_name + "_rename"
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    table.rename(new_table_name)

    # Assert
    assert table.name == ("main", new_table_name)


# ------------------------------------------ ADD COLUMN ----------------------------------------- #


def test_add_column(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    table.add_column(dl.Column("y", dl.Varchar()))

    # Assert
    assert table.schema.columns == [
        dl.Column("x", dl.Int64(), field_id=1),
        dl.Column("y", dl.Varchar(), field_id=2),
    ]


# ---------------------------------------- RENAME COLUMN ---------------------------------------- #


def test_rename_column(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    table.rename_column("x", "y")

    # Assert
    assert table.schema.columns == [dl.Column("y", dl.Int64(), field_id=1)]


def test_rename_column_already_exists(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64(), "y": dl.Varchar()})

    # Act & Assert
    with pytest.raises(dlexc.AlreadyExistsError, match="column with name 'y' already exists"):
        table.rename_column("x", "y")


# ---------------------------------------- REMOVE COLUMN ---------------------------------------- #


def test_remove_column(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64(), "y": dl.Varchar()})

    # Act
    table.remove_column("x")

    # Assert
    assert table.schema.columns == [dl.Column("y", dl.Varchar(), field_id=2)]


# ------------------------------------- UPDATE COLUMN DTYPE ------------------------------------- #


def test_update_column_dtype(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int32()})

    # Act
    table.update_column_dtype("x", dl.Int64())

    # Assert
    assert table.schema.columns == [dl.Column("x", dl.Int64(), field_id=1)]


def test_update_column_dtype_struct(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"s": dl.Struct({"a": dl.Int64()})})

    # Act
    table.update_column_dtype("s", dl.Struct({"a": dl.Int64(), "b": dl.Varchar()}))

    # Assert
    assert table.schema.columns == [
        dl.Column(
            "s",
            dl.Struct(
                [
                    dl.Column("a", dl.Int64(), field_id=2),
                    dl.Column("b", dl.Varchar(), field_id=3),
                ]
            ),
            field_id=1,
        ),
    ]


def test_update_nested_column_dtype(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"s": dl.Struct({"a": dl.Int32()})})

    # Act
    table.update_column_dtype("s", dl.Struct({"a": dl.Int64()}))

    # Assert
    assert table.schema.columns == [
        dl.Column(
            "s",
            dl.Struct([dl.Column("a", dl.Int64(), field_id=2)]),
            field_id=1,
        ),
    ]


# ------------------------------------ UPDATE COLUMN DEFAULT ------------------------------------ #


@pytest.mark.parametrize("default_value", [42, ("duckdb", "100"), None])
def test_update_column_default(
    shared_ducklake: dl.Ducklake,
    random_table_name: str,
    default_value: int | tuple[str, str] | None,
) -> None:
    # Arrange
    table = shared_ducklake.create_table(
        random_table_name,
        [dl.Column("x", dl.Int64(), default_value=7)],
    )

    # Act
    table.update_column_default("x", default_value)

    # Assert
    assert table.schema.columns == [
        dl.Column("x", dl.Int64(), field_id=1, default_value=default_value),
    ]


# ---------------------------------- UPDATE COLUMN NULLABILITY ---------------------------------- #


@pytest.mark.parametrize("nullable", [True, False])
def test_update_column_nullability(
    shared_ducklake: dl.Ducklake, random_table_name: str, nullable: bool
) -> None:
    # Arrange
    table = shared_ducklake.create_table(
        random_table_name,
        [dl.Column("x", dl.Int64(), nullable=not nullable)],
    )

    # Act
    table.update_column_nullability("x", nullable)

    # Assert
    assert table.schema.columns == [
        dl.Column("x", dl.Int64(), nullable=nullable, field_id=1),
    ]


# ---------------------------------------- UPDATE SCHEMA ---------------------------------------- #


def test_update_schema(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int32(), "y": dl.Varchar()})

    # Act
    table.update_schema({"x": dl.Int64(), "z": dl.Float64()})

    # Assert
    assert table.schema.columns == [
        dl.Column("x", dl.Int64(), field_id=1),
        dl.Column("z", dl.Float64(), field_id=3),
    ]


# ----------------------------------------- UPDATE TAGS ----------------------------------------- #


def test_add_column_tag(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    table.add_column_tag("x", "comment", "team-a")

    # Assert
    assert table.schema.columns == [
        dl.Column("x", dl.Int64(), field_id=1, tags={"comment": "team-a"}),
    ]


def test_remove_column_tag(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(
        random_table_name,
        [dl.Column("x", dl.Int64(), tags={"comment": "team-a"})],
    )

    # Act
    table.remove_column_tag("x", "comment")

    # Assert
    assert table.schema.columns == [dl.Column("x", dl.Int64(), field_id=1)]


def test_add_nested_column_tag(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"s": dl.Struct({"a": dl.Int64()})})

    # Act
    table.add_column_tag(["s", "a"], "comment", "team-a")

    # Assert
    assert table.schema.columns == [
        dl.Column(
            "s",
            dl.Struct([dl.Column("a", dl.Int64(), field_id=2, tags={"comment": "team-a"})]),
            field_id=1,
        ),
    ]


# ----------------------------------------- TABLE TAGS ------------------------------------------ #


def test_add_table_tag(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    table.add_tag("env", "prod")

    # Assert
    assert table.tags == {"env": "prod"}


def test_remove_table_tag(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(
        random_table_name, {"x": dl.Int64()}, tags={"env": "prod"}
    )

    # Act
    table.remove_tag("env")

    # Assert
    assert table.tags == {}


def test_remove_missing_table_tag_raises(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act & Assert
    with pytest.raises(dlexc.NotFoundError):
        table.remove_tag("missing")


def test_remove_missing_column_tag_raises(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act & Assert
    with pytest.raises(dlexc.NotFoundError):
        table.remove_column_tag("x", "missing")


# -------------------------------------- UPDATE PARTITIONING ------------------------------------ #


def test_update_partitioning_set(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64(), "y": dl.Varchar()})

    # Act
    table.update_partitioning(dl.Partitioning("x"))

    # Assert
    assert table.partitioning is not None
    assert [c.name for c in table.partitioning.columns] == ["x"]


def test_update_partitioning_reset(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()}, partition_by="x")
    assert table.partitioning is not None

    # Act
    table.update_partitioning(None)

    # Assert
    assert table.partitioning is None
