import pytest

import ducklake as dl
import ducklake.exceptions as dlexc

# ----------------------------------------- RENAME TABLE ---------------------------------------- #


def test_rename_table(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    new_table_name = random_table_name + "_rename"
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).rename(new_table_name)

    # Assert
    table = shared_ducklake.get_table(new_table_name)
    assert table.name == ("main", new_table_name)


# ------------------------------------------ ADD COLUMN ----------------------------------------- #


def test_add_column(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).add_column(dl.Column("y", dl.Varchar()))

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.schema.columns == [
        dl.Column("x", dl.Int64(), field_id=1),
        dl.Column("y", dl.Varchar(), field_id=2),
    ]


# ---------------------------------------- RENAME COLUMN ---------------------------------------- #


def test_rename_column(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).rename_column("x", "y")

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.schema.columns == [dl.Column("y", dl.Int64(), field_id=1)]


def test_rename_column_already_exists(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64(), "y": dl.Varchar()})

    # Act & Assert
    with pytest.raises(dlexc.AlreadyExistsError, match="column with name 'y' already exists"):
        with shared_ducklake.transaction() as tx:
            tx.table(random_table_name).rename_column("x", "y")


# ---------------------------------------- REMOVE COLUMN ---------------------------------------- #


def test_remove_column(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64(), "y": dl.Varchar()})

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).remove_column("x")

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.schema.columns == [dl.Column("y", dl.Varchar(), field_id=2)]


# ------------------------------------- UPDATE COLUMN DTYPE ------------------------------------- #


def test_update_column_dtype(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int32()})

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).update_column_dtype("x", dl.Int64())

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.schema.columns == [dl.Column("x", dl.Int64(), field_id=1)]


def test_update_column_dtype_struct(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"s": dl.Struct({"a": dl.Int64()})})

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).update_column_dtype(
            "s", dl.Struct({"a": dl.Int64(), "b": dl.Varchar()})
        )

    # Assert
    table = shared_ducklake.get_table(random_table_name)
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
    shared_ducklake.create_table(random_table_name, {"s": dl.Struct({"a": dl.Int32()})})

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).update_column_dtype("s", dl.Struct({"a": dl.Int64()}))

    # Assert
    table = shared_ducklake.get_table(random_table_name)
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
    shared_ducklake.create_table(
        random_table_name,
        [dl.Column("x", dl.Int64(), default_value=7)],
    )

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).update_column_default("x", default_value)

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.schema.columns == [
        dl.Column("x", dl.Int64(), field_id=1, default_value=default_value),
    ]


# ---------------------------------- UPDATE COLUMN NULLABILITY ---------------------------------- #


@pytest.mark.parametrize("nullable", [True, False])
def test_update_column_nullability(
    shared_ducklake: dl.Ducklake, random_table_name: str, nullable: bool
) -> None:
    # Arrange
    shared_ducklake.create_table(
        random_table_name,
        [dl.Column("x", dl.Int64(), nullable=not nullable)],
    )

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).update_column_nullability("x", nullable)

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.schema.columns == [
        dl.Column("x", dl.Int64(), nullable=nullable, field_id=1),
    ]


# ---------------------------------------- UPDATE SCHEMA ---------------------------------------- #


def test_update_schema(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int32(), "y": dl.Varchar()})

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).update_schema(dl.Schema({"x": dl.Int64(), "z": dl.Float64()}))

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.schema.columns == [
        dl.Column("x", dl.Int64(), field_id=1),
        dl.Column("z", dl.Float64(), field_id=3),
    ]


# ----------------------------------------- UPDATE TAGS ----------------------------------------- #


def test_add_column_tag(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).add_column_tag("x", "comment", "team-a")

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.schema.columns == [
        dl.Column("x", dl.Int64(), field_id=1, tags={"comment": "team-a"}),
    ]


def test_remove_column_tag(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(
        random_table_name,
        [dl.Column("x", dl.Int64(), tags={"comment": "team-a"})],
    )

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).remove_column_tag("x", "comment")

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.schema.columns == [dl.Column("x", dl.Int64(), field_id=1)]


def test_add_nested_column_tag(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"s": dl.Struct({"a": dl.Int64()})})

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).add_column_tag(["s", "a"], "comment", "team-a")

    # Assert
    table = shared_ducklake.get_table(random_table_name)
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
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).add_tag("env", "prod")

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.tags == {"env": "prod"}


def test_remove_table_tag(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()}, tags={"env": "prod"})

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).remove_tag("env")

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.tags == {}


# -------------------------------------- UPDATE PARTITIONING ------------------------------------ #


def test_update_partitioning_set(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64(), "y": dl.Varchar()})

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).update_partitioning(dl.Partitioning("x"))

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.partitioning is not None
    assert [c.name for c in table.partitioning.columns] == ["x"]


def test_update_partitioning_reset(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()}, partition_by="x")

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).update_partitioning(None)

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.partitioning is None


# ---------------------------------- NESTED COLUMN OPERATIONS ----------------------------------- #


def test_rename_nested_column(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"s": dl.Struct({"a": dl.Int64()})})

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).rename_column(["s", "a"], "b")

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.schema.columns == [
        dl.Column("s", dl.Struct([dl.Column("b", dl.Int64(), field_id=2)]), field_id=1),
    ]


def test_remove_nested_column(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(
        random_table_name,
        {"s": dl.Struct({"a": dl.Int64(), "b": dl.Varchar()})},
    )

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).remove_column(["s", "a"])

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.schema.columns == [
        dl.Column("s", dl.Struct([dl.Column("b", dl.Varchar(), field_id=3)]), field_id=1),
    ]


def test_remove_nested_column_tag(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    shared_ducklake.create_table(
        random_table_name,
        {"s": dl.Struct([dl.Column("a", dl.Int64(), tags={"comment": "team-a"})])},
    )

    # Act
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).remove_column_tag(["s", "a"], "comment")

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.schema.columns == [
        dl.Column("s", dl.Struct([dl.Column("a", dl.Int64(), field_id=2)]), field_id=1),
    ]
