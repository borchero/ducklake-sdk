from typing import Any

import pytest

import ducklake as dl
from ducklake.typedefs import _serialize_metadata_value

# ----------------------------------------- TABLE NAME ------------------------------------------ #


@pytest.mark.parametrize(
    ("schema", "name", "expected"),
    [
        ("main", "users", '"main"."users"'),
        ("with space", "name", '"with space"."name"'),
        ("schema", 'with"quote', '"schema"."with""quote"'),
        ('with"quote', 'and"quote', '"with""quote"."and""quote"'),
    ],
)
def test_table_name_str(schema: str, name: str, expected: str) -> None:
    # Arrange
    table_name = dl.TableName(schema, name)

    # Act
    actual = str(table_name)

    # Assert
    assert actual == expected


# ------------------------------------------- SCHEMA -------------------------------------------- #


def test_schema_from_mapping_equals_from_sequence() -> None:
    # Arrange
    from_mapping = dl.Schema({"x": dl.Int64(), "y": dl.Varchar()})
    from_sequence = dl.Schema([dl.Column("x", dl.Int64()), dl.Column("y", dl.Varchar())])

    # Act & Assert
    assert from_mapping == from_sequence


def test_schema_eq_non_schema() -> None:
    # Act & Assert
    assert dl.Schema({"x": dl.Int64()}) != "not a schema"


# ------------------------------------------- COLUMN -------------------------------------------- #


@pytest.mark.parametrize(
    "other",
    [
        dl.Column("y", dl.Int64()),
        dl.Column("x", dl.Varchar()),
        dl.Column("x", dl.Int64(), nullable=False),
        dl.Column("x", dl.Int64(), tags={"k": "v"}),
        dl.Column("x", dl.Int64(), initial_default=1),
        dl.Column("x", dl.Int64(), default_value=1),
        dl.Column("x", dl.Int64(), field_id=42),
    ],
)
def test_column_inequality(other: dl.Column) -> None:
    # Arrange
    base = dl.Column("x", dl.Int64())

    # Act & Assert
    assert base != other


def test_column_eq_non_column() -> None:
    # Act & Assert
    assert dl.Column("x", dl.Int64()) != "not a column"


# ------------------------------------------ DATA TYPES ----------------------------------------- #


def test_decimal_inequality() -> None:
    # Act & Assert
    assert dl.Decimal(10, 2) != dl.Decimal(10, 3)
    assert dl.Decimal(10, 2) != dl.Decimal(11, 2)
    assert dl.Decimal(10, 2) != dl.Int64()


def test_timestamp_inequality_by_precision() -> None:
    # Act & Assert
    assert dl.Timestamp("microseconds") != dl.Timestamp("milliseconds")
    assert dl.Timestamp() == dl.Timestamp("microseconds")


def test_list_inequality() -> None:
    # Act & Assert
    assert dl.List(dl.Int64()) != dl.List(dl.Varchar())
    assert dl.List(dl.Int64()) != dl.Int64()


def test_struct_inequality() -> None:
    # Act & Assert
    assert dl.Struct({"a": dl.Int64()}) != dl.Struct({"a": dl.Varchar()})
    assert dl.Struct({"a": dl.Int64()}) != dl.Struct({"b": dl.Int64()})


def test_map_inequality() -> None:
    # Act & Assert
    assert dl.Map(dl.Varchar(), dl.Int64()) != dl.Map(dl.Varchar(), dl.Varchar())
    assert dl.Map(dl.Varchar(), dl.Int64()) != dl.Map(dl.Int64(), dl.Int64())


# ----------------------------------------- COMPLEX TYPES --------------------------------------- #


def test_list_inner_column_must_be_named_element() -> None:
    # Act & Assert
    with pytest.raises(ValueError, match="List inner column must be named 'element'"):
        dl.List(dl.Column("not_element", dl.Int64()))


def test_list_accepts_correctly_named_inner_column() -> None:
    # Arrange
    inner = dl.Column("element", dl.Int64(), default_value=5)

    # Act
    list_type = dl.List(inner)

    # Assert
    assert list_type.inner is inner


def test_map_key_column_must_be_named_key() -> None:
    # Act & Assert
    with pytest.raises(ValueError, match="Map key column must be named 'key'"):
        dl.Map(dl.Column("not_key", dl.Varchar()), dl.Int64())


def test_map_value_column_must_be_named_value() -> None:
    # Act & Assert
    with pytest.raises(ValueError, match="Map value column must be named 'value'"):
        dl.Map(dl.Varchar(), dl.Column("not_value", dl.Int64()))


def test_map_accepts_correctly_named_columns() -> None:
    # Arrange
    key = dl.Column("key", dl.Varchar())
    value = dl.Column("value", dl.Int64())

    # Act
    map_type = dl.Map(key, value)

    # Assert
    assert map_type.key is key
    assert map_type.value is value


# ----------------------------------------- PARTITIONING ---------------------------------------- #


def test_partitioning_empty_raises() -> None:
    # Act & Assert
    with pytest.raises(ValueError, match="at least one column"):
        dl.Partitioning([])


def test_partitioning_from_string() -> None:
    # Act
    partitioning = dl.Partitioning("country")

    # Assert
    assert len(partitioning.columns) == 1
    assert partitioning.columns[0].name == "country"
    assert partitioning.columns[0].transform is None


def test_partitioning_from_partition_column() -> None:
    # Arrange
    column = dl.PartitionColumn("country")

    # Act
    partitioning = dl.Partitioning(column)

    # Assert
    assert partitioning.columns == [column]


def test_partitioning_from_string_sequence() -> None:
    # Act
    partitioning = dl.Partitioning(["country", "year"])

    # Assert
    assert [c.name for c in partitioning.columns] == ["country", "year"]
    assert all(c.transform is None for c in partitioning.columns)


@pytest.mark.parametrize("num_buckets", [0, -1])
def test_partition_column_bucket_requires_positive_num_buckets(num_buckets: int) -> None:
    # Act & Assert
    with pytest.raises(ValueError, match="`num_buckets` to be a positive integer"):
        dl.PartitionColumn("user_id", transform="bucket", num_buckets=num_buckets)


# ---------------------------------- _serialize_metadata_value ---------------------------------- #


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (True, "true"),
        (False, "false"),
        (42, "42"),
        (3.14, "3.14"),
        ("hello", "hello"),
        (None, None),
    ],
)
def test_serialize_metadata_value(value: Any, expected: str | None) -> None:  # noqa: ANN401
    # Act
    actual = _serialize_metadata_value(value)

    # Assert
    assert actual == expected
