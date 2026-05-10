import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ducklake as dl
import ducklake.exceptions as dlexc


def test_automatic_resolution_concurrent_schema_change(
    shared_ducklake: dl.Ducklake, random_schema_name: str
) -> None:
    # Arrange
    current_snapshot_id = shared_ducklake.get_latest_snapshot().id

    # Act
    with shared_ducklake.transaction() as tx1:
        tx1.create_schema(random_schema_name)
        with shared_ducklake.transaction() as tx2:
            tx2.create_schema(random_schema_name + "_inner")

    # Assert
    assert shared_ducklake.get_latest_snapshot().id == current_snapshot_id + 2


@pytest.mark.skip_config(catalog="mysql", reason="Data inlining is not yet supported for MySQL.")
def test_automatic_resolution_concurrent_write(
    shared_ducklake: dl.Ducklake, random_table_name: str, random_schema_name: str
) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    df = pl.DataFrame({"x": [1, 2, 3]})

    # Act
    with shared_ducklake.transaction() as tx1:
        tx1.table(random_table_name).write_polars(df)
        with shared_ducklake.transaction() as tx2:
            tx2.table(random_table_name).write_polars(df)

    # Assert
    expected = pl.concat([df, df])
    assert_frame_equal(expected, shared_ducklake.get_table(random_table_name).read_polars())


def test_automatic_resolution_with_true_conflict_and_inline_data_table_conflict(
    shared_ducklake: dl.Ducklake, random_table_name: str, random_schema_name: str
) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    with pytest.raises(dlexc.TransactionConflictError):
        with shared_ducklake.transaction() as tx1:
            tx1.create_schema(random_schema_name)
            tx1.table(random_table_name).add_column(dl.Column("y", dl.Varchar()))
            with shared_ducklake.transaction() as tx2:
                tx2.create_schema(random_schema_name)
                tx2.table(random_table_name).add_column(dl.Column("z", dl.Varchar()))


def test_conflict_concurrent_create_same_schema(
    shared_ducklake: dl.Ducklake, random_schema_name: str
) -> None:
    # Act / Assert
    with pytest.raises(dlexc.TransactionConflictError):
        with shared_ducklake.transaction() as tx1:
            tx1.create_schema(random_schema_name)
            with shared_ducklake.transaction() as tx2:
                tx2.create_schema(random_schema_name)


def test_conflict_concurrent_drop_same_schema(
    shared_ducklake: dl.Ducklake, random_schema_name: str
) -> None:
    # Arrange
    shared_ducklake.create_schema(random_schema_name)

    # Act / Assert
    with pytest.raises(dlexc.TransactionConflictError):
        with shared_ducklake.transaction() as tx1:
            tx1.delete_schema(random_schema_name)
            with shared_ducklake.transaction() as tx2:
                tx2.delete_schema(random_schema_name)


def test_conflict_drop_schema_and_create_table_in_it(
    shared_ducklake: dl.Ducklake, random_schema_name: str, random_table_name: str
) -> None:
    # Arrange
    shared_ducklake.create_schema(random_schema_name)

    # Act / Assert
    with pytest.raises(dlexc.TransactionConflictError):
        with shared_ducklake.transaction() as tx1:
            tx1.delete_schema(random_schema_name)
            with shared_ducklake.transaction() as tx2:
                tx2.create_table((random_schema_name, random_table_name), {"x": dl.Int64()})


def test_conflict_create_table_and_drop_schema(
    shared_ducklake: dl.Ducklake, random_schema_name: str, random_table_name: str
) -> None:
    # Arrange
    shared_ducklake.create_schema(random_schema_name)

    # Act / Assert
    with pytest.raises(dlexc.TransactionConflictError):
        with shared_ducklake.transaction() as tx1:
            tx1.create_table((random_schema_name, random_table_name), {"x": dl.Int64()})
            with shared_ducklake.transaction() as tx2:
                tx2.delete_schema(random_schema_name)


def test_conflict_concurrent_create_same_table(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Act / Assert
    with pytest.raises(dlexc.TransactionConflictError):
        with shared_ducklake.transaction() as tx1:
            tx1.create_table(random_table_name, {"x": dl.Int64()})
            with shared_ducklake.transaction() as tx2:
                tx2.create_table(random_table_name, {"y": dl.Varchar()})


def test_conflict_concurrent_drop_same_table(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act / Assert
    with pytest.raises(dlexc.TransactionConflictError):
        with shared_ducklake.transaction() as tx1:
            tx1.table(random_table_name).delete()
            with shared_ducklake.transaction() as tx2:
                tx2.table(random_table_name).delete()


def test_conflict_concurrent_alter_same_table(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act / Assert
    with pytest.raises(dlexc.TransactionConflictError):
        with shared_ducklake.transaction() as tx1:
            tx1.table(random_table_name).add_column(dl.Column("y", dl.Varchar()))
            with shared_ducklake.transaction() as tx2:
                tx2.table(random_table_name).add_column(dl.Column("z", dl.Varchar()))


def test_conflict_alter_then_drop_same_table(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act / Assert
    with pytest.raises(dlexc.TransactionConflictError):
        with shared_ducklake.transaction() as tx1:
            tx1.table(random_table_name).add_column(dl.Column("y", dl.Varchar()))
            with shared_ducklake.transaction() as tx2:
                tx2.table(random_table_name).delete()


@pytest.mark.skip_config(catalog="mysql", reason="Data inlining is not yet supported for MySQL.")
def test_conflict_insert_then_drop_same_table(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    df = pl.DataFrame({"x": [1, 2, 3]})

    # Act / Assert
    with pytest.raises(dlexc.TransactionConflictError):
        with shared_ducklake.transaction() as tx1:
            tx1.table(random_table_name).write_polars(df)
            with shared_ducklake.transaction() as tx2:
                tx2.table(random_table_name).delete()


@pytest.mark.skip_config(catalog="mysql", reason="Data inlining is not yet supported for MySQL.")
def test_conflict_insert_then_alter_same_table(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    df = pl.DataFrame({"x": [1, 2, 3]})

    # Act / Assert
    with pytest.raises(dlexc.TransactionConflictError):
        with shared_ducklake.transaction() as tx1:
            tx1.table(random_table_name).write_polars(df)
            with shared_ducklake.transaction() as tx2:
                tx2.table(random_table_name).add_column(dl.Column("y", dl.Varchar()))
