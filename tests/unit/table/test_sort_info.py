import pytest

import ducklake as dl
import ducklake.exceptions as dlexc


def test_sort_info_lifecycle(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(
        random_table_name,
        {"x": dl.Int64(), "y": dl.Varchar()},
        sort_by=dl.SortInfo(dl.SortColumn("x", direction="descending", null_order="nulls_first")),
    )
    initial_snapshot = shared_ducklake.get_latest_snapshot().id

    # Act
    with shared_ducklake.transaction() as tx:
        tx_table = tx.table(random_table_name)
        tx_table.update_sort_info(dl.SortInfo("y"))
        in_transaction = tx_table.sort_info
    updated_snapshot = shared_ducklake.get_latest_snapshot().id
    table.update_sort_info(None)
    initial = shared_ducklake.at(initial_snapshot).table(random_table_name).sort_info
    updated = shared_ducklake.at(updated_snapshot).table(random_table_name).sort_info

    # Assert
    assert initial is not None
    assert [(c.expression, c.direction, c.null_order) for c in initial.columns] == [
        ("x", "descending", "nulls_first")
    ]
    assert in_transaction is not None
    assert [c.expression for c in in_transaction.columns] == ["y"]
    assert updated is not None
    assert [c.expression for c in updated.columns] == ["y"]
    assert table.sort_info is None


def test_sort_info_uses_renamed_column_in_transaction(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(
        random_table_name,
        {"x": dl.Int64(), "y": dl.Int64()},
        sort_by=dl.SortInfo(["x", "y"]),
    )

    # Act
    with shared_ducklake.transaction() as tx:
        tx_table = tx.table(random_table_name)
        tx_table.rename_column("x", "z")
        in_transaction = tx_table.sort_info
    persisted = table.sort_info

    # Assert
    assert in_transaction is not None
    assert [column.expression for column in in_transaction.columns] == ["z", "y"]
    assert persisted is not None
    assert [column.expression for column in persisted.columns] == ["z", "y"]


def test_sort_info_rejects_unknown_column(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    sort = dl.SortInfo("missing")

    # Act
    with pytest.raises(dlexc.NotFoundError, match="column referenced by sort expression"):
        shared_ducklake.create_table(random_table_name, {"x": dl.Int64()}, sort_by=sort)

    # Assert
    assert not shared_ducklake.has_table(random_table_name)


def test_sorted_column_cannot_be_dropped(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(
        random_table_name,
        {"x": dl.Int64(), "y": dl.Int64()},
        sort_by=dl.SortInfo("x"),
    )

    # Act
    with pytest.raises(ValueError, match="sorted by it"):
        table.remove_column("x")
    table.update_sort_info(None)
    table.remove_column("x")

    # Assert
    assert [column.name for column in table.schema.columns] == ["y"]
