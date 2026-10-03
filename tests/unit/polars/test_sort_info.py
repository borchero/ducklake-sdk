import polars as pl
import pytest

import ducklake as dl


@pytest.mark.parametrize("eager", [False, True])
def test_sort_info_applied_to_parquet(
    shared_ducklake: dl.Ducklake, random_table_name: str, eager: bool
) -> None:
    # Arrange
    table = shared_ducklake.create_table(
        random_table_name,
        {"x": dl.Int64(), "y": dl.Int64()},
        sort_by=dl.SortInfo(
            [
                dl.SortColumn("x", direction="descending", null_order="nulls_first"),
                dl.SortColumn("y"),
            ]
        ),
    )
    table.set_metadata(data_inlining_row_limit=0)
    data = pl.DataFrame({"x": [1, None, 2, 1, None], "y": [2, 2, 1, 1, 1]})

    # Act
    if eager:
        table.write_polars(data)
    else:
        table.sink_polars(data.lazy())
    files = table.scan().data_files

    # Assert
    assert len(files) == 1
    assert pl.read_parquet(files[0].path).to_dict(as_series=False) == {
        "x": [None, None, 2, 1, 1],
        "y": [1, 2, 1, 1, 2],
    }


@pytest.mark.skip_config(catalog="mysql", reason="Data inlining is not yet supported for MySQL.")
def test_sort_info_applied_to_inline_data(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(
        random_table_name, {"x": dl.Int64()}, sort_by=dl.SortInfo("x")
    )

    # Act
    table.write_polars(pl.DataFrame({"x": [3, 1, 2]}))
    result = table.read_polars()

    # Assert
    assert result["x"].to_list() == [1, 2, 3]


def test_sort_info_applied_with_partitioning(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(
        random_table_name,
        {"part": dl.Int64(), "x": dl.Int64()},
        partition_by="part",
        sort_by=dl.SortInfo(dl.SortColumn("x", direction="descending")),
    )
    data = pl.LazyFrame({"part": [1, 2, 1, 2, 1, 2], "x": [1, 4, 3, 2, 2, 3]})

    # Act
    table.sink_polars(data)
    files = table.scan().data_files

    # Assert
    assert len(files) == 2
    assert sorted(pl.read_parquet(file.path)["x"].to_list() for file in files) == [
        [3, 2, 1],
        [4, 3, 2],
    ]


def test_sort_info_applied_within_transaction(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    data = pl.LazyFrame({"x": [3, 1, 2]})

    # Act
    with shared_ducklake.transaction() as tx:
        table = tx.create_table(random_table_name, {"x": dl.Int64()}, sort_by="x")
        table.sink_polars(data)
    files = shared_ducklake.table(random_table_name).scan().data_files

    # Assert
    assert len(files) == 1
    assert pl.read_parquet(files[0].path)["x"].to_list() == [1, 2, 3]
