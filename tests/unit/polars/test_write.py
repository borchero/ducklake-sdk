import datetime as dt
from typing import Any

import polars as pl
import pytest
import sqlalchemy as sa
from polars.testing import assert_frame_equal

import ducklake as dl
from ducklake.typedefs import _ParamLessPartitionTransform


def test_sink_parquet(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64(), "y": dl.Varchar()})
    lf = pl.LazyFrame({"x": range(100), "y": ["foo"] * 100})

    # Act
    table.sink_polars(lf)

    # Assert
    # -- Basics
    scan_result = table.scan()
    assert len(scan_result.data_files) == 1
    assert len(scan_result.inline_data) == 0

    # -- Data file stats
    data_file = scan_result.data_files[0]
    assert len(data_file.delete_files) == 0
    assert data_file.inline_deletes is None
    assert data_file.statistics.num_rows == 100
    assert data_file.statistics.column_stats[1].min_value == 0
    assert data_file.statistics.column_stats[1].max_value == 99
    assert data_file.statistics.column_stats[1].null_count == 0
    assert data_file.statistics.column_stats[2].min_value == "foo"
    assert data_file.statistics.column_stats[2].max_value == "foo"
    assert data_file.statistics.column_stats[2].null_count == 0
    assert data_file.statistics.file_size_bytes is not None
    assert data_file.statistics.footer_size_bytes is not None
    assert data_file.statistics.file_size_bytes > data_file.statistics.footer_size_bytes > 0

    # -- Data itself
    assert_frame_equal(lf, pl.scan_parquet(data_file.path))

    # -- Table stats
    table_stats = read_table_stats(str(shared_ducklake._connection_args), random_table_name)
    assert table_stats["record_count"] == 100
    assert table_stats["next_row_id"] == 100
    assert table_stats["file_size_bytes"] == data_file.statistics.file_size_bytes

    # -- Table column stats
    table_column_stats = read_table_column_stats(
        str(shared_ducklake._connection_args), random_table_name
    )
    assert table_column_stats[1]["min_value"] == "0"
    assert table_column_stats[1]["max_value"] == "99"
    assert not table_column_stats[1]["contains_null"]
    assert table_column_stats[2]["min_value"] == "foo"
    assert table_column_stats[2]["max_value"] == "foo"
    assert not table_column_stats[2]["contains_null"]


def test_write_parquet(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64(), "y": dl.Varchar()})
    num_rows = table.metadata["data_inlining_row_limit"] * 2
    df = pl.DataFrame({"x": range(num_rows), "y": ["foo"] * num_rows})

    # Act
    table.write_polars(df)

    # Assert
    roundtrip_df = table.read_polars()
    assert_frame_equal(df, roundtrip_df)


@pytest.mark.skip_config(catalog="mysql", reason="Data inlining is not yet supported for MySQL.")
def test_write_parquet_inline(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64(), "y": dl.Varchar()})
    num_rows = table.metadata["data_inlining_row_limit"]
    df = pl.DataFrame({"x": range(num_rows), "y": ["foo"] * num_rows})

    # Act
    table.write_polars(df)

    # Assert
    # -- Basics
    scan_result = table.scan()
    assert len(scan_result.data_files) == 0
    assert len(scan_result.inline_data) == 1

    # -- Data itself
    assert_frame_equal(df, pl.DataFrame(scan_result.inline_data[0]))

    # -- Table stats
    table_stats = read_table_stats(str(shared_ducklake._connection_args), random_table_name)
    assert table_stats["record_count"] == num_rows
    assert table_stats["next_row_id"] == num_rows
    assert table_stats["file_size_bytes"] == 0

    # -- Table column stats
    table_column_stats = read_table_column_stats(
        str(shared_ducklake._connection_args), random_table_name
    )
    assert table_column_stats[1]["min_value"] == "0"
    assert table_column_stats[1]["max_value"] == f"{num_rows - 1}"
    assert not table_column_stats[1]["contains_null"]
    assert table_column_stats[2]["min_value"] == "foo"
    assert table_column_stats[2]["max_value"] == "foo"
    assert not table_column_stats[2]["contains_null"]


# ------------------------------------------ PARTITIONS ----------------------------------------- #


def test_sink_parquet_partition(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(
        random_table_name, {"x": dl.Int64(), "y": dl.Varchar()}, partition_by="x"
    )
    lf = pl.LazyFrame({"x": [1, 1, 1, 2, 2, 3], "y": ["foo"] * 6})

    # Act
    table.sink_polars(lf)
    lf_roundtrip = table.scan_polars()

    # Assert
    scan_result = table.scan()
    assert len(scan_result.data_files) == 3

    data_file_1 = [f for f in scan_result.data_files if f.statistics.num_rows == 3][0]
    assert data_file_1.statistics.column_stats[1].min_value == 1
    assert data_file_1.statistics.column_stats[1].max_value == 1

    data_file_2 = [f for f in scan_result.data_files if f.statistics.num_rows == 2][0]
    assert data_file_2.statistics.column_stats[1].min_value == 2
    assert data_file_2.statistics.column_stats[1].max_value == 2

    data_file_3 = [f for f in scan_result.data_files if f.statistics.num_rows == 1][0]
    assert data_file_3.statistics.column_stats[1].min_value == 3
    assert data_file_3.statistics.column_stats[1].max_value == 3

    assert_frame_equal(lf, lf_roundtrip, check_row_order=False)


@pytest.mark.parametrize(
    ("transform", "num_data_files"), [("year", 2), ("month", 3), ("day", 4), ("hour", 1)]
)
def test_sink_parquet_partition_year(
    shared_ducklake: dl.Ducklake,
    random_table_name: str,
    transform: _ParamLessPartitionTransform,
    num_data_files: int,
) -> None:
    # Arrange
    table = shared_ducklake.create_table(
        random_table_name,
        {"x": dl.Timestamp()},
        partition_by=dl.PartitionColumn("x", transform=transform),
    )
    lf = pl.LazyFrame(
        {
            "x": [
                dt.datetime(2020, 1, 1),
                dt.datetime(2020, 1, 2),
                dt.datetime(2021, 2, 3),
                dt.datetime(2021, 3, 4),
            ]
        }
    )

    # Act
    table.sink_polars(lf)
    lf_roundtrip = table.scan_polars()

    # Assert
    scan_result = table.scan()
    assert len(scan_result.data_files) == num_data_files
    assert_frame_equal(lf, lf_roundtrip, check_row_order=False)


# ------------------------------------------- DEFAULTS ------------------------------------------ #


def test_sink_parquet_default(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(
        random_table_name,
        [
            dl.Column("x", dl.Int64(), default_value=10),
            dl.Column("y", dl.Varchar(), default_value="foo"),
        ],
    )
    lf = pl.LazyFrame({"x": [1, 2, None], "y": [None, "3", "4"]})

    # Act
    table.sink_polars(lf)
    actual = table.scan_polars()

    # Assert
    expected = pl.LazyFrame({"x": [1, 2, 10], "y": ["foo", "3", "4"]})
    assert_frame_equal(expected, actual)


def test_sink_parquet_default_struct(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(
        random_table_name,
        [
            dl.Column(
                "y",
                dl.Struct(
                    [
                        dl.Column("a", dl.Varchar()),
                        dl.Column("b", dl.Int64(), default_value=20),
                    ]
                ),
            ),
        ],
    )
    lf = pl.LazyFrame({"y": [{"a": "a1", "b": 1}, {"a": "a2", "b": None}, {"a": None, "b": 10}]})

    # Act
    table.sink_polars(lf)
    actual = table.scan_polars()

    # Assert
    expected = pl.LazyFrame(
        {"y": [{"a": "a1", "b": 1}, {"a": "a2", "b": 20}, {"a": None, "b": 10}]}
    )
    assert_frame_equal(expected, actual)


def test_sink_parquet_default_list(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(
        random_table_name,
        [dl.Column("y", dl.List(dl.Column("element", dl.Int64(), default_value=5)))],
    )
    lf = pl.LazyFrame({"y": [[1, 2, None], [None, 3, 4], None]})

    # Act
    table.sink_polars(lf)
    actual = table.scan_polars()

    # Assert
    expected = pl.LazyFrame({"y": [[1, 2, 5], [5, 3, 4], None]})
    assert_frame_equal(expected, actual)


@pytest.mark.skip_config(catalog="mysql", reason="Data inlining is not yet supported for MySQL.")
def test_write_parquet_default_list(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(
        random_table_name,
        [dl.Column("y", dl.List(dl.Column("element", dl.Int64(), default_value=5)))],
    )
    df = pl.DataFrame({"y": [[1, 2, None], [None, 3, 4], None]})

    # Act
    table.write_polars(df)
    actual = table.scan_polars()

    # Assert
    expected = pl.LazyFrame({"y": [[1, 2, 5], [5, 3, 4], None]})
    assert_frame_equal(expected, actual)


# ------------------------------------------ MANY FILES ----------------------------------------- #


def test_sink_many_tiny_files(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    """This test is meant to verify that inserting into a table succeeds when the number of file
    column stats causes a naïve query to fail due to exceeding the maximum number of bind
    parameters."""
    # Arrange
    num_columns = 100
    num_files = 100_000 // num_columns
    table = shared_ducklake.create_table(
        random_table_name, {f"c{i}": dl.Int64() for i in range(num_columns)}
    )
    # Force one tiny file per row by minimizing the row group size and the target file size.
    table.set_metadata(parquet_row_group_size=1, target_file_size=1)
    lf = pl.LazyFrame({f"c{i}": range(num_files) for i in range(num_columns)})

    # Act
    table.sink_polars(lf)

    # Assert
    scan_result = table.scan()
    assert len(scan_result.data_files) == num_files
    assert_frame_equal(lf, table.scan_polars(), check_row_order=False)


# ----------------------------------------------------------------------------------------------- #
#                                              UTILS                                              #
# ----------------------------------------------------------------------------------------------- #


def read_table_stats(url: str, table: str) -> dict[str, Any]:
    engine = sa.create_engine(url)
    query = f"""
        SELECT ducklake_table_stats.*
        FROM ducklake_table_stats
        JOIN ducklake_table
            ON ducklake_table_stats.table_id = ducklake_table.table_id
        WHERE ducklake_table.table_name = '{table}'
    """
    try:
        with engine.connect() as conn:
            row = conn.execute(sa.text(query)).mappings().first()
            assert row is not None
            return dict(row)
    finally:
        engine.dispose()


def read_table_column_stats(url: str, table: str) -> dict[int, dict[str, Any]]:
    engine = sa.create_engine(url)
    query = f"""
        SELECT ducklake_table_column_stats.*
        FROM ducklake_table_column_stats
        JOIN ducklake_table
            ON ducklake_table_column_stats.table_id = ducklake_table.table_id
        WHERE ducklake_table.table_name = '{table}'
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(sa.text(query)).mappings().all()
            return {
                row["column_id"]: {k: v for k, v in row.items() if k != "column_id"}
                for row in rows
            }
    finally:
        engine.dispose()
