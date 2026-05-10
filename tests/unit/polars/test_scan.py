import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ducklake as dl


def test_scan_single_file(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64(), "y": dl.Varchar()})
    lf = pl.LazyFrame({"x": range(100), "y": ["foo"] * 100})
    table.sink_polars(lf)

    # Act
    scanned = table.scan_polars()

    # Assert
    assert_frame_equal(lf, scanned)


def test_read_polars(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    df = pl.DataFrame({"x": [1, 2, 3]})
    table.sink_polars(df.lazy())

    # Act
    actual = table.read_polars()

    # Assert
    assert_frame_equal(df, actual)


def test_read_polars_with_file_paths(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.sink_polars(pl.LazyFrame({"x": [1, 2, 3]}))

    # Act
    df = table.read_polars(include_file_paths="path")

    # Assert
    assert "path" in df.columns
    assert df.height == 3
    assert df["path"].n_unique() == 1


@pytest.mark.skip_config(catalog="mysql", reason="Data inlining is not yet supported for MySQL.")
def test_scan_multi_file_and_inline(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64(), "y": dl.Varchar()})
    num_inline_rows = table.metadata["data_inlining_row_limit"]
    lf = pl.LazyFrame({"x": range(num_inline_rows * 2), "y": ["foo"] * num_inline_rows * 2})
    df = pl.DataFrame({"x": range(num_inline_rows), "y": ["bar"] * num_inline_rows})
    table.sink_polars(lf)
    table.sink_polars(lf)
    table.write_polars(df)

    # Act
    scanned = table.scan_polars()

    # Assert
    all_data = pl.concat([lf, lf, df.lazy()])
    assert_frame_equal(all_data, scanned)


# --------------------------------------- INITIAL DEFAULTS -------------------------------------- #


def test_initial_defaults(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    lf = pl.LazyFrame({"x": [1, 2, 3]})

    # Act
    table.sink_polars(lf)
    table.add_column(dl.Column("y", dl.Int64(), initial_default=42))
    table.add_column(dl.Column("z", dl.Int64()))
    table.sink_polars(lf.with_columns(y=pl.lit(0, dtype=pl.Int64), z=pl.lit(0, dtype=pl.Int64)))

    # Assert
    expected = pl.LazyFrame(
        {
            "x": [1, 2, 3, 1, 2, 3],
            "y": [42, 42, 42, 0, 0, 0],
            "z": [None, None, None, 0, 0, 0],
        }
    )
    assert_frame_equal(expected, table.scan_polars(), check_row_order=False)


@pytest.mark.skip_config(catalog="mysql", reason="Data inlining is not yet supported for MySQL.")
def test_initial_defaults_inline(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    df = pl.DataFrame({"x": [1, 2, 3]})

    # Act
    table.write_polars(df)
    table.add_column(dl.Column("y", dl.Int64(), initial_default=42))
    table.add_column(dl.Column("z", dl.Int64()))
    table.sink_polars(
        df.lazy().with_columns(y=pl.lit(0, dtype=pl.Int64), z=pl.lit(0, dtype=pl.Int64))
    )

    # Assert
    expected = pl.LazyFrame(
        {
            "x": [1, 2, 3, 1, 2, 3],
            "y": [42, 42, 42, 0, 0, 0],
            "z": [None, None, None, 0, 0, 0],
        }
    )
    assert_frame_equal(expected, table.scan_polars(), check_row_order=False)


@pytest.mark.skip_config(catalog="mysql", reason="Data inlining is not yet supported for MySQL.")
def test_initial_defaults_inline_only(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    df = pl.DataFrame({"x": [1, 2, 3]})

    # Act
    table.write_polars(df)
    table.add_column(dl.Column("y", dl.Int64(), initial_default=42))
    table.add_column(dl.Column("z", dl.Int64()))
    table.write_polars(df.with_columns(y=pl.lit(0, dtype=pl.Int64), z=pl.lit(0, dtype=pl.Int64)))

    # Assert
    expected = pl.LazyFrame(
        {
            "x": [1, 2, 3, 1, 2, 3],
            "y": [42, 42, 42, 0, 0, 0],
            "z": [None, None, None, 0, 0, 0],
        }
    )
    assert_frame_equal(expected, table.scan_polars(), check_row_order=False)
