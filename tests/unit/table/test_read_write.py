from typing import cast

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ducklake as dl

pytestmark = pytest.mark.skip_config(
    catalog="mysql", reason="The DuckDB MySQL connector is unreliable."
)

# -------------------------------------------- DUCKDB ------------------------------------------- #


def test_scan_duckdb(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    num_rows = table.metadata["data_inlining_row_limit"] * 2
    lf = pl.LazyFrame({"x": range(num_rows)})

    # Act
    table.sink_polars(lf)
    roundtrip_df = table.scan_duckdb()

    # Assert
    assert_frame_equal(lf, roundtrip_df.pl(lazy=True))


@pytest.mark.skip_config(catalog="mysql", reason="Data inlining is not yet supported for MySQL.")
def test_scan_duckdb_inline(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    num_rows = table.metadata["data_inlining_row_limit"]
    df = pl.DataFrame({"x": range(num_rows)})

    # Act
    table.write_polars(df)
    roundtrip_df = table.scan_duckdb()

    # Assert
    assert_frame_equal(df, roundtrip_df.pl())


# -------------------------------------------- ARROW -------------------------------------------- #


def test_read_write_arrow(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    df = pl.DataFrame({"x": [1, 2, 3]}, schema={"x": pl.Int64})

    # Act
    table.write_arrow(df.to_arrow())
    roundtrip_df = cast(pl.DataFrame, pl.from_arrow(table.read_arrow()))

    # Assert
    assert_frame_equal(df, roundtrip_df)
