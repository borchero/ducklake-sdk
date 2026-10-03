from contextlib import nullcontext

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ducklake as dl

pytestmark = pytest.mark.skip_config(
    catalog="mysql", reason="Data inlining is not yet supported for MySQL."
)


@pytest.fixture()
def boundary_frame(catalog: str) -> pl.DataFrame:
    max_columns = {"sqlite": 1997, "postgres": 1597}[catalog]
    return pl.DataFrame({f"c{i}": pl.Series([i], dtype=pl.Int32) for i in range(max_columns)})


@pytest.fixture()
def boundary_table(ducklake: dl.Ducklake, boundary_frame: pl.DataFrame) -> dl.Table:
    return ducklake.create_table("boundary", dl.Schema(boundary_frame.schema))


# -------------------------------------------- TESTS -------------------------------------------- #


@pytest.mark.parametrize("transactional", [False, True])
@pytest.mark.parametrize("too_wide", [False, True])
def test_write_at_column_limit(
    ducklake: dl.Ducklake, boundary_frame: pl.DataFrame, transactional: bool, too_wide: bool
) -> None:
    # Arrange
    data = boundary_frame.with_columns(extra=pl.lit(1)) if too_wide else boundary_frame

    # Act
    with ducklake.transaction() if transactional else nullcontext(ducklake) as writer:
        table = writer.create_table("table", dl.Schema(data.schema))
        with (
            pytest.warns(UserWarning, match="Writing DataFrame to Parquet")
            if too_wide
            else nullcontext()
        ):
            table.write_polars(data)

    # Assert
    table = ducklake.table("table")
    assert table._get_write_info()[2] is not too_wide
    scan = table.scan()
    assert len(scan.data_files) == int(too_wide)
    assert len(scan.inline_data) == int(not too_wide)
    assert_frame_equal(table.read_polars(), data)


def test_inlining_support_tracks_schema(boundary_table: dl.Table) -> None:
    # Arrange
    extra = dl.Column("extra", dl.Int64())

    # Act
    boundary_table.add_column(extra)
    widened_support = boundary_table._get_write_info()[2]
    boundary_table.remove_column("extra")

    # Assert
    assert not widened_support
    assert boundary_table._get_write_info()[2]


@pytest.mark.parametrize("widen_first", [False, True])
def test_inline_write_rejects_wide_schema(
    ducklake: dl.Ducklake,
    boundary_table: dl.Table,
    boundary_frame: pl.DataFrame,
    widen_first: bool,
) -> None:
    # Arrange
    data = boundary_frame.with_columns(extra=pl.lit(1)) if widen_first else boundary_frame

    # Act
    with pytest.raises(ValueError, match="cannot inline data"):
        with ducklake.transaction() as tx:
            table = tx.table("boundary")
            if widen_first:
                table.add_column(dl.Column("extra", dl.Int64()))
            table._write_inline_data(data)
            if not widen_first:
                table.add_column(dl.Column("extra", dl.Int64()))

    # Assert
    assert boundary_table._get_write_info()[2]
    assert boundary_table.read_polars().is_empty()
