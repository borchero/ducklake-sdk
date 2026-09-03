from typing import cast

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ducklake as dl

pytestmark = pytest.mark.skip_config(
    catalog="mysql", reason="The DuckDB MySQL connector is unreliable."
)


def _setup_view(
    ducklake: dl.Ducklake, table_name: str, view_name: str
) -> tuple[dl.View, pl.DataFrame]:
    table = ducklake.create_table(table_name, {"x": dl.Int64()})
    table.sink_polars(pl.LazyFrame({"x": [1, 2, 3, 4]}, schema={"x": pl.Int64}))
    view = ducklake.create_view(view_name, f"SELECT x FROM {table_name} WHERE x > 2")
    return view, pl.DataFrame({"x": [3, 4]}, schema={"x": pl.Int64})


def test_read_arrow(
    shared_ducklake: dl.Ducklake, random_table_name: str, random_view_name: str
) -> None:
    # Arrange
    view, expected = _setup_view(shared_ducklake, random_table_name, random_view_name)

    # Act
    actual = cast(pl.DataFrame, pl.from_arrow(view.read_arrow()))

    # Assert
    assert_frame_equal(actual, expected)


def test_scan_duckdb(
    shared_ducklake: dl.Ducklake, random_table_name: str, random_view_name: str
) -> None:
    # Arrange
    view, expected = _setup_view(shared_ducklake, random_table_name, random_view_name)

    # Act
    actual = view.scan_duckdb().pl()

    # Assert
    assert_frame_equal(actual, expected)


def test_read_polars(
    shared_ducklake: dl.Ducklake, random_table_name: str, random_view_name: str
) -> None:
    # Arrange
    view, expected = _setup_view(shared_ducklake, random_table_name, random_view_name)

    # Act
    actual = view.read_polars()

    # Assert
    assert_frame_equal(actual, expected)


def test_read_polars_with_column_aliases(
    shared_ducklake: dl.Ducklake, random_table_name: str, random_view_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.sink_polars(pl.LazyFrame({"x": [1, 2, 3]}, schema={"x": pl.Int64}))
    view = shared_ducklake.create_view(
        random_view_name,
        f"SELECT x FROM {random_table_name}",
        column_aliases=["renamed x"],
    )

    # Act
    actual = view.read_polars()

    # Assert
    assert actual.columns == ["renamed x"]


def test_read_polars_with_partial_column_aliases(
    shared_ducklake: dl.Ducklake, random_table_name: str, random_view_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64(), "y": dl.Int64()})
    table.sink_polars(pl.LazyFrame({"x": [1], "y": [2]}, schema={"x": pl.Int64, "y": pl.Int64}))
    view = shared_ducklake.create_view(
        random_view_name,
        f"SELECT x, y FROM {random_table_name}",
        column_aliases=["renamed x"],
    )
    expected = pl.DataFrame(
        {"renamed x": [1], "y": [2]}, schema={"renamed x": pl.Int64, "y": pl.Int64}
    )

    # Act
    actual = view.read_polars()

    # Assert
    assert_frame_equal(actual, expected)


def test_read_polars_matches_duckdb(
    shared_ducklake: dl.Ducklake, random_table_name: str, random_view_name: str
) -> None:
    # Arrange
    view, _ = _setup_view(shared_ducklake, random_table_name, random_view_name)

    # Act: the Polars SQLContext path must agree with the DuckDB path.
    polars_result = view.read_polars()
    duckdb_result = view.scan_duckdb().pl()

    # Assert
    assert_frame_equal(polars_result, duckdb_result)


def test_read_polars_with_qualified_table_reference(
    shared_ducklake: dl.Ducklake,
    random_schema_name: str,
    random_table_name: str,
    random_view_name: str,
) -> None:
    # Arrange
    shared_ducklake.create_schema(random_schema_name)
    table_name = dl.TableName(random_schema_name, random_table_name)
    table = shared_ducklake.create_table(table_name, {"x": dl.Int64()})
    table.sink_polars(pl.LazyFrame({"x": [1, 2, 3]}, schema={"x": pl.Int64}))
    view = shared_ducklake.create_view(random_view_name, f"SELECT x FROM {table_name} WHERE x > 1")
    expected = pl.DataFrame({"x": [2, 3]}, schema={"x": pl.Int64})

    # Act
    actual = view.read_polars()

    # Assert
    assert_frame_equal(actual, expected)


def test_read_polars_with_unqualified_table_in_view_schema(
    shared_ducklake: dl.Ducklake,
    random_schema_name: str,
    random_table_name: str,
    random_view_name: str,
) -> None:
    # Arrange
    shared_ducklake.create_schema(random_schema_name)
    table = shared_ducklake.create_table(
        (random_schema_name, random_table_name), {"x": dl.Int64()}
    )
    table.sink_polars(pl.LazyFrame({"x": [1]}, schema={"x": pl.Int64}))
    view = shared_ducklake.create_view(
        (random_schema_name, random_view_name), f"SELECT x FROM {random_table_name}"
    )
    expected = pl.DataFrame({"x": [1]}, schema={"x": pl.Int64})

    # Act
    actual = view.read_polars()

    # Assert
    assert_frame_equal(actual, expected)


def test_read_polars_with_nested_view(
    shared_ducklake: dl.Ducklake, random_table_name: str, random_view_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.sink_polars(pl.LazyFrame({"x": [1, 2]}, schema={"x": pl.Int64}))
    inner_view_name = random_view_name + "_inner"
    shared_ducklake.create_view(inner_view_name, f"SELECT x FROM {random_table_name}")
    view = shared_ducklake.create_view(
        random_view_name, f"SELECT x FROM {inner_view_name} WHERE x > 1"
    )
    expected = pl.DataFrame({"x": [2]}, schema={"x": pl.Int64})

    # Act
    actual = view.read_polars()

    # Assert
    assert_frame_equal(actual, expected)


def test_read_polars_with_cte(
    shared_ducklake: dl.Ducklake, random_table_name: str, random_view_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.sink_polars(pl.LazyFrame({"x": [1, 2, 3]}, schema={"x": pl.Int64}))
    view = shared_ducklake.create_view(
        random_view_name,
        f"WITH recent AS (SELECT x FROM {random_table_name}) SELECT x FROM recent WHERE x > 1",
    )
    expected = pl.DataFrame({"x": [2, 3]}, schema={"x": pl.Int64})

    # Act
    actual = view.read_polars()

    # Assert
    assert_frame_equal(actual, expected)
