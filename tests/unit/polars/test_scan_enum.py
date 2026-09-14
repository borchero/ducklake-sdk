import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ducklake as dl


@pytest.fixture(
    params=["enum-mismatched-order", "enum-lexicographic-order", "categorical-mismatched-order"]
)
def fruit_table(
    request: pytest.FixtureRequest, shared_ducklake: dl.Ducklake, random_table_name: str
) -> tuple[dl.Table, pl.DataFrame]:
    categories = ["pear", "apple", "banana"]
    if request.param == "categorical-mismatched-order":
        dtype = pl.Categorical(pl.Categories(random_table_name))
    else:
        if request.param == "enum-lexicographic-order":
            categories = sorted(categories)
        dtype = pl.Enum(categories)
    # A separate category mapping makes the categorical codes independent of other tests.
    fruit = pl.Series("fruit", categories + ["banana", "apple", None], dtype=dtype)
    df = fruit.to_frame()
    table = shared_ducklake.create_table(random_table_name, df.to_arrow().schema)
    table.sink_polars(df.lazy())
    return table, df


@pytest.mark.parametrize("value", ["apple", "banana", "pear"])
@pytest.mark.parametrize("typed_literal", [False, True], ids=["string-literal", "typed-literal"])
@pytest.mark.parametrize("comparison", ["eq", "ne", "lt", "le", "gt", "ge"])
def test_scan_dictionary_column_filter_pushdown(
    fruit_table: tuple[dl.Table, pl.DataFrame],
    value: str,
    typed_literal: bool,
    comparison: str,
) -> None:
    # Arrange
    table, df = fruit_table
    literal = pl.lit(value, dtype=df.schema["fruit"] if typed_literal else pl.String)
    predicate = getattr(pl.col("fruit"), comparison)(literal)
    expected = df.filter(predicate)

    # Act
    scanned = table.scan_polars()
    actual = scanned.filter(predicate).collect()

    # Assert
    assert scanned.collect_schema() == df.schema
    assert_frame_equal(actual, expected)
