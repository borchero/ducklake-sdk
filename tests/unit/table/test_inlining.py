import datetime as dt
import decimal

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ducklake as dl

pytestmark = pytest.mark.skip_config(
    catalog="mysql", reason="Data inlining is not yet supported for MySQL."
)


@pytest.mark.parametrize(
    ("dl_dtype", "pl_dtype", "values"),
    [
        (dl.Boolean(), pl.Boolean, [True, False, None]),
        (dl.Int8(), pl.Int8, [-1, 0, 1, None]),
        (dl.Int16(), pl.Int16, [-1, 0, 1, None]),
        (dl.Int32(), pl.Int32, [-1, 0, 1, None]),
        (dl.Int64(), pl.Int64, [-1, 0, 1, None]),
        (dl.UInt8(), pl.UInt8, [0, 1, 2, None]),
        (dl.UInt16(), pl.UInt16, [0, 1, 2, None]),
        (dl.UInt32(), pl.UInt32, [0, 1, 2, None]),
        (dl.UInt64(), pl.UInt64, [0, 1, 2, None]),
        (dl.Float32(), pl.Float32, [-1.5, 0.0, 1.5, None]),
        (dl.Float64(), pl.Float64, [-1.5, 0.0, 1.5, None]),
        (
            dl.Decimal(10, 2),
            pl.Decimal(10, 2),
            [decimal.Decimal("1.23"), decimal.Decimal("-4.56"), None],
        ),
        (dl.Date(), pl.Date, [dt.date(2020, 1, 1), dt.date(2021, 6, 30), None]),
        (dl.Time(), pl.Time, [dt.time(0, 0, 0), dt.time(12, 30, 45), None]),
        (
            dl.Timestamp(),
            pl.Datetime("us"),
            [dt.datetime(2020, 1, 1, 12, 30, 45), dt.datetime(2021, 6, 30), None],
        ),
        (
            dl.TimestampTz(),
            pl.Datetime("us", "UTC"),
            [
                dt.datetime(2020, 1, 1, 12, 30, 45, tzinfo=dt.timezone.utc),
                dt.datetime(2021, 6, 30, tzinfo=dt.timezone.utc),
                None,
            ],
        ),
        (dl.Varchar(), pl.String, ["foo", "bar", None]),
        (dl.Blob(), pl.Binary, [b"foo", b"bar", None]),
        (dl.Json(), pl.String, ['{"a":1}', '{"b":2}', None]),
        (dl.List(dl.Int64()), pl.List(pl.Int64), [[1, 2, 3], [], None]),
        (
            dl.Struct({"a": dl.Int64(), "b": dl.Varchar()}),
            pl.Struct({"a": pl.Int64, "b": pl.String}),
            [{"a": 1, "b": "foo"}, {"a": 2, "b": None}, {"a": None, "b": None}],
        ),
        pytest.param(
            dl.Struct({"a": dl.Int64(), "b": dl.Varchar()}),
            pl.Struct({"a": pl.Int64, "b": pl.String}),
            [{"a": 1, "b": "foo"}, {"a": 2, "b": None}, None],
            marks=pytest.mark.skip(reason="Polars does not correctly read NULL struct elements."),
        ),
    ],
)
def test_inline(
    shared_ducklake: dl.Ducklake,
    random_table_name: str,
    dl_dtype: dl.DataType,
    pl_dtype: pl.DataType,
    values: list[object],
) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl_dtype})
    df = pl.DataFrame({"x": values}, schema={"x": pl_dtype})

    # Act
    table.write_polars(df)

    # Assert
    scan_result = table.scan()
    assert len(scan_result.data_files) == 0
    assert len(scan_result.inline_data) == 1
    assert_frame_equal(df, table.read_polars())
