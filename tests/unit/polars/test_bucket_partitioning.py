import datetime as dt
import re

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ducklake as dl


@pytest.mark.parametrize(
    ("dtype", "values", "expected_buckets"),
    [
        pytest.param(dl.Int64(), [34, 35, 36, 37], {3, 5, 6}, id="int64"),
        pytest.param(dl.Varchar(), ["iceberg"], {1}, id="string"),
        pytest.param(dl.Blob(), [bytes([0, 1, 2, 3])], {1}, id="binary"),
        pytest.param(dl.Boolean(), [True], {4}, id="boolean"),
        pytest.param(dl.Date(), [dt.date(2017, 11, 16)], {2}, id="date"),
        pytest.param(
            dl.Timestamp("microseconds"),
            [dt.datetime(2017, 11, 16, 22, 31, 8)],
            {7},
            id="timestamp",
        ),
        pytest.param(dl.Float64(), [1.0], {7}, id="float64"),
        pytest.param(
            dl.Timestamp("milliseconds"),
            [dt.datetime(2017, 11, 16, 22, 31, 8)],
            {6},
            id="timestamp-ms",
        ),
    ],
)
def test_sink_parquet_partition_bucket_matches_iceberg_test_vectors(
    shared_ducklake: dl.Ducklake,
    random_table_name: str,
    dtype: dl.DataType,
    values: list[object],
    expected_buckets: set[int],
) -> None:
    # Arrange
    table = shared_ducklake.create_table(
        random_table_name,
        {"x": dtype},
        partition_by=dl.PartitionColumn("x", transform="bucket", num_buckets=8),
    )
    pl_dtype = pl.Schema(dl.Schema({"x": dtype}))["x"]
    lf = pl.LazyFrame({"x": values}, schema={"x": pl_dtype})

    # Act
    table.sink_polars(lf)
    lf_roundtrip = table.scan_polars()

    # Assert
    scan_result = table.scan()
    assert len(scan_result.data_files) == len(expected_buckets)
    assert {
        _partition_value_from_path(f.path, "x") for f in scan_result.data_files
    } == expected_buckets
    assert_frame_equal(lf, lf_roundtrip, check_row_order=False)


def test_sink_parquet_partition_bucket_rejects_unverified_dtype(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    ducklake_dtype = dl.Decimal(9, 2)
    table = shared_ducklake.create_table(
        random_table_name,
        {"x": ducklake_dtype},
        partition_by=dl.PartitionColumn("x", transform="bucket", num_buckets=8),
    )
    pl_dtype = pl.Schema(dl.Schema({"x": ducklake_dtype}))["x"]
    lf = pl.LazyFrame({"x": [1.0]}, schema={"x": pl_dtype})

    # Act & Assert
    with pytest.raises(NotImplementedError, match="bucket"):
        table.sink_polars(lf)


# ----------------------------------------------------------------------------------------------- #
#                                              UTILS                                              #
# ----------------------------------------------------------------------------------------------- #


def _partition_value_from_path(path: str, column: str) -> int:
    match = re.search(rf"/{column}=([^/]+)/", path)
    assert match is not None, f"no `{column}=` partition segment found in path {path!r}"
    return int(match.group(1))
