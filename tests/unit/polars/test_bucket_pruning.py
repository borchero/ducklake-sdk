import datetime as dt
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from urllib.parse import unquote, urlparse

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ducklake as dl
from ducklake.polars.scan import _literal_buckets
from ducklake.polars.sink import _create_bucket_partition


@pytest.fixture
def bucket_table(tmp_path: Path) -> Iterator[tuple[dl.Table, pl.DataFrame]]:
    with dl.create(
        f"sqlite:///{tmp_path}/catalog.sqlite", data_path=str(tmp_path / "data")
    ) as lake:
        table = lake.create_table(
            "bucketed",
            {"x": dl.Int64(), "y": dl.Int64()},
            partition_by=dl.PartitionColumn("x", transform="bucket", num_buckets=16),
        )
        data = pl.DataFrame({"x": range(1024), "y": range(1024)})
        table.sink_polars(data.lazy())
        yield table, data


@pytest.mark.parametrize(
    "predicate",
    [
        pytest.param(pl.col("x") == 512, id="equality"),
        pytest.param(pl.lit(512) == pl.col("x"), id="reversed-equality"),
        pytest.param(pl.col("x").is_in([511, 512, 513]), id="in"),
        pytest.param((pl.col("x") == 512) & (pl.col("y") > 500), id="conjunction"),
    ],
)
def test_bucket_pruning_avoids_reads(
    bucket_table: tuple[dl.Table, pl.DataFrame], predicate: pl.Expr
) -> None:
    # Arrange
    table, data = bucket_table
    expected = data.filter(predicate)
    buckets = set(
        expected.select(_create_bucket_partition(pl.col("x"), pl.Int64(), 16))
        .to_series()
        .to_list()
    )
    files = table.scan().data_files
    irrelevant = [
        file
        for file in files
        if pl.read_parquet(file.path)
        .select(_create_bucket_partition(pl.col("x"), pl.Int64(), 16))
        .item(0, 0)
        not in buckets
    ]
    assert irrelevant
    for file in irrelevant:
        stats = file.statistics.column_stats[1]
        assert isinstance(stats.min_value, int)
        assert isinstance(stats.max_value, int)
        assert stats.min_value < 511 < 513 < stats.max_value
        Path(unquote(urlparse(file.path).path)).unlink()

    # Act
    actual = table.scan_polars().filter(predicate).collect()

    # Assert
    assert_frame_equal(actual, expected, check_row_order=False)


@pytest.mark.parametrize(
    "predicate",
    [
        pytest.param(pl.col("x") > 512, id="range"),
        pytest.param((pl.col("x") == 512) | (pl.col("y") == 513), id="mixed-or"),
        pytest.param(pl.col("x").cast(pl.String) == "512", id="cast-column"),
        pytest.param(pl.col("x") == 512.5, id="lossy-literal"),
        pytest.param(pl.col("x").is_in([]), id="empty-in"),
        pytest.param(pl.col("x").is_in([None, 512]), id="null-in"),
        pytest.param((pl.col("x") == 511) & (pl.col("x") == 512), id="contradiction"),
        pytest.param(~pl.col("x").is_in([511, 512]), id="not-in"),
        pytest.param(pl.col("x").cast(pl.Boolean).fill_null(False), id="unsupported-function"),
    ],
)
def test_bucket_pruning_fallbacks(
    bucket_table: tuple[dl.Table, pl.DataFrame], predicate: pl.Expr
) -> None:
    # Arrange
    table, data = bucket_table

    # Act
    actual = table.scan_polars().filter(predicate).collect()

    # Assert
    assert_frame_equal(actual, data.filter(predicate), check_row_order=False)


def test_bucket_pruning_preserves_snapshot_and_projection(
    bucket_table: tuple[dl.Table, pl.DataFrame],
) -> None:
    # Arrange
    table, data = bucket_table
    scan = table.scan_polars(include_file_paths="path")
    table.sink_polars(pl.LazyFrame({"x": [512], "y": [-1]}))

    # Act
    actual = scan.filter(pl.col("x") == 512).select("y", "path").head(1).collect()

    # Assert
    assert actual["y"].to_list() == data.filter(pl.col("x") == 512)["y"].to_list()
    assert actual["path"].is_not_null().all()


@pytest.mark.parametrize("selection", [pl.len(), pl.col("y"), pl.col("x").sum()])
def test_bucket_scan_projection_without_filter(
    bucket_table: tuple[dl.Table, pl.DataFrame], selection: pl.Expr
) -> None:
    # Arrange
    table, data = bucket_table

    # Act
    actual = table.scan_polars().select(selection).collect()

    # Assert
    assert_frame_equal(actual, data.select(selection), check_row_order=False)


@pytest.mark.parametrize("partitioning", [None, dl.Partitioning("y")])
def test_bucket_pruning_after_partition_evolution(
    bucket_table: tuple[dl.Table, pl.DataFrame], partitioning: dl.Partitioning | None
) -> None:
    # Arrange
    table, data = bucket_table
    table.update_partitioning(partitioning)
    table.sink_polars(data.lazy())
    table.update_partitioning(
        dl.Partitioning(dl.PartitionColumn("x", transform="bucket", num_buckets=8))
    )
    table.sink_polars(data.lazy())
    predicate = pl.col("x").is_in([511, 512, 513])

    # Act
    actual = table.scan_polars().filter(predicate).collect()

    # Assert
    assert_frame_equal(actual, pl.concat([data] * 3).filter(predicate), check_row_order=False)


@pytest.mark.parametrize(
    "predicate",
    [
        pytest.param(pl.col("x") == 512, id="equality"),
        pytest.param(pl.col("x").eq_missing(None), id="null-equality"),
        pytest.param(pl.col("x").is_in([None, 512]), id="in"),
        pytest.param(pl.col("x").is_in([None, 512], nulls_equal=True), id="in-nulls-equal"),
    ],
)
def test_bucket_pruning_preserves_inline_and_null_rows(
    bucket_table: tuple[dl.Table, pl.DataFrame], predicate: pl.Expr
) -> None:
    # Arrange
    table, data = bucket_table
    extra = pl.DataFrame({"x": [512, None], "y": [-1, -2]}, schema=data.schema)
    table.sink_polars(extra.lazy())
    table.write_polars(extra)

    # Act
    actual = table.scan_polars().filter(predicate).collect()

    # Assert
    assert_frame_equal(
        actual, pl.concat([data, extra, extra]).filter(predicate), check_row_order=False
    )


@pytest.mark.parametrize("value", [None, "invalid", "-1", "16"])
def test_bucket_pruning_retains_unknown_metadata(
    bucket_table: tuple[dl.Table, pl.DataFrame], tmp_path: Path, value: str | None
) -> None:
    # Arrange
    table, data = bucket_table
    with closing(sqlite3.connect(tmp_path / "catalog.sqlite")) as conn, conn:
        conn.execute("UPDATE ducklake_file_partition_value SET partition_value = ?", (value,))
    predicate = pl.col("x") == 512

    # Act
    actual = table.scan_polars().filter(predicate).collect()

    # Assert
    assert_frame_equal(actual, data.filter(predicate), check_row_order=False)


def test_bucket_pruning_uses_partition_key_index(
    bucket_table: tuple[dl.Table, pl.DataFrame],
) -> None:
    # Arrange
    table, data = bucket_table
    table.update_partitioning(
        dl.Partitioning(
            [
                dl.PartitionColumn("y"),
                dl.PartitionColumn("x", transform="bucket", num_buckets=16),
            ]
        )
    )
    extra = data.with_columns(y=pl.lit(1, dtype=pl.Int64))
    table.sink_polars(extra.lazy())
    scan = table.scan()
    target_bucket = _literal_buckets(pl.Series([512]), pl.Int64(), 16)
    assert target_bucket is not None
    removed = 0
    for file in scan.data_files:
        if (value := file.bucket_values.get(1)) is not None and value[1] not in target_bucket:
            Path(unquote(urlparse(file.path).path)).unlink()
            removed += 1
    assert removed > 0

    # Act
    actual = table.scan_polars().filter(pl.col("x") == 512).collect()

    # Assert
    expected = pl.concat([data, extra]).filter(pl.col("x") == 512)
    assert_frame_equal(actual, expected, check_row_order=False)


@pytest.mark.parametrize(
    ("values", "dtype", "expected"),
    [
        pytest.param(pl.Series([34, 35, 36, 37]), pl.Int64(), {3, 5, 6}, id="integer"),
        pytest.param(pl.Series([34]), pl.Int32(), {3}, id="narrow-integer"),
        pytest.param(pl.Series(["iceberg"]), pl.String(), {1}, id="string"),
        pytest.param(pl.Series([bytes([0, 1, 2, 3])]), pl.Binary(), {1}, id="binary"),
        pytest.param(pl.Series([True]), pl.Boolean(), {4}, id="boolean"),
        pytest.param(pl.Series([dt.date(2017, 11, 16)]), pl.Date(), {2}, id="date"),
        pytest.param(
            pl.Series([dt.datetime(2017, 11, 16, 22, 31, 8)]),
            pl.Datetime("us"),
            {7},
            id="timestamp",
        ),
        pytest.param(pl.Series([1.0]), pl.Float64(), {7}, id="float"),
        pytest.param(pl.Series([None]), pl.Float64(), set(), id="null"),
        pytest.param(pl.Series([128]), pl.Int8(), None, id="out-of-range"),
        pytest.param(pl.Series([1.5]), pl.Int64(), None, id="float-to-int"),
        pytest.param(pl.Series([float(2**53)]), pl.Int64(), None, id="lossy-comparison"),
        pytest.param(pl.Series([1.1]), pl.Float32(), None, id="lossy-float"),
        pytest.param(pl.Series([float("nan")]), pl.Float64(), None, id="nan"),
        pytest.param(pl.Series(["34"]), pl.Int64(), None, id="string-to-int"),
        pytest.param(pl.Series([1.0]), pl.Decimal(9, 2), None, id="unsupported-type"),
    ],
)
def test_bucket_literal_conversion(
    values: pl.Series, dtype: pl.DataType, expected: set[int] | None
) -> None:
    # Arrange
    num_buckets = 8

    # Act
    actual = _literal_buckets(values, dtype, num_buckets)

    # Assert
    assert actual == expected
