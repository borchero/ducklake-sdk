from __future__ import annotations

import datetime as dt
from threading import Barrier
from typing import TYPE_CHECKING

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ducklake as dl
from ducklake.polars.scan import _enum_statistics, _new_column_expression

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


def test_scan_single_file(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64(), "y": dl.Varchar()})
    lf = pl.LazyFrame({"x": range(100), "y": ["foo"] * 100})
    table.sink_polars(lf)

    # Act
    scanned = table.scan_polars()

    # Assert
    assert_frame_equal(lf, scanned)


@pytest.mark.parametrize("missing_size", [False, True])
def test_scan_source_sizes(
    scan_table: dl.Table, mocker: MockerFixture, missing_size: bool
) -> None:
    # Arrange
    first = pl.DataFrame({"x": range(100)})
    second = pl.DataFrame({"x": range(100, 300)})
    scan_table.sink_polars(first.lazy())
    scan_table.sink_polars(second.lazy())
    scan_result = scan_table.scan()
    sizes = [file.statistics.file_size_bytes for file in scan_result.data_files]
    assert len(sizes) == 2 and all(size is not None for size in sizes)
    if missing_size:
        scan_result.data_files[1].statistics.file_size_bytes = None
    mocker.patch.object(scan_table, "scan", return_value=scan_result)
    scan_parquet = mocker.spy(pl, "scan_parquet")

    # Act
    actual = scan_table.scan_polars().collect()

    # Assert
    assert scan_parquet.call_args.kwargs["_source_sizes"] == (None if missing_size else sizes)
    assert_frame_equal(actual, pl.concat([first, second]), check_row_order=False)


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


def test_read_polars_uses_connection_time_zone(
    catalog_url: str, storage_path: str, random_table_name: str
) -> None:
    # Arrange
    ducklake = dl.create(catalog_url, data_path=storage_path, time_zone="Europe/Berlin")
    table = ducklake.create_table(random_table_name, {"x": dl.TimestampTz()})
    df = pl.DataFrame(
        {
            "x": pl.Series(
                [
                    dt.datetime(2024, 1, 1, 12, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 7, 1, 12, tzinfo=dt.timezone.utc),
                ],
                dtype=pl.Datetime("us", "UTC"),
            )
        }
    )
    table.sink_polars(df.lazy())

    # Act
    actual = table.read_polars()
    overridden = table.scan_polars(time_zone="UTC").collect()

    # Assert
    expected = df.with_columns(pl.col("x").dt.convert_time_zone("Europe/Berlin"))
    assert_frame_equal(actual, expected)
    assert_frame_equal(overridden, df)


def test_read_polars_applies_time_zone_to_nested_timestamps(
    catalog_url: str, storage_path: str, random_table_name: str
) -> None:
    # Arrange
    ducklake = dl.create(catalog_url, data_path=storage_path, time_zone="Europe/Berlin")
    table = ducklake.create_table(
        random_table_name,
        {"events": dl.List(dl.Struct({"at": dl.TimestampTz()}))},
    )
    source_dtype = pl.List(pl.Struct({"at": pl.Datetime("us", "UTC")}))
    df = pl.DataFrame(
        {
            "events": pl.Series(
                [
                    [{"at": dt.datetime(2024, 7, 1, 12, tzinfo=dt.timezone.utc)}],
                    [],
                ],
                dtype=source_dtype,
            )
        }
    )
    table.sink_polars(df.lazy())

    # Act
    actual = table.read_polars()

    # Assert
    expected_dtype = pl.List(pl.Struct({"at": pl.Datetime("us", time_zone="Europe/Berlin")}))
    expected = df.with_columns(pl.col("events").cast(expected_dtype))
    assert_frame_equal(actual, expected)


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


# --------------------------------------- ENUM / CATEGORICAL ------------------------------------ #

_ENUM_DTYPE = pl.Enum(["banana", "pear", "apple", "zebra"])


def test_scan_enum(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    schema = pl.Schema({"x": pl.Enum(["a", "b", "c"])})
    table = shared_ducklake.create_table(random_table_name, dl.Schema(schema))
    lf = pl.LazyFrame({"x": ["a", "b", "c", "a"]}, schema=schema)
    table.sink_polars(lf)

    # Act
    scanned = table.scan_polars()

    # Assert
    assert scanned.collect_schema()["x"] == pl.Enum(["a", "b", "c"])
    assert_frame_equal(lf, scanned)


@pytest.fixture()
def enum_table(
    shared_ducklake: dl.Ducklake, random_table_name: str, request: pytest.FixtureRequest
) -> tuple[dl.Table, pl.DataFrame]:
    data = pl.DataFrame({"x": ["apple", "banana", "pear", None]}, schema={"x": _ENUM_DTYPE})
    for _ in range(getattr(request, "param", 0)):
        data = data.select(pl.struct(pl.all()).alias("record"))
    table = shared_ducklake.create_table(random_table_name, dl.Schema(data.schema))
    return table, data


@pytest.mark.parametrize(
    "predicate",
    [
        pl.col("x") == "banana",
        pl.col("x") == pl.lit("pear", dtype=_ENUM_DTYPE),
        pl.col("x") != "pear",
        pl.col("x") < "apple",
        pl.col("x") > "banana",
        pl.col("x").is_in(["banana", "pear"]),
    ],
)
def test_scan_enum_filter(enum_table: tuple[dl.Table, pl.DataFrame], predicate: pl.Expr) -> None:
    # Arrange
    table, data = enum_table
    table.sink_polars(data.lazy())
    expected = data.filter(predicate)

    # Act
    actual = table.scan_polars().filter(predicate).collect()

    # Assert
    assert_frame_equal(actual, expected)


@pytest.mark.parametrize(
    "enum_table", [0, 1, 2], indirect=True, ids=["scalar", "struct", "nested-struct"]
)
def test_scan_enum_prunes_files(enum_table: tuple[dl.Table, pl.DataFrame]) -> None:
    # Arrange
    table, data = enum_table
    column = table.schema.columns[0]
    expression = pl.col(column.name)
    while isinstance(column.data_type, dl.Struct):
        column = column.data_type.fields[0]
        expression = expression.struct.field(column.name)
    assert column.field_id is not None
    _, generator, _ = table._get_write_info()
    data.write_parquet(
        f"{generator.base_path}data.parquet",
        arrow_schema=table.schema,
        storage_options=table._storage_options.to_dict(),
        mkdir=True,
    )
    # Register tighter lexical bounds than Polars' dictionary-wide statistics. The second
    # file deliberately does not exist: collecting succeeds only if it is pruned.
    table.write_data_files(
        [
            dl.WriteDataFile(
                "data.parquet",
                statistics=dl.DataFileStatistics(
                    num_rows=4,
                    column_stats={
                        column.field_id: dl.ColumnStats(
                            min_value="apple", max_value="pear", null_count=1
                        )
                    },
                ),
            ),
            dl.WriteDataFile(
                "irrelevant.parquet",
                statistics=dl.DataFileStatistics(
                    num_rows=1,
                    column_stats={
                        column.field_id: dl.ColumnStats(
                            min_value="zebra", max_value="zebra", null_count=0
                        )
                    },
                ),
            ),
        ]
    )
    predicate = expression == "banana"
    expected = data.filter(predicate)

    # Act
    actual = table.scan_polars().filter(predicate).collect()

    # Assert
    assert_frame_equal(actual, expected)


@pytest.mark.parametrize(
    ("lower", "upper", "expected"),
    [
        ("apple", "pear", ("banana", "apple")),
        ("apple", "zebra", ("banana", "zebra")),
        ("pear", "pear", ("pear", "pear")),
        ("ap", "pe", ("banana", "apple")),
        (None, "pear", (None, None)),
        ("apple", None, (None, None)),
        ("unknown", "unknown", (None, None)),
    ],
)
def test_enum_statistics_bounds(
    lower: str | None, upper: str | None, expected: tuple[str | None, str | None]
) -> None:
    # Arrange
    minimum = pl.Series("x_min", [lower], dtype=pl.String)
    maximum = pl.Series("x_max", [upper], dtype=pl.String)

    # Act
    converted_min, converted_max = _enum_statistics(minimum, maximum, _ENUM_DTYPE)

    # Assert
    assert (converted_min.item(), converted_max.item()) == expected
    assert converted_min.dtype == converted_max.dtype == _ENUM_DTYPE


def test_scan_categorical(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    schema = pl.Schema({"x": pl.Categorical()})
    table = shared_ducklake.create_table(random_table_name, dl.Schema(schema))
    lf = pl.LazyFrame({"x": ["a", "b", "c", "a"]}, schema=schema)
    table.sink_polars(lf)

    # Act
    scanned = table.scan_polars()

    # Assert
    assert scanned.collect_schema()["x"] == pl.Categorical()
    assert_frame_equal(lf, scanned)


# --------------------------------------- INITIAL DEFAULTS -------------------------------------- #


@pytest.mark.parametrize(
    ("column", "dtype", "type_name"),
    [
        pytest.param(
            dl.Column("added", dl.Struct({"key": dl.Varchar()}), initial_default={"key": "hello"}),
            pl.Struct({"key": pl.String()}),
            "struct",
            id="struct",
        ),
        pytest.param(
            dl.Column("added", dl.List(dl.Varchar()), initial_default=["hello"]),
            pl.List(pl.String()),
            "list",
            id="list",
        ),
    ],
)
def test_nested_initial_defaults_are_not_supported(
    column: dl.Column, dtype: pl.DataType, type_name: str
) -> None:
    # Arrange
    expected_message = f"Initial defaults for {type_name} columns are not supported"

    # Act
    with pytest.raises(NotImplementedError) as exc_info:
        _new_column_expression(column, dtype)

    # Assert
    assert str(exc_info.value) == expected_message


@pytest.mark.parametrize("reader", ["read_polars", "scan_polars"])
@pytest.mark.parametrize(
    "inline",
    [
        pytest.param(
            True,
            marks=pytest.mark.skip_config(
                catalog="mysql", reason="Data inlining is not yet supported for MySQL."
            ),
            id="inline",
        ),
        pytest.param(False, id="file"),
    ],
)
@pytest.mark.parametrize("write_after", [False, True], ids=["old_rows_only", "new_rows_too"])
@pytest.mark.parametrize(
    ("dtype", "value"),
    [
        pytest.param(pl.UInt32(), 7, id="scalar"),
        pytest.param(pl.List(pl.String()), ["hello"], id="list"),
        pytest.param(
            pl.Struct({"key": pl.String(), "count": pl.UInt32()}),
            {"key": "hello", "count": 7},
            id="struct",
        ),
        pytest.param(
            pl.List(pl.Struct({"key": pl.String(), "count": pl.UInt32()})),
            [{"key": "hello", "count": 7}],
            id="list_of_struct",
        ),
    ],
)
def test_added_column_is_null_for_existing_rows(
    shared_ducklake: dl.Ducklake,
    random_table_name: str,
    dtype: pl.DataType,
    value: object,
    write_after: bool,
    inline: bool,
    reader: str,
) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"id": dl.Int64()})
    before = pl.DataFrame({"id": [0, 1]})
    if inline:
        table.write_polars(before)
    else:
        table.sink_polars(before.lazy())
    table.add_column(dl.Schema(pl.Schema({"added": dtype})).columns[0])
    expected = before.with_columns(pl.lit(None, dtype=dtype).alias("added"))
    if write_after:
        after = pl.DataFrame({"id": [2], "added": pl.Series([value], dtype=dtype)})
        if inline:
            table.write_polars(after)
        else:
            table.sink_polars(after.lazy())
        expected = pl.concat([expected, after])

    # Act
    actual = table.read_polars() if reader == "read_polars" else table.scan_polars().collect()
    scan = table.scan()

    # Assert
    assert bool(scan.inline_data) is inline
    assert bool(scan.data_files) is not inline
    assert_frame_equal(actual.sort("id"), expected)


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


# ----------------------------------------- DEFERRED SCANS --------------------------------------- #


@pytest.fixture()
def scan_table(shared_ducklake: dl.Ducklake, random_table_name: str) -> dl.Table:
    return shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})


def test_scan_defers_file_metadata(scan_table: dl.Table, mocker: MockerFixture) -> None:
    # Arrange
    scan = mocker.spy(scan_table, "scan")

    # Act
    query = scan_table.scan_polars(include_file_paths="path")
    schema = query.collect_schema()
    query.explain(optimized=False)

    # Assert
    scan.assert_not_called()
    assert schema == pl.Schema({"x": pl.Int64, "path": pl.String})


@pytest.mark.parametrize("historical", [False, True])
def test_scan_collection_respects_snapshot(
    scan_table: dl.Table, shared_ducklake: dl.Ducklake, historical: bool
) -> None:
    # Arrange
    table = scan_table
    if historical:
        snapshot = shared_ducklake.get_latest_snapshot().id
        table = shared_ducklake.at(snapshot).table(scan_table.name)
    query = table.scan_polars()
    data = pl.DataFrame({"x": [1]})
    expected = data.clear() if historical else data

    # Act
    scan_table.sink_polars(data.lazy())
    first = query.collect()
    scan_table.sink_polars(data.lazy())
    second = query.collect()

    # Assert
    assert_frame_equal(first, expected)
    assert_frame_equal(second, pl.concat([expected, expected]))


def test_scan_rejects_changed_schema(scan_table: dl.Table) -> None:
    # Arrange
    query = scan_table.scan_polars()
    query.collect_schema()

    # Act
    scan_table.rename_column("x", "y")

    # Assert
    with pytest.raises(pl.exceptions.SchemaError, match="schema changed"):
        query.collect()


def test_scan_resolves_metadata_in_parallel(scan_table: dl.Table, mocker: MockerFixture) -> None:
    # Arrange
    barrier = Barrier(2, timeout=10)
    original_scan = scan_table.scan

    def synchronized_scan() -> dl.ScanResult:
        barrier.wait()
        return original_scan()

    scan = mocker.patch.object(scan_table, "scan", side_effect=synchronized_scan)
    query = pl.concat([scan_table.scan_polars(), scan_table.scan_polars()])

    # Act
    actual = query.collect()

    # Assert
    assert scan.call_count == 2
    assert_frame_equal(actual, pl.DataFrame(schema={"x": pl.Int64}))
