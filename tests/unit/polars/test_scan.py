import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ducklake as dl
from ducklake._polars_types import POLARS_LOGICAL_TYPE_TAG

ENUM_CATEGORIES = ["z", "a", "m"]


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


def test_scan_enum(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    enum = pl.Enum(ENUM_CATEGORIES)
    table = shared_ducklake.create_table(random_table_name, pl.Schema({"priority": enum}))
    expected = pl.DataFrame(
        {"priority": ["z", "a", "m", None]},
        schema_overrides={"priority": enum},
    )
    table.sink_polars(expected.lazy())

    # Act
    scanned = table.scan_polars()
    actual = scanned.collect()

    # Assert
    assert scanned.collect_schema() == expected.schema
    assert_frame_equal(actual, expected)
    assert_frame_equal(table.read_polars(), expected)


def test_scan_enum_uses_category_order(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    enum = pl.Enum(ENUM_CATEGORIES)
    table = shared_ducklake.create_table(random_table_name, pl.Schema({"priority": enum}))
    table.sink_polars(
        pl.LazyFrame(
            {"priority": ["z", "a", "m"]},
            schema_overrides={"priority": enum},
        )
    )

    # Act
    actual = table.scan_polars().filter(pl.col("priority") > "z").collect()

    # Assert
    expected = pl.DataFrame(
        {"priority": ["a", "m"]},
        schema_overrides={"priority": enum},
    )
    assert_frame_equal(actual, expected)


def test_scan_enum_tracks_column_history(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    enum = pl.Enum(ENUM_CATEGORIES)
    original_schema = pl.Schema(
        {
            "priority": enum,
            "details": pl.Struct({"priority": enum}),
        }
    )
    table = shared_ducklake.create_table(random_table_name, original_schema)
    original = pl.DataFrame(
        {
            "priority": ["z", "a"],
            "details": [{"priority": "m"}, {"priority": "z"}],
        },
        schema=original_schema,
    )
    table.write_polars(original)
    original_snapshot = shared_ducklake.get_latest_snapshot().id

    # Act
    table.rename_column("priority", "level")
    with shared_ducklake.transaction() as tx:
        tx.table(random_table_name).rename_column(["details", "priority"], "level")
    current = shared_ducklake.get_table(random_table_name).read_polars()
    historic = shared_ducklake.at(original_snapshot).get_table(random_table_name).read_polars()

    # Assert
    current_schema = pl.Schema(
        {
            "level": enum,
            "details": pl.Struct({"level": enum}),
        }
    )
    expected_current = pl.DataFrame(
        {
            "level": ["z", "a"],
            "details": [{"level": "m"}, {"level": "z"}],
        },
        schema=current_schema,
    )
    assert_frame_equal(current, expected_current)
    assert_frame_equal(historic, original)


def test_scan_nested_enum(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    enum = pl.Enum(ENUM_CATEGORIES)
    schema = pl.Schema(
        {
            "priorities": pl.List(enum),
            "details": pl.Struct({"priority": enum, "count": pl.Int64}),
        }
    )
    table = shared_ducklake.create_table(random_table_name, schema)
    expected = pl.DataFrame(
        {
            "priorities": [["z", "m"], [None], None],
            "details": [
                {"priority": "z", "count": 1},
                {"priority": "m", "count": 2},
                {"priority": None, "count": 3},
            ],
        },
        schema=schema,
    )
    table.sink_polars(expected.lazy())

    # Act
    actual = table.read_polars()

    # Assert
    assert_frame_equal(actual, expected)


def test_scan_ignores_enum_metadata_for_removed_column(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    enum = pl.Enum(ENUM_CATEGORIES)
    table = shared_ducklake.create_table(
        random_table_name,
        pl.Schema({"priority": enum, "value": pl.Int64}),
    )
    table.sink_polars(
        pl.LazyFrame(
            {"priority": ["z", "a"], "value": [1, 2]},
            schema_overrides={"priority": enum},
        )
    )

    # Act
    table.remove_column("priority")
    actual = table.read_polars()

    # Assert
    expected = pl.DataFrame({"value": [1, 2]})
    assert_frame_equal(actual, expected)


@pytest.mark.skip_config(catalog="mysql", reason="Data inlining is not yet supported for MySQL.")
def test_scan_enum_from_data_file_and_inline_data(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    enum = pl.Enum(ENUM_CATEGORIES)
    table = shared_ducklake.create_table(random_table_name, pl.Schema({"priority": enum}))
    file_data = pl.DataFrame({"priority": ["z", "a"]}, schema_overrides={"priority": enum})
    inline_data = pl.DataFrame({"priority": ["m", None]}, schema_overrides={"priority": enum})
    table.sink_polars(file_data.lazy())
    table.write_polars(inline_data)

    # Act
    actual = table.read_polars()

    # Assert
    expected = pl.concat([file_data, inline_data])
    assert_frame_equal(actual, expected)


@pytest.mark.parametrize(
    ("column", "metadata", "error"),
    [
        (
            dl.Column("priority", dl.Varchar()),
            "not-json",
            "Invalid Polars logical type metadata",
        ),
        (
            dl.Column("priority", dl.Int64()),
            '{"type":"enum","version":1,"metadata":{"categories":["z","a","m"]}}',
            "Invalid Polars Enum logical type metadata",
        ),
        (
            dl.Column("priority", dl.Varchar()),
            '{"type":"enum","categories":["z","a","m"]}',
            "Invalid Polars logical type metadata",
        ),
        (
            dl.Column("priority", dl.Varchar()),
            '{"type":"future","version":1,"metadata":{}}',
            "Unsupported Polars logical type",
        ),
        (
            dl.Column("priority", dl.Varchar()),
            '{"type":"enum","version":2,"metadata":{"categories":["z","a","m"]}}',
            "Unsupported version",
        ),
        (
            dl.Column("priority", dl.Varchar()),
            '{"type":"enum","version":1,"metadata":{"categories":["z","z"]}}',
            "Invalid Polars Enum logical type metadata",
        ),
    ],
)
def test_scan_rejects_invalid_enum_metadata(
    shared_ducklake: dl.Ducklake,
    random_table_name: str,
    column: dl.Column,
    metadata: str,
    error: str,
) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, [column])
    table._pytable.add_column_tag("priority", POLARS_LOGICAL_TYPE_TAG, metadata)

    # Act & Assert
    with pytest.raises(ValueError, match=error):
        table.scan_polars()


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
