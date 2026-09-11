import polars as pl
import pytest

import ducklake as dl


def _assert_enum_filters_roundtrip(
    ducklake: dl.Ducklake, table_name: str, categories: list[str]
) -> None:
    """Sink an enum-typed frame and assert equality filters still work.

    DuckLake stores the column as varchar but keeps the enum categories, so ``scan_ducklake``
    reconstructs the dtype as ``pl.Enum``. Per-file min/max statistics are byte-ordered strings;
    if they are materialized as the enum dtype, polars compares them by category *code* during
    predicate pushdown and can prune files whose code-order range does not bracket the (byte-order)
    filter value -- silently returning zero rows.
    """
    enum = pl.Enum(categories)
    values = ["apple", "banana", "pear", "banana", "apple"]
    df = pl.DataFrame({"fruit": pl.Series(values, dtype=enum)})

    # Create the table from the enum-typed schema and sink it (as enum).
    table = ducklake.create_table(table_name, df.to_arrow().schema)
    table.sink_polars(df.lazy())

    scanned = table.scan_polars()
    assert scanned.collect_schema()["fruit"] == enum

    # Compare against a plain string constant. Every value is present, so the
    # equality filter must return the same rows regardless of statistics-based pushdown.
    for value in ["apple", "banana", "pear"]:
        expected = df.filter(pl.col("fruit").cast(pl.String) == value).height
        actual = scanned.filter(pl.col("fruit") == value).select(pl.len()).collect().item()
        assert actual == expected, f"filter for {value!r} returned {actual}, expected {expected}"


@pytest.mark.skip_config(catalog="mysql", reason="Data inlining is not yet supported for MySQL.")
def test_scan_enum_column_filter_pushdown(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    """Regression test for an enum whose definition order differs from lexicographic order.

    Here the enum's *definition* order (pear, apple, banana) differs from its lexicographic order
    (apple, banana, pear). With the buggy statistics dtype this made every equality filter return
    zero rows, since the byte-ordered min/max no longer bracketed the filter value once compared in
    category-code order.
    """
    _assert_enum_filters_roundtrip(
        shared_ducklake, random_table_name, categories=["pear", "apple", "banana"]
    )


@pytest.mark.skip_config(catalog="mysql", reason="Data inlining is not yet supported for MySQL.")
def test_scan_enum_column_filter_lexicographic_order(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    """Control test for an enum whose definition order already matches lexicographic order.

    When the enum's definition order (apple, banana, pear) coincides with lexicographic order, the
    category codes agree with byte order, so statistics pushdown happens to work even without the
    fix. This documents that the definition order is exactly what triggers the bug.
    """
    _assert_enum_filters_roundtrip(
        shared_ducklake, random_table_name, categories=["apple", "banana", "pear"]
    )
