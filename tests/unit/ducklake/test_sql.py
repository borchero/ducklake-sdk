from __future__ import annotations

import datetime as dt

import polars as pl
import pytest
import sqlalchemy as sa

import ducklake as dl

pytestmark = pytest.mark.skip_config(
    catalog="mysql", reason="The DuckDB MySQL connector is unreliable."
)


@pytest.mark.parametrize("query_type", ["raw", "text", "ddl"])
def test_execute_sql(
    shared_ducklake: dl.Ducklake, random_table_name: str, query_type: str
) -> None:
    # Arrange
    query: str | sa.ClauseElement = f"CREATE TABLE {random_table_name} (x INTEGER)"
    if query_type == "text":
        query = sa.text(query)
    elif query_type == "ddl":
        query = sa.schema.CreateTable(
            sa.Table(random_table_name, sa.MetaData(), sa.Column("x", sa.Integer))
        )

    # Act
    shared_ducklake.execute_sql(query)

    # Assert
    assert random_table_name in [table.name.name for table in shared_ducklake.list_tables()]


@pytest.fixture(params=["parquet", "inline"])
def sql_table(
    shared_ducklake: dl.Ducklake, random_table_name: str, request: pytest.FixtureRequest
) -> dl.Table:
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    df = pl.DataFrame({"x": range(10)})
    if request.param == "parquet":
        table.sink_polars(df.lazy())
    else:
        table.write_polars(df)
    return table


@pytest.mark.parametrize(("predicate", "expected"), [("x >= 5", list(range(5))), ("TRUE", [])])
def test_execute_sql_delete(
    shared_ducklake: dl.Ducklake,
    sql_table: dl.Table,
    predicate: str,
    expected: list[int],
) -> None:
    # Arrange
    snapshot_id = shared_ducklake.get_latest_snapshot().id
    historical_table = shared_ducklake.at(snapshot_id).table(sql_table.name)

    # Act
    shared_ducklake.execute_sql(f"DELETE FROM {sql_table.name} WHERE {predicate}")

    # Assert
    assert sql_table.read_polars()["x"].sort().to_list() == expected
    assert historical_table.read_polars()["x"].sort().to_list() == list(range(10))


@pytest.mark.parametrize(
    ("predicate", "expected"),
    [
        (sa.column("x") >= 5, list(range(5))),
        (sa.column("x").in_([1, 3, 5]), [0, 2, 4, 6, 7, 8, 9]),
        (sa.column("x").in_([]), list(range(10))),
        (
            sa.or_(
                sa.column("x") == sa.bindparam("threshold", 5),
                sa.column("x") + 1 == sa.bindparam("threshold", 5),
            ),
            [0, 1, 2, 3, 6, 7, 8, 9],
        ),
    ],
)
def test_execute_sql_delete_sqlalchemy(
    shared_ducklake: dl.Ducklake,
    sql_table: dl.Table,
    predicate: sa.ColumnElement[bool],
    expected: list[int],
) -> None:
    # Arrange
    query = sa.delete(sa.table(sql_table.name.name, schema=sql_table.name.schema)).where(predicate)

    # Act
    shared_ducklake.execute_sql(query)

    # Assert
    assert sql_table.read_polars()["x"].sort().to_list() == expected


@pytest.mark.parametrize("time_travel", [False, True])
def test_execute_sql_delete_readonly(
    shared_ducklake: dl.Ducklake, sql_table: dl.Table, time_travel: bool
) -> None:
    # Arrange
    readonly = (
        shared_ducklake.at(shared_ducklake.get_latest_snapshot().id)
        if time_travel
        else shared_ducklake.readonly()
    )

    # Act
    with pytest.raises(Exception, match="read.only|READ_ONLY"):
        readonly.execute_sql(f"DELETE FROM {sql_table.name}")

    # Assert
    assert sql_table.read_polars()["x"].sort().to_list() == list(range(10))


def test_scan_duckdb_uses_connection_time_zone(
    catalog_url: str, storage_path: str, random_table_name: str
) -> None:
    # Arrange
    with dl.create(catalog_url, data_path=storage_path, time_zone="Europe/Berlin") as ducklake:
        table = ducklake.create_table(random_table_name, {"x": dl.TimestampTz()})
        series = pl.Series(
            [dt.datetime(2024, 7, 1, 12, tzinfo=dt.timezone.utc)],
            dtype=pl.Datetime("us", "UTC"),
        )
        table.sink_polars(pl.LazyFrame({"x": series}))

        # Act
        result = table.scan_duckdb().project("x::VARCHAR").fetchone()

        # Assert
        assert result == ("2024-07-01 14:00:00+02",)
