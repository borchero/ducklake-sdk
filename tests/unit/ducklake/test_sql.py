import datetime as dt

import polars as pl
import pytest
import sqlalchemy as sa

import ducklake as dl

pytestmark = pytest.mark.skip_config(
    catalog="mysql", reason="The DuckDB MySQL connector is unreliable."
)


def test_execute_sql(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Act
    shared_ducklake.execute_sql(f"CREATE TABLE {random_table_name} (x INTEGER)")

    # Assert
    assert random_table_name in [table.name.name for table in shared_ducklake.list_tables()]


def test_execute_sql_sqlalchemy(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    lf = pl.LazyFrame({"x": range(10)})
    table.sink_polars(lf)

    # Act
    query = sa.delete(sa.table(random_table_name)).where(sa.column("x") >= 5)
    shared_ducklake.execute_sql(query)

    # Assert
    assert table.read_polars().height == 5


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
