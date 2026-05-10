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
