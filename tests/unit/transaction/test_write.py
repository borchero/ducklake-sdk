import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ducklake as dl


@pytest.mark.parametrize("create_schema", [False, True])
def test_create_write_in_transaction(
    shared_ducklake: dl.Ducklake,
    random_schema_name: str,
    random_table_name: str,
    create_schema: bool,
) -> None:
    # Arrange
    lf = pl.LazyFrame({"x": [1, 2, 3]})
    table_name = (random_schema_name, random_table_name) if create_schema else random_table_name

    # Act
    with shared_ducklake.transaction() as tx:
        if create_schema:
            tx.create_schema(random_schema_name)
        table = tx.create_table(table_name, {"x": dl.Int64()})
        table.sink_polars(lf)

    # Assert
    table = shared_ducklake.get_table(table_name)
    assert_frame_equal(lf, table.scan_polars())
