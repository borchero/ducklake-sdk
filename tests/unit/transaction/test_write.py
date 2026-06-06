import polars as pl
from polars.testing import assert_frame_equal

import ducklake as dl


def test_create_write_in_transaction(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    lf = pl.LazyFrame({"x": [1, 2, 3]})

    # Act
    with shared_ducklake.transaction() as tx:
        table = tx.create_table(random_table_name, {"x": dl.Int64()})
        table.sink_polars(lf)

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert_frame_equal(lf, table.scan_polars())
