import polars as pl
from polars.testing import assert_frame_equal

import ducklake as dl
from ducklake._polars_types import POLARS_LOGICAL_TYPES_TAG


def test_create_write_in_transaction(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    lf = pl.LazyFrame({"x": [1, 2, 3]})

    # Act
    with shared_ducklake.transaction() as tx:
        table = tx.create_table(random_table_name, {"x": dl.Int64()})
        table.sink_polars(lf)

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert_frame_equal(lf, table.scan_polars())


def test_create_write_enum_in_transaction(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    enum = pl.Enum(["low", "medium", "high"])
    df = pl.DataFrame({"priority": ["low", "high"]}, schema_overrides={"priority": enum})

    # Act
    with shared_ducklake.transaction() as tx:
        table = tx.create_table(random_table_name, pl.Schema({"priority": enum}))
        table.sink_polars(df.lazy())

    # Assert
    table = shared_ducklake.get_table(random_table_name)
    assert table.tags[POLARS_LOGICAL_TYPES_TAG] == (
        '{"1":{"type":"enum","categories":["low","medium","high"]}}'
    )
    assert_frame_equal(table.read_polars(), df)
