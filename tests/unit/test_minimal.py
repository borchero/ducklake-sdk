from pathlib import Path

import polars as pl
from polars.testing import assert_frame_equal

import ducklake as dl
from ducklake.transaction import Transaction


def test_readme_example(tmp_path: Path) -> None:
    # Arrange
    ducklake = dl.create(
        f"sqlite:///{tmp_path}/metadata.sqlite", data_path=f"{tmp_path}/data_files/"
    )
    table = ducklake.create_table(
        "events",
        schema={"id": dl.Int64(), "message": dl.Varchar()},
    )

    # Act
    lf = pl.LazyFrame({"id": [1, 2, 3], "message": ["hello", "ducklake", "sdk"]})
    table.sink_polars(lf)

    # Assert
    lf_roundtrip = table.scan_polars()
    assert_frame_equal(lf, lf_roundtrip)


def test_top_level_transaction_export() -> None:
    assert dl.Transaction is Transaction
