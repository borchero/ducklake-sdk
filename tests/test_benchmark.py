from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import pytest

import ducklake as dl

if TYPE_CHECKING:
    from pytest_codspeed import BenchmarkFixture

pytestmark = pytest.mark.benchmark


@pytest.mark.parametrize(
    ("table_count", "files_per_table", "column_count"),
    [(1, 100, 20), (50, 2, 20), (50, 100, 20), (50, 100, 64)],
    ids=["single-table", "100-files-total", "100-files-per-table", "wide-tables"],
)
def test_commit(
    ducklake: dl.Ducklake,
    benchmark: BenchmarkFixture,
    table_count: int,
    files_per_table: int,
    column_count: int,
) -> None:
    # Arrange
    names = [f"table_{index}" for index in range(table_count)]
    schema = {f"column_{index}": dl.Int64() for index in range(column_count)}
    with ducklake.transaction() as tx:
        for name in names:
            tx.create_table(name, schema)

    statistics = {
        name: dl.DataFileStatistics(
            num_rows=1000,
            file_size_bytes=8192,
            footer_size_bytes=512,
            column_stats={
                column.field_id: dl.ColumnStats(
                    min_value=0, max_value=999, null_count=0, contains_nan=False
                )
                for column in ducklake.table(name).schema.columns
                if column.field_id is not None
            },
        )
        for name in names
    }
    # Seed persisted statistics so subsequent commits exercise their UPDATE path.
    with ducklake.transaction() as tx:
        for name in names:
            tx.table(name).write_data_files(
                [dl.WriteDataFile("seed.parquet", statistics=statistics[name])]
            )

    def setup() -> tuple[tuple[dl.Transaction], dict[str, object]]:
        prefix = uuid.uuid4().hex
        tx = ducklake.transaction()
        for name in names:
            tx.table(name).write_data_files(
                [
                    dl.WriteDataFile(f"{prefix}_{index}.parquet", statistics=statistics[name])
                    for index in range(files_per_table)
                ]
            )
        return (tx,), {}

    # Act
    benchmark.pedantic(dl.Transaction.commit, setup=setup, rounds=10)
