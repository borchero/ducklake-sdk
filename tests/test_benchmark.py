from __future__ import annotations

import uuid
from contextlib import ExitStack
from typing import TYPE_CHECKING

import pytest
import sqlalchemy as sa

import ducklake as dl

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

    from pytest_codspeed import BenchmarkFixture

pytestmark = pytest.mark.benchmark


@pytest.fixture
def catalog_factory(
    catalog: str, tmp_path: Path
) -> Iterator[Callable[[], tuple[dl.Ducklake, str]]]:
    from _testutils import make_catalog_url

    with ExitStack() as stack:

        def create() -> tuple[dl.Ducklake, str]:
            stack.close()
            url = stack.enter_context(make_catalog_url(catalog, tmp_path))
            lake = stack.enter_context(dl.create(url, data_path=str(tmp_path / uuid.uuid4().hex)))
            return lake, url

        yield create


@pytest.mark.parametrize(
    ("table_count", "files_per_table", "column_count"),
    [(1, 100, 20), (50, 2, 20), (50, 100, 20), (50, 100, 64)],
    ids=["single-table", "100-files-total", "100-files-per-table", "wide-tables"],
)
def test_commit(
    benchmark: BenchmarkFixture,
    catalog_factory: Callable[[], tuple[dl.Ducklake, str]],
    table_count: int,
    files_per_table: int,
    column_count: int,
) -> None:
    # Arrange
    catalog_url = ""

    def setup() -> tuple[tuple[dl.Transaction], dict[str, object]]:
        nonlocal catalog_url
        ducklake, catalog_url = catalog_factory()
        names = [f"table_{index}" for index in range(table_count)]
        schema = {f"column_{index}": dl.Int64() for index in range(column_count)}
        with ducklake.transaction() as tx:
            for name in names:
                tx.create_table(name, schema)

        statistics = {}
        for name in names:
            columns = ducklake.table(name).schema.columns
            statistics[name] = dl.DataFileStatistics(
                num_rows=1000,
                file_size_bytes=8192,
                footer_size_bytes=512,
                column_stats={
                    column.field_id: dl.ColumnStats(
                        min_value=0, max_value=999, null_count=0, contains_nan=False
                    )
                    for column in columns
                    if column.field_id is not None
                },
            )
            assert len(statistics[name].column_stats) == column_count

        # Seed persisted statistics so the measured append exercises their UPDATE path.
        with ducklake.transaction() as tx:
            for name in names:
                tx.table(name).write_data_files(
                    [dl.WriteDataFile("seed.parquet", statistics=statistics[name])]
                )

        tx = ducklake.transaction()
        for name in names:
            tx.table(name).write_data_files(
                [
                    dl.WriteDataFile(f"file_{index}.parquet", statistics=statistics[name])
                    for index in range(files_per_table)
                ]
            )
        return (tx,), {}

    # Act
    benchmark.pedantic(dl.Transaction.commit, setup=setup, rounds=3, iterations=1, warmup_rounds=0)

    # Assert
    expected_files = table_count * (files_per_table + 1)
    engine = sa.create_engine(catalog_url)
    try:
        with engine.connect() as connection:
            assert connection.scalar(sa.text("SELECT count(*) FROM ducklake_data_file")) == (
                expected_files
            )
            assert connection.scalar(
                sa.text("SELECT count(*) FROM ducklake_file_column_stats")
            ) == (expected_files * column_count)
            assert connection.scalar(
                sa.text("SELECT sum(record_count) FROM ducklake_table_stats")
            ) == (expected_files * 1000)
    finally:
        engine.dispose()
