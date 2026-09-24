import polars as pl
import sqlalchemy as sa
from _testutils import storage_file_exists

import ducklake as dl


def test_delete(catalog_url: str, storage_path: str) -> None:
    # Arrange
    engine = sa.create_engine(catalog_url)
    try:
        with dl.create(catalog_url, data_path=f"{storage_path}/lake") as lake:
            table = lake.create_table("data", {"x": dl.Int64()})
            table.sink_polars(pl.LazyFrame({"x": [1, 2, 3]}))
            files = [file.path for file in table.scan().data_files]
            orphan = f"{storage_path}/lake/nested/orphan.parquet"
            sibling = f"{storage_path}/lake-other/keep.parquet"
            for path in (orphan, sibling):
                pl.LazyFrame({"x": [4]}).sink_parquet(
                    path, mkdir=True, storage_options=lake._storage_options.to_dict()
                )
            files.append(orphan)
            with engine.begin() as conn:
                conn.execute(sa.text("CREATE TABLE unrelated (value INTEGER)"))

            # Act
            lake.delete()

            # Assert
            assert sa.inspect(engine).get_table_names() == []
            assert all(not storage_file_exists(lake, path) for path in files)
            assert storage_file_exists(lake, sibling)
    finally:
        engine.dispose()
