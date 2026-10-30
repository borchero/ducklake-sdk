import base64
from collections.abc import Iterator
from pathlib import Path
from typing import Literal

import polars as pl
import pytest
from _testutils import make_catalog_url, make_storage_path
from polars.testing import assert_frame_equal

import ducklake as dl


@pytest.fixture()
def encrypted_lake(catalog_url: str, storage_path: str) -> Iterator[dl.Ducklake]:
    with dl.create(catalog_url, data_path=storage_path, encrypted=True) as lake:
        yield lake


@pytest.fixture()
def encrypted_table(encrypted_lake: dl.Ducklake) -> dl.Table:
    return encrypted_lake.create_table("encrypted", {"x": dl.Int64()})


@pytest.fixture()
def encrypted_target(catalog: str, storage: str, tmp_path: Path) -> Iterator[dl.Ducklake]:
    with (
        make_catalog_url(catalog, tmp_path) as catalog_url,
        make_storage_path(storage, tmp_path) as storage_path,
        dl.create(catalog_url, data_path=storage_path, encrypted=True) as lake,
    ):
        yield lake


@pytest.fixture()
def encrypted_files(encrypted_lake: dl.Ducklake, encrypted_table: dl.Table) -> dl.Table:
    encrypted_lake.set_metadata(data_inlining_row_limit=0)
    conn = encrypted_lake._duckdb_connection
    conn.execute("INSERT INTO encrypted SELECT range FROM range(100)")
    conn.execute("INSERT INTO encrypted SELECT range FROM range(100, 200)")
    conn.execute("DELETE FROM encrypted WHERE x < 20")
    return encrypted_table


@pytest.fixture()
def encrypted_parquet(encrypted_table: dl.Table, tmp_path: Path) -> tuple[str, str]:
    path = str(tmp_path / "encrypted.parquet")
    key = b"0123456789abcdef0123456789abcdef"
    pl.DataFrame({"x": range(10)}).write_parquet(
        path,
        encryption=pl.ParquetEncryptionProperties(footer_key=key),
        arrow_schema=encrypted_table.schema,
    )
    return path, base64.b64encode(key).decode("ascii")


# -------------------------------------------- TESTS -------------------------------------------- #


@pytest.mark.parametrize("encrypted", [False, True])
def test_encryption_metadata(catalog_url: str, storage_path: str, encrypted: bool) -> None:
    # Arrange
    with dl.create(catalog_url, data_path=storage_path, encrypted=encrypted) as lake:
        lake.create_table("test", {"x": dl.Int64()})

    # Act
    with dl.connect(catalog_url) as lake:
        metadata = lake.metadata
        table = lake.table("test")
        table_metadata = table.metadata
        write_metadata, _, _, write_encrypted = table._get_write_info()
        with lake.transaction() as tx:
            pending = tx.create_table("pending", {"x": dl.Int64()})
            pending_metadata, _, _, pending_encrypted = pending._get_write_info()

    # Assert
    assert metadata["encrypted"] is encrypted
    assert "encrypted" not in table_metadata
    assert "encrypted" not in write_metadata
    assert "encrypted" not in pending_metadata
    assert write_encrypted is encrypted
    assert pending_encrypted is encrypted


@pytest.mark.differential
@pytest.mark.parametrize(
    ("mode", "transactional", "partitioned"),
    [("write", False, False), ("sink", True, False), ("lazy_sink", True, True)],
)
def test_polars_encrypted_writes(
    encrypted_lake: dl.Ducklake,
    encrypted_table: dl.Table,
    transactional: bool,
    partitioned: bool,
    mode: str,
) -> None:
    # Arrange
    encrypted_table.set_metadata(data_inlining_row_limit=0)
    if partitioned:
        encrypted_table.update_partitioning(dl.Partitioning(dl.PartitionColumn("x")))
    df = pl.DataFrame({"x": [1, 2, 3]})

    # Act
    with encrypted_lake.transaction() as tx:
        table = tx.table("encrypted") if transactional else encrypted_table
        if mode == "write":
            table.write_polars(df)
        elif mode == "sink":
            table.sink_polars(df.lazy())
        else:
            table.sink_polars(df.lazy(), lazy=True).collect()
    scan = encrypted_table.scan()
    result = encrypted_table.read_polars()
    reference = encrypted_lake._duckdb_connection.execute(
        "SELECT x FROM encrypted ORDER BY x"
    ).fetchall()

    # Assert
    assert_frame_equal(result.sort("x"), df)
    assert reference == [(1,), (2,), (3,)]
    assert len(scan.data_files) == (3 if partitioned else 1)
    assert all(file.encryption_key for file in scan.data_files)
    assert sum(file.statistics.num_rows for file in scan.data_files) == 3
    assert all(file.statistics.column_stats for file in scan.data_files)
    for file in scan.data_files:
        with pytest.raises(pl.exceptions.ComputeError, match="(?i)encrypt"):
            pl.read_parquet(file.path, storage_options=encrypted_table._storage_options.to_dict())


def test_register_encrypted_file(
    encrypted_table: dl.Table, encrypted_parquet: tuple[str, str]
) -> None:
    # Arrange
    path, key = encrypted_parquet
    file = dl.WriteDataFile(path, encryption_key=key)

    # Act
    encrypted_table.write_data_files(file)
    files = encrypted_table.scan().data_files

    # Assert
    assert len(files) == 1
    assert files[0].encryption_key == key
    assert files[0].statistics.num_rows == 10
    assert_frame_equal(encrypted_table.read_polars(), pl.DataFrame({"x": range(10)}))
    stats = next(iter(files[0].statistics.column_stats.values()))
    assert stats.min_value == 0
    assert stats.max_value == 9
    assert stats.null_count == 0


@pytest.mark.parametrize("key", ["not base64!", base64.b64encode(b"x" * 32).decode("ascii")])
def test_register_encrypted_file_with_wrong_key(
    encrypted_table: dl.Table, encrypted_parquet: tuple[str, str], key: str
) -> None:
    # Arrange
    path, _ = encrypted_parquet

    # Act
    with pytest.raises((ValueError, RuntimeError)):
        encrypted_table.write_data_files(dl.WriteDataFile(path, encryption_key=key))

    # Assert
    assert encrypted_table.scan().data_files == []


def test_encrypted_scan_tracks_new_keys_and_inline_data(encrypted_table: dl.Table) -> None:
    # Arrange
    scan = encrypted_table.scan_polars()
    encrypted_table.sink_polars(pl.LazyFrame({"x": [1, 2]}))
    first = scan.collect()
    encrypted_table.sink_polars(pl.LazyFrame({"x": [3, 4]}))
    encrypted_table.write_polars(pl.DataFrame({"x": [5]}))

    # Act
    result = scan.collect()
    metadata = encrypted_table.scan()
    keys = [file.encryption_key for file in metadata.data_files]
    count = scan.select(pl.len()).collect().item()

    # Assert
    assert_frame_equal(first, pl.DataFrame({"x": [1, 2]}))
    assert_frame_equal(result, pl.DataFrame({"x": [1, 2, 3, 4, 5]}))
    assert len(keys) == len(set(keys)) == 2
    assert all(keys)
    assert metadata.inline_data
    assert count == 5


@pytest.mark.differential
def test_scan_encrypted_files(encrypted_files: dl.Table) -> None:
    # Arrange
    table = encrypted_files

    # Act
    try:
        result = table.read_polars()
    except pl.exceptions.ComputeError as error:
        if "AAD unique file identifier is not set" in str(error):
            pytest.xfail("DuckDB omits Parquet AAD: https://github.com/duckdb/duckdb/issues/27827")
        raise

    # Assert
    assert_frame_equal(result, pl.DataFrame({"x": range(20, 200)}))


@pytest.mark.differential
@pytest.mark.parametrize("operation", ["copy_tables", "move_tables"])
def test_transfer_encrypted_files(
    encrypted_lake: dl.Ducklake,
    encrypted_files: dl.Table,
    operation: Literal["copy_tables", "move_tables"],
    encrypted_target: dl.Ducklake,
) -> None:
    # Arrange
    source = encrypted_files.scan()

    # Act
    transferred = getattr(encrypted_lake, operation)(
        {"transferred": encrypted_files}, encrypted_target
    )[0]
    scan = transferred.scan()
    rows = encrypted_target._duckdb_connection.execute(
        f"SELECT x FROM {transferred.name} ORDER BY x"
    ).fetchall()

    # Assert
    assert [file.encryption_key for file in scan.data_files] == [
        file.encryption_key for file in source.data_files
    ]
    assert [delete.encryption_key for file in scan.data_files for delete in file.delete_files] == [
        delete.encryption_key for file in source.data_files for delete in file.delete_files
    ]
    assert rows == [(i,) for i in range(20, 200)]


@pytest.mark.parametrize(
    ("operation", "encrypted_source"), [("copy_tables", False), ("move_tables", True)]
)
def test_transfer_rejects_mismatched_encryption(
    encrypted_lake: dl.Ducklake,
    shared_ducklake: dl.Ducklake,
    random_table_name: str,
    operation: Literal["copy_tables", "move_tables"],
    encrypted_source: bool,
) -> None:
    # Arrange
    source, target = (
        (encrypted_lake, shared_ducklake)
        if encrypted_source
        else (shared_ducklake, encrypted_lake)
    )
    table = source.create_table(random_table_name, {"x": dl.Int64()})
    source_snapshot = source.get_latest_snapshot().id
    target_snapshot = target.get_latest_snapshot().id

    # Act
    with pytest.raises(ValueError, match="same encryption setting"):
        getattr(source, operation)([table], target)

    # Assert
    assert source.has_table(random_table_name)
    assert not target.has_table(random_table_name)
    assert source.get_latest_snapshot().id == source_snapshot
    assert target.get_latest_snapshot().id == target_snapshot


def test_polars_reads_encrypted_data_and_deletes(
    encrypted_table: dl.Table,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    for i in range(2):
        encrypted_table.sink_polars(pl.LazyFrame({"x": range(i * 10, (i + 1) * 10)}))
    scan = encrypted_table.scan()
    delete_key = b"e" * 32
    for i, data_file in enumerate(scan.data_files):
        delete_path = str(tmp_path / f"deletes_{i}.parquet")
        pl.DataFrame({"file_path": [data_file.path] * 2, "pos": [1, 4]}).write_parquet(
            delete_path,
            encryption=pl.ParquetEncryptionProperties(footer_key=delete_key),
        )
        data_file.delete_files = [
            dl.DeleteFile(
                delete_path,
                num_deletes=2,
                encryption_key=base64.b64encode(delete_key).decode("ascii"),
            )
        ]
        data_file.inline_deletes = pl.Series([7], dtype=pl.Int64).to_arrow()
    monkeypatch.setattr(encrypted_table, "scan", lambda: scan)
    lazy = encrypted_table.scan_polars(include_file_paths="file_path")

    # Act
    result = lazy.collect()
    filtered = lazy.filter(pl.col("x") >= 3).select("x").head(3).collect()
    count = lazy.select(pl.len()).collect().item()

    # Assert
    assert result["x"].to_list() == [0, 2, 3, 5, 6, 8, 9, 10, 12, 13, 15, 16, 18, 19]
    assert result["file_path"].to_list() == [
        file.path for file in scan.data_files for _ in range(7)
    ]
    assert filtered["x"].to_list() == [3, 5, 6]
    assert count == 14
