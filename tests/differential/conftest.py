from collections.abc import Iterator
from pathlib import Path

import duckdb
import pytest
from _testutils import make_catalog_url, make_storage_path

from ducklake._storage import StorageOptionSet
from ducklake.connect import _sanitize_url
from ducklake.ducklake import _make_duckdb_connection


@pytest.fixture()
def reference_catalog_url(catalog: str, tmp_path: Path) -> Iterator[str]:
    with make_catalog_url(catalog, tmp_path) as url:
        yield url


@pytest.fixture()
def reference_duckdb_connection(
    reference_catalog_url: str, storage: str, tmp_path: Path
) -> Iterator[duckdb.DuckDBPyConnection]:
    with make_storage_path(storage, tmp_path) as storage_path:
        args = _sanitize_url(reference_catalog_url)
        conn = _make_duckdb_connection(
            args, data_path=storage_path, storage_options=StorageOptionSet()
        )
        yield conn
        conn.execute("USE memory")
        conn.execute("DETACH my_ducklake")
        conn.close()
