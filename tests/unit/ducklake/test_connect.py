import os
import subprocess
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest
import sqlalchemy as sa
from _testutils import assert_ducklake_catalogs_equal, make_catalog_url, make_storage_path

import ducklake as dl
import ducklake.exceptions as dlexc
from ducklake.connect import _sanitize_url


def test_create_connect(catalog_url: str, storage_path: str) -> None:
    # Act & Assert
    dl.create(catalog_url, data_path=storage_path)
    dl.connect(catalog_url)


def test_create_connect_sqlalchemy_url(catalog_url: str, storage_path: str) -> None:
    # Arrange
    sa_url = sa.make_url(catalog_url)

    # Act & Assert
    dl.create(sa_url, data_path=storage_path)
    dl.connect(sa_url)


def test_connection_time_zone_is_not_persisted(catalog_url: str, storage_path: str) -> None:
    # Arrange
    with dl.create(catalog_url, data_path=storage_path, time_zone="Europe/Berlin") as created:
        # Act
        readonly = created.readonly()
        with (
            dl.connect(catalog_url) as connected,
            dl.connect(catalog_url, time_zone="America/New_York") as configured,
        ):
            # Assert
            assert created.time_zone == "Europe/Berlin"
            assert readonly.time_zone == "Europe/Berlin"
            assert connected.time_zone == "UTC"
            assert configured.time_zone == "America/New_York"


def test_invalid_connection_time_zone_fails_before_creation(
    catalog_url: str, storage_path: str
) -> None:
    # Act & Assert
    with pytest.raises(ValueError, match="invalid time zone 'invalid'"):
        dl.create(catalog_url, data_path=storage_path, time_zone="invalid")


def test_fail_recreate(catalog_url: str, storage_path: str) -> None:
    # Arrange
    dl.create(catalog_url, data_path=storage_path)

    # Act & Assert
    with pytest.raises(dlexc.AlreadyInitializedError):
        dl.create(catalog_url, data_path=storage_path)


def test_fail_connect_when_not_created(catalog_url: str) -> None:
    # Act & Assert
    with pytest.raises(dlexc.NotInitializedError):
        dl.connect(catalog_url)


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("sqlite:///path/to/db.db", 'Ducklake(url="sqlite:///path/to/db.db")'),
        (
            "postgresql://user:secret@localhost:5432/db",
            'Ducklake(url="postgresql://user:***@localhost:5432/db")',
        ),
        (
            "mysql://user:p%40ss@localhost:3306/db",
            'Ducklake(url="mysql://user:***@localhost:3306/db")',
        ),
    ],
)
def test_ducklake_repr(url: str, expected: str) -> None:
    # Arrange
    ducklake = dl.Ducklake.__new__(dl.Ducklake)
    ducklake._connection_args = _sanitize_url(url)

    # Act
    actual = repr(ducklake)

    # Assert
    assert actual == expected


def test_data_path(catalog: str, tmp_path: Path) -> None:
    # The data path is normalized on creation into a `file://` URL, which is only deterministic
    # for a known local path, so this assertion is restricted to the SQLite catalog.
    if catalog != "sqlite":
        pytest.skip("The data path assertion is only run against the SQLite catalog.")

    # Arrange
    catalog_url = f"sqlite:///{tmp_path}/catalog.db"
    data_path = tmp_path / "data"

    # Act
    with dl.create(catalog_url, data_path=str(data_path)) as lake:
        # Assert
        assert lake.metadata["data_path"] == data_path.as_uri() + "/"


# ----------------------------------------------------------------------------------------------- #
#                                          URL PARSING                                            #
# ----------------------------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("sqlite:///path/to/db.db", "sqlite:///path/to/db.db"),
        ("sqlite:///path/to/db.db?mode=ro", "sqlite:///path/to/db.db?mode=ro"),
        ("postgresql://localhost", "postgresql://localhost"),
        ("postgresql://user@localhost", "postgresql://user@localhost"),
        (
            "postgresql://user:pass@localhost:5432/db",
            "postgresql://user:***@localhost:5432/db",
        ),
        (
            "postgresql://user:pass@localhost:5432/db?sslmode=require",
            "postgresql://user:***@localhost:5432/db?sslmode=require",
        ),
        ("mysql://user:pass@localhost:3306/db", "mysql://user:***@localhost:3306/db"),
        ("postgresql://[::1]:5432/db", "postgresql://[::1]:5432/db"),
    ],
)
def test_connection_args_str(url: str, expected: str) -> None:
    # Act
    args = _sanitize_url(url)
    actual = str(args)

    # Assert
    assert actual == expected


def test_connection_args_supports_dialect_with_driver() -> None:
    # Act
    args = _sanitize_url("postgresql+psycopg2://localhost/db")

    # Assert
    assert args.dialect == "postgresql"


def test_connection_args_unsupported_dialect_raises() -> None:
    # Act & Assert
    with pytest.raises(ValueError, match="Dialect 'oracle' is currently not supported"):
        _sanitize_url("oracle://user:pass@localhost/db")


def test_connection_args_sqlite_requires_database() -> None:
    # Act & Assert
    with pytest.raises(ValueError, match="Database must be provided"):
        _sanitize_url("sqlite://")


def test_connection_args_postgres_requires_host() -> None:
    # Act & Assert
    with pytest.raises(ValueError, match="Host must be provided"):
        _sanitize_url("postgresql:///db")


def test_connection_args_url_decodes_credentials() -> None:
    # Act
    args = _sanitize_url("postgresql://us%40er:p%40ss@localhost/db")

    # Assert
    assert args.username == "us@er"
    assert args.password == "p@ss"


def test_connection_args_accepts_sqlalchemy_url() -> None:
    # Arrange
    url = sa.make_url("postgresql://user:pass@localhost:5432/db")

    # Act
    args = _sanitize_url(url)

    # Assert
    assert args.dialect == "postgresql"
    assert args.username == "user"
    assert args.host == "localhost"
    assert args.port == 5432
    assert args.database == "db"


# ----------------------------------------------------------------------------------------------- #
#                                            MIGRATIONS                                           #
# ----------------------------------------------------------------------------------------------- #

SNAPSHOTS_PATH = Path(__file__).parent.parent.parent / "_snapshots"


@pytest.fixture(scope="module")
def catalog_latest(catalog: str, storage: str) -> Iterator[str]:
    with (
        tempfile.TemporaryDirectory() as tmpdir,
        make_catalog_url(catalog, Path(tmpdir)) as catalog_url,
        make_storage_path(storage, Path(tmpdir)) as storage_path,
        dl.create(catalog_url, data_path=storage_path),
    ):
        yield catalog_url


def test_migrate_v01(catalog: str, catalog_url: str, catalog_latest: str) -> None:
    # Arrange
    _load_snapshot(catalog, catalog_url, version="v0.1")

    # Act
    with dl.connect(catalog_url, migrate=True):
        # Assert
        assert_ducklake_catalogs_equal(
            catalog_latest,
            catalog_url,
            extra_ignored_columns={
                # The `path` is not set to "[name]/" but an empty string in the official migration
                "ducklake_schema": ["path"],
            },
        )


def test_migrate_v02(catalog: str, catalog_url: str, catalog_latest: str) -> None:
    # Arrange
    _load_snapshot(catalog, catalog_url, version="v0.2")

    # Act
    with dl.connect(catalog_url, migrate=True):
        # Assert
        assert_ducklake_catalogs_equal(catalog_latest, catalog_url)


def test_migrate_v03(catalog: str, catalog_url: str, catalog_latest: str) -> None:
    # Arrange
    _load_snapshot(catalog, catalog_url, version="v0.3")

    # Act
    with dl.connect(catalog_url, migrate=True):
        # Assert
        assert_ducklake_catalogs_equal(catalog_latest, catalog_url)


@pytest.mark.skip_config(
    catalog="mysql",
    reason="The DuckDB MySQL connector incorrectly uses `DATETIME` instead of `TIMESTAMP`.",
)
def test_migrate_v04(catalog: str, catalog_url: str, catalog_latest: str) -> None:
    # Arrange
    _load_snapshot(catalog, catalog_url, version="v0.4")

    # Act
    with dl.connect(catalog_url, migrate=True):
        # Assert
        assert_ducklake_catalogs_equal(catalog_latest, catalog_url)


# -------------------------------------------- UTILS -------------------------------------------- #


def _load_snapshot(catalog: str, url: str, *, version: str) -> None:
    args = _sanitize_url(url)
    env: dict[str, str] = {}
    match catalog:
        case "sqlite":
            cmd = ["sqlite3", args.database]
        case "postgres":
            cmd = [
                "psql",
                "-h",
                args.host,
                "-p",
                str(args.port or 5432),
                "-U",
                args.username,
                "-d",
                args.database,
            ]
            if args.password is not None:
                env = {"PGPASSWORD": args.password}
        case "mysql":
            cmd = [
                "mysql",
                "-h",
                args.host,
                "-P",
                str(args.port or 3306),
                "-u",
                args.username,
                f"-p{args.password}",
                args.database,
            ]

    snapshot_path = SNAPSHOTS_PATH / version / "catalogs" / f"{catalog}.sql"
    with snapshot_path.open() as f:
        subprocess.run(cast("list[str]", cmd), stdin=f, check=True, env={**os.environ, **env})
