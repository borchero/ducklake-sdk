import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

import ducklake as dl


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--catalog",
        choices=["sqlite", "postgres", "mysql"],
        default="sqlite",
        help="The catalog database to run tests against.",
    )
    parser.addoption(
        "--storage",
        choices=["local", "s3", "gcs", "abs"],
        default="local",
        help="The storage backend to run tests against.",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    catalog = config.getoption("--catalog")
    storage = config.getoption("--storage")
    for item in items:
        # Skip tests based on `skip_config`
        for marker in item.iter_markers():
            if marker.name == "skip_config":
                skip_catalog = marker.kwargs.get("catalog")
                skip_storage = marker.kwargs.get("storage")
                if (skip_catalog is None or skip_catalog == catalog) and (
                    skip_storage is None or skip_storage == storage
                ):
                    reason = marker.kwargs.get("reason", "skipped by skip_config marker")
                    item.add_marker(pytest.mark.skip(reason=reason))

        # Skip some tests for MySQL due to the connector's current limitations
        if item.get_closest_marker("differential") is not None:
            if catalog == "mysql":
                reason = "The DuckDB MySQL connector is unreliable."
                item.add_marker(pytest.mark.skip(reason=reason))


# ------------------------------------------- CATALOG ------------------------------------------- #


@pytest.fixture(scope="session")
def catalog(request: pytest.FixtureRequest) -> str:
    return request.config.getoption("--catalog")


@pytest.fixture()
def catalog_url(catalog: str, tmp_path: Path) -> Iterator[str]:
    from _testutils import make_catalog_url

    with make_catalog_url(catalog, tmp_path) as url:
        yield url


# ------------------------------------------- STORAGE ------------------------------------------- #


@pytest.fixture(scope="session")
def storage(request: pytest.FixtureRequest) -> str:
    return request.config.getoption("--storage")


@pytest.fixture()
def storage_path(storage: str, tmp_path: Path) -> Iterator[str]:
    from _testutils import make_storage_path

    with make_storage_path(storage, tmp_path) as path:
        yield path


# ------------------------------------------- DUCKLAKE ------------------------------------------ #


@pytest.fixture()
def ducklake(catalog_url: str, storage_path: str) -> Iterator[dl.Ducklake]:
    with dl.create(catalog_url, data_path=storage_path) as ducklake:
        yield ducklake


@pytest.fixture(scope="session")
def shared_ducklake(catalog: str, storage: str) -> Iterator[dl.Ducklake]:
    from _testutils import make_catalog_url, make_storage_path

    with (
        tempfile.TemporaryDirectory() as tmpdir,
        make_catalog_url(catalog, Path(tmpdir)) as catalog_url,
        make_storage_path(storage, Path(tmpdir)) as storage_path,
        dl.create(catalog_url, data_path=storage_path) as ducklake,
    ):
        yield ducklake
