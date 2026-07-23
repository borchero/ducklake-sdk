from pathlib import Path

import pytest
from _testutils import assert_ducklake_catalogs_equal

import ducklake as dl


@pytest.mark.differential
@pytest.mark.usefixtures("reference_duckdb_connection")
def test_match_reference_schema(
    catalog_url: str, storage_path: str, reference_catalog_url: str
) -> None:
    # Act
    with dl.create(catalog_url, data_path=storage_path):
        # Assert
        assert_ducklake_catalogs_equal(reference_catalog_url, catalog_url)


def test_data_path(catalog: str, tmp_path: Path) -> None:
    # The differential catalog comparison masks the `data_path` metadata value (paths differ per
    # run), so assert the actual, normalized data path here against a known local `tmp_path`.
    if catalog != "sqlite":
        pytest.skip("The data path assertion is only run against the SQLite catalog.")

    # Arrange
    catalog_url = f"sqlite:///{tmp_path}/catalog.db"
    data_path = tmp_path / "data"

    # Act
    with dl.create(catalog_url, data_path=str(data_path)) as lake:
        # Assert
        assert lake.metadata["data_path"] == data_path.as_uri() + "/"
