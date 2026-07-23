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
