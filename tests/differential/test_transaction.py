import duckdb
import pytest
from _testutils import assert_ducklake_catalogs_equal

import ducklake as dl


@pytest.mark.differential
def test_match_reference_table_double_rename_and_alter(
    ducklake: dl.Ducklake,
    catalog_url: str,
    reference_catalog_url: str,
    reference_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    # Act
    ducklake.create_table("test", {"x": dl.Int64()})
    with ducklake.transaction() as tx:
        tx.table("test").rename("test_tmp")
        tx.table("test_tmp").rename("test2")
        tx.table("test2").add_column(dl.Column("y", dl.Varchar()))

    reference_duckdb_connection.execute("CREATE TABLE test (x BIGINT)")
    reference_duckdb_connection.execute(
        """BEGIN;
        ALTER TABLE test RENAME TO test_tmp;
        ALTER TABLE test_tmp RENAME TO test2;
        ALTER TABLE test2 ADD COLUMN y VARCHAR;
        COMMIT;
        """
    )

    # Assert
    assert_ducklake_catalogs_equal(reference_catalog_url, catalog_url)
