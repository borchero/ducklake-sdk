use futures::TryStreamExt;
use sea_query::Table;

use crate::{Ducklake, DucklakeResult, io};

impl Ducklake {
    /// Permanently delete a DuckLake's data and catalog contents.
    ///
    /// Recursively deletes files under the data path and drops every table in the metadata
    /// catalog, including tables not managed by DuckLake. The catalog database itself is retained.
    ///
    /// Uses this connection's storage options and closes its pool on success. Other connections
    /// must not access the DuckLake during deletion. Deletion cannot be rolled back: a failure
    /// may leave some files or tables already deleted. Read-only connections cannot delete.
    pub async fn delete(&mut self) -> DucklakeResult<()> {
        self.conn.check_writable()?;
        let pool = self.conn.pool();
        let tables = pool.list_tables().await?;

        // First, we delete all data files
        let data_path = self.conn.metadata().data_path().resolve()?;
        let store = data_path.object_store(Some(self.conn.storage_options().to_vec()));
        let locations = store
            .list(Some(&data_path.path()))
            .map_ok(|object| object.location);
        io::delete_objects(store.as_ref(), locations).await?;

        // Then, we clean up the catalog
        let mut tx = pool.begin().await?;
        for table in tables {
            tx.execute(&Table::drop().table(table).if_exists().take())
                .await?;
        }
        tx.commit().await?;
        pool.close().await;
        Ok(())
    }
}
