use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::catalog::Catalog;
use crate::{DucklakeResult, db};

#[derive(Clone)]
pub(super) struct CatalogCache {
    pool: db::Pool,
    /// Mapping from `schema_version` to the catalog for that version of the schema.
    catalogs: Arc<RwLock<HashMap<i64, Arc<Catalog>>>>,
}

impl CatalogCache {
    pub(super) fn new(pool: db::Pool) -> Self {
        Self {
            pool,
            catalogs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub(super) async fn get(
        &self,
        snapshot_id: i64,
        schema_version: i64,
    ) -> DucklakeResult<Arc<Catalog>> {
        if let Some(catalog) = self.catalogs.read().unwrap().get(&schema_version) {
            Ok(catalog.clone())
        } else {
            let catalog = Catalog::load(&self.pool, snapshot_id).await?;
            let catalog = Arc::new(catalog);
            self.catalogs
                .write()
                .unwrap()
                .insert(schema_version, catalog.clone());
            Ok(catalog)
        }
    }
}
