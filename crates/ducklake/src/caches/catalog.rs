use std::collections::HashMap;
use std::sync::{Arc, RwLock, Weak};

use crate::catalog::Catalog;
use crate::{DucklakeResult, db};

#[derive(Clone)]
pub(super) struct CatalogCache {
    pool: db::Pool,
    /// Mapping from `schema_version` to the catalog for that version of the schema.
    catalogs: Arc<RwLock<HashMap<i64, Weak<Catalog>>>>,
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
        if let Some(catalog) = self
            .catalogs
            .read()
            .unwrap()
            .get(&schema_version)
            .and_then(Weak::upgrade)
        {
            return Ok(catalog);
        }

        let loaded = Arc::new(Catalog::load(&self.pool, snapshot_id).await?);
        let mut catalogs = self.catalogs.write().unwrap();
        catalogs.retain(|_, catalog| catalog.strong_count() > 0);
        if let Some(catalog) = catalogs.get(&schema_version).and_then(Weak::upgrade) {
            return Ok(catalog);
        }
        catalogs.insert(schema_version, Arc::downgrade(&loaded));
        Ok(loaded)
    }
}
