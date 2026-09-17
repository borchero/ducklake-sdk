use std::collections::HashMap;
use std::sync::{Arc, RwLock, Weak};

use crate::catalog::Catalog;
use crate::db;
use crate::primitives::AsyncLazy;

pub(super) type LazyCatalog = AsyncLazy<Arc<Catalog>>;

#[derive(Clone)]
pub(super) struct CatalogCache {
    pool: db::Pool,
    /// Mapping from `schema_version` to the catalog for that version of the schema.
    catalogs: Arc<RwLock<HashMap<i64, Weak<LazyCatalog>>>>,
}

impl CatalogCache {
    pub(super) fn new(pool: db::Pool) -> Self {
        Self {
            pool,
            catalogs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub(super) fn get(&self, snapshot_id: i64, schema_version: i64) -> Arc<LazyCatalog> {
        let mut catalogs = self.catalogs.write().unwrap();
        if let Some(catalog) = catalogs.get(&schema_version).and_then(Weak::upgrade) {
            return catalog;
        }
        catalogs.retain(|_, catalog| catalog.strong_count() > 0);
        let pool = self.pool.clone();
        let catalog = Arc::new(AsyncLazy::new(move |_| {
            let pool = pool.clone();
            async move { Catalog::load(&pool, snapshot_id).await.map(Arc::new) }
        }));
        catalogs.insert(schema_version, Arc::downgrade(&catalog));
        catalog
    }
}
