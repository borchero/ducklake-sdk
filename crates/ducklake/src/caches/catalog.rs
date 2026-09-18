use std::sync::Arc;

use crate::catalog::Catalog;
use crate::db;
use crate::primitives::{AsyncLazy, WeakCache};

pub(super) type LazyCatalog = AsyncLazy<Arc<Catalog>>;

#[derive(Clone)]
pub(super) struct CatalogCache {
    pool: db::Pool,
    /// Mapping from `schema_version` to the catalog for that version of the schema.
    catalogs: Arc<WeakCache<i64, LazyCatalog>>,
}

impl CatalogCache {
    pub(super) fn new(pool: db::Pool) -> Self {
        Self {
            pool,
            catalogs: Arc::new(WeakCache::new()),
        }
    }

    pub(super) fn get(&self, snapshot_id: i64, schema_version: i64) -> Arc<LazyCatalog> {
        self.catalogs.get_or_insert_with(schema_version, || {
            let pool = self.pool.clone();
            AsyncLazy::new(move |_| {
                let pool = pool.clone();
                async move { Catalog::load(&pool, snapshot_id).await.map(Arc::new) }
            })
        })
    }
}
