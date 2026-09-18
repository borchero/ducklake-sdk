use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use sea_query::{Asterisk, ExprTrait, Query};

use super::catalog::{CatalogCache, LazyCatalog};
use super::table_stats::{LazyTableStats, TableStatsCache};
use crate::caches::TableStats;
use crate::catalog::Catalog;
use crate::spec::*;
use crate::{DucklakeResult, db};

#[derive(Clone)]
pub(crate) struct SnapshotCache {
    pool: db::Pool,
    catalog_cache: CatalogCache,
    table_stats_cache: TableStatsCache,
    current: Arc<RwLock<Arc<Snapshot>>>,
}

impl SnapshotCache {
    pub(crate) async fn new(
        pool: db::Pool,
        snapshot_info: Option<SnapshotInfo>,
    ) -> DucklakeResult<Self> {
        let catalog_cache = CatalogCache::new(pool.clone());
        let table_stats_cache = TableStatsCache::new(pool.clone());
        let snapshot_info = match snapshot_info {
            Some(snapshot_info) => snapshot_info,
            None => SnapshotInfo::load_latest(&pool).await?,
        };
        let current = Arc::new(Snapshot::new(
            snapshot_info,
            catalog_cache.clone(),
            table_stats_cache.clone(),
        ));
        let cache = Self {
            pool,
            catalog_cache,
            table_stats_cache,
            current: Arc::new(RwLock::new(current)),
        };
        Ok(cache)
    }

    /* ------------------------------------------ GET ------------------------------------------ */

    pub(crate) async fn get_latest(&self) -> DucklakeResult<Arc<Snapshot>> {
        let snapshot_info = SnapshotInfo::load_latest(&self.pool).await?;
        let snapshot = self.insert_snapshot(snapshot_info);
        Ok(snapshot)
    }

    pub(crate) fn get_current(&self) -> Arc<Snapshot> {
        self.current.read().unwrap().clone()
    }

    pub(crate) async fn get_for_schema_version(
        &self,
        schema_version: i64,
    ) -> DucklakeResult<Arc<Snapshot>> {
        let snapshot = self.get_current();
        if snapshot.info().schema_version == schema_version {
            return Ok(snapshot);
        }

        // Try to find a live snapshot at this schema_version.
        if let Some(info) =
            SnapshotInfo::load_for_schema_version(&self.pool, schema_version).await?
        {
            return Ok(self.get_snapshot(info));
        }

        // Fall back to ducklake_schema_versions. ducklake_expire_snapshots prunes
        // ducklake_snapshot but retains ducklake_schema_versions so older data files (and
        // ducklake_inlined_data_tables) can still be projected through their historical
        // schema. The synthesized SnapshotInfo is intentionally not cached: its sentinel
        // `next_catalog_id` / `next_file_id` / `snapshot_time` are correct for the read path
        // that lands here, but would be wrong for any cache hit that later reaches the
        // table_stats accessor.
        let info = SnapshotInfo::synthesize_for_schema_version(&self.pool, schema_version).await?;
        Ok(self.new_snapshot(info))
    }

    /* ----------------------------------------- MODIFY ---------------------------------------- */

    pub(crate) fn insert_snapshot(&self, snapshot_info: SnapshotInfo) -> Arc<Snapshot> {
        let mut current = self.current.write().unwrap();
        if current.info().id == snapshot_info.id {
            return current.clone();
        }
        let snapshot = self.new_snapshot(snapshot_info);
        // A concurrent head lookup may return an older snapshot after a newer commit.
        if snapshot.info().id > current.info().id {
            *current = snapshot.clone();
        }
        snapshot
    }

    /// Resolve a snapshot without advancing the head or retaining historical snapshots.
    pub(crate) fn get_snapshot(&self, snapshot_info: SnapshotInfo) -> Arc<Snapshot> {
        let current = self.current.read().unwrap();
        if current.info().id == snapshot_info.id {
            return current.clone();
        }
        self.new_snapshot(snapshot_info)
    }

    fn new_snapshot(&self, snapshot_info: SnapshotInfo) -> Arc<Snapshot> {
        Arc::new(Snapshot::new(
            snapshot_info,
            self.catalog_cache.clone(),
            self.table_stats_cache.clone(),
        ))
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                            SNAPSHOT                                           */
/* --------------------------------------------------------------------------------------------- */

pub(crate) struct Snapshot {
    info: SnapshotInfo,
    catalog: Arc<LazyCatalog>,
    table_stats: Arc<LazyTableStats>,
}

impl Snapshot {
    fn new(
        info: SnapshotInfo,
        catalog_cache: CatalogCache,
        table_stats_cache: TableStatsCache,
    ) -> Self {
        // Acquire shared lazy values before the previous snapshot can be evicted, preserving
        // unchanged metadata without retaining the snapshot itself or loading anything eagerly.
        let catalog = catalog_cache.get(info.id, info.schema_version);
        let table_stats = table_stats_cache.get(info.id, info.schema_version, info.next_file_id);
        Self {
            info,
            catalog,
            table_stats,
        }
    }

    pub(crate) fn info(&self) -> &SnapshotInfo {
        &self.info
    }

    pub(crate) async fn catalog(&self) -> DucklakeResult<&Arc<Catalog>> {
        self.catalog.get().await
    }

    pub(crate) async fn table_stats(&self) -> DucklakeResult<&Arc<HashMap<i64, TableStats>>> {
        let catalog = self.catalog.get().await?;
        self.table_stats.get_with_arg(catalog.clone()).await
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                         SNAPSHOT INFO                                         */
/* --------------------------------------------------------------------------------------------- */

#[derive(Debug, Clone)]
pub(crate) struct SnapshotInfo {
    pub id: i64,
    pub schema_version: i64,
    pub next_catalog_id: i64,
    pub next_file_id: i64,
    pub snapshot_time: chrono::DateTime<chrono::Utc>,
}

impl SnapshotInfo {
    async fn load_latest(pool: &db::Pool) -> DucklakeResult<Self> {
        // Read the latest snapshot
        let query = Query::select()
            .column(Asterisk)
            .from(ducklake_snapshot::Table)
            .order_by(
                ducklake_snapshot::Column::SnapshotId,
                sea_query::Order::Desc,
            )
            .limit(1)
            .to_owned();
        let snapshot: DucklakeSnapshot = pool.fetch_one(&query).await?;

        // Translate into snapshot struct
        Ok(snapshot.into())
    }

    pub(crate) async fn load_for_id(pool: &db::Pool, snapshot_id: i64) -> DucklakeResult<Self> {
        // Read the snapshot for the given ID
        let query = Query::select()
            .column(Asterisk)
            .from(ducklake_snapshot::Table)
            .and_where(ducklake_snapshot::Column::SnapshotId.col().eq(snapshot_id))
            .to_owned();
        let snapshot: DucklakeSnapshot = pool.fetch_one(&query).await?;

        // Translate into snapshot struct
        Ok(snapshot.into())
    }

    pub(crate) async fn load_for_timestamp(
        pool: &db::Pool,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> DucklakeResult<Self> {
        // Read the most recent snapshot at the provided timestamp
        let query = Query::select()
            .column(Asterisk)
            .from(ducklake_snapshot::Table)
            .and_where(ducklake_snapshot::Column::SnapshotTime.col().lte(timestamp))
            .order_by(
                ducklake_snapshot::Column::SnapshotTime,
                sea_query::Order::Desc,
            )
            .limit(1)
            .to_owned();
        let snapshot: DucklakeSnapshot = pool.fetch_one(&query).await?;

        // Translate into snapshot struct
        Ok(snapshot.into())
    }

    async fn load_for_schema_version(
        pool: &db::Pool,
        schema_version: i64,
    ) -> DucklakeResult<Option<Self>> {
        // Read the latest live snapshot for the given schema version.
        let query = Query::select()
            .column(Asterisk)
            .from(ducklake_snapshot::Table)
            .and_where(
                ducklake_snapshot::Column::SchemaVersion
                    .col()
                    .eq(schema_version),
            )
            .order_by(
                ducklake_snapshot::Column::SnapshotId,
                sea_query::Order::Desc,
            )
            .limit(1)
            .to_owned();
        let snapshot: Option<DucklakeSnapshot> = pool.fetch_optional(&query).await?;
        Ok(snapshot.map(Into::into))
    }

    async fn synthesize_for_schema_version(
        pool: &db::Pool,
        schema_version: i64,
    ) -> DucklakeResult<Self> {
        // Used when ducklake_expire_snapshots has pruned every snapshot at this
        // schema_version but ducklake_schema_versions still references it. The catalog can be
        // reconstructed from any snapshot id that falls inside the schema_version's validity
        // range; we use the earliest begin_snapshot recorded for it.
        let query = Query::select()
            .column(ducklake_schema_versions::Column::BeginSnapshot)
            .from(ducklake_schema_versions::Table)
            .and_where(
                ducklake_schema_versions::Column::SchemaVersion
                    .col()
                    .eq(schema_version),
            )
            .order_by(
                ducklake_schema_versions::Column::BeginSnapshot,
                sea_query::Order::Asc,
            )
            .limit(1)
            .to_owned();
        let (begin_snapshot,): (i64,) = pool.fetch_one(&query).await?;

        // Negative next_catalog_id and next_file_id to be abundantly clear these are fake
        Ok(Self {
            id: begin_snapshot,
            schema_version,
            next_catalog_id: -1,
            next_file_id: -1,
            snapshot_time: chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap(),
        })
    }
}

impl From<DucklakeSnapshot> for SnapshotInfo {
    fn from(snapshot: DucklakeSnapshot) -> Self {
        Self {
            id: snapshot.snapshot_id,
            schema_version: snapshot.schema_version,
            next_catalog_id: snapshot.next_catalog_id,
            next_file_id: snapshot.next_file_id,
            snapshot_time: snapshot.snapshot_time.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::SnapshotCache;
    use crate::db;

    #[rstest::fixture]
    async fn cache() -> SnapshotCache {
        let pool = db::Pool::new("sqlite://:memory:").await.unwrap();
        crate::spec::init_catalog(
            &pool,
            crate::spec::InitConfig {
                data_path: "file:///tmp/ducklake-cache-test/".to_string(),
            },
        )
        .await
        .unwrap();
        SnapshotCache::new(pool, None).await.unwrap()
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn stale_snapshot_does_not_replace_current_snapshot(#[future] cache: SnapshotCache) {
        // Arrange
        let cache = cache.await;
        let stale = cache.get_current().info().clone();
        let mut latest = stale.clone();
        latest.id += 1;
        cache.insert_snapshot(latest.clone());

        // Act
        cache.insert_snapshot(stale);

        // Assert
        assert_eq!(cache.get_current().info().id, latest.id);
    }

    #[rstest::rstest]
    #[case(false, false)]
    #[case(false, true)]
    #[case(true, false)]
    #[tokio::test]
    async fn metadata_is_reused_only_while_its_version_is_retained(
        #[future] cache: SnapshotCache,
        #[case] schema_changed: bool,
        #[case] files_changed: bool,
    ) {
        // Arrange
        let cache = cache.await;
        let snapshot = cache.get_current();
        let catalog = Arc::downgrade(snapshot.catalog().await.unwrap());
        let table_stats = Arc::downgrade(snapshot.table_stats().await.unwrap());
        let old_snapshot = Arc::downgrade(&snapshot);
        let mut next_info = snapshot.info().clone();
        next_info.schema_version += i64::from(schema_changed);
        next_info.next_file_id += i64::from(files_changed);

        // Act
        drop(snapshot);
        // Advance twice without reading metadata in the intermediate snapshot.
        for _ in 0..2 {
            next_info.id += 1;
            cache.insert_snapshot(next_info.clone());
        }
        let current = cache.get_current();

        // Assert
        assert!(old_snapshot.upgrade().is_none());
        assert_eq!(catalog.upgrade().is_some(), !schema_changed);
        assert_eq!(
            table_stats.upgrade().is_some(),
            !schema_changed && !files_changed
        );
        assert_eq!(
            std::sync::Weak::ptr_eq(&catalog, &Arc::downgrade(current.catalog().await.unwrap())),
            !schema_changed,
        );
        assert_eq!(
            std::sync::Weak::ptr_eq(
                &table_stats,
                &Arc::downgrade(current.table_stats().await.unwrap())
            ),
            !schema_changed && !files_changed,
        );
        drop(current);
        drop(cache);
        assert!(catalog.upgrade().is_none());
        assert!(table_stats.upgrade().is_none());
    }
}
