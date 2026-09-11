use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

use sea_query::{Asterisk, ExprTrait, Query};

use super::catalog::CatalogCache;
use super::table_stats::TableStatsCache;
use crate::caches::TableStats;
use crate::catalog::Catalog;
use crate::primitives::AsyncLazy;
use crate::spec::*;
use crate::{DucklakeResult, db};

#[derive(Clone)]
pub(crate) struct SnapshotCache {
    pool: db::Pool,
    catalog_cache: CatalogCache,
    table_stats_cache: TableStatsCache,
    snapshots: Arc<RwLock<SnapshotCacheState>>,
    capacity: usize,
}

struct SnapshotCacheState {
    current: Arc<Snapshot>,
    historical: HashMap<i64, Arc<Snapshot>>,
    historical_lru: VecDeque<i64>,
}

impl SnapshotCache {
    pub(crate) async fn new(
        pool: db::Pool,
        snapshot_info: Option<SnapshotInfo>,
        capacity: usize,
    ) -> DucklakeResult<Self> {
        debug_assert!(capacity > 0);
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
            snapshots: Arc::new(RwLock::new(SnapshotCacheState {
                current,
                historical: HashMap::new(),
                historical_lru: VecDeque::new(),
            })),
            capacity,
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
        self.snapshots.read().unwrap().current.clone()
    }

    pub(crate) async fn get_for_schema_version(
        &self,
        schema_version: i64,
    ) -> DucklakeResult<Arc<Snapshot>> {
        // First check if we already have a snapshot for the given schema version
        if let Some(snapshot) = self.get_for_cached_schema_version(schema_version) {
            return Ok(snapshot);
        }

        // Try to find a live snapshot at this schema_version.
        if let Some(info) =
            SnapshotInfo::load_for_schema_version(&self.pool, schema_version).await?
        {
            return Ok(self.insert_historical_snapshot(info));
        }

        // Fall back to ducklake_schema_versions. ducklake_expire_snapshots prunes
        // ducklake_snapshot but retains ducklake_schema_versions so older data files (and
        // ducklake_inlined_data_tables) can still be projected through their historical
        // schema. The synthesized SnapshotInfo is intentionally not cached: its sentinel
        // `next_catalog_id` / `next_file_id` / `snapshot_time` are correct for the read path
        // that lands here, but would be wrong for any cache hit that later reaches the
        // table_stats accessor.
        let info = SnapshotInfo::synthesize_for_schema_version(&self.pool, schema_version).await?;
        Ok(Arc::new(Snapshot::new(
            info,
            self.catalog_cache.clone(),
            self.table_stats_cache.clone(),
        )))
    }

    /* ----------------------------------------- MODIFY ---------------------------------------- */

    pub(crate) fn insert_snapshot(&self, snapshot_info: SnapshotInfo) -> Arc<Snapshot> {
        let mut snapshots = self.snapshots.write().unwrap();
        if snapshots.current.info().id == snapshot_info.id {
            return snapshots.current.clone();
        }
        if snapshots.current.info().id > snapshot_info.id {
            if let Some(snapshot) = snapshots.get_historical(snapshot_info.id) {
                return snapshot;
            }
            let snapshot = self.new_snapshot(snapshot_info);
            snapshots.insert_historical(snapshot.clone(), self.capacity.saturating_sub(1));
            return snapshot;
        }

        let snapshot = snapshots
            .remove_historical(snapshot_info.id)
            .unwrap_or_else(|| self.new_snapshot(snapshot_info));
        let previous = std::mem::replace(&mut snapshots.current, snapshot.clone());
        snapshots.insert_historical(previous, self.capacity.saturating_sub(1));
        snapshot
    }

    pub(crate) fn insert_historical_snapshot(&self, snapshot_info: SnapshotInfo) -> Arc<Snapshot> {
        let mut snapshots = self.snapshots.write().unwrap();
        if snapshots.current.info().id == snapshot_info.id {
            return snapshots.current.clone();
        }
        if let Some(snapshot) = snapshots.get_historical(snapshot_info.id) {
            return snapshot;
        }

        let snapshot = self.new_snapshot(snapshot_info);
        snapshots.insert_historical(snapshot.clone(), self.capacity.saturating_sub(1));
        snapshot
    }

    pub(crate) fn remove_snapshots(&self, snapshot_ids: &[i64]) {
        let mut snapshots = self.snapshots.write().unwrap();
        for snapshot_id in snapshot_ids {
            snapshots.remove_historical(*snapshot_id);
        }
    }

    fn get_for_cached_schema_version(&self, schema_version: i64) -> Option<Arc<Snapshot>> {
        let mut snapshots = self.snapshots.write().unwrap();
        if snapshots.current.info().schema_version == schema_version {
            return Some(snapshots.current.clone());
        }
        let snapshot_id = snapshots
            .historical
            .values()
            .find(|snapshot| snapshot.info().schema_version == schema_version)
            .map(|snapshot| snapshot.info().id)?;
        snapshots.get_historical(snapshot_id)
    }

    fn new_snapshot(&self, snapshot_info: SnapshotInfo) -> Arc<Snapshot> {
        Arc::new(Snapshot::new(
            snapshot_info,
            self.catalog_cache.clone(),
            self.table_stats_cache.clone(),
        ))
    }
}

impl SnapshotCacheState {
    fn get_historical(&mut self, snapshot_id: i64) -> Option<Arc<Snapshot>> {
        let snapshot = self.historical.get(&snapshot_id)?.clone();
        self.touch(snapshot_id);
        Some(snapshot)
    }

    fn insert_historical(&mut self, snapshot: Arc<Snapshot>, capacity: usize) {
        if capacity == 0 || snapshot.info().id == self.current.info().id {
            return;
        }
        let snapshot_id = snapshot.info().id;
        self.historical.insert(snapshot_id, snapshot);
        self.touch(snapshot_id);
        while self.historical.len() > capacity {
            if let Some(expired_id) = self.historical_lru.pop_front() {
                self.historical.remove(&expired_id);
            }
        }
    }

    fn remove_historical(&mut self, snapshot_id: i64) -> Option<Arc<Snapshot>> {
        self.historical_lru.retain(|id| *id != snapshot_id);
        self.historical.remove(&snapshot_id)
    }

    fn touch(&mut self, snapshot_id: i64) {
        self.historical_lru.retain(|id| *id != snapshot_id);
        self.historical_lru.push_back(snapshot_id);
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                            SNAPSHOT                                           */
/* --------------------------------------------------------------------------------------------- */

pub(crate) struct Snapshot {
    info: SnapshotInfo,
    catalog: AsyncLazy<Arc<Catalog>>,
    table_stats: AsyncLazy<Arc<HashMap<i64, TableStats>>, Arc<Catalog>>,
}

impl Snapshot {
    fn new(
        info: SnapshotInfo,
        catalog_cache: CatalogCache,
        table_stats_cache: TableStatsCache,
    ) -> Self {
        let lazy_catalog = AsyncLazy::new(move |_| {
            let cache = catalog_cache.clone();
            async move { cache.get(info.id, info.schema_version).await }
        });
        let lazy_table_stats = AsyncLazy::new(move |catalog: Arc<Catalog>| {
            let cache = table_stats_cache.clone();
            async move { cache.get(info.id, info.next_file_id, &catalog).await }
        });
        Self {
            info,
            catalog: lazy_catalog,
            table_stats: lazy_table_stats,
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

    use chrono::{TimeZone, Utc};

    use super::{SnapshotCache, SnapshotInfo};
    use crate::db;

    fn snapshot_info(id: i64) -> SnapshotInfo {
        SnapshotInfo {
            id,
            schema_version: id,
            next_catalog_id: id,
            next_file_id: id,
            snapshot_time: Utc.timestamp_opt(id, 0).unwrap(),
        }
    }

    #[tokio::test]
    async fn historical_snapshot_is_released_when_no_connection_pins_it() {
        let pool = db::Pool::new("sqlite://:memory:").await.unwrap();
        let cache = SnapshotCache::new(pool, Some(snapshot_info(2)), 1)
            .await
            .unwrap();
        let historical = {
            let historical = cache.insert_historical_snapshot(snapshot_info(1));
            Arc::downgrade(&historical)
        };

        assert_eq!(cache.get_current().info().id, 2);
        assert!(historical.upgrade().is_none());
    }

    #[tokio::test]
    async fn inserting_an_existing_snapshot_reuses_its_arc() {
        let pool = db::Pool::new("sqlite://:memory:").await.unwrap();
        let cache = SnapshotCache::new(pool, Some(snapshot_info(2)), 2)
            .await
            .unwrap();
        let first = cache.insert_historical_snapshot(snapshot_info(1));

        let second = cache.insert_historical_snapshot(snapshot_info(1));

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn current_snapshot_is_protected_from_historical_eviction() {
        let pool = db::Pool::new("sqlite://:memory:").await.unwrap();
        let cache = SnapshotCache::new(pool, Some(snapshot_info(3)), 2)
            .await
            .unwrap();

        cache.insert_historical_snapshot(snapshot_info(1));
        cache.insert_historical_snapshot(snapshot_info(2));

        assert_eq!(cache.get_current().info().id, 3);
        assert_eq!(cache.snapshots.read().unwrap().historical.len(), 1);
        assert!(cache.snapshots.read().unwrap().historical.contains_key(&2));
    }

    #[tokio::test]
    async fn stale_latest_snapshot_does_not_replace_current_snapshot() {
        let pool = db::Pool::new("sqlite://:memory:").await.unwrap();
        let cache = SnapshotCache::new(pool, Some(snapshot_info(3)), 2)
            .await
            .unwrap();

        let stale = cache.insert_snapshot(snapshot_info(2));

        assert_eq!(stale.info().id, 2);
        assert_eq!(cache.get_current().info().id, 3);
    }

    #[tokio::test]
    async fn historical_cache_evicts_the_least_recently_used_snapshot() {
        let pool = db::Pool::new("sqlite://:memory:").await.unwrap();
        let cache = SnapshotCache::new(pool, Some(snapshot_info(4)), 3)
            .await
            .unwrap();
        cache.insert_historical_snapshot(snapshot_info(1));
        cache.insert_historical_snapshot(snapshot_info(2));
        cache.insert_historical_snapshot(snapshot_info(1));

        cache.insert_historical_snapshot(snapshot_info(3));

        let snapshots = cache.snapshots.read().unwrap();
        assert!(snapshots.historical.contains_key(&1));
        assert!(!snapshots.historical.contains_key(&2));
        assert!(snapshots.historical.contains_key(&3));
    }

    #[tokio::test]
    async fn pinned_historical_snapshot_survives_shared_cache_eviction() {
        let pool = db::Pool::new("sqlite://:memory:").await.unwrap();
        let cache = SnapshotCache::new(pool, Some(snapshot_info(3)), 1)
            .await
            .unwrap();
        let pinned = cache.insert_historical_snapshot(snapshot_info(1));
        let pinned_weak = Arc::downgrade(&pinned);

        cache.insert_historical_snapshot(snapshot_info(2));

        assert_eq!(cache.get_current().info().id, 3);
        assert_eq!(pinned.info().id, 1);
        assert!(pinned_weak.upgrade().is_some());
        drop(pinned);
        assert!(pinned_weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn catalog_and_table_stats_are_released_with_evicted_snapshot() {
        let pool = db::Pool::new("sqlite://:memory:").await.unwrap();
        crate::spec::init_catalog(
            &pool,
            crate::spec::InitConfig {
                data_path: "file:///tmp/ducklake-cache-test/".to_string(),
            },
        )
        .await
        .unwrap();
        let cache = SnapshotCache::new(pool, None, 1).await.unwrap();
        let snapshot = cache.get_current();
        let catalog = {
            let catalog = snapshot.catalog().await.unwrap().clone();
            Arc::downgrade(&catalog)
        };
        let table_stats = {
            let table_stats = snapshot.table_stats().await.unwrap().clone();
            Arc::downgrade(&table_stats)
        };

        cache.insert_snapshot(snapshot_info(1));
        drop(snapshot);

        assert!(catalog.upgrade().is_none());
        assert!(table_stats.upgrade().is_none());
    }
}
