use std::collections::{BTreeMap, HashMap};
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
pub struct SnapshotCache {
    pool: db::Pool,
    catalog_cache: CatalogCache,
    table_stats_cache: TableStatsCache,
    snapshots: Arc<RwLock<BTreeMap<i64, Arc<Snapshot>>>>,
}

impl SnapshotCache {
    pub async fn new(pool: db::Pool, snapshot_info: Option<SnapshotInfo>) -> DucklakeResult<Self> {
        let cache = Self {
            pool: pool.clone(),
            catalog_cache: CatalogCache::new(pool.clone()),
            table_stats_cache: TableStatsCache::new(pool),
            snapshots: Arc::new(RwLock::new(BTreeMap::new())),
        };

        // Unless a snapshot info is provided, we fetch the latest snapshot here to ensure that
        // the cache is properly initialized and we can always fetch the "current" snapshot.
        if let Some(snapshot_info) = snapshot_info {
            cache.insert_snapshot(snapshot_info);
        } else {
            cache.get_latest().await?;
        }
        Ok(cache)
    }

    /* ------------------------------------------ GET ------------------------------------------ */

    pub async fn get_latest(&self) -> DucklakeResult<Arc<Snapshot>> {
        let snapshot_info = SnapshotInfo::load_latest(&self.pool).await?;
        let snapshot = self.insert_snapshot(snapshot_info);
        Ok(snapshot)
    }

    pub fn get_current(&self) -> Arc<Snapshot> {
        let snapshots = self.snapshots.read().unwrap();
        let (_, snapshot) = snapshots.last_key_value().unwrap();
        snapshot.clone()
    }

    pub async fn get_for_schema_version(
        &self,
        schema_version: i64,
    ) -> DucklakeResult<Arc<Snapshot>> {
        // First check if we already have a snapshot for the given schema version
        if let Some(snapshot) = self
            .snapshots
            .read()
            .unwrap()
            .values()
            .find(|s| s.info().schema_version == schema_version)
        {
            return Ok(snapshot.clone());
        }

        // Try to find a live snapshot at this schema_version.
        if let Some(info) =
            SnapshotInfo::load_for_schema_version(&self.pool, schema_version).await?
        {
            return Ok(self.insert_snapshot(info));
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

    pub fn insert_snapshot(&self, snapshot_info: SnapshotInfo) -> Arc<Snapshot> {
        let snapshot = Arc::new(Snapshot::new(
            snapshot_info.clone(),
            self.catalog_cache.clone(),
            self.table_stats_cache.clone(),
        ));
        self.snapshots
            .write()
            .unwrap()
            .entry(snapshot_info.id)
            .or_insert(snapshot.clone());
        snapshot
    }

    pub fn remove_snapshots(&self, snapshot_ids: &[i64]) {
        let mut snapshots = self.snapshots.write().unwrap();
        for snapshot_id in snapshot_ids {
            snapshots.remove(snapshot_id);
        }
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                            SNAPSHOT                                           */
/* --------------------------------------------------------------------------------------------- */

pub struct Snapshot {
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

    pub fn info(&self) -> &SnapshotInfo {
        &self.info
    }

    pub async fn catalog(&self) -> DucklakeResult<&Arc<Catalog>> {
        self.catalog.get().await
    }

    pub async fn table_stats(&self) -> DucklakeResult<&Arc<HashMap<i64, TableStats>>> {
        let catalog = self.catalog.get().await?;
        self.table_stats.get_with_arg(catalog.clone()).await
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                         SNAPSHOT INFO                                         */
/* --------------------------------------------------------------------------------------------- */

#[derive(Debug, Clone)]
pub struct SnapshotInfo {
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

    pub async fn load_for_id(pool: &db::Pool, snapshot_id: i64) -> DucklakeResult<Self> {
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

    pub async fn load_for_timestamp(
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
