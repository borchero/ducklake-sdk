use std::sync::Arc;

use sea_query::{Asterisk, ExprTrait, Iden, Query};

use super::caches::{Metadata, MetadataCache, Snapshot, SnapshotCache};
use crate::caches::SnapshotInfo;
use crate::spec::*;
use crate::*;

/// Client for interacting with a DuckLake.
pub struct Ducklake {
    conn: DucklakeConnection,
}

#[derive(Clone)]
#[repr(transparent)]
pub(crate) struct DucklakeConnection(Arc<DucklakeConnectionInner>);

struct DucklakeConnectionInner {
    /// Database connection pool which is used to execute queries against the catalog database.
    /// This pool is constant for the lifetime of the Ducklake connection.
    pool: db::Pool,
    /// Metadata queried from the catalog database upon initialization. The metadata is never
    /// re-read from the database after initialization but is updated in-memory if changes are
    /// made via this connection.
    metadata_cache: Arc<MetadataCache>,
    /// Cache (or "manager") for retrieving snapshot information from the catalog database. The
    /// cache returns Snapshot instances that allow retrieving catalogs and table stats for the
    /// snapshot. These are properly cached.
    snapshot_cache: Arc<SnapshotCache>,
    /// Storage options to use for connecting to cloud storage.
    storage_options: Vec<(String, String)>,
    /// Fixed snapshot to use for all operations. This is relevant if the user performed time
    /// travel to a particular snapshot.
    travel_snapshot: Option<Arc<Snapshot>>,
}

/* ----------------------------------------- CONNECTION ---------------------------------------- */

impl Ducklake {
    /// Create a new DuckLake by bootstrapping a new catalog database.
    ///
    /// # Arguments
    ///
    /// - `options`: Options for creating the DuckLake instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog database is already initialized. In this case, use
    /// [`Ducklake::connect`] instead.
    pub async fn create(options: CreateOptions) -> DucklakeResult<Self> {
        // Initialize the database pool and create the catalog schema if it doesn't exist
        let pool = db::Pool::new(&options.url).await?;
        if pool
            .table_exists(&spec::ducklake_metadata::Table.to_string())
            .await?
        {
            return Err(DucklakeError::CatalogAlreadyInitialized);
        }

        let config = spec::InitConfig {
            data_path: options.data_path,
        };
        spec::init_catalog(&pool, config).await?;

        // Create the ducklake instance
        Self::new(pool, None, options.storage_options).await
    }

    /// Connect to an existing DuckLake by attaching to an existing catalog database.
    ///
    /// # Arguments
    ///
    /// - `options`: Options for connecting to the catalog database.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog database is not initialized yet. In this case, use
    /// [`Ducklake::create`] instead.
    pub async fn connect(options: ConnectOptions) -> DucklakeResult<Self> {
        let pool = Self::init_catalog(&options.url, options.migrate).await?;
        let snapshot = match options.connection_type {
            ConnectionType::Latest => None,
            ConnectionType::SnapshotId(id) => Some(SnapshotInfo::load_for_id(&pool, id).await?),
            ConnectionType::SnapshotTimestamp(timestamp) => {
                Some(SnapshotInfo::load_for_timestamp(&pool, timestamp).await?)
            }
        };
        Self::new(pool, snapshot, options.storage_options).await
    }

    /// Disconnect from the catalog database, gracefully closing the underlying connection pool.
    #[cfg(feature = "python")]
    pub async fn disconnect(&mut self) {
        self.conn.0.pool.close().await;
    }

    /// Disconnect from the catalog database, gracefully closing the underlying connection pool.
    #[cfg(not(feature = "python"))]
    pub async fn disconnect(self) {
        self.conn.0.pool.close().await;
    }

    /* ----------------------------------------- SETUP ----------------------------------------- */

    async fn init_catalog(url: &str, migrate: bool) -> DucklakeResult<db::Pool> {
        // Initialize the database pool and query the current catalog version to run migrations
        let pool = db::Pool::new(url).await?;
        if !pool
            .table_exists(&spec::ducklake_metadata::Table.to_string())
            .await?
        {
            return Err(DucklakeError::CatalogNotInitialized);
        }

        // NOTE: We do not load the full metadata here but only the version field to not rely on
        //  the current schema of the ducklake_metadata table. This allows to correctly read this
        //  field for older versions of the catalog (e.g. DuckLake v0.1).
        let version = get_version(&pool).await?;
        if !spec::SUPPORTED_VERSIONS.contains(&version.as_str()) {
            return Err(DucklakeError::UnsupportedVersion(version));
        }
        if version != spec::LATEST_VERSION && !migrate {
            return Err(DucklakeError::OutdatedVersion(
                version,
                spec::LATEST_VERSION.to_string(),
            ));
        }
        spec::migrate_catalog(&pool, &version).await?;

        Ok(pool)
    }

    async fn new(
        pool: db::Pool,
        travel_snapshot: Option<SnapshotInfo>,
        storage_options: Vec<(String, String)>,
    ) -> DucklakeResult<Self> {
        let has_travel_snapshot = travel_snapshot.is_some();

        // Initialize the caches
        let snapshot_cache = SnapshotCache::new(pool.clone(), travel_snapshot).await?;
        let metadata_cache = MetadataCache::new(pool.clone()).await?;

        // Construct the ducklake
        let travel_snapshot = if has_travel_snapshot {
            Some(snapshot_cache.get_current())
        } else {
            None
        };
        let connection = DucklakeConnectionInner {
            pool,
            metadata_cache: Arc::new(metadata_cache),
            snapshot_cache: Arc::new(snapshot_cache),
            storage_options,
            travel_snapshot,
        };
        let ducklake = Ducklake {
            conn: DucklakeConnection(Arc::new(connection)),
        };
        Ok(ducklake)
    }
}

/* ---------------------------------------- TIME TRAVEL ---------------------------------------- */

impl Ducklake {
    /// Connect to the DuckLake at the provided snapshot ID.
    pub async fn at_snapshot_id(&self, snapshot_id: i64) -> DucklakeResult<Self> {
        let snapshot_info = SnapshotInfo::load_for_id(self.conn.pool(), snapshot_id).await?;
        self.at_snapshot(snapshot_info)
    }

    /// Connect to the DuckLake at the provided snapshot timestamp.
    pub async fn at_snapshot_timestamp(
        &self,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> DucklakeResult<Self> {
        let snapshot_info = SnapshotInfo::load_for_timestamp(self.conn.pool(), timestamp).await?;
        self.at_snapshot(snapshot_info)
    }

    fn at_snapshot(&self, snapshot_info: SnapshotInfo) -> DucklakeResult<Self> {
        let travel_snapshot = self.conn.0.snapshot_cache.insert_snapshot(snapshot_info);
        let connection = DucklakeConnectionInner {
            pool: self.conn.0.pool.clone(),
            metadata_cache: self.conn.0.metadata_cache.clone(),
            snapshot_cache: self.conn.0.snapshot_cache.clone(),
            storage_options: self.conn.0.storage_options.clone(),
            travel_snapshot: Some(travel_snapshot),
        };
        let ducklake = Ducklake {
            conn: DucklakeConnection(Arc::new(connection)),
        };
        Ok(ducklake)
    }
}

/* -------------------------------------- SNAPSHOT QUERIES ------------------------------------- */

/// Metadata for a snapshot in the catalog.
pub struct SnapshotMetadata {
    /// The unique identifier of the snapshot.
    pub id: i64,
    /// The time at which the snapshot was committed.
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Ducklake {
    /// Obtain the ID and timestamp of the latest snapshot in the catalog. This fails when run on
    /// an immutable DuckLake instance.
    pub async fn latest_snapshot(&self) -> DucklakeResult<SnapshotMetadata> {
        let snapshot = self.conn.latest_snapshot(false).await?;
        let info = snapshot.info();
        Ok(SnapshotMetadata {
            id: info.id,
            timestamp: info.snapshot_time,
        })
    }

    /// List all snapshots in the catalog.
    pub async fn list_snapshots(&self) -> DucklakeResult<Vec<SnapshotMetadata>> {
        if let Some(travel_snapshot) = &self.conn.0.travel_snapshot {
            let info = travel_snapshot.info();
            Ok(vec![SnapshotMetadata {
                id: info.id,
                timestamp: info.snapshot_time,
            }])
        } else {
            list_snapshots(self.conn.pool()).await
        }
    }
}

/* ---------------------------------------- READ QUERIES --------------------------------------- */

impl Ducklake {
    /// Get a handle to the table with the provided name.
    pub async fn table(
        &self,
        name: impl TryInto<TableName, Error = impl Into<DucklakeError>>,
    ) -> DucklakeResult<Table> {
        let name = name.try_into().map_err(|e| e.into())?;
        let snapshot = self.conn.latest_snapshot(true).await?;
        let catalog = snapshot.catalog().await?;
        let schema_id = catalog.schema(&name.schema)?.id().unwrap();
        let table_id = catalog.table(&name)?.id().unwrap();
        Ok(Table::new(self.conn.clone(), schema_id, table_id))
    }

    /// List all tables in the catalog, optionally restricted to a specific schema.
    pub async fn list_tables(&self, schema: Option<&str>) -> DucklakeResult<Vec<Table>> {
        let snapshot = self.conn.latest_snapshot(true).await?;
        let catalog = snapshot.catalog().await?;
        let table_ids = catalog.list_table_ids(schema);
        let tables = table_ids
            .into_iter()
            .map(|id| {
                catalog.table(id).map(|table| {
                    let schema_id = catalog.schema(&table.name().schema).unwrap().id().unwrap();
                    Table::new(self.conn.clone(), schema_id, id)
                })
            })
            .collect::<DucklakeResult<Vec<_>>>()?;
        Ok(tables)
    }

    /// List the names of all schemas in the catalog.
    pub async fn list_schemas(&self) -> DucklakeResult<Vec<String>> {
        let snapshot = self.conn.latest_snapshot(true).await?;
        let catalog = snapshot.catalog().await?;
        Ok(catalog.list_schema_names())
    }
}

impl DucklakeConnection {
    pub(crate) fn pool(&self) -> &db::Pool {
        &self.0.pool
    }

    pub(crate) fn snapshot_cache(&self) -> &SnapshotCache {
        &self.0.snapshot_cache
    }
}

/* ---------------------------------------- TRANSACTIONS --------------------------------------- */

impl Ducklake {
    /// Start a new transaction to make changes to the catalog.
    pub async fn transaction(&self) -> DucklakeResult<Transaction<'_>> {
        self.conn.transaction(None).await
    }

    /// Start a new transaction with the provided author information. The author information is
    /// attached to the snapshot created when the transaction is committed.
    pub async fn transaction_with_author(
        &self,
        author_info: AuthorInfo,
    ) -> DucklakeResult<Transaction<'_>> {
        self.conn.transaction(Some(author_info)).await
    }
}

impl DucklakeConnection {
    /// Start a new transaction to make changes to the catalog. If `author_info` is provided, it
    /// is attached to the snapshot created when the transaction is committed.
    pub async fn transaction(
        &self,
        author_info: Option<AuthorInfo>,
    ) -> DucklakeResult<Transaction<'_>> {
        let snapshot = self.latest_snapshot(false).await?;
        let metadata = self.0.metadata_cache.get_metadata();
        let tx = Transaction::new(
            &self.0.snapshot_cache,
            &self.0.pool,
            &self.0.storage_options,
            metadata,
            author_info.unwrap_or_default(),
            snapshot,
        )
        .await?;
        Ok(tx)
    }

    /// Read the latest snapshot from the database and return it. Future calls to
    /// [`DucklakeConnection::current_snapshot`] will also return this snapshot.
    pub async fn latest_snapshot(
        &self,
        tolerate_immutable: bool,
    ) -> DucklakeResult<Arc<Snapshot>> {
        if let Some(travel_snapshot) = &self.0.travel_snapshot {
            if !tolerate_immutable {
                return Err(DucklakeError::ImmutableDucklake);
            }
            Ok(travel_snapshot.clone())
        } else {
            self.0.snapshot_cache.get_latest().await
        }
    }

    /// Read the current snapshot from the in-memory cache. This might be stale if another
    /// process has committed a new snapshot since the last time
    /// [`DucklakeConnection::latest_snapshot`] was called.
    pub fn current_snapshot(&self) -> Arc<Snapshot> {
        if let Some(travel_snapshot) = &self.0.travel_snapshot {
            travel_snapshot.clone()
        } else {
            self.0.snapshot_cache.get_current()
        }
    }
}

/* --------------------------------------- PUBLIC METHODS -------------------------------------- */

macro_rules! within_transaction {
    ($(
        $(#[$meta:meta])*
        fn $name:ident($($arg:ident: $ty:ty),*) -> $ret:ty;
    )*) => {
        impl Ducklake {
            $(
            $(#[$meta])*
            pub async fn $name(&self, $($arg: $ty),*) -> $ret {
                let mut tx = self.transaction().await?;
                let result = tx.$name($($arg),*)?;
                tx.commit().await?;
                Ok(result)
            }
            )*
        }
    };
}

within_transaction! {
    /// Create a new schema in the catalog.
    fn create_schema(name: &str, path: Option<String>) -> DucklakeResult<()>;
    /// Delete an existing schema from the catalog.
    fn delete_schema(name: &str) -> DucklakeResult<()>;
}

impl Ducklake {
    /// Create a new table in the catalog.
    pub async fn create_table(
        &self,
        name: impl TryInto<TableName, Error = impl Into<DucklakeError>>,
        columns: Vec<Column>,
        partition_columns: Option<Vec<PartitionColumn>>,
        path: Option<String>,
        tags: Option<Vec<Tag>>,
    ) -> DucklakeResult<()> {
        let mut tx = self.transaction().await?;
        tx.create_table(name, columns, partition_columns, path, tags)?;
        tx.commit().await?;
        Ok(())
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                            METADATA                                           */
/* --------------------------------------------------------------------------------------------- */

/* -------------------------------------------- GET -------------------------------------------- */

impl DucklakeConnection {
    pub(crate) fn metadata(&self) -> Arc<Metadata> {
        self.0.metadata_cache.get_metadata()
    }
}

/* -------------------------------------------- SET -------------------------------------------- */

impl Ducklake {
    /// Set a metadata option at the global or schema scope.
    pub async fn set_metadata(
        &self,
        key: &str,
        value: &str,
        schema: Option<&str>,
    ) -> DucklakeResult<()> {
        if let Some(schema_name) = schema {
            let snapshot = self.conn.latest_snapshot(true).await?;
            let catalog = snapshot.catalog().await?;
            let schema_id = catalog.schema(schema_name)?.id().unwrap();
            self.conn
                .0
                .metadata_cache
                .set_schema(schema_id, key.to_string(), value.to_string())
                .await?;
        } else {
            self.conn
                .0
                .metadata_cache
                .set_global(key.to_string(), value.to_string())
                .await?;
        }
        Ok(())
    }

    /// Unset a metadata option at the global or schema scope.
    pub async fn unset_metadata(&self, key: &str, schema: Option<&str>) -> DucklakeResult<()> {
        if let Some(schema_name) = schema {
            let snapshot = self.conn.latest_snapshot(true).await?;
            let catalog = snapshot.catalog().await?;
            let schema_id = catalog.schema(schema_name)?.id().unwrap();
            self.conn
                .0
                .metadata_cache
                .unset_schema(schema_id, key)
                .await?;
        } else {
            self.conn.0.metadata_cache.unset_global(key).await?;
        }
        Ok(())
    }
}

impl DucklakeConnection {
    pub(crate) async fn set_table_metadata(
        &self,
        key: &str,
        value: &str,
        table_id: i64,
    ) -> DucklakeResult<()> {
        self.0
            .metadata_cache
            .set_table(table_id, key.to_string(), value.to_string())
            .await
    }

    pub(crate) async fn unset_table_metadata(
        &self,
        key: &str,
        table_id: i64,
    ) -> DucklakeResult<()> {
        self.0.metadata_cache.unset_table(table_id, key).await
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                            QUERIES                                            */
/* --------------------------------------------------------------------------------------------- */

async fn get_version(pool: &db::Pool) -> DucklakeResult<String> {
    let query = Query::select()
        .column(ducklake_metadata::Column::Value)
        .from(ducklake_metadata::Table)
        .and_where(
            ducklake_metadata::Column::Key
                .col()
                .eq(spec::metadata::VERSION),
        )
        .to_owned();
    let version: (String,) = pool.fetch_one(&query).await?;
    Ok(version.0)
}

async fn list_snapshots(pool: &db::Pool) -> DucklakeResult<Vec<SnapshotMetadata>> {
    let query = Query::select()
        .column(Asterisk)
        .from(ducklake_snapshot::Table)
        .order_by(
            ducklake_snapshot::Column::SnapshotId,
            sea_query::Order::Desc,
        )
        .to_owned();
    let snapshots: Vec<DucklakeSnapshot> = pool.fetch_all(&query).await?;
    Ok(snapshots
        .into_iter()
        .map(|snapshot| SnapshotMetadata {
            id: snapshot.snapshot_id,
            timestamp: snapshot.snapshot_time.0,
        })
        .collect())
}
