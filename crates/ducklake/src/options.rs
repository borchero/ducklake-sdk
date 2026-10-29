use std::time::Duration;

/* ------------------------------------------- CREATE ------------------------------------------ */

/// Options for creating a new DuckLake instance.
pub struct CreateOptions {
    pub(crate) url: String,
    pub(crate) data_path: String,
    pub(crate) storage_options: Vec<(String, String)>,
    pub(crate) time_zone: chrono_tz::Tz,
    pub(crate) database_options: DatabaseOptions,
}

impl CreateOptions {
    /// Create a new `CreateOptions` instance with the specified URL and data path.
    pub fn new(url: &str, data_path: &str) -> Self {
        Self {
            url: url.to_string(),
            data_path: data_path.to_string(),
            storage_options: Vec::new(),
            time_zone: chrono_tz::UTC,
            database_options: DatabaseOptions::default(),
        }
    }

    /// Set the time zone used to represent timezone-aware timestamps when reading data.
    ///
    /// The default is `UTC`. This setting is local to the connection and is not persisted in the
    /// DuckLake catalog.
    ///
    /// # Errors
    ///
    /// Returns [`crate::DucklakeError::InvalidTimeZone`] if `time_zone` is not a valid IANA time
    /// zone name.
    pub fn with_time_zone(mut self, time_zone: &str) -> crate::DucklakeResult<Self> {
        self.time_zone = time_zone
            .parse()
            .map_err(|_| crate::DucklakeError::InvalidTimeZone(time_zone.to_string()))?;
        Ok(self)
    }

    /// Add a storage option to the `CreateOptions`.
    pub fn with_storage_option(mut self, key: &str, value: &str) -> Self {
        self.storage_options
            .push((key.to_string(), value.to_string()));
        self
    }

    /// Add multiple storage options to the `CreateOptions`.
    pub fn with_storage_options(mut self, options: Vec<(String, String)>) -> Self {
        self.storage_options.extend(options);
        self
    }

    /// Configure the catalog database connection pool.
    pub fn with_database_options(mut self, options: DatabaseOptions) -> Self {
        self.database_options = options;
        self
    }
}

/* ------------------------------------------ CONNECT ------------------------------------------ */

pub(crate) enum ConnectionType {
    Latest,
    SnapshotId(i64),
    SnapshotTimestamp(chrono::DateTime<chrono::Utc>),
}

/// Options for connecting to an existing DuckLake instance.
pub struct ConnectOptions {
    pub(crate) url: String,
    pub(crate) migrate: bool,
    pub(crate) readonly: bool,
    pub(crate) storage_options: Vec<(String, String)>,
    pub(crate) connection_type: ConnectionType,
    pub(crate) time_zone: chrono_tz::Tz,
    pub(crate) database_options: DatabaseOptions,
}

impl ConnectOptions {
    /// Create a new `ConnectOptions` instance with the specified URL.
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            migrate: false,
            readonly: false,
            storage_options: Vec::new(),
            connection_type: ConnectionType::Latest,
            time_zone: chrono_tz::UTC,
            database_options: DatabaseOptions::default(),
        }
    }

    /// Configure the catalog database connection pool.
    ///
    /// Invalid settings are rejected when connecting to the DuckLake, before opening a connection.
    pub fn with_database_options(mut self, options: DatabaseOptions) -> Self {
        self.database_options = options;
        self
    }

    /// Set the time zone used to represent timezone-aware timestamps when reading data.
    ///
    /// The default is `UTC`. This setting is local to the connection and is not read from or
    /// persisted in the DuckLake catalog.
    ///
    /// # Errors
    ///
    /// Returns [`crate::DucklakeError::InvalidTimeZone`] if `time_zone` is not a valid IANA time
    /// zone name.
    pub fn with_time_zone(mut self, time_zone: &str) -> crate::DucklakeResult<Self> {
        self.time_zone = time_zone
            .parse()
            .map_err(|_| crate::DucklakeError::InvalidTimeZone(time_zone.to_string()))?;
        Ok(self)
    }

    /// Set whether to automatically run migrations if the catalog version is outdated.
    pub fn with_migrate(mut self, migrate: bool) -> Self {
        self.migrate = migrate;
        self
    }

    /// Set whether the connection should be read-only. A read-only connection follows the latest
    /// snapshot for reads but rejects all write operations.
    pub fn with_readonly(mut self, readonly: bool) -> Self {
        self.readonly = readonly;
        self
    }

    /// Add a storage option to the `ConnectOptions`.
    pub fn with_storage_option(mut self, key: &str, value: &str) -> Self {
        self.storage_options
            .push((key.to_string(), value.to_string()));
        self
    }

    /// Add multiple storage options to the `ConnectOptions`.
    pub fn with_storage_options(mut self, options: Vec<(String, String)>) -> Self {
        self.storage_options.extend(options);
        self
    }

    /// Connect to the latest state of the catalog (default).
    pub fn with_latest_snapshot(mut self) -> Self {
        self.connection_type = ConnectionType::Latest;
        self
    }

    /// Connect to the state of the catalog at the specified snapshot ID.
    pub fn with_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.connection_type = ConnectionType::SnapshotId(snapshot_id);
        self
    }

    /// Connect to the state of the catalog at the specified snapshot timestamp.
    pub fn with_snapshot_timestamp(mut self, timestamp: chrono::DateTime<chrono::Utc>) -> Self {
        self.connection_type = ConnectionType::SnapshotTimestamp(timestamp);
        self
    }
}

/* -------------------------------------- DATABASE OPTIONS ------------------------------------- */

/// Connection pool settings for the catalog database.
#[derive(Clone, Debug)]
pub struct DatabaseOptions {
    /// Maximum connections in the pool. `None` selects four for PostgreSQL/MySQL and one for
    /// SQLite. Must be greater than zero.
    pub max_connections: Option<u32>,
    /// Minimum connections to maintain. Defaults to zero and must not exceed the maximum.
    pub min_connections: u32,
    /// Maximum time to acquire a connection, including connection establishment and health
    /// checks. Defaults to 30 seconds. This does not limit query execution time.
    pub acquire_timeout: Duration,
    /// Check idle connections for liveness before acquiring them. Defaults to true.
    pub test_before_acquire: bool,
    /// Close connections idle for longer than this duration, subject to periodic pool cleanup
    /// and `min_connections`. Defaults to ten minutes. `None` disables idle expiry.
    pub idle_timeout: Option<Duration>,
    /// Maximum connection lifetime before recycling. Defaults to 30 minutes. `None` disables
    /// recycling. Connections in use are not interrupted.
    pub max_lifetime: Option<Duration>,
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            max_connections: None,
            min_connections: 0,
            acquire_timeout: Duration::from_secs(30),
            test_before_acquire: true,
            idle_timeout: Some(Duration::from_secs(10 * 60)),
            max_lifetime: Some(Duration::from_secs(30 * 60)),
        }
    }
}
