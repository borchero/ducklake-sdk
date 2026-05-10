/* ------------------------------------------- CREATE ------------------------------------------ */

/// Options for creating a new DuckLake instance.
pub struct CreateOptions {
    pub(crate) url: String,
    pub(crate) data_path: String,
    pub(crate) storage_options: Vec<(String, String)>,
}

impl CreateOptions {
    /// Create a new `CreateOptions` instance with the specified URL and data path.
    pub fn new(url: &str, data_path: &str) -> Self {
        Self {
            url: url.to_string(),
            data_path: data_path.to_string(),
            storage_options: Vec::new(),
        }
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
    pub(crate) storage_options: Vec<(String, String)>,
    pub(crate) connection_type: ConnectionType,
}

impl ConnectOptions {
    /// Create a new `ConnectOptions` instance with the specified URL.
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            migrate: false,
            storage_options: Vec::new(),
            connection_type: ConnectionType::Latest,
        }
    }

    /// Set whether to automatically run migrations if the catalog version is outdated.
    pub fn with_migrate(mut self, migrate: bool) -> Self {
        self.migrate = migrate;
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
