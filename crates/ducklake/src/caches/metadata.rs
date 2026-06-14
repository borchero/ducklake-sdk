use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use sea_query::{Asterisk, ExprTrait, Query};

use crate::spec::*;
use crate::{DucklakeError, DucklakeResult, Interval, db, io, spec};

pub(crate) struct MetadataCache {
    pool: db::Pool,
    metadata: RwLock<Arc<Metadata>>,
}

impl MetadataCache {
    pub(crate) async fn new(pool: db::Pool) -> DucklakeResult<Self> {
        let metadata = Metadata::load(&pool).await?;
        Ok(Self {
            pool,
            metadata: RwLock::new(Arc::new(metadata)),
        })
    }

    pub(crate) fn get_metadata(&self) -> Arc<Metadata> {
        self.metadata.read().unwrap().clone()
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                            METADATA                                           */
/* --------------------------------------------------------------------------------------------- */

#[derive(Clone, Debug)]
pub(crate) struct Metadata {
    global: HashMap<String, String>,
    schema: HashMap<i64, HashMap<String, String>>,
    table: HashMap<i64, HashMap<String, String>>,
}

/// Resolved metadata configuration for a single table, combining global, schema-level, and
/// table-level metadata overrides.
#[derive(Clone, Debug)]
pub struct TableMetadata {
    /// Maximum number of rows that may be inlined into the catalog instead of being written to a
    /// data file.
    pub data_inlining_row_limit: u64,
    /// Target size of data files in bytes.
    pub target_file_size: u64,
    /// Target size of Parquet row groups in bytes, if any.
    pub parquet_row_group_size_bytes: Option<u64>,
    /// Target number of rows per Parquet row group.
    pub parquet_row_group_size: u64,
    /// Compression codec to use when writing Parquet files.
    pub parquet_compression: String,
    /// Compression level to use when writing Parquet files.
    pub parquet_compression_level: i64,
    /// Parquet format version to use when writing Parquet files.
    pub parquet_version: i64,
    /// Whether to use the Hive file pattern when writing partitioned data files.
    pub hive_file_pattern: bool,
    /// Fraction of rows that need to be deleted in a data file before it is rewritten.
    pub rewrite_delete_threshold: f64,
    /// Whether to automatically compact small data files.
    pub auto_compact: bool,
}

// Defaults taken from https://ducklake.select/docs/stable/duckdb/usage/configuration#ducklake-specific-configuration
const DEFAULT_DATA_INLINING_ROW_LIMIT: u64 = 10;
const DEFAULT_TARGET_FILE_SIZE: u64 = 512 * 1024 * 1024;
const DEFAULT_PARQUET_ROW_GROUP_SIZE: u64 = 122880;
const DEFAULT_PARQUET_COMPRESSION: &str = "snappy";
const DEFAULT_PARQUET_COMPRESSION_LEVEL: i64 = 3;
const DEFAULT_PARQUET_VERSION: i64 = 1;
const DEFAULT_HIVE_FILE_PATTERN: bool = true;
const DEFAULT_REWRITE_DELETE_THRESHOLD: f64 = 0.95;
const DEFAULT_AUTO_COMPACT: bool = true;
const DEFAULT_DELETE_OLDER_THAN: &str = "2 days";

/* -------------------------------------------- LOAD ------------------------------------------- */

impl Metadata {
    async fn load(pool: &db::Pool) -> DucklakeResult<Self> {
        let query = Query::select()
            .column(Asterisk)
            .from(ducklake_metadata::Table)
            .to_owned();
        let entries = pool.fetch_all(&query).await?;
        Ok(Self::new(entries))
    }

    fn new(entries: Vec<DucklakeMetadata>) -> Self {
        // Partition into outputs
        let (global_entries, other_entries): (Vec<_>, Vec<_>) =
            entries.into_iter().partition(|e| e.scope.is_none());
        let (schema_entries, table_entries): (Vec<_>, Vec<_>) = other_entries
            .into_iter()
            .partition(|e| e.scope.as_ref().unwrap() == "schema");

        // Process outputs into hashmaps
        let global = global_entries
            .into_iter()
            .map(|e| (e.key, e.value))
            .collect();

        let mut schema: HashMap<i64, HashMap<String, String>> = HashMap::new();
        for e in schema_entries {
            schema
                .entry(e.scope_id.unwrap())
                .or_default()
                .insert(e.key, e.value);
        }

        let mut table: HashMap<i64, HashMap<String, String>> = HashMap::new();
        for e in table_entries {
            table
                .entry(e.scope_id.unwrap())
                .or_default()
                .insert(e.key, e.value);
        }

        Metadata {
            global,
            schema,
            table,
        }
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                           ACCESSORS                                           */
/* --------------------------------------------------------------------------------------------- */

impl Metadata {
    pub(crate) fn data_path(&self) -> io::DucklakePath {
        self.global
            .get(spec::metadata::DATA_PATH)
            .map(|s| s.parse().unwrap())
            .unwrap_or_default()
    }

    pub(crate) fn expire_older_than(&self) -> Option<Interval> {
        self.global
            .get(spec::metadata::EXPIRE_OLDER_THAN)
            .and_then(|s| literals::parse(s).ok().flatten())
    }

    pub(crate) fn delete_older_than(&self) -> Interval {
        self.global
            .get(spec::metadata::DELETE_OLDER_THAN)
            .and_then(|s| literals::parse(s).ok().flatten())
            .unwrap_or_else(|| {
                // SAFETY: `DEFAULT_DELETE_OLDER_THAN` is a valid interval literal.
                literals::parse(DEFAULT_DELETE_OLDER_THAN)
                    .ok()
                    .flatten()
                    .unwrap()
            })
    }

    pub(crate) fn table_metadata(
        &self,
        schema_id: Option<i64>,
        table_id: Option<i64>,
    ) -> TableMetadata {
        TableMetadata {
            data_inlining_row_limit: self
                .get_key(spec::metadata::DATA_INLINING_ROW_LIMIT, schema_id, table_id)
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_DATA_INLINING_ROW_LIMIT),
            target_file_size: self
                .get_key(spec::metadata::TARGET_FILE_SIZE, schema_id, table_id)
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_TARGET_FILE_SIZE),
            parquet_row_group_size_bytes: self
                .get_key(
                    spec::metadata::PARQUET_ROW_GROUP_SIZE_BYTES,
                    schema_id,
                    table_id,
                )
                .and_then(|s| s.parse().ok()),
            parquet_row_group_size: self
                .get_key(spec::metadata::PARQUET_ROW_GROUP_SIZE, schema_id, table_id)
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_PARQUET_ROW_GROUP_SIZE),
            parquet_compression: self
                .get_key(spec::metadata::PARQUET_COMPRESSION, schema_id, table_id)
                .unwrap_or(DEFAULT_PARQUET_COMPRESSION)
                .to_string(),
            parquet_compression_level: self
                .get_key(
                    spec::metadata::PARQUET_COMPRESSION_LEVEL,
                    schema_id,
                    table_id,
                )
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_PARQUET_COMPRESSION_LEVEL),
            parquet_version: self
                .get_key(spec::metadata::PARQUET_VERSION, schema_id, table_id)
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_PARQUET_VERSION),
            hive_file_pattern: self
                .get_key(spec::metadata::HIVE_FILE_PATTERN, schema_id, table_id)
                .map(|s| s == "true")
                .unwrap_or(DEFAULT_HIVE_FILE_PATTERN),
            rewrite_delete_threshold: self
                .get_key(
                    spec::metadata::REWRITE_DELETE_THRESHOLD,
                    schema_id,
                    table_id,
                )
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_REWRITE_DELETE_THRESHOLD),
            auto_compact: self
                .get_key(spec::metadata::AUTO_COMPACT, schema_id, table_id)
                .map(|s| s == "true")
                .unwrap_or(DEFAULT_AUTO_COMPACT),
        }
    }
}

/* ------------------------------------------- UTILS ------------------------------------------- */

impl Metadata {
    fn get_key(
        &'_ self,
        key: &str,
        schema_id: Option<i64>,
        table_id: Option<i64>,
    ) -> Option<&'_ str> {
        if let Some(table_id) = table_id
            && let Some(table_meta) = self.table.get(&table_id)
            && let Some(value) = table_meta.get(key)
        {
            return Some(value.as_str());
        }
        if let Some(schema_id) = schema_id
            && let Some(schema_meta) = self.schema.get(&schema_id)
            && let Some(value) = schema_meta.get(key)
        {
            return Some(value.as_str());
        }
        self.global.get(key).map(|s| s.as_str())
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                            MUTATION                                           */
/* --------------------------------------------------------------------------------------------- */

const READ_ONLY_KEYS: &[&str] = &[
    spec::metadata::VERSION,
    spec::metadata::CREATED_BY,
    spec::metadata::TABLE,
    spec::metadata::DATA_PATH,
    spec::metadata::ENCRYPTED,
];

impl MetadataCache {
    pub(crate) async fn set_global(&self, key: String, value: String) -> DucklakeResult<()> {
        self.update(&self.pool, &key, Some(&value), None, None)
            .await?;
        Arc::make_mut(&mut self.metadata.write().unwrap())
            .global
            .insert(key, value);
        Ok(())
    }

    pub(crate) async fn set_schema(
        &self,
        schema_id: i64,
        key: String,
        value: String,
    ) -> DucklakeResult<()> {
        self.update(
            &self.pool,
            &key,
            Some(&value),
            Some("schema"),
            Some(schema_id),
        )
        .await?;
        Arc::make_mut(&mut self.metadata.write().unwrap())
            .schema
            .entry(schema_id)
            .or_default()
            .insert(key, value);
        Ok(())
    }

    pub(crate) async fn set_table(
        &self,
        table_id: i64,
        key: String,
        value: String,
    ) -> DucklakeResult<()> {
        self.update(
            &self.pool,
            &key,
            Some(&value),
            Some("table"),
            Some(table_id),
        )
        .await?;
        Arc::make_mut(&mut self.metadata.write().unwrap())
            .table
            .entry(table_id)
            .or_default()
            .insert(key, value);
        Ok(())
    }

    pub(crate) async fn unset_global(&self, key: &str) -> DucklakeResult<()> {
        self.update(&self.pool, key, None, None, None).await?;
        Arc::make_mut(&mut self.metadata.write().unwrap())
            .global
            .remove(key);
        Ok(())
    }

    pub(crate) async fn unset_schema(&self, schema_id: i64, key: &str) -> DucklakeResult<()> {
        self.update(&self.pool, key, None, Some("schema"), Some(schema_id))
            .await?;
        if let Some(schema_meta) = Arc::make_mut(&mut self.metadata.write().unwrap())
            .schema
            .get_mut(&schema_id)
        {
            schema_meta.remove(key);
        }
        Ok(())
    }

    pub(crate) async fn unset_table(&self, table_id: i64, key: &str) -> DucklakeResult<()> {
        self.update(&self.pool, key, None, Some("table"), Some(table_id))
            .await?;
        if let Some(table_meta) = Arc::make_mut(&mut self.metadata.write().unwrap())
            .table
            .get_mut(&table_id)
        {
            table_meta.remove(key);
        }
        Ok(())
    }
}

/* ------------------------------------------ DATABASE ----------------------------------------- */

impl MetadataCache {
    async fn update(
        &self,
        pool: &db::Pool,
        key: &str,
        value: Option<&str>,
        scope: Option<&str>,
        scope_id: Option<i64>,
    ) -> crate::DucklakeResult<()> {
        if READ_ONLY_KEYS.contains(&key) {
            return Err(DucklakeError::ReadOnlyMetadata(key.to_string()));
        }
        let mut tx = pool.begin().await?;

        // Delete existing entry
        let mut delete = Query::delete();
        delete
            .from_table(ducklake_metadata::Table)
            .and_where(ducklake_metadata::Column::Key.col().eq(key));
        if let Some(s) = scope {
            delete.and_where(ducklake_metadata::Column::Scope.col().eq(s));
        } else {
            delete.and_where(ducklake_metadata::Column::Scope.col().is_null());
        }
        if let Some(id) = scope_id {
            delete.and_where(ducklake_metadata::Column::ScopeId.col().eq(id));
        } else {
            delete.and_where(ducklake_metadata::Column::ScopeId.col().is_null());
        }
        tx.execute(&delete).await?;

        // Insert new entry if the update actually inserts a value
        if let Some(value) = value {
            tx.insert_entity(DucklakeMetadata {
                key: key.to_string(),
                value: value.to_string(),
                scope: scope.map(|s| s.to_string()),
                scope_id,
            })
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }
}
