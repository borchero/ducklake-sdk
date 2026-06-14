use ducklake_macros::ducklake_table;

use crate::db::{UtcDateTime, UuidText, sea_query_ext};

/// This table describes the columns that are part of a table, including their types, default
/// values etc.
#[ducklake_table]
pub(crate) struct DucklakeColumn {
    pub column_id: i64,
    pub begin_snapshot: i64,
    pub end_snapshot: Option<i64>,
    pub table_id: i64,
    pub column_order: Option<i64>,
    pub column_name: String,
    pub column_type: String,
    pub initial_default: Option<String>,
    pub default_value: Option<String>,
    pub nulls_allowed: bool,
    pub parent_column: Option<i64>,
    pub default_value_type: Option<String>,
    pub default_value_dialect: Option<String>,
}

/// Mappings contain the information used to map parquet fields to column ids in the absence of
/// `field-id`s in the Parquet file.
#[ducklake_table]
pub(crate) struct DucklakeColumnMapping {
    pub mapping_id: i64,
    pub table_id: i64,
    pub r#type: Option<String>,
}

/// Columns can also have tags, those are defined in this table.
#[ducklake_table]
pub struct DucklakeColumnTag {
    pub table_id: i64,
    pub column_id: i64,
    pub begin_snapshot: i64,
    pub end_snapshot: Option<i64>,
    pub key: String,
    pub value: String,
}

/// Data files contain the actual row data.
#[ducklake_table]
pub(crate) struct DucklakeDataFile {
    #[primary_key]
    pub data_file_id: i64,
    pub table_id: i64,
    pub begin_snapshot: i64,
    pub end_snapshot: Option<i64>,
    pub file_order: Option<i64>,
    pub path: String,
    pub path_is_relative: bool,
    pub file_format: String,
    pub record_count: i64,
    pub file_size_bytes: Option<i64>,
    pub footer_size: Option<i64>,
    pub row_id_start: Option<i64>,
    pub partition_id: Option<i64>,
    pub encryption_key: Option<String>,
    pub mapping_id: Option<i64>,
    pub partial_max: Option<i64>,
}

/// Delete files contains the row ids of rows that are deleted. Each data file will have its own
/// delete file if any deletes are present for this data file.
#[ducklake_table]
pub(crate) struct DucklakeDeleteFile {
    #[primary_key]
    pub delete_file_id: i64,
    pub table_id: i64,
    pub begin_snapshot: i64,
    pub end_snapshot: Option<i64>,
    pub data_file_id: i64,
    pub path: String,
    pub path_is_relative: bool,
    pub format: String,
    pub delete_count: Option<i64>,
    pub file_size_bytes: Option<i64>,
    pub footer_size: Option<i64>,
    pub encryption_key: Option<String>,
    pub partial_max: Option<i64>,
}

/// This table contains column-level statistics for a single data file.
#[ducklake_table]
pub(crate) struct DucklakeFileColumnStats {
    pub data_file_id: i64,
    pub table_id: i64,
    pub column_id: i64,
    pub column_size_bytes: Option<i64>,
    pub value_count: Option<i64>,
    pub null_count: Option<i64>,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub contains_nan: Option<bool>,
    pub extra_stats: Option<String>,
}

/// Files that are no longer part of any snapshot are scheduled for deletion.
#[ducklake_table]
pub(crate) struct DucklakeFilesScheduledForDeletion {
    pub data_file_id: i64,
    pub path: String,
    pub path_is_relative: bool,
    pub schedule_start: Option<UtcDateTime>,
}

/// This table defines which data file belongs to which partition.
#[ducklake_table]
pub(crate) struct DucklakeFilePartitionValue {
    pub data_file_id: i64,
    pub table_id: i64,
    pub partition_key_index: i64,
    pub partition_value: Option<String>,
}

/// This table links DuckLake snapshots with inlined data tables.
#[ducklake_table]
pub(crate) struct DucklakeInlinedDataTables {
    pub table_id: i64,
    pub table_name: String,
    pub schema_version: i64,
}

/// The ducklake_metadata table contains key/value pairs with information about the specific setup
/// of the DuckLake catalog.
#[ducklake_table]
pub(crate) struct DucklakeMetadata {
    #[not_null]
    pub key: String,
    #[not_null]
    pub value: String,
    pub scope: Option<String>,
    pub scope_id: Option<i64>,
}

/// This table contains the information used to map a name to a column_id for a given mapping_id
/// with the `map_by_name` type.
#[ducklake_table]
pub(crate) struct DucklakeNameMapping {
    pub mapping_id: i64,
    pub column_id: i64,
    pub source_name: Option<String>,
    pub target_field_id: Option<i64>,
    pub parent_column: Option<i64>,
    pub is_partition: Option<bool>,
}

/// Partitions can refer to one or more columns, possibly with transformations such as hashing or
/// bucketing.
#[ducklake_table]
pub(crate) struct DucklakePartitionColumn {
    pub partition_id: i64,
    pub table_id: i64,
    pub partition_key_index: i64,
    pub column_id: i64,
    pub transform: String,
}

/// This table defines valid partitions.
#[ducklake_table]
pub(crate) struct DucklakePartitionInfo {
    pub partition_id: i64,
    pub table_id: i64,
    pub begin_snapshot: i64,
    pub end_snapshot: Option<i64>,
}

/// This table defines valid schemas.
#[ducklake_table]
pub(crate) struct DucklakeSchema {
    #[primary_key]
    pub schema_id: i64,
    pub schema_uuid: Option<UuidText>,
    pub begin_snapshot: i64,
    pub end_snapshot: Option<i64>,
    pub schema_name: String,
    pub path: String,
    pub path_is_relative: bool,
}

/// This table contains the schema versions for a range of snapshots. It is necessary to compact
/// files with different schemas.
#[ducklake_table]
pub(crate) struct DucklakeSchemaVersions {
    pub begin_snapshot: i64,
    pub schema_version: i64,
    pub table_id: Option<i64>,
}

/// This table contains the valid snapshots in a DuckLake.
#[ducklake_table]
pub(crate) struct DucklakeSnapshot {
    #[primary_key]
    pub snapshot_id: i64,
    pub snapshot_time: UtcDateTime,
    pub schema_version: i64,
    pub next_catalog_id: i64,
    pub next_file_id: i64,
}

/// This table lists changes that happened in a snapshot for easier conflict detection.
#[ducklake_table]
pub(crate) struct DucklakeSnapshotChanges {
    #[primary_key]
    pub snapshot_id: i64,
    pub changes_made: String,
    pub author: Option<String>,
    pub commit_message: Option<String>,
    pub commit_extra_info: Option<String>,
}

/// This table describes tables. Inception!
#[ducklake_table]
pub(crate) struct DucklakeTable {
    pub table_id: i64,
    pub table_uuid: Option<UuidText>,
    pub begin_snapshot: i64,
    pub end_snapshot: Option<i64>,
    pub schema_id: i64,
    pub table_name: String,
    pub path: String,
    pub path_is_relative: bool,
}

/// This table contains column-level statistics for an entire table.
#[ducklake_table]
pub(crate) struct DucklakeTableColumnStats {
    pub table_id: i64,
    pub column_id: i64,
    pub contains_null: Option<bool>,
    pub contains_nan: Option<bool>,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub extra_stats: Option<String>,
}

/// This table contains table-level statistics.
#[ducklake_table]
pub(crate) struct DucklakeTableStats {
    pub table_id: i64,
    pub record_count: Option<i64>,
    pub next_row_id: i64,
    pub file_size_bytes: Option<i64>,
}

/// Schemas, tables, and views etc can have tags, those are declared in this table.
#[ducklake_table]
pub struct DucklakeTag {
    pub object_id: i64,
    pub begin_snapshot: i64,
    pub end_snapshot: Option<i64>,
    pub key: String,
    pub value: String,
}

/// This table stores macro definitions. Each macro is associated with a schema and tracks its
/// lifecycle through snapshots.
#[ducklake_table]
pub(crate) struct DucklakeMacro {
    pub schema_id: i64,
    pub macro_id: i64,
    pub macro_name: String,
    pub begin_snapshot: i64,
    pub end_snapshot: Option<i64>,
}

/// This table stores macro implementations. A single macro can have multiple implementations.
#[ducklake_table]
pub(crate) struct DucklakeMacroImpl {
    pub macro_id: i64,
    pub impl_id: i64,
    pub dialect: Option<String>,
    pub sql: Option<String>,
    pub r#type: Option<String>,
}

/// This table stores the parameters for each macro implementation.
#[ducklake_table]
pub(crate) struct DucklakeMacroParameters {
    pub macro_id: i64,
    pub impl_id: i64,
    pub column_id: i64,
    pub parameter_name: Option<String>,
    pub parameter_type: Option<String>,
    pub default_value: Option<String>,
    pub default_value_type: Option<String>,
}

/// This table records the version history of sort settings for tables. Each row represents one
/// sort configuration applied to a table, with snapshot-based validity tracking.
#[ducklake_table]
pub(crate) struct DucklakeSortInfo {
    pub sort_id: i64,
    pub table_id: i64,
    pub begin_snapshot: i64,
    pub end_snapshot: Option<i64>,
}

/// The ducklake_sort_expression table stores the individual sort key expressions for each sort
/// configuration. Each row corresponds to one expression in a SET SORTED BY clause.
#[ducklake_table]
pub(crate) struct DucklakeSortExpression {
    pub sort_id: i64,
    pub table_id: i64,
    pub sort_key_index: i64,
    pub expression: Option<String>,
    pub dialect: Option<String>,
    pub sort_direction: Option<String>,
    pub null_order: Option<String>,
}

/// This table contains per-file statistics for the shredded sub-fields of variant columns.
#[ducklake_table]
pub(crate) struct DucklakeFileVariantStats {
    pub data_file_id: i64,
    pub table_id: i64,
    pub column_id: i64,
    pub variant_path: Option<String>,
    pub shredded_type: Option<String>,
    pub column_size_bytes: Option<i64>,
    pub value_count: Option<i64>,
    pub null_count: Option<i64>,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub contains_nan: Option<bool>,
    pub extra_stats: Option<String>,
}

/// This table describes SQL-style VIEW definitions.
#[ducklake_table]
pub(crate) struct DucklakeView {
    pub view_id: i64,
    pub view_uuid: Option<UuidText>,
    pub begin_snapshot: i64,
    pub end_snapshot: Option<i64>,
    pub schema_id: i64,
    pub view_name: String,
    pub dialect: Option<String>,
    pub sql: Option<String>,
    pub column_aliases: Option<String>,
}

/* ------------------------------------------- UTILS ------------------------------------------- */

/// Meta-table that can be created on demand for every table in the DuckLake to store inline
/// deletes. While the name of this table does not exist, it can be used as a type to read via
/// sqlx.
#[ducklake_table]
pub(crate) struct DucklakeInlinedDelete {
    pub file_id: i64,
    pub row_id: i64,
    pub begin_snapshot: i64,
}

impl DucklakeInlinedDelete {
    pub(crate) fn table_name(table_id: i64) -> String {
        format!("ducklake_inlined_delete_{}", table_id)
    }
}

/// Partial table for inlined data tables. This includes only the columns which are added beyond
/// the columns that contain the data.
#[ducklake_table]
pub(crate) struct DucklakeInlinedData {
    pub row_id: i64,
    pub begin_snapshot: i64,
    pub end_snapshot: Option<i64>,
}

impl DucklakeInlinedData {
    pub(crate) fn table_name(table_id: i64, schema_version: i64) -> String {
        format!("ducklake_inlined_data_{}_{}", table_id, schema_version)
    }
}
