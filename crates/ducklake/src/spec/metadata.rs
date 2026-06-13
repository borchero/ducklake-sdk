// DuckLake format version.
pub static VERSION: &str = "version";

// Tool used to write the DuckLake.
pub static CREATED_BY: &str = "created_by";

// Program that wrote the schema (e.g., DuckDB v1.3.2).
pub static TABLE: &str = "table";

// Path to data files (must end with '/').
pub static DATA_PATH: &str = "data_path";

// Whether to encrypt Parquet files ('true' or 'false').
pub static ENCRYPTED: &str = "encrypted";

// Maximum rows to inline in a single insert.
pub static DATA_INLINING_ROW_LIMIT: &str = "data_inlining_row_limit";

// Target data file size for insertion/compaction.
pub static TARGET_FILE_SIZE: &str = "target_file_size";

// Bytes per row group in Parquet files.
pub static PARQUET_ROW_GROUP_SIZE_BYTES: &str = "parquet_row_group_size_bytes";

// Rows per row group in Parquet files.
pub static PARQUET_ROW_GROUP_SIZE: &str = "parquet_row_group_size";

// Compression algorithm for Parquet files.
pub static PARQUET_COMPRESSION: &str = "parquet_compression";

// Compression level for Parquet files.
pub static PARQUET_COMPRESSION_LEVEL: &str = "parquet_compression_level";

// Parquet format version (1 or 2).
pub static PARQUET_VERSION: &str = "parquet_version";

// Write partitioned data in hive-like folder structure ('true' or 'false').
pub static HIVE_FILE_PATTERN: &str = "hive_file_pattern";

// Require explicit commit message for snapshot commit ('true' or 'false').
#[allow(dead_code)]
pub static REQUIRE_COMMIT_MESSAGE: &str = "require_commit_message";

// Minimum data (0-1) to remove before rewrite is warranted.
pub static REWRITE_DELETE_THRESHOLD: &str = "rewrite_delete_threshold";

// Age threshold for deleting unused files (duration string, e.g., '7d').
#[allow(dead_code)]
pub static DELETE_OLDER_THAN: &str = "delete_older_than";

// Age threshold for expiring snapshots (duration string, e.g., '30d').
pub static EXPIRE_OLDER_THAN: &str = "expire_older_than";

// Whether a table is included when compaction functions are called without a specific table
// argument ('true' or 'false').
pub static AUTO_COMPACT: &str = "auto_compact";

// Create separate output files per thread during parallel insertion ('true' or 'false').
#[allow(dead_code)]
pub static PER_THREAD_OUTPUT: &str = "per_thread_output";
