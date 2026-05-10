/// Result type returned by all fallible DuckLake operations.
pub type DucklakeResult<T> = Result<T, DucklakeError>;
use std::convert::Infallible;
use std::num::{ParseFloatError, ParseIntError};
use std::str::ParseBoolError;

use crate::utils::format_identifier;

/// Error type returned by all fallible DuckLake operations.
#[derive(thiserror::Error, Debug)]
pub enum DucklakeError {
    #[error("the DuckLake SDK does not currently support version {0}")]
    UnsupportedVersion(String),
    #[error(
        "the catalog version is outdated (current: {0}, expected: {1}) and automatic migrations disabled"
    )]
    OutdatedVersion(String, String),
    #[error("{entity} with name '{name}' already exists")]
    AlreadyExists { entity: &'static str, name: String },
    #[error("{entity} with name '{name}' does not exist")]
    NotFound { entity: &'static str, name: String },
    #[error("entity with id '{id}' was not found")]
    EntityNotFound { id: i64 },
    #[error("column with id '{id}' was not found")]
    ColumnNotFound { id: i64 },
    #[error("invalid changes: {0}")]
    InvalidChanges(String),
    #[error("invalid data type: {0}")]
    InvalidDataType(String),
    #[error("invalid partitions: {0}")]
    InvalidPartitions(String),
    #[error("invalid partition transform: {0}")]
    InvalidPartitionTransform(String),
    #[error("invalid schema name '{name}': {reason}")]
    InvalidSchemaName { name: String, reason: &'static str },
    #[error("invalid table name '{name}': {reason}")]
    InvalidTableName { name: String, reason: &'static str },
    #[error("invalid column name '{name}': {reason}")]
    InvalidColumnName { name: String, reason: &'static str },
    #[error("cannot cast column from type '{old}' to type '{new}'")]
    InvalidCast {
        old: crate::DataType,
        new: crate::DataType,
    },
    #[error("duplicate column name '{0}'")]
    DuplicateColumnName(String),
    #[error("catalog is not initialized yet, call `Ducklake::create` first")]
    CatalogNotInitialized,
    #[error("catalog is already initialized, use `Ducklake::connect` instead")]
    CatalogAlreadyInitialized,
    #[error("catalog is already initialized but does not declare a version")]
    UnknownVersion,
    #[error("transaction conflict: {0}")]
    TransactionConflict(String),
    #[error("connection url '{0}' specifies unsupported database scheme")]
    UnsupportedDatabase(String),
    #[error(
        "failed to commit transaction because of high write concurrency: the number of retries was exceeded"
    )]
    RetriesExceeded,
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("parsing error: {0}")]
    Parsing(String),
    #[error("invalid version: {0}")]
    InvalidVersion(String),
    #[error("invalid path '{path}' (reason: {reason})")]
    InvalidPath { path: String, reason: &'static str },
    #[error("metadata key '{0}' cannot be set")]
    ReadOnlyMetadata(String),
    #[error("invalid default value for column '{column}': {reason}")]
    InvalidDefault {
        column: String,
        reason: &'static str,
    },
    #[error("cannot mark column '{column}' as non-nullable because it already contains nulls")]
    InvalidNullabilityChange { column: String },
    #[error("cannot insert null value into non-nullable column '{column}'")]
    InvalidNullValue { column: String },
    #[error("URL scheme '{0}' is not currently supported for file paths")]
    UnsupportedUrlScheme(String),
    #[error("unsupported Arrow data type: {0}")]
    UnsupportedArrowDataType(String),
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("when time-traveling in a DuckLake connection, no changes may be performed")]
    ImmutableDucklake,
}

impl From<Infallible> for DucklakeError {
    fn from(_: Infallible) -> Self {
        unreachable!()
    }
}

impl From<ParseBoolError> for DucklakeError {
    fn from(value: ParseBoolError) -> Self {
        Self::Parsing(value.to_string())
    }
}

impl From<ParseIntError> for DucklakeError {
    fn from(value: ParseIntError) -> Self {
        Self::Parsing(value.to_string())
    }
}

impl From<ParseFloatError> for DucklakeError {
    fn from(value: ParseFloatError) -> Self {
        Self::Parsing(value.to_string())
    }
}

impl From<rust_decimal::Error> for DucklakeError {
    fn from(value: rust_decimal::Error) -> Self {
        Self::Parsing(value.to_string())
    }
}

impl From<uuid::Error> for DucklakeError {
    fn from(value: uuid::Error) -> Self {
        Self::Parsing(value.to_string())
    }
}

impl From<chrono::ParseError> for DucklakeError {
    fn from(value: chrono::ParseError) -> Self {
        Self::Parsing(value.to_string())
    }
}

impl From<url::ParseError> for DucklakeError {
    fn from(value: url::ParseError) -> Self {
        Self::Parsing(value.to_string())
    }
}

impl From<std::string::FromUtf8Error> for DucklakeError {
    fn from(value: std::string::FromUtf8Error) -> Self {
        Self::Parsing(value.to_string())
    }
}

impl From<hex::FromHexError> for DucklakeError {
    fn from(value: hex::FromHexError) -> Self {
        Self::Parsing(value.to_string())
    }
}

impl DucklakeError {
    pub(crate) fn schema_already_exists(name: &str) -> Self {
        DucklakeError::AlreadyExists {
            entity: "schema",
            name: name.to_string(),
        }
    }

    pub(crate) fn schema_not_found(name: &str) -> Self {
        DucklakeError::NotFound {
            entity: "schema",
            name: name.to_string(),
        }
    }

    pub(crate) fn table_already_exists(name: &crate::TableName) -> Self {
        DucklakeError::AlreadyExists {
            entity: "table",
            name: name.to_string(),
        }
    }

    pub(crate) fn table_not_found(name: &crate::TableName) -> Self {
        DucklakeError::NotFound {
            entity: "table",
            name: name.to_string(),
        }
    }

    pub(crate) fn column_already_exists(name: &str) -> Self {
        DucklakeError::AlreadyExists {
            entity: "column",
            name: name.to_string(),
        }
    }

    pub(crate) fn column_not_found(name: &str) -> Self {
        DucklakeError::NotFound {
            entity: "column",
            name: name.to_string(),
        }
    }

    pub(crate) fn column_path_not_found(path: &[String]) -> Self {
        DucklakeError::NotFound {
            entity: "column",
            name: format_identifier(path),
        }
    }
}
