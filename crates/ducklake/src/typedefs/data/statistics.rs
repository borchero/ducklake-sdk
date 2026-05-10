use std::collections::HashMap;

use crate::Value;

/* -------------------------------------------- FILE ------------------------------------------- */

/// Statistics describing the contents of a data file.
#[derive(Debug, Clone)]
pub struct DataFileStatistics {
    /// The number of rows in the data file.
    pub num_rows: usize,
    /// The size of the data file in bytes.
    pub file_size_bytes: Option<usize>,
    /// The size of the data file's footer in bytes.
    pub footer_size_bytes: Option<usize>,
    /// Per-column statistics, keyed by field ID.
    pub column_stats: HashMap<i64, FileColumnStats>,
}

/// Statistics describing a single column within a data file.
#[derive(Debug, Clone)]
pub struct FileColumnStats {
    /// The size of the column's data in bytes.
    pub size_bytes: Option<usize>,
    /// The minimum value of the column.
    pub min_value: Option<Value>,
    /// The maximum value of the column.
    pub max_value: Option<Value>,
    /// The number of null values in the column.
    pub null_count: Option<usize>,
    /// Whether the column contains any NaN values.
    pub contains_nan: Option<bool>,
}

/* ---------------------------------------- RECORD BATCH --------------------------------------- */

#[derive(Debug, Clone)]
pub(crate) struct RecordBatchStatistics {
    pub column_stats: HashMap<i64, ArrayColumnStats>,
}

#[derive(Debug, Clone)]
pub(crate) struct ArrayColumnStats {
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
    pub null_count: Option<usize>,
    pub contains_nan: Option<bool>,
}
