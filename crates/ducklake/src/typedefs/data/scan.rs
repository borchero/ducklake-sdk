use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};

use super::DataFileStatistics;

/// Result of scanning a table at a specific snapshot.
pub struct ScanResult {
    /// The data files that need to be read to produce the table contents.
    pub data_files: Vec<ScanDataFile>,
    /// Record batches whose contents are inlined in the catalog.
    pub inline_data: Vec<RecordBatch>,
}

/// A data file that needs to be read as part of a [`ScanResult`].
pub struct ScanDataFile {
    /// The path of the data file.
    pub path: String,
    /// Statistics describing the contents of the data file.
    pub statistics: DataFileStatistics,
    /// Delete files that need to be applied to the data file.
    pub delete_files: Vec<ScanDeleteFile>,
    /// Row indices that have been deleted via inline deletes.
    pub inline_deletes: Option<Arc<Int64Array>>,
}

/// A delete file that needs to be applied to a data file.
pub struct ScanDeleteFile {
    /// The path of the delete file.
    pub path: String,
    /// The number of rows deleted by the delete file.
    pub num_deletes: usize,
    /// The size of the delete file in bytes.
    pub file_size_bytes: Option<usize>,
    /// The size of the delete file's footer in bytes.
    pub footer_size_bytes: Option<usize>,
}
