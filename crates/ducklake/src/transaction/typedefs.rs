use std::collections::HashMap;

use crate::catalog::ColumnRef;
use crate::{ArrayColumnStats, FileColumnStats, Value, io};

#[derive(Debug, Clone)]
pub(super) struct CommitDataFile {
    pub path: io::DucklakePath,
    pub partition_values: Option<Vec<Option<Value>>>,
    pub num_rows: usize,
    pub file_size_bytes: Option<usize>,
    pub footer_size_bytes: Option<usize>,
    pub column_stats: HashMap<ColumnRef, FileColumnStats>,
}

#[derive(Debug, Clone)]
pub(super) struct CommitInlineData {
    pub record_batch: arrow_array::RecordBatch,
    pub column_stats: HashMap<ColumnRef, ArrayColumnStats>,
}
