use std::collections::HashMap;

use crate::catalog::ColumnRef;
use crate::{ArrayColumnStats, FileColumnStats, io};

#[derive(Debug, Clone)]
pub(super) struct CommitDataFile {
    pub path: io::DucklakePath,
    pub partition_values: Option<Vec<Option<String>>>,
    pub num_rows: usize,
    pub file_size_bytes: Option<usize>,
    pub footer_size_bytes: Option<usize>,
    pub column_stats: HashMap<ColumnRef, FileColumnStats>,
    pub delete_files: Vec<CommitDeleteFile>,
    pub inline_deletes: Vec<i64>,
}

#[derive(Debug, Clone)]
pub(super) struct CommitDeleteFile {
    pub path: io::DucklakePath,
    pub num_deletes: usize,
    pub file_size_bytes: Option<usize>,
    pub footer_size_bytes: Option<usize>,
}

#[derive(Debug, Clone)]
pub(crate) struct TransferDataFile {
    pub data_file: crate::WriteDataFile,
    pub partition_values: Option<Vec<Option<String>>>,
    pub delete_files: Vec<TransferDeleteFile>,
    pub inline_deletes: Vec<i64>,
}

#[derive(Debug, Clone)]
pub(crate) struct TransferDeleteFile {
    pub path: String,
    pub num_deletes: usize,
    pub file_size_bytes: Option<usize>,
    pub footer_size_bytes: Option<usize>,
}

#[derive(Debug, Clone)]
pub(super) struct CommitInlineData {
    pub record_batch: arrow_array::RecordBatch,
    pub column_stats: HashMap<ColumnRef, ArrayColumnStats>,
}
