use indexmap::IndexMap;

use super::DataFileStatistics;
use crate::Value;

/// Description of a data file that should be committed to a table.
#[derive(Debug, Clone)]
pub struct WriteDataFile {
    /// The path of the data file.
    pub path: String,
    /// Statistics describing the contents of the data file. If omitted, statistics are computed
    /// from the data file when committing.
    pub statistics: Option<DataFileStatistics>,
    /// The partition values associated with the data file. Ignored if the table is not
    /// partitioned.
    pub partition_values: Option<IndexMap<String, Option<Value>>>,
}
