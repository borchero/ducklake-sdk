use std::ops::Deref;

use super::*;
use crate::io;

/// A table in a catalog.
#[derive(Debug, Clone)]
pub(in crate::catalog) struct CatalogTable {
    pub state: CatalogState,
    pub name: crate::TableName,
    pub columns: CatalogColumns,
    pub partition: Option<CatalogTablePartition>,
    pub tags: Vec<crate::Tag>,
    pub path: io::DucklakePath,
}

impl CatalogTable {
    /// Get the names of the partition columns if this table is partitioned.
    pub fn partition_column_names(&self) -> Option<Vec<String>> {
        self.partition.as_ref().map(|partition| {
            partition
                .columns
                .iter()
                .map(|col| self.columns.column_by_arena_idx(col.column).name.clone())
                .collect()
        })
    }
}

impl Deref for CatalogTable {
    type Target = CatalogState;

    fn deref(&self) -> &CatalogState {
        &self.state
    }
}
