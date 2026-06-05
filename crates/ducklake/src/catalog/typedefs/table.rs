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

impl Deref for CatalogTable {
    type Target = CatalogState;

    fn deref(&self) -> &CatalogState {
        &self.state
    }
}
