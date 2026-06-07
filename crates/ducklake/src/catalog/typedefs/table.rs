use super::*;
use crate::io;

/// A table in a catalog.
#[derive(Debug, Clone)]
pub(in crate::catalog) struct CatalogTable {
    pub id: Option<i64>,
    pub name: crate::TableName,
    pub columns: CatalogColumns,
    pub partition: Option<CatalogTablePartition>,
    pub tags: Vec<crate::Tag>,
    pub path: io::DucklakePath,
}
