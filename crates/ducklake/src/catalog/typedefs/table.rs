use super::*;
use crate::io;

/// A table in a catalog.
#[derive(Debug, Clone)]
pub(in crate::catalog) struct CatalogTable {
    pub id: Option<i64>,
    pub name: crate::QualifiedName,
    pub columns: CatalogColumns,
    pub partition: Option<CatalogTablePartition>,
    pub sort_info: Option<CatalogTableSortInfo>,
    pub tags: crate::Tags,
    pub path: io::DucklakePath,
}
