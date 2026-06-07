use itertools::Itertools;

use super::*;
use crate::DucklakeResult;
use crate::spec::*;

#[derive(Debug, Clone)]
pub(in crate::catalog) struct CatalogTablePartition {
    pub id: Option<i64>,
    pub columns: Vec<CatalogPartitionColumn>,
}

#[derive(Debug, Clone)]
pub(in crate::catalog) struct CatalogPartitionColumn {
    pub column: ArenaIdx,
    transform: crate::PartitionTransform,
}

/* ----------------------------------------- TRANSFORM ----------------------------------------- */

impl CatalogTablePartition {
    /// Transforms DuckLake partition info and columns into a catalog table partition.
    pub fn from_ducklake(
        partition_info: DucklakePartitionInfo,
        partition_columns: Vec<DucklakePartitionColumn>,
        columns: &CatalogColumns,
    ) -> DucklakeResult<CatalogTablePartition> {
        let columns: Vec<_> = partition_columns
            .into_iter()
            .sorted_by_key(|col| col.partition_key_index)
            .map(|col| {
                col.transform
                    .parse()
                    .map(|transform| CatalogPartitionColumn {
                        column: *columns.by_id.get(&col.column_id).unwrap(),
                        transform,
                    })
            })
            .collect::<DucklakeResult<_>>()?;
        let partition = CatalogTablePartition {
            id: Some(partition_info.partition_id),
            columns,
        };
        Ok(partition)
    }

    pub fn from_partition(
        partition: crate::Partition,
        columns: &CatalogColumns,
    ) -> DucklakeResult<Self> {
        let catalog_columns = partition
            .0
            .into_iter()
            .map(|col| {
                Ok(CatalogPartitionColumn {
                    column: columns.arena_idx_by_path(&[col.column])?,
                    transform: col.transform,
                })
            })
            .collect::<DucklakeResult<_>>()?;
        Ok(CatalogTablePartition {
            id: None,
            columns: catalog_columns,
        })
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn into_partition(&self, columns: &CatalogColumns) -> crate::Partition {
        let columns = self
            .columns
            .iter()
            .map(|col| crate::PartitionColumn {
                column: columns.arena[col.column.0].name.clone(),
                transform: col.transform,
            })
            .collect();
        crate::Partition(columns)
    }
}
