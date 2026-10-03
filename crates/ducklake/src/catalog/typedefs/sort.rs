use itertools::Itertools;

use super::{ArenaIdx, CatalogColumns};
use crate::spec::{DucklakeSortExpression, DucklakeSortInfo};
use crate::{DucklakeError, DucklakeResult, NullOrder, SortDirection, SortExpression};

#[derive(Debug, Clone)]
pub(in crate::catalog) struct CatalogTableSortInfo {
    columns: Vec<CatalogSortColumn>,
}

#[derive(Debug, Clone)]
struct CatalogSortColumn {
    column: ArenaIdx,
    dialect: String,
    direction: SortDirection,
    null_order: NullOrder,
}

/* ----------------------------------------- TRANSFORM ----------------------------------------- */

impl CatalogTableSortInfo {
    pub(in crate::catalog) fn from_ducklake(
        info: DucklakeSortInfo,
        expressions: Vec<DucklakeSortExpression>,
        columns: &CatalogColumns,
    ) -> DucklakeResult<Self> {
        let columns = expressions
            .into_iter()
            .sorted_by_key(|expr| expr.sort_key_index)
            .map(|expr| {
                let expression = expr.expression.ok_or_else(|| {
                    DucklakeError::InvalidChanges(format!(
                        "sort {} has an empty expression",
                        info.sort_id
                    ))
                })?;
                Ok(CatalogSortColumn {
                    column: column_by_name(columns, &expression)?,
                    dialect: expr.dialect.unwrap_or_else(|| "duckdb".to_string()),
                    direction: expr.sort_direction.as_deref().unwrap_or("ASC").parse()?,
                    null_order: expr.null_order.as_deref().unwrap_or("NULLS_LAST").parse()?,
                })
            })
            .collect::<DucklakeResult<Vec<_>>>()?;
        Ok(Self { columns })
    }

    pub(in crate::catalog) fn from_sort_info(
        sort_info: crate::SortInfo,
        columns: &CatalogColumns,
    ) -> DucklakeResult<Self> {
        let columns = sort_info
            .0
            .into_iter()
            .map(|expression| {
                if expression.expression.is_empty() || expression.dialect != "duckdb" {
                    return Err(DucklakeError::InvalidChanges(
                        "sort columns require a nonempty name and the DuckDB dialect".to_string(),
                    ));
                }
                Ok(CatalogSortColumn {
                    column: column_by_name(columns, &expression.expression)?,
                    dialect: expression.dialect,
                    direction: expression.direction,
                    null_order: expression.null_order,
                })
            })
            .collect::<DucklakeResult<Vec<_>>>()?;
        Ok(Self { columns })
    }

    #[allow(clippy::wrong_self_convention)]
    pub(in crate::catalog) fn into_sort_info(&self, columns: &CatalogColumns) -> crate::SortInfo {
        let expressions = self
            .columns
            .iter()
            .map(|sort_column| SortExpression {
                expression: columns.arena[sort_column.column.0].name.clone(),
                dialect: sort_column.dialect.clone(),
                direction: sort_column.direction,
                null_order: sort_column.null_order,
            })
            .collect();
        crate::SortInfo(expressions)
    }
}

/* ------------------------------------------ CHANGES ------------------------------------------ */

impl CatalogTableSortInfo {
    pub(in crate::catalog) fn references_column(&self, column: ArenaIdx) -> bool {
        self.columns.iter().any(|sort| sort.column == column)
    }
}

fn column_by_name(columns: &CatalogColumns, name: &str) -> DucklakeResult<ArenaIdx> {
    columns
        .root_columns
        .get(name)
        .copied()
        .ok_or_else(|| DucklakeError::NotFound {
            entity: "column referenced by sort expression",
            name: name.to_owned(),
        })
}
