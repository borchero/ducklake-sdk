use sea_query::{ColumnDef, Condition, Expr, ExprTrait, Query, Table};

use crate::{DucklakeResult, db};

/// Handle to a table that can be used for `IS IN` queries targeting IDs.
///
/// This is useful as `IS IN` with a list of values might run into limitations wrt. the parameter
/// count when the number of values exceeds the dialect's maximum number of parameters.
pub(super) struct LookupTableHandle {
    name: String,
}

impl LookupTableHandle {
    pub(super) async fn new(
        tx: &mut db::Transaction,
        name: &str,
        ids: &[i64],
    ) -> DucklakeResult<Self> {
        // Create the table
        let query = Table::create()
            .table(name.to_string())
            .col(ColumnDef::new_with_type(
                "id",
                tx.dialect().column_type_i64(),
            ))
            .take();
        tx.execute(&query).await?;

        // Insert the IDs
        let rows = ids.iter().map(|id| vec![(*id).into()]).collect();
        tx.insert_rows(name, &["id"], rows).await?;

        // Return the handle
        Ok(Self {
            name: name.to_string(),
        })
    }

    pub(super) fn condition_is_in(&self, expr: Expr) -> Condition {
        expr.in_subquery(Query::select().column("id").from(self.name.clone()).take())
            .into()
    }

    pub(super) async fn drop(self, tx: &mut db::Transaction) -> DucklakeResult<()> {
        let query = Table::drop().table(self.name).take();
        tx.execute(&query).await?;
        Ok(())
    }
}
