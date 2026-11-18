use std::sync::atomic::{AtomicU64, Ordering};

use sea_query::{ColumnDef, Condition, Expr, ExprTrait, Query, Table};

use crate::{DucklakeResult, db};

static TEMP_LOOKUP_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Handle to a session-scoped temporary table that can be used for `IS IN` queries targeting IDs.
///
/// This is useful as `IS IN` with a list of values might run into limitations wrt. the parameter
/// count when the number of values exceeds the dialect's maximum number of parameters.
pub(super) struct LookupTableHandle {
    name: String,
}

impl LookupTableHandle {
    pub(super) async fn new(
        tx: &mut db::Transaction<'_>,
        prefix: &str,
        ids: &[i64],
    ) -> DucklakeResult<Self> {
        let seq = TEMP_LOOKUP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = format!("{prefix}_{seq}");
        let query = Table::create()
            .table(name.clone())
            .temporary()
            .col(ColumnDef::new_with_type(
                "id",
                tx.dialect().column_type_i64(),
            ))
            .take();
        tx.execute(&query).await?;

        // Insert the IDs
        let rows = ids.iter().map(|id| vec![(*id).into()]).collect();
        tx.insert_rows(&name, &["id"], rows).await?;

        // Return the handle
        Ok(Self { name })
    }

    pub(super) fn condition_is_in(&self, expr: Expr) -> Condition {
        expr.in_subquery(Query::select().column("id").from(self.name.clone()).take())
            .into()
    }

    pub(super) async fn drop(self, tx: &mut db::Transaction<'_>) -> DucklakeResult<()> {
        tx.execute(&db::DropTemporaryTable(self.name)).await
    }
}
