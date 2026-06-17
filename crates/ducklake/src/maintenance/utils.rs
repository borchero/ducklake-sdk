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
        let entities = ids
            .iter()
            .map(|id| LookupTableEntry {
                table_name: name,
                id: *id,
            })
            .collect::<Vec<_>>();
        tx.insert_entities(entities).await?;

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

struct LookupTableEntry<'a> {
    table_name: &'a str,
    id: i64,
}

impl db::sea_query_ext::InsertableEntity for LookupTableEntry<'_> {
    const NUM_COLUMNS: usize = 1;

    #[cfg_attr(coverage_nightly, coverage(off))]
    fn insert_into_table(&self) -> sea_query::InsertStatement {
        unimplemented!()
    }

    fn insert_all_into_table(
        entities: impl IntoIterator<Item = Self>,
    ) -> sea_query::InsertStatement {
        let mut entities = entities.into_iter().peekable();
        let mut query = Query::insert();
        query
            // SAFETY: entities is guaranteed to be non-empty by `insert_entities`
            .into_table(entities.peek().unwrap().table_name.to_string())
            .columns(["id"]);
        for entity in entities {
            query.values_panic([entity.id.into()]);
        }
        query.take()
    }
}
