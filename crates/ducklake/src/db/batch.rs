use sea_query::{
    Asterisk,
    CaseStatement,
    Condition,
    Expr,
    ExprTrait,
    IntoIden,
    IntoTableRef,
    Query,
    Value,
};
use sea_query_sqlx::SqlxValues;
use strum::IntoEnumIterator;

use super::{AnyTransaction, Dialect, RowType, SqlConvertible, Transaction, log_sql};
use crate::DucklakeResult;

impl Transaction {
    /// Delete rows matching non-null keys.
    pub(crate) async fn delete_rows<C: IntoIden + Copy, const K: usize>(
        &mut self,
        table: impl IntoTableRef,
        keys: [C; K],
        rows: &[[Value; K]],
    ) -> DucklakeResult<()> {
        let table = table.into_table_ref();
        self.execute_batches(rows, self.dialect().max_rows_per_key_batch(K), |rows| {
            Query::delete()
                .from_table(table.clone())
                .cond_where(matching_rows(&keys, rows.iter()))
                .to_owned()
        })
        .await
    }

    /// Fetch all rows matching non-null keys, with no guaranteed result ordering.
    pub(crate) async fn fetch_rows<O: RowType, C: IntoIden + Copy, const K: usize>(
        &mut self,
        table: impl IntoTableRef,
        keys: [C; K],
        rows: &[[Value; K]],
    ) -> DucklakeResult<Vec<O>> {
        let table = table.into_table_ref();
        let mut result = Vec::new();
        for query in batch_queries(
            self.dialect(),
            rows,
            self.dialect().max_rows_per_key_batch(K),
            |rows| {
                Query::select()
                    .column(Asterisk)
                    .from(table.clone())
                    .cond_where(matching_rows(&keys, rows.iter()))
                    .to_owned()
            },
        ) {
            let (sql, values) = query?;
            log_sql(sql.as_str(), Some(&values));
            result.extend(dispatch_tx!(self, tx => {
                sqlx::query_as_with::<_, O, _>(sql, values).fetch_all(&mut **tx).await?
            }));
        }
        Ok(result)
    }

    /// Set the same values on rows matching non-null keys and an additional filter.
    pub(crate) async fn update_matching_rows<
        C: IntoIden + Copy,
        const K: usize,
        const V: usize,
    >(
        &mut self,
        table: impl IntoTableRef,
        keys: [C; K],
        rows: &[[Value; K]],
        values: [(C, Value); V],
        filter: Condition,
    ) -> DucklakeResult<()> {
        let table = table.into_table_ref();
        self.execute_batches(rows, self.dialect().max_rows_per_key_batch(K), |rows| {
            Query::update()
                .table(table.clone())
                .values(
                    values
                        .iter()
                        .map(|(column, value)| (*column, Expr::val(value.clone()))),
                )
                .cond_where(filter.clone())
                .cond_where(matching_rows(&keys, rows.iter()))
                .to_owned()
        })
        .await
    }

    /// Update rows identified by non-null keys with different values for each row.
    pub(crate) async fn update_rows<C: IntoIden + Copy, const K: usize, const V: usize>(
        &mut self,
        table: impl IntoTableRef,
        keys: [C; K],
        columns: [C; V],
        rows: Vec<([Value; K], [Value; V])>,
    ) -> DucklakeResult<()> {
        let table = table.into_table_ref();
        self.execute_batches(&rows, self.dialect().max_rows_per_key_batch(K), |rows| {
            let mut query = Query::update();
            query.table(table.clone());
            for (index, column) in columns.iter().enumerate() {
                query.value(*column, replacement_value(&keys, rows, index, *column));
            }
            query.cond_where(matching_rows(&keys, rows.iter().map(|(keys, _)| keys)));
            query
        })
        .await
    }

    /// Copy matching rows, applying per-row replacements and fixed values to the copies.
    /// The column enum must describe every column of the table. Source keys must be unique,
    /// and copies must not match the source filter, so batches cannot copy earlier results.
    pub(crate) async fn copy_rows_with_updates<
        C: IntoIden + Copy + PartialEq + IntoEnumIterator,
        const K: usize,
        const V: usize,
        const F: usize,
    >(
        &mut self,
        table: impl IntoTableRef,
        keys: [C; K],
        columns: [C; V],
        rows: Vec<([Value; K], [Value; V])>,
        fixed: [(C, Value); F],
        filter: Condition,
    ) -> DucklakeResult<()> {
        let table = table.into_table_ref();
        let all_columns: Vec<_> = C::iter().collect();
        self.execute_batches(&rows, self.dialect().max_rows_per_key_batch(K), |rows| {
            let select = Query::select()
                .exprs(all_columns.iter().map(|column| {
                    if let Some((_, value)) = fixed.iter().find(|(key, _)| key == column) {
                        Expr::val(value.clone())
                    } else if let Some(index) = columns.iter().position(|key| key == column) {
                        replacement_value(&keys, rows, index, *column)
                    } else {
                        Expr::col(*column)
                    }
                }))
                .from(table.clone())
                .cond_where(filter.clone())
                .cond_where(matching_rows(&keys, rows.iter().map(|(keys, _)| keys)))
                .to_owned();
            Query::insert()
                .into_table(table.clone())
                .columns(all_columns.iter().copied())
                .select_from(select)
                .unwrap()
                .to_owned()
        })
        .await
    }

    /// Insert buffered catalog rows in parameter-limited batches.
    pub(crate) async fn insert_rows(
        &mut self,
        table: &str,
        columns: &[&str],
        rows: Vec<Vec<sea_query::Value>>,
    ) -> DucklakeResult<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let chunk_size = self.dialect().max_bind_params() / columns.len();
        let mut rows = rows.into_iter();
        loop {
            let chunk: Vec<_> = rows.by_ref().take(chunk_size).collect();
            if chunk.is_empty() {
                break;
            }
            let mut query = sea_query::Query::insert();
            query
                .into_table(table.to_owned())
                .columns(columns.iter().map(|c| (*c).to_owned()));
            for row in chunk {
                query.values_panic(row.into_iter().map(Expr::val));
            }
            self.execute(&query).await?;
        }
        Ok(())
    }

    async fn execute_batches<T, Q: SqlConvertible>(
        &mut self,
        items: &[T],
        max_items: usize,
        query: impl Fn(&[T]) -> Q,
    ) -> DucklakeResult<()> {
        for query in batch_queries(self.dialect(), items, max_items, query) {
            let (sql, values) = query?;
            log_sql(sql.as_str(), Some(&values));
            dispatch_tx!(self, tx => {
                sqlx::query_with(sql, values).execute(&mut **tx).await?;
            });
        }
        Ok(())
    }
}

fn matching_row<C: IntoIden + Copy, const K: usize>(keys: &[C; K], row: &[Value; K]) -> Condition {
    keys.iter()
        .zip(row)
        .fold(Condition::all(), |condition, (key, value)| {
            condition.add(Expr::col(*key).eq(value.clone()))
        })
}

fn matching_rows<'a, C: IntoIden + Copy, const K: usize>(
    keys: &[C; K],
    rows: impl Iterator<Item = &'a [Value; K]>,
) -> Condition {
    if K == 1 {
        Condition::all().add(Expr::col(keys[0]).is_in(rows.map(|row| row[0].clone())))
    } else {
        rows.fold(Condition::any(), |condition, row| {
            condition.add(matching_row(keys, row))
        })
    }
}

fn replacement_value<C: IntoIden + Copy, const K: usize, const V: usize>(
    keys: &[C; K],
    rows: &[([Value; K], [Value; V])],
    index: usize,
    column: C,
) -> Expr {
    let mut values = CaseStatement::new();
    for (row_keys, row_values) in rows {
        values = values.case(matching_row(keys, row_keys), row_values[index].clone());
    }
    values.finally(Expr::col(column)).into()
}

fn batch_queries<T, Q: SqlConvertible>(
    dialect: Dialect,
    mut items: &[T],
    max_items: usize,
    query: impl Fn(&[T]) -> Q,
) -> impl Iterator<Item = DucklakeResult<(sqlx::SqlStr, SqlxValues)>> {
    std::iter::from_fn(move || {
        if items.is_empty() {
            return None;
        }
        let mut count = items.len().min(max_items);
        loop {
            let (sql, values) = query(&items[..count]).to_sql(dialect);
            if values.0.0.len() <= dialect.max_bind_params() {
                items = &items[count..];
                return Some(Ok((sql, values)));
            }
            if count == 1 {
                items = &[];
                return Some(Err(sqlx::Error::Protocol(
                    "a single batch item exceeds the database bind parameter limit".into(),
                )
                .into()));
            }
            count /= 2;
        }
    })
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use ducklake_tag::Column;
    use sea_query::{Expr, Query};

    use super::*;
    use crate::db::sea_query_ext::CreateTable;
    use crate::spec::{DucklakeTag, ducklake_tag};

    fn tag(id: i64, key: &str, value: &str) -> DucklakeTag {
        DucklakeTag {
            object_id: id,
            begin_snapshot: 1,
            end_snapshot: None,
            key: key.into(),
            value: value.into(),
        }
    }

    async fn empty_transaction() -> Transaction {
        crate::db::Pool::new("sqlite://:memory:")
            .await
            .unwrap()
            .begin()
            .await
            .unwrap()
    }

    async fn tag_table(rows: impl IntoIterator<Item = DucklakeTag>) -> Transaction {
        let mut tx = empty_transaction().await;
        tx.execute(&sea_query::Table::create_entity::<DucklakeTag>(
            tx.dialect(),
        ))
        .await
        .unwrap();
        tx.insert_entities(rows).await.unwrap();
        tx
    }

    async fn stored_tags(tx: &mut Transaction) -> Vec<DucklakeTag> {
        tx.fetch_all(
            &Query::select()
                .column(Asterisk)
                .from(ducklake_tag::Table)
                .order_by(Column::ObjectId, sea_query::Order::Asc)
                .order_by(Column::BeginSnapshot, sea_query::Order::Asc)
                .order_by(Column::Key, sea_query::Order::Asc)
                .to_owned(),
        )
        .await
        .unwrap()
    }

    fn batch_keys() -> Vec<[Value; 2]> {
        (0..257_i64)
            .map(|id| [id.into(), "selected".into()])
            .collect()
    }

    async fn batch_tag_table() -> Transaction {
        tag_table(
            (0..257)
                .map(|id| tag(id, "selected", "original"))
                .chain([tag(0, "untouched", "original")]),
        )
        .await
    }

    #[tokio::test]
    async fn delete_rows_matches_compound_keys_across_batches() {
        let mut tx = batch_tag_table().await;

        tx.delete_rows(
            ducklake_tag::Table,
            [Column::ObjectId, Column::Key],
            &batch_keys(),
        )
        .await
        .unwrap();

        let remaining: Vec<_> = stored_tags(&mut tx)
            .await
            .into_iter()
            .map(|row| (row.object_id, row.key))
            .collect();
        assert_eq!(remaining, [(0, "untouched".into())]);
    }

    #[tokio::test]
    async fn fetch_rows_matches_compound_keys_across_batches() {
        let mut tx = batch_tag_table().await;

        let rows: Vec<DucklakeTag> = tx
            .fetch_rows(
                ducklake_tag::Table,
                [Column::ObjectId, Column::Key],
                &batch_keys(),
            )
            .await
            .unwrap();

        let mut keys: Vec<_> = rows
            .into_iter()
            .map(|row| (row.object_id, row.key))
            .collect();
        keys.sort();
        assert_eq!(
            keys,
            (0..257)
                .map(|id| (id, "selected".into()))
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn update_matching_rows_preserves_retired_and_unmatched_rows() {
        let mut old = tag(0, "selected", "old");
        old.begin_snapshot = 0;
        old.end_snapshot = Some(1);
        let mut tx = tag_table([
            old,
            tag(0, "selected", "live"),
            tag(1, "selected", "live"),
            tag(0, "untouched", "live"),
        ])
        .await;

        tx.update_matching_rows(
            ducklake_tag::Table,
            [Column::ObjectId, Column::Key],
            &[
                [0_i64.into(), "selected".into()],
                [1_i64.into(), "selected".into()],
            ],
            [(Column::EndSnapshot, 2_i64.into())],
            Condition::all().add(Expr::col(Column::EndSnapshot).is_null()),
        )
        .await
        .unwrap();

        let ends: Vec<_> = stored_tags(&mut tx)
            .await
            .into_iter()
            .map(|row| row.end_snapshot)
            .collect();
        assert_eq!(ends, [Some(1), Some(2), None, Some(2)]);
    }

    #[tokio::test]
    async fn update_rows_applies_values_to_their_keys() {
        let mut tx = tag_table((0..3).map(|id| tag(id, "key", "original"))).await;

        tx.update_rows(
            ducklake_tag::Table,
            [Column::ObjectId],
            [Column::Value],
            vec![
                ([1_i64.into()], ["first".into()]),
                ([0_i64.into()], ["second".into()]),
            ],
        )
        .await
        .unwrap();

        let values: Vec<_> = stored_tags(&mut tx)
            .await
            .into_iter()
            .map(|row| row.value)
            .collect();
        assert_eq!(values, ["second", "first", "original"]);
    }

    #[tokio::test]
    async fn copy_rows_preserves_history_and_applies_replacements() {
        let mut old = tag(0, "key", "historical");
        old.begin_snapshot = 0;
        old.end_snapshot = Some(1);
        let current = (0..3).map(|id| DucklakeTag {
            end_snapshot: Some(2),
            ..tag(id, "key", "original")
        });
        let mut tx = tag_table([old].into_iter().chain(current)).await;

        tx.copy_rows_with_updates(
            ducklake_tag::Table,
            [Column::ObjectId],
            [Column::Value],
            vec![
                ([1_i64.into()], ["first".into()]),
                ([0_i64.into()], ["second".into()]),
            ],
            [
                (Column::BeginSnapshot, 2_i64.into()),
                (Column::EndSnapshot, None::<i64>.into()),
            ],
            Condition::all().add(Expr::col(Column::EndSnapshot).eq(2_i64)),
        )
        .await
        .unwrap();

        let versions: Vec<_> = stored_tags(&mut tx)
            .await
            .into_iter()
            .map(|row| {
                (
                    row.object_id,
                    row.begin_snapshot,
                    row.end_snapshot,
                    row.value,
                )
            })
            .collect();
        assert_eq!(
            versions,
            [
                (0, 0, Some(1), "historical".into()),
                (0, 1, Some(2), "original".into()),
                (0, 2, None, "second".into()),
                (1, 1, Some(2), "original".into()),
                (1, 2, None, "first".into()),
                (2, 1, Some(2), "original".into()),
            ]
        );
    }

    #[tokio::test]
    async fn empty_insert_does_not_query_the_table() {
        let mut tx = empty_transaction().await;
        tx.insert_entities(Vec::<DucklakeTag>::new()).await.unwrap();
    }

    #[tokio::test]
    async fn empty_delete_does_not_query_the_table() {
        let mut tx = empty_transaction().await;
        tx.delete_rows(ducklake_tag::Table, [Column::ObjectId], &[])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn empty_fetch_does_not_query_the_table() {
        let mut tx = empty_transaction().await;
        let rows: Vec<DucklakeTag> = tx
            .fetch_rows(ducklake_tag::Table, [Column::ObjectId], &[])
            .await
            .unwrap();
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn empty_matching_update_does_not_query_the_table() {
        let mut tx = empty_transaction().await;
        tx.update_matching_rows(
            ducklake_tag::Table,
            [Column::ObjectId],
            &[],
            [(Column::Value, "unused".into())],
            Condition::all(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn empty_update_does_not_query_the_table() {
        let mut tx = empty_transaction().await;
        tx.update_rows(
            ducklake_tag::Table,
            [Column::ObjectId],
            [Column::Value],
            Vec::new(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn empty_copy_does_not_query_the_table() {
        let mut tx = empty_transaction().await;
        tx.copy_rows_with_updates(
            ducklake_tag::Table,
            [Column::ObjectId],
            [Column::Value],
            Vec::new(),
            [(Column::BeginSnapshot, 2_i64.into())],
            Condition::all(),
        )
        .await
        .unwrap();
    }

    #[test]
    fn batches_respect_actual_parameter_counts() {
        let items = [1_i64, 2, 3];
        let batches: Vec<_> = batch_queries(Dialect::Sqlite, &items, 256, |items| {
            Query::select()
                .expr(Expr::val(99_i64))
                .exprs(
                    items
                        .iter()
                        .flat_map(|&item| std::iter::repeat_n(Expr::val(item), 16_000)),
                )
                .to_owned()
        })
        .map(|query| query.unwrap().1.0.0)
        .collect();

        assert_eq!(
            batches.iter().map(Vec::len).collect::<Vec<_>>(),
            [16_001, 32_001]
        );
        let values: Vec<_> = batches
            .into_iter()
            .flat_map(|values| values.into_iter().skip(1))
            .collect();
        assert_eq!(
            values,
            items
                .into_iter()
                .flat_map(|item| std::iter::repeat_n(sea_query::Value::from(item), 16_000))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn oversized_item_returns_error_and_stops() {
        let items = [()];
        let mut batches = batch_queries(Dialect::Sqlite, &items, 256, |_| {
            Query::select()
                .exprs(std::iter::repeat_n(
                    Expr::val(1_i64),
                    Dialect::Sqlite.max_bind_params() + 1,
                ))
                .to_owned()
        });

        assert!(matches!(
            batches.next(),
            Some(Err(crate::DucklakeError::Database(sqlx::Error::Protocol(
                _
            ))))
        ));
        assert!(batches.next().is_none());
    }
}
