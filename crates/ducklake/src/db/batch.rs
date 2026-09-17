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
    use sea_query::{Expr, Query};

    use super::*;
    use crate::db::sea_query_ext::CreateTable;
    use crate::spec::{DucklakeTag, ducklake_tag};

    #[tokio::test]
    async fn bulk_operations_preserve_unmatched_rows_and_history() {
        use ducklake_tag::Column;

        let pool = crate::db::Pool::new("sqlite://:memory:").await.unwrap();
        let mut tx = pool.begin().await.unwrap();
        tx.execute(&sea_query::Table::create_entity::<DucklakeTag>(
            tx.dialect(),
        ))
        .await
        .unwrap();
        tx.insert_entities(
            (0..600)
                .map(|id| DucklakeTag {
                    object_id: id,
                    begin_snapshot: 1,
                    end_snapshot: None,
                    key: "shared".into(),
                    value: "original".into(),
                })
                .chain([DucklakeTag {
                    object_id: 99,
                    begin_snapshot: 1,
                    end_snapshot: None,
                    key: "untouched".into(),
                    value: "original".into(),
                }]),
        )
        .await
        .unwrap();

        let keys: Vec<_> = (0..600_i64)
            .map(|id| [id.into(), "shared".into()])
            .collect();
        tx.update_matching_rows(
            ducklake_tag::Table,
            [Column::ObjectId, Column::Key],
            &keys,
            [(Column::EndSnapshot, 2_i64.into())],
            Condition::all().add(Expr::col(Column::EndSnapshot).is_null()),
        )
        .await
        .unwrap();
        tx.copy_rows_with_updates(
            ducklake_tag::Table,
            [Column::ObjectId, Column::Key],
            [Column::Value],
            keys.iter()
                .cloned()
                .map(|keys| (keys, ["copied".into()]))
                .collect(),
            [
                (Column::BeginSnapshot, 2_i64.into()),
                (Column::EndSnapshot, None::<i64>.into()),
            ],
            Condition::all().add(Expr::col(Column::EndSnapshot).eq(2_i64)),
        )
        .await
        .unwrap();
        tx.update_rows(
            ducklake_tag::Table,
            [Column::ObjectId, Column::Key, Column::BeginSnapshot],
            [Column::Value],
            (0..600_i64)
                .map(|id| {
                    (
                        [id.into(), "shared".into(), 2_i64.into()],
                        ["updated".into()],
                    )
                })
                .collect(),
        )
        .await
        .unwrap();

        let ids: Vec<_> = (0..600_i64).map(|id| [id.into()]).collect();
        let rows: Vec<DucklakeTag> = tx
            .fetch_rows(ducklake_tag::Table, [Column::ObjectId], &ids)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1201);
        assert_eq!(
            rows.iter()
                .filter(|row| row.end_snapshot == Some(2) && row.value == "original")
                .count(),
            600
        );
        assert_eq!(
            rows.iter()
                .filter(|row| row.begin_snapshot == 2
                    && row.end_snapshot.is_none()
                    && row.value == "updated")
                .count(),
            600
        );

        tx.delete_rows(
            ducklake_tag::Table,
            [Column::ObjectId, Column::Key, Column::BeginSnapshot],
            &(0..600_i64)
                .map(|id| [id.into(), "shared".into(), 1_i64.into()])
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        let rows: Vec<DucklakeTag> = tx
            .fetch_rows(ducklake_tag::Table, [Column::ObjectId], &ids)
            .await
            .unwrap();
        assert_eq!(rows.len(), 601);
        assert!(rows.iter().all(|row| row.end_snapshot.is_none()));
        assert!(rows.iter().any(|row| row.key == "untouched"
            && row.value == "original"
            && row.begin_snapshot == 1));
        tx.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn empty_operations_do_not_query_the_table() {
        use ducklake_tag::Column;

        let pool = crate::db::Pool::new("sqlite://:memory:").await.unwrap();
        let mut tx = pool.begin().await.unwrap();
        tx.insert_entities(Vec::<DucklakeTag>::new()).await.unwrap();
        tx.delete_rows(ducklake_tag::Table, [Column::ObjectId], &[])
            .await
            .unwrap();
        tx.update_matching_rows(
            ducklake_tag::Table,
            [Column::ObjectId],
            &[],
            [(Column::Value, "unused".into())],
            Condition::all(),
        )
        .await
        .unwrap();
        tx.update_rows(
            ducklake_tag::Table,
            [Column::ObjectId],
            [Column::Value],
            Vec::new(),
        )
        .await
        .unwrap();
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
        let rows: Vec<DucklakeTag> = tx
            .fetch_rows(ducklake_tag::Table, [Column::ObjectId], &[])
            .await
            .unwrap();
        assert!(rows.is_empty());
        tx.rollback().await.unwrap();
    }

    #[test]
    fn empty_input_does_not_build_a_query() {
        let items: [i64; 0] = [];
        let mut batches = batch_queries(
            Dialect::Sqlite,
            &items,
            256,
            |_| -> sea_query::SelectStatement { panic!("empty input must not build a query") },
        );
        assert!(batches.next().is_none());
    }

    #[test]
    fn batches_preserve_items_and_bound_expression_depth() {
        let items: Vec<i64> = (0..513).collect();
        let batches: Vec<_> = batch_queries(Dialect::Sqlite, &items, 256, |items| {
            Query::select()
                .exprs(items.iter().copied().map(Expr::val))
                .to_owned()
        })
        .map(|query| query.unwrap().1.0.0)
        .collect();

        assert_eq!(
            batches.iter().map(Vec::len).collect::<Vec<_>>(),
            [256, 256, 1]
        );
        assert_eq!(
            batches.into_iter().flatten().collect::<Vec<_>>(),
            items
                .into_iter()
                .map(sea_query::Value::from)
                .collect::<Vec<_>>()
        );
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
