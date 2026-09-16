use std::collections::{HashMap, HashSet};

use arrow_array::RecordBatch;
use sea_query::{Asterisk, CaseStatement, ColumnDef, Condition, Expr, ExprTrait, Query, Table};
use strum::IntoEnumIterator;

use crate::db::sea_query_ext::{CreateTable, InsertableEntity};
use crate::spec::*;
use crate::transaction::CommitState;
use crate::{DucklakeResult, db};

/// Typed changes collected for one commit attempt, before any catalog writes.
#[derive(Default)]
pub(super) struct TransactionChanges {
    pub new_schemas: Vec<DucklakeSchema>,
    pub new_tables: Vec<DucklakeTable>,
    pub new_views: Vec<DucklakeView>,
    pub new_columns: Vec<DucklakeColumn>,
    pub new_partition_info: Vec<DucklakePartitionInfo>,
    pub new_partition_columns: Vec<DucklakePartitionColumn>,
    pub new_tags: Vec<DucklakeTag>,
    pub new_column_tags: Vec<DucklakeColumnTag>,
    pub dropped_schemas: HashSet<i64>,
    pub dropped_tables: HashSet<i64>,
    pub dropped_views: HashSet<i64>,
    pub detached_tables: HashSet<i64>,
    pub renamed_tables: HashMap<i64, String>,
    pub retired_partition_tables: HashSet<i64>,
    pub retired_columns: HashSet<(i64, i64)>,
    pub table_tags: HashMap<i64, Vec<TagChange>>,
    pub column_tags: HashMap<(i64, i64), Vec<TagChange>>,
    pub files: FileChanges,
    pub inline_tables: HashMap<i64, crate::Schema>,
    pub inline_data: HashMap<i64, Vec<RecordBatch>>,
    pub written_columns: HashMap<i64, HashSet<i64>>,
}

/// Explicit tag edits retain their order because SQL determines key equality.
pub(super) struct TagChange {
    pub key: String,
    pub value: Option<String>,
}

#[derive(Default)]
pub(super) struct FileChanges {
    pub data_files: Vec<DucklakeDataFile>,
    pub partition_values: Vec<DucklakeFilePartitionValue>,
    pub column_stats: Vec<DucklakeFileColumnStats>,
    pub delete_files: Vec<DucklakeDeleteFile>,
    pub inline_deletes: HashMap<i64, Vec<DucklakeInlinedDelete>>,
}

impl TransactionChanges {
    /// Persist the collected changes in dependency order within a single transaction.
    pub(super) async fn persist(
        self,
        tx: &mut db::Transaction,
        state: &mut CommitState<'_>,
    ) -> DucklakeResult<()> {
        let snapshot_id = state.snapshot_id();

        // Detaching transfers ownership of file metadata, so remove those rows physically.
        for table in [
            "ducklake_file_column_stats",
            "ducklake_file_partition_value",
            "ducklake_data_file",
            "ducklake_delete_file",
        ] {
            let ids: Vec<_> = self.detached_tables.iter().copied().collect();
            for ids in ids.chunks(256) {
                let query = Query::delete()
                    .from_table(table)
                    .and_where(Expr::col("table_id").is_in(ids.iter().copied()))
                    .to_owned();
                tx.execute(&query).await?;
            }
        }

        let retired_tables: Vec<_> = self
            .dropped_tables
            .iter()
            .copied()
            .chain(self.renamed_tables.keys().copied())
            .collect();
        retire_ids(
            tx,
            snapshot_id,
            "ducklake_table",
            "table_id",
            retired_tables,
        )
        .await?;
        retire_ids(
            tx,
            snapshot_id,
            "ducklake_schema",
            "schema_id",
            self.dropped_schemas.iter().copied().collect(),
        )
        .await?;
        retire_ids(
            tx,
            snapshot_id,
            "ducklake_view",
            "view_id",
            self.dropped_views.iter().copied().collect(),
        )
        .await?;
        for table in [
            "ducklake_column",
            "ducklake_column_tag",
            "ducklake_data_file",
            "ducklake_delete_file",
        ] {
            retire_ids(
                tx,
                snapshot_id,
                table,
                "table_id",
                self.dropped_tables.iter().copied().collect(),
            )
            .await?;
        }
        retire_ids(
            tx,
            snapshot_id,
            "ducklake_partition_info",
            "table_id",
            self.dropped_tables
                .iter()
                .chain(&self.retired_partition_tables)
                .copied()
                .collect(),
        )
        .await?;
        retire_ids(
            tx,
            snapshot_id,
            "ducklake_tag",
            "object_id",
            self.dropped_tables
                .iter()
                .chain(&self.dropped_schemas)
                .chain(&self.dropped_views)
                .copied()
                .collect(),
        )
        .await?;
        retire_where(
            tx,
            snapshot_id,
            "ducklake_column",
            self.retired_columns
                .iter()
                .map(|&(table, column)| {
                    Condition::all()
                        .add(Expr::col("table_id").eq(table))
                        .add(Expr::col("column_id").eq(column))
                })
                .collect(),
        )
        .await?;

        tx.insert_entities(self.new_schemas).await?;
        persist_table_renames(tx, snapshot_id, self.renamed_tables).await?;
        tx.insert_entities(self.new_tables).await?;
        tx.insert_entities(self.new_views).await?;
        tx.insert_entities(self.new_columns).await?;
        tx.insert_entities(self.new_partition_info).await?;
        tx.insert_entities(self.new_partition_columns).await?;
        tx.insert_entities(self.new_tags).await?;
        tx.insert_entities(self.new_column_tags).await?;

        // Initial tags are ordinary inserts. Explicit edits retire earlier values using SQL's
        // key equality, including values inserted earlier in this same commit.
        persist_tags(
            tx,
            snapshot_id,
            self.table_tags,
            |id, key| {
                Condition::all()
                    .add(Expr::col("object_id").eq(*id))
                    .add(Expr::col("key").eq(key))
            },
            |object_id, key, value| DucklakeTag {
                object_id,
                begin_snapshot: snapshot_id,
                end_snapshot: None,
                key,
                value,
            },
        )
        .await?;
        persist_tags(
            tx,
            snapshot_id,
            self.column_tags,
            |&(table, column), key| {
                Condition::all()
                    .add(Expr::col("table_id").eq(table))
                    .add(Expr::col("column_id").eq(column))
                    .add(Expr::col("key").eq(key))
            },
            |(table_id, column_id), key, value| DucklakeColumnTag {
                table_id,
                column_id,
                begin_snapshot: snapshot_id,
                end_snapshot: None,
                key,
                value,
            },
        )
        .await?;

        persist_files(tx, self.files).await?;
        persist_inline_data(
            tx,
            self.inline_tables,
            self.inline_data,
            state.schema_version(),
        )
        .await?;
        persist_statistics(tx, state, self.written_columns).await
    }
}

async fn retire_ids(
    tx: &mut db::Transaction,
    snapshot_id: i64,
    table: &'static str,
    column: &'static str,
    ids: Vec<i64>,
) -> DucklakeResult<()> {
    for ids in ids.chunks(256) {
        let query = Query::update()
            .table(table)
            .value("end_snapshot", snapshot_id)
            .and_where(Expr::col("end_snapshot").is_null())
            .and_where(Expr::col(column).is_in(ids.iter().copied()))
            .to_owned();
        tx.execute(&query).await?;
    }
    Ok(())
}

async fn retire_where(
    tx: &mut db::Transaction,
    snapshot_id: i64,
    table: &'static str,
    conditions: Vec<Condition>,
) -> DucklakeResult<()> {
    // Bound expression depth and parameters for compound keys on every backend.
    for conditions in conditions.chunks(256) {
        let query = Query::update()
            .table(table)
            .value("end_snapshot", snapshot_id)
            .and_where(Expr::col("end_snapshot").is_null())
            .cond_where(
                conditions
                    .iter()
                    .cloned()
                    .fold(Condition::any(), Condition::add),
            )
            .to_owned();
        tx.execute(&query).await?;
    }
    Ok(())
}

async fn persist_table_renames(
    tx: &mut db::Transaction,
    snapshot_id: i64,
    renames: HashMap<i64, String>,
) -> DucklakeResult<()> {
    use ducklake_table::Column;

    let renames: Vec<_> = renames.into_iter().collect();
    for renames in renames.chunks(256) {
        let mut names = CaseStatement::new();
        for (table_id, name) in renames {
            names = names.case(Expr::col(Column::TableId).eq(*table_id), name.clone());
        }
        let names = names.finally(Expr::col(Column::TableName));
        let columns: Vec<_> = Column::iter().collect();
        // Copy the just-retired rows so UUIDs and paths retain their exact stored values.
        let select = Query::select()
            .exprs(columns.iter().map(|column| match column {
                Column::BeginSnapshot => Expr::val(snapshot_id),
                Column::EndSnapshot => Expr::val(None::<i64>),
                Column::TableName => names.clone().into(),
                _ => Expr::col(*column),
            }))
            .from(ducklake_table::Table)
            .and_where(Expr::col(Column::TableId).is_in(renames.iter().map(|(id, _)| *id)))
            .and_where(Expr::col(Column::EndSnapshot).eq(snapshot_id))
            .to_owned();
        let query = Query::insert()
            .into_table(ducklake_table::Table)
            .columns(columns)
            .select_from(select)
            .unwrap()
            .to_owned();
        tx.execute(&query).await?;
    }
    Ok(())
}

async fn persist_tags<K: Clone, E: InsertableEntity>(
    tx: &mut db::Transaction,
    snapshot_id: i64,
    changes: HashMap<K, Vec<TagChange>>,
    filter: impl Fn(&K, &str) -> Condition,
    entity: impl Fn(K, String, String) -> E,
) -> DucklakeResult<()> {
    let mut pending: Vec<_> = changes
        .into_iter()
        .map(|(key, changes)| (key, changes.into_iter()))
        .collect();
    loop {
        let mut retirements = Vec::new();
        let mut inserts = Vec::new();
        // Different objects are independent; edits to one object's keys must stay ordered.
        for (key, changes) in &mut pending {
            if let Some(change) = changes.next() {
                retirements.push(filter(key, &change.key));
                if let Some(value) = change.value {
                    inserts.push(entity(key.clone(), change.key, value));
                }
            }
        }
        if retirements.is_empty() {
            break;
        }
        retire_where(tx, snapshot_id, E::TABLE, retirements).await?;
        tx.insert_entities(inserts).await?;
    }
    Ok(())
}

async fn persist_files(tx: &mut db::Transaction, files: FileChanges) -> DucklakeResult<()> {
    tx.insert_entities(files.data_files).await?;
    tx.insert_entities(files.partition_values).await?;
    tx.insert_entities(files.column_stats).await?;
    tx.insert_entities(files.delete_files).await?;
    for (table_id, deletes) in files.inline_deletes {
        let name = DucklakeInlinedDelete::table_name(table_id);
        let query = Table::create_entity::<DucklakeInlinedDelete>(tx.dialect())
            .table(name.clone())
            .if_not_exists()
            .to_owned();
        tx.execute(&query).await?;
        tx.insert_entities_into(&name, deletes).await?;
    }
    Ok(())
}

async fn persist_inline_data(
    tx: &mut db::Transaction,
    tables: HashMap<i64, crate::Schema>,
    data: HashMap<i64, Vec<arrow_array::RecordBatch>>,
    schema_version: i64,
) -> DucklakeResult<()> {
    #[cfg(feature = "mysql")]
    if matches!(tx.dialect(), db::Dialect::MySql) {
        if !data.is_empty() {
            unimplemented!("data inlining is not yet implemented for MySQL");
        }
        return Ok(());
    }

    let mut names = HashMap::new();
    let mut registrations = Vec::new();
    for (table_id, schema) in tables {
        let name = DucklakeInlinedData::table_name(table_id, schema_version);
        let dialect = tx.dialect();
        let mut query = Table::create();
        query.table(name.clone());
        for column in ["row_id", "begin_snapshot", "end_snapshot"] {
            query.col(ColumnDef::new_with_type(column, dialect.column_type_i64()));
        }
        for (column, info) in schema.columns {
            query.col(ColumnDef::new_with_type(
                column,
                dialect.column_type_for_data_inlining(&info.dtype),
            ));
        }
        tx.execute(&query).await?;
        registrations.push(DucklakeInlinedDataTables {
            table_id,
            table_name: name.clone(),
            schema_version,
        });
        names.insert(table_id, name);
    }
    tx.insert_entities(registrations).await?;

    // Resolve existing inline tables together; new ones are already known in memory.
    let existing: Vec<_> = data
        .keys()
        .filter(|id| !names.contains_key(id))
        .copied()
        .collect();
    for ids in existing.chunks(256) {
        let query = Query::select()
            .column(Asterisk)
            .from(ducklake_inlined_data_tables::Table)
            .and_where(Expr::col("table_id").is_in(ids.iter().copied()))
            .order_by("schema_version", sea_query::Order::Asc)
            .to_owned();
        let tables: Vec<DucklakeInlinedDataTables> = tx.fetch_all(&query).await?;
        for table in tables {
            names.insert(table.table_id, table.table_name);
        }
    }
    for (table_id, batches) in data {
        let name = names.get(&table_id).ok_or(sqlx::Error::RowNotFound)?;
        for batch in batches {
            tx.insert_all_arrow(name, batch).await?;
        }
    }
    Ok(())
}

/// Persist the final statistics once, even when a table was written multiple times.
async fn persist_statistics(
    tx: &mut db::Transaction,
    state: &mut CommitState<'_>,
    written_columns: HashMap<i64, HashSet<i64>>,
) -> DucklakeResult<()> {
    let mut new_tables = Vec::new();
    let mut updated_tables = Vec::new();
    let mut new_columns = Vec::new();
    let mut updated_columns = Vec::new();
    for (table_id, column_ids) in written_columns {
        let column_ids: Vec<_> = column_ids
            .into_iter()
            .filter(|&column_id| state.column_stats_changed(table_id, column_id))
            .collect();
        let stats = state.table_stats(table_id).await?;
        let entity = DucklakeTableStats {
            table_id,
            record_count: stats.record_count(),
            next_row_id: stats.next_row_id(),
            file_size_bytes: stats.file_size_bytes(),
        };
        if stats.is_persisted() {
            updated_tables.push((
                [table_id.into()],
                [
                    entity.record_count.into(),
                    entity.next_row_id.into(),
                    entity.file_size_bytes.into(),
                ],
            ));
        } else {
            new_tables.push(entity);
        }

        for column_id in column_ids {
            let stats = stats.column_stats_mut(column_id);
            let entity = DucklakeTableColumnStats {
                table_id,
                column_id,
                contains_null: stats.contains_null(),
                contains_nan: stats.contains_nan(),
                min_value: stats.min_value().map(ToString::to_string),
                max_value: stats.max_value().map(ToString::to_string),
                extra_stats: None,
            };
            if stats.is_persisted() {
                updated_columns.push((
                    [table_id.into(), column_id.into()],
                    [
                        entity.contains_null.into(),
                        entity.contains_nan.into(),
                        entity.min_value.into(),
                        entity.max_value.into(),
                    ],
                ));
            } else {
                new_columns.push(entity);
            }
        }
    }

    tx.insert_entities(new_tables).await?;
    tx.insert_entities(new_columns).await?;
    use ducklake_table_stats::Column as TableColumn;
    tx.update_rows(
        ducklake_table_stats::Table,
        [TableColumn::TableId],
        [
            TableColumn::RecordCount,
            TableColumn::NextRowId,
            TableColumn::FileSizeBytes,
        ],
        updated_tables,
    )
    .await?;
    use ducklake_table_column_stats::Column as StatsColumn;
    tx.update_rows(
        ducklake_table_column_stats::Table,
        [StatsColumn::TableId, StatsColumn::ColumnId],
        [
            StatsColumn::ContainsNull,
            StatsColumn::ContainsNan,
            StatsColumn::MinValue,
            StatsColumn::MaxValue,
        ],
        updated_columns,
    )
    .await?;
    Ok(())
}
