use std::collections::{HashMap, HashSet};

use arrow_array::RecordBatch;
use sea_query::{ColumnDef, Condition, Expr, ExprTrait, IntoIden, Table, Value};
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
    pub new_macros: Vec<DucklakeMacro>,
    pub new_macro_impls: Vec<DucklakeMacroImpl>,
    pub new_macro_parameters: Vec<DucklakeMacroParameters>,
    pub new_columns: Vec<DucklakeColumn>,
    pub new_partition_info: Vec<DucklakePartitionInfo>,
    pub new_partition_columns: Vec<DucklakePartitionColumn>,
    pub new_sort_info: Vec<DucklakeSortInfo>,
    pub new_sort_expressions: Vec<DucklakeSortExpression>,
    pub new_tags: Vec<DucklakeTag>,
    pub new_column_tags: Vec<DucklakeColumnTag>,
    pub dropped_schemas: HashSet<i64>,
    pub dropped_tables: HashSet<i64>,
    pub dropped_views: HashSet<i64>,
    pub dropped_macros: HashSet<i64>,
    pub detached_tables: HashSet<i64>,
    pub renamed_tables: HashMap<i64, String>,
    pub retired_partition_tables: HashSet<i64>,
    pub retired_sort_tables: HashSet<i64>,
    pub retired_columns: HashSet<(i64, i64)>,
    pub table_tags: HashMap<i64, Vec<TagChange>>,
    pub column_tags: HashMap<(i64, i64), Vec<TagChange>>,
    pub files: FileChanges,
    pub inline_tables: HashMap<i64, crate::Schema>,
    pub inline_write_schemas: HashMap<i64, crate::Schema>,
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
        tx: &mut db::Transaction<'_>,
        state: &mut CommitState<'_>,
    ) -> DucklakeResult<()> {
        let snapshot_id = state.snapshot_id();

        macro_rules! delete {
            ($table:ident, $column:ident, $ids:expr) => {
                tx.delete_rows($table::Table, [$table::Column::$column], &$ids)
                    .await?;
            };
        }
        macro_rules! retire {
            ($table:ident, $column:ident, $ids:expr) => {
                retire_rows(
                    tx,
                    snapshot_id,
                    $table::Table,
                    [$table::Column::$column],
                    $table::Column::EndSnapshot,
                    $ids.map(|id| [(*id).into()]).collect(),
                )
                .await?;
            };
            ($table:ident, [$($column:ident),+], $rows:expr) => {
                retire_rows(
                    tx,
                    snapshot_id,
                    $table::Table,
                    [$($table::Column::$column),+],
                    $table::Column::EndSnapshot,
                    $rows,
                )
                .await?;
            };
        }

        // Detaching transfers ownership of file metadata, so remove those rows physically.
        let detached_tables: Vec<_> = self
            .detached_tables
            .into_iter()
            .map(|id| [id.into()])
            .collect();
        delete!(ducklake_file_column_stats, TableId, detached_tables);
        delete!(ducklake_file_partition_value, TableId, detached_tables);
        delete!(ducklake_data_file, TableId, detached_tables);
        delete!(ducklake_delete_file, TableId, detached_tables);

        retire!(
            ducklake_table,
            TableId,
            self.dropped_tables.iter().chain(self.renamed_tables.keys())
        );
        retire!(ducklake_schema, SchemaId, self.dropped_schemas.iter());
        retire!(ducklake_view, ViewId, self.dropped_views.iter());
        retire!(ducklake_macro, MacroId, self.dropped_macros.iter());
        retire!(ducklake_column, TableId, self.dropped_tables.iter());
        retire!(ducklake_column_tag, TableId, self.dropped_tables.iter());
        retire!(ducklake_data_file, TableId, self.dropped_tables.iter());
        retire!(ducklake_delete_file, TableId, self.dropped_tables.iter());
        retire!(
            ducklake_partition_info,
            TableId,
            self.dropped_tables
                .iter()
                .chain(&self.retired_partition_tables)
        );
        retire!(
            ducklake_sort_info,
            TableId,
            self.dropped_tables.iter().chain(&self.retired_sort_tables)
        );
        retire!(
            ducklake_tag,
            ObjectId,
            self.dropped_tables
                .iter()
                .chain(&self.dropped_schemas)
                .chain(&self.dropped_views)
        );
        retire!(
            ducklake_column,
            [TableId, ColumnId],
            self.retired_columns
                .iter()
                .map(|&(table, column)| [table.into(), column.into()])
                .collect()
        );

        tx.insert_entities(self.new_schemas).await?;
        persist_table_renames(tx, snapshot_id, self.renamed_tables).await?;
        tx.insert_entities(self.new_tables).await?;
        tx.insert_entities(self.new_views).await?;
        tx.insert_entities(self.new_macros).await?;
        tx.insert_entities(self.new_macro_impls).await?;
        tx.insert_entities(self.new_macro_parameters).await?;
        tx.insert_entities(self.new_columns).await?;
        tx.insert_entities(self.new_partition_info).await?;
        tx.insert_entities(self.new_partition_columns).await?;
        tx.insert_entities(self.new_sort_info).await?;
        tx.insert_entities(self.new_sort_expressions).await?;
        tx.insert_entities(self.new_tags).await?;
        tx.insert_entities(self.new_column_tags).await?;

        // Initial tags are ordinary inserts. Explicit edits retire earlier values using SQL's
        // key equality, including values inserted earlier in this same commit.
        persist_tags(
            tx,
            snapshot_id,
            ducklake_tag::Table,
            [ducklake_tag::Column::ObjectId, ducklake_tag::Column::Key],
            ducklake_tag::Column::EndSnapshot,
            self.table_tags,
            |id, key| [(*id).into(), key.into()],
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
            ducklake_column_tag::Table,
            [
                ducklake_column_tag::Column::TableId,
                ducklake_column_tag::Column::ColumnId,
                ducklake_column_tag::Column::Key,
            ],
            ducklake_column_tag::Column::EndSnapshot,
            self.column_tags,
            |&(table, column), key| [table.into(), column.into(), key.into()],
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
            self.inline_write_schemas,
            self.inline_data,
            state.schema_version(),
        )
        .await?;
        persist_statistics(tx, state, self.written_columns).await
    }
}

/* ------------------------------------------ DELETION ----------------------------------------- */

async fn retire_rows<C: IntoIden + Copy, const K: usize>(
    tx: &mut db::Transaction<'_>,
    snapshot_id: i64,
    table: impl IntoIden,
    keys: [C; K],
    end_snapshot: C,
    rows: Vec<[Value; K]>,
) -> DucklakeResult<()> {
    tx.update_matching_rows(
        table,
        keys,
        &rows,
        [(end_snapshot, snapshot_id.into())],
        Condition::all().add(Expr::col(end_snapshot).is_null()),
    )
    .await
}

/* ---------------------------------------- PERSISTENCE ---------------------------------------- */

async fn persist_table_renames(
    tx: &mut db::Transaction<'_>,
    snapshot_id: i64,
    renames: HashMap<i64, String>,
) -> DucklakeResult<()> {
    use ducklake_table::Column;

    tx.copy_rows_with_updates(
        ducklake_table::Table,
        [Column::TableId],
        [Column::TableName],
        renames
            .into_iter()
            .map(|(id, name)| ([id.into()], [name.into()]))
            .collect(),
        [
            (Column::BeginSnapshot, snapshot_id.into()),
            (Column::EndSnapshot, None::<i64>.into()),
        ],
        Condition::all().add(Expr::col(Column::EndSnapshot).eq(snapshot_id)),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn persist_tags<K: Clone, E: InsertableEntity, C: IntoIden + Copy, const N: usize>(
    tx: &mut db::Transaction<'_>,
    snapshot_id: i64,
    table: impl IntoIden,
    keys: [C; N],
    end_snapshot: C,
    changes: HashMap<K, Vec<TagChange>>,
    key_values: impl Fn(&K, &str) -> [Value; N],
    entity: impl Fn(K, String, String) -> E,
) -> DucklakeResult<()> {
    let table = table.into_iden();
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
                retirements.push(key_values(key, &change.key));
                if let Some(value) = change.value {
                    inserts.push(entity(key.clone(), change.key, value));
                }
            }
        }
        if retirements.is_empty() {
            break;
        }
        retire_rows(
            tx,
            snapshot_id,
            table.clone(),
            keys,
            end_snapshot,
            retirements,
        )
        .await?;
        tx.insert_entities(inserts).await?;
    }
    Ok(())
}

async fn persist_files(tx: &mut db::Transaction<'_>, files: FileChanges) -> DucklakeResult<()> {
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
    tx: &mut db::Transaction<'_>,
    mut tables: HashMap<i64, crate::Schema>,
    write_schemas: HashMap<i64, crate::Schema>,
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
    // Resolve existing registrations before creating tables needed by inline writes.
    let existing: Vec<_> = data.keys().map(|id| [(*id).into()]).collect();
    let existing_tables: Vec<DucklakeInlinedDataTables> = tx
        .fetch_rows(
            ducklake_inlined_data_tables::Table,
            [ducklake_inlined_data_tables::Column::TableId],
            &existing,
        )
        .await?;
    // Fetching by key does not guarantee ordering; select the latest registration per table.
    let mut latest: HashMap<i64, DucklakeInlinedDataTables> = HashMap::new();
    for table in existing_tables {
        if latest
            .get(&table.table_id)
            .is_none_or(|previous| table.schema_version > previous.schema_version)
        {
            latest.insert(table.table_id, table);
        }
    }
    names.extend(
        latest
            .into_iter()
            .filter(|(_, table)| table.schema_version >= schema_version)
            .map(|(id, table)| (id, table.table_name)),
    );
    // Tables created while inlining was disabled get their storage on the first inline write.
    for (table_id, schema) in write_schemas {
        if !names.contains_key(&table_id) {
            tables.entry(table_id).or_insert(schema);
        }
    }
    let mut registrations = Vec::new();
    for (table_id, schema) in tables {
        let name = DucklakeInlinedData::table_name(table_id, schema_version);
        let dialect = tx.dialect();
        let mut query = Table::create();
        query.table(name.clone());
        for column in ducklake_inlined_data::Column::iter() {
            query.col(ColumnDef::new_with_type(column, dialect.column_type_i64()));
        }
        for (column, info) in schema.columns {
            query.col(ColumnDef::new_with_type(
                column,
                dialect.column_type_for_data_inlining(&info.dtype)?,
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
    tx: &mut db::Transaction<'_>,
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

#[cfg(all(test, feature = "sqlite"))]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use rstest::{fixture, rstest};
    use sea_query::{Asterisk, Query};

    use crate::spec::{DucklakeInlinedDataTables, ducklake_inlined_data_tables};
    use crate::{Column, CreateOptions, DataType, Ducklake, IfExistsStrategy};

    #[fixture]
    async fn lake() -> Ducklake {
        Ducklake::create(CreateOptions::new(
            "sqlite://:memory:",
            "file:///tmp/inline-storage-tests/",
        ))
        .await
        .unwrap()
    }

    async fn create_table(lake: &Ducklake) -> crate::Table {
        lake.create_table(
            "example",
            vec![Column::new("x".into(), DataType::Int64)],
            None,
            None,
            None,
            None,
            IfExistsStrategy::Fail,
        )
        .await
        .unwrap()
    }

    async fn registrations(lake: &Ducklake) -> Vec<DucklakeInlinedDataTables> {
        lake.conn
            .pool()
            .fetch_all(
                &Query::select()
                    .column(Asterisk)
                    .from(ducklake_inlined_data_tables::Table)
                    .to_owned(),
            )
            .await
            .unwrap()
    }

    async fn assert_storage_count(lake: &Ducklake, expected: usize) {
        let registrations = registrations(lake).await;
        assert_eq!(registrations.len(), expected);
        let physical = lake.conn.pool().list_tables().await.unwrap();
        assert_eq!(
            physical
                .iter()
                .filter(|name| name.starts_with("ducklake_inlined_data_")
                    && *name != "ducklake_inlined_data_tables")
                .count(),
            expected,
        );
        for registration in registrations {
            assert!(physical.contains(&registration.table_name));
        }
    }

    fn batch(columns: &[(&str, Vec<i64>)]) -> RecordBatch {
        RecordBatch::try_from_iter(columns.iter().map(|(name, values)| {
            (
                *name,
                Arc::new(Int64Array::from(values.clone())) as ArrayRef,
            )
        }))
        .unwrap()
    }

    #[rstest]
    #[case("global", "0")]
    #[case("global", "10")]
    #[case("schema", "0")]
    #[case("schema", "10")]
    #[case("table", "0")]
    #[case("table", "10")]
    #[tokio::test]
    async fn test_eager_storage_respects_effective_limit(
        #[future(awt)] lake: Ducklake,
        #[case] scope: &str,
        #[case] limit: &str,
    ) {
        // Arrange
        let inherited = if limit == "0" { "10" } else { "0" };
        lake.set_metadata("data_inlining_row_limit", inherited, None)
            .await
            .unwrap();
        if scope != "table" {
            lake.set_metadata(
                "data_inlining_row_limit",
                limit,
                (scope == "schema").then_some("main"),
            )
            .await
            .unwrap();
        }

        // Act
        let table = create_table(&lake).await;
        let initial = usize::from(if scope == "table" { inherited } else { limit } != "0");
        assert_storage_count(&lake, initial).await;
        if scope == "table" {
            table
                .set_metadata("data_inlining_row_limit", limit)
                .await
                .unwrap();
        }
        table
            .add_column(Column::new("y".into(), DataType::Int64))
            .await
            .unwrap();

        // Assert
        assert_storage_count(&lake, initial + usize::from(limit != "0")).await;
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn test_first_inline_write_creates_storage_and_later_writes_reuse_it(
        #[future(awt)] lake: Ducklake,
        #[case] reenable: bool,
    ) {
        // Arrange
        lake.set_metadata("data_inlining_row_limit", "0", None)
            .await
            .unwrap();
        let table = create_table(&lake).await;
        assert_storage_count(&lake, 0).await;
        if reenable {
            lake.set_metadata("data_inlining_row_limit", "10", None)
                .await
                .unwrap();
        }
        let data = batch(&[("x", vec![1, 2])]);

        // Act
        table.write_inline_data(vec![data.clone()]).await.unwrap();
        let first_name = registrations(&lake).await.remove(0).table_name;
        table.write_inline_data(vec![data]).await.unwrap();
        lake.set_metadata("data_inlining_row_limit", "0", None)
            .await
            .unwrap();

        // Assert
        assert_storage_count(&lake, 1).await;
        assert_eq!(registrations(&lake).await[0].table_name, first_name);
        let scan = table.scan().await.unwrap();
        let values: Vec<_> = scan
            .inline_data
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(values, vec![1, 2, 1, 2]);
    }

    #[rstest]
    #[tokio::test]
    async fn test_inline_write_after_disabled_schema_change_preserves_history(
        #[future(awt)] lake: Ducklake,
    ) {
        // Arrange
        let table = create_table(&lake).await;
        table
            .write_inline_data(vec![batch(&[("x", vec![1])])])
            .await
            .unwrap();
        let original_name = registrations(&lake).await.remove(0).table_name;
        lake.set_metadata("data_inlining_row_limit", "0", None)
            .await
            .unwrap();
        table
            .add_column(Column::new("y".into(), DataType::Int64))
            .await
            .unwrap();
        assert_storage_count(&lake, 1).await;
        lake.set_metadata("data_inlining_row_limit", "10", None)
            .await
            .unwrap();

        // Act
        table
            .write_inline_data(vec![batch(&[("x", vec![2]), ("y", vec![3])])])
            .await
            .unwrap();

        // Assert
        assert_storage_count(&lake, 2).await;
        assert!(
            registrations(&lake)
                .await
                .iter()
                .any(|entry| entry.table_name == original_name)
        );
        let scan = table.scan().await.unwrap();
        let mut rows = Vec::new();
        for batch in scan.inline_data {
            let x = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let y = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            rows.extend(x.iter().zip(y.iter()));
        }
        rows.sort();
        assert_eq!(rows, vec![(Some(1), None), (Some(2), Some(3))]);
    }
}
