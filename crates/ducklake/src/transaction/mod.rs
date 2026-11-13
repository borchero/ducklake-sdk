mod changes;
mod commit_state;
mod executors;
mod macro_;
mod schema;
mod table;
mod transaction_changes;
mod typedefs;
mod view;

use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use changes::{AppliedChangeSet, Change, ChangeSet};
use commit_state::CommitState;
use sea_query::{Asterisk, ExprTrait, Query};
pub use table::TransactionTable;
use transaction_changes::TransactionChanges;
pub(crate) use typedefs::TransferDataFile;
use typedefs::*;

use super::catalog::Catalog;
use crate::caches::{Metadata, Snapshot, SnapshotCache, SnapshotInfo};
use crate::primitives::Borrowed;
use crate::spec::*;
use crate::{DucklakeError, DucklakeResult, QualifiedName, db};

/// Transaction to make changes to the DuckLake.
pub struct Transaction<'a> {
    /// The snapshot cache
    snapshot_cache: Borrowed<'a, SnapshotCache>,
    /// The pool to connect to the database.
    pool: Borrowed<'a, db::Pool>,
    /// The storage options to use for connecting to cloud storage.
    storage_options: Borrowed<'a, Vec<(String, String)>>,

    /// Metadata snapshotted at the start of the transaction.
    metadata: Arc<Metadata>,
    /// The snapshot at which the transaction was started.
    snapshot: Arc<Snapshot>,
    /// The transaction-local catalog. Unless schema-affecting changes are made, this simply
    /// represents the catalog at the start of the transaction. If schema-affecting changes are
    /// made (e.g. creating a table, ...), the catalog is cloned and updated accordingly.
    catalog: Arc<Catalog>,

    /// The changes performed within this transaction.
    changes: Vec<Change>,
    /// Information about the author of the transaction.
    author_info: AuthorInfo,
}

/* ------------------------------------------ TYPEDEFS ----------------------------------------- */

/// Information about the author of a transaction. The fields are attached to the snapshot
/// created when the transaction is committed.
#[derive(Clone, Default)]
pub struct AuthorInfo {
    /// The name of the author of the transaction.
    pub author: Option<String>,
    /// A commit message describing the transaction.
    pub message: Option<String>,
    /// Additional information to attach to the snapshot.
    pub extra_info: Option<String>,
}

/// Strategy to apply when an entity's presence or absence conflicts with the requested operation.
pub enum IfExistsStrategy {
    Fail,
    Skip,
}

/* -------------------------------------------- NEW -------------------------------------------- */

impl<'a> Transaction<'a> {
    pub(crate) async fn new(
        snapshot_cache: &'a SnapshotCache,
        pool: &'a db::Pool,
        storage_options: &'a Vec<(String, String)>,
        metadata: Arc<Metadata>,
        author_info: AuthorInfo,
        snapshot: Arc<Snapshot>,
    ) -> DucklakeResult<Self> {
        let catalog = snapshot.catalog().await?.clone();
        let tx = Transaction {
            snapshot_cache: Borrowed::new(snapshot_cache),
            pool: Borrowed::new(pool),
            storage_options: Borrowed::new(storage_options),
            metadata,
            changes: Vec::new(),
            author_info,
            snapshot,
            catalog,
        };
        Ok(tx)
    }

    pub(super) fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub(super) fn catalog_mut(&mut self) -> &mut Catalog {
        Arc::make_mut(&mut self.catalog)
    }

    /// List all tables in the transaction-local catalog, optionally restricted to a specific
    /// schema.
    pub fn list_tables(&self, schema: Option<&str>) -> DucklakeResult<Vec<QualifiedName>> {
        if let Some(schema) = schema {
            Ok(self
                .catalog
                .schema(schema)?
                .list_tables()
                .into_iter()
                .map(|table| table.name().clone())
                .collect())
        } else {
            let mut tables = Vec::new();
            for schema in self.catalog.list_schemas() {
                tables.extend(
                    schema
                        .list_tables()
                        .into_iter()
                        .map(|table| table.name().clone()),
                );
            }
            Ok(tables)
        }
    }

    /// List all views in the transaction-local catalog, optionally restricted to a specific
    /// schema.
    pub fn list_views(&self, schema: Option<&str>) -> DucklakeResult<Vec<QualifiedName>> {
        if let Some(schema) = schema {
            Ok(self
                .catalog
                .schema(schema)?
                .list_views()
                .into_iter()
                .map(|view| view.name().clone())
                .collect())
        } else {
            let mut views = Vec::new();
            for schema in self.catalog.list_schemas() {
                views.extend(
                    schema
                        .list_views()
                        .into_iter()
                        .map(|view| view.name().clone()),
                );
            }
            Ok(views)
        }
    }

    /// List all macros in the transaction-local catalog, optionally restricted to a schema.
    pub fn list_macros(
        &self,
        schema: Option<&str>,
    ) -> DucklakeResult<Vec<(QualifiedName, crate::MacroType)>> {
        if let Some(schema) = schema {
            Ok(self
                .catalog
                .schema(schema)?
                .list_macros()
                .into_iter()
                .map(|macro_| (macro_.name().clone(), macro_.macro_type()))
                .collect())
        } else {
            let mut macros = Vec::new();
            for schema in self.catalog.list_schemas() {
                macros.extend(
                    schema
                        .list_macros()
                        .into_iter()
                        .map(|macro_| (macro_.name().clone(), macro_.macro_type())),
                );
            }
            Ok(macros)
        }
    }

    /// List the names of all schemas in the transaction-local catalog.
    pub fn list_schemas(&self) -> Vec<String> {
        self.catalog
            .list_schemas()
            .into_iter()
            .map(|schema| schema.name().to_string())
            .collect()
    }
}

#[cfg(feature = "python")]
impl<'a> Transaction<'a> {
    pub fn into_owned(self) -> Transaction<'static> {
        Transaction {
            snapshot_cache: self.snapshot_cache.into_owned(),
            pool: self.pool.into_owned(),
            storage_options: self.storage_options.into_owned(),
            metadata: self.metadata,
            changes: self.changes,
            author_info: self.author_info,
            snapshot: self.snapshot,
            catalog: self.catalog,
        }
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                             COMMIT                                            */
/* --------------------------------------------------------------------------------------------- */

impl<'a> Transaction<'a> {
    /// Commit the transaction, persisting all changes as a new snapshot in the catalog.
    pub async fn commit(self) -> DucklakeResult<()> {
        // First, we initialize a changeset type which allows us to de-duplicate changes and
        // provides high-level utilities.
        let change_set = ChangeSet::new(self.changes);

        // If the change set is empty, there's nothing to commit
        if change_set.is_empty() {
            return Ok(());
        }

        // To remedy conflicts caused by high-concurrency writes, we retry committing the
        // transaction. When retrying, we need to obtain the latest snapshot from the database.
        // For the first commit, it is integral that we use the transaction snapshot though as,
        // otherwise, the validation that happened against the catalog would be worthless.
        let max_retry_count = 10;
        let mut latest_snapshot = self.snapshot.clone();
        for i in 0..(max_retry_count + 1) {
            let table_stats = if change_set.requires_table_stats() {
                Some(
                    crate::caches::TableStats::load_for_tables(
                        &self.pool,
                        latest_snapshot.catalog().await?,
                        latest_snapshot.info().id,
                        &change_set.table_ids_with_data_writes(&self.catalog),
                    )
                    .await?,
                )
            } else {
                None
            };
            let mut state = CommitState::new(
                latest_snapshot.info(),
                Cow::Borrowed(&self.catalog),
                change_set.changes_schema(),
                table_stats,
            );

            // Apply the changes from the transaction
            let apply_result =
                Self::apply_changes(&self.pool, &mut state, &change_set, &self.author_info).await;

            // Handle any errors
            match apply_result {
                // If the commit succeeded, the transaction could be executed successfully. Last
                // thing we want to do is update our local cache with the snapshot that we created.
                // Otherwise, we will return erroneous information in metadata queries (e.g.
                // table name, table schema, ...).
                Ok(snapshot) => {
                    self.snapshot_cache.insert_snapshot(snapshot);
                    return Ok(());
                }
                // If the commit failed, we retry if the error indicates a primary key violation.
                // In this case, another snapshot has been committed in the meantime. We need to
                // perform conflict resolution and, if there's no conflict, try committing again.
                // The same can happen if we tried to create a new inline table that already
                // existed ("table already exists error").
                Err(ref err @ DucklakeError::Database(sqlx::Error::Database(ref db_err)))
                    if db_err.kind() == sqlx::error::ErrorKind::UniqueViolation
                        || self.pool.dialect().is_table_already_exists_error(err)
                            && i < max_retry_count =>
                {
                    // We already assign the latest snapshot for the next iteration as, otherwise,
                    // we'd possibly query a newer snapshot in the new iteration and miss changes.
                    latest_snapshot = self.snapshot_cache.get_latest().await?;

                    // Fetch all changes between the transaction snapshot and the new latest
                    // snapshot.);
                    let changes_made = Self::get_snapshot_changes(
                        &self.pool,
                        self.snapshot.info().id,
                        latest_snapshot.info().id,
                    )
                    .await?;

                    // In case the changes could be applied and we only failed because of a primary
                    // key violation, we can check whether we need to retry.
                    let tx_changes = change_set.applied_change_set(&mut state);
                    for snapshot_change in changes_made {
                        tx_changes.check_conflict(&snapshot_change, &self.catalog)?;
                    }

                    // If the check does not yield an error, we can retry
                    continue;
                }
                // If any other error occurred, there's no point in retrying. We simply return the
                // error.
                Err(err) => return Err(err),
            }
        }

        // If we get this far, we have exceeded the number of retries and return an error
        Err(DucklakeError::RetriesExceeded)
    }

    async fn apply_changes(
        pool: &db::Pool,
        state: &mut CommitState<'_>,
        change_set: &ChangeSet,
        author_info: &AuthorInfo,
    ) -> DucklakeResult<SnapshotInfo> {
        let mut changes = TransactionChanges::default();

        // First, we apply all the changes from the changeset
        change_set
            .apply(&mut changes, state, pool.dialect())
            .await?;
        let applied_changes = change_set.applied_change_set(state);

        // Then, we extract information from the applied changes to finalize the commit
        let table_ids_with_inline_data_writes: HashSet<_> = change_set
            .table_ids_with_inline_data_writes(state)
            .into_iter()
            .collect();
        let created_table_ids: HashSet<_> =
            change_set.created_table_ids(state).into_iter().collect();
        let table_ids_with_schema_changes = change_set.table_ids_with_schema_changes(state);

        if table_ids_with_inline_data_writes.iter().any(|id| {
            table_ids_with_schema_changes.contains(id) && !created_table_ids.contains(id)
        }) {
            unimplemented!(
                "Cannot currently write inline data to a table that has schema changes within the same transaction"
            );
        }

        // Only at this point, we start the database transaction and persist changes
        let mut tx = pool.begin().await?;
        changes.persist(&mut tx, state).await?;
        let snapshot_info = Self::finalize_commit(
            &mut tx,
            state,
            &applied_changes,
            &table_ids_with_schema_changes,
            author_info,
        )
        .await?;
        tx.commit().await?;

        Ok(snapshot_info)
    }

    async fn finalize_commit(
        tx: &mut db::Transaction<'_>,
        state: &CommitState<'_>,
        applied_changes: &AppliedChangeSet,
        table_ids_with_schema_changes: &[i64],
        author_info: &AuthorInfo,
    ) -> DucklakeResult<SnapshotInfo> {
        let snapshot_id = state.snapshot_id();
        let schema_version = state.schema_version();

        // Write a new snapshot
        let snapshot = DucklakeSnapshot::from(state);
        let snapshot_info = SnapshotInfo::from(snapshot.clone());
        tx.insert_entity(snapshot).await?;

        // Write the schema changes
        let snapshot_changes = DucklakeSnapshotChanges {
            snapshot_id,
            changes_made: applied_changes.to_string(),
            author: author_info.author.clone(),
            commit_message: author_info.message.clone(),
            commit_extra_info: author_info.extra_info.clone(),
        };
        tx.insert_entity(snapshot_changes).await?;

        // Update schema version if necessary
        let schema_versions = table_ids_with_schema_changes
            .iter()
            .map(|table_id| DucklakeSchemaVersions {
                begin_snapshot: snapshot_id,
                schema_version,
                table_id: Some(*table_id),
            })
            .collect::<Vec<_>>();
        tx.insert_entities(schema_versions).await?;
        Ok(snapshot_info)
    }

    async fn get_snapshot_changes(
        pool: &db::Pool,
        from_snapshot: i64,
        to_snapshot: i64,
    ) -> DucklakeResult<Vec<AppliedChangeSet>> {
        let query = Query::select()
            .column(Asterisk)
            .from(ducklake_snapshot_changes::Table)
            .and_where(
                ducklake_snapshot_changes::Column::SnapshotId
                    .col()
                    .between(from_snapshot + 1, to_snapshot),
            )
            .to_owned();
        let changes: Vec<DucklakeSnapshotChanges> = pool.fetch_all(&query).await?;
        changes
            .into_iter()
            .map(|c| c.changes_made.parse::<AppliedChangeSet>())
            .collect()
    }
}

// NOTE: Unlike an actual database transaction, `Drop` does not need to be implemented because the
//  transaction object does not actually start any database transaction until `commit` is called.
//  Hence, rollbacks are automatically handled within the `commit` method.

/* --------------------------------------------------------------------------------------------- */
/*                                             UTILS                                             */
/* --------------------------------------------------------------------------------------------- */

struct TransactionGuard<'a, 'tx> {
    catalog_snapshot: Arc<Catalog>,
    changes_snapshot: Vec<Change>,
    is_committed: bool,
    tx: &'tx mut Transaction<'a>,
}

impl TransactionGuard<'_, '_> {
    fn commit(mut self) {
        self.is_committed = true;
    }
}

impl Drop for TransactionGuard<'_, '_> {
    fn drop(&mut self) {
        // When the guard is dropped, it reverted, we revert the transaction changes, unless the
        // guard was committed.
        if !self.is_committed {
            self.tx.catalog = self.catalog_snapshot.clone();
            self.tx.changes = self.changes_snapshot.clone();
        }
    }
}

impl<'a> Transaction<'a> {
    fn guard(&mut self) -> TransactionGuard<'a, '_> {
        TransactionGuard {
            catalog_snapshot: self.catalog.clone(),
            changes_snapshot: self.changes.clone(),
            is_committed: false,
            tx: self,
        }
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod scoped_stats_tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use sea_query::{Expr, ExprTrait, Query};

    use crate::spec::ducklake_table_column_stats;
    use crate::{Column, CreateOptions, Ducklake, IfExistsStrategy};

    #[rstest::rstest]
    #[case(false, 1, true)]
    #[case(true, 1, true)]
    #[case(true, 2, true)]
    #[case(false, 1, false)]
    #[case(true, 1, false)]
    #[case(true, 2, false)]
    #[tokio::test]
    async fn test_write_does_not_load_unrelated_column_statistics(
        #[case] existing: bool,
        #[case] table_count: usize,
        #[case] inline: bool,
    ) {
        assert_scoped_write("sqlite://:memory:", existing, table_count, inline).await;
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    #[ignore = "requires an empty database at DUCKLAKE_TEST_POSTGRES_URI"]
    async fn test_postgres_write_does_not_load_unrelated_column_statistics() {
        let url = std::env::var("DUCKLAKE_TEST_POSTGRES_URI").unwrap();
        assert_scoped_write(&url, true, 2, false).await;
    }

    async fn write_data(
        mut table: super::TransactionTable<'_, '_>,
        batch: &RecordBatch,
        inline: bool,
    ) {
        if inline {
            table.write_inline_data(vec![batch.clone()]).unwrap();
        } else {
            table
                .write_data_files(vec![crate::WriteDataFile {
                    path: format!("{}.parquet", uuid::Uuid::new_v4()),
                    statistics: Some(crate::DataFileStatistics {
                        num_rows: 2,
                        file_size_bytes: Some(100),
                        footer_size_bytes: Some(20),
                        column_stats: [(
                            1,
                            crate::FileColumnStats {
                                size_bytes: Some(80),
                                min_value: Some(crate::Value::Int32(1)),
                                max_value: Some(crate::Value::Int32(2)),
                                null_count: Some(0),
                                contains_nan: Some(false),
                            },
                        )]
                        .into(),
                    }),
                    partition_values: None,
                    encryption_key: None,
                }])
                .await
                .unwrap();
        }
    }

    async fn assert_scoped_write(url: &str, existing: bool, table_count: usize, inline: bool) {
        // Arrange
        let lake = Ducklake::create(CreateOptions::new(url, "file:///tmp/scoped-stats/"))
            .await
            .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        let mut tx = lake.transaction().await.unwrap();
        tx.create_table(
            "unrelated",
            vec![Column::try_from(batch.schema().field(0)).unwrap()],
            None,
            None,
            None,
            None,
            IfExistsStrategy::Fail,
        )
        .unwrap()
        .write_inline_data(vec![batch.clone()])
        .unwrap();
        tx.commit().await.unwrap();
        // An invalid integer makes decoding unrelated statistics observable.
        let mut db_tx = lake.conn.pool().begin().await.unwrap();
        db_tx
            .execute(
                &Query::update()
                    .table(ducklake_table_column_stats::Table)
                    .value(
                        ducklake_table_column_stats::Column::MinValue,
                        Expr::value("unrelated-statistic"),
                    )
                    .to_owned(),
            )
            .await
            .unwrap();

        db_tx.commit().await.unwrap();

        let targets: Vec<String> = (0..table_count).map(|i| format!("target_{i}")).collect();
        if existing {
            let mut tx = lake.transaction().await.unwrap();
            for name in &targets {
                let table = tx
                    .create_table(
                        name.as_str(),
                        vec![Column::try_from(batch.schema().field(0)).unwrap()],
                        None,
                        None,
                        None,
                        None,
                        IfExistsStrategy::Fail,
                    )
                    .unwrap();
                write_data(table, &batch, inline).await;
            }
            tx.commit().await.unwrap();
        }
        // Start before another commit to exercise snapshot conflict retries as well.
        let mut tx = lake.transaction().await.unwrap();
        let mut concurrent = lake.transaction().await.unwrap();
        concurrent
            .create_schema("other", None, IfExistsStrategy::Fail)
            .unwrap();
        concurrent.commit().await.unwrap();

        // Act
        for name in &targets {
            if existing {
                write_data(tx.table(name.as_str()).unwrap(), &batch, inline).await;
            } else {
                let table = tx
                    .create_table(
                        name.as_str(),
                        vec![Column::try_from(batch.schema().field(0)).unwrap()],
                        None,
                        None,
                        None,
                        None,
                        IfExistsStrategy::Fail,
                    )
                    .unwrap();
                write_data(table, &batch, inline).await;
            }
        }
        let result = tx.commit().await;

        // Assert
        assert!(
            result.is_ok(),
            "write loaded unrelated statistics: {result:?}"
        );
        let query = Query::select()
            .column(crate::spec::ducklake_table::Column::TableId)
            .from(crate::spec::ducklake_table::Table)
            .and_where(Expr::col(crate::spec::ducklake_table::Column::TableName).is_in(targets))
            .to_owned();
        let ids: Vec<(i64,)> = lake.conn.pool().fetch_all(&query).await.unwrap();
        let snapshot = lake.conn.current_snapshot();
        let stats = crate::caches::TableStats::load_for_tables(
            lake.conn.pool(),
            snapshot.catalog().await.unwrap(),
            snapshot.info().id,
            &ids.iter().map(|(id,)| *id).collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        assert_eq!(stats.len(), table_count);
        for stats in stats.values() {
            assert_eq!(stats.record_count(), Some(if existing { 4 } else { 2 }));
            assert_eq!(stats.next_row_id(), if existing { 4 } else { 2 });
            let column = stats.column_stats(1).unwrap();
            assert_eq!(column.min_value(), Some(&crate::Value::Int32(1)));
            assert_eq!(column.max_value(), Some(&crate::Value::Int32(2)));
        }
    }
}
