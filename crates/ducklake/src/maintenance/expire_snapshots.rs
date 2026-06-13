use sea_query::{
    Alias,
    Asterisk,
    Condition,
    Expr,
    ExprTrait,
    IntoColumnRef,
    IntoIden,
    Query,
    Table,
    all,
};

use super::DryRun;
use crate::catalog::Catalog;
use crate::spec::*;
use crate::{Ducklake, DucklakeResult, SnapshotMetadata, db, io};

/* ----------------------------------------- PUBLIC API ---------------------------------------- */

impl Ducklake {
    /// Expire snapshots based on the global `expire_older_than` configuration.
    ///
    /// If the configuration option is not set, this function will silently do nothing. Otherwise
    /// it will remove all snapshots that are older than the configured duration. It also performs
    /// a cascading delete for all metadata that is only reachable from expired snapshots and marks
    /// data & delete files that are only reachable from expired snapshots for deletion.
    ///
    /// The `dry_run` flag allows to understand which snapshots would be expired upon execution.
    ///
    /// Note that the latest snapshot is always retained.
    pub async fn expire_snapshots(
        &self,
        dry_run: DryRun,
    ) -> DucklakeResult<Vec<SnapshotMetadata>> {
        let Some(interval) = self.conn.metadata().expire_older_than() else {
            // If there's no configuration, we silently do nothing
            return Ok(vec![]);
        };

        let timestamp = chrono::Utc::now() - interval.months - interval.delta;
        self.expire_snapshots_older_than(timestamp, dry_run).await
    }

    /// Expire snapshots by their versions.
    ///
    /// The functionality matches [`expire_snapshots`] for a predefined set of versions.
    ///
    /// Versions that do not exist or match the latest snapshot are silently ignored.
    pub async fn expire_snapshots_versions(
        &self,
        versions: &[i64],
        dry_run: DryRun,
    ) -> DucklakeResult<Vec<SnapshotMetadata>> {
        let condition = ducklake_snapshot::Column::SnapshotId
            .col()
            .is_in(versions.to_vec());
        self.expire_snapshots_with_condition(condition.into(), dry_run)
            .await
    }

    /// Expire snapshots older than a specific timestamp.
    ///
    /// The functionality matches [`expire_snapshots`] for a predefined timestamp.
    pub async fn expire_snapshots_older_than(
        &self,
        timestamp: chrono::DateTime<chrono::Utc>,
        dry_run: DryRun,
    ) -> DucklakeResult<Vec<SnapshotMetadata>> {
        let condition = ducklake_snapshot::Column::SnapshotTime.col().lt(timestamp);
        self.expire_snapshots_with_condition(condition.into(), dry_run)
            .await
    }

    async fn expire_snapshots_with_condition(
        &self,
        condition: Condition,
        dry_run: DryRun,
    ) -> DucklakeResult<Vec<SnapshotMetadata>> {
        // NOTE: We must fetch the catalog before we start the transaction as we could otherwise
        //  deadlock if there's only one connection in the connection pool.
        let catalog = if matches!(dry_run, DryRun::No) {
            let latest_snapshot = self.conn.latest_snapshot(false).await?;
            Some(latest_snapshot.catalog().await?.clone())
        } else {
            None
        };

        let mut tx = self.conn.pool().begin().await?;
        let snapshots = find_expired_snapshots(&mut tx, condition).await?;

        if let Some(catalog) = catalog {
            // If we're not in dry-run mode, we actually expire all snapshots
            let data_path = self.conn.metadata().data_path();
            let snapshot_ids: Vec<_> = snapshots.iter().map(|s| s.snapshot_id).collect();
            expire_snapshots(&mut tx, &catalog, &data_path, &snapshot_ids).await?;
            tx.commit().await?;

            // Once we've done that, we clean up our local caches to prevent accessing
            // expired snapshots
            self.conn.snapshot_cache().remove_snapshots(&snapshot_ids);
        }
        Ok(snapshots.into_iter().map(SnapshotMetadata::from).collect())
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                        "ORCHESTRATION"                                        */
/* --------------------------------------------------------------------------------------------- */

async fn expire_snapshots(
    tx: &mut db::Transaction,
    catalog: &Catalog,
    data_path: &io::DucklakePath,
    snapshot_ids: &[i64],
) -> DucklakeResult<()> {
    // First, we delete all of the snapshots from `ducklake_snapshot`. Subsequent queries can then
    // conceptually anti-join against that table.
    delete_snapshots(tx, snapshot_ids).await?;

    // Afterwards, we check which tables can be deleted based on the expired snapshots. We
    // restrict ourselves to deleting tables that cannot be reached in *any* version anymore, i.e.
    // if the remaining snapshots reference at least one version of the table, we keep all of them
    // around for simplicity.
    let table_ids = find_expired_tables(tx).await?;

    // At this point, we can clean up all other tables:
    // - Tables with a `table_id` column: rows should be deleted for expired tables
    delete_tables(tx, &table_ids).await?;

    // - Data files that are only reachable from expired snapshots. The files are moved to the
    //   `ducklake_files_scheduled_for_deletion` table to be garbage-collected at a later point.
    //   However, all metadata (including file column stats) are removed already. Note that this
    //   is different to the "cheap" metadata tables which are only cleaned up when an entire table
    //   is unreachable from the remaining snapshots.
    mark_data_files_for_deletion(tx, catalog, data_path).await?;

    // - Remaining catalog objects with `begin_snapshot`/`end_snapshot` or those that reference
    //   tables with expired catalog objects. Generally, it is easier to delete from these tables
    //   as we don't need to keep versions around (as for tables) or simply anti-join against
    //   other tables.
    delete_expired_catalog_objects(tx).await?;

    Ok(())
}

/* --------------------------------------------------------------------------------------------- */
/*                                           DELETIONS                                           */
/* --------------------------------------------------------------------------------------------- */

/* ----------------------------------------- SNAPSHOTS ----------------------------------------- */

async fn find_expired_snapshots(
    tx: &mut db::Transaction,
    condition: Condition,
) -> DucklakeResult<Vec<DucklakeSnapshot>> {
    // Build query - make sure to retin the most recent snapshot
    let latest_snapshot = Query::select()
        .expr(ducklake_snapshot::Column::SnapshotId.col().max())
        .from(ducklake_snapshot::Table)
        .to_owned();
    let condition = condition.and(
        ducklake_snapshot::Column::SnapshotId
            .col()
            .ne(latest_snapshot),
    );
    let query = Query::select()
        .column(Asterisk)
        .from(ducklake_snapshot::Table)
        .cond_where(condition)
        .take();

    // Execute
    tx.fetch_all(&query).await
}

async fn delete_snapshots(tx: &mut db::Transaction, snapshot_ids: &[i64]) -> DucklakeResult<()> {
    // Build query
    let query = Query::delete()
        .from_table(ducklake_snapshot::Table)
        .cond_where(
            ducklake_snapshot::Column::SnapshotId
                .col()
                .is_in(snapshot_ids),
        )
        .take();

    // Execute
    tx.execute(&query).await
}

/* ------------------------------------------- TABLES ------------------------------------------ */

async fn find_expired_tables(tx: &mut db::Transaction) -> DucklakeResult<Vec<i64>> {
    // Build the query
    let alias = Alias::new("ducklake_table_2");
    let base_condition = condition_inactive_snapshot(
        ducklake_table::Table,
        ducklake_table::Column::BeginSnapshot,
        ducklake_table::Column::EndSnapshot,
    );
    let self_reference_condition = Expr::not_exists(
        Query::select()
            .expr(Expr::val(1))
            .from_as(ducklake_table::Table, alias.clone())
            .cond_where(all![
                Expr::col((alias.clone(), ducklake_table::Column::TableId)).eq(Expr::col((
                    ducklake_table::Table,
                    ducklake_table::Column::TableId
                ))),
                condition_inactive_snapshot(
                    alias,
                    ducklake_table::Column::BeginSnapshot,
                    ducklake_table::Column::EndSnapshot
                )
                .not()
            ])
            .take(),
    );
    let full_condition = all![base_condition, self_reference_condition];
    let query = Query::select()
        .distinct()
        .column(ducklake_table::Column::TableId)
        .from(ducklake_table::Table)
        .cond_where(full_condition)
        .take();

    // Execute
    let table_ids: Vec<(i64,)> = tx.fetch_all(&query).await?;
    Ok(table_ids.into_iter().map(|(id,)| id).collect())
}

async fn delete_tables(tx: &mut db::Transaction, table_ids: &[i64]) -> DucklakeResult<()> {
    macro_rules! delete_from_table(
        ($table:ident) => {{
            let query = Query::delete()
                .from_table($table::Table)
                .cond_where($table::Column::TableId.col().is_in(table_ids))
                .take();
            tx.execute(&query).await?;
        }};
    );

    // Before deleting from all tables, we first fetch all the inlined data tables that we also
    // need to drop... and drop them
    let inlined_data_table_query = Query::select()
        .column(ducklake_inlined_data_tables::Column::TableName)
        .from(ducklake_inlined_data_tables::Table)
        .and_where(
            ducklake_inlined_data_tables::Column::TableId
                .col()
                .is_in(table_ids),
        )
        .take();
    let inlined_data_tables: Vec<(String,)> = tx.fetch_all(&inlined_data_table_query).await?;
    for (table_name,) in inlined_data_tables {
        let drop_query = Table::drop().table(table_name).take();
        tx.execute(&drop_query).await?;
    }

    // Then, we can delete from all tables referencing a table ID. We do NOT handle data & delete
    // files here; they are treated separately to clean up files properly.
    delete_from_table!(ducklake_column_mapping);
    delete_from_table!(ducklake_column_tag);
    delete_from_table!(ducklake_column);
    delete_from_table!(ducklake_inlined_data_tables);
    delete_from_table!(ducklake_partition_column);
    delete_from_table!(ducklake_partition_info);
    delete_from_table!(ducklake_schema_versions);
    delete_from_table!(ducklake_sort_expression);
    delete_from_table!(ducklake_sort_info);
    delete_from_table!(ducklake_table_column_stats);
    delete_from_table!(ducklake_table_stats);
    delete_from_table!(ducklake_table);

    Ok(())
}

/* ----------------------------------------- DATA FILES ---------------------------------------- */

async fn mark_data_files_for_deletion(
    tx: &mut db::Transaction,
    catalog: &Catalog,
    data_path: &io::DucklakePath,
) -> DucklakeResult<()> {
    // Find all data files and delete files to be marked for deletion
    let data_files_query = Query::select()
        .columns([
            ducklake_data_file::Column::DataFileId,
            ducklake_data_file::Column::TableId,
            ducklake_data_file::Column::Path,
            ducklake_data_file::Column::PathIsRelative,
        ])
        .from(ducklake_data_file::Table)
        .cond_where(condition_inactive_snapshot(
            ducklake_data_file::Table,
            ducklake_data_file::Column::BeginSnapshot,
            ducklake_data_file::Column::EndSnapshot,
        ))
        .take();
    let expired_data_files: Vec<(i64, i64, String, bool)> =
        tx.fetch_all(&data_files_query).await?;
    let expired_data_file_ids: Vec<i64> = expired_data_files.iter().map(|item| item.0).collect();

    let delete_files_query = Query::select()
        .columns([
            ducklake_delete_file::Column::DeleteFileId,
            ducklake_delete_file::Column::TableId,
            ducklake_delete_file::Column::Path,
            ducklake_delete_file::Column::PathIsRelative,
        ])
        .from(ducklake_delete_file::Table)
        .cond_where(condition_inactive_snapshot(
            ducklake_delete_file::Table,
            ducklake_delete_file::Column::BeginSnapshot,
            ducklake_delete_file::Column::EndSnapshot,
        ))
        .take();
    let expired_delete_files: Vec<(i64, i64, String, bool)> =
        tx.fetch_all(&delete_files_query).await?;
    let expired_delete_file_ids: Vec<i64> =
        expired_delete_files.iter().map(|item| item.0).collect();

    // Build the files scheduled for deletion
    let now = db::UtcDateTime::now();
    let files_scheduled_for_deletion = expired_data_files
        .into_iter()
        .chain(expired_delete_files)
        .map(|(file_id, table_id, path, path_is_relative)| {
            let full_path = build_deletion_file_path(
                io::DucklakePath::new(&path, path_is_relative),
                table_id,
                catalog,
                data_path,
            );
            DucklakeFilesScheduledForDeletion {
                data_file_id: file_id,
                path: full_path.to_string(),
                path_is_relative: full_path.is_relative(),
                schedule_start: now,
            }
        })
        .collect::<Vec<_>>();

    // Insert into the `ducklake_files_scheduled_for_deletion` table
    tx.insert_entities(files_scheduled_for_deletion).await?;

    // Delete from the source tables
    let delete_data_file_query = Query::delete()
        .from_table(ducklake_data_file::Table)
        .and_where(
            ducklake_data_file::Column::DataFileId
                .col()
                .is_in(expired_data_file_ids),
        )
        .take();
    tx.execute(&delete_data_file_query).await?;

    let delete_delete_file_query = Query::delete()
        .from_table(ducklake_delete_file::Table)
        .and_where(
            ducklake_delete_file::Column::DeleteFileId
                .col()
                .is_in(expired_delete_file_ids),
        )
        .take();
    tx.execute(&delete_delete_file_query).await?;

    Ok(())
}

fn build_deletion_file_path(
    path: io::DucklakePath,
    table_id: i64,
    catalog: &Catalog,
    base_data_path: &io::DucklakePath,
) -> io::DucklakePath {
    if path.is_absolute() {
        return path;
    }

    // If the path is relative, it is relative to the table's data path. We need to make sure
    // it's relative to the catalog's data path.
    let table_data_path = catalog.table(table_id).unwrap().data_path(base_data_path);
    let full_path = table_data_path.join(&path);
    if let Some(suffix) = full_path.as_str().strip_prefix(base_data_path.as_str()) {
        io::DucklakePath::Relative(suffix.to_string())
    } else {
        full_path
    }
}

/* -------------------------------------- METADATA TABLES -------------------------------------- */

async fn delete_expired_catalog_objects(tx: &mut db::Transaction) -> DucklakeResult<()> {
    macro_rules! delete_from_catalog_table(
        ($table:ident) => {{
            let query = Query::delete()
                .from_table($table::Table)
                .cond_where(
                    condition_inactive_snapshot(
                        $table::Table,
                        $table::Column::BeginSnapshot,
                        $table::Column::EndSnapshot,
                    )
                )
                .take();
            tx.execute(&query).await?;
        }};
    );

    macro_rules! delete_from_dependent_table(
        ($table:ident, $ref_table:ident, $column:ident) => {{
            let query = Query::delete()
                .from_table($table::Table)
                .cond_where(condition_not_exists_in_reference(
                    ($table::Table, $table::Column::$column),
                    ($ref_table::Table, $ref_table::Column::$column),
                ))
                .take();
            tx.execute(&query).await?;
        }};
    );

    // First, we delete from catalog tables
    delete_from_catalog_table!(ducklake_schema);
    delete_from_catalog_table!(ducklake_tag);
    delete_from_catalog_table!(ducklake_macro);
    delete_from_catalog_table!(ducklake_view);

    // Then, we delete from dependent tables
    delete_from_dependent_table!(ducklake_snapshot_changes, ducklake_snapshot, SnapshotId);
    delete_from_dependent_table!(ducklake_name_mapping, ducklake_column_mapping, MappingId);
    delete_from_dependent_table!(ducklake_macro_impl, ducklake_macro, MacroId);
    delete_from_dependent_table!(ducklake_macro_parameters, ducklake_macro, MacroId);
    delete_from_dependent_table!(ducklake_file_column_stats, ducklake_data_file, DataFileId);
    delete_from_dependent_table!(ducklake_file_variant_stats, ducklake_data_file, DataFileId);
    delete_from_dependent_table!(
        ducklake_file_partition_value,
        ducklake_data_file,
        DataFileId
    );

    Ok(())
}

/* --------------------------------------------------------------------------------------------- */
/*                                             UTILS                                             */
/* --------------------------------------------------------------------------------------------- */

impl From<DucklakeSnapshot> for SnapshotMetadata {
    fn from(snapshot: DucklakeSnapshot) -> Self {
        SnapshotMetadata {
            id: snapshot.snapshot_id,
            timestamp: snapshot.snapshot_time.0,
        }
    }
}

fn condition_inactive_snapshot(
    table: impl IntoIden,
    begin_snapshot: impl IntoIden,
    end_snapshot: impl IntoIden,
) -> Condition {
    let table = table.into_iden();
    let begin_snapshot = begin_snapshot.into_iden();
    let end_snapshot = end_snapshot.into_iden();
    all![
        Expr::col((table.clone(), end_snapshot.clone())).is_not_null(),
        Expr::not_exists(
            Query::select()
                .column(ducklake_snapshot::Column::SnapshotId)
                .from(ducklake_snapshot::Table)
                .and_where(
                    ducklake_snapshot::Column::SnapshotId
                        .col()
                        .gte(Expr::col((table.clone(), begin_snapshot)))
                )
                .and_where(
                    ducklake_snapshot::Column::SnapshotId
                        .col()
                        .lt(Expr::col((table, end_snapshot)))
                )
                .take()
        )
    ]
}

fn condition_not_exists_in_reference(
    column: impl IntoColumnRef,
    reference: (impl IntoIden, impl IntoIden),
) -> Expr {
    let reference_table = reference.0.into_iden();
    Expr::not_exists(
        Query::select()
            .expr(Expr::val(1))
            .from(reference_table.clone())
            .and_where(Expr::col((reference_table, reference.1)).eq(Expr::col(column)))
            .take(),
    )
}
