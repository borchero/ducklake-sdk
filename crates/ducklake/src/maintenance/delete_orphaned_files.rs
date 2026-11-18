use std::collections::{HashMap, HashSet};

use futures::{StreamExt, stream};
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;
use sea_query::{Expr, ExprTrait, IntoIden, JoinType, Order, Query, UnionType};

use super::DryRun;
use crate::ducklake::SnapshotAccess;
use crate::spec::*;
use crate::{Ducklake, DucklakeResult, io};

/* ----------------------------------------- PUBLIC API ---------------------------------------- */

impl Ducklake {
    /// Delete files in the data directory that are not referenced by any snapshot.
    ///
    /// Orphaned files are files that physically exist below the catalog's data path but are not
    /// referenced by any data file, delete file, or file scheduled for deletion. Such files can
    /// occur, for example, when a writer crashes after writing a file but before committing the
    /// corresponding catalog entry.
    ///
    /// The `dry_run` flag allows understanding which files would be deleted upon execution without
    /// actually deleting them.
    ///
    /// Returns the fully-qualified paths of the files that were deleted (or that would be deleted
    /// when `dry_run` is [`DryRun::Yes`]).
    pub async fn delete_orphaned_files(&self, dry_run: DryRun) -> DucklakeResult<Vec<String>> {
        let interval = self.conn.metadata().delete_older_than();
        let timestamp = chrono::Utc::now() - interval.months - interval.delta;
        self.delete_orphaned_files_filtered(Some(timestamp), dry_run)
            .await
    }

    /// Delete orphaned files last modified before a specific timestamp.
    ///
    /// The functionality matches [`Ducklake::delete_orphaned_files`] for a predefined timestamp.
    pub async fn delete_orphaned_files_older_than(
        &self,
        timestamp: chrono::DateTime<chrono::Utc>,
        dry_run: DryRun,
    ) -> DucklakeResult<Vec<String>> {
        self.delete_orphaned_files_filtered(Some(timestamp), dry_run)
            .await
    }

    /// Delete all orphaned files regardless of their last modification time.
    ///
    /// The functionality matches [`Ducklake::delete_orphaned_files`] but ignores the
    /// `delete_older_than` configuration.
    pub async fn delete_all_orphaned_files(&self, dry_run: DryRun) -> DucklakeResult<Vec<String>> {
        self.delete_orphaned_files_filtered(None, dry_run).await
    }

    async fn delete_orphaned_files_filtered(
        &self,
        max_age: Option<chrono::DateTime<chrono::Utc>>,
        dry_run: DryRun,
    ) -> DucklakeResult<Vec<String>> {
        let data_path = self.conn.metadata().data_path();

        // 1) Collect all object store locations referenced by the catalog BEFORE listing.
        let mut referenced = self.collect_referenced_locations(&data_path).await?;

        // 2) List all files below the data path and find candidate orphans.
        let resolved_data_path = data_path.resolve()?;
        let store = resolved_data_path.object_store(Some(self.conn.storage_options().to_vec()));
        let prefix = resolved_data_path.path();

        let mut candidates = Vec::new();
        let mut listing = store.list(Some(&prefix));
        while let Some(meta) = listing.next().await {
            let meta = meta?;
            if referenced.contains(&meta.location) {
                continue;
            }
            if let Some(max_age) = max_age
                && meta.last_modified >= max_age
            {
                continue;
            }
            candidates.push(meta.location);
        }

        // Re-check referenced locations after listing so concurrent in-flight writers that
        // uploaded objects before committing their snapshot transaction are never deleted.
        let orphans = if candidates.is_empty() {
            candidates
        } else {
            referenced = self.collect_referenced_locations(&data_path).await?;
            candidates
                .into_iter()
                .filter(|loc| !referenced.contains(loc))
                .collect()
        };

        // 3) Delete the orphaned files unless this is a dry run.
        if matches!(dry_run, DryRun::No) {
            self.conn.check_writable()?;
            let locations = stream::iter(orphans.clone().into_iter().map(Ok));
            io::delete_objects(store.as_ref(), locations).await?;
        }

        Ok(orphans
            .iter()
            .map(|location| resolved_data_path.display_location(location))
            .collect())
    }

    /// Collect the object store locations of all files referenced by the catalog, i.e. all data
    /// files, delete files, and files already scheduled for deletion.
    async fn collect_referenced_locations(
        &self,
        data_path: &io::DucklakePath,
    ) -> DucklakeResult<HashSet<ObjectStorePath>> {
        let pool = self.conn.pool();
        let snapshot = self.conn.snapshot(SnapshotAccess::Head).await?;
        let catalog = snapshot.catalog().await?;

        // Fetch the file paths from all relevant catalog tables.
        let data_file_query = Query::select()
            .columns([
                ducklake_data_file::Column::TableId,
                ducklake_data_file::Column::Path,
                ducklake_data_file::Column::PathIsRelative,
            ])
            .from(ducklake_data_file::Table)
            .union(
                UnionType::All,
                Query::select()
                    .columns([
                        ducklake_delete_file::Column::TableId,
                        ducklake_delete_file::Column::Path,
                        ducklake_delete_file::Column::PathIsRelative,
                    ])
                    .from(ducklake_delete_file::Table)
                    .take(),
            )
            .take();
        let table_files: Vec<(i64, String, bool)> = pool.fetch_all(&data_file_query).await?;

        let scheduled_query = Query::select()
            .columns([
                ducklake_files_scheduled_for_deletion::Column::Path,
                ducklake_files_scheduled_for_deletion::Column::PathIsRelative,
            ])
            .from(ducklake_files_scheduled_for_deletion::Table)
            .take();
        let scheduled_files: Vec<(String, bool)> = pool.fetch_all(&scheduled_query).await?;

        // Resolve all referenced file paths to object store locations.
        let mut table_paths: HashMap<i64, io::DucklakePath> = HashMap::new();
        let mut result = HashSet::new();
        for (table_id, path, path_is_relative) in table_files {
            let table_data_path = match table_paths.get(&table_id) {
                Some(path) => path.clone(),
                None => {
                    let resolved = match catalog.table(table_id) {
                        Ok(table) => table.data_path(data_path),
                        Err(_) => {
                            let query = Query::select()
                                .columns([
                                    (
                                        ducklake_schema::Table.into_iden(),
                                        ducklake_schema::Column::Path.into_iden(),
                                    ),
                                    (
                                        ducklake_schema::Table.into_iden(),
                                        ducklake_schema::Column::PathIsRelative.into_iden(),
                                    ),
                                    (
                                        ducklake_table::Table.into_iden(),
                                        ducklake_table::Column::Path.into_iden(),
                                    ),
                                    (
                                        ducklake_table::Table.into_iden(),
                                        ducklake_table::Column::PathIsRelative.into_iden(),
                                    ),
                                ])
                                .from(ducklake_table::Table)
                                .join(
                                    JoinType::InnerJoin,
                                    ducklake_schema::Table,
                                    Expr::col((
                                        ducklake_schema::Table,
                                        ducklake_schema::Column::SchemaId,
                                    ))
                                    .eq(Expr::col((
                                        ducklake_table::Table,
                                        ducklake_table::Column::SchemaId,
                                    ))),
                                )
                                .and_where(ducklake_table::Column::TableId.col().eq(table_id))
                                .order_by(
                                    (ducklake_table::Table, ducklake_table::Column::BeginSnapshot),
                                    Order::Desc,
                                )
                                .limit(1)
                                .take();
                            let row: (String, bool, String, bool) = pool.fetch_one(&query).await?;
                            let schema_path = io::DucklakePath::new(&row.0, row.1);
                            let table_path = io::DucklakePath::new(&row.2, row.3);
                            data_path.join(&schema_path).join(&table_path)
                        }
                    };
                    table_paths.insert(table_id, resolved.clone());
                    resolved
                }
            };
            let file_path = io::DucklakePath::new(&path, path_is_relative);
            let full_path = table_data_path.join(&file_path);
            result.insert(full_path.resolve()?.path());
        }

        // Files scheduled for deletion store paths relative to the catalog's data path.
        for (path, path_is_relative) in scheduled_files {
            let file_path = io::DucklakePath::new(&path, path_is_relative);
            let path = data_path.join(&file_path);
            result.insert(path.resolve()?.path());
        }

        Ok(result)
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                             TESTS                                             */
/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::{
        Column,
        CreateOptions,
        DataFileStatistics,
        DataType,
        IfExistsStrategy,
        WriteDataFile,
    };

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn test_delete_orphaned_files_handles_dropped_table_and_preserves_referenced() {
        // Arrange
        let temp_dir =
            std::env::temp_dir().join(format!("ducklake-orphan-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        #[cfg(unix)]
        let temp_dir = std::fs::canonicalize(&temp_dir).unwrap();
        let data_url = url::Url::from_directory_path(&temp_dir)
            .unwrap()
            .to_string();
        let lake = Ducklake::create(CreateOptions::new("sqlite://:memory:", &data_url))
            .await
            .unwrap();

        let mut tx = lake.transaction().await.unwrap();
        let mut table = tx
            .create_table(
                "events",
                vec![Column::new("id".to_string(), DataType::Int64)],
                None,
                None,
                None,
                None,
                IfExistsStrategy::Fail,
            )
            .unwrap();
        let (_, generator, _, _) = table.get_write_info().unwrap();
        let file_name = generator.generate_relative(&Default::default());
        let file_dir = url::Url::parse(generator.base_path())
            .unwrap()
            .to_file_path()
            .unwrap();
        std::fs::create_dir_all(&file_dir).unwrap();
        let committed_file = file_dir.join(&file_name);
        std::fs::write(&committed_file, b"PAR1").unwrap();
        table
            .write_data_files(vec![WriteDataFile {
                path: file_name,
                encryption_key: None,
                statistics: Some(DataFileStatistics {
                    num_rows: 1,
                    file_size_bytes: Some(4),
                    footer_size_bytes: None,
                    column_stats: HashMap::new(),
                }),
                partition_values: None,
            }])
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let mut drop_tx = lake.transaction().await.unwrap();
        drop_tx
            .table("events")
            .unwrap()
            .rename("events_old")
            .unwrap();
        drop_tx
            .delete_table("events_old", IfExistsStrategy::Fail)
            .unwrap();
        drop_tx.commit().await.unwrap();

        let orphan_file = temp_dir.join("orphan.parquet");
        std::fs::write(&orphan_file, b"ORPHAN").unwrap();

        // Act
        let deleted = lake.delete_all_orphaned_files(DryRun::No).await.unwrap();

        // Assert
        assert_eq!(deleted, vec![orphan_file.to_string_lossy().to_string()]);
        assert!(!orphan_file.exists());
        assert!(committed_file.exists());

        std::fs::remove_dir_all(&temp_dir).unwrap();
    }
}
