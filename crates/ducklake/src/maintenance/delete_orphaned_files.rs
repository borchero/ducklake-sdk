use std::collections::{HashMap, HashSet};

use futures::{StreamExt, stream};
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;
use sea_query::{Expr, ExprTrait, IntoIden, JoinType, Query};

use super::DryRun;
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
    /// If `cleanup_all` is `true`, all orphaned files are deleted regardless of their age.
    /// Otherwise, only orphaned files whose last modification time is strictly before `older_than`
    /// are deleted. If `older_than` is `None`, the current time is used, meaning all orphaned
    /// files that exist at the time of the call are eligible for deletion.
    ///
    /// The `dry_run` flag allows understanding which files would be deleted upon execution without
    /// actually deleting them.
    ///
    /// Returns the fully-qualified paths of the files that were deleted (or that would be deleted
    /// when `dry_run` is [`DryRun::Yes`]).
    pub async fn delete_orphaned_files(
        &self,
        cleanup_all: bool,
        older_than: Option<chrono::DateTime<chrono::Utc>>,
        dry_run: DryRun,
    ) -> DucklakeResult<Vec<String>> {
        let data_path = self.conn.metadata().data_path();

        // 1) Collect all object store locations referenced by the catalog.
        let referenced = self.collect_referenced_locations(&data_path).await?;

        // 2) List all files below the data path and find the orphaned ones.
        let resolved = data_path.resolve()?;
        let store = resolved.object_store(Some(self.conn.storage_options().to_vec()));
        let prefix = resolved.path();
        let threshold = if cleanup_all {
            None
        } else {
            Some(older_than.unwrap_or_else(chrono::Utc::now))
        };

        let mut orphans = Vec::new();
        let mut listing = store.list(Some(&prefix));
        while let Some(meta) = listing.next().await {
            let meta = meta?;
            if referenced.contains(&meta.location) {
                continue;
            }
            if let Some(threshold) = threshold
                && meta.last_modified >= threshold
            {
                continue;
            }
            orphans.push(meta.location);
        }

        // 3) Delete the orphaned files unless this is a dry run. We batch the deletes via
        // `delete_stream` so the object store can use batch APIs where available, and tolerate
        // files that are already gone to keep the operation idempotent.
        if matches!(dry_run, DryRun::No) {
            let locations = orphans.clone();
            let mut deletion =
                store.delete_stream(stream::iter(locations.into_iter().map(Ok)).boxed());
            while let Some(result) = deletion.next().await {
                match result {
                    Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }

        Ok(orphans
            .iter()
            .map(|location| resolved.display_location(location))
            .collect())
    }

    /// Collect the object store locations of all files referenced by the catalog, i.e. all data
    /// files, delete files, and files already scheduled for deletion.
    async fn collect_referenced_locations(
        &self,
        data_path: &io::DucklakePath,
    ) -> DucklakeResult<HashSet<ObjectStorePath>> {
        let pool = self.conn.pool();

        // Build a lookup from table ID to the (possibly multiple) data paths under which the
        // table's files may live. Data and delete file paths are stored relative to the table's
        // data path, so we need this to resolve them. We collect *all* candidate paths for a
        // table ID (in case a table was relocated across versions) to err on the side of treating
        // files as referenced rather than orphaned.
        let table_path_query = Query::select()
            .distinct()
            .columns([
                (
                    ducklake_table::Table.into_iden(),
                    ducklake_table::Column::TableId.into_iden(),
                ),
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
                Expr::col((ducklake_schema::Table, ducklake_schema::Column::SchemaId)).eq(
                    Expr::col((ducklake_table::Table, ducklake_table::Column::SchemaId)),
                ),
            )
            .take();
        let tables: Vec<(i64, String, bool, String, bool)> =
            pool.fetch_all(&table_path_query).await?;
        let mut paths_by_table_id: HashMap<i64, Vec<io::DucklakePath>> = HashMap::new();
        for (table_id, schema_path, schema_is_relative, table_path, table_is_relative) in tables {
            let schema_path = io::DucklakePath::new(&schema_path, schema_is_relative);
            let table_path = io::DucklakePath::new(&table_path, table_is_relative);
            let full = data_path.join(&schema_path).join(&table_path);
            paths_by_table_id.entry(table_id).or_default().push(full);
        }

        // Fetch the file paths from all relevant catalog tables.
        let data_file_query = Query::select()
            .columns([
                ducklake_data_file::Column::TableId,
                ducklake_data_file::Column::Path,
                ducklake_data_file::Column::PathIsRelative,
            ])
            .from(ducklake_data_file::Table)
            .take();
        let data_files: Vec<(i64, String, bool)> = pool.fetch_all(&data_file_query).await?;

        let delete_file_query = Query::select()
            .columns([
                ducklake_delete_file::Column::TableId,
                ducklake_delete_file::Column::Path,
                ducklake_delete_file::Column::PathIsRelative,
            ])
            .from(ducklake_delete_file::Table)
            .take();
        let delete_files: Vec<(i64, String, bool)> = pool.fetch_all(&delete_file_query).await?;

        let scheduled_query = Query::select()
            .columns([
                ducklake_files_scheduled_for_deletion::Column::Path,
                ducklake_files_scheduled_for_deletion::Column::PathIsRelative,
            ])
            .from(ducklake_files_scheduled_for_deletion::Table)
            .take();
        let scheduled_files: Vec<(String, bool)> = pool.fetch_all(&scheduled_query).await?;

        // Resolve all referenced file paths to object store locations.
        let mut result = HashSet::new();
        for (table_id, path, path_is_relative) in data_files.into_iter().chain(delete_files) {
            let file_path = io::DucklakePath::new(&path, path_is_relative);
            if file_path.is_absolute() {
                result.insert(file_path.resolve()?.path());
            } else if let Some(base_paths) = paths_by_table_id.get(&table_id) {
                for base_path in base_paths {
                    result.insert(base_path.join(&file_path).resolve()?.path());
                }
            } else {
                result.insert(data_path.join(&file_path).resolve()?.path());
            }
        }
        // Files scheduled for deletion store paths relative to the catalog's data path.
        for (path, path_is_relative) in scheduled_files {
            let file_path = io::DucklakePath::new(&path, path_is_relative);
            let full = if file_path.is_absolute() {
                file_path
            } else {
                data_path.join(&file_path)
            };
            result.insert(full.resolve()?.path());
        }

        Ok(result)
    }
}
