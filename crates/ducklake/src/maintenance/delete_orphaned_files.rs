use std::collections::HashSet;

use futures::{StreamExt, stream};
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;
use sea_query::{Query, UnionType};

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

        // 1) Collect all object store locations referenced by the catalog.
        let referenced = self.collect_referenced_locations(&data_path).await?;

        // 2) List all files below the data path and find the orphaned ones.
        let resolved_data_path = data_path.resolve()?;
        let store = resolved_data_path.object_store(Some(self.conn.storage_options().to_vec()));
        let prefix = resolved_data_path.path();

        let mut orphans = Vec::new();
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
        let snapshot = self.conn.latest_snapshot(false).await?;
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
        let mut result = HashSet::new();
        for (table_id, path, path_is_relative) in table_files {
            let file_path = io::DucklakePath::new(&path, path_is_relative);
            let path = catalog
                .table(table_id)
                .unwrap()
                .data_path(data_path)
                .join(&file_path);
            result.insert(path.resolve()?.path());
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
