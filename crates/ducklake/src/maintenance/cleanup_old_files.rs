use object_store::ObjectStoreExt;
use sea_query::{Condition, ExprTrait, Query};

use super::DryRun;
use crate::spec::*;
use crate::{Ducklake, DucklakeResult, io};

/* ----------------------------------------- PUBLIC API ---------------------------------------- */

impl Ducklake {
    /// Delete files that have been scheduled for deletion based on the global `delete_older_than`
    /// configuration.
    ///
    /// Files are only scheduled for deletion once the snapshots referencing them are expired (see
    /// [`Ducklake::expire_snapshots`]). This function deletes all such files whose deletion was
    /// scheduled longer ago than the configured duration. If the configuration option is not set,
    /// it defaults to two days.
    ///
    /// The `dry_run` flag allows to understand which files would be deleted upon execution.
    ///
    /// Returns the paths of the files that were deleted (or that would be deleted in dry-run mode).
    pub async fn cleanup_old_files(&self, dry_run: DryRun) -> DucklakeResult<Vec<String>> {
        let interval = self.conn.metadata().delete_older_than();
        let timestamp = chrono::Utc::now() - interval.months - interval.delta;
        self.cleanup_old_files_filtered(CleanupFilter::OlderThan(timestamp), dry_run)
            .await
    }

    /// Delete files scheduled for deletion before a specific timestamp.
    ///
    /// The functionality matches [`Ducklake::cleanup_old_files`] for a predefined timestamp.
    pub async fn cleanup_old_files_older_than(
        &self,
        timestamp: chrono::DateTime<chrono::Utc>,
        dry_run: DryRun,
    ) -> DucklakeResult<Vec<String>> {
        self.cleanup_old_files_filtered(CleanupFilter::OlderThan(timestamp), dry_run)
            .await
    }

    /// Delete all files scheduled for deletion regardless of when they were scheduled.
    ///
    /// The functionality matches [`Ducklake::cleanup_old_files`] but ignores the
    /// `delete_older_than` configuration.
    pub async fn cleanup_all_old_files(&self, dry_run: DryRun) -> DucklakeResult<Vec<String>> {
        self.cleanup_old_files_filtered(CleanupFilter::All, dry_run)
            .await
    }

    async fn cleanup_old_files_filtered(
        &self,
        filter: CleanupFilter,
        dry_run: DryRun,
    ) -> DucklakeResult<Vec<String>> {
        let data_path = self.conn.metadata().data_path();
        let storage_options = self.conn.storage_options().to_vec();

        let mut tx = self.conn.pool().begin().await?;

        // 1) Find all files scheduled for deletion that match the filter.
        let mut select_query = Query::select()
            .columns([
                ducklake_files_scheduled_for_deletion::Column::Path,
                ducklake_files_scheduled_for_deletion::Column::PathIsRelative,
            ])
            .from(ducklake_files_scheduled_for_deletion::Table)
            .take();
        if let Some(condition) = filter.condition() {
            select_query.cond_where(condition);
        }
        let files: Vec<(String, bool)> = tx.fetch_all(&select_query).await?;

        // Resolve the full path of each file relative to the catalog's data path.
        let paths = files
            .into_iter()
            .map(|(path, path_is_relative)| {
                let stored = io::DucklakePath::new(&path, path_is_relative);
                if stored.is_absolute() {
                    stored
                } else {
                    data_path.join(&stored)
                }
            })
            .collect::<Vec<_>>();

        // 2) In dry-run mode, we return the paths that would be deleted without touching anything.
        if matches!(dry_run, DryRun::Yes) {
            tx.rollback().await?;
            return Ok(paths.iter().map(|path| path.to_string()).collect());
        }

        // 3) Delete each file from the object store. Files that are already gone are tolerated.
        for path in &paths {
            delete_file(path, &storage_options).await?;
        }

        // 4) Now that the files are gone, delete the corresponding rows from the database.
        let mut delete_query = Query::delete()
            .from_table(ducklake_files_scheduled_for_deletion::Table)
            .take();
        if let Some(condition) = filter.condition() {
            delete_query.cond_where(condition);
        }
        tx.execute(&delete_query).await?;

        tx.commit().await?;
        Ok(paths.into_iter().map(|path| path.to_string()).collect())
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                             UTILS                                             */
/* --------------------------------------------------------------------------------------------- */

async fn delete_file(
    path: &io::DucklakePath,
    storage_options: &[(String, String)],
) -> DucklakeResult<()> {
    let io_path = path.resolve()?;
    let store = io_path.object_store(Some(storage_options.to_vec()));
    let object_path = io_path.path();
    match store.delete(&object_path).await {
        // We tolerate files that are already gone to keep the operation idempotent.
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/* ------------------------------------------ FILTERS ------------------------------------------ */

enum CleanupFilter {
    All,
    OlderThan(chrono::DateTime<chrono::Utc>),
}

impl CleanupFilter {
    fn condition(&self) -> Option<Condition> {
        match self {
            Self::All => None,
            Self::OlderThan(timestamp) => Some(
                ducklake_files_scheduled_for_deletion::Column::ScheduleStart
                    .col()
                    .lt(*timestamp)
                    .into(),
            ),
        }
    }
}
