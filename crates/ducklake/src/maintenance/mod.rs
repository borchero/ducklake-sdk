mod cleanup_old_files;
mod delete_orphaned_files;
mod expire_snapshots;
mod utils;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DryRun {
    Yes,
    No,
}
