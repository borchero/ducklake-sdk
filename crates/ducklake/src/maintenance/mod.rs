mod delete_orphaned_files;
mod expire_snapshots;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DryRun {
    Yes,
    No,
}
