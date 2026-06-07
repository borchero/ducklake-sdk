use std::collections::HashSet;

use itertools::Itertools;

use super::{AppliedChange, AppliedChangeSet};
use crate::catalog::{ColumnRef, SchemaRef, TableRef};
use crate::transaction::{CommitDataFile, CommitInlineData, CommitState, executors};
use crate::{DucklakeResult, db, io};

/* ----------------------------------------- CHANGE SET ---------------------------------------- */

/// A set of changes that ought to be applied within a transaction.
#[derive(Debug, Clone)]
pub struct ChangeSet {
    changes: Vec<Change>,
}

impl ChangeSet {
    /// Initialize a new change set from the provided changes.
    ///
    /// The change set retains the order of changes, but de-duplicates them based on their type.
    /// For example: if a table is renamed multiple times, only the last rename is kept.
    pub fn new(changes: Vec<Change>) -> Self {
        // Reverse so `unique_by` keeps the LAST occurrence of each key, then reverse back to
        // preserve input order. The double-`.rev()` is materialized between the two reversals
        // because `Rev<UniqueBy<Rev<_>>>` cancels the reversals out via `DoubleEndedIterator`.
        let mut changes = changes
            .into_iter()
            .rev()
            .unique_by(|c| HashableChange::from(c))
            .collect_vec();
        changes.reverse();

        // We also have to handle some special cases: if a table is deleted, all previous changes
        // to this table become irrelevant and should be removed. If a table is created in the
        // same transaction, the drop itself should also be removed.
        let created_tables: HashSet<_> = changes
            .iter()
            .filter_map(|c| {
                if let Change::CreateTable { table_ref, .. } = c {
                    Some(*table_ref)
                } else {
                    None
                }
            })
            .collect();
        let deleted_tables: HashSet<_> = changes
            .iter()
            .filter_map(|c| {
                if let Change::DeleteTable { table_ref } = c {
                    Some(*table_ref)
                } else {
                    None
                }
            })
            .collect();
        changes.retain(|c| match c {
            Change::DeleteTable { table_ref } => !created_tables.contains(table_ref),
            c => c
                .affected_table_ref()
                .map(|r| !deleted_tables.contains(&r))
                .unwrap_or(true),
        });

        // The same applies to schemas. However, there we have on modifications other than schema
        // creation
        let created_schemas: HashSet<_> = changes
            .iter()
            .filter_map(|c| {
                if let Change::CreateSchema { schema_ref, .. } = c {
                    Some(*schema_ref)
                } else {
                    None
                }
            })
            .collect();
        let deleted_schemas: HashSet<_> = changes
            .iter()
            .filter_map(|c| {
                if let Change::DeleteSchema { schema_ref } = c {
                    Some(*schema_ref)
                } else {
                    None
                }
            })
            .collect();
        changes.retain(|c| match c {
            Change::DeleteSchema { schema_ref } => !created_schemas.contains(schema_ref),
            Change::CreateSchema { schema_ref, .. } => !deleted_schemas.contains(schema_ref),
            _ => true,
        });

        Self { changes }
    }

    /// Whether this change set is empty.
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    /// Compute the applied from the changes in this change set.
    pub fn applied_change_set(&self, state: &mut CommitState<'_>) -> AppliedChangeSet {
        let applied_changes = self
            .changes
            .iter()
            .map(|c| c.applied_change(state))
            .collect();
        AppliedChangeSet::new(applied_changes)
    }

    /// Apply the changes in this change set within the provided transaction.
    ///
    /// The state is used to obtain IDs for newly created objects as well as other metadata such
    /// as the current snapshot ID.
    pub async fn apply(
        &self,
        tx: &mut db::Transaction,
        state: &mut CommitState<'_>,
    ) -> DucklakeResult<()> {
        // First, we execute all the changes
        for change in &self.changes {
            change.apply(tx, state).await?;
        }

        // Then, we need to make sure to create inlined data tables for all tables for which
        // the schema was changed. Note that this also includes newly created tables.
        for table_ref in self.table_refs_with_schema_changes() {
            executors::create_inlined_data_table(tx, state, &table_ref).await?;
        }

        // TODO: Ideally, we'd want to write inlined data only here. However, this requires
        //  modifying the inlined data before writing...

        // Finally, we need to check whether we wrote inline data without writing any data files.
        // In this case, we need to bump the next file ID as table stats are cached by file ID
        // (both in this SDK and the official DuckDB DuckLake extension).
        if self.contains_inline_data_writes_and_no_file_writes() {
            // Advance the file ID by generating and discarding a new one.
            let _ = state.file_id();
        }

        Ok(())
    }

    /// Whether there is at least one inline data write in this change set but no file write.
    fn contains_inline_data_writes_and_no_file_writes(&self) -> bool {
        self.changes
            .iter()
            .any(|c| matches!(c, Change::WriteTableInlineData { .. }))
            && !self
                .changes
                .iter()
                .any(|c| matches!(c, Change::WriteTableDataFiles { .. }))
    }

    /// Whether any of the changes in this change set affects the schema of the DuckLake. This
    /// essentially applies to all changes that do not insert or re-arrange data.
    pub fn changes_schema(&self) -> bool {
        self.changes.iter().any(|c| c.changes_schema())
    }

    /// Obtain the IDs of all tables for which inline data is written. This is necessary for
    /// detecting writes of inline data in transactions where the table schema is changed.
    /// This is currently not handled but we need to throw an error.
    pub fn table_ids_with_inline_data_writes(&self, state: &mut CommitState<'_>) -> Vec<i64> {
        self.changes
            .iter()
            .filter_map(|c| {
                if let Change::WriteTableInlineData { table_ref, .. } = c {
                    Some(state.table_id(*table_ref))
                } else {
                    None
                }
            })
            .unique()
            .collect()
    }

    /// Obtain the IDs of all tables for which the schema changes in this change set. This includes
    /// created and deleted tables. This information is required for the `DucklakeSchemaVersions`
    /// table, which tracks schema changes on a per-table basis.
    pub fn table_ids_with_schema_changes(&self, state: &mut CommitState<'_>) -> Vec<i64> {
        self.table_refs_with_schema_changes()
            .into_iter()
            .map(|r| state.table_id(r))
            .collect()
    }

    fn table_refs_with_schema_changes(&self) -> Vec<TableRef> {
        self.changes
            .iter()
            .filter_map(|c| c.table_ref_with_schema_change())
            .unique()
            .collect()
    }

    /// Whether any of the changes in this change set requires to read or write table stats.
    pub fn requires_table_stats(&self) -> bool {
        self.changes.iter().any(|c| c.requires_table_stats())
    }
}

/* ------------------------------------------- CHANGE ------------------------------------------ */

/// A change to be applied within a transaction.
#[derive(Debug, Clone)]
pub enum Change {
    // --- SCHEMA CHANGES ---
    CreateSchema {
        schema_ref: SchemaRef,
        name: String,
        path: io::DucklakePath,
    },
    DeleteSchema {
        schema_ref: SchemaRef,
    },
    // --- TABLE CHANGES ---
    CreateTable {
        schema_ref: SchemaRef,
        table_ref: TableRef,
        column_refs: Vec<Vec<ColumnRef>>,
        partition_column_refs: Option<Vec<ColumnRef>>,
        name: crate::TableName,
        columns: Vec<crate::Column>,
        partition_columns: Option<Vec<crate::PartitionColumn>>,
        path: io::DucklakePath,
        tags: Option<Vec<crate::Tag>>,
    },
    WriteTableDataFiles {
        table_ref: TableRef,
        data_files: Vec<CommitDataFile>,
    },
    WriteTableInlineData {
        table_ref: TableRef,
        data: Vec<CommitInlineData>,
    },
    RenameTable {
        table_ref: TableRef,
        name: crate::TableName,
    },
    UpdateTablePartitioning {
        table_ref: TableRef,
        partition_column_refs: Option<Vec<ColumnRef>>,
        partition_columns: Option<Vec<crate::PartitionColumn>>,
    },
    DeleteTable {
        table_ref: TableRef,
    },
    AddTableTag {
        table_ref: TableRef,
        tag: crate::Tag,
    },
    RemoveTableTag {
        table_ref: TableRef,
        key: String,
    },
    // --- COLUMN CHANGES ---
    AddTableColumn {
        parent_column_ref: Option<ColumnRef>,
        column_refs: Vec<ColumnRef>,
        column: crate::Column,
    },
    UpdateTableColumn {
        parent_column_ref: Option<ColumnRef>,
        column_ref: ColumnRef,
        column: crate::Column,
    },
    RemoveTableColumn {
        column_ref: ColumnRef,
    },
    AddTableColumnTag {
        column_ref: ColumnRef,
        tag: crate::Tag,
    },
    RemoveTableColumnTag {
        column_ref: ColumnRef,
        key: String,
    },
}

impl Change {
    fn applied_change<'a>(&self, state: &mut CommitState<'a>) -> AppliedChange {
        use Change::*;
        match self {
            CreateSchema { name, .. } => AppliedChange::CreatedSchema {
                name: name.to_owned().into(),
            },
            DeleteSchema { schema_ref } => AppliedChange::DroppedSchema {
                id: state.schema_id(*schema_ref),
            },
            CreateTable { name, .. } | RenameTable { name, .. } => AppliedChange::CreatedTable {
                name: name.to_owned(),
            },
            UpdateTablePartitioning { table_ref, .. }
            | AddTableTag { table_ref, .. }
            | RemoveTableTag { table_ref, .. } => AppliedChange::AlteredTable {
                id: state.table_id(*table_ref),
            },
            AddTableColumn { column_refs, .. } => AppliedChange::AlteredTable {
                id: state.table_id(column_refs[0].table_ref),
            },
            UpdateTableColumn { column_ref, .. }
            | RemoveTableColumn { column_ref }
            | AddTableColumnTag { column_ref, .. }
            | RemoveTableColumnTag { column_ref, .. } => AppliedChange::AlteredTable {
                id: state.table_id(column_ref.table_ref),
            },
            DeleteTable { table_ref } => AppliedChange::DroppedTable {
                id: state.table_id(*table_ref),
            },
            WriteTableDataFiles { table_ref, .. } => AppliedChange::InsertedIntoTable {
                id: state.table_id(*table_ref),
            },
            WriteTableInlineData { table_ref, .. } => AppliedChange::InlinedInsert {
                id: state.table_id(*table_ref),
            },
        }
    }

    async fn apply<'a>(
        &self,
        tx: &mut db::Transaction,
        state: &mut CommitState<'a>,
    ) -> DucklakeResult<()> {
        use Change::*;
        match self {
            // --- SCHEMA CHANGES ---
            CreateSchema {
                schema_ref,
                name,
                path,
            } => executors::create_schema(tx, state, schema_ref, name, path).await,
            DeleteSchema { schema_ref } => executors::delete_schema(tx, state, schema_ref).await,
            // --- TABLE CHANGES ---
            CreateTable {
                schema_ref,
                table_ref,
                column_refs,
                partition_column_refs,
                name,
                columns,
                partition_columns,
                path,
                tags,
            } => {
                executors::create_table(
                    tx,
                    state,
                    schema_ref,
                    table_ref,
                    column_refs,
                    partition_column_refs,
                    name,
                    columns,
                    partition_columns,
                    path,
                    tags,
                )
                .await
            }
            WriteTableDataFiles {
                table_ref,
                data_files,
            } => executors::write_table_data(tx, state, table_ref, data_files).await,
            WriteTableInlineData { table_ref, data } => {
                executors::write_table_inline_data(tx, state, table_ref, data).await
            }
            RenameTable { table_ref, name } => {
                executors::rename_table(tx, state, table_ref, name).await
            }
            UpdateTablePartitioning {
                table_ref,
                partition_column_refs,
                partition_columns,
            } => {
                executors::update_table_partitioning(
                    tx,
                    state,
                    table_ref,
                    partition_column_refs,
                    partition_columns,
                )
                .await
            }
            DeleteTable { table_ref } => executors::delete_table(tx, state, table_ref).await,
            AddTableTag { table_ref, tag } => {
                executors::add_table_tag(tx, state, table_ref, tag).await
            }
            RemoveTableTag { table_ref, key } => {
                executors::remove_table_tag(tx, state, table_ref, key).await
            }
            // --- COLUMN CHANGES ---
            AddTableColumn {
                parent_column_ref,
                column_refs,
                column,
            } => {
                executors::add_table_column(tx, state, parent_column_ref, column_refs, column)
                    .await
            }
            UpdateTableColumn {
                parent_column_ref,
                column_ref,
                column,
            } => {
                executors::update_table_column(tx, state, parent_column_ref, column_ref, column)
                    .await
            }
            RemoveTableColumn { column_ref } => {
                executors::remove_table_column(tx, state, column_ref).await
            }
            AddTableColumnTag { column_ref, tag } => {
                executors::add_table_column_tag(tx, state, column_ref, tag).await
            }
            RemoveTableColumnTag { column_ref, key } => {
                executors::remove_table_column_tag(tx, state, column_ref, key).await
            }
        }
    }

    /// Whether this change affects the schema of the DuckLake.
    fn changes_schema(&self) -> bool {
        use Change::*;
        match self {
            CreateSchema { .. }
            | DeleteSchema { .. }
            | CreateTable { .. }
            | RenameTable { .. }
            | UpdateTablePartitioning { .. }
            | DeleteTable { .. }
            | AddTableTag { .. }
            | RemoveTableTag { .. }
            | AddTableColumn { .. }
            | UpdateTableColumn { .. }
            | RemoveTableColumn { .. }
            | AddTableColumnTag { .. }
            | RemoveTableColumnTag { .. } => true,
            WriteTableDataFiles { .. } | WriteTableInlineData { .. } => false,
        }
    }

    /// The TableRef which is affected by this change, if any.
    fn affected_table_ref(&self) -> Option<TableRef> {
        use Change::*;
        match self {
            CreateTable { table_ref, .. }
            | RenameTable { table_ref, .. }
            | UpdateTablePartitioning { table_ref, .. }
            | DeleteTable { table_ref, .. }
            | AddTableTag { table_ref, .. }
            | RemoveTableTag { table_ref, .. }
            | WriteTableDataFiles { table_ref, .. }
            | WriteTableInlineData { table_ref, .. } => Some(*table_ref),
            UpdateTableColumn { column_ref, .. }
            | RemoveTableColumn { column_ref, .. }
            | AddTableColumnTag { column_ref, .. }
            | RemoveTableColumnTag { column_ref, .. } => Some(column_ref.table_ref),
            AddTableColumn { column_refs, .. } => column_refs.first().map(|r| r.table_ref),
            CreateSchema { .. } | DeleteSchema { .. } => None,
        }
    }

    /// The TableRef whose schema this change affects, if any.
    fn table_ref_with_schema_change(&self) -> Option<TableRef> {
        use Change::*;
        match self {
            CreateTable { table_ref, .. }
            | RenameTable { table_ref, .. }
            | UpdateTablePartitioning { table_ref, .. }
            | DeleteTable { table_ref, .. }
            | AddTableTag { table_ref, .. }
            | RemoveTableTag { table_ref, .. } => Some(*table_ref),
            UpdateTableColumn { column_ref, .. }
            | RemoveTableColumn { column_ref }
            | AddTableColumnTag { column_ref, .. }
            | RemoveTableColumnTag { column_ref, .. } => Some(column_ref.table_ref),
            AddTableColumn { column_refs, .. } => column_refs.first().map(|r| r.table_ref),
            CreateSchema { .. }
            | DeleteSchema { .. }
            | WriteTableDataFiles { .. }
            | WriteTableInlineData { .. } => None,
        }
    }

    fn requires_table_stats(&self) -> bool {
        use Change::*;
        match self {
            WriteTableDataFiles { .. } | WriteTableInlineData { .. } => true,
            CreateSchema { .. }
            | DeleteSchema { .. }
            | CreateTable { .. }
            | RenameTable { .. }
            | UpdateTablePartitioning { .. }
            | DeleteTable { .. }
            | AddTableTag { .. }
            | RemoveTableTag { .. }
            | AddTableColumn { .. }
            | UpdateTableColumn { .. }
            | RemoveTableColumn { .. }
            | AddTableColumnTag { .. }
            | RemoveTableColumnTag { .. } => false,
        }
    }
}

/* ------------------------------------ CHANGE DEDUPLICATION ----------------------------------- */

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum HashableChange {
    // --- SCHEMA CHANGES ---
    CreateSchema {
        schema_ref: SchemaRef,
    },
    DeleteSchema {
        schema_ref: SchemaRef,
    },
    // --- TABLE CHANGES ---
    CreateTable {
        table_ref: TableRef,
    },
    RenameTable {
        table_ref: TableRef,
    },
    UpdateTablePartitioning {
        table_ref: TableRef,
    },
    DeleteTable {
        table_ref: TableRef,
    },
    AddTableTag {
        table_ref: TableRef,
        key: String,
    },
    RemoveTableTag {
        table_ref: TableRef,
        key: String,
    },
    // --- COLUMN CHANGES ---
    AddTableColumn {
        column_refs: Vec<ColumnRef>,
    },
    UpdateTableColumn {
        column_ref: ColumnRef,
    },
    RemoveTableColumn {
        column_ref: ColumnRef,
    },
    AddTableColumnTag {
        column_ref: ColumnRef,
        key: String,
    },
    RemoveTableColumnTag {
        column_ref: ColumnRef,
        key: String,
    },
    // --- DATA OPERATIONS ---
    WriteTableDataFiles {
        table_ref: TableRef,
        paths: Vec<io::DucklakePath>,
    },
    WriteTableInlineData {
        // NOTE: We can only write inline data once per table within a transaction as, otherwise,
        //  we might need to run into schema violations.
        table_ref: TableRef,
    },
}

impl From<&Change> for HashableChange {
    fn from(change: &Change) -> Self {
        use HashableChange::*;
        match change {
            Change::CreateSchema { schema_ref, .. } => CreateSchema {
                schema_ref: *schema_ref,
            },
            Change::DeleteSchema { schema_ref } => DeleteSchema {
                schema_ref: *schema_ref,
            },
            Change::CreateTable { table_ref, .. } => CreateTable {
                table_ref: *table_ref,
            },
            Change::RenameTable { table_ref, .. } => RenameTable {
                table_ref: *table_ref,
            },
            Change::UpdateTablePartitioning { table_ref, .. } => UpdateTablePartitioning {
                table_ref: *table_ref,
            },
            Change::DeleteTable { table_ref } => DeleteTable {
                table_ref: *table_ref,
            },
            Change::AddTableTag { table_ref, tag } => AddTableTag {
                table_ref: *table_ref,
                key: tag.key.clone(),
            },
            Change::RemoveTableTag { table_ref, key } => RemoveTableTag {
                table_ref: *table_ref,
                key: key.clone(),
            },
            Change::AddTableColumn { column_refs, .. } => AddTableColumn {
                column_refs: column_refs.clone(),
            },
            Change::UpdateTableColumn { column_ref, .. } => UpdateTableColumn {
                column_ref: *column_ref,
            },
            Change::RemoveTableColumn { column_ref } => RemoveTableColumn {
                column_ref: *column_ref,
            },
            Change::AddTableColumnTag { column_ref, tag } => AddTableColumnTag {
                column_ref: *column_ref,
                key: tag.key.clone(),
            },
            Change::RemoveTableColumnTag { column_ref, key } => RemoveTableColumnTag {
                column_ref: *column_ref,
                key: key.clone(),
            },
            Change::WriteTableDataFiles {
                table_ref,
                data_files,
            } => WriteTableDataFiles {
                table_ref: *table_ref,
                paths: data_files.iter().map(|f| f.path.clone()).collect(),
            },
            Change::WriteTableInlineData { table_ref, .. } => WriteTableInlineData {
                table_ref: *table_ref,
            },
        }
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                             TESTS                                             */
/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::Tag;

    fn make_rename(table: usize, name: &str) -> Change {
        Change::RenameTable {
            table_ref: TableRef::mock(table),
            name: name.parse().unwrap(),
        }
    }

    fn make_add_tag(table: usize, key: &str, value: &str) -> Change {
        Change::AddTableTag {
            table_ref: TableRef::mock(table),
            tag: Tag {
                key: key.to_string(),
                value: value.to_string(),
            },
        }
    }

    fn changes_of(set: &ChangeSet) -> &[Change] {
        &set.changes
    }

    #[test]
    fn test_change_set_preserves_order_without_duplicates() {
        let changes = vec![
            make_rename(1, "a.foo"),
            make_rename(2, "b.bar"),
            make_add_tag(1, "k", "v"),
        ];
        let set = ChangeSet::new(changes);
        assert_eq!(changes_of(&set).len(), 3);
    }

    #[test]
    fn test_change_set_dedups_keeping_last_rename() {
        // Two renames of the same table collapse to the last one
        let changes = vec![
            make_rename(1, "a.first"),
            make_add_tag(2, "k", "v"),
            make_rename(1, "a.last"),
        ];
        let set = ChangeSet::new(changes);
        let result = changes_of(&set);
        assert_eq!(result.len(), 2);
        // The unique rename must keep the LAST name
        match &result[1] {
            Change::RenameTable { name, .. } => assert_eq!(name.to_string(), "\"a\".\"last\""),
            _ => panic!("expected RenameTable last"),
        }
        match &result[0] {
            Change::AddTableTag { .. } => {}
            _ => panic!("expected AddTableTag first"),
        }
    }

    #[test]
    fn test_change_set_dedups_tag_with_same_key() {
        // Adding the same tag key twice collapses to one; different keys do not
        let changes = vec![
            make_add_tag(1, "k", "v1"),
            make_add_tag(1, "k", "v2"),
            make_add_tag(1, "other", "x"),
        ];
        let set = ChangeSet::new(changes);
        let result = changes_of(&set);
        assert_eq!(result.len(), 2);
        // Last write wins for key "k"
        match &result[0] {
            Change::AddTableTag { tag, .. } => {
                assert_eq!(tag.key, "k");
                assert_eq!(tag.value, "v2");
            }
            _ => panic!("expected AddTableTag first"),
        }
    }

    #[test]
    fn test_change_set_does_not_dedup_different_tables() {
        let changes = vec![make_rename(1, "a.x"), make_rename(2, "b.y")];
        let set = ChangeSet::new(changes);
        assert_eq!(changes_of(&set).len(), 2);
    }

    #[test]
    fn test_changes_schema_true_for_schema_changes() {
        let set = ChangeSet::new(vec![make_rename(1, "a.x")]);
        assert!(set.changes_schema());
    }

    #[test]
    fn test_changes_schema_false_for_pure_data_writes() {
        let set = ChangeSet::new(vec![Change::WriteTableDataFiles {
            table_ref: TableRef::mock(1),
            data_files: vec![],
        }]);
        assert!(!set.changes_schema());
    }

    #[test]
    fn test_requires_table_stats_for_writes() {
        let set = ChangeSet::new(vec![Change::WriteTableDataFiles {
            table_ref: TableRef::mock(1),
            data_files: vec![],
        }]);
        assert!(set.requires_table_stats());
    }

    #[test]
    fn test_requires_table_stats_false_for_rename() {
        let set = ChangeSet::new(vec![make_rename(1, "a.x")]);
        assert!(!set.requires_table_stats());
    }

    #[test]
    fn test_hashable_change_distinguishes_by_table_ref() {
        let a = HashableChange::from(&make_rename(1, "a.x"));
        let b = HashableChange::from(&make_rename(2, "a.x"));
        assert_ne!(a, b);
    }

    #[test]
    fn test_hashable_change_ignores_rename_name_difference() {
        // Two renames of the same table dedup regardless of the target name
        let a = HashableChange::from(&make_rename(1, "a.x"));
        let b = HashableChange::from(&make_rename(1, "b.y"));
        assert_eq!(a, b);
    }

    #[test]
    fn test_hashable_change_tag_distinguishes_keys() {
        let a = HashableChange::from(&make_add_tag(1, "k1", "v"));
        let b = HashableChange::from(&make_add_tag(1, "k2", "v"));
        assert_ne!(a, b);
    }
}
