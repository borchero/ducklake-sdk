use std::fmt::Display;
use std::str::FromStr;

use itertools::Itertools;

use crate::catalog::Catalog;
use crate::utils::{format_identifier, parse_identifier};
use crate::{DucklakeError, DucklakeResult};

/* -------------------------------------- CHANGE TYPE SET -------------------------------------- */

pub(in crate::transaction) struct AppliedChangeSet {
    changes: Vec<AppliedChange>,
}

impl AppliedChangeSet {
    /// Create a new changeset from a list of applied changes.
    ///
    /// The applied changes are deduplicated for an efficient representation of the changeset.
    pub(super) fn new(changes: Vec<AppliedChange>) -> Self {
        let changes = changes.into_iter().unique().collect();
        Self { changes }
    }

    pub(crate) fn check_conflict(
        &self,
        other: &AppliedChangeSet,
        catalog: &Catalog,
    ) -> DucklakeResult<()> {
        // NOTE: It _seems_ like the checks here are inefficient as the loop is O(n^2). However,
        //  we expect the number of changes in a single commit to be very small, so this is fine
        //  (and likely even more efficient than using hash sets for each type of change).
        for (lhs_change, rhs_change) in self.changes.iter().cartesian_product(other.changes.iter())
        {
            lhs_change.check_conflict(rhs_change, catalog)?;
        }
        Ok(())
    }
}

impl Display for AppliedChangeSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(first) = self.changes.first() {
            write!(f, "{first}")?;
        }
        for applied_change in self.changes.iter().skip(1) {
            write!(f, ",{applied_change}")?;
        }
        Ok(())
    }
}

impl FromStr for AppliedChangeSet {
    type Err = DucklakeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v = s
            .split(',')
            .map(|change| change.parse::<AppliedChange>())
            .collect::<DucklakeResult<Vec<_>>>()?;
        Ok(Self::new(v))
    }
}

/* ---------------------------------------- CHANGE TYPE ---------------------------------------- */

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(in crate::transaction) enum AppliedChange {
    CreatedSchema { name: SchemaName },
    CreatedTable { name: crate::TableName },
    CreatedView { name: crate::TableName },
    CreatedScalarMacro { name: String },
    CreatedTableMacro { name: String },
    InsertedIntoTable { id: i64 },
    DeletedFromTable { id: i64 },
    CompactedTable { id: i64 },
    InlinedInsert { id: i64 },
    InlinedDelete { id: i64 },
    InlineFlush { id: i64 },
    MergeAdjacent { id: i64 },
    RewriteDelete { id: i64 },
    DroppedSchema { id: i64 },
    DroppedTable { id: i64 },
    DroppedView { id: i64 },
    DroppedScalarMacro { id: i64 },
    DroppedTableMacro { id: i64 },
    AlteredTable { id: i64 },
    AlteredView { id: i64 },
}

impl AppliedChange {
    fn check_conflict(&self, other: &AppliedChange, catalog: &Catalog) -> DucklakeResult<()> {
        // List of conflicting change types can be found here:
        // https://ducklake.select/docs/stable/duckdb/advanced_features/conflict_resolution#logical-conflicts
        use AppliedChange::*;
        let message = match (self, other) {
            // --- Schemas ---
            (CreatedSchema { name: lhs_name }, CreatedSchema { name: rhs_name })
                if lhs_name == rhs_name =>
            {
                format!(
                    "attempting to create schema {lhs_name} but it was already {}",
                    other.action()
                )
            }
            (DroppedSchema { id: lhs_id }, DroppedSchema { id: rhs_id }) if lhs_id == rhs_id => {
                format!(
                    "attempting to drop schema with ID {lhs_id} but it was already {}",
                    other.action()
                )
            }
            (DroppedSchema { id: schema_id }, CreatedTable { name: table_name })
            | (CreatedTable { name: table_name }, DroppedSchema { id: schema_id }) => {
                let schema = catalog.schema(*schema_id)?;
                if schema.name() == table_name.schema {
                    format!(
                        "attempting to drop schema with ID {schema_id} but table {table_name} was created in it"
                    )
                } else {
                    return Ok(());
                }
            }
            (DroppedSchema { id: schema_id }, CreatedView { name: view_name })
            | (CreatedView { name: view_name }, DroppedSchema { id: schema_id }) => {
                let schema = catalog.schema(*schema_id)?;
                if schema.name() == view_name.schema {
                    format!(
                        "attempting to drop schema with ID {schema_id} but view {view_name} was created in it"
                    )
                } else {
                    return Ok(());
                }
            }
            // --- Tables ---
            (CreatedTable { name: lhs_name }, CreatedTable { name: rhs_name })
                if lhs_name == rhs_name =>
            {
                format!(
                    "attempting to create table {lhs_name} but it was already {}",
                    other.action()
                )
            }
            (DroppedTable { id: lhs_id }, DroppedTable { id: rhs_id }) if lhs_id == rhs_id => {
                format!(
                    "attempting to drop table with ID {lhs_id} but it was already {}",
                    other.action()
                )
            }
            (AlteredTable { id: lhs_id }, AlteredTable { id: rhs_id })
            | (AlteredTable { id: lhs_id }, DroppedTable { id: rhs_id })
                if lhs_id == rhs_id =>
            {
                format!(
                    "attempting to alter table with ID {lhs_id} but it was {}",
                    other.action()
                )
            }
            // --- Views ---
            (CreatedView { name: lhs_name }, CreatedView { name: rhs_name })
                if lhs_name == rhs_name =>
            {
                format!(
                    "attempting to create view {lhs_name} but it was already {}",
                    other.action()
                )
            }
            (DroppedView { id: lhs_id }, DroppedView { id: rhs_id }) if lhs_id == rhs_id => {
                format!(
                    "attempting to drop view with ID {lhs_id} but it was already {}",
                    other.action()
                )
            }
            (AlteredView { id: lhs_id }, AlteredView { id: rhs_id })
            | (AlteredView { id: lhs_id }, DroppedView { id: rhs_id })
                if lhs_id == rhs_id =>
            {
                format!(
                    "attempting to alter view with ID {lhs_id} but it was {}",
                    other.action()
                )
            }
            // --- Data ---
            (
                InsertedIntoTable { id: lhs_id } | InlinedInsert { id: lhs_id },
                DroppedTable { id: rhs_id },
            )
            | (
                InsertedIntoTable { id: lhs_id } | InlinedInsert { id: lhs_id },
                AlteredTable { id: rhs_id },
            ) if lhs_id == rhs_id => {
                format!(
                    "attempting to insert into table with ID {lhs_id} but the table was {}",
                    other.action()
                )
            }
            (
                DeletedFromTable { id: lhs_id } | InlinedDelete { id: lhs_id },
                DroppedTable { id: rhs_id },
            )
            | (
                DeletedFromTable { id: lhs_id } | InlinedDelete { id: lhs_id },
                AlteredTable { id: rhs_id },
            )
            | (
                DeletedFromTable { id: lhs_id } | InlinedDelete { id: lhs_id },
                DeletedFromTable { id: rhs_id },
            )
            | (
                DeletedFromTable { id: lhs_id } | InlinedDelete { id: lhs_id },
                CompactedTable { id: rhs_id },
            ) if lhs_id == rhs_id => {
                format!(
                    "attempting to delete from table with ID {lhs_id} but the table was {}",
                    other.action()
                )
            }
            // --- Compaction ---
            (CompactedTable { id: lhs_id }, DroppedTable { id: rhs_id })
            | (
                CompactedTable { id: lhs_id },
                DeletedFromTable { id: rhs_id } | InlinedDelete { id: rhs_id },
            ) if lhs_id == rhs_id => {
                format!(
                    "attempting to compact table with ID {lhs_id} but it was {}",
                    other.action()
                )
            }
            _ => return Ok(()),
        };
        Err(DucklakeError::TransactionConflict(message))
    }

    fn action(&self) -> &str {
        use AppliedChange::*;
        match self {
            CreatedSchema { .. }
            | CreatedTable { .. }
            | CreatedView { .. }
            | CreatedScalarMacro { .. }
            | CreatedTableMacro { .. } => "created",
            DroppedSchema { .. }
            | DroppedTable { .. }
            | DroppedView { .. }
            | DroppedScalarMacro { .. }
            | DroppedTableMacro { .. } => "dropped",
            AlteredTable { .. } | AlteredView { .. } => "altered",
            CompactedTable { .. } => "compacted",
            InsertedIntoTable { .. } | InlinedInsert { .. } => "inserted into",
            DeletedFromTable { .. } | InlinedDelete { .. } => "deleted from",
            InlineFlush { .. } => "flushed inlined changes for",
            MergeAdjacent { .. } => "merged adjacent changes for",
            RewriteDelete { .. } => "rewrote delete file for",
        }
    }
}

impl Display for AppliedChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use AppliedChange::*;
        match self {
            CreatedSchema { name } => write!(f, "created_schema:{}", name),
            CreatedTable { name } => write!(f, "created_table:{}", name),
            CreatedView { name } => write!(f, "created_view:{}", name),
            CreatedScalarMacro { name } => write!(f, "created_scalar_macro:{}", name),
            CreatedTableMacro { name } => write!(f, "created_table_macro:{}", name),
            InsertedIntoTable { id } => write!(f, "inserted_into_table:{id}"),
            DeletedFromTable { id } => write!(f, "deleted_from_table:{id}"),
            InlinedInsert { id } => write!(f, "inlined_insert:{id}"),
            InlinedDelete { id } => write!(f, "inlined_delete:{id}"),
            InlineFlush { id } => write!(f, "inline_flush:{id}"),
            MergeAdjacent { id } => write!(f, "merge_adjacent:{id}"),
            RewriteDelete { id } => write!(f, "rewrite_delete:{id}"),
            CompactedTable { id } => write!(f, "compacted_table:{id}"),
            DroppedSchema { id } => write!(f, "dropped_schema:{id}"),
            DroppedTable { id } => write!(f, "dropped_table:{id}"),
            DroppedView { id } => write!(f, "dropped_view:{id}"),
            DroppedScalarMacro { id } => write!(f, "dropped_scalar_macro:{id}"),
            DroppedTableMacro { id } => write!(f, "dropped_table_macro:{id}"),
            AlteredTable { id } => write!(f, "altered_table:{id}"),
            AlteredView { id } => write!(f, "altered_view:{id}"),
        }
    }
}

impl FromStr for AppliedChange {
    type Err = DucklakeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use AppliedChange::*;

        let mut parts = s.splitn(2, ':');
        let applied_change = parts.next().unwrap();
        let param = parts.next().ok_or(DucklakeError::Parsing(
            "invalid change type format (missing ':')".to_owned(),
        ))?;

        let result = match applied_change {
            "created_schema" => CreatedSchema {
                name: param.parse()?,
            },
            "created_table" => CreatedTable {
                name: param.parse()?,
            },
            "created_view" => CreatedView {
                name: param.parse()?,
            },
            "created_scalar_macro" => CreatedScalarMacro {
                name: param.to_string(),
            },
            "created_table_macro" => CreatedTableMacro {
                name: param.to_string(),
            },
            "inserted_into_table" => InsertedIntoTable {
                id: parse_id(param)?,
            },
            "deleted_from_table" => DeletedFromTable {
                id: parse_id(param)?,
            },
            "inlined_insert" => InlinedInsert {
                id: parse_id(param)?,
            },
            "inlined_delete" => InlinedDelete {
                id: parse_id(param)?,
            },
            "inline_flush" | "flushed_inlined" => InlineFlush {
                id: parse_id(param)?,
            },
            "merge_adjacent" => MergeAdjacent {
                id: parse_id(param)?,
            },
            "rewrite_delete" => RewriteDelete {
                id: parse_id(param)?,
            },
            "compacted_table" => CompactedTable {
                id: parse_id(param)?,
            },
            "dropped_schema" => DroppedSchema {
                id: parse_id(param)?,
            },
            "dropped_table" => DroppedTable {
                id: parse_id(param)?,
            },
            "dropped_view" => DroppedView {
                id: parse_id(param)?,
            },
            "dropped_scalar_macro" => DroppedScalarMacro {
                id: parse_id(param)?,
            },
            "dropped_table_macro" => DroppedTableMacro {
                id: parse_id(param)?,
            },
            "altered_table" => AlteredTable {
                id: parse_id(param)?,
            },
            "altered_view" => AlteredView {
                id: parse_id(param)?,
            },
            applied_change => {
                return Err(DucklakeError::Parsing(format!(
                    "unknown change type '{applied_change}'"
                )));
            }
        };
        Ok(result)
    }
}

/* ---------------------------------------- STRING UTILS --------------------------------------- */

fn parse_id(s: &str) -> DucklakeResult<i64> {
    s.parse()
        .map_err(|err| DucklakeError::Parsing(format!("invalid entity ID '{s}': {err}")))
}

/* ------------------------------------------- SCHEMA ------------------------------------------ */

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(in crate::transaction) struct SchemaName(pub String);

impl From<String> for SchemaName {
    fn from(s: String) -> Self {
        SchemaName(s)
    }
}

impl FromStr for SchemaName {
    type Err = crate::DucklakeError;

    fn from_str(s: &str) -> DucklakeResult<Self> {
        if let Some(components) = parse_identifier(s)
            && components.len() == 1
        {
            return Ok(SchemaName(components[0].clone()));
        }
        Err(DucklakeError::InvalidSchemaName {
            name: s.into(),
            reason: "expected format 'schema'",
        })
    }
}

impl Display for SchemaName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format_identifier(&[&self.0]))
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                             TESTS                                             */
/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn test_applied_changes_to_string() {
        let changes = vec![
            AppliedChange::CreatedSchema {
                name: "test_schema".to_string().into(),
            },
            AppliedChange::DroppedSchema { id: 42 },
        ];
        let applied_changes = AppliedChangeSet::new(changes);
        assert_eq!(
            applied_changes.to_string(),
            "created_schema:\"test_schema\",dropped_schema:42"
        );
    }

    #[test]
    fn test_applied_changes_from_str() {
        let s = "created_schema:\"test_schema\",dropped_schema:42";
        let applied_changes = s.parse::<AppliedChangeSet>().unwrap();
        assert_eq!(applied_changes.changes.len(), 2);
        match &applied_changes.changes[0] {
            AppliedChange::CreatedSchema { name } => {
                assert_eq!(name, &"test_schema".to_string().into())
            }
            _ => panic!(),
        }
        match &applied_changes.changes[1] {
            AppliedChange::DroppedSchema { id } => assert_eq!(*id, 42),
            _ => panic!(),
        }
    }
}
