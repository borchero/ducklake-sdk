use crate::catalog::SchemaRef;
use crate::spec::*;
use crate::transaction::{CommitState, TransactionChanges};
use crate::{DucklakeResult, db, io};

pub(crate) fn create_schema<'a>(
    changes: &mut TransactionChanges,
    state: &mut CommitState<'a>,
    schema_ref: &SchemaRef,
    name: &str,
    path: &io::DucklakePath,
) -> DucklakeResult<()> {
    let schema_id = state.schema_id(*schema_ref);

    // Create the schema
    let schema = DucklakeSchema {
        schema_id,
        begin_snapshot: state.snapshot_id(),
        end_snapshot: None,
        schema_uuid: Some(db::UuidText::now_v7()),
        schema_name: name.to_owned(),
        path: path.to_string(),
        path_is_relative: path.is_relative(),
    };
    changes.new_schemas.push(schema);

    Ok(())
}

pub(crate) fn delete_schema<'a>(
    changes: &mut TransactionChanges,
    state: &mut CommitState<'a>,
    schema_ref: &SchemaRef,
) -> DucklakeResult<()> {
    let schema_id = state.schema_id(*schema_ref);

    changes.dropped_schemas.insert(schema_id);

    Ok(())
}
