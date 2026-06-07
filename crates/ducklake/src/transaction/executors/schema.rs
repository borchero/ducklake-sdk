use sea_query::{ExprTrait, Query};

use crate::catalog::SchemaRef;
use crate::spec::*;
use crate::transaction::CommitState;
use crate::{DucklakeResult, db, io};

pub async fn create_schema<'a>(
    tx: &mut db::Transaction,
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
    tx.insert_entity(schema).await?;

    Ok(())
}

pub async fn delete_schema<'a>(
    tx: &mut db::Transaction,
    state: &mut CommitState<'a>,
    schema_ref: &SchemaRef,
) -> DucklakeResult<()> {
    let schema_id = state.schema_id(*schema_ref);

    set_end_snapshot!(ducklake_schema, state, tx, conditions: { SchemaId => schema_id });
    set_end_snapshot!(ducklake_tag, state, tx, conditions: { ObjectId => schema_id });

    Ok(())
}
