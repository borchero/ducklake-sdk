use sea_query::{ExprTrait, Query};

use crate::catalog::{SchemaRef, ViewRef};
use crate::spec::*;
use crate::transaction::CommitState;
use crate::{DucklakeResult, db};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_view<'a>(
    tx: &mut db::Transaction,
    state: &mut CommitState<'a>,
    schema_ref: &SchemaRef,
    view_ref: &ViewRef,
    name: &crate::TableName,
    sql: &str,
    dialect: &str,
    column_aliases: &Option<Vec<String>>,
    tags: &Option<Vec<crate::Tag>>,
) -> DucklakeResult<()> {
    let view_id = state.view_id(*view_ref);

    // 1/2) Create the view
    let view = DucklakeView {
        view_id,
        view_uuid: Some(db::UuidText::now_v7()),
        begin_snapshot: state.snapshot_id(),
        end_snapshot: None,
        schema_id: state.schema_id(*schema_ref),
        view_name: name.name.clone(),
        dialect: dialect.to_string(),
        sql: sql.to_string(),
        column_aliases: Some(crate::utils::format_identifier_list(
            column_aliases.as_deref().unwrap_or_default(),
        )),
    };
    tx.insert_entity(view).await?;

    // 2/2) Optionally add tags to the view
    if let Some(tags) = tags
        && !tags.is_empty()
    {
        let ducklake_tags = tags.iter().map(|t| DucklakeTag {
            object_id: view_id,
            begin_snapshot: state.snapshot_id(),
            end_snapshot: None,
            key: t.key.clone(),
            value: t.value.clone(),
        });
        tx.insert_entities(ducklake_tags).await?;
    }

    Ok(())
}

pub(crate) async fn delete_view<'a>(
    tx: &mut db::Transaction,
    state: &mut CommitState<'a>,
    view_ref: &ViewRef,
) -> DucklakeResult<()> {
    let view_id = state.view_id(*view_ref);

    set_end_snapshot!(ducklake_view, state, tx, conditions: { ViewId => view_id });
    set_end_snapshot!(ducklake_tag, state, tx, conditions: { ObjectId => view_id });

    Ok(())
}
