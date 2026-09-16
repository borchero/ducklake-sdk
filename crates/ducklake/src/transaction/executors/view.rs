use crate::catalog::{SchemaRef, ViewRef};
use crate::spec::*;
use crate::transaction::{CommitState, TransactionChanges};
use crate::{DucklakeResult, db};

#[allow(clippy::too_many_arguments)]
pub(crate) fn create_view<'a>(
    changes: &mut TransactionChanges,
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
    changes.new_views.push(view);

    // 2/2) Optionally add tags to the view
    if let Some(tags) = tags
        && !tags.is_empty()
    {
        let snapshot_id = state.snapshot_id();
        let ducklake_tags = tags.iter().map(|t| DucklakeTag {
            object_id: view_id,
            begin_snapshot: snapshot_id,
            end_snapshot: None,
            key: t.key.clone(),
            value: t.value.clone(),
        });
        changes.new_tags.extend(ducklake_tags);
    }

    Ok(())
}

pub(crate) fn delete_view<'a>(
    changes: &mut TransactionChanges,
    state: &mut CommitState<'a>,
    view_ref: &ViewRef,
) -> DucklakeResult<()> {
    let view_id = state.view_id(*view_ref);

    changes.dropped_views.insert(view_id);

    Ok(())
}
