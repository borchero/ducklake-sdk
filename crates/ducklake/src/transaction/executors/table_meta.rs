use itertools::Itertools;
use sea_query::{Expr, ExprTrait, Query};
use strum::IntoEnumIterator;

use crate::catalog::{ColumnRef, SchemaRef, TableRef};
use crate::spec::*;
use crate::transaction::CommitState;
use crate::{DucklakeResult, Value, db, io};

/* --------------------------------------------------------------------------------------------- */
/*                                             TABLE                                             */
/* --------------------------------------------------------------------------------------------- */

#[allow(clippy::too_many_arguments)]
pub async fn create_table<'a>(
    tx: &mut db::Transaction,
    state: &mut CommitState<'a>,
    schema_ref: &SchemaRef,
    table_ref: &TableRef,
    column_refs: &[Vec<ColumnRef>],
    partition_column_refs: &Option<Vec<ColumnRef>>,
    name: &crate::TableName,
    columns: &[crate::Column],
    partition_columns: &Option<Vec<crate::PartitionColumn>>,
    path: &io::DucklakePath,
    tags: &Option<Vec<crate::Tag>>,
) -> DucklakeResult<()> {
    let table_id = state.table_id(*table_ref);

    // 1/4) Create the table
    let table = DucklakeTable {
        table_id,
        schema_id: state.schema_id(*schema_ref),
        begin_snapshot: state.snapshot_id(),
        end_snapshot: None,
        table_uuid: Some(db::UuidText::now_v7()),
        table_name: name.name.clone(),
        path: path.to_string(),
        path_is_relative: true,
    };
    tx.insert_entity(table).await?;

    // 2/4) Create the columns and, optionally, their tags
    let mut ducklake_columns = Vec::new();
    let mut column_tags = Vec::new();
    for (column, column_refs) in columns.iter().zip(column_refs.iter()) {
        add_column_to_buffers(
            state,
            table_id,
            &None,
            column_refs,
            column,
            &mut ducklake_columns,
            &mut column_tags,
        )?;
    }
    tx.insert_entities(ducklake_columns).await?;
    tx.insert_entities(column_tags).await?;

    // 3/4) Optionally create partition
    if let Some(partition_column_refs) = partition_column_refs
        && let Some(partition_columns) = partition_columns
    {
        create_partitioning(
            tx,
            state,
            table_ref,
            table_id,
            partition_column_refs,
            partition_columns,
        )
        .await?;
    }

    // 4/4) Optionally add tags to the table
    if let Some(tags) = tags
        && !tags.is_empty()
    {
        let ducklake_tags = tags.iter().map(|t| DucklakeTag {
            object_id: table_id,
            begin_snapshot: state.snapshot_id(),
            end_snapshot: None,
            key: t.key.clone(),
            value: t.value.clone(),
        });
        tx.insert_entities(ducklake_tags).await?;
    }

    Ok(())
}

pub async fn rename_table<'a>(
    tx: &mut db::Transaction,
    state: &mut CommitState<'a>,
    table_ref: &TableRef,
    name: &crate::TableName,
) -> DucklakeResult<()> {
    let table_id = state.table_id(*table_ref);

    // Set the current active record as deleted.
    set_end_snapshot!(ducklake_table, state, tx, conditions: { TableId => table_id });

    // "Copy" the previously active record, updating the name and the snapshot IDs.
    copy_row_with_updates!(
        ducklake_table, state, tx,
        conditions: { TableId => table_id },
        updates: { TableName => name.name.clone() }
    );

    Ok(())
}

pub async fn update_table_partitioning<'a>(
    tx: &mut db::Transaction,
    state: &mut CommitState<'a>,
    table_ref: &TableRef,
    partition_column_refs: &Option<Vec<ColumnRef>>,
    partition_columns: &Option<Vec<crate::PartitionColumn>>,
) -> DucklakeResult<()> {
    let table_id = state.table_id(*table_ref);

    // Set the current partitioning as deleted
    set_end_snapshot!(ducklake_partition_info, state, tx, conditions: { TableId => table_id });

    // Optionally apply the new partitioning
    if let Some(partition_column_refs) = partition_column_refs
        && let Some(partition_columns) = partition_columns
    {
        create_partitioning(
            tx,
            state,
            table_ref,
            table_id,
            partition_column_refs,
            partition_columns,
        )
        .await?;
    }

    Ok(())
}

pub async fn delete_table<'a>(
    tx: &mut db::Transaction,
    state: &mut CommitState<'a>,
    table_ref: &TableRef,
) -> DucklakeResult<()> {
    let table_id = state.table_id(*table_ref);

    set_end_snapshot!(ducklake_table, state, tx, conditions: { TableId => table_id });
    set_end_snapshot!(ducklake_column, state, tx, conditions: { TableId => table_id });
    set_end_snapshot!(ducklake_partition_info, state, tx, conditions: { TableId => table_id });
    set_end_snapshot!(ducklake_tag, state, tx, conditions: { ObjectId => table_id });
    set_end_snapshot!(ducklake_column_tag, state, tx, conditions: { TableId => table_id });
    set_end_snapshot!(ducklake_data_file, state, tx, conditions: { TableId => table_id });
    set_end_snapshot!(ducklake_delete_file, state, tx, conditions: { TableId => table_id });

    Ok(())
}

pub async fn add_table_tag<'a>(
    tx: &mut db::Transaction,
    state: &mut CommitState<'a>,
    table_ref: &TableRef,
    tag: &crate::Tag,
) -> DucklakeResult<()> {
    let table_id = state.table_id(*table_ref);

    // Delete any existing tag with the same key
    set_end_snapshot!(
        ducklake_tag, state, tx,
        conditions: { ObjectId => table_id, Key => &tag.key }
    );

    // Create the new tag
    let ducklake_tag = DucklakeTag {
        object_id: table_id,
        begin_snapshot: state.snapshot_id(),
        end_snapshot: None,
        key: tag.key.clone(),
        value: tag.value.clone(),
    };
    tx.insert_entity(ducklake_tag).await?;

    Ok(())
}

pub async fn remove_table_tag<'a>(
    tx: &mut db::Transaction,
    state: &mut CommitState<'a>,
    table_ref: &TableRef,
    key: &String,
) -> DucklakeResult<()> {
    let table_id = state.table_id(*table_ref);
    set_end_snapshot!(
        ducklake_tag, state, tx,
        conditions: { ObjectId => table_id, Key => key }
    );
    Ok(())
}

/* ------------------------------------------- UTILS ------------------------------------------- */

async fn create_partitioning<'a>(
    tx: &mut db::Transaction,
    state: &mut CommitState<'a>,
    table_ref: &TableRef,
    table_id: i64,
    partition_column_refs: &[ColumnRef],
    partition_columns: &[crate::PartitionColumn],
) -> DucklakeResult<()> {
    let partition_id = state.partition_id(*table_ref);
    let partition_info = DucklakePartitionInfo {
        partition_id,
        table_id,
        begin_snapshot: state.snapshot_id(),
        end_snapshot: None,
    };
    let partition_columns = partition_columns
        .iter()
        .enumerate()
        .zip(partition_column_refs.iter())
        .map(|((i, p), column_ref)| DucklakePartitionColumn {
            partition_id,
            table_id,
            partition_key_index: i as i64,
            column_id: state.column_id(*column_ref),
            transform: p.transform.to_string(),
        });

    tx.insert_entity(partition_info).await?;

    tx.insert_entities(partition_columns).await?;
    Ok(())
}

/* --------------------------------------------------------------------------------------------- */
/*                                             COLUMN                                            */
/* --------------------------------------------------------------------------------------------- */

pub async fn add_table_column(
    tx: &mut db::Transaction,
    state: &mut CommitState<'_>,
    parent_column_ref: &Option<ColumnRef>,
    column_refs: &[ColumnRef],
    column: &crate::Column,
) -> DucklakeResult<()> {
    let table_ref = column_refs[0].table_ref;
    let table_id = state.table_id(table_ref);

    // Create columns and tags
    let mut ducklake_columns = Vec::new();
    let mut ducklake_column_tags = Vec::new();
    add_column_to_buffers(
        state,
        table_id,
        parent_column_ref,
        column_refs,
        column,
        &mut ducklake_columns,
        &mut ducklake_column_tags,
    )?;

    tx.insert_entities(ducklake_columns).await?;
    tx.insert_entities(ducklake_column_tags).await?;

    // Optionally add tags
    if !column.tags.is_empty() {
        todo!()
    }

    Ok(())
}

pub async fn update_table_column<'a>(
    tx: &mut db::Transaction,
    state: &mut CommitState<'a>,
    parent_column_ref: &Option<ColumnRef>,
    column_ref: &ColumnRef,
    column: &crate::Column,
) -> DucklakeResult<()> {
    let table_id = state.table_id(column_ref.table_ref);
    let column_id = state.column_id(*column_ref);

    // Set the current active column as deleted
    set_end_snapshot!(
        ducklake_column, state, tx,
        conditions: { TableId => table_id, ColumnId => column_id }
    );

    // Create a new version of the column with the up-to-date information.
    // NOTE: We ignore updating tags here as there are separate functions for that. The vector
    //  is used as stub for calling the utility function.
    let mut ducklake_columns = Vec::new();
    let mut ducklake_column_tags = Vec::new();
    add_column_to_buffers(
        state,
        table_id,
        parent_column_ref,
        &[*column_ref],
        column,
        &mut ducklake_columns,
        &mut ducklake_column_tags,
    )?;

    tx.insert_entities(ducklake_columns).await?;

    Ok(())
}

pub async fn remove_table_column<'a>(
    tx: &mut db::Transaction,
    state: &mut CommitState<'a>,
    column_ref: &ColumnRef,
) -> DucklakeResult<()> {
    let table_id = state.table_id(column_ref.table_ref);
    let column_id = state.column_id(*column_ref);

    // Set the current active column as deleted
    set_end_snapshot!(
        ducklake_column, state, tx,
        conditions: { TableId => table_id, ColumnId => column_id }
    );

    Ok(())
}

pub async fn add_table_column_tag<'a>(
    tx: &mut db::Transaction,
    state: &mut CommitState<'a>,
    column_ref: &ColumnRef,
    tag: &crate::Tag,
) -> DucklakeResult<()> {
    let table_id = state.table_id(column_ref.table_ref);
    let column_id = state.column_id(*column_ref);

    // Delete any existing tag with the same key
    set_end_snapshot!(
        ducklake_column_tag, state, tx,
        conditions: { TableId => table_id, ColumnId => column_id, Key => &tag.key }
    );

    // Create the new tag
    let ducklake_column_tag = DucklakeColumnTag {
        table_id,
        column_id,
        begin_snapshot: state.snapshot_id(),
        end_snapshot: None,
        key: tag.key.clone(),
        value: tag.value.clone(),
    };
    tx.insert_entity(ducklake_column_tag).await?;

    Ok(())
}

pub async fn remove_table_column_tag<'a>(
    tx: &mut db::Transaction,
    state: &mut CommitState<'a>,
    column_ref: &ColumnRef,
    key: &String,
) -> DucklakeResult<()> {
    let table_id = state.table_id(column_ref.table_ref);
    let column_id = state.column_id(*column_ref);
    set_end_snapshot!(
        ducklake_column_tag, state, tx,
        conditions: { TableId => table_id, ColumnId => column_id, Key => key }
    );
    Ok(())
}

/* ------------------------------------------- UTILS ------------------------------------------- */

fn add_column_to_buffers(
    state: &mut CommitState<'_>,
    table_id: i64,
    parent_column_ref: &Option<ColumnRef>,
    column_refs: &[ColumnRef],
    column: &crate::Column,
    ducklake_columns: &mut Vec<DucklakeColumn>,
    ducklake_column_tags: &mut Vec<DucklakeColumnTag>,
) -> DucklakeResult<()> {
    let parent_column_id = parent_column_ref
        .as_ref()
        .map(|col_ref| state.column_id(*col_ref));
    let column_ids = column_refs
        .iter()
        .map(|column_ref| state.column_id(*column_ref))
        .collect_vec();

    for (i, flat_column) in column.flatten().into_iter().enumerate() {
        let column_id = column_ids[i];
        let (default_value, default_value_type, default_value_dialect) =
            to_default_value_columns(&flat_column.column.dtype, &flat_column.column.default_value);
        let ducklake_column = DucklakeColumn {
            column_id,
            table_id,
            begin_snapshot: state.snapshot_id(),
            end_snapshot: None,
            // NOTE: For simplicity, we simply assign the column ID for the order. This
            //  mirrors the behavior of the official DuckLake implementation as of v0.3.
            column_order: Some(column_id),
            column_name: flat_column.column.name,
            column_type: flat_column.column.dtype.to_string(),
            nulls_allowed: flat_column.column.nullable,
            // NOTE: It is fine to simply default to the parent column ID whenever the parent
            //  index is none because this only happens for the first flattened column.
            parent_column: flat_column
                .parent_index
                .map(|idx| column_ids[idx])
                .or(parent_column_id),
            initial_default: flat_column
                .column
                .initial_default
                .as_ref()
                .map(|v| v.to_string()),
            default_value,
            default_value_type,
            default_value_dialect,
        };
        ducklake_columns.push(ducklake_column);

        ducklake_column_tags.extend(flat_column.column.tags.into_iter().map(|t| {
            DucklakeColumnTag {
                table_id,
                column_id,
                begin_snapshot: state.snapshot_id(),
                end_snapshot: None,
                key: t.key,
                value: t.value,
            }
        }));
    }

    Ok(())
}

fn to_default_value_columns(
    dtype: &crate::DataType,
    default: &crate::ColumnDefault,
) -> (Option<String>, Option<String>, Option<String>) {
    match default {
        crate::ColumnDefault::Literal(v) => (
            Some(Value::to_string_opt(v.as_ref())),
            // NOTE: For some reason, nested dtypes have an empty string written by the DuckDB
            //  DuckLake extension
            if dtype.is_nested() {
                Some("".to_string())
            } else {
                Some("literal".to_string())
            },
            // NOTE: Literals are written with DuckDB syntax (this is what `Value` is using)
            Some("duckdb".to_string()),
        ),
        crate::ColumnDefault::Expression {
            dialect,
            expression,
        } => (
            Some(expression.clone()),
            Some("expression".to_string()),
            Some(dialect.clone()),
        ),
    }
}
