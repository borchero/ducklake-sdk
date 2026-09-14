use itertools::Itertools;

use crate::catalog::{ColumnRef, SchemaRef, TableRef};
use crate::spec::*;
use crate::transaction::transaction_changes::TagChange;
use crate::transaction::{CommitState, TransactionChanges};
use crate::{DucklakeResult, Value, db, io};

/* --------------------------------------------------------------------------------------------- */
/*                                             TABLE                                             */
/* --------------------------------------------------------------------------------------------- */

#[allow(clippy::too_many_arguments)]
pub(crate) fn create_table<'a>(
    changes: &mut TransactionChanges,
    state: &mut CommitState<'a>,
    schema_ref: &SchemaRef,
    table_ref: &TableRef,
    column_refs: &[Vec<ColumnRef>],
    partition_column_refs: &Option<Vec<ColumnRef>>,
    name: &crate::TableName,
    columns: &[crate::Column],
    retired_columns: &[DucklakeColumn],
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
    changes.new_tables.push(table);

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
    // Retain dropped field IDs so future columns cannot reuse IDs still present in transferred
    // Parquet files. An empty snapshot interval keeps these columns out of every target schema.
    ducklake_columns.extend(retired_columns.iter().cloned().map(|mut column| {
        column.table_id = table_id;
        column.begin_snapshot = state.snapshot_id();
        column.end_snapshot = Some(state.snapshot_id());
        column
    }));
    changes.new_columns.extend(ducklake_columns);
    changes.new_column_tags.extend(column_tags);

    // 3/4) Optionally create partition
    if let Some(partition_column_refs) = partition_column_refs
        && let Some(partition_columns) = partition_columns
    {
        create_partitioning(
            changes,
            state,
            table_ref,
            table_id,
            partition_column_refs,
            partition_columns,
        )?;
    }

    // 4/4) Optionally add tags to the table
    if let Some(tags) = tags
        && !tags.is_empty()
    {
        let snapshot_id = state.snapshot_id();
        let ducklake_tags = tags.iter().map(|t| DucklakeTag {
            object_id: table_id,
            begin_snapshot: snapshot_id,
            end_snapshot: None,
            key: t.key.clone(),
            value: t.value.clone(),
        });
        changes.new_tags.extend(ducklake_tags);
    }

    Ok(())
}

pub(crate) fn rename_table<'a>(
    changes: &mut TransactionChanges,
    state: &mut CommitState<'a>,
    table_ref: &TableRef,
    name: &crate::TableName,
) -> DucklakeResult<()> {
    let table_id = state.table_id(*table_ref);

    if let Some(table) = changes
        .new_tables
        .iter_mut()
        .find(|t| t.table_id == table_id)
    {
        table.table_name = name.name.clone();
    } else {
        changes.renamed_tables.insert(table_id, name.name.clone());
    }

    Ok(())
}

pub(crate) fn update_table_partitioning<'a>(
    changes: &mut TransactionChanges,
    state: &mut CommitState<'a>,
    table_ref: &TableRef,
    partition_column_refs: &Option<Vec<ColumnRef>>,
    partition_columns: &Option<Vec<crate::PartitionColumn>>,
) -> DucklakeResult<()> {
    let table_id = state.table_id(*table_ref);

    // Set the current partitioning as deleted
    changes.retired_partition_tables.insert(table_id);
    for partition in &mut changes.new_partition_info {
        if partition.table_id == table_id {
            partition.end_snapshot = Some(state.snapshot_id());
        }
    }

    // Optionally apply the new partitioning
    if let Some(partition_column_refs) = partition_column_refs
        && let Some(partition_columns) = partition_columns
    {
        create_partitioning(
            changes,
            state,
            table_ref,
            table_id,
            partition_column_refs,
            partition_columns,
        )?;
    }

    Ok(())
}

pub(crate) fn delete_table<'a>(
    changes: &mut TransactionChanges,
    state: &mut CommitState<'a>,
    table_ref: &TableRef,
    detach_files: bool,
) -> DucklakeResult<()> {
    let table_id = state.table_id(*table_ref);

    changes.dropped_tables.insert(table_id);
    if detach_files {
        changes.detached_tables.insert(table_id);
    }

    Ok(())
}

pub(crate) fn add_table_tag<'a>(
    changes: &mut TransactionChanges,
    state: &mut CommitState<'a>,
    table_ref: &TableRef,
    tag: &crate::Tag,
) -> DucklakeResult<()> {
    let table_id = state.table_id(*table_ref);
    changes
        .table_tags
        .entry(table_id)
        .or_default()
        .push(TagChange {
            key: tag.key.clone(),
            value: Some(tag.value.clone()),
        });

    Ok(())
}

pub(crate) fn remove_table_tag<'a>(
    changes: &mut TransactionChanges,
    state: &mut CommitState<'a>,
    table_ref: &TableRef,
    key: &str,
) -> DucklakeResult<()> {
    let table_id = state.table_id(*table_ref);
    changes
        .table_tags
        .entry(table_id)
        .or_default()
        .push(TagChange {
            key: key.to_owned(),
            value: None,
        });

    Ok(())
}

/* ------------------------------------------- UTILS ------------------------------------------- */

fn create_partitioning<'a>(
    changes: &mut TransactionChanges,
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
        })
        .collect_vec();

    changes.new_partition_info.push(partition_info);

    changes.new_partition_columns.extend(partition_columns);
    Ok(())
}

/* --------------------------------------------------------------------------------------------- */
/*                                             COLUMN                                            */
/* --------------------------------------------------------------------------------------------- */

pub(crate) fn add_table_column(
    changes: &mut TransactionChanges,
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

    changes.new_columns.extend(ducklake_columns);
    changes.new_column_tags.extend(ducklake_column_tags);

    // Optionally add tags
    if !column.tags.is_empty() {
        todo!()
    }

    Ok(())
}

pub(crate) fn update_table_column<'a>(
    changes: &mut TransactionChanges,
    state: &mut CommitState<'a>,
    parent_column_ref: &Option<ColumnRef>,
    column_ref: &ColumnRef,
    column: &crate::Column,
) -> DucklakeResult<()> {
    let table_id = state.table_id(column_ref.table_ref);
    let column_id = state.column_id(*column_ref);

    changes.retired_columns.insert((table_id, column_id));
    // A column created earlier in this commit only needs its final definition.
    changes
        .new_columns
        .retain(|c| c.table_id != table_id || c.column_id != column_id);

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

    changes.new_columns.extend(ducklake_columns);

    Ok(())
}

pub(crate) fn remove_table_column<'a>(
    changes: &mut TransactionChanges,
    state: &mut CommitState<'a>,
    column_ref: &ColumnRef,
) -> DucklakeResult<()> {
    let table_id = state.table_id(column_ref.table_ref);
    let column_id = state.column_id(*column_ref);

    changes.retired_columns.insert((table_id, column_id));
    // Retain the field ID even if the column was added and removed in this commit.
    for column in &mut changes.new_columns {
        if column.table_id == table_id && column.column_id == column_id {
            column.end_snapshot = Some(state.snapshot_id());
        }
    }

    Ok(())
}

pub(crate) fn add_table_column_tag<'a>(
    changes: &mut TransactionChanges,
    state: &mut CommitState<'a>,
    column_ref: &ColumnRef,
    tag: &crate::Tag,
) -> DucklakeResult<()> {
    let table_id = state.table_id(column_ref.table_ref);
    let column_id = state.column_id(*column_ref);
    changes
        .column_tags
        .entry((table_id, column_id))
        .or_default()
        .push(TagChange {
            key: tag.key.clone(),
            value: Some(tag.value.clone()),
        });

    Ok(())
}

pub(crate) fn remove_table_column_tag<'a>(
    changes: &mut TransactionChanges,
    state: &mut CommitState<'a>,
    column_ref: &ColumnRef,
    key: &str,
) -> DucklakeResult<()> {
    let table_id = state.table_id(column_ref.table_ref);
    let column_id = state.column_id(*column_ref);
    changes
        .column_tags
        .entry((table_id, column_id))
        .or_default()
        .push(TagChange {
            key: key.to_owned(),
            value: None,
        });

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
