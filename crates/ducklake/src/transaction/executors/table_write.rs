use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};

use crate::DucklakeResult;
use crate::catalog::TableRef;
use crate::spec::*;
use crate::transaction::{CommitDataFile, CommitInlineData, CommitState, TransactionChanges};

/* ------------------------------------------- FILES ------------------------------------------- */

pub(crate) async fn write_table_data(
    changes: &mut TransactionChanges,
    state: &mut CommitState<'_>,
    table_ref: &TableRef,
    data_files: &Vec<CommitDataFile>,
) -> DucklakeResult<()> {
    let table_id = state.table_id(*table_ref);

    // Accumulate metadata by catalog relation across every table write in the commit.
    let files = &mut changes.files;
    let mut all_column_ids = HashSet::new();
    for data_file in data_files {
        let file_id = state.file_id();
        let row_id_start = {
            let start = state.table_stats(table_id).await?.next_row_id();
            update_table_stats_from_file(state, table_id, data_file).await?;
            start
        };
        let ducklake_data_file = DucklakeDataFile {
            data_file_id: file_id,
            table_id,
            begin_snapshot: state.snapshot_id(),
            end_snapshot: None,
            file_order: Some(file_id),
            path: data_file.path.to_string(),
            path_is_relative: data_file.path.is_relative(),
            file_format: "parquet".to_string(),
            record_count: data_file.num_rows as i64,
            file_size_bytes: data_file.file_size_bytes.map(|s| s as i64),
            footer_size: data_file.footer_size_bytes.map(|s| s as i64),
            row_id_start: Some(row_id_start),
            partition_id: data_file
                .partition_values
                .as_ref()
                .map(|_| state.partition_id(*table_ref)),
            encryption_key: None, // TODO: Implement encryption
            mapping_id: None,
            partial_max: None,
        };
        files.data_files.push(ducklake_data_file);

        for delete_file in &data_file.delete_files {
            files.delete_files.push(DucklakeDeleteFile {
                delete_file_id: state.file_id(),
                table_id,
                begin_snapshot: state.snapshot_id(),
                end_snapshot: None,
                data_file_id: file_id,
                path: delete_file.path.to_string(),
                path_is_relative: delete_file.path.is_relative(),
                format: "parquet".to_string(),
                delete_count: Some(delete_file.num_deletes as i64),
                file_size_bytes: delete_file.file_size_bytes.map(|size| size as i64),
                footer_size: delete_file.footer_size_bytes.map(|size| size as i64),
                encryption_key: None,
                partial_max: None,
            });
        }

        if !data_file.inline_deletes.is_empty() {
            files.inline_deletes.entry(table_id).or_default().extend(
                data_file
                    .inline_deletes
                    .iter()
                    .map(|row_id| DucklakeInlinedDelete {
                        file_id,
                        row_id: *row_id,
                        begin_snapshot: state.snapshot_id(),
                    }),
            );
        }

        if let Some(partition_values) = &data_file.partition_values {
            for (idx, value) in partition_values.iter().enumerate() {
                let ducklake_partition_value = DucklakeFilePartitionValue {
                    data_file_id: file_id,
                    table_id,
                    partition_key_index: idx as i64,
                    partition_value: value.clone(),
                };
                files.partition_values.push(ducklake_partition_value);
            }
        }

        for (column_ref, stats) in &data_file.column_stats {
            let column_id = state.column_id(*column_ref);
            all_column_ids.insert(column_id);
            let ducklake_column_stat = DucklakeFileColumnStats {
                data_file_id: file_id,
                table_id,
                column_id,
                column_size_bytes: stats.size_bytes.map(|s| s as i64),
                value_count: None, // TODO: Populate this by updating DataFile
                null_count: stats.null_count.map(|c| c as i64),
                min_value: stats.min_value.as_ref().map(|v| v.to_string()),
                max_value: stats.max_value.as_ref().map(|v| v.to_string()),
                contains_nan: stats.contains_nan,
                extra_stats: None, // TODO: Support extra stats
            };
            files.column_stats.push(ducklake_column_stat);
        }
    }
    // Record which table and column statistics need to be persisted.
    changes
        .written_columns
        .entry(table_id)
        .or_default()
        .extend(all_column_ids);

    Ok(())
}

/* ---------------------------------------- INLINE DATA ---------------------------------------- */

pub(crate) fn create_inlined_data_table(
    changes: &mut TransactionChanges,
    state: &mut CommitState<'_>,
    table_ref: &TableRef,
) {
    let table_id = state.table_id(*table_ref);
    changes
        .inline_tables
        .insert(table_id, state.table_schema(*table_ref));
}

pub(crate) async fn write_table_inline_data(
    changes: &mut TransactionChanges,
    state: &mut CommitState<'_>,
    table_ref: &TableRef,
    inline_data: &Vec<CommitInlineData>,
) -> DucklakeResult<()> {
    let table_id = state.table_id(*table_ref);
    // Assign row IDs while collecting; persistence can then group writes by table.
    let snapshot_id = state.snapshot_id();
    let mut all_column_ids = HashSet::new();
    for data in inline_data {
        let table_stats = state.table_stats(table_id).await?;
        let num_rows = data.record_batch.num_rows();
        let row_ids = {
            let row_ids = table_stats.next_row_id()..(table_stats.next_row_id() + num_rows as i64);
            update_table_stats_from_inline_data(state, table_id, data).await?;
            row_ids
        };
        let begin_snapshot = std::iter::repeat_n(snapshot_id, num_rows);
        let end_snapshot = std::iter::repeat_n(Option::<i64>::None, num_rows);

        let mut fields = data.record_batch.schema().fields().to_vec();
        fields.push(Arc::new(ArrowField::new(
            "row_id",
            ArrowDataType::Int64,
            true,
        )));
        fields.push(Arc::new(ArrowField::new(
            "begin_snapshot",
            ArrowDataType::Int64,
            true,
        )));
        fields.push(Arc::new(ArrowField::new(
            "end_snapshot",
            ArrowDataType::Int64,
            true,
        )));
        let new_schema = Arc::new(ArrowSchema::new(fields));

        let mut columns = data.record_batch.columns().to_vec();
        columns.push(Arc::new(arrow_array::Int64Array::from_iter(row_ids)));
        columns.push(Arc::new(arrow_array::Int64Array::from_iter(begin_snapshot)));
        columns.push(Arc::new(arrow_array::Int64Array::from_iter(end_snapshot)));

        let record_batch = RecordBatch::try_new(new_schema, columns)?;
        changes
            .inline_data
            .entry(table_id)
            .or_default()
            .push(record_batch);

        // Make sure we collect column IDs with stats for update later
        all_column_ids.extend(
            data.column_stats
                .keys()
                .map(|column_ref| state.column_id(*column_ref)),
        );
    }

    // Record which table and column statistics need to be persisted.
    changes
        .written_columns
        .entry(table_id)
        .or_default()
        .extend(all_column_ids);

    Ok(())
}

/* --------------------------------------------------------------------------------------------- */
/*                                             UTILS                                             */
/* --------------------------------------------------------------------------------------------- */

/* ------------------------------------ TABLE STATS - UPDATE ----------------------------------- */

async fn update_table_stats_from_file(
    state: &mut CommitState<'_>,
    table_id: i64,
    data_file: &CommitDataFile,
) -> DucklakeResult<()> {
    let stats = state.table_stats(table_id).await?;
    stats.advance_row_id(data_file.num_rows as i64);
    stats.add_record_count(data_file.num_rows as i64);
    stats.add_file_size_bytes(data_file.file_size_bytes.map(|s| s as i64));

    for (column_ref, column_stats) in data_file.column_stats.iter() {
        let column_id = state.column_id(*column_ref);
        let stats = state
            .table_stats(table_id)
            .await?
            .column_stats_mut(column_id);

        stats.update_contains_null(column_stats.null_count.map(|c| c > 0));
        stats.update_contains_nan(column_stats.contains_nan);
        stats.update_min_value(column_stats.min_value.as_ref());
        stats.update_max_value(column_stats.max_value.as_ref());
    }
    Ok(())
}

async fn update_table_stats_from_inline_data(
    state: &mut CommitState<'_>,
    table_id: i64,
    inline_data: &CommitInlineData,
) -> DucklakeResult<()> {
    let stats = state.table_stats(table_id).await?;
    stats.advance_row_id(inline_data.record_batch.num_rows() as i64);
    stats.add_record_count(inline_data.record_batch.num_rows() as i64);

    for (column_ref, column_stats) in inline_data.column_stats.iter() {
        let column_id = state.column_id(*column_ref);
        let stats = state
            .table_stats(table_id)
            .await?
            .column_stats_mut(column_id);

        stats.update_contains_null(column_stats.null_count.map(|c| c > 0));
        stats.update_contains_nan(column_stats.contains_nan);
        stats.update_min_value(column_stats.min_value.as_ref());
        stats.update_max_value(column_stats.max_value.as_ref());
    }
    Ok(())
}
