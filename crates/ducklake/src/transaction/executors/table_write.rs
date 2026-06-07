use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use sea_query::{ColumnDef, ExprTrait, Query, Table};

use crate::catalog::TableRef;
use crate::spec::*;
use crate::transaction::{CommitDataFile, CommitInlineData, CommitState};
use crate::{DucklakeResult, db};

/* ------------------------------------------- FILES ------------------------------------------- */

pub async fn write_table_data(
    tx: &mut db::Transaction,
    state: &mut CommitState<'_>,
    table_ref: &TableRef,
    data_files: &Vec<CommitDataFile>,
) -> DucklakeResult<()> {
    let table_id = state.table_id(*table_ref);

    // First, we iterate over all data files:
    //  - Collect the data files entries to add to the catalog
    //  - Collect the associated column stats entries to add to the catalog
    //  - Collect the partition values for each data file (if any)
    //  - Update the table and column stats stored in memory (added to the catalog database later).
    // NOTE: We also collect all column IDs to know which ones to sync to the catalog database
    //  later.
    let mut ducklake_data_files = Vec::with_capacity(data_files.len());
    let mut ducklake_partition_values = Vec::new();
    let mut ducklake_file_column_stats = Vec::with_capacity(data_files.len()); // surely too small
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
        ducklake_data_files.push(ducklake_data_file);

        if let Some(partition_values) = &data_file.partition_values {
            for (idx, value) in partition_values.iter().enumerate() {
                let ducklake_partition_value = DucklakeFilePartitionValue {
                    data_file_id: file_id,
                    table_id,
                    partition_key_index: idx as i64,
                    partition_value: value.as_ref().map(|v| v.to_string()),
                };
                ducklake_partition_values.push(ducklake_partition_value);
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
            ducklake_file_column_stats.push(ducklake_column_stat);
        }
    }
    tx.insert_entities(ducklake_data_files).await?;
    tx.insert_entities(ducklake_partition_values).await?;
    tx.insert_entities(ducklake_file_column_stats).await?;

    // After all data files have been added, we insert/update table and column stats
    persist_table_stats(tx, state, table_id).await?;
    persist_column_stats(tx, state, table_id, all_column_ids).await?;

    Ok(())
}

/* ---------------------------------------- INLINE DATA ---------------------------------------- */

pub async fn create_inlined_data_table(
    tx: &mut db::Transaction,
    state: &mut CommitState<'_>,
    table_ref: &TableRef,
) -> DucklakeResult<()> {
    // Data inlining is not supported for MySQL, so we simply skip in that case
    #[cfg(feature = "mysql")]
    if matches!(tx.dialect(), db::Dialect::MySql) {
        return Ok(());
    }

    let table_id = state.table_id(*table_ref);
    let schema = state.table_schema(*table_ref);
    let inlined_table_name = DucklakeInlinedData::table_name(table_id, state.schema_version());

    // Create the new table
    let query = {
        let dialect = tx.dialect();
        let mut table = Table::create();
        table
            .table(inlined_table_name.clone())
            .col(ColumnDef::new_with_type(
                ducklake_inlined_data::Column::RowId,
                dialect.column_type_i64(),
            ))
            .col(ColumnDef::new_with_type(
                ducklake_inlined_data::Column::BeginSnapshot,
                dialect.column_type_i64(),
            ))
            .col(ColumnDef::new_with_type(
                ducklake_inlined_data::Column::EndSnapshot,
                dialect.column_type_i64(),
            ));
        for (name, column) in &schema.columns {
            table.col(ColumnDef::new_with_type(
                name.clone(),
                tx.dialect().column_type_for_data_inlining(&column.dtype),
            ));
        }
        table.to_owned()
    };
    tx.execute(&query).await?;

    // Append the new table to the list of inlined data tables tracked for this table
    let inlined_data_table = DucklakeInlinedDataTables {
        table_id,
        table_name: inlined_table_name,
        schema_version: state.schema_version(),
    };
    tx.insert_entity(inlined_data_table).await?;

    Ok(())
}

pub async fn write_table_inline_data(
    tx: &mut db::Transaction,
    state: &mut CommitState<'_>,
    table_ref: &TableRef,
    inline_data: &Vec<CommitInlineData>,
) -> DucklakeResult<()> {
    #[cfg(feature = "mysql")]
    if matches!(tx.dialect(), db::Dialect::MySql) {
        unimplemented!("data inlining is not yet implemented for MySQL");
    }

    let table_id = state.table_id(*table_ref);

    // First, we need to fetch the name of the table that we need to insert into. The table is
    // guaranteed to exist and is simply the one with the latest schema version for this table ID.
    let query = Query::select()
        .column(ducklake_inlined_data_tables::Column::TableName)
        .from(ducklake_inlined_data_tables::Table)
        .and_where(
            ducklake_inlined_data_tables::Column::TableId
                .col()
                .eq(table_id),
        )
        .order_by(
            ducklake_inlined_data_tables::Column::SchemaVersion,
            sea_query::Order::Desc,
        )
        .limit(1)
        .to_owned();
    let (inlined_table_name,): (String,) = tx.fetch_one(&query).await?;

    // Then, we insert the data into the inlined table. As the input is Arrow, we want to insert
    // Arrow here. For this to work, we need to manually add `row_id`, `begin_snapshot`, and
    // `end_snapshot` columns.
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
        tx.insert_all_arrow(&inlined_table_name, record_batch)
            .await?;

        // Make sure we collect column IDs with stats for update later
        all_column_ids.extend(
            data.column_stats
                .keys()
                .map(|column_ref| state.column_id(*column_ref)),
        );
    }

    // After we've inserted all data, we need to update the stats
    persist_table_stats(tx, state, table_id).await?;
    persist_column_stats(tx, state, table_id, all_column_ids).await?;

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

/* ------------------------------------ TABLE STATS - WRITE ------------------------------------ */

async fn persist_table_stats(
    tx: &mut db::Transaction,
    state: &mut CommitState<'_>,
    table_id: i64,
) -> DucklakeResult<()> {
    let table_stats = state.table_stats(table_id).await?;
    if table_stats.is_persisted() {
        let query = Query::update()
            .table(ducklake_table_stats::Table)
            .values([
                (
                    ducklake_table_stats::Column::RecordCount,
                    table_stats.record_count().into(),
                ),
                (
                    ducklake_table_stats::Column::NextRowId,
                    table_stats.next_row_id().into(),
                ),
                (
                    ducklake_table_stats::Column::FileSizeBytes,
                    table_stats.file_size_bytes().into(),
                ),
            ])
            .and_where(ducklake_table_stats::Column::TableId.col().eq(table_id))
            .to_owned();
        tx.execute(&query).await?;
    } else {
        let entity = DucklakeTableStats {
            table_id,
            record_count: table_stats.record_count(),
            next_row_id: table_stats.next_row_id(),
            file_size_bytes: table_stats.file_size_bytes(),
        };
        tx.insert_entity(entity).await?;
        table_stats.set_persisted();
    }
    Ok(())
}

async fn persist_column_stats(
    tx: &mut db::Transaction,
    state: &mut CommitState<'_>,
    table_id: i64,
    all_column_ids: impl IntoIterator<Item = i64>,
) -> DucklakeResult<()> {
    let table_stats = state.table_stats(table_id).await?;
    for column_id in all_column_ids {
        let column_stats = table_stats.column_stats_mut(column_id);
        if column_stats.is_persisted() {
            let query = Query::update()
                .table(ducklake_table_column_stats::Table)
                .values([
                    (
                        ducklake_table_column_stats::Column::ContainsNull,
                        column_stats.contains_null().into(),
                    ),
                    (
                        ducklake_table_column_stats::Column::ContainsNan,
                        column_stats.contains_nan().into(),
                    ),
                    (
                        ducklake_table_column_stats::Column::MinValue,
                        column_stats.min_value().map(|v| v.to_string()).into(),
                    ),
                    (
                        ducklake_table_column_stats::Column::MaxValue,
                        column_stats.max_value().map(|v| v.to_string()).into(),
                    ),
                ])
                .and_where(
                    ducklake_table_column_stats::Column::TableId
                        .col()
                        .eq(table_id),
                )
                .and_where(
                    ducklake_table_column_stats::Column::ColumnId
                        .col()
                        .eq(column_id),
                )
                .to_owned();
            tx.execute(&query).await?;
        } else {
            let entity = DucklakeTableColumnStats {
                table_id,
                column_id,
                contains_null: column_stats.contains_null(),
                contains_nan: column_stats.contains_nan(),
                min_value: column_stats.min_value().map(|v| v.to_string()),
                max_value: column_stats.max_value().map(|v| v.to_string()),
                extra_stats: None, // TODO: Support extra stats
            };
            tx.insert_entity(entity).await?;
            column_stats.set_persisted();
        }
    }
    Ok(())
}
