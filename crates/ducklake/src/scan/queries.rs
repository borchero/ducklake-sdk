use sea_query::{Asterisk, Expr, ExprTrait, JoinType, Order, Query, SelectStatement};

use crate::spec::*;

pub(super) fn build_data_files_query(table_id: i64, snapshot_id: i64) -> SelectStatement {
    Query::select()
        .column(Asterisk)
        .from(ducklake_data_file::Table)
        .and_where(ducklake_data_file::Column::TableId.col().eq(table_id))
        .filter_for_snapshot(
            ducklake_data_file::Column::BeginSnapshot.col(),
            ducklake_data_file::Column::EndSnapshot.col(),
            snapshot_id,
        )
        .to_owned()
}

pub(super) fn build_column_stats_query(table_id: i64, snapshot_id: i64) -> SelectStatement {
    Query::select()
        .column((ducklake_file_column_stats::Table, Asterisk))
        .from(ducklake_file_column_stats::Table)
        .join(
            JoinType::InnerJoin,
            ducklake_data_file::Table,
            Expr::col((
                ducklake_file_column_stats::Table,
                ducklake_file_column_stats::Column::DataFileId,
            ))
            .equals((
                ducklake_data_file::Table,
                ducklake_data_file::Column::DataFileId,
            )),
        )
        .and_where(
            Expr::col((
                ducklake_file_column_stats::Table,
                ducklake_file_column_stats::Column::TableId,
            ))
            .eq(table_id),
        )
        .filter_for_snapshot(
            ducklake_data_file::Column::BeginSnapshot.col(),
            ducklake_data_file::Column::EndSnapshot.col(),
            snapshot_id,
        )
        .to_owned()
}

pub(super) fn build_delete_files_query(table_id: i64, snapshot_id: i64) -> SelectStatement {
    Query::select()
        .column(Asterisk)
        .from(ducklake_delete_file::Table)
        .and_where(ducklake_delete_file::Column::TableId.col().eq(table_id))
        .filter_for_snapshot(
            ducklake_delete_file::Column::BeginSnapshot.col(),
            ducklake_delete_file::Column::EndSnapshot.col(),
            snapshot_id,
        )
        .to_owned()
}

pub(super) fn build_inlined_data_tables_query(table_id: i64) -> SelectStatement {
    Query::select()
        .column(Asterisk)
        .from(ducklake_inlined_data_tables::Table)
        .and_where(
            ducklake_inlined_data_tables::Column::TableId
                .col()
                .eq(table_id),
        )
        .to_owned()
}

pub(super) fn build_inlined_data_query<'a>(
    table_name: &str,
    columns: impl Iterator<Item = &'a String>,
    snapshot_id: i64,
) -> SelectStatement {
    Query::select()
        .columns(columns.map(|c| c.to_string()).collect::<Vec<_>>())
        .from(table_name.to_string())
        .filter_for_snapshot(
            Expr::col("begin_snapshot"),
            Expr::col("end_snapshot"),
            snapshot_id,
        )
        .order_by("row_id", Order::Asc)
        .to_owned()
}

pub(super) fn build_inlined_deletes_query(table_id: i64, snapshot_id: i64) -> SelectStatement {
    Query::select()
        .column(Asterisk)
        .from(DucklakeInlinedDelete::table_name(table_id))
        .and_where(
            ducklake_inlined_delete::Column::BeginSnapshot
                .col()
                .lte(snapshot_id),
        )
        .to_owned()
}
