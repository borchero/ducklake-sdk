/// Sets `end_snapshot` to the current snapshot ID for rows matching the given conditions.
/// This marks records as "deleted" (soft delete) for snapshot-based versioning.
///
/// # Arguments
/// * `$table` - The table module (e.g., `ducklake_table`)
/// * `$state` - The commit state (provides `snapshot_id()`)
/// * `$tx` - The database transaction
/// * `conditions` - Column conditions to identify rows to update
#[macro_export]
macro_rules! set_end_snapshot {
    (
        $table:ident, $state:expr, $tx:expr,
        conditions: { $($cond_col:ident => $cond_val:expr),* $(,)? }
    ) => {{
        let query = Query::update()
            .table($table::Table)
            .value($table::Column::EndSnapshot, $state.snapshot_id())
            $(.and_where($table::Column::$cond_col.col().eq($cond_val)))*
            .and_where($table::Column::EndSnapshot.col().is_null())
            .to_owned();
        $tx.execute(&query).await?;
    }};
}

/// Copies a row from a table, replacing specific columns with new values.
/// This is the standard pattern for "soft updates" where we version rows via snapshots.
///
/// The macro automatically:
/// - Sets `BeginSnapshot` to the current snapshot ID
/// - Sets `EndSnapshot` to `None`
/// - Keeps all other columns unchanged unless specified in `updates`
///
/// # Arguments
/// * `$table` - The table module (e.g., `ducklake_table`)
/// * `$state` - The commit state (provides `snapshot_id()`)
/// * `$tx` - The database transaction
/// * `conditions` - Column conditions to identify the source row (uses `end_snapshot = current`)
/// * `updates` - Columns to replace with new values
macro_rules! copy_row_with_updates {
    (
        $table:ident, $state:expr, $tx:expr,
        conditions: { $($cond_col:ident => $cond_val:expr),* $(,)? },
        updates: { $($upd_col:ident => $upd_val:expr),* $(,)? }
    ) => {{
        let columns = $table::Column::iter().collect_vec();
        let exprs = columns.iter().map(|col| {
            match col {
                $table::Column::BeginSnapshot => Expr::val($state.snapshot_id()),
                $table::Column::EndSnapshot => Expr::val(None::<i64>),
                $($table::Column::$upd_col => Expr::val($upd_val.clone()),)*
                _ => col.col(),
            }
        }).collect_vec();

        let select = Query::select()
            .exprs(exprs)
            .from($table::Table)
            $(.and_where($table::Column::$cond_col.col().eq($cond_val)))*
            .and_where($table::Column::EndSnapshot.col().eq($state.snapshot_id()))
            .to_owned();

        let query = Query::insert()
            .into_table($table::Table)
            .columns(columns)
            .select_from(select)
            .unwrap()
            .to_owned();
        $tx.execute(&query).await?;
    }};
}

mod schema;
mod table_meta;
mod table_write;

pub(super) use schema::*;
pub(super) use table_meta::*;
pub(super) use table_write::*;
