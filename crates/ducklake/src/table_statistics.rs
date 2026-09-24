use std::collections::HashMap;

use sea_query::{Alias, Asterisk, Expr, ExprTrait, JoinType, Query, SelectStatement, UnionType};

use crate::ducklake::SnapshotAccess;
use crate::spec::*;
use crate::{Ducklake, DucklakeResult, Table, db};

/// Aggregate storage statistics for a table at one snapshot.
///
/// Counts come from catalog metadata; no data or delete files are opened.
#[derive(Debug, Clone, Default)]
pub struct TableStatistics {
    /// Current rows, including inline rows and excluding recorded deletions.
    pub num_rows: i64,
    /// Current rows stored inline in the catalog.
    pub num_inline_rows: i64,
    /// Recorded deletions for currently active data files.
    pub num_deleted_rows: i64,
    /// Number of active data files.
    pub num_data_files: i64,
    /// Total file size, or `None` if any file has an unknown size.
    pub total_file_size_bytes: Option<i64>,
    /// Smallest known file size, or `None` if no file size is known.
    pub min_file_size_bytes: Option<i64>,
    /// Largest known file size, or `None` if no file size is known.
    pub max_file_size_bytes: Option<i64>,
}

// Bound both bind parameters and compound SELECT terms (SQLite allows 500 terms).
const BATCH_SIZE: usize = 128;

impl Ducklake {
    /// List tables and their aggregate statistics at a single snapshot.
    pub async fn list_tables_with_statistics(
        &self,
        schema: Option<&str>,
    ) -> DucklakeResult<Vec<(Table, TableStatistics)>> {
        // Find all tables
        let snapshot = self.conn.snapshot(SnapshotAccess::Any).await?;
        let catalog = snapshot.catalog().await?;
        let tables = self.list_tables_from_catalog(catalog, schema)?;

        // Load their statistics
        let ids: Vec<_> = tables.iter().map(|table| table.id).collect();
        let mut statistics = HashMap::new();
        for batch in ids.chunks(BATCH_SIZE) {
            statistics.extend(load_statistics(self.conn.pool(), batch, snapshot.info().id).await?);
        }
        Ok(tables
            .into_iter()
            .map(|table| {
                let stats = statistics.remove(&table.id).unwrap();
                (table, stats)
            })
            .collect())
    }
}

// PostgreSQL SUM(bigint) produces NUMERIC; MySQL uses SIGNED for integer casts.
fn sum_i64(pool: &db::Pool, expression: Expr) -> Expr {
    let cast = match pool.dialect() {
        #[cfg(feature = "mysql")]
        db::Dialect::MySql => "SIGNED",
        #[allow(unreachable_patterns)]
        _ => "BIGINT",
    };
    expression.sum().cast_as(Alias::new(cast))
}

async fn load_statistics(
    pool: &db::Pool,
    ids: &[i64],
    snapshot_id: i64,
) -> DucklakeResult<HashMap<i64, TableStatistics>> {
    use ducklake_data_file::Column as File;
    use ducklake_delete_file::Column as Delete;

    let mut stats: HashMap<_, _> = ids
        .iter()
        .map(|id| {
            (
                *id,
                TableStatistics {
                    total_file_size_bytes: Some(0),
                    ..Default::default()
                },
            )
        })
        .collect();
    let files_query = Query::select()
        .column(File::TableId)
        .expr(Expr::col(Asterisk).count())
        .expr(File::FileSizeBytes.col().count())
        .expr(sum_i64(pool, File::RecordCount.col()))
        .expr(sum_i64(pool, File::FileSizeBytes.col()))
        .expr(File::FileSizeBytes.col().min())
        .expr(File::FileSizeBytes.col().max())
        .from(ducklake_data_file::Table)
        .and_where(File::TableId.col().is_in(ids.iter().copied()))
        .filter_for_snapshot(
            File::BeginSnapshot.col(),
            File::EndSnapshot.col(),
            snapshot_id,
        )
        .group_by_col(File::TableId)
        .to_owned();
    let deletes_query = Query::select()
        .column((ducklake_delete_file::Table, Delete::TableId))
        .expr(sum_i64(
            pool,
            Expr::col((ducklake_delete_file::Table, Delete::DeleteCount)),
        ))
        .from(ducklake_delete_file::Table)
        .join(
            JoinType::InnerJoin,
            ducklake_data_file::Table,
            Expr::col((ducklake_delete_file::Table, Delete::DataFileId))
                .equals((ducklake_data_file::Table, File::DataFileId)),
        )
        .and_where(
            Expr::col((ducklake_delete_file::Table, Delete::TableId)).is_in(ids.iter().copied()),
        )
        .filter_for_snapshot(
            Expr::col((ducklake_delete_file::Table, Delete::BeginSnapshot)),
            Expr::col((ducklake_delete_file::Table, Delete::EndSnapshot)),
            snapshot_id,
        )
        .filter_for_snapshot(
            Expr::col((ducklake_data_file::Table, File::BeginSnapshot)),
            Expr::col((ducklake_data_file::Table, File::EndSnapshot)),
            snapshot_id,
        )
        .group_by_col((ducklake_delete_file::Table, Delete::TableId))
        .to_owned();
    let inline_query = Query::select()
        .column(Asterisk)
        .from(ducklake_inlined_data_tables::Table)
        .and_where(
            ducklake_inlined_data_tables::Column::TableId
                .col()
                .is_in(ids.iter().copied()),
        )
        .to_owned();
    #[allow(clippy::type_complexity)]
    let (files, deletes, inline): (
        Vec<(i64, i64, i64, i64, Option<i64>, Option<i64>, Option<i64>)>,
        Vec<(i64, Option<i64>)>,
        Vec<DucklakeInlinedDataTables>,
    ) = tokio::try_join!(
        pool.fetch_all(&files_query),
        pool.fetch_all(&deletes_query),
        pool.fetch_all(&inline_query)
    )?;
    for (id, count, known_sizes, rows, size, min, max) in files {
        let entry = stats.get_mut(&id).unwrap();
        entry.num_data_files = count;
        entry.num_rows = rows;
        entry.total_file_size_bytes = if count == known_sizes { size } else { None };
        entry.min_file_size_bytes = min;
        entry.max_file_size_bytes = max;
    }
    for (id, count) in deletes {
        stats.get_mut(&id).unwrap().num_deleted_rows = count.unwrap_or_default();
    }
    let inline_counts: Vec<_> = inline
        .into_iter()
        .map(|table| {
            Query::select()
                .expr(Expr::val(table.table_id))
                .expr(Expr::col(Asterisk).count())
                .from(table.table_name)
                .filter_for_snapshot(
                    Expr::col("begin_snapshot"),
                    Expr::col("end_snapshot"),
                    snapshot_id,
                )
                .to_owned()
        })
        .collect();
    for (id, count) in fetch_counts(pool, inline_counts).await? {
        stats.get_mut(&id).unwrap().num_inline_rows += count;
    }
    // Inline-delete tables are optional. Discover their existence once per batch, instead of
    // issuing one potentially failing SELECT (or an existence check) for every table.
    if stats.values().any(|entry| entry.num_data_files > 0) {
        let existing = pool.table_names().await?;
        let inline_deletes: Vec<_> = ids
            .iter()
            .filter_map(|id| {
                let name = DucklakeInlinedDelete::table_name(*id);
                if stats[id].num_data_files == 0 || !existing.contains(&name) {
                    return None;
                }
                Some(
                    Query::select()
                        .expr(Expr::val(*id))
                        .expr(Expr::col(Asterisk).count())
                        .from_as(name, Alias::new("deletes"))
                        .join(
                            JoinType::InnerJoin,
                            ducklake_data_file::Table,
                            Expr::col((Alias::new("deletes"), Alias::new("file_id")))
                                .equals((ducklake_data_file::Table, File::DataFileId)),
                        )
                        .and_where(
                            Expr::col((Alias::new("deletes"), Alias::new("begin_snapshot")))
                                .lte(snapshot_id),
                        )
                        .and_where(Expr::col((ducklake_data_file::Table, File::TableId)).eq(*id))
                        .filter_for_snapshot(
                            Expr::col((ducklake_data_file::Table, File::BeginSnapshot)),
                            Expr::col((ducklake_data_file::Table, File::EndSnapshot)),
                            snapshot_id,
                        )
                        .to_owned(),
                )
            })
            .collect();
        for (id, count) in fetch_counts(pool, inline_deletes).await? {
            stats.get_mut(&id).unwrap().num_deleted_rows += count;
        }
    }
    for entry in stats.values_mut() {
        entry.num_rows += entry.num_inline_rows - entry.num_deleted_rows;
    }
    Ok(stats)
}

async fn fetch_counts(
    pool: &db::Pool,
    queries: Vec<SelectStatement>,
) -> DucklakeResult<Vec<(i64, i64)>> {
    let mut counts = Vec::new();
    for batch in queries.chunks(BATCH_SIZE) {
        let mut query = batch[0].clone();
        for other in &batch[1..] {
            query.union(UnionType::All, other.clone());
        }
        counts.extend(pool.fetch_all::<(i64, i64)>(&query).await?);
    }
    Ok(counts)
}
