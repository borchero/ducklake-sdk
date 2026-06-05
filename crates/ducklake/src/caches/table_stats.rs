use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use sea_query::{Asterisk, Expr, ExprTrait, JoinType, Query, SelectStatement, all};

use crate::catalog::Catalog;
use crate::spec::*;
use crate::{DucklakeResult, db};

/* --------------------------------------------------------------------------------------------- */
/*                                             CACHE                                             */
/* --------------------------------------------------------------------------------------------- */

#[derive(Clone)]
pub struct TableStatsCache {
    pool: db::Pool,
    table_stats: Arc<RwLock<HashMap<i64, SnapshotTableStats>>>,
}

impl TableStatsCache {
    pub fn new(pool: db::Pool) -> Self {
        Self {
            pool,
            table_stats: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get(
        &self,
        snapshot_id: i64,
        next_file_id: i64,
        catalog: &Catalog,
    ) -> DucklakeResult<Arc<HashMap<i64, TableStats>>> {
        if let Some(stats) = self.table_stats.read().unwrap().get(&next_file_id) {
            Ok(stats.0.clone())
        } else {
            let stats = SnapshotTableStats::load(&self.pool, catalog, snapshot_id)
                .await?
                .0;
            self.table_stats
                .write()
                .unwrap()
                .insert(next_file_id, SnapshotTableStats(stats.clone()));
            Ok(stats)
        }
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                             STATS                                             */
/* --------------------------------------------------------------------------------------------- */

#[repr(transparent)]
struct SnapshotTableStats(Arc<HashMap<i64, TableStats>>);

#[derive(Debug, Clone)]
pub struct TableStats {
    next_row_id: i64,
    record_count: Option<i64>,
    file_size_bytes: Option<i64>,
    column_stats: HashMap<i64, ColumnStats>,

    /// Whether the table stats have been persisted to the database. This is used to determine
    /// whether to insert or update upon changes.
    is_persisted: bool,
}

#[derive(Debug, Clone)]
pub struct ColumnStats {
    contains_null: Option<bool>,
    contains_nan: Option<bool>,
    min_value: Option<crate::Value>,
    max_value: Option<crate::Value>,
    // TODO: Add support for `extra_stats`
    /// Whether the column stats have been persisted to the database. This is used to determine
    /// whether to insert or update upon changes.
    is_persisted: bool,
}

/* ------------------------------------------ DEFAULT ------------------------------------------ */

impl Default for TableStats {
    fn default() -> Self {
        Self {
            next_row_id: 0,
            record_count: Some(0),
            file_size_bytes: Some(0),
            column_stats: HashMap::new(),
            is_persisted: false,
        }
    }
}

impl Default for ColumnStats {
    fn default() -> Self {
        Self {
            contains_null: Some(false),
            contains_nan: Some(false),
            min_value: None,
            max_value: None,
            is_persisted: false,
        }
    }
}

/* -------------------------------------------- LOAD ------------------------------------------- */

impl SnapshotTableStats {
    async fn load(pool: &db::Pool, catalog: &Catalog, snapshot_id: i64) -> DucklakeResult<Self> {
        // Build queries for all active tables and active columns
        let table_stats_query = Self::build_table_stats_query(snapshot_id);
        let column_stats_query = Self::build_column_stats_query(snapshot_id);

        // Execute queries in parallel
        let (table_stats, column_stats): (Vec<DucklakeTableStats>, Vec<DucklakeTableColumnStats>) =
            tokio::try_join!(
                pool.fetch_all(&table_stats_query),
                pool.fetch_all(&column_stats_query)
            )?;

        // Build stats hashmap for tables
        let mut table_stats_map: HashMap<i64, TableStats> = table_stats
            .into_iter()
            .map(|stat| {
                (
                    stat.table_id,
                    TableStats {
                        next_row_id: stat.next_row_id,
                        record_count: stat.record_count,
                        file_size_bytes: stat.file_size_bytes,
                        column_stats: HashMap::new(),
                        is_persisted: true,
                    },
                )
            })
            .collect();

        // Populate column stats for each table
        let column_dtypes: HashMap<_, _> = table_stats_map
            .keys()
            .map(|id| catalog.table(*id).map(|t| (*id, t.column_data_types())))
            .collect::<DucklakeResult<_>>()?;
        for stat in column_stats {
            let dtype = column_dtypes
                .get(&stat.table_id)
                .and_then(|d| d.get(&stat.column_id))
                .expect("column dtype not found for column in statistics");
            let stats = ColumnStats {
                contains_null: stat.contains_null,
                contains_nan: stat.contains_nan,
                min_value: stat
                    .min_value
                    .and_then(|v| crate::Value::parse(dtype, &v).transpose())
                    .transpose()?,
                max_value: stat
                    .max_value
                    .and_then(|v| crate::Value::parse(dtype, &v).transpose())
                    .transpose()?,
                is_persisted: true,
            };
            table_stats_map
                .get_mut(&stat.table_id)
                .unwrap()
                .column_stats
                .insert(stat.column_id, stats);
        }

        Ok(Self(Arc::new(table_stats_map)))
    }

    fn build_table_stats_query(snapshot_id: i64) -> SelectStatement {
        Query::select()
            .column((ducklake_table_stats::Table, Asterisk))
            .from(ducklake_table_stats::Table)
            .join(
                JoinType::InnerJoin,
                ducklake_table::Table,
                Expr::col((
                    ducklake_table_stats::Table,
                    ducklake_table_stats::Column::TableId,
                ))
                .equals((ducklake_table::Table, ducklake_table::Column::TableId)),
            )
            .filter_for_snapshot(
                Expr::col((ducklake_table::Table, ducklake_table::Column::BeginSnapshot)),
                Expr::col((ducklake_table::Table, ducklake_table::Column::EndSnapshot)),
                snapshot_id,
            )
            .to_owned()
    }

    fn build_column_stats_query(snapshot_id: i64) -> SelectStatement {
        Query::select()
            .column((ducklake_table_column_stats::Table, Asterisk))
            .from(ducklake_table_column_stats::Table)
            .join(
                JoinType::InnerJoin,
                ducklake_column::Table,
                all![
                    Expr::col((
                        ducklake_table_column_stats::Table,
                        ducklake_table_column_stats::Column::TableId,
                    ))
                    .equals((ducklake_column::Table, ducklake_column::Column::TableId)),
                    Expr::col((
                        ducklake_table_column_stats::Table,
                        ducklake_table_column_stats::Column::ColumnId,
                    ))
                    .equals((ducklake_column::Table, ducklake_column::Column::ColumnId))
                ],
            )
            .filter_for_snapshot(
                Expr::col((
                    ducklake_column::Table,
                    ducklake_column::Column::BeginSnapshot,
                )),
                Expr::col((ducklake_column::Table, ducklake_column::Column::EndSnapshot)),
                snapshot_id,
            )
            .to_owned()
    }
}

/* ----------------------------------------- ACCESSORS ----------------------------------------- */

impl TableStats {
    pub fn next_row_id(&self) -> i64 {
        self.next_row_id
    }

    pub fn record_count(&self) -> Option<i64> {
        self.record_count
    }

    pub fn file_size_bytes(&self) -> Option<i64> {
        self.file_size_bytes
    }

    pub fn is_persisted(&self) -> bool {
        self.is_persisted
    }

    pub fn column_stats(&self, column_id: i64) -> Option<&ColumnStats> {
        self.column_stats.get(&column_id)
    }
}

impl ColumnStats {
    pub fn contains_null(&self) -> Option<bool> {
        self.contains_null
    }

    pub fn contains_nan(&self) -> Option<bool> {
        self.contains_nan
    }

    pub fn min_value(&self) -> Option<&crate::Value> {
        self.min_value.as_ref()
    }

    pub fn max_value(&self) -> Option<&crate::Value> {
        self.max_value.as_ref()
    }

    pub fn is_persisted(&self) -> bool {
        self.is_persisted
    }
}

/* ----------------------------------------- EVOLUTION ----------------------------------------- */

impl TableStats {
    pub fn advance_row_id(&mut self, record_count: i64) {
        self.next_row_id += record_count;
    }

    pub fn add_record_count(&mut self, record_count: i64) {
        self.record_count = Some(self.record_count.unwrap_or_default() + record_count);
    }

    pub fn add_file_size_bytes(&mut self, file_size_bytes: Option<i64>) {
        self.file_size_bytes = match file_size_bytes {
            Some(n) => Some(self.file_size_bytes.unwrap_or_default() + n),
            None => self.file_size_bytes,
        };
    }

    pub fn set_persisted(&mut self) {
        self.is_persisted = true;
    }

    pub fn column_stats_mut(&mut self, column_id: i64) -> &mut ColumnStats {
        self.column_stats.entry(column_id).or_default()
    }
}

impl ColumnStats {
    pub fn update_contains_null(&mut self, contains_null: Option<bool>) {
        self.contains_null = match (self.contains_null, contains_null) {
            (Some(old), Some(new)) => Some(old || new),
            (None, Some(true)) | (Some(true), None) => Some(true),
            _ => None,
        };
    }

    pub fn update_contains_nan(&mut self, contains_nan: Option<bool>) {
        self.contains_nan = match (self.contains_nan, contains_nan) {
            (Some(old), Some(new)) => Some(old || new),
            (None, Some(true)) | (Some(true), None) => Some(true),
            _ => None,
        };
    }

    pub fn update_min_value(&mut self, min_value: Option<&crate::Value>) {
        self.min_value = match (self.min_value.take(), min_value) {
            (Some(old), Some(new)) => old
                .partial_cmp(new)
                .map(|ord| if ord.is_gt() { new.clone() } else { old }),
            (old, new) => old.or(new.cloned()),
        };
    }

    pub fn update_max_value(&mut self, max_value: Option<&crate::Value>) {
        self.max_value = match (self.max_value.take(), max_value) {
            (Some(old), Some(new)) => old
                .partial_cmp(new)
                .map(|ord| if ord.is_lt() { new.clone() } else { old }),
            (old, new) => old.or(new.cloned()),
        };
    }

    pub fn set_persisted(&mut self) {
        self.is_persisted = true;
    }
}
