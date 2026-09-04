mod parsing;
mod queries;

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::Int64Array;

use crate::caches::{Snapshot, SnapshotCache};
use crate::spec::*;
use crate::{DucklakeResult, db, io};

pub(crate) struct TableScan {
    pub result: crate::ScanResult,
    pub data_file_metadata: Vec<TransferDataFileMetadata>,
}

pub(crate) struct TransferDataFileMetadata {
    pub partition_values: Option<Vec<Option<String>>>,
}

pub(crate) async fn scan_table(
    pool: &db::Pool,
    table_id: i64,
    snapshot: Arc<Snapshot>,
    snapshot_cache: &SnapshotCache,
    data_path: &io::DucklakePath,
) -> DucklakeResult<crate::ScanResult> {
    Ok(
        scan_table_inner(pool, table_id, snapshot, snapshot_cache, data_path, false)
            .await?
            .result,
    )
}

pub(crate) async fn scan_table_with_transfer_metadata(
    pool: &db::Pool,
    table_id: i64,
    snapshot: Arc<Snapshot>,
    snapshot_cache: &SnapshotCache,
    data_path: &io::DucklakePath,
) -> DucklakeResult<TableScan> {
    scan_table_inner(pool, table_id, snapshot, snapshot_cache, data_path, true).await
}

async fn scan_table_inner(
    pool: &db::Pool,
    table_id: i64,
    snapshot: Arc<Snapshot>,
    snapshot_cache: &SnapshotCache,
    data_path: &io::DucklakePath,
    include_transfer_metadata: bool,
) -> DucklakeResult<TableScan> {
    let snapshot_id = snapshot.info().id;

    // Build all queries
    let data_files_query = queries::build_data_files_query(table_id, snapshot_id);
    let column_stats_query = queries::build_column_stats_query(table_id, snapshot_id);
    let delete_files_query = queries::build_delete_files_query(table_id, snapshot_id);
    let partition_values_query = queries::build_partition_values_query(table_id);
    let partition_info_query = queries::build_partition_info_query(table_id, snapshot_id);
    let inlined_data_query = queries::build_inlined_data_tables_query(table_id);
    let inlined_deletes_query = queries::build_inlined_deletes_query(table_id, snapshot_id);

    // Execute all queries in parallel for the latest snapshot
    #[allow(clippy::type_complexity)]
    let (
        fetched_data_files,
        fetched_column_stats,
        fetched_delete_files,
        fetched_partition_values,
        fetched_partition_infos,
        fetched_inlined_data_tables,
        fetched_inlined_deletes,
    ): (
        Vec<DucklakeDataFile>,
        Vec<DucklakeFileColumnStats>,
        Vec<DucklakeDeleteFile>,
        Vec<DucklakeFilePartitionValue>,
        Vec<DucklakePartitionInfo>,
        Vec<DucklakeInlinedDataTables>,
        Vec<DucklakeInlinedDelete>,
    ) = tokio::try_join!(
        pool.fetch_all(&data_files_query),
        pool.fetch_all(&column_stats_query),
        pool.fetch_all(&delete_files_query),
        async {
            if include_transfer_metadata {
                pool.fetch_all(&partition_values_query).await
            } else {
                Ok(Vec::new())
            }
        },
        async {
            if include_transfer_metadata {
                pool.fetch_all(&partition_info_query).await
            } else {
                Ok(Vec::new())
            }
        },
        pool.fetch_all(&inlined_data_query),
        async {
            pool.fetch_all(&inlined_deletes_query).await.or_else(|err| {
                if pool.dialect().is_table_not_found_error(&err) {
                    Ok(Vec::new())
                } else {
                    Err(err)
                }
            })
        }
    )?;

    // Fetch all the inlined data tables. For this, we first need to get all relevant schemas
    // from the catalog.
    let snapshots =
        futures::future::try_join_all(fetched_inlined_data_tables.iter().map(|table| async {
            snapshot_cache
                .get_for_schema_version(table.schema_version)
                .await
        }))
        .await?;

    // Then, we can read the inlined data for each existing table with the known schema
    let fetched_inlined_data =
        futures::future::try_join_all(fetched_inlined_data_tables.into_iter().zip(snapshots).map(
            |(table, snapshot_with_schema)| async move {
                let catalog = snapshot_with_schema.catalog().await?;
                let schema = catalog.table(table_id)?.schema();
                let query = queries::build_inlined_data_query(
                    &table.table_name,
                    schema.columns.keys(),
                    snapshot_id,
                );
                pool.fetch_all_arrow(&query, &schema.to_arrow()).await
            },
        ))
        .await?
        .into_iter()
        .filter(|arr| arr.num_rows() > 0)
        .collect();

    // Build the data files along with their delete files. To this end, we first need to hash
    // our fetched data for faster lookup.
    let column_stats_by_file_id: HashMap<_, _> =
        fetched_column_stats
            .into_iter()
            .fold(HashMap::new(), |mut acc, stats| {
                acc.entry(stats.data_file_id)
                    .or_insert_with(Vec::new)
                    .push(stats);
                acc
            });
    let delete_files_by_file_id: HashMap<_, _> =
        fetched_delete_files
            .into_iter()
            .fold(HashMap::new(), |mut acc, df| {
                acc.entry(df.data_file_id).or_insert_with(Vec::new).push(df);
                acc
            });
    let inline_deletes_by_file_id: HashMap<_, _> =
        fetched_inlined_deletes
            .into_iter()
            .fold(HashMap::new(), |mut acc, record| {
                acc.entry(record.file_id)
                    .or_insert_with(Vec::new)
                    .push(record.row_id);
                acc
            });
    let mut partition_values_by_file_id: HashMap<_, _> = fetched_partition_values
        .into_iter()
        .fold(HashMap::new(), |mut acc, value| {
            acc.entry(value.data_file_id)
                .or_insert_with(Vec::new)
                .push(value);
            acc
        });
    let current_partition_id = fetched_partition_infos
        .into_iter()
        .next()
        .map(|partition| partition.partition_id);

    // Before iterating over the data files, we extract some information from the catalog
    let catalog = snapshot.catalog().await?;
    let column_dtypes = catalog.table(table_id)?.column_data_types();

    // Then, we can iterate over the data files
    let mut result = Vec::with_capacity(fetched_data_files.len());
    let mut data_file_metadata = Vec::with_capacity(fetched_data_files.len());
    for fetched_data_file in fetched_data_files {
        let file_id = fetched_data_file.data_file_id;
        let partition_values = match (fetched_data_file.partition_id, current_partition_id) {
            (Some(file_partition_id), Some(current_partition_id))
                if file_partition_id == current_partition_id =>
            {
                Some(
                    partition_values_by_file_id
                        .remove(&file_id)
                        .unwrap_or_default()
                        .into_iter()
                        .map(|value| value.partition_value)
                        .collect(),
                )
            }
            _ => None,
        };

        let (data_file, statistics) = parsing::parse_data_file(
            fetched_data_file,
            column_stats_by_file_id.get(&file_id),
            &column_dtypes,
            data_path,
        )?;

        let delete_files = delete_files_by_file_id
            .get(&file_id)
            .map(|files| {
                files
                    .iter()
                    .map(|file| parsing::parse_delete_file(file, data_path))
                    .collect()
            })
            .unwrap_or_default();

        result.push(crate::ScanDataFile {
            path: data_file,
            statistics,
            delete_files,
            inline_deletes: inline_deletes_by_file_id
                .get(&file_id)
                .map(|ids| Arc::new(Int64Array::from(ids.clone()))),
        });
        data_file_metadata.push(TransferDataFileMetadata { partition_values });
    }

    Ok(TableScan {
        result: crate::ScanResult {
            data_files: result,
            inline_data: fetched_inlined_data,
        },
        data_file_metadata,
    })
}
