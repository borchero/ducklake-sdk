mod parsing;
mod queries;

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::Int64Array;

use crate::caches::{Snapshot, SnapshotCache};
use crate::spec::*;
use crate::{DucklakeResult, db, io};

pub(crate) async fn scan_table(
    pool: &db::Pool,
    table_id: i64,
    snapshot: Arc<Snapshot>,
    snapshot_cache: &SnapshotCache,
    data_path: &io::DucklakePath,
) -> DucklakeResult<crate::ScanResult> {
    let snapshot_id = snapshot.info().id;

    // Build all queries
    let data_files_query = queries::build_data_files_query(table_id, snapshot_id);
    let column_stats_query = queries::build_column_stats_query(table_id, snapshot_id);
    let delete_files_query = queries::build_delete_files_query(table_id, snapshot_id);
    let inlined_data_query = queries::build_inlined_data_tables_query(table_id);
    let inlined_deletes_query = queries::build_inlined_deletes_query(table_id, snapshot_id);

    // Execute all queries in parallel for the latest snapshot
    #[allow(clippy::type_complexity)]
    let (
        fetched_data_files,
        fetched_column_stats,
        fetched_delete_files,
        fetched_inlined_data_tables,
        fetched_inlined_deletes,
    ): (
        Vec<DucklakeDataFile>,
        Vec<DucklakeFileColumnStats>,
        Vec<DucklakeDeleteFile>,
        Vec<DucklakeInlinedDataTables>,
        Vec<DucklakeInlinedDelete>,
    ) = tokio::try_join!(
        pool.fetch_all(&data_files_query),
        pool.fetch_all(&column_stats_query),
        pool.fetch_all(&delete_files_query),
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

    // Before iterating over the data files, we extract some information from the catalog
    let catalog = snapshot.catalog().await?;
    let column_dtypes = catalog.table(table_id)?.column_data_types();

    // Then, we can iterate over the data files
    let mut result = Vec::with_capacity(fetched_data_files.len());
    for fetched_data_file in fetched_data_files {
        let file_id = fetched_data_file.data_file_id;

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
    }

    Ok(crate::ScanResult {
        data_files: result,
        inline_data: fetched_inlined_data,
    })
}
