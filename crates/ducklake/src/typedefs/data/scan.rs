use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};

use super::DataFileStatistics;

/// Result of scanning a table at a specific snapshot.
pub struct ScanResult {
    /// The data files that need to be read to produce the table contents.
    pub data_files: Vec<ScanDataFile>,
    /// Record batches whose contents are inlined in the catalog.
    pub inline_data: Vec<RecordBatch>,
}

/// Precomputed bucket IDs allowed by a row predicate on one field.
///
/// Callers must compute `values` with DuckLake's bucket hash and the source column's
/// logical type, not the type of a predicate literal or an implicit cast. Bucket
/// collisions are possible: the original row predicate must still be applied.
#[derive(Debug, Clone)]
pub struct BucketFilter {
    /// The source column's field ID.
    pub field_id: i64,
    /// The number of buckets used when computing `values`.
    pub num_buckets: u32,
    /// The allowed bucket IDs.
    pub values: Vec<u32>,
}

impl ScanResult {
    /// Remove files excluded by any of the supplied bucket filters (AND semantics).
    ///
    /// Missing metadata and mismatching bucket counts cannot exclude a file.
    /// Invalid filters are ignored. Inline data is never pruned, and retained files
    /// keep their delete files and inline deletes unchanged. The original row
    /// predicate must still be applied to all rows read from this result.
    pub fn prune_buckets(&mut self, filters: &[BucketFilter]) {
        let filters: Vec<_> = filters
            .iter()
            .filter(|filter| {
                filter.num_buckets > 0
                    && filter
                        .values
                        .iter()
                        .all(|value| *value < filter.num_buckets)
            })
            .map(|filter| {
                (
                    filter.field_id,
                    filter.num_buckets,
                    filter.values.iter().copied().collect::<HashSet<_>>(),
                )
            })
            .collect();
        self.data_files.retain(|file| {
            filters.iter().all(|(field_id, count, values)| {
                match file.bucket_values.get(field_id) {
                    Some(&(num_buckets, value))
                        if num_buckets == *count && value < num_buckets =>
                    {
                        values.contains(&value)
                    }
                    _ => true,
                }
            })
        });
    }
}

/// A data file that needs to be read as part of a [`ScanResult`].
pub struct ScanDataFile {
    /// The path of the data file.
    pub path: String,
    /// Statistics describing the contents of the data file.
    pub statistics: DataFileStatistics,
    /// Delete files that need to be applied to the data file.
    pub delete_files: Vec<ScanDeleteFile>,
    /// Row indices that have been deleted via inline deletes.
    pub inline_deletes: Option<Arc<Int64Array>>,
    /// Safe bucket metadata: field ID to (number of buckets, file bucket ID).
    ///
    /// Only current partition definitions and primitive column versions older than
    /// the file are exposed. Equal snapshots are ambiguous for transferred files,
    /// whose original source types are unknown. Missing metadata is not inferred.
    pub bucket_values: HashMap<i64, (u32, u32)>,
}

/// A delete file that needs to be applied to a data file.
pub struct ScanDeleteFile {
    /// The path of the delete file.
    pub path: String,
    /// The number of rows deleted by the delete file.
    pub num_deletes: usize,
    /// The size of the delete file in bytes.
    pub file_size_bytes: Option<usize>,
    /// The size of the delete file's footer in bytes.
    pub footer_size_bytes: Option<usize>,
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};
    use rstest::{fixture, rstest};

    use super::*;

    #[fixture]
    fn scan() -> ScanResult {
        let data_files = [
            HashMap::from([(1, (8, 2)), (2, (4, 1))]),
            HashMap::from([(1, (8, 3)), (2, (4, 2))]),
            HashMap::new(),
            HashMap::from([(1, (16, 2))]),
            HashMap::from([(1, (8, 8))]),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, bucket_values)| ScanDataFile {
            path: index.to_string(),
            statistics: DataFileStatistics {
                num_rows: 10,
                file_size_bytes: None,
                footer_size_bytes: None,
                column_stats: HashMap::new(),
            },
            delete_files: vec![ScanDeleteFile {
                path: format!("{index}.deletes"),
                num_deletes: 1,
                file_size_bytes: Some(20),
                footer_size_bytes: Some(5),
            }],
            inline_deletes: Some(Arc::new(Int64Array::from(vec![4]))),
            bucket_values,
        })
        .collect();
        ScanResult {
            data_files,
            inline_data: vec![
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)])),
                    vec![Arc::new(Int64Array::from(vec![42]))],
                )
                .unwrap(),
            ],
        }
    }

    fn filter(field_id: i64, num_buckets: u32, values: &[u32]) -> BucketFilter {
        BucketFilter {
            field_id,
            num_buckets,
            values: values.to_vec(),
        }
    }

    #[rstest]
    #[case(vec![], vec!["0", "1", "2", "3", "4"])]
    #[case(vec![filter(1, 8, &[2])], vec!["0", "2", "3", "4"])]
    #[case(vec![filter(1, 8, &[2, 3])], vec!["0", "1", "2", "3", "4"])]
    #[case(vec![filter(1, 8, &[2]), filter(2, 4, &[2])], vec!["2", "3", "4"])]
    #[case(vec![filter(1, 8, &[])], vec!["2", "3", "4"])]
    #[case(vec![filter(99, 8, &[])], vec!["0", "1", "2", "3", "4"])]
    #[case(vec![filter(1, 0, &[])], vec!["0", "1", "2", "3", "4"])]
    #[case(vec![filter(1, 8, &[8])], vec!["0", "1", "2", "3", "4"])]
    fn bucket_pruning_preserves_unknown_files_and_deletes(
        mut scan: ScanResult,
        #[case] filters: Vec<BucketFilter>,
        #[case] expected: Vec<&str>,
    ) {
        // Arrange
        let inline_data = scan.inline_data.clone();

        // Act
        scan.prune_buckets(&filters);

        // Assert
        assert_eq!(
            scan.data_files
                .iter()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(scan.inline_data, inline_data);
        for file in &scan.data_files {
            assert_eq!(file.statistics.num_rows, 10);
            assert_eq!(file.delete_files[0].path, format!("{}.deletes", file.path));
            assert_eq!(file.inline_deletes.as_ref().unwrap().values(), &[4]);
        }
    }
}
