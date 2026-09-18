use std::collections::HashMap;

use crate::spec::{DucklakeDataFile, DucklakeFilePartitionValue};

pub(crate) struct BucketColumn {
    pub partition_key_index: i64,
    pub field_id: i64,
    pub num_buckets: u32,
    pub begin_snapshot: i64,
}

pub(super) fn parse_bucket_values(
    file: &DucklakeDataFile,
    current_partition_id: Option<i64>,
    columns: &[BucketColumn],
    values: Option<&Vec<DucklakeFilePartitionValue>>,
) -> HashMap<i64, (u32, u32)> {
    if current_partition_id.is_none() || file.partition_id != current_partition_id {
        return HashMap::new();
    }

    let mut by_index = HashMap::new();
    for value in values.into_iter().flatten() {
        if value.table_id != file.table_id || value.data_file_id != file.data_file_id {
            continue;
        }
        let bucket = value.partition_value.as_deref().and_then(|value| {
            if value.is_empty() || !value.bytes().all(|c| c.is_ascii_digit()) {
                return None;
            }
            value.parse::<u32>().ok()
        });
        by_index
            .entry(value.partition_key_index)
            .and_modify(|value| *value = None)
            .or_insert(bucket);
    }

    columns
        .iter()
        .filter_map(|column| {
            // Transfers register historical files and current column definitions in the
            // same snapshot, losing the original source type. Require an older column
            // version; even non-type edits conservatively disable pruning for old files.
            if file.begin_snapshot <= column.begin_snapshot {
                return None;
            }
            let bucket = by_index
                .get(&column.partition_key_index)
                .copied()
                .flatten()?;
            (bucket < column.num_buckets)
                .then_some((column.field_id, (column.num_buckets, bucket)))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use rstest::{fixture, rstest};

    use super::*;

    #[fixture]
    fn file() -> DucklakeDataFile {
        DucklakeDataFile {
            data_file_id: 1,
            table_id: 2,
            begin_snapshot: 10,
            end_snapshot: None,
            file_order: None,
            path: "file.parquet".into(),
            path_is_relative: true,
            file_format: "parquet".into(),
            record_count: 10,
            file_size_bytes: None,
            footer_size: None,
            row_id_start: None,
            partition_id: Some(3),
            encryption_key: None,
            mapping_id: None,
            partial_max: None,
        }
    }

    #[fixture]
    fn columns() -> Vec<BucketColumn> {
        vec![BucketColumn {
            partition_key_index: 4,
            field_id: 21,
            num_buckets: 8,
            begin_snapshot: 5,
        }]
    }

    fn value(value: Option<&str>) -> DucklakeFilePartitionValue {
        DucklakeFilePartitionValue {
            data_file_id: 1,
            table_id: 2,
            partition_key_index: 4,
            partition_value: value.map(str::to_owned),
        }
    }

    #[rstest]
    #[case(Some("0"), Some(0))]
    #[case(Some("7"), Some(7))]
    #[case(Some("0002"), Some(2))]
    #[case(None, None)]
    #[case(Some(""), None)]
    #[case(Some("null"), None)]
    #[case(Some("-1"), None)]
    #[case(Some("+1"), None)]
    #[case(Some(" 1"), None)]
    #[case(Some("1 "), None)]
    #[case(Some("1.0"), None)]
    #[case(Some("8"), None)]
    #[case(Some("4294967296"), None)]
    fn bucket_values_require_valid_numeric_ids(
        file: DucklakeDataFile,
        columns: Vec<BucketColumn>,
        #[case] input: Option<&str>,
        #[case] expected: Option<u32>,
    ) {
        // Arrange
        let values = vec![value(input)];

        // Act
        let result = parse_bucket_values(&file, Some(3), &columns, Some(&values));

        // Assert
        assert_eq!(result.get(&21), expected.map(|value| (8, value)).as_ref());
    }

    #[rstest]
    #[case(Some("2"), Some("2"))]
    #[case(Some("2"), Some("3"))]
    #[case(Some("2"), None)]
    #[case(None, Some("2"))]
    #[case(Some("bad"), Some("2"))]
    fn duplicate_bucket_values_are_unknown(
        file: DucklakeDataFile,
        columns: Vec<BucketColumn>,
        #[case] first: Option<&str>,
        #[case] second: Option<&str>,
    ) {
        // Arrange
        let values = vec![value(first), value(second), value(Some("2"))];

        // Act
        let result = parse_bucket_values(&file, Some(3), &columns, Some(&values));

        // Assert
        assert!(result.is_empty());
    }

    #[rstest]
    #[case(None, 10, false)]
    #[case(Some(2), 10, false)]
    #[case(Some(3), 4, false)]
    #[case(Some(3), 5, false)]
    #[case(Some(3), 10, true)]
    fn bucket_values_require_current_partition_and_column_version(
        mut file: DucklakeDataFile,
        columns: Vec<BucketColumn>,
        #[case] partition_id: Option<i64>,
        #[case] file_snapshot: i64,
        #[case] known: bool,
    ) {
        // Arrange
        file.partition_id = partition_id;
        file.begin_snapshot = file_snapshot;
        let values = vec![value(Some("2"))];

        // Act
        let result = parse_bucket_values(&file, Some(3), &columns, Some(&values));

        // Assert
        assert_eq!(result.contains_key(&21), known);
    }

    #[rstest]
    #[case(99, 1, 4)]
    #[case(2, 99, 4)]
    #[case(2, 1, 99)]
    fn bucket_values_require_matching_table_file_and_key(
        file: DucklakeDataFile,
        columns: Vec<BucketColumn>,
        #[case] table_id: i64,
        #[case] data_file_id: i64,
        #[case] partition_key_index: i64,
    ) {
        // Arrange
        let mut row = value(Some("2"));
        row.table_id = table_id;
        row.data_file_id = data_file_id;
        row.partition_key_index = partition_key_index;
        let values = vec![row];

        // Act
        let result = parse_bucket_values(&file, Some(3), &columns, Some(&values));

        // Assert
        assert!(result.is_empty());
    }

    #[rstest]
    fn missing_bucket_values_are_unknown(file: DucklakeDataFile, columns: Vec<BucketColumn>) {
        // Arrange
        let values = None;

        // Act
        let result = parse_bucket_values(&file, Some(3), &columns, values);

        // Assert
        assert!(result.is_empty());
    }
}
