use std::collections::HashMap;

use crate::spec::*;
use crate::{DucklakeResult, Value, io};

pub(super) fn parse_data_file(
    data_file: DucklakeDataFile,
    column_stats: Option<&Vec<DucklakeFileColumnStats>>,
    column_dtypes: &HashMap<i64, crate::DataType>,
    base_path: &io::DucklakePath,
) -> DucklakeResult<(String, crate::DataFileStatistics)> {
    let path = io::DucklakePath::new(&data_file.path, data_file.path_is_relative);

    // Get the column stats
    let column_stats = column_stats
        .map(|stats| {
            stats
                .iter()
                .map(|stat| Ok((stat.column_id, parse_column_stats(stat, column_dtypes)?)))
                .filter_map(|result| match result {
                    Ok((id, Some(stats))) => Some(Ok((id, stats))),
                    Ok((_, None)) => None,
                    Err(e) => Some(Err(e)),
                })
                .collect::<DucklakeResult<HashMap<_, _>>>()
        })
        .transpose()?
        .unwrap_or_default();

    // Build the data file
    let path = base_path.join(&path).to_string();
    let statistics = crate::DataFileStatistics {
        num_rows: data_file.record_count as usize,
        file_size_bytes: data_file.file_size_bytes.map(|s| s as usize),
        footer_size_bytes: data_file.footer_size.map(|s| s as usize),
        column_stats,
    };
    Ok((path, statistics))
}

fn parse_column_stats(
    stats: &DucklakeFileColumnStats,
    column_dtypes: &HashMap<i64, crate::DataType>,
) -> DucklakeResult<Option<crate::FileColumnStats>> {
    // NOTE: The catalog might not supply a dtype because the column has been dropped.
    if let Some(dtype) = column_dtypes.get(&stats.column_id) {
        let output = crate::FileColumnStats {
            size_bytes: stats.column_size_bytes.map(|s| s as usize),
            null_count: stats.null_count.map(|c| c as usize),
            min_value: stats
                .min_value
                .as_ref()
                .map(|v| Value::parse(dtype, v))
                .transpose()?
                .flatten(),
            max_value: stats
                .max_value
                .as_ref()
                .map(|v| Value::parse(dtype, v))
                .transpose()?
                .flatten(),
            contains_nan: stats.contains_nan,
        };
        Ok(Some(output))
    } else {
        Ok(None)
    }
}

pub(super) fn parse_delete_file(
    delete_file: &DucklakeDeleteFile,
    base_path: &io::DucklakePath,
) -> crate::ScanDeleteFile {
    if delete_file.format != "parquet" {
        unimplemented!(
            "Encountered unsupported delete file format '{}'. Currently, only 'parquet' delete files are supported.",
            delete_file.format
        );
    }
    let path = io::DucklakePath::new(&delete_file.path.clone(), delete_file.path_is_relative);
    crate::ScanDeleteFile {
        path: base_path.join(&path).to_string(),
        num_deletes: delete_file.delete_count.unwrap_or_default() as usize,
        file_size_bytes: delete_file.file_size_bytes.map(|s| s as usize),
        footer_size_bytes: delete_file.footer_size.map(|s| s as usize),
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                             TESTS                                             */
/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::Value;

    fn make_data_file(
        path: &str,
        path_is_relative: bool,
        record_count: i64,
        file_size_bytes: Option<i64>,
        footer_size: Option<i64>,
    ) -> DucklakeDataFile {
        DucklakeDataFile {
            data_file_id: 1,
            table_id: 1,
            begin_snapshot: 0,
            end_snapshot: None,
            file_order: None,
            path: path.to_string(),
            path_is_relative,
            file_format: "parquet".to_string(),
            record_count,
            file_size_bytes,
            footer_size,
            row_id_start: Some(0),
            partition_id: None,
            encryption_key: None,
            mapping_id: None,
            partial_max: None,
        }
    }

    fn make_delete_file(
        path: &str,
        path_is_relative: bool,
        format: &str,
        delete_count: Option<i64>,
    ) -> DucklakeDeleteFile {
        DucklakeDeleteFile {
            delete_file_id: 1,
            table_id: 1,
            begin_snapshot: 0,
            end_snapshot: None,
            data_file_id: 1,
            path: path.to_string(),
            path_is_relative,
            format: format.to_string(),
            delete_count,
            file_size_bytes: Some(256),
            footer_size: Some(32),
            encryption_key: None,
            partial_max: None,
        }
    }

    fn make_column_stats(
        column_id: i64,
        min: Option<&str>,
        max: Option<&str>,
        null_count: Option<i64>,
    ) -> DucklakeFileColumnStats {
        DucklakeFileColumnStats {
            data_file_id: 1,
            table_id: 1,
            column_id,
            column_size_bytes: Some(128),
            value_count: Some(10),
            null_count,
            min_value: min.map(String::from),
            max_value: max.map(String::from),
            contains_nan: None,
            extra_stats: None,
        }
    }

    #[test]
    fn test_parse_data_file_relative_path_joined_to_base() {
        let base = io::DucklakePath::Relative("data/".to_string());
        let df = make_data_file("file.parquet", true, 100, Some(1024), Some(64));
        let dtypes = HashMap::new();
        let (path, stats) = parse_data_file(df, None, &dtypes, &base).unwrap();
        assert_eq!(path, "data/file.parquet");
        assert_eq!(stats.num_rows, 100);
        assert_eq!(stats.file_size_bytes, Some(1024));
        assert_eq!(stats.footer_size_bytes, Some(64));
        assert!(stats.column_stats.is_empty());
    }

    #[test]
    fn test_parse_data_file_absolute_path_overrides_base() {
        let base = io::DucklakePath::Relative("data/".to_string());
        let df = make_data_file("s3://bucket/x.parquet", false, 5, None, None);
        let dtypes = HashMap::new();
        let (path, stats) = parse_data_file(df, None, &dtypes, &base).unwrap();
        assert_eq!(path, "s3://bucket/x.parquet");
        assert_eq!(stats.num_rows, 5);
        assert_eq!(stats.file_size_bytes, None);
        assert_eq!(stats.footer_size_bytes, None);
    }

    #[test]
    fn test_parse_data_file_with_column_stats() {
        let base = io::DucklakePath::Relative("data/".to_string());
        let df = make_data_file("file.parquet", true, 42, None, None);
        let stats = vec![
            make_column_stats(1, Some("10"), Some("100"), Some(3)),
            make_column_stats(2, Some("hello"), Some("world"), Some(0)),
        ];
        let mut dtypes = HashMap::new();
        dtypes.insert(1, crate::DataType::Int32);
        dtypes.insert(2, crate::DataType::Varchar);
        let (_, parsed) = parse_data_file(df, Some(&stats), &dtypes, &base).unwrap();

        assert_eq!(parsed.column_stats.len(), 2);
        let col1 = &parsed.column_stats[&1];
        assert_eq!(col1.min_value, Some(Value::Int32(10)));
        assert_eq!(col1.max_value, Some(Value::Int32(100)));
        assert_eq!(col1.null_count, Some(3));
        assert_eq!(col1.size_bytes, Some(128));

        let col2 = &parsed.column_stats[&2];
        assert_eq!(col2.min_value, Some(Value::Varchar("hello".to_string())));
        assert_eq!(col2.max_value, Some(Value::Varchar("world".to_string())));
    }

    #[test]
    fn test_parse_data_file_drops_stats_for_unknown_columns() {
        // A column that was dropped doesn't appear in `column_dtypes` and should be ignored.
        let base = io::DucklakePath::Relative("data/".to_string());
        let df = make_data_file("file.parquet", true, 1, None, None);
        let stats = vec![
            make_column_stats(1, Some("1"), Some("2"), Some(0)),
            make_column_stats(999, Some("dropped"), Some("dropped"), Some(0)),
        ];
        let mut dtypes = HashMap::new();
        dtypes.insert(1, crate::DataType::Int32);
        let (_, parsed) = parse_data_file(df, Some(&stats), &dtypes, &base).unwrap();
        assert_eq!(parsed.column_stats.len(), 1);
        assert!(parsed.column_stats.contains_key(&1));
    }

    #[test]
    fn test_parse_data_file_null_min_max() {
        let base = io::DucklakePath::Relative("data/".to_string());
        let df = make_data_file("file.parquet", true, 1, None, None);
        let stats = vec![make_column_stats(1, None, None, Some(10))];
        let mut dtypes = HashMap::new();
        dtypes.insert(1, crate::DataType::Int32);
        let (_, parsed) = parse_data_file(df, Some(&stats), &dtypes, &base).unwrap();
        let col = &parsed.column_stats[&1];
        assert_eq!(col.min_value, None);
        assert_eq!(col.max_value, None);
        assert_eq!(col.null_count, Some(10));
    }

    #[test]
    fn test_parse_delete_file_relative_path() {
        let base = io::DucklakePath::Relative("data/".to_string());
        let df = make_delete_file("deletes.parquet", true, "parquet", Some(7));
        let parsed = parse_delete_file(&df, &base);
        assert_eq!(parsed.path, "data/deletes.parquet");
        assert_eq!(parsed.num_deletes, 7);
        assert_eq!(parsed.file_size_bytes, Some(256));
        assert_eq!(parsed.footer_size_bytes, Some(32));
    }

    #[test]
    fn test_parse_delete_file_absolute_path() {
        let base = io::DucklakePath::Relative("data/".to_string());
        let df = make_delete_file("s3://bucket/d.parquet", false, "parquet", None);
        let parsed = parse_delete_file(&df, &base);
        assert_eq!(parsed.path, "s3://bucket/d.parquet");
        assert_eq!(parsed.num_deletes, 0);
    }

    #[test]
    #[should_panic(expected = "unsupported delete file format")]
    fn test_parse_delete_file_rejects_non_parquet() {
        let base = io::DucklakePath::Relative("data/".to_string());
        let df = make_delete_file("file.csv", true, "csv", Some(1));
        let _ = parse_delete_file(&df, &base);
    }
}
