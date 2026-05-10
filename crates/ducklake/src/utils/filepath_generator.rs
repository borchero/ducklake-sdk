use indexmap::IndexMap;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use uuid::Uuid;

use crate::{Value, io};

/* ----------------------------------------- DATA FILE ----------------------------------------- */

/// Generator for paths of new data files within a table's data directory.
pub struct DataFilePathGenerator {
    base_path: io::DucklakePath,
    hive_file_pattern: bool,
}

impl DataFilePathGenerator {
    pub(crate) fn new(base_path: io::DucklakePath, hive_file_pattern: bool) -> Self {
        Self {
            base_path,
            hive_file_pattern,
        }
    }

    /// Get the base path under which all generated paths are created.
    pub fn base_path(&self) -> &str {
        self.base_path.as_str()
    }

    /// Generate a new data file path relative to the base path for the provided partition values.
    pub fn generate_relative(&self, partition_values: &IndexMap<String, Option<Value>>) -> String {
        const ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.');
        if partition_values.is_empty() || !self.hive_file_pattern {
            Self::last_path_component()
        } else {
            let mut components = Vec::with_capacity(partition_values.len() + 1);
            for (col, val) in partition_values {
                let val_str = Value::to_string_opt(val.as_ref());
                let encoded = utf8_percent_encode(&val_str, ENCODE_SET);
                components.push(format!("{}={}", col, encoded));
            }
            components.push(Self::last_path_component());
            components.join("/")
        }
    }

    /// Generate a new absolute data file path for the provided partition values.
    pub fn generate_absolute(&self, partition_values: &IndexMap<String, Option<Value>>) -> String {
        self.base_path
            .join_str(&self.generate_relative(partition_values))
            .to_string()
    }

    fn last_path_component() -> String {
        format!("ducklake-{}.parquet", Uuid::now_v7())
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                             TESTS                                             */
/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    fn make_generator(hive: bool) -> DataFilePathGenerator {
        DataFilePathGenerator::new(io::DucklakePath::Relative("data/".to_string()), hive)
    }

    #[test]
    fn test_base_path() {
        let generator = make_generator(false);
        assert_eq!(generator.base_path(), "data/");
    }

    #[test]
    fn test_generate_relative_empty_partitions() {
        let generator = make_generator(true);
        let partitions = IndexMap::new();
        let path = generator.generate_relative(&partitions);
        assert!(path.starts_with("ducklake-"));
        assert!(path.ends_with(".parquet"));
        assert!(
            !path.contains('='),
            "no hive components for empty partitions"
        );
    }

    #[test]
    fn test_generate_relative_no_hive() {
        let generator = make_generator(false);
        let mut partitions = IndexMap::new();
        partitions.insert("year".to_string(), Some(Value::Int32(2024)));
        let path = generator.generate_relative(&partitions);
        // Without hive, partitions are ignored
        assert!(!path.contains("year="));
        assert!(path.starts_with("ducklake-"));
        assert!(path.ends_with(".parquet"));
    }

    #[test]
    fn test_generate_relative_with_hive_partitions() {
        let generator = make_generator(true);
        let mut partitions = IndexMap::new();
        partitions.insert("year".to_string(), Some(Value::Int32(2024)));
        partitions.insert("month".to_string(), Some(Value::Int32(1)));
        let path = generator.generate_relative(&partitions);
        assert!(path.starts_with("year=2024/month=1/ducklake-"));
        assert!(path.ends_with(".parquet"));
    }

    #[test]
    fn test_generate_relative_null_partition_value() {
        let generator = make_generator(true);
        let mut partitions = IndexMap::new();
        partitions.insert("year".to_string(), None);
        let path = generator.generate_relative(&partitions);
        assert!(path.starts_with("year=NULL/ducklake-"));
    }

    #[test]
    fn test_generate_relative_percent_encodes_special_chars() {
        let generator = make_generator(true);
        let mut partitions = IndexMap::new();
        partitions.insert("k".to_string(), Some(Value::Varchar("a/b c".to_string())));
        let path = generator.generate_relative(&partitions);
        // `/` and ` ` should be percent-encoded in the value
        assert!(path.starts_with("k=a%2Fb%20c/ducklake-"), "got: {path}");
    }

    #[test]
    fn test_generate_relative_preserves_allowed_chars() {
        let generator = make_generator(true);
        let mut partitions = IndexMap::new();
        partitions.insert("k".to_string(), Some(Value::Varchar("a-b_c.d".to_string())));
        let path = generator.generate_relative(&partitions);
        assert!(path.starts_with("k=a-b_c.d/ducklake-"), "got: {path}");
    }

    #[test]
    fn test_generate_absolute_prepends_base() {
        let generator = make_generator(true);
        let mut partitions = IndexMap::new();
        partitions.insert("y".to_string(), Some(Value::Int32(2024)));
        let path = generator.generate_absolute(&partitions);
        assert!(path.starts_with("data/y=2024/ducklake-"), "got: {path}");
    }
}
