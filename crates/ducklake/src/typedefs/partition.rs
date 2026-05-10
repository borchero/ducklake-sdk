use std::fmt::Display;
use std::str::FromStr;
use std::sync::LazyLock;

use regex::Regex;

use crate::{DucklakeError, DucklakeResult};

#[derive(Debug, Clone)]
pub(crate) struct Partition(pub Vec<PartitionColumn>);

impl From<Vec<PartitionColumn>> for Partition {
    fn from(columns: Vec<PartitionColumn>) -> Self {
        Self(columns)
    }
}

/* -------------------------------------- PARTITION COLUMN ------------------------------------- */

/// A column used to partition a table along with the transform applied to its values.
#[derive(Debug, Clone)]
pub struct PartitionColumn {
    /// The name of the column to partition by.
    pub column: String,
    /// The transform applied to the column's values to derive the partition value.
    pub transform: PartitionTransform,
}

/// A transform applied to a column's values to derive partition values.
#[derive(Debug, Clone, Copy)]
pub enum PartitionTransform {
    /// Use the column's value as-is.
    Identity,
    /// Hash the column's value into the given number of buckets.
    Bucket(u32),
    /// Extract the year from a date or timestamp value.
    Year,
    /// Extract the month from a date or timestamp value.
    Month,
    /// Extract the day from a date or timestamp value.
    Day,
    /// Extract the hour from a timestamp value.
    Hour,
}

impl FromStr for PartitionTransform {
    type Err = DucklakeError;

    fn from_str(s: &str) -> DucklakeResult<Self> {
        static RE_DECIMAL: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^bucket\((\d+)\)$").unwrap());
        match s {
            "identity" => Ok(PartitionTransform::Identity),
            "year" => Ok(PartitionTransform::Year),
            "month" => Ok(PartitionTransform::Month),
            "day" => Ok(PartitionTransform::Day),
            "hour" => Ok(PartitionTransform::Hour),
            _ => {
                if let Some(caps) = RE_DECIMAL.captures(s) {
                    let num_buckets = caps[1]
                        .parse::<u32>()
                        .map_err(|_| DucklakeError::InvalidPartitionTransform(s.to_string()))?;
                    Ok(PartitionTransform::Bucket(num_buckets))
                } else {
                    Err(DucklakeError::InvalidPartitionTransform(s.to_string()))
                }
            }
        }
    }
}

impl Display for PartitionTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            PartitionTransform::Identity => "identity",
            PartitionTransform::Bucket(n) => return write!(f, "bucket({n})"),
            PartitionTransform::Year => "year",
            PartitionTransform::Month => "month",
            PartitionTransform::Day => "day",
            PartitionTransform::Hour => "hour",
        };
        write!(f, "{}", s)
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                             TESTS                                             */
/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("identity")]
    #[case("year")]
    #[case("month")]
    #[case("day")]
    #[case("hour")]
    #[case("bucket(16)")]
    #[case("bucket(1)")]
    #[case("bucket(4294967295)")]
    fn test_partition_transform_roundtrip(#[case] input: &str) {
        let parsed: PartitionTransform = input.parse().unwrap();
        assert_eq!(parsed.to_string(), input);
    }

    #[rstest]
    #[case("identity", matches!(PartitionTransform::Identity, _))]
    #[case("year", matches!(PartitionTransform::Year, _))]
    #[case("month", matches!(PartitionTransform::Month, _))]
    #[case("day", matches!(PartitionTransform::Day, _))]
    #[case("hour", matches!(PartitionTransform::Hour, _))]
    fn test_partition_transform_simple_parse(#[case] input: &str, #[case] _marker: bool) {
        assert!(input.parse::<PartitionTransform>().is_ok());
    }

    #[test]
    fn test_bucket_parse_captures_arg() {
        let parsed: PartitionTransform = "bucket(42)".parse().unwrap();
        match parsed {
            PartitionTransform::Bucket(n) => assert_eq!(n, 42),
            _ => panic!("expected Bucket"),
        }
    }

    #[rstest]
    #[case("")]
    #[case("identity ")]
    #[case("unknown")]
    #[case("bucket")]
    #[case("bucket()")]
    #[case("bucket(abc)")]
    #[case("bucket(-1)")]
    #[case("bucket(10) ")]
    #[case("bucket(99999999999)")] // exceeds u32::MAX
    fn test_partition_transform_invalid(#[case] input: &str) {
        assert!(input.parse::<PartitionTransform>().is_err());
    }

    #[rstest]
    #[case(PartitionTransform::Identity, "identity")]
    #[case(PartitionTransform::Year, "year")]
    #[case(PartitionTransform::Month, "month")]
    #[case(PartitionTransform::Day, "day")]
    #[case(PartitionTransform::Hour, "hour")]
    #[case(PartitionTransform::Bucket(8), "bucket(8)")]
    fn test_partition_transform_display(
        #[case] transform: PartitionTransform,
        #[case] expected: &str,
    ) {
        assert_eq!(transform.to_string(), expected);
    }
}
