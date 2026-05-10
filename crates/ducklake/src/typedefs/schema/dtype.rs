use std::fmt::Display;

use super::Column;

/// A DuckLake data type.
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    Float32,
    Float64,
    Decimal { precision: u8, scale: u8 },
    Time,
    TimeTz,
    Date,
    Timestamp { precision: TimestampPrecision },
    TimestampTz,
    Interval,
    Varchar,
    Blob,
    Json,
    Uuid,
    List(Box<Column>),
    Struct(Vec<Column>),
    Map(Box<Column>, Box<Column>),
    // TODO: Add geometry data types
    // TODO: Add variant data type
}

/// The precision of a [`DataType::Timestamp`] value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimestampPrecision {
    Seconds,
    Milliseconds,
    #[default]
    Microseconds,
    Nanoseconds,
}

impl DataType {
    /// Construct a [`DataType::Boolean`] value.
    pub fn boolean() -> Self {
        DataType::Boolean
    }

    /// Construct a [`DataType::Int8`] value.
    pub fn int8() -> Self {
        DataType::Int8
    }

    /// Construct a [`DataType::Int16`] value.
    pub fn int16() -> Self {
        DataType::Int16
    }

    /// Construct a [`DataType::Int32`] value.
    pub fn int32() -> Self {
        DataType::Int32
    }

    /// Construct a [`DataType::Int64`] value.
    pub fn int64() -> Self {
        DataType::Int64
    }

    /// Construct a [`DataType::Int128`] value.
    pub fn int128() -> Self {
        DataType::Int128
    }

    /// Construct a [`DataType::UInt8`] value.
    pub fn uint8() -> Self {
        DataType::UInt8
    }

    /// Construct a [`DataType::UInt16`] value.
    pub fn uint16() -> Self {
        DataType::UInt16
    }

    /// Construct a [`DataType::UInt32`] value.
    pub fn uint32() -> Self {
        DataType::UInt32
    }

    /// Construct a [`DataType::UInt64`] value.
    pub fn uint64() -> Self {
        DataType::UInt64
    }

    /// Construct a [`DataType::UInt128`] value.
    pub fn uint128() -> Self {
        DataType::UInt128
    }

    /// Construct a [`DataType::Float32`] value.
    pub fn float32() -> Self {
        DataType::Float32
    }

    /// Construct a [`DataType::Float64`] value.
    pub fn float64() -> Self {
        DataType::Float64
    }

    /// Construct a [`DataType::Decimal`] value with the provided precision and scale.
    pub fn decimal(precision: u8, scale: u8) -> Self {
        DataType::Decimal { precision, scale }
    }

    /// Construct a [`DataType::Time`] value.
    pub fn time() -> Self {
        DataType::Time
    }

    /// Construct a [`DataType::TimeTz`] value.
    pub fn time_tz() -> Self {
        DataType::TimeTz
    }

    /// Construct a [`DataType::Date`] value.
    pub fn date() -> Self {
        DataType::Date
    }

    /// Construct a [`DataType::Timestamp`] value with the provided precision.
    pub fn timestamp(precision: TimestampPrecision) -> Self {
        DataType::Timestamp { precision }
    }

    /// Construct a [`DataType::TimestampTz`] value.
    pub fn timestamp_tz() -> Self {
        DataType::TimestampTz
    }

    /// Construct a [`DataType::Interval`] value.
    pub fn interval() -> Self {
        DataType::Interval
    }

    /// Construct a [`DataType::Varchar`] value.
    pub fn varchar() -> Self {
        DataType::Varchar
    }

    /// Construct a [`DataType::Blob`] value.
    pub fn blob() -> Self {
        DataType::Blob
    }

    /// Construct a [`DataType::Json`] value.
    pub fn json() -> Self {
        DataType::Json
    }

    /// Construct a [`DataType::Uuid`] value.
    pub fn uuid() -> Self {
        DataType::Uuid
    }

    /// Construct a [`DataType::List`] value with the provided element type.
    pub fn list(inner: DataType) -> Self {
        let column = Column::new("element".into(), inner);
        DataType::List(Box::new(column))
    }

    /// Construct a [`DataType::Struct`] value with the provided fields.
    pub fn struct_(fields: Vec<Column>) -> Self {
        DataType::Struct(fields)
    }

    /// Construct a [`DataType::Map`] value with the provided key and value types.
    pub fn map(key: DataType, value: DataType) -> Self {
        let key_col = Column::new("key".into(), key);
        let value_col = Column::new("value".into(), value);
        DataType::Map(Box::new(key_col), Box::new(value_col))
    }
}

impl DataType {
    /// Whether the data type is a nested type (i.e. list, struct, or map).
    pub fn is_nested(&self) -> bool {
        matches!(
            self,
            DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _)
        )
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use DataType::*;
        match self {
            Boolean => write!(f, "boolean"),
            Int8 => write!(f, "int8"),
            Int16 => write!(f, "int16"),
            Int32 => write!(f, "int32"),
            Int64 => write!(f, "int64"),
            Int128 => write!(f, "int128"),
            UInt8 => write!(f, "uint8"),
            UInt16 => write!(f, "uint16"),
            UInt32 => write!(f, "uint32"),
            UInt64 => write!(f, "uint64"),
            UInt128 => write!(f, "uint128"),
            Float32 => write!(f, "float32"),
            Float64 => write!(f, "float64"),
            Decimal { precision, scale } => {
                write!(f, "decimal({}, {})", precision, scale)
            }
            Time => write!(f, "time"),
            TimeTz => write!(f, "timetz"),
            Date => write!(f, "date"),
            Timestamp { precision } => match precision {
                TimestampPrecision::Seconds => write!(f, "timestamp_s"),
                TimestampPrecision::Milliseconds => write!(f, "timestamp_ms"),
                TimestampPrecision::Microseconds => write!(f, "timestamp"),
                TimestampPrecision::Nanoseconds => write!(f, "timestamp_ns"),
            },
            TimestampTz => write!(f, "timestamptz"),
            Interval => write!(f, "interval"),
            Varchar => write!(f, "varchar"),
            Blob => write!(f, "blob"),
            Json => write!(f, "json"),
            Uuid => write!(f, "uuid"),
            List(_) => write!(f, "list"),
            Struct(_) => write!(f, "struct"),
            Map(_, _) => write!(f, "map"),
        }
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
    #[case(DataType::boolean(), "boolean")]
    #[case(DataType::int8(), "int8")]
    #[case(DataType::int16(), "int16")]
    #[case(DataType::int32(), "int32")]
    #[case(DataType::int64(), "int64")]
    #[case(DataType::int128(), "int128")]
    #[case(DataType::uint8(), "uint8")]
    #[case(DataType::uint16(), "uint16")]
    #[case(DataType::uint32(), "uint32")]
    #[case(DataType::uint64(), "uint64")]
    #[case(DataType::uint128(), "uint128")]
    #[case(DataType::float32(), "float32")]
    #[case(DataType::float64(), "float64")]
    #[case(DataType::time(), "time")]
    #[case(DataType::time_tz(), "timetz")]
    #[case(DataType::date(), "date")]
    #[case(DataType::timestamp_tz(), "timestamptz")]
    #[case(DataType::interval(), "interval")]
    #[case(DataType::varchar(), "varchar")]
    #[case(DataType::blob(), "blob")]
    #[case(DataType::json(), "json")]
    #[case(DataType::uuid(), "uuid")]
    fn test_primitive_display(#[case] dtype: DataType, #[case] expected: &str) {
        assert_eq!(dtype.to_string(), expected);
    }

    #[rstest]
    #[case(DataType::decimal(10, 2), "decimal(10, 2)")]
    #[case(DataType::decimal(38, 0), "decimal(38, 0)")]
    fn test_decimal_display(#[case] dtype: DataType, #[case] expected: &str) {
        assert_eq!(dtype.to_string(), expected);
    }

    #[rstest]
    #[case(TimestampPrecision::Seconds, "timestamp_s")]
    #[case(TimestampPrecision::Milliseconds, "timestamp_ms")]
    #[case(TimestampPrecision::Microseconds, "timestamp")]
    #[case(TimestampPrecision::Nanoseconds, "timestamp_ns")]
    fn test_timestamp_display(#[case] precision: TimestampPrecision, #[case] expected: &str) {
        assert_eq!(DataType::timestamp(precision).to_string(), expected);
    }

    #[test]
    fn test_nested_display() {
        assert_eq!(DataType::list(DataType::int32()).to_string(), "list");
        assert_eq!(
            DataType::struct_(vec![Column::new("a".into(), DataType::int32())]).to_string(),
            "struct"
        );
        assert_eq!(
            DataType::map(DataType::varchar(), DataType::int32()).to_string(),
            "map"
        );
    }

    #[rstest]
    #[case(DataType::boolean(), false)]
    #[case(DataType::int32(), false)]
    #[case(DataType::varchar(), false)]
    #[case(DataType::list(DataType::int32()), true)]
    #[case(DataType::struct_(vec![Column::new("a".into(), DataType::int32())]), true)]
    #[case(DataType::map(DataType::varchar(), DataType::int32()), true)]
    fn test_is_nested(#[case] dtype: DataType, #[case] expected: bool) {
        assert_eq!(dtype.is_nested(), expected);
    }

    #[test]
    fn test_list_wraps_in_element_column() {
        let dtype = DataType::list(DataType::int32());
        match dtype {
            DataType::List(inner) => {
                assert_eq!(inner.name, "element");
                assert_eq!(inner.dtype, DataType::int32());
            }
            _ => panic!("expected List"),
        }
    }

    #[test]
    fn test_map_wraps_in_key_value_columns() {
        let dtype = DataType::map(DataType::varchar(), DataType::int32());
        match dtype {
            DataType::Map(key, value) => {
                assert_eq!(key.name, "key");
                assert_eq!(key.dtype, DataType::varchar());
                assert_eq!(value.name, "value");
                assert_eq!(value.dtype, DataType::int32());
            }
            _ => panic!("expected Map"),
        }
    }

    #[test]
    fn test_timestamp_precision_default_is_microseconds() {
        assert_eq!(
            TimestampPrecision::default(),
            TimestampPrecision::Microseconds
        );
    }

    #[test]
    fn test_decimal_constructor() {
        assert_eq!(
            DataType::decimal(15, 4),
            DataType::Decimal {
                precision: 15,
                scale: 4
            }
        );
    }
}
