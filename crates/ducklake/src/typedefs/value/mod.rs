mod format;
mod parse;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use indexmap::IndexMap;
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::primitives::{Interval, TimeWithTimezone};

/// A typed DuckLake value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int128(i128),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    UInt128(u128),
    Float32(f32),
    Float64(f64),
    Decimal(Decimal),
    Time(NaiveTime),
    TimeTz(TimeWithTimezone),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    TimestampTz(DateTime<Utc>),
    Interval(Interval),
    Varchar(String),
    Blob(Vec<u8>),
    Json(String),
    Uuid(Uuid),
    List(Vec<Option<Value>>),
    Struct(IndexMap<String, Option<Value>>),
    Map(Vec<(Value, Option<Value>)>),
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;

        use Value::*;

        match (self, other) {
            (Boolean(a), Boolean(b)) => a.partial_cmp(b),
            (Int8(a), Int8(b)) => a.partial_cmp(b),
            (Int16(a), Int16(b)) => a.partial_cmp(b),
            (Int32(a), Int32(b)) => a.partial_cmp(b),
            (Int64(a), Int64(b)) => a.partial_cmp(b),
            (Int128(a), Int128(b)) => a.partial_cmp(b),
            (UInt8(a), UInt8(b)) => a.partial_cmp(b),
            (UInt16(a), UInt16(b)) => a.partial_cmp(b),
            (UInt32(a), UInt32(b)) => a.partial_cmp(b),
            (UInt64(a), UInt64(b)) => a.partial_cmp(b),
            (UInt128(a), UInt128(b)) => a.partial_cmp(b),
            (Float32(a), Float32(b)) => a.partial_cmp(b),
            (Float64(a), Float64(b)) => a.partial_cmp(b),
            (Decimal(a), Decimal(b)) => a.partial_cmp(b),
            (Time(a), Time(b)) => a.partial_cmp(b),
            (TimeTz(a), TimeTz(b)) => a.time.partial_cmp(&b.time),
            (Date(a), Date(b)) => a.partial_cmp(b),
            (Timestamp(a), Timestamp(b)) => a.partial_cmp(b),
            (TimestampTz(a), TimestampTz(b)) => a.partial_cmp(b),
            // NOTE: Intervals cannot be compared
            (Interval(_), Interval(_)) => None,
            (Varchar(a), Varchar(b)) => a.partial_cmp(b),
            (Blob(a), Blob(b)) => a.partial_cmp(b),
            (Json(a), Json(b)) => a.partial_cmp(b),
            (Uuid(a), Uuid(b)) => a.partial_cmp(b),
            (List(a), List(b)) => a.partial_cmp(b),
            (Struct(a), Struct(b)) => {
                // Compare structs by their entries in iteration order
                let a_iter = a.iter();
                let b_iter = b.iter();
                for (a_entry, b_entry) in a_iter.zip(b_iter) {
                    match a_entry.0.partial_cmp(b_entry.0) {
                        Some(Ordering::Equal) => {}
                        ord => return ord,
                    }
                    match a_entry.1.partial_cmp(b_entry.1) {
                        Some(Ordering::Equal) => {}
                        ord => return ord,
                    }
                }
                a.len().partial_cmp(&b.len())
            }
            (Map(a), Map(b)) => a.partial_cmp(b),
            // NOTE: Different variants cannot be compared
            _ => None,
        }
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                             TESTS                                             */
/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::str::FromStr;

    use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
    use indexmap::IndexMap;
    use rstest::rstest;
    use rust_decimal::Decimal;
    use uuid::Uuid;

    use super::Value;
    use crate::primitives::TimeWithTimezone;
    use crate::typedefs::schema::{Column, DataType, TimestampPrecision};

    fn parse(dtype: &DataType, s: &str) -> Value {
        Value::parse(dtype, s).unwrap().unwrap()
    }

    #[rstest]
    #[case(DataType::Boolean, "true", Value::Boolean(true))]
    #[case(DataType::Boolean, "false", Value::Boolean(false))]
    #[case(DataType::Int8, "-8", Value::Int8(-8))]
    #[case(DataType::Int16, "1234", Value::Int16(1234))]
    #[case(DataType::Int32, "-2147483648", Value::Int32(i32::MIN))]
    #[case(DataType::Int64, "9999999999", Value::Int64(9_999_999_999))]
    #[case(DataType::UInt8, "255", Value::UInt8(u8::MAX))]
    #[case(DataType::UInt16, "0", Value::UInt16(0))]
    #[case(DataType::UInt32, "4294967295", Value::UInt32(u32::MAX))]
    #[case(DataType::UInt64, "18446744073709551615", Value::UInt64(u64::MAX))]
    #[case(DataType::Float32, "1.5", Value::Float32(1.5))]
    #[case(DataType::Float64, "-3.25", Value::Float64(-3.25))]
    #[case(DataType::Varchar, "hello", Value::Varchar("hello".to_string()))]
    #[case(DataType::Json, "{\"a\":1}", Value::Json("{\"a\":1}".to_string()))]
    fn test_primitive_parse(
        #[case] dtype: DataType,
        #[case] input: &str,
        #[case] expected: Value,
    ) {
        assert_eq!(parse(&dtype, input), expected);
    }

    #[test]
    fn test_parse_null_returns_none() {
        let result = Value::parse(&DataType::Int32, "NULL").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_decimal() {
        let dtype = DataType::Decimal {
            precision: 10,
            scale: 2,
        };
        assert_eq!(parse(&dtype, "3.14"), Value::Decimal(Decimal::new(314, 2)));
    }

    #[test]
    fn test_parse_date() {
        let dtype = DataType::Date;
        let expected = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        assert_eq!(parse(&dtype, "2024-01-15"), Value::Date(expected));
    }

    #[test]
    fn test_parse_time() {
        let dtype = DataType::Time;
        let expected = NaiveTime::from_hms_opt(12, 34, 56).unwrap();
        assert_eq!(parse(&dtype, "12:34:56"), Value::Time(expected));
    }

    #[test]
    fn test_parse_timetz() {
        let dtype = DataType::TimeTz;
        let parsed = parse(&dtype, "12:34:56+02");
        match parsed {
            Value::TimeTz(t) => {
                assert_eq!(t.time, NaiveTime::from_hms_opt(12, 34, 56).unwrap());
                assert_eq!(t.offset, chrono::FixedOffset::east_opt(7200).unwrap());
            }
            other => panic!("expected TimeTz, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_timestamp_tz() {
        let dtype = DataType::TimestampTz;
        let parsed = parse(&dtype, "2024-01-15T12:34:56Z");
        match parsed {
            Value::TimestampTz(ts) => {
                assert_eq!(
                    ts,
                    DateTime::<Utc>::from_str("2024-01-15T12:34:56Z").unwrap()
                );
            }
            other => panic!("expected TimestampTz, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_timestamp_strips_tz() {
        let dtype = DataType::Timestamp {
            precision: TimestampPrecision::Microseconds,
        };
        let parsed = parse(&dtype, "2024-01-15T12:34:56Z");
        match parsed {
            Value::Timestamp(ts) => {
                assert_eq!(
                    ts,
                    NaiveDate::from_ymd_opt(2024, 1, 15)
                        .unwrap()
                        .and_hms_opt(12, 34, 56)
                        .unwrap()
                );
            }
            other => panic!("expected Timestamp, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_blob() {
        assert_eq!(
            parse(&DataType::Blob, "deadbeef"),
            Value::Blob(vec![0xde, 0xad, 0xbe, 0xef])
        );
    }

    #[test]
    fn test_parse_uuid() {
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(
            parse(&DataType::Uuid, uuid_str),
            Value::Uuid(Uuid::from_str(uuid_str).unwrap())
        );
    }

    #[test]
    fn test_parse_list() {
        let dtype = DataType::list(DataType::Int32);
        let parsed = parse(&dtype, "[1, 2, 3]");
        assert_eq!(
            parsed,
            Value::List(vec![
                Some(Value::Int32(1)),
                Some(Value::Int32(2)),
                Some(Value::Int32(3)),
            ])
        );
    }

    #[test]
    fn test_parse_list_with_null_element() {
        let dtype = DataType::list(DataType::Int32);
        let parsed = parse(&dtype, "[1, NULL, 3]");
        assert_eq!(
            parsed,
            Value::List(vec![Some(Value::Int32(1)), None, Some(Value::Int32(3)),])
        );
    }

    #[test]
    fn test_parse_struct() {
        let dtype = DataType::struct_(vec![
            Column::new("a".into(), DataType::Int32),
            Column::new("b".into(), DataType::Varchar),
        ]);
        let parsed = parse(&dtype, "{a: 1, b: hello}");
        let mut expected: IndexMap<String, Option<Value>> = IndexMap::new();
        expected.insert("a".to_string(), Some(Value::Int32(1)));
        expected.insert("b".to_string(), Some(Value::Varchar("hello".to_string())));
        assert_eq!(parsed, Value::Struct(expected));
    }

    #[test]
    fn test_parse_struct_unknown_field_errors() {
        let dtype = DataType::struct_(vec![Column::new("a".into(), DataType::Int32)]);
        assert!(Value::parse(&dtype, "{a: 1, b: 2}").is_err());
    }

    #[test]
    fn test_parse_map() {
        let dtype = DataType::map(DataType::Varchar, DataType::Int32);
        let parsed = parse(&dtype, "{a=1, b=2}");
        assert_eq!(
            parsed,
            Value::Map(vec![
                (Value::Varchar("a".to_string()), Some(Value::Int32(1))),
                (Value::Varchar("b".to_string()), Some(Value::Int32(2))),
            ])
        );
    }

    #[test]
    fn test_parse_map_null_key_errors() {
        let dtype = DataType::map(DataType::Varchar, DataType::Int32);
        assert!(Value::parse(&dtype, "{NULL=1}").is_err());
    }

    /* -------------------------------------- FORMAT / DISPLAY ------------------------------------- */

    #[rstest]
    #[case(Value::Boolean(true), "true")]
    #[case(Value::Int32(-42), "-42")]
    #[case(Value::Int64(100), "100")]
    #[case(Value::UInt32(u32::MAX), "4294967295")]
    #[case(Value::Float64(3.25), "3.25")]
    #[case(Value::Varchar("hello".to_string()), "hello")]
    #[case(Value::Json("{\"a\":1}".to_string()), "{\"a\":1}")]
    #[case(Value::Blob(vec![0xde, 0xad]), "dead")]
    fn test_primitive_display(#[case] value: Value, #[case] expected: &str) {
        assert_eq!(value.to_string(), expected);
    }

    #[test]
    fn test_display_date() {
        let value = Value::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        assert_eq!(value.to_string(), "2024-01-15");
    }

    #[test]
    fn test_display_list_with_nulls() {
        let value = Value::List(vec![Some(Value::Int32(1)), None, Some(Value::Int32(3))]);
        assert_eq!(value.to_string(), "[1, NULL, 3]");
    }

    #[test]
    fn test_display_struct() {
        let mut inner: IndexMap<String, Option<Value>> = IndexMap::new();
        inner.insert("a".to_string(), Some(Value::Int32(1)));
        inner.insert("b".to_string(), None);
        let value = Value::Struct(inner);
        assert_eq!(value.to_string(), "{a: 1, b: NULL}");
    }

    #[test]
    fn test_display_map() {
        let value = Value::Map(vec![
            (Value::Varchar("a".to_string()), Some(Value::Int32(1))),
            (Value::Varchar("b".to_string()), None),
        ]);
        assert_eq!(value.to_string(), "{a=1, b=NULL}");
    }

    #[test]
    fn test_to_string_opt_none() {
        assert_eq!(Value::to_string_opt(None), "NULL");
    }

    #[test]
    fn test_to_string_opt_some() {
        assert_eq!(Value::to_string_opt(Some(&Value::Int32(42))), "42");
    }

    /* ------------------------------------------ ROUNDTRIP ---------------------------------------- */

    #[rstest]
    #[case(DataType::Boolean, Value::Boolean(false))]
    #[case(DataType::Int32, Value::Int32(-42))]
    #[case(DataType::UInt64, Value::UInt64(u64::MAX))]
    #[case(DataType::Varchar, Value::Varchar("hello".to_string()))]
    #[case(DataType::Blob, Value::Blob(vec![1, 2, 3]))]
    fn test_roundtrip(#[case] dtype: DataType, #[case] value: Value) {
        let serialized = value.to_string();
        let parsed = Value::parse(&dtype, &serialized).unwrap().unwrap();
        assert_eq!(parsed, value);
    }

    #[test]
    fn test_roundtrip_timetz() {
        let original = Value::TimeTz(TimeWithTimezone {
            time: NaiveTime::from_hms_opt(8, 15, 30).unwrap(),
            offset: chrono::FixedOffset::east_opt(5 * 3600 + 30 * 60).unwrap(),
        });
        let parsed = Value::parse(&DataType::TimeTz, &original.to_string())
            .unwrap()
            .unwrap();
        assert_eq!(parsed, original);
    }

    /* ---------------------------------------- COMPARISON ----------------------------------------- */

    #[test]
    fn test_partial_cmp_same_variant() {
        assert!(Value::Int32(1) < Value::Int32(2));
        assert_eq!(
            Value::Varchar("a".into()).partial_cmp(&Value::Varchar("a".into())),
            Some(std::cmp::Ordering::Equal)
        );
    }

    #[test]
    fn test_partial_cmp_different_variants_is_none() {
        assert_eq!(Value::Int32(1).partial_cmp(&Value::Int64(1)), None);
    }

    #[test]
    fn test_partial_cmp_interval_is_none() {
        use crate::primitives::Interval;
        let a = Value::Interval(Interval {
            months: chrono::Months::new(1),
            delta: chrono::TimeDelta::zero(),
        });
        let b = a.clone();
        assert_eq!(a.partial_cmp(&b), None);
    }
}
