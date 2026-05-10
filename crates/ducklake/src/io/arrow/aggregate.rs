use arrow_arith::aggregate;
use arrow_schema::TimeUnit as ArrowTimeUnit;

use crate::io::arrow::conversion::{IntoLogical, IntoLogicalWithContext};
use crate::{DataType, Value};

macro_rules! aggregate {
    ($array:expr, $array_type:ident, $fn:ident, $value_type:ident) => {{
        let array = $array
            .as_any()
            .downcast_ref::<arrow_array::$array_type>()
            .unwrap();
        aggregate::$fn(array).map(|v| Value::$value_type(v.into_logical()))
    }};
    ($array:expr, $array_type:ident, $fn:ident, $value_type:ident, $ctx:expr) => {{
        let array = $array
            .as_any()
            .downcast_ref::<arrow_array::$array_type>()
            .unwrap();
        aggregate::$fn(array).map(|v| Value::$value_type(v.into_logical_with_context(&$ctx)))
    }};
}

pub fn find_min(data_type: &DataType, array: &arrow_array::ArrayRef) -> Option<Value> {
    if array.is_empty() {
        return None;
    }
    match data_type {
        DataType::Boolean => aggregate!(array, BooleanArray, min_boolean, Boolean),
        DataType::Int8 => aggregate!(array, Int8Array, min, Int8),
        DataType::Int16 => aggregate!(array, Int16Array, min, Int16),
        DataType::Int32 => aggregate!(array, Int32Array, min, Int32),
        DataType::Int64 => aggregate!(array, Int64Array, min, Int64),
        DataType::Int128 => {
            aggregate!(array, FixedSizeBinaryArray, min_fixed_size_binary, Int128)
        }
        DataType::UInt8 => aggregate!(array, UInt8Array, min, UInt8),
        DataType::UInt16 => aggregate!(array, UInt16Array, min, UInt16),
        DataType::UInt32 => aggregate!(array, UInt32Array, min, UInt32),
        DataType::UInt64 => aggregate!(array, UInt64Array, min, UInt64),
        DataType::UInt128 => {
            aggregate!(array, FixedSizeBinaryArray, min_fixed_size_binary, UInt128)
        }
        DataType::Float32 => aggregate!(array, Float32Array, min, Float32),
        DataType::Float64 => aggregate!(array, Float64Array, min, Float64),
        DataType::Decimal {
            precision: _,
            scale,
        } => {
            aggregate!(array, Decimal128Array, min, Decimal, *scale)
        }
        DataType::Time => arrow_match_time!(array.data_type(),
            microsecond => aggregate!(array, Time64MicrosecondArray, min, Time, ArrowTimeUnit::Microsecond),
            nanosecond => aggregate!(array, Time64NanosecondArray, min, Time, ArrowTimeUnit::Nanosecond)
        ),
        DataType::TimeTz => aggregate!(array, FixedSizeBinaryArray, min_fixed_size_binary, TimeTz),
        DataType::Date => aggregate!(array, Date32Array, min, Date),
        DataType::Timestamp { precision } => aggregate!(
            array,
            TimestampMicrosecondArray,
            min,
            Timestamp,
            (*precision).into()
        ),
        DataType::TimestampTz => aggregate!(array, TimestampMicrosecondArray, min, TimestampTz),
        DataType::Interval => aggregate!(array, IntervalMonthDayNanoArray, min, Interval),
        DataType::Varchar => arrow_match_varchar!(array.data_type(),
            utf8 => aggregate!(array, StringArray, min_string, Varchar),
            large_utf8 => aggregate!(array, LargeStringArray, min_string, Varchar),
            utf8_view => aggregate!(array, StringViewArray, min_string_view, Varchar)
        ),
        DataType::Blob => arrow_match_binary!(array.data_type(),
            binary => aggregate!(array, BinaryArray, min_binary, Blob),
            large_binary => aggregate!(array, LargeBinaryArray, min_binary, Blob),
            binary_view => aggregate!(array, BinaryViewArray, min_binary_view, Blob)
        ),
        DataType::Json => arrow_match_varchar!(array.data_type(),
            utf8 => aggregate!(array, StringArray, min_string, Varchar),
            large_utf8 => aggregate!(array, LargeStringArray, min_string, Varchar),
            utf8_view => aggregate!(array, StringViewArray, min_string_view, Varchar)
        ),
        DataType::Uuid => aggregate!(array, FixedSizeBinaryArray, min_fixed_size_binary, Uuid),
        // Nested types are not supported: they always have a `None` min value
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _) => None,
    }
}

pub fn find_max(data_type: &DataType, array: &arrow_array::ArrayRef) -> Option<Value> {
    if array.is_empty() {
        return None;
    }
    match data_type {
        DataType::Boolean => aggregate!(array, BooleanArray, max_boolean, Boolean),
        DataType::Int8 => aggregate!(array, Int8Array, max, Int8),
        DataType::Int16 => aggregate!(array, Int16Array, max, Int16),
        DataType::Int32 => aggregate!(array, Int32Array, max, Int32),
        DataType::Int64 => aggregate!(array, Int64Array, max, Int64),
        DataType::Int128 => {
            aggregate!(array, FixedSizeBinaryArray, max_fixed_size_binary, Int128)
        }
        DataType::UInt8 => aggregate!(array, UInt8Array, max, UInt8),
        DataType::UInt16 => aggregate!(array, UInt16Array, max, UInt16),
        DataType::UInt32 => aggregate!(array, UInt32Array, max, UInt32),
        DataType::UInt64 => aggregate!(array, UInt64Array, max, UInt64),
        DataType::UInt128 => {
            aggregate!(array, FixedSizeBinaryArray, max_fixed_size_binary, UInt128)
        }
        DataType::Float32 => aggregate!(array, Float32Array, max, Float32),
        DataType::Float64 => aggregate!(array, Float64Array, max, Float64),
        DataType::Decimal {
            precision: _,
            scale,
        } => {
            aggregate!(array, Decimal128Array, max, Decimal, *scale)
        }
        DataType::Time => arrow_match_time!(array.data_type(),
            microsecond => aggregate!(array, Time64MicrosecondArray, max, Time, ArrowTimeUnit::Microsecond),
            nanosecond => aggregate!(array, Time64NanosecondArray, max, Time, ArrowTimeUnit::Nanosecond)
        ),
        DataType::TimeTz => aggregate!(array, FixedSizeBinaryArray, max_fixed_size_binary, TimeTz),
        DataType::Date => aggregate!(array, Date32Array, max, Date),
        DataType::Timestamp { precision } => aggregate!(
            array,
            TimestampMicrosecondArray,
            max,
            Timestamp,
            (*precision).into()
        ),
        DataType::TimestampTz => aggregate!(array, TimestampMicrosecondArray, max, TimestampTz),
        DataType::Interval => aggregate!(array, IntervalMonthDayNanoArray, max, Interval),
        DataType::Varchar => arrow_match_varchar!(array.data_type(),
            utf8 => aggregate!(array, StringArray, max_string, Varchar),
            large_utf8 => aggregate!(array, LargeStringArray, max_string, Varchar),
            utf8_view => aggregate!(array, StringViewArray, max_string_view, Varchar)
        ),
        DataType::Blob => arrow_match_binary!(array.data_type(),
            binary => aggregate!(array, BinaryArray, max_binary, Blob),
            large_binary => aggregate!(array, LargeBinaryArray, max_binary, Blob),
            binary_view => aggregate!(array, BinaryViewArray, max_binary_view, Blob)
        ),
        DataType::Json => arrow_match_varchar!(array.data_type(),
            utf8 => aggregate!(array, StringArray, max_string, Varchar),
            large_utf8 => aggregate!(array, LargeStringArray, max_string, Varchar),
            utf8_view => aggregate!(array, StringViewArray, max_string_view, Varchar)
        ),
        DataType::Uuid => aggregate!(array, FixedSizeBinaryArray, max_fixed_size_binary, Uuid),
        // Nested types are not supported: they always have a `None` max value
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _) => None,
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                             TESTS                                             */
/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::sync::Arc;

    use arrow_array::ArrayRef;

    use super::*;
    use crate::Column;

    #[test]
    fn test_find_min_max_int32() {
        let array: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![
            Some(3),
            None,
            Some(1),
            Some(2),
        ]));
        assert_eq!(find_min(&DataType::Int32, &array), Some(Value::Int32(1)));
        assert_eq!(find_max(&DataType::Int32, &array), Some(Value::Int32(3)));
    }

    #[test]
    fn test_find_min_max_float64_ignores_none() {
        let array: ArrayRef = Arc::new(arrow_array::Float64Array::from(vec![
            Some(-1.0),
            Some(2.5),
            Some(0.0),
        ]));
        assert_eq!(
            find_min(&DataType::Float64, &array),
            Some(Value::Float64(-1.0))
        );
        assert_eq!(
            find_max(&DataType::Float64, &array),
            Some(Value::Float64(2.5))
        );
    }

    #[test]
    fn test_find_min_max_boolean() {
        let array: ArrayRef = Arc::new(arrow_array::BooleanArray::from(vec![
            Some(false),
            Some(true),
            Some(false),
        ]));
        assert_eq!(
            find_min(&DataType::Boolean, &array),
            Some(Value::Boolean(false))
        );
        assert_eq!(
            find_max(&DataType::Boolean, &array),
            Some(Value::Boolean(true))
        );
    }

    #[test]
    fn test_find_min_max_string() {
        let array: ArrayRef = Arc::new(arrow_array::StringArray::from(vec![
            "banana", "apple", "cherry",
        ]));
        assert_eq!(
            find_min(&DataType::Varchar, &array),
            Some(Value::Varchar("apple".to_string()))
        );
        assert_eq!(
            find_max(&DataType::Varchar, &array),
            Some(Value::Varchar("cherry".to_string()))
        );
    }

    #[test]
    fn test_find_min_max_empty_array_is_none() {
        let array: ArrayRef = Arc::new(arrow_array::Int32Array::from(Vec::<i32>::new()));
        assert_eq!(find_min(&DataType::Int32, &array), None);
        assert_eq!(find_max(&DataType::Int32, &array), None);
    }

    #[test]
    fn test_find_min_max_all_null_is_none() {
        let array: ArrayRef =
            Arc::new(arrow_array::Int32Array::from(vec![None::<i32>, None, None]));
        assert_eq!(find_min(&DataType::Int32, &array), None);
        assert_eq!(find_max(&DataType::Int32, &array), None);
    }

    #[test]
    fn test_find_min_max_nested_types_returns_none() {
        // For nested types, the function returns None regardless of array content.
        let array: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]));
        let list_dtype = DataType::list(DataType::Int32);
        let struct_dtype = DataType::struct_(vec![Column::new("a".into(), DataType::Int32)]);
        let map_dtype = DataType::map(DataType::Varchar, DataType::Int32);
        assert_eq!(find_min(&list_dtype, &array), None);
        assert_eq!(find_max(&list_dtype, &array), None);
        assert_eq!(find_min(&struct_dtype, &array), None);
        assert_eq!(find_max(&struct_dtype, &array), None);
        assert_eq!(find_min(&map_dtype, &array), None);
        assert_eq!(find_max(&map_dtype, &array), None);
    }

    #[test]
    fn test_find_min_max_varchar_large_utf8() {
        let array: ArrayRef =
            Arc::new(arrow_array::LargeStringArray::from(vec!["zebra", "apple"]));
        assert_eq!(
            find_min(&DataType::Varchar, &array),
            Some(Value::Varchar("apple".to_string()))
        );
        assert_eq!(
            find_max(&DataType::Varchar, &array),
            Some(Value::Varchar("zebra".to_string()))
        );
    }

    #[test]
    fn test_find_min_max_blob() {
        let array: ArrayRef = Arc::new(arrow_array::BinaryArray::from(vec![
            &[0xff, 0x00][..],
            &[0x00, 0x01][..],
        ]));
        assert_eq!(
            find_min(&DataType::Blob, &array),
            Some(Value::Blob(vec![0x00, 0x01]))
        );
        assert_eq!(
            find_max(&DataType::Blob, &array),
            Some(Value::Blob(vec![0xff, 0x00]))
        );
    }
}
