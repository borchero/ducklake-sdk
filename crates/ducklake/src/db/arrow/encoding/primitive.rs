use arrow_array::{Array, ArrayRef};
use arrow_schema::TimeUnit;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rust_decimal::Decimal;
use sqlx::Arguments;
use uuid::Uuid;

use super::{ArrayExtractor, TypeEncoder};
use crate::DucklakeResult;
use crate::io::arrow::conversion::{IntoLogical, IntoLogicalWithContext};
use crate::primitives::{Interval, TimeWithTimezone};
use crate::spec::literals;

macro_rules! impl_array_extractor {
    ($name:ident, $encode_fn:ident, $value_ty:ty) => {
        impl $name {
            #[inline]
            fn option_value(&self, row_idx: usize) -> Option<$value_ty> {
                if self.array.is_null(row_idx) {
                    None
                } else {
                    Some(self.value(row_idx))
                }
            }
        }

        impl<E: TypeEncoder> ArrayExtractor<E> for $name {
            fn extract(&self, args: &mut E::Arguments, row_idx: usize) -> DucklakeResult<()> {
                let value = E::$encode_fn(self.option_value(row_idx));
                args.add(value).map_err(sqlx::Error::Encode)?;
                Ok(())
            }

            fn extract_text(&self, row_idx: usize) -> String {
                literals::format(self.option_value(row_idx).as_ref())
            }
        }
    };
}

/* ----------------------------------------- PRIMITIVES ---------------------------------------- */

macro_rules! primitive_extractor {
    ($name:ident, $array:ty, $value_ty:ty, $encode_fn:ident) => {
        pub struct $name {
            array: $array,
        }

        impl $name {
            pub fn new(array: &ArrayRef) -> Self {
                Self {
                    array: array.as_any().downcast_ref::<$array>().unwrap().clone(),
                }
            }

            #[inline]
            fn value(&self, row_idx: usize) -> $value_ty {
                self.array.value(row_idx).into_logical()
            }
        }

        impl_array_extractor!($name, $encode_fn, $value_ty);
    };
}

primitive_extractor! {BooleanArrayExtractor, arrow_array::BooleanArray, bool, encode_bool}
primitive_extractor! {Int8ArrayExtractor, arrow_array::Int8Array, i8, encode_i8}
primitive_extractor! {Int16ArrayExtractor, arrow_array::Int16Array, i16, encode_i16}
primitive_extractor! {Int32ArrayExtractor, arrow_array::Int32Array, i32, encode_i32}
primitive_extractor! {Int64ArrayExtractor, arrow_array::Int64Array, i64, encode_i64}
primitive_extractor! {UInt8ArrayExtractor, arrow_array::UInt8Array, u8, encode_u8}
primitive_extractor! {UInt16ArrayExtractor, arrow_array::UInt16Array, u16, encode_u16}
primitive_extractor! {UInt32ArrayExtractor, arrow_array::UInt32Array, u32, encode_u32}
primitive_extractor! {UInt64ArrayExtractor, arrow_array::UInt64Array, u64, encode_u64}
primitive_extractor! {Float32ArrayExtractor, arrow_array::Float32Array, f32, encode_f32}
primitive_extractor! {Float64ArrayExtractor, arrow_array::Float64Array, f64, encode_f64}
primitive_extractor! {Int128ArrayExtractor, arrow_array::FixedSizeBinaryArray, i128, encode_i128}
primitive_extractor! {UInt128ArrayExtractor, arrow_array::FixedSizeBinaryArray, u128, encode_u128}
primitive_extractor! {TimeTzArrayExtractor, arrow_array::FixedSizeBinaryArray, TimeWithTimezone, encode_time_tz}
primitive_extractor! {DateArrayExtractor, arrow_array::Date32Array, NaiveDate, encode_date}
primitive_extractor! {TimestampTzArrayExtractor, arrow_array::TimestampMicrosecondArray, DateTime<Utc>, encode_timestamp_tz}
primitive_extractor! {IntervalArrayExtractor, arrow_array::IntervalMonthDayNanoArray, Interval, encode_interval}
primitive_extractor! {UuidArrayExtractor, arrow_array::FixedSizeBinaryArray, Uuid, encode_uuid}

/* ------------------------------------------ DECIMAL ------------------------------------------ */

pub struct DecimalArrayExtractor {
    array: arrow_array::Decimal128Array,
    scale: u32,
}

impl DecimalArrayExtractor {
    pub fn new(array: &ArrayRef, scale: u8) -> Self {
        Self {
            array: array
                .as_any()
                .downcast_ref::<arrow_array::Decimal128Array>()
                .unwrap()
                .clone(),
            scale: scale as u32,
        }
    }

    fn value(&self, row_idx: usize) -> Decimal {
        Decimal::from_i128_with_scale(self.array.value(row_idx), self.scale)
    }
}

impl_array_extractor!(DecimalArrayExtractor, encode_decimal, Decimal);

/* -------------------------------------------- TIME ------------------------------------------- */

macro_rules! primitive_time_unit_extractor {
    ($name:ident, $array:ty, $value_ty:ty, $encode_fn:ident, $unit:expr) => {
        pub struct $name {
            array: $array,
        }

        impl $name {
            pub fn new(array: &ArrayRef) -> Self {
                Self {
                    array: array.as_any().downcast_ref::<$array>().unwrap().clone(),
                }
            }

            #[inline]
            fn value(&self, row_idx: usize) -> $value_ty {
                self.array.value(row_idx).into_logical_with_context(&$unit)
            }
        }

        impl_array_extractor!($name, $encode_fn, $value_ty);
    };
}

primitive_time_unit_extractor! {TimeMicrosecondArrayExtractor, arrow_array::Time64MicrosecondArray, NaiveTime, encode_time, TimeUnit::Microsecond}
primitive_time_unit_extractor! {TimeNanosecondArrayExtractor, arrow_array::Time64NanosecondArray, NaiveTime, encode_time, TimeUnit::Nanosecond}

/* ----------------------------------------- TIMESTAMP ----------------------------------------- */

primitive_time_unit_extractor! {TimestampSecondArrayExtractor, arrow_array::TimestampSecondArray, NaiveDateTime, encode_timestamp, TimeUnit::Second}
primitive_time_unit_extractor! {TimestampMillisecondArrayExtractor, arrow_array::TimestampMillisecondArray, NaiveDateTime, encode_timestamp, TimeUnit::Millisecond}
primitive_time_unit_extractor! {TimestampMicrosecondArrayExtractor, arrow_array::TimestampMicrosecondArray, NaiveDateTime, encode_timestamp, TimeUnit::Microsecond}
primitive_time_unit_extractor! {TimestampNanosecondArrayExtractor, arrow_array::TimestampNanosecondArray, NaiveDateTime, encode_timestamp, TimeUnit::Nanosecond}

/* ------------------------------------------ STRING ------------------------------------------- */

pub struct StringArrayExtractor {
    array: arrow_array::StringViewArray,
}

impl StringArrayExtractor {
    pub fn new(array: &ArrayRef) -> Self {
        Self {
            array: array
                .as_any()
                .downcast_ref::<arrow_array::StringViewArray>()
                .unwrap()
                .clone(),
        }
    }

    fn value(&self, row_idx: usize) -> &str {
        self.array.value(row_idx)
    }

    fn option_value(&self, row_idx: usize) -> Option<&str> {
        if self.array.is_null(row_idx) {
            None
        } else {
            Some(self.value(row_idx))
        }
    }
}

impl<E: TypeEncoder> ArrayExtractor<E> for StringArrayExtractor {
    fn extract(&self, args: &mut E::Arguments, row_idx: usize) -> DucklakeResult<()> {
        let value = E::encode_string(self.option_value(row_idx));
        args.add(value).map_err(sqlx::Error::Encode)?;
        Ok(())
    }

    fn extract_text(&self, row_idx: usize) -> String {
        literals::format(self.option_value(row_idx).map(str::to_string).as_ref())
    }
}

/* ------------------------------------------ BINARY ------------------------------------------- */

pub struct BinaryArrayExtractor {
    array: arrow_array::BinaryViewArray,
}

impl BinaryArrayExtractor {
    pub fn new(array: &ArrayRef) -> Self {
        Self {
            array: array
                .as_any()
                .downcast_ref::<arrow_array::BinaryViewArray>()
                .unwrap()
                .clone(),
        }
    }

    fn value(&self, row_idx: usize) -> &[u8] {
        self.array.value(row_idx)
    }

    fn option_value(&self, row_idx: usize) -> Option<&[u8]> {
        if self.array.is_null(row_idx) {
            None
        } else {
            Some(self.value(row_idx))
        }
    }
}

impl<E: TypeEncoder> ArrayExtractor<E> for BinaryArrayExtractor {
    fn extract(&self, args: &mut E::Arguments, row_idx: usize) -> DucklakeResult<()> {
        let value = E::encode_binary(self.option_value(row_idx));
        args.add(value).map_err(sqlx::Error::Encode)?;
        Ok(())
    }

    fn extract_text(&self, row_idx: usize) -> String {
        literals::format(self.option_value(row_idx).map(<[u8]>::to_vec).as_ref())
    }
}
