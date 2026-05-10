use std::sync::Arc;

use arrow_array::{ArrayRef, builder as arrow_builder};
use arrow_schema::TimeUnit;
use rust_decimal::Decimal;

use super::{ArrayAppender, TypeDecoder};
use crate::DucklakeResult;
use crate::io::arrow::conversion::{IntoPhysical, IntoPhysicalWithContext};
use crate::primitives::{Interval, TimeWithTimezone};
use crate::spec::literals;

macro_rules! impl_array_appender {
    ($name:ident, $decode_fn:ident) => {
        impl<D: TypeDecoder> ArrayAppender<D> for $name {
            fn append(&mut self, row: &D::Row, name: &str) -> DucklakeResult<()> {
                self.append_option(D::$decode_fn(row, name)?);
                Ok(())
            }

            fn append_text(&mut self, text: &str) -> DucklakeResult<()> {
                self.append_option(literals::parse(text)?);
                Ok(())
            }

            fn append_null(&mut self) {
                self.builder.append_null();
            }

            fn finish(&mut self) -> ArrayRef {
                Arc::new(self.builder.finish())
            }
        }
    };
}

macro_rules! impl_array_appender_into_physical {
    ($name:ident, $decode_fn:ident, $decode_ty:ty) => {
        impl $name {
            #[inline]
            fn append_option(&mut self, value: Option<$decode_ty>) {
                self.builder.append_option(value.map(|v| v.into_physical()));
            }
        }

        impl_array_appender!($name, $decode_fn);
    };
}

/* ----------------------------------------- PRIMITIVES ---------------------------------------- */

macro_rules! primitive_appender {
    ($name:ident, $builder:ty, $decode_fn:ident, $decode_ty:ty) => {
        pub struct $name {
            builder: $builder,
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    builder: <$builder>::new(),
                }
            }
        }

        impl_array_appender_into_physical!($name, $decode_fn, $decode_ty);
    };
}

primitive_appender! {BooleanArrayAppender, arrow_builder::BooleanBuilder, decode_bool, bool}
primitive_appender! {Int8ArrayAppender, arrow_builder::Int8Builder, decode_i8, i8}
primitive_appender! {Int16ArrayAppender, arrow_builder::Int16Builder, decode_i16, i16}
primitive_appender! {Int32ArrayAppender, arrow_builder::Int32Builder, decode_i32, i32}
primitive_appender! {Int64ArrayAppender, arrow_builder::Int64Builder, decode_i64, i64}
primitive_appender! {UInt8ArrayAppender, arrow_builder::UInt8Builder, decode_u8, u8}
primitive_appender! {UInt16ArrayAppender, arrow_builder::UInt16Builder, decode_u16, u16}
primitive_appender! {UInt32ArrayAppender, arrow_builder::UInt32Builder, decode_u32, u32}
primitive_appender! {UInt64ArrayAppender, arrow_builder::UInt64Builder, decode_u64, u64}
primitive_appender! {Float32ArrayAppender, arrow_builder::Float32Builder, decode_f32, f32}
primitive_appender! {Float64ArrayAppender, arrow_builder::Float64Builder, decode_f64, f64}
primitive_appender! {DateArrayAppender, arrow_builder::Date32Builder, decode_date, chrono::NaiveDate}
primitive_appender! {TimeArrayAppender, arrow_builder::Time64MicrosecondBuilder, decode_time, chrono::NaiveTime}
primitive_appender! {IntervalArrayAppender, arrow_builder::IntervalMonthDayNanoBuilder, decode_interval, Interval}
primitive_appender! {StringViewArrayAppender, arrow_builder::StringViewBuilder, decode_string, String}
primitive_appender! {LargeBinaryArrayAppender, arrow_builder::LargeBinaryBuilder, decode_binary, Vec<u8>}

/* ------------------------------------------ BINARIES ----------------------------------------- */

macro_rules! binary_appender {
    ($name:ident, $size:expr, $decode_fn:ident, $decode_ty:ty) => {
        pub struct $name {
            builder: arrow_builder::FixedSizeBinaryBuilder,
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    builder: arrow_builder::FixedSizeBinaryBuilder::new($size),
                }
            }

            #[inline]
            fn append_option(&mut self, value: Option<$decode_ty>) {
                if let Some(value) = value {
                    self.builder.append_value(value.into_physical()).unwrap();
                } else {
                    self.builder.append_null();
                }
            }
        }

        impl_array_appender!($name, $decode_fn);
    };
}

binary_appender! {Int128ArrayAppender, 16, decode_i128, i128}
binary_appender! {UInt128ArrayAppender, 16, decode_u128, u128}
binary_appender! {TimeTzArrayAppender, 8, decode_time_tz, TimeWithTimezone}
binary_appender! {UuidArrayAppender, 16, decode_uuid, uuid::Uuid}

/* ------------------------------------------ DECIMAL ------------------------------------------ */

pub struct DecimalArrayAppender {
    builder: arrow_builder::Decimal128Builder,
}

impl DecimalArrayAppender {
    pub fn new(precision: u8, scale: i8) -> DucklakeResult<Self> {
        let builder =
            arrow_builder::Decimal128Builder::new().with_precision_and_scale(precision, scale)?;
        Ok(Self { builder })
    }

    fn append_option(&mut self, value: Option<Decimal>) {
        self.builder.append_option(value.map(|v| v.into_physical()));
    }
}

impl_array_appender!(DecimalArrayAppender, decode_decimal);

/* ----------------------------------------- TIMESTAMP ----------------------------------------- */

macro_rules! timestamp_appender {
    ($name:ident, $builder:ty, $unit:expr) => {
        pub struct $name {
            builder: $builder,
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    builder: <$builder>::new(),
                }
            }

            fn append_option(&mut self, value: Option<chrono::NaiveDateTime>) {
                self.builder
                    .append_option(value.map(|v| v.into_physical_with_context(&$unit)));
            }
        }

        impl_array_appender!($name, decode_timestamp);
    };
}

timestamp_appender! {TimestampSecondArrayAppender, arrow_builder::TimestampSecondBuilder, TimeUnit::Second}
timestamp_appender! {TimestampMillisecondArrayAppender, arrow_builder::TimestampMillisecondBuilder, TimeUnit::Millisecond}
timestamp_appender! {TimestampMicrosecondArrayAppender, arrow_builder::TimestampMicrosecondBuilder, TimeUnit::Microsecond}
timestamp_appender! {TimestampNanosecondArrayAppender, arrow_builder::TimestampNanosecondBuilder, TimeUnit::Nanosecond}

/* ---------------------------------------- TIMESTAMPTZ ---------------------------------------- */

pub struct TimestampTzArrayAppender {
    builder: arrow_builder::TimestampMicrosecondBuilder,
}

impl TimestampTzArrayAppender {
    pub fn new() -> Self {
        Self {
            builder: arrow_builder::TimestampMicrosecondBuilder::new().with_timezone("UTC"),
        }
    }
}

impl_array_appender_into_physical!(
    TimestampTzArrayAppender,
    decode_timestamp_tz,
    chrono::DateTime<chrono::Utc>
);
