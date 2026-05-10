use arrow_array::types::{Date32Type, IntervalMonthDayNano};
use arrow_schema::TimeUnit;
use chrono::{
    DateTime,
    FixedOffset,
    Months,
    NaiveDate,
    NaiveDateTime,
    NaiveTime,
    TimeDelta,
    Timelike,
    Utc,
};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::primitives::{Interval, TimeWithTimezone};

/* --------------------------------------------------------------------------------------------- */
/*                                          IMPL TRAITS                                          */
/* --------------------------------------------------------------------------------------------- */

/// Trait for logical types that can be constructed from their physical Arrow representation.
pub trait FromPhysical {
    type ArrowType: ?Sized;

    fn from_physical(value: &Self::ArrowType) -> Self;
}

/// Trait for logical types that can be constructed from their physical Arrow representation with
/// some additional context.
pub trait FromPhysicalWithContext {
    type ArrowType: ?Sized;
    type Context;

    fn from_physical_with_context(value: &Self::ArrowType, context: &Self::Context) -> Self;
}

/// Trait for logical types that can be converted into their physical Arrow representation.
pub trait IntoPhysical {
    type ArrowType;

    fn into_physical(self) -> Self::ArrowType;
}

/// Trait for logical types that can be converted into their physical Arrow representation with
/// some additional context.
pub trait IntoPhysicalWithContext {
    type ArrowType;
    type Context;

    fn into_physical_with_context(self, context: &Self::Context) -> Self::ArrowType;
}

/* --------------------------------------------------------------------------------------------- */
/*                                       SYNTHESIZED TRAITS                                      */
/* --------------------------------------------------------------------------------------------- */

/// Trait for physical Arrow types that can be converted into their logical representation.
/// This trait is auto-synthesized for any type that implements `FromPhysical`.
pub trait IntoLogical<Logical> {
    fn into_logical(self) -> Logical;
}

impl<Physical: ?Sized, Logical> IntoLogical<Logical> for &Physical
where
    Logical: FromPhysical<ArrowType = Physical>,
{
    fn into_logical(self) -> Logical {
        Logical::from_physical(self)
    }
}

/// Trait for physical Arrow types that can be converted into their logical representation with
/// some additional context. This trait is auto-synthesized for any type that implements
/// `FromPhysicalWithContext`.
pub trait IntoLogicalWithContext<T, C> {
    fn into_logical_with_context(self, context: &C) -> T;
}

impl<T: ?Sized, Logical, Context> IntoLogicalWithContext<Logical, Context> for &T
where
    Logical: FromPhysicalWithContext<ArrowType = T, Context = Context>,
{
    fn into_logical_with_context(self, context: &Context) -> Logical {
        Logical::from_physical_with_context(self, context)
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                             TYPES                                             */
/* --------------------------------------------------------------------------------------------- */

/* ----------------------------------------- PRIMITIVE ----------------------------------------- */

macro_rules! impl_copy {
    ($t:ty) => {
        impl FromPhysical for $t {
            type ArrowType = Self;
            fn from_physical(literal: &Self::ArrowType) -> Self {
                *literal
            }
        }

        impl IntoPhysical for $t {
            type ArrowType = Self;
            fn into_physical(self) -> $t {
                self
            }
        }
    };
}

impl_copy!(bool);
impl_copy!(i8);
impl_copy!(i16);
impl_copy!(i32);
impl_copy!(i64);
impl_copy!(u8);
impl_copy!(u16);
impl_copy!(u32);
impl_copy!(u64);
impl_copy!(f32);
impl_copy!(f64);

/* -------------------------------------------- i128 ------------------------------------------- */

impl FromPhysical for i128 {
    type ArrowType = [u8];

    fn from_physical(value: &Self::ArrowType) -> Self {
        i128::from_le_bytes(value.try_into().unwrap())
    }
}

impl IntoPhysical for i128 {
    type ArrowType = [u8; 16];

    fn into_physical(self) -> [u8; 16] {
        self.to_le_bytes()
    }
}

/* -------------------------------------------- u128 ------------------------------------------- */

impl FromPhysical for u128 {
    type ArrowType = [u8];

    fn from_physical(value: &Self::ArrowType) -> Self {
        u128::from_le_bytes(value.try_into().unwrap())
    }
}

impl IntoPhysical for u128 {
    type ArrowType = [u8; 16];

    fn into_physical(self) -> [u8; 16] {
        self.to_le_bytes()
    }
}

/* ------------------------------------------ DECIMAL ------------------------------------------ */

impl FromPhysicalWithContext for Decimal {
    type ArrowType = i128;
    type Context = u8; // scale

    fn from_physical_with_context(value: &Self::ArrowType, context: &Self::Context) -> Self {
        Decimal::from_i128_with_scale(*value, *context as u32)
    }
}

impl IntoPhysical for Decimal {
    type ArrowType = i128;

    fn into_physical(self) -> Self::ArrowType {
        self.mantissa()
    }
}

/* ----------------------------------------- NAIVE TIME ---------------------------------------- */

impl FromPhysicalWithContext for NaiveTime {
    type ArrowType = i64;
    type Context = TimeUnit;

    fn from_physical_with_context(value: &Self::ArrowType, context: &Self::Context) -> Self {
        let (secs, nanos) = match context {
            TimeUnit::Microsecond => (*value / 1_000_000, (*value % 1_000_000) * 1000),
            TimeUnit::Nanosecond => (*value / 1_000_000_000, *value % 1_000_000_000),
            _ => unreachable!(),
        };
        NaiveTime::from_num_seconds_from_midnight_opt(secs as u32, nanos as u32).unwrap()
    }
}

impl IntoPhysical for NaiveTime {
    type ArrowType = i64;

    fn into_physical(self) -> Self::ArrowType {
        self.num_seconds_from_midnight() as i64 * 1_000_000 + (self.nanosecond() as i64 / 1000)
    }
}

/* ------------------------------------- TIME WITH TIMEZONE ------------------------------------ */

impl FromPhysical for TimeWithTimezone {
    type ArrowType = [u8];

    fn from_physical(value: &Self::ArrowType) -> Self {
        let mut micros_bytes = [0u8; 8];
        micros_bytes[..5].copy_from_slice(&value[0..5]);
        let micros = i64::from_le_bytes(micros_bytes);

        let mut offset_bytes = [0u8; 4];
        offset_bytes[..3].copy_from_slice(&value[5..8]);
        // Sign-extend the 24-bit offset to 32 bits.
        if offset_bytes[2] & 0x80 != 0 {
            offset_bytes[3] = 0xff;
        }
        let offset_secs = i32::from_le_bytes(offset_bytes);

        let time = micros.into_logical_with_context(&TimeUnit::Microsecond);
        let offset = FixedOffset::east_opt(offset_secs).unwrap();
        TimeWithTimezone { time, offset }
    }
}

impl IntoPhysical for TimeWithTimezone {
    type ArrowType = [u8; 8];

    fn into_physical(self) -> Self::ArrowType {
        let micros = self.time.into_physical().to_le_bytes();
        let offset = self.offset.local_minus_utc().to_le_bytes();
        let mut bytes = [0u8; 8];
        bytes[0..5].copy_from_slice(&micros[..5]);
        bytes[5..8].copy_from_slice(&offset[..3]);
        bytes
    }
}

/* ----------------------------------------- NAIVE DATE ---------------------------------------- */

impl FromPhysical for NaiveDate {
    type ArrowType = i32;

    fn from_physical(value: &Self::ArrowType) -> Self {
        Date32Type::to_naive_date_opt(*value).unwrap()
    }
}

impl IntoPhysical for NaiveDate {
    type ArrowType = i32;

    fn into_physical(self) -> Self::ArrowType {
        Date32Type::from_naive_date(self)
    }
}

/* --------------------------------------- NAIVE DATETIME -------------------------------------- */

impl FromPhysicalWithContext for NaiveDateTime {
    type ArrowType = i64;
    type Context = TimeUnit;

    fn from_physical_with_context(value: &Self::ArrowType, context: &Self::Context) -> Self {
        let date_time = match context {
            TimeUnit::Second => DateTime::from_timestamp_secs(*value).unwrap(),
            TimeUnit::Millisecond => DateTime::from_timestamp_millis(*value).unwrap(),
            TimeUnit::Microsecond => DateTime::from_timestamp_micros(*value).unwrap(),
            TimeUnit::Nanosecond => DateTime::from_timestamp_nanos(*value),
        };
        date_time.naive_utc()
    }
}

impl IntoPhysicalWithContext for NaiveDateTime {
    type ArrowType = i64;
    type Context = TimeUnit;

    fn into_physical_with_context(self, context: &Self::Context) -> Self::ArrowType {
        match context {
            TimeUnit::Second => self.and_utc().timestamp(),
            TimeUnit::Millisecond => self.and_utc().timestamp_millis(),
            TimeUnit::Microsecond => self.and_utc().timestamp_micros(),
            TimeUnit::Nanosecond => self.and_utc().timestamp_nanos_opt().unwrap(),
        }
    }
}

/* ------------------------------------------ DATETIME ----------------------------------------- */

impl FromPhysical for DateTime<Utc> {
    type ArrowType = i64;

    fn from_physical(value: &Self::ArrowType) -> Self {
        let naive_datetime: NaiveDateTime =
            value.into_logical_with_context(&TimeUnit::Microsecond);
        naive_datetime.and_utc()
    }
}

impl IntoPhysical for DateTime<Utc> {
    type ArrowType = i64;

    fn into_physical(self) -> Self::ArrowType {
        self.timestamp_micros()
    }
}

/* ------------------------------------------ INTERVAL ----------------------------------------- */

impl FromPhysical for Interval {
    type ArrowType = arrow_array::types::IntervalMonthDayNano;

    fn from_physical(value: &Self::ArrowType) -> Self {
        const MICROS_PER_DAY: i64 = 86_400_000_000;
        let micros = (value.days as i64) * MICROS_PER_DAY + value.nanoseconds / 1000;
        Interval {
            months: Months::new(value.months as u32),
            delta: TimeDelta::microseconds(micros),
        }
    }
}

impl IntoPhysical for Interval {
    type ArrowType = IntervalMonthDayNano;

    fn into_physical(self) -> Self::ArrowType {
        const MICROS_PER_DAY: i64 = 86_400_000_000;
        let num_days = self.delta.num_microseconds().unwrap_or(0) / MICROS_PER_DAY;
        let num_micros = self.delta.num_microseconds().unwrap_or(0) - (num_days * MICROS_PER_DAY);
        IntervalMonthDayNano::new(
            self.months.as_u32() as i32,
            num_days as i32,
            num_micros * 1000,
        )
    }
}

/* ------------------------------------------- STRING ------------------------------------------ */

impl FromPhysical for String {
    type ArrowType = str;

    fn from_physical(value: &Self::ArrowType) -> Self {
        value.to_string()
    }
}

impl IntoPhysical for String {
    type ArrowType = String;

    fn into_physical(self) -> Self::ArrowType {
        self.clone()
    }
}

/* -------------------------------------------- BLOB ------------------------------------------- */

impl FromPhysical for Vec<u8> {
    type ArrowType = [u8];

    fn from_physical(value: &Self::ArrowType) -> Self {
        value.to_vec()
    }
}

impl IntoPhysical for Vec<u8> {
    type ArrowType = Vec<u8>;

    fn into_physical(self) -> Self::ArrowType {
        self.clone()
    }
}

/* -------------------------------------------- UUID ------------------------------------------- */

impl FromPhysical for Uuid {
    type ArrowType = [u8];

    fn from_physical(value: &Self::ArrowType) -> Self {
        Uuid::from_slice(value).unwrap()
    }
}

impl IntoPhysical for Uuid {
    type ArrowType = [u8; 16];

    fn into_physical(self) -> Self::ArrowType {
        *self.as_bytes()
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
    #[case(true)]
    #[case(false)]
    fn test_bool_roundtrip(#[case] value: bool) {
        assert_eq!(bool::from_physical(&value.into_physical()), value);
    }

    #[rstest]
    #[case(i8::MIN)]
    #[case(-1)]
    #[case(0)]
    #[case(1)]
    #[case(i8::MAX)]
    fn test_i8_roundtrip(#[case] value: i8) {
        assert_eq!(i8::from_physical(&value.into_physical()), value);
    }

    #[rstest]
    #[case(i16::MIN)]
    #[case(-1)]
    #[case(0)]
    #[case(1)]
    #[case(i16::MAX)]
    fn test_i16_roundtrip(#[case] value: i16) {
        assert_eq!(i16::from_physical(&value.into_physical()), value);
    }

    #[rstest]
    #[case(i32::MIN)]
    #[case(-1)]
    #[case(0)]
    #[case(1)]
    #[case(i32::MAX)]
    fn test_i32_roundtrip(#[case] value: i32) {
        assert_eq!(i32::from_physical(&value.into_physical()), value);
    }

    #[rstest]
    #[case(i64::MIN)]
    #[case(-1)]
    #[case(0)]
    #[case(1)]
    #[case(i64::MAX)]
    fn test_i64_roundtrip(#[case] value: i64) {
        assert_eq!(i64::from_physical(&value.into_physical()), value);
    }

    #[rstest]
    #[case(u8::MIN)]
    #[case(1)]
    #[case(u8::MAX)]
    fn test_u8_roundtrip(#[case] value: u8) {
        assert_eq!(u8::from_physical(&value.into_physical()), value);
    }

    #[rstest]
    #[case(u16::MIN)]
    #[case(1)]
    #[case(u16::MAX)]
    fn test_u16_roundtrip(#[case] value: u16) {
        assert_eq!(u16::from_physical(&value.into_physical()), value);
    }

    #[rstest]
    #[case(u32::MIN)]
    #[case(1)]
    #[case(u32::MAX)]
    fn test_u32_roundtrip(#[case] value: u32) {
        assert_eq!(u32::from_physical(&value.into_physical()), value);
    }

    #[rstest]
    #[case(u64::MIN)]
    #[case(1)]
    #[case(u64::MAX)]
    fn test_u64_roundtrip(#[case] value: u64) {
        assert_eq!(u64::from_physical(&value.into_physical()), value);
    }

    #[rstest]
    #[case(f32::MIN)]
    #[case(-1.5)]
    #[case(0.0)]
    #[case(1.5)]
    #[case(f32::MAX)]
    fn test_f32_roundtrip(#[case] value: f32) {
        assert_eq!(f32::from_physical(&value.into_physical()), value);
    }

    #[rstest]
    #[case(f64::MIN)]
    #[case(-1.5)]
    #[case(0.0)]
    #[case(1.5)]
    #[case(f64::MAX)]
    fn test_f64_roundtrip(#[case] value: f64) {
        assert_eq!(f64::from_physical(&value.into_physical()), value);
    }

    #[rstest]
    #[case(i128::MIN)]
    #[case(-1)]
    #[case(0)]
    #[case(1)]
    #[case(i128::MAX)]
    fn test_i128_roundtrip(#[case] value: i128) {
        let physical = value.into_physical();
        assert_eq!(i128::from_physical(&physical), value);
    }

    #[rstest]
    #[case(u128::MIN)]
    #[case(1)]
    #[case(u128::MAX)]
    fn test_u128_roundtrip(#[case] value: u128) {
        let physical = value.into_physical();
        assert_eq!(u128::from_physical(&physical), value);
    }

    #[rstest]
    #[case(Decimal::new(0, 0), 0)]
    #[case(Decimal::new(12345, 2), 2)]
    #[case(Decimal::new(-12345, 4), 4)]
    #[case(Decimal::new(i64::MAX, 10), 10)]
    fn test_decimal_roundtrip(#[case] value: Decimal, #[case] scale: u8) {
        let physical = value.into_physical();
        assert_eq!(
            Decimal::from_physical_with_context(&physical, &scale),
            value
        );
    }

    #[rstest]
    #[case(NaiveTime::from_hms_opt(0, 0, 0).unwrap())]
    #[case(NaiveTime::from_hms_opt(12, 34, 56).unwrap())]
    #[case(NaiveTime::from_hms_micro_opt(23, 59, 59, 999_999).unwrap())]
    #[case(NaiveTime::from_hms_micro_opt(1, 2, 3, 456_789).unwrap())]
    fn test_naive_time_roundtrip(#[case] value: NaiveTime) {
        let physical = value.into_physical();
        assert_eq!(
            NaiveTime::from_physical_with_context(&physical, &TimeUnit::Microsecond),
            value
        );
    }

    #[rstest]
    #[case(NaiveTime::from_hms_opt(0, 0, 0).unwrap(), 0)]
    #[case(NaiveTime::from_hms_opt(12, 0, 0).unwrap(), 3600)]
    #[case(NaiveTime::from_hms_micro_opt(23, 59, 59, 999_999).unwrap(), -3600)]
    #[case(NaiveTime::from_hms_micro_opt(8, 30, 0, 123_456).unwrap(), 19_800)]
    #[case(NaiveTime::from_hms_opt(15, 0, 0).unwrap(), -28_800)]
    fn test_time_with_timezone_roundtrip(#[case] time: NaiveTime, #[case] offset_secs: i32) {
        let value = TimeWithTimezone {
            time,
            offset: FixedOffset::east_opt(offset_secs).unwrap(),
        };
        let physical = value.clone().into_physical();
        assert_eq!(TimeWithTimezone::from_physical(&physical), value);
    }

    #[rstest]
    #[case(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())]
    #[case(NaiveDate::from_ymd_opt(2026, 5, 6).unwrap())]
    #[case(NaiveDate::from_ymd_opt(1900, 12, 31).unwrap())]
    #[case(NaiveDate::from_ymd_opt(9999, 12, 31).unwrap())]
    fn test_naive_date_roundtrip(#[case] value: NaiveDate) {
        let physical = value.into_physical();
        assert_eq!(NaiveDate::from_physical(&physical), value);
    }

    #[rstest]
    #[case(
        NaiveDate::from_ymd_opt(2026, 5, 6).unwrap().and_hms_opt(0, 0, 0).unwrap(),
        TimeUnit::Second,
    )]
    #[case(
        NaiveDate::from_ymd_opt(2026, 5, 6).unwrap().and_hms_milli_opt(12, 34, 56, 789).unwrap(),
        TimeUnit::Millisecond,
    )]
    #[case(
        NaiveDate::from_ymd_opt(2026, 5, 6).unwrap().and_hms_micro_opt(12, 34, 56, 789_012).unwrap(),
        TimeUnit::Microsecond,
    )]
    #[case(
        NaiveDate::from_ymd_opt(2026, 5, 6).unwrap().and_hms_nano_opt(12, 34, 56, 789_012_345).unwrap(),
        TimeUnit::Nanosecond,
    )]
    #[case(
        NaiveDate::from_ymd_opt(1900, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
        TimeUnit::Microsecond,
    )]
    fn test_naive_datetime_roundtrip(#[case] value: NaiveDateTime, #[case] unit: TimeUnit) {
        let physical = value.into_physical_with_context(&unit);
        assert_eq!(
            NaiveDateTime::from_physical_with_context(&physical, &unit),
            value
        );
    }

    #[rstest]
    #[case(DateTime::<Utc>::from_timestamp_micros(0).unwrap())]
    #[case(DateTime::<Utc>::from_timestamp_micros(1_700_000_000_000_000).unwrap())]
    #[case(DateTime::<Utc>::from_timestamp_micros(-1_000_000).unwrap())]
    fn test_datetime_utc_roundtrip(#[case] value: DateTime<Utc>) {
        let physical = value.into_physical();
        assert_eq!(<DateTime<Utc>>::from_physical(&physical), value);
    }

    #[rstest]
    #[case(IntervalMonthDayNano::new(0, 0, 0))]
    #[case(IntervalMonthDayNano::new(1, 2, 3_000))]
    #[case(IntervalMonthDayNano::new(-1, -2, -3_000))]
    #[case(IntervalMonthDayNano::new(12, 30, 86_399_999_999_000))]
    fn test_interval_roundtrip_from_physical(#[case] value: IntervalMonthDayNano) {
        let logical = Interval::from_physical(&value);
        assert_eq!(logical.into_physical(), value);
    }

    #[rstest]
    #[case(Interval { months: Months::new(0), delta: TimeDelta::zero() })]
    #[case(Interval { months: Months::new(5), delta: TimeDelta::microseconds(123_456) })]
    #[case(Interval { months: Months::new(12), delta: TimeDelta::microseconds(-123_456) })]
    #[case(Interval { months: Months::new(1), delta: TimeDelta::days(7) + TimeDelta::microseconds(42) })]
    fn test_interval_roundtrip_from_logical(#[case] value: Interval) {
        let physical = value.clone().into_physical();
        assert_eq!(Interval::from_physical(&physical), value);
    }

    #[rstest]
    #[case("")]
    #[case("hello")]
    #[case("with unicode: αβγ 🦆")]
    fn test_string_roundtrip(#[case] value: &str) {
        let owned = value.to_string();
        let physical = owned.clone().into_physical();
        assert_eq!(String::from_physical(&physical), owned);
    }

    #[rstest]
    #[case(vec![])]
    #[case(vec![0u8])]
    #[case(vec![1, 2, 3, 4, 5])]
    #[case((0..=255u8).collect::<Vec<u8>>())]
    fn test_blob_roundtrip(#[case] value: Vec<u8>) {
        let physical = value.clone().into_physical();
        assert_eq!(<Vec<u8>>::from_physical(&physical), value);
    }

    #[rstest]
    #[case(Uuid::nil())]
    #[case(Uuid::max())]
    #[case(Uuid::from_u128(0x0123456789abcdef0123456789abcdef))]
    fn test_uuid_roundtrip(#[case] value: Uuid) {
        let physical = value.into_physical();
        assert_eq!(Uuid::from_physical(&physical), value);
    }
}
