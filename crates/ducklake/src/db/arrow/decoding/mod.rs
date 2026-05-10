mod factory;
mod nested;
mod primitive;

use arrow_array::ArrayRef;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
pub use factory::make_array_appender;
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::DucklakeResult;
use crate::primitives::{Interval, TimeWithTimezone};

/* ---------------------------------- STEP 1: DATABASE -> RUST --------------------------------- */

pub trait DecodableRow: sqlx::Row {
    type Decoder: TypeDecoder<Row = Self>;
}

pub trait TypeDecoder: Send + Sync + 'static {
    type Row: sqlx::Row;

    fn decode_bool(row: &Self::Row, name: &str) -> DucklakeResult<Option<bool>>;
    fn decode_i8(row: &Self::Row, name: &str) -> DucklakeResult<Option<i8>>;
    fn decode_i16(row: &Self::Row, name: &str) -> DucklakeResult<Option<i16>>;
    fn decode_i32(row: &Self::Row, name: &str) -> DucklakeResult<Option<i32>>;
    fn decode_i64(row: &Self::Row, name: &str) -> DucklakeResult<Option<i64>>;
    fn decode_i128(row: &Self::Row, name: &str) -> DucklakeResult<Option<i128>>;
    fn decode_u8(row: &Self::Row, name: &str) -> DucklakeResult<Option<u8>>;
    fn decode_u16(row: &Self::Row, name: &str) -> DucklakeResult<Option<u16>>;
    fn decode_u32(row: &Self::Row, name: &str) -> DucklakeResult<Option<u32>>;
    fn decode_u64(row: &Self::Row, name: &str) -> DucklakeResult<Option<u64>>;
    fn decode_u128(row: &Self::Row, name: &str) -> DucklakeResult<Option<u128>>;
    fn decode_f32(row: &Self::Row, name: &str) -> DucklakeResult<Option<f32>>;
    fn decode_f64(row: &Self::Row, name: &str) -> DucklakeResult<Option<f64>>;
    fn decode_decimal(row: &Self::Row, name: &str) -> DucklakeResult<Option<Decimal>>;
    fn decode_time(row: &Self::Row, name: &str) -> DucklakeResult<Option<NaiveTime>>;
    fn decode_time_tz(row: &Self::Row, name: &str) -> DucklakeResult<Option<TimeWithTimezone>>;
    fn decode_date(row: &Self::Row, name: &str) -> DucklakeResult<Option<NaiveDate>>;
    fn decode_timestamp(row: &Self::Row, name: &str) -> DucklakeResult<Option<NaiveDateTime>>;
    fn decode_timestamp_tz(row: &Self::Row, name: &str) -> DucklakeResult<Option<DateTime<Utc>>>;
    fn decode_interval(row: &Self::Row, name: &str) -> DucklakeResult<Option<Interval>>;
    fn decode_string(row: &Self::Row, name: &str) -> DucklakeResult<Option<String>>;
    fn decode_binary(row: &Self::Row, name: &str) -> DucklakeResult<Option<Vec<u8>>>;
    fn decode_uuid(row: &Self::Row, name: &str) -> DucklakeResult<Option<Uuid>>;

    /// Decodes a column whose value is always serialized as text by the underlying
    /// database (used to decode nested types — lists, structs, maps). This might be
    /// different to strings which might be stored as native text of binary.
    fn decode_text(row: &Self::Row, name: &str) -> DucklakeResult<Option<String>>;
}

/* ----------------------------------- STEP 2: RUST -> ARROW ----------------------------------- */

pub trait ArrayAppender<D: TypeDecoder>: Send + Sync {
    fn append(&mut self, row: &D::Row, name: &str) -> DucklakeResult<()>;
    fn append_text(&mut self, text: &str) -> DucklakeResult<()>;
    fn append_null(&mut self);
    fn finish(&mut self) -> ArrayRef;
}
