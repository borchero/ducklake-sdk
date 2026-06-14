mod factory;
mod nested;
mod primitive;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
pub(in crate::db) use factory::make_column_encoder;
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::DucklakeResult;
use crate::primitives::{Interval, TimeWithTimezone};

/* ----------------------------------- STEP 1: ARRAY -> RUST ----------------------------------- */

pub(in crate::db) trait ArrayExtractor<E: TypeEncoder>: Send + Sync {
    fn extract(&self, args: &mut E::Arguments, row_idx: usize) -> DucklakeResult<()>;
    fn extract_text(&self, row_idx: usize) -> String;
}

/* ---------------------------------- STEP 2: RUST -> DATABASE --------------------------------- */

pub(in crate::db) trait EncodableArguments: Default {
    type Encoder: TypeEncoder<Arguments = Self> + 'static;
}

pub(in crate::db) trait Bindable<DB: sqlx::Database> =
    sqlx::Encode<'static, DB> + sqlx::Type<DB> + Send + 'static;

pub(in crate::db) trait TypeEncoder: Send + Sync + 'static {
    type Arguments: sqlx::Arguments<Database = Self::Database> + Default;
    type Database: sqlx::Database;

    fn encode_bool(value: Option<bool>) -> impl Bindable<Self::Database>;
    fn encode_i8(value: Option<i8>) -> impl Bindable<Self::Database>;
    fn encode_i16(value: Option<i16>) -> impl Bindable<Self::Database>;
    fn encode_i32(value: Option<i32>) -> impl Bindable<Self::Database>;
    fn encode_i64(value: Option<i64>) -> impl Bindable<Self::Database>;
    fn encode_i128(value: Option<i128>) -> impl Bindable<Self::Database>;
    fn encode_u8(value: Option<u8>) -> impl Bindable<Self::Database>;
    fn encode_u16(value: Option<u16>) -> impl Bindable<Self::Database>;
    fn encode_u32(value: Option<u32>) -> impl Bindable<Self::Database>;
    fn encode_u64(value: Option<u64>) -> impl Bindable<Self::Database>;
    fn encode_u128(value: Option<u128>) -> impl Bindable<Self::Database>;
    fn encode_f32(value: Option<f32>) -> impl Bindable<Self::Database>;
    fn encode_f64(value: Option<f64>) -> impl Bindable<Self::Database>;
    fn encode_decimal(value: Option<Decimal>) -> impl Bindable<Self::Database>;
    fn encode_time(value: Option<NaiveTime>) -> impl Bindable<Self::Database>;
    fn encode_time_tz(value: Option<TimeWithTimezone>) -> impl Bindable<Self::Database>;
    fn encode_date(value: Option<NaiveDate>) -> impl Bindable<Self::Database>;
    fn encode_timestamp(value: Option<NaiveDateTime>) -> impl Bindable<Self::Database>;
    fn encode_timestamp_tz(value: Option<DateTime<Utc>>) -> impl Bindable<Self::Database>;
    fn encode_interval(value: Option<Interval>) -> impl Bindable<Self::Database>;
    fn encode_string(value: Option<&str>) -> impl Bindable<Self::Database>;
    fn encode_binary(value: Option<&[u8]>) -> impl Bindable<Self::Database>;
    fn encode_uuid(value: Option<Uuid>) -> impl Bindable<Self::Database>;

    /// Encode a value whose representation in the database is always text
    /// (used for nested types — lists, structs, maps). Mirrors
    /// `RowDecoder::decode_text` on the read side.
    fn encode_text(value: Option<&str>) -> impl Bindable<Self::Database>;
}
