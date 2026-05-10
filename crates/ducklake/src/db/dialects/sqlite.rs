//! The mapping from SQLite types to the logical types is taken from:
//! https://ducklake.select/docs/stable/specification/data_types#sqlite.
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rust_decimal::Decimal;
use sea_query::ColumnType;
use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use uuid::Uuid;

use crate::db::arrow::{Bindable, DecodableRow, EncodableArguments, TypeDecoder, TypeEncoder};
use crate::primitives::{Interval, TimeWithTimezone};
use crate::spec::literals;
use crate::{DataType, DucklakeResult};

pub struct SqliteDialect;

/* --------------------------------------------------------------------------------------------- */
/*                                          COLUMN TYPES                                         */
/* --------------------------------------------------------------------------------------------- */

pub fn column_type_for_data_type(data_type: &DataType) -> ColumnType {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => ColumnType::custom("bigint"),
        DataType::Int128
        | DataType::UInt64
        | DataType::UInt128
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal { .. }
        | DataType::Time
        | DataType::TimeTz
        | DataType::Date
        | DataType::Timestamp { .. }
        | DataType::TimestampTz
        | DataType::Interval
        | DataType::Varchar
        | DataType::Json
        | DataType::Uuid
        | DataType::List(_)
        | DataType::Struct(_)
        | DataType::Map(_, _) => ColumnType::string(None),
        DataType::Blob => ColumnType::Blob,
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                            DECODING                                           */
/* --------------------------------------------------------------------------------------------- */

impl DecodableRow for SqliteRow {
    type Decoder = SqliteDialect;
}

impl TypeDecoder for SqliteDialect {
    type Row = SqliteRow;

    fn decode_bool(row: &SqliteRow, name: &str) -> DucklakeResult<Option<bool>> {
        Ok(row.try_get(name)?)
    }

    fn decode_i8(row: &SqliteRow, name: &str) -> DucklakeResult<Option<i8>> {
        Ok(row.try_get(name)?)
    }

    fn decode_i16(row: &SqliteRow, name: &str) -> DucklakeResult<Option<i16>> {
        Ok(row.try_get(name)?)
    }

    fn decode_i32(row: &SqliteRow, name: &str) -> DucklakeResult<Option<i32>> {
        Ok(row.try_get(name)?)
    }

    fn decode_i64(row: &SqliteRow, name: &str) -> DucklakeResult<Option<i64>> {
        Ok(row.try_get(name)?)
    }

    fn decode_i128(row: &Self::Row, name: &str) -> DucklakeResult<Option<i128>> {
        let value: Option<&str> = row.try_get(name)?;
        Ok(value.map(literals::parse).transpose()?.flatten())
    }

    fn decode_u8(row: &SqliteRow, name: &str) -> DucklakeResult<Option<u8>> {
        Ok(row.try_get(name)?)
    }

    fn decode_u16(row: &SqliteRow, name: &str) -> DucklakeResult<Option<u16>> {
        Ok(row.try_get(name)?)
    }

    fn decode_u32(row: &SqliteRow, name: &str) -> DucklakeResult<Option<u32>> {
        Ok(row.try_get(name)?)
    }

    fn decode_u64(row: &SqliteRow, name: &str) -> DucklakeResult<Option<u64>> {
        let value: Option<&str> = row.try_get(name)?;
        Ok(value.map(literals::parse).transpose()?.flatten())
    }

    fn decode_u128(row: &Self::Row, name: &str) -> DucklakeResult<Option<u128>> {
        let value: Option<&str> = row.try_get(name)?;
        Ok(value.map(literals::parse).transpose()?.flatten())
    }

    fn decode_f32(row: &SqliteRow, name: &str) -> DucklakeResult<Option<f32>> {
        let value: Option<&str> = row.try_get(name)?;
        Ok(value.map(literals::parse).transpose()?.flatten())
    }

    fn decode_f64(row: &SqliteRow, name: &str) -> DucklakeResult<Option<f64>> {
        let value: Option<&str> = row.try_get(name)?;
        Ok(value.map(literals::parse).transpose()?.flatten())
    }

    fn decode_decimal(row: &SqliteRow, name: &str) -> DucklakeResult<Option<Decimal>> {
        let value: Option<&str> = row.try_get(name)?;
        Ok(value.map(literals::parse).transpose()?.flatten())
    }

    fn decode_time(row: &Self::Row, name: &str) -> DucklakeResult<Option<chrono::NaiveTime>> {
        let value: Option<&str> = row.try_get(name)?;
        Ok(value.map(literals::parse).transpose()?.flatten())
    }

    fn decode_time_tz(row: &SqliteRow, name: &str) -> DucklakeResult<Option<TimeWithTimezone>> {
        let value: Option<&str> = row.try_get(name)?;
        Ok(value.map(literals::parse).transpose()?.flatten())
    }

    fn decode_date(row: &Self::Row, name: &str) -> DucklakeResult<Option<NaiveDate>> {
        let value: Option<&str> = row.try_get(name)?;
        Ok(value.map(literals::parse).transpose()?.flatten())
    }

    fn decode_timestamp(row: &Self::Row, name: &str) -> DucklakeResult<Option<NaiveDateTime>> {
        let value: Option<&str> = row.try_get(name)?;
        Ok(value.map(literals::parse).transpose()?.flatten())
    }

    fn decode_timestamp_tz(row: &Self::Row, name: &str) -> DucklakeResult<Option<DateTime<Utc>>> {
        let value: Option<&str> = row.try_get(name)?;
        Ok(value.map(literals::parse).transpose()?.flatten())
    }

    fn decode_interval(row: &Self::Row, name: &str) -> DucklakeResult<Option<Interval>> {
        let value: Option<&str> = row.try_get(name)?;
        Ok(value.map(literals::parse).transpose()?.flatten())
    }

    fn decode_string(row: &SqliteRow, name: &str) -> DucklakeResult<Option<String>> {
        let value: Option<&str> = row.try_get(name)?;
        Ok(value.map(literals::parse).transpose()?.flatten())
    }

    fn decode_binary(row: &Self::Row, name: &str) -> DucklakeResult<Option<Vec<u8>>> {
        Ok(row.try_get(name)?)
    }

    fn decode_uuid(row: &Self::Row, name: &str) -> DucklakeResult<Option<Uuid>> {
        let value: Option<&str> = row.try_get(name)?;
        Ok(value.map(literals::parse).transpose()?.flatten())
    }

    fn decode_text(row: &Self::Row, name: &str) -> DucklakeResult<Option<String>> {
        Ok(row.try_get(name)?)
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                            ENCODING                                           */
/* --------------------------------------------------------------------------------------------- */

impl EncodableArguments for sqlx::sqlite::SqliteArguments<'static> {
    type Encoder = SqliteDialect;
}

impl TypeEncoder for SqliteDialect {
    type Arguments = sqlx::sqlite::SqliteArguments<'static>;
    type Database = sqlx::Sqlite;

    fn encode_bool(value: Option<bool>) -> impl Bindable<Self::Database> {
        value
    }

    fn encode_i8(value: Option<i8>) -> impl Bindable<Self::Database> {
        value.map(|v| v as i64)
    }

    fn encode_i16(value: Option<i16>) -> impl Bindable<Self::Database> {
        value.map(|v| v as i64)
    }

    fn encode_i32(value: Option<i32>) -> impl Bindable<Self::Database> {
        value.map(|v| v as i64)
    }

    fn encode_i64(value: Option<i64>) -> impl Bindable<Self::Database> {
        value
    }

    fn encode_i128(value: Option<i128>) -> impl Bindable<Self::Database> {
        literals::format(value.as_ref())
    }

    fn encode_u8(value: Option<u8>) -> impl Bindable<Self::Database> {
        value.map(|v| v as i64)
    }

    fn encode_u16(value: Option<u16>) -> impl Bindable<Self::Database> {
        value.map(|v| v as i64)
    }

    fn encode_u32(value: Option<u32>) -> impl Bindable<Self::Database> {
        value.map(|v| v as i64)
    }

    fn encode_u64(value: Option<u64>) -> impl Bindable<Self::Database> {
        literals::format(value.as_ref())
    }

    fn encode_u128(value: Option<u128>) -> impl Bindable<Self::Database> {
        literals::format(value.as_ref())
    }

    fn encode_f32(value: Option<f32>) -> impl Bindable<Self::Database> {
        literals::format(value.as_ref())
    }

    fn encode_f64(value: Option<f64>) -> impl Bindable<Self::Database> {
        literals::format(value.as_ref())
    }

    fn encode_decimal(value: Option<Decimal>) -> impl Bindable<Self::Database> {
        literals::format(value.as_ref())
    }

    fn encode_time(value: Option<NaiveTime>) -> impl Bindable<Self::Database> {
        literals::format(value.as_ref())
    }

    fn encode_time_tz(value: Option<TimeWithTimezone>) -> impl Bindable<Self::Database> {
        literals::format(value.as_ref())
    }

    fn encode_date(value: Option<NaiveDate>) -> impl Bindable<Self::Database> {
        literals::format(value.as_ref())
    }

    fn encode_timestamp(value: Option<NaiveDateTime>) -> impl Bindable<Self::Database> {
        literals::format(value.as_ref())
    }

    fn encode_timestamp_tz(value: Option<DateTime<Utc>>) -> impl Bindable<Self::Database> {
        literals::format(value.as_ref())
    }

    fn encode_interval(value: Option<Interval>) -> impl Bindable<Self::Database> {
        literals::format(value.as_ref())
    }

    fn encode_string(value: Option<&str>) -> impl Bindable<Self::Database> {
        literals::format(value.map(|s| s.to_string()).as_ref())
    }

    fn encode_binary(value: Option<&[u8]>) -> impl Bindable<Self::Database> {
        value.map(<[u8]>::to_vec)
    }

    fn encode_uuid(value: Option<Uuid>) -> impl Bindable<Self::Database> {
        literals::format(value.as_ref())
    }

    fn encode_text(value: Option<&str>) -> impl Bindable<Self::Database> {
        value.map(str::to_string)
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                          QUERY VALUES                                         */
/* --------------------------------------------------------------------------------------------- */

pub fn adapt_values(values: sea_query_sqlx::SqlxValues) -> sea_query_sqlx::SqlxValues {
    use sea_query::Value;
    let values = values
        .0
        .into_iter()
        .map(|v| match v {
            Value::Uuid(Some(uuid)) => Value::String(Some(uuid.to_string())),
            Value::Uuid(None) => Value::String(None),
            other => other,
        })
        .collect();
    sea_query_sqlx::SqlxValues(sea_query::Values(values))
}
