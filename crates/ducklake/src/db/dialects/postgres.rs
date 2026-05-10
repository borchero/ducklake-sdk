//! The mapping from PostgreSQL types to the logical types is taken from the DuckLake docs:
//! https://ducklake.select/docs/stable/specification/data_types#postgresql
use chrono::{DateTime, FixedOffset, Months, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, Utc};
use rust_decimal::Decimal;
use sea_query::ColumnType;
use sqlx::Row;
use sqlx::postgres::types::PgTimeTz;
use sqlx::postgres::{PgArguments, PgRow};
use uuid::Uuid;

use crate::db::arrow::{Bindable, DecodableRow, EncodableArguments, TypeDecoder, TypeEncoder};
use crate::primitives::{Interval, TimeWithTimezone};
use crate::spec::literals;
use crate::{DataType, DucklakeResult};

pub struct PostgresDialect;

/* --------------------------------------------------------------------------------------------- */
/*                                          COLUMN TYPES                                         */
/* --------------------------------------------------------------------------------------------- */

pub fn column_type_for_data_type(data_type: &DataType) -> ColumnType {
    match data_type {
        DataType::Boolean => ColumnType::Boolean,
        DataType::Int8 => ColumnType::SmallInteger,
        DataType::Int16 => ColumnType::SmallInteger,
        DataType::Int32 => ColumnType::Integer,
        DataType::Int64 => ColumnType::BigInteger,
        DataType::Int128 => ColumnType::String(Default::default()),
        DataType::UInt8 => ColumnType::Integer,
        DataType::UInt16 => ColumnType::Integer,
        DataType::UInt32 => ColumnType::BigInteger,
        DataType::UInt64 => ColumnType::String(Default::default()),
        DataType::UInt128 => ColumnType::String(Default::default()),
        DataType::Float32 => ColumnType::Float,
        DataType::Float64 => ColumnType::Double,
        DataType::Decimal { precision, scale } => {
            ColumnType::Decimal(Some((*precision as u32, *scale as u32)))
        }
        DataType::Time => ColumnType::Time,
        DataType::TimeTz => ColumnType::custom("TIMETZ"),
        DataType::Date | DataType::Timestamp { .. } | DataType::TimestampTz => {
            ColumnType::String(Default::default())
        }
        DataType::Interval => ColumnType::Interval(None, None),
        DataType::Varchar | DataType::Json | DataType::Blob => ColumnType::Blob,
        DataType::Uuid => ColumnType::Uuid,
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _) => {
            ColumnType::String(Default::default())
        }
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                            DECODING                                           */
/* --------------------------------------------------------------------------------------------- */

impl DecodableRow for PgRow {
    type Decoder = PostgresDialect;
}

impl TypeDecoder for PostgresDialect {
    type Row = PgRow;

    fn decode_bool(row: &PgRow, name: &str) -> DucklakeResult<Option<bool>> {
        Ok(row.try_get(name)?)
    }

    fn decode_i8(row: &PgRow, name: &str) -> DucklakeResult<Option<i8>> {
        let value: Option<i16> = row.try_get(name)?;
        Ok(value.map(|v| v as i8))
    }

    fn decode_i16(row: &PgRow, name: &str) -> DucklakeResult<Option<i16>> {
        Ok(row.try_get(name)?)
    }

    fn decode_i32(row: &PgRow, name: &str) -> DucklakeResult<Option<i32>> {
        Ok(row.try_get(name)?)
    }

    fn decode_i64(row: &PgRow, name: &str) -> DucklakeResult<Option<i64>> {
        Ok(row.try_get(name)?)
    }

    fn decode_i128(row: &Self::Row, name: &str) -> DucklakeResult<Option<i128>> {
        let value: Option<&str> = row.try_get(name)?;
        Ok(value.map(literals::parse).transpose()?.flatten())
    }

    fn decode_u8(row: &PgRow, name: &str) -> DucklakeResult<Option<u8>> {
        let value: Option<i32> = row.try_get(name)?;
        Ok(value.map(|v| v as u8))
    }

    fn decode_u16(row: &PgRow, name: &str) -> DucklakeResult<Option<u16>> {
        let value: Option<i32> = row.try_get(name)?;
        Ok(value.map(|v| v as u16))
    }

    fn decode_u32(row: &PgRow, name: &str) -> DucklakeResult<Option<u32>> {
        let value: Option<i64> = row.try_get(name)?;
        Ok(value.map(|v| v as u32))
    }

    fn decode_u64(row: &PgRow, name: &str) -> DucklakeResult<Option<u64>> {
        let value: Option<&str> = row.try_get(name)?;
        Ok(value.map(literals::parse).transpose()?.flatten())
    }

    fn decode_u128(row: &Self::Row, name: &str) -> DucklakeResult<Option<u128>> {
        let value: Option<&str> = row.try_get(name)?;
        Ok(value.map(literals::parse).transpose()?.flatten())
    }

    fn decode_f32(row: &PgRow, name: &str) -> DucklakeResult<Option<f32>> {
        Ok(row.try_get(name)?)
    }

    fn decode_f64(row: &PgRow, name: &str) -> DucklakeResult<Option<f64>> {
        Ok(row.try_get(name)?)
    }

    fn decode_decimal(row: &PgRow, name: &str) -> DucklakeResult<Option<Decimal>> {
        Ok(row.try_get(name)?)
    }

    fn decode_time(row: &Self::Row, name: &str) -> DucklakeResult<Option<chrono::NaiveTime>> {
        Ok(row.try_get(name)?)
    }

    fn decode_time_tz(row: &PgRow, name: &str) -> DucklakeResult<Option<TimeWithTimezone>> {
        let value: Option<PgTimeTz<NaiveTime, FixedOffset>> = row.try_get(name)?;
        Ok(value.map(|v| TimeWithTimezone {
            time: v.time,
            offset: v.offset,
        }))
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
        let value: Option<sqlx::postgres::types::PgInterval> = row.try_get(name)?;
        Ok(value.map(|v| Interval {
            months: Months::new(v.months as u32),
            delta: TimeDelta::microseconds(v.microseconds),
        }))
    }

    fn decode_string(row: &PgRow, name: &str) -> DucklakeResult<Option<String>> {
        let value: Option<Vec<u8>> = row.try_get(name)?;
        Ok(value.map(String::from_utf8).transpose()?)
    }

    fn decode_binary(row: &Self::Row, name: &str) -> DucklakeResult<Option<Vec<u8>>> {
        Ok(row.try_get(name)?)
    }

    fn decode_uuid(row: &Self::Row, name: &str) -> DucklakeResult<Option<Uuid>> {
        Ok(row.try_get(name)?)
    }

    fn decode_text(row: &Self::Row, name: &str) -> DucklakeResult<Option<String>> {
        Ok(row.try_get(name)?)
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                            ENCODING                                           */
/* --------------------------------------------------------------------------------------------- */

impl EncodableArguments for PgArguments {
    type Encoder = PostgresDialect;
}

impl TypeEncoder for PostgresDialect {
    type Arguments = PgArguments;
    type Database = sqlx::Postgres;

    fn encode_bool(value: Option<bool>) -> impl Bindable<Self::Database> {
        value
    }

    fn encode_i8(value: Option<i8>) -> impl Bindable<Self::Database> {
        value.map(|v| v as i16)
    }

    fn encode_i16(value: Option<i16>) -> impl Bindable<Self::Database> {
        value
    }

    fn encode_i32(value: Option<i32>) -> impl Bindable<Self::Database> {
        value
    }

    fn encode_i64(value: Option<i64>) -> impl Bindable<Self::Database> {
        value
    }

    fn encode_i128(value: Option<i128>) -> impl Bindable<Self::Database> {
        literals::format(value.as_ref())
    }

    fn encode_u8(value: Option<u8>) -> impl Bindable<Self::Database> {
        value.map(|v| v as i16)
    }

    fn encode_u16(value: Option<u16>) -> impl Bindable<Self::Database> {
        value.map(|v| v as i32)
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
        value
    }

    fn encode_f64(value: Option<f64>) -> impl Bindable<Self::Database> {
        value
    }

    fn encode_decimal(value: Option<Decimal>) -> impl Bindable<Self::Database> {
        value
    }

    fn encode_time(value: Option<NaiveTime>) -> impl Bindable<Self::Database> {
        value
    }

    fn encode_time_tz(value: Option<TimeWithTimezone>) -> impl Bindable<Self::Database> {
        value.map(|v| PgTimeTz {
            time: v.time,
            offset: v.offset,
        })
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
        value.map(|v| sqlx::postgres::types::PgInterval {
            months: v.months.as_u32() as i32,
            days: 0,
            microseconds: v.delta.num_microseconds().unwrap_or(0),
        })
    }

    fn encode_string(value: Option<&str>) -> impl Bindable<Self::Database> {
        value.map(|v| v.as_bytes().to_vec())
    }

    fn encode_binary(value: Option<&[u8]>) -> impl Bindable<Self::Database> {
        value.map(<[u8]>::to_vec)
    }

    fn encode_uuid(value: Option<Uuid>) -> impl Bindable<Self::Database> {
        value
    }

    fn encode_text(value: Option<&str>) -> impl Bindable<Self::Database> {
        value.map(str::to_string)
    }
}
