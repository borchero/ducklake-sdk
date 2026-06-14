use std::sync::Arc;

use arrow_schema::{
    DataType as ArrowDataType,
    Field as ArrowField,
    Schema as ArrowSchema,
    TimeUnit as ArrowTimeUnit,
    extension,
};
use itertools::Itertools;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use super::{Column, ColumnDefault, DataType, Schema, TimestampPrecision};
use crate::DucklakeError;

/* ------------------------------------------- SCHEMA ------------------------------------------ */

impl Schema {
    pub(crate) fn to_arrow(&self) -> ArrowSchema {
        let fields = self
            .columns
            .values()
            .map(|col| col.to_arrow_field())
            .collect_vec();
        ArrowSchema::new(fields)
    }
}

/* -------------------------------------- COLUMN -> ARROW -------------------------------------- */

impl Column {
    pub fn to_arrow_field(&self) -> ArrowField {
        const PARQUET_FIELD_ID_KEY: &str = "PARQUET:field_id";

        let data_type = match &self.dtype {
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::Int8 => ArrowDataType::Int8,
            DataType::Int16 => ArrowDataType::Int16,
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::Int128 => ArrowDataType::FixedSizeBinary(16),
            DataType::UInt8 => ArrowDataType::UInt8,
            DataType::UInt16 => ArrowDataType::UInt16,
            DataType::UInt32 => ArrowDataType::UInt32,
            DataType::UInt64 => ArrowDataType::UInt64,
            DataType::UInt128 => ArrowDataType::FixedSizeBinary(16),
            DataType::Float32 => ArrowDataType::Float32,
            DataType::Float64 => ArrowDataType::Float64,
            DataType::Decimal { precision, scale } => {
                ArrowDataType::Decimal128(*precision, *scale as i8)
            }
            DataType::Time => ArrowDataType::Time64(ArrowTimeUnit::Microsecond),
            DataType::TimeTz => ArrowDataType::FixedSizeBinary(8),
            DataType::Date => ArrowDataType::Date32,
            DataType::Timestamp { precision } => {
                ArrowDataType::Timestamp((*precision).into(), None)
            }
            DataType::TimestampTz => {
                ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, Some("UTC".into()))
            }
            DataType::Interval => {
                ArrowDataType::Interval(arrow_schema::IntervalUnit::MonthDayNano)
            }
            DataType::Varchar => ArrowDataType::Utf8View,
            DataType::Blob => ArrowDataType::LargeBinary,
            DataType::Json => ArrowDataType::Utf8View,
            DataType::Uuid => ArrowDataType::FixedSizeBinary(16),
            DataType::List(inner) => ArrowDataType::LargeList(Arc::new(inner.to_arrow_field())),
            DataType::Struct(fields) => {
                let fields = fields.iter().map(|f| f.to_arrow_field()).collect();
                ArrowDataType::Struct(fields)
            }
            DataType::Map(key, value) => {
                let entries = ArrowField::new_struct(
                    "entries",
                    vec![key.to_arrow_field(), value.to_arrow_field()],
                    false,
                );
                ArrowDataType::Map(Arc::new(entries), false)
            }
        };

        let field = {
            let field = ArrowField::new(&self.name, data_type, self.nullable);
            match &self.dtype {
                DataType::Int128 => {
                    field.with_extension_type(extension::Opaque::new("hugeint", "DuckLake"))
                }
                DataType::UInt128 => {
                    field.with_extension_type(extension::Opaque::new("uhugeint", "DuckLake"))
                }
                DataType::TimeTz => {
                    field.with_extension_type(extension::Opaque::new("time_tz", "DuckLake"))
                }
                DataType::Json => field.with_extension_type(extension::Json::default()),
                DataType::Uuid => field.with_extension_type(extension::Uuid),
                _ => field,
            }
        };
        if let Some(field_id) = self.field_id {
            field.with_metadata([(PARQUET_FIELD_ID_KEY.to_string(), field_id.to_string())].into())
        } else {
            field
        }
    }
}

impl From<TimestampPrecision> for ArrowTimeUnit {
    fn from(precision: TimestampPrecision) -> Self {
        match precision {
            TimestampPrecision::Seconds => ArrowTimeUnit::Second,
            TimestampPrecision::Milliseconds => ArrowTimeUnit::Millisecond,
            TimestampPrecision::Microseconds => ArrowTimeUnit::Microsecond,
            TimestampPrecision::Nanoseconds => ArrowTimeUnit::Nanosecond,
        }
    }
}

/* -------------------------------------- ARROW -> COLUMN -------------------------------------- */

impl TryFrom<&ArrowField> for Column {
    type Error = DucklakeError;

    fn try_from(field: &ArrowField) -> Result<Self, Self::Error> {
        let dtype = match field.data_type() {
            ArrowDataType::Boolean => Ok(DataType::boolean()),
            ArrowDataType::Int8 => Ok(DataType::int8()),
            ArrowDataType::Int16 => Ok(DataType::int16()),
            ArrowDataType::Int32 => Ok(DataType::int32()),
            ArrowDataType::Int64 => Ok(DataType::int64()),
            ArrowDataType::UInt8 => Ok(DataType::uint8()),
            ArrowDataType::UInt16 => Ok(DataType::uint16()),
            ArrowDataType::UInt32 => Ok(DataType::uint32()),
            ArrowDataType::UInt64 => Ok(DataType::uint64()),
            ArrowDataType::Float32 => Ok(DataType::float32()),
            ArrowDataType::Float64 => Ok(DataType::float64()),
            ArrowDataType::Decimal128(precision, scale) => {
                Ok(DataType::decimal(*precision, *scale as u8))
            }
            ArrowDataType::Date32 => Ok(DataType::date()),
            ArrowDataType::Time64(ArrowTimeUnit::Microsecond | ArrowTimeUnit::Nanosecond) => {
                Ok(DataType::time())
            }
            ArrowDataType::FixedSizeBinary(8)
                if field.try_extension_type::<extension::Opaque>().is_ok() =>
            {
                Ok(DataType::time_tz())
            }
            ArrowDataType::Timestamp(time_unit, None) => {
                let precision = match time_unit {
                    ArrowTimeUnit::Second => TimestampPrecision::Seconds,
                    ArrowTimeUnit::Millisecond => TimestampPrecision::Milliseconds,
                    ArrowTimeUnit::Microsecond => TimestampPrecision::Microseconds,
                    ArrowTimeUnit::Nanosecond => TimestampPrecision::Nanoseconds,
                };
                Ok(DataType::timestamp(precision))
            }
            ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, Some(_)) => {
                Ok(DataType::timestamp_tz())
            }
            ArrowDataType::Interval(_) => Ok(DataType::interval()),
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
                if field.try_extension_type::<extension::Json>().is_ok() {
                    Ok(DataType::json())
                } else {
                    Ok(DataType::varchar())
                }
            }
            ArrowDataType::Binary | ArrowDataType::LargeBinary | ArrowDataType::BinaryView => {
                Ok(DataType::blob())
            }
            ArrowDataType::FixedSizeBinary(16)
                if field.try_extension_type::<extension::Uuid>().is_ok() =>
            {
                Ok(DataType::uuid())
            }
            ArrowDataType::List(inner) | ArrowDataType::LargeList(inner) => {
                let inner_type = Column::try_from(&**inner)?;
                Ok(DataType::List(Box::new(inner_type)))
            }
            ArrowDataType::Struct(fields) => {
                let mut columns = Vec::with_capacity(fields.len());
                for field in fields {
                    let column = Column::try_from(&**field)?;
                    columns.push(column);
                }
                Ok(DataType::struct_(
                    fields
                        .iter()
                        .map(|f| Column::try_from(&**f))
                        .try_collect()?,
                ))
            }
            ArrowDataType::Map(entries, _) => {
                let ArrowDataType::Struct(fields) = entries.data_type() else {
                    panic!("map entries field must have a struct data type")
                };
                let key_type = Column::try_from(&*fields[0])?;
                let value_type = Column::try_from(&*fields[1])?;
                Ok(DataType::Map(Box::new(key_type), Box::new(value_type)))
            }
            other => Err(DucklakeError::UnsupportedArrowDataType(format!(
                "{other:?}"
            ))),
        }?;
        Ok(Column {
            name: field.name().clone(),
            dtype,
            nullable: field.is_nullable(),
            tags: Vec::new(),
            initial_default: None,
            default_value: ColumnDefault::Literal(None),
            field_id: field
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .and_then(|id_str| id_str.parse().ok()),
        })
    }
}
