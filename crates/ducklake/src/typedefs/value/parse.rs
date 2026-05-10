use chrono::{DateTime, Utc};
use indexmap::IndexMap;

use super::Value;
use crate::spec::literals;
use crate::{DucklakeError, DucklakeResult};

impl Value {
    pub(crate) fn parse(dtype: &crate::DataType, value: &str) -> DucklakeResult<Option<Self>> {
        use crate::DataType::*;
        let result = match dtype {
            Boolean => literals::parse(value)?.map(Value::Boolean),
            Int8 => literals::parse(value)?.map(Value::Int8),
            Int16 => literals::parse(value)?.map(Value::Int16),
            Int32 => literals::parse(value)?.map(Value::Int32),
            Int64 => literals::parse(value)?.map(Value::Int64),
            Int128 => literals::parse(value)?.map(Value::Int128),
            UInt8 => literals::parse(value)?.map(Value::UInt8),
            UInt16 => literals::parse(value)?.map(Value::UInt16),
            UInt32 => literals::parse(value)?.map(Value::UInt32),
            UInt64 => literals::parse(value)?.map(Value::UInt64),
            UInt128 => literals::parse(value)?.map(Value::UInt128),
            Float32 => literals::parse(value)?.map(Value::Float32),
            Float64 => literals::parse(value)?.map(Value::Float64),
            Decimal { .. } => literals::parse(value)?.map(Value::Decimal),
            Time => literals::parse(value)?.map(Value::Time),
            TimeTz => literals::parse(value)?.map(Value::TimeTz),
            Date => literals::parse(value)?.map(Value::Date),
            Timestamp { .. } => {
                literals::parse::<DateTime<Utc>>(value)?.map(|v| Value::Timestamp(v.naive_utc()))
            }
            TimestampTz => literals::parse(value)?.map(Value::TimestampTz),
            Interval => literals::parse(value)?.map(Value::Interval),
            Varchar => literals::parse(value)?.map(Value::Varchar),
            Blob => literals::parse(value)?.map(Value::Blob),
            Json => literals::parse(value)?.map(Value::Json),
            Uuid => literals::parse(value)?.map(Value::Uuid),
            List(inner) => literals::parse::<Vec<String>>(value)?
                .map(|elements| {
                    elements
                        .into_iter()
                        .map(|elem| Value::parse(&inner.dtype, &elem))
                        .collect::<DucklakeResult<_>>()
                })
                .transpose()?
                .map(Value::List),
            Struct(fields) => literals::parse::<IndexMap<String, String>>(value)?
                .map(|entries| {
                    entries
                        .into_iter()
                        .map(|(key, value_str)| {
                            let field =
                                fields.iter().find(|f| f.name == key).ok_or_else(|| {
                                    crate::DucklakeError::Parsing(format!(
                                        "unknown struct field: {key}"
                                    ))
                                })?;
                            let value = Value::parse(&field.dtype, &value_str)?;
                            Ok((key, value))
                        })
                        .collect::<DucklakeResult<_>>()
                })
                .transpose()?
                .map(Value::Struct),
            Map(key, val) => literals::parse::<Vec<(String, String)>>(value)?
                .map(|elements| {
                    elements
                        .into_iter()
                        .map(|(k, v)| {
                            let key = Value::parse(&key.dtype, &k)?.ok_or(
                                DucklakeError::Parsing("encountered NULL map key".to_string()),
                            )?;
                            let value = Value::parse(&val.dtype, &v)?;
                            Ok((key, value))
                        })
                        .collect::<DucklakeResult<_>>()
                })
                .transpose()?
                .map(Value::Map),
        };
        Ok(result)
    }
}
