use std::fmt::Display;

use indexmap::IndexMap;

use super::Value;
use crate::spec::literals;

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Value::*;
        match self {
            Boolean(v) => literals::format(Some(v)).fmt(f),
            Int8(v) => literals::format(Some(v)).fmt(f),
            Int16(v) => literals::format(Some(v)).fmt(f),
            Int32(v) => literals::format(Some(v)).fmt(f),
            Int64(v) => literals::format(Some(v)).fmt(f),
            Int128(v) => literals::format(Some(v)).fmt(f),
            UInt8(v) => literals::format(Some(v)).fmt(f),
            UInt16(v) => literals::format(Some(v)).fmt(f),
            UInt32(v) => literals::format(Some(v)).fmt(f),
            UInt64(v) => literals::format(Some(v)).fmt(f),
            UInt128(v) => literals::format(Some(v)).fmt(f),
            Float32(v) => literals::format(Some(v)).fmt(f),
            Float64(v) => literals::format(Some(v)).fmt(f),
            Decimal(v) => literals::format(Some(v)).fmt(f),
            Time(v) => literals::format(Some(v)).fmt(f),
            Date(v) => literals::format(Some(v)).fmt(f),
            Timestamp(v) => literals::format(Some(&v.and_utc())).fmt(f),
            TimestampTz(v) => literals::format(Some(v)).fmt(f),
            TimeTz(v) => literals::format(Some(v)).fmt(f),
            Interval(v) => literals::format(Some(v)).fmt(f),
            Varchar(v) => literals::format(Some(v)).fmt(f),
            Blob(v) => literals::format(Some(v)).fmt(f),
            Json(v) => literals::format(Some(v)).fmt(f),
            Uuid(v) => literals::format(Some(v)).fmt(f),
            List(values) => {
                let inner = values
                    .iter()
                    .map(|v| Value::to_string_opt(v.as_ref()))
                    .collect::<Vec<_>>();
                literals::format(Some(&inner)).fmt(f)
            }
            Struct(values) => {
                let inner = values
                    .iter()
                    .map(|(k, v)| (k.to_string(), Value::to_string_opt(v.as_ref())))
                    .collect::<IndexMap<_, _>>();
                literals::format(Some(&inner)).fmt(f)
            }
            Map(entries) => {
                let inner = entries
                    .iter()
                    .map(|(k, v)| (k.to_string(), Value::to_string_opt(v.as_ref())))
                    .collect::<Vec<_>>();
                literals::format(Some(&inner)).fmt(f)
            }
        }
    }
}

impl Value {
    pub(crate) fn to_string_opt(val: Option<&Value>) -> String {
        val.as_ref()
            .map(|v| v.to_string())
            .unwrap_or_else(|| literals::NULL_STRING.to_string())
    }
}
