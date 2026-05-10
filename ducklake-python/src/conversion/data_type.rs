use ducklake::{Column, DataType, TimestampPrecision};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;

use super::Wrap;
use super::py_modules::*;

impl FromPyObject<'_, '_> for Wrap<DataType> {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let type_name = ob.get_type().qualname()?.to_string();

        let dtype = match type_name.as_str() {
            "Boolean" => DataType::Boolean,
            "Int8" => DataType::Int8,
            "Int16" => DataType::Int16,
            "Int32" => DataType::Int32,
            "Int64" => DataType::Int64,
            "UInt8" => DataType::UInt8,
            "UInt16" => DataType::UInt16,
            "UInt32" => DataType::UInt32,
            "UInt64" => DataType::UInt64,
            "Float32" => DataType::Float32,
            "Float64" => DataType::Float64,
            "Decimal" => {
                let precision: u8 = ob.getattr("precision")?.extract()?;
                let scale: u8 = ob.getattr("scale")?.extract()?;
                DataType::Decimal { precision, scale }
            }
            "Time" => DataType::Time,
            "TimeTz" => DataType::TimeTz,
            "Date" => DataType::Date,
            "Timestamp" => {
                let precision_str: PyBackedStr = ob.getattr("precision")?.extract()?;
                let precision = match &*precision_str {
                    "seconds" => TimestampPrecision::Seconds,
                    "milliseconds" => TimestampPrecision::Milliseconds,
                    "microseconds" => TimestampPrecision::Microseconds,
                    "nanoseconds" => TimestampPrecision::Nanoseconds,
                    _ => {
                        return Err(PyTypeError::new_err(format!(
                            "Invalid timestamp precision: {}",
                            precision_str
                        )));
                    }
                };
                DataType::Timestamp { precision }
            }
            "TimestampTz" => DataType::TimestampTz,
            "Interval" => DataType::Interval,
            "Varchar" => DataType::Varchar,
            "Blob" => DataType::Blob,
            "Json" => DataType::Json,
            "Uuid" => DataType::Uuid,
            "List" => {
                let inner = ob.getattr("inner")?.extract::<Wrap<Column>>()?;
                DataType::List(Box::new(inner.0))
            }
            "Struct" => {
                let fields: Vec<Wrap<Column>> = ob.getattr("fields")?.extract()?;
                let columns = fields.into_iter().map(|f| f.0).collect();
                DataType::Struct(columns)
            }
            "Map" => {
                let key = ob.getattr("key")?.extract::<Wrap<Column>>()?;
                let value = ob.getattr("value")?.extract::<Wrap<Column>>()?;
                DataType::Map(Box::new(key.0), Box::new(value.0))
            }
            dt => {
                return Err(PyTypeError::new_err(format!(
                    "'{}' is not a valid DuckLake data type",
                    dt
                )));
            }
        };
        Ok(Wrap(dtype))
    }
}

impl<'py> IntoPyObject<'py> for Wrap<DataType> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dl = ducklake_module(py).bind(py);
        match &self.0 {
            DataType::Boolean => dl.getattr("Boolean")?.call0(),
            DataType::Int8 => dl.getattr("Int8")?.call0(),
            DataType::Int16 => dl.getattr("Int16")?.call0(),
            DataType::Int32 => dl.getattr("Int32")?.call0(),
            DataType::Int64 => dl.getattr("Int64")?.call0(),
            DataType::Int128 => dl.getattr("Int128")?.call0(),
            DataType::UInt8 => dl.getattr("UInt8")?.call0(),
            DataType::UInt16 => dl.getattr("UInt16")?.call0(),
            DataType::UInt32 => dl.getattr("UInt32")?.call0(),
            DataType::UInt64 => dl.getattr("UInt64")?.call0(),
            DataType::UInt128 => dl.getattr("UInt128")?.call0(),
            DataType::Float32 => dl.getattr("Float32")?.call0(),
            DataType::Float64 => dl.getattr("Float64")?.call0(),
            DataType::Decimal { precision, scale } => {
                dl.getattr("Decimal")?.call1((*precision, *scale))
            }
            DataType::Time => dl.getattr("Time")?.call0(),
            DataType::TimeTz => dl.getattr("TimeTz")?.call0(),
            DataType::Date => dl.getattr("Date")?.call0(),
            DataType::Timestamp { precision } => {
                let precision_str = match precision {
                    TimestampPrecision::Seconds => "seconds",
                    TimestampPrecision::Milliseconds => "milliseconds",
                    TimestampPrecision::Microseconds => "microseconds",
                    TimestampPrecision::Nanoseconds => "nanoseconds",
                };
                dl.getattr("Timestamp")?.call1((precision_str,))
            }
            DataType::TimestampTz => dl.getattr("TimestampTz")?.call0(),
            DataType::Interval => dl.getattr("Interval")?.call0(),
            DataType::Varchar => dl.getattr("Varchar")?.call0(),
            DataType::Blob => dl.getattr("Blob")?.call0(),
            DataType::Json => dl.getattr("Json")?.call0(),
            DataType::Uuid => dl.getattr("Uuid")?.call0(),
            DataType::List(inner) => {
                let inner_py = Wrap(*inner.clone()).into_pyobject(py)?;
                dl.getattr("List")?.call1((inner_py,))
            }
            DataType::Struct(columns) => {
                let fields: Result<Vec<_>, _> = columns
                    .iter()
                    .map(|col| Wrap(col.clone()).into_pyobject(py))
                    .collect();
                dl.getattr("Struct")?.call1((fields?,))
            }
            DataType::Map(key, value) => {
                let key_py = Wrap(*key.clone()).into_pyobject(py)?;
                let value_py = Wrap(*value.clone()).into_pyobject(py)?;
                dl.getattr("Map")?.call1((key_py, value_py))
            }
        }
    }
}
