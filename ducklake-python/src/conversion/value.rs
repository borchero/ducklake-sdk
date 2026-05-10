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
use ducklake::{Interval, TimeWithTimezone, Value};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{
    PyBool,
    PyBytes,
    PyDate,
    PyDateTime,
    PyDelta,
    PyDict,
    PyFloat,
    PyInt,
    PyList,
    PyString,
    PyTime,
};

use super::Wrap;
use super::py_modules::*;

impl FromPyObject<'_, '_> for Wrap<Value> {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let py = ob.py();

        // NOTE:
        //  - bool must be checked before int (Python bool is a subclass of int)
        //  - datetime must be checked before date (Python datetime is a subclass of date)
        if ob.is_instance_of::<PyBool>() {
            value_from_bool(&ob)
        } else if ob.is_instance_of::<PyInt>() {
            value_from_int(&ob)
        } else if ob.is_instance_of::<PyFloat>() {
            value_from_float(&ob)
        } else if ob.is_instance_of::<PyString>() {
            value_from_str(&ob)
        } else if ob.is_instance_of::<PyBytes>() {
            value_from_bytes(&ob)
        } else if ob.is_instance_of::<PyDateTime>() {
            value_from_datetime(&ob)
        } else if ob.is_instance_of::<PyDate>() {
            value_from_date(&ob)
        } else if ob.is_instance_of::<PyTime>() {
            value_from_time(&ob)
        } else if ob.is_instance_of::<PyDelta>() {
            value_from_timedelta(&ob)
        } else if ob.is_instance_of::<PyList>() {
            value_from_list(&ob)
        } else if ob.is_instance_of::<PyDict>() {
            value_from_dict(&ob)
        } else {
            let dec_mod = decimal_module(py).bind(py);
            let decimal_cls = dec_mod.getattr("Decimal")?;
            if ob.is_instance(&decimal_cls)? {
                return value_from_decimal(&ob).map(Wrap);
            }

            let uuid_mod = uuid_module(py).bind(py);
            let uuid_cls = uuid_mod.getattr("UUID")?;
            if ob.is_instance(&uuid_cls)? {
                return value_from_uuid(&ob).map(Wrap);
            }

            let relativedelta_mod = relativedelta_module(py).bind(py);
            let relativedelta_cls = relativedelta_mod.getattr("relativedelta")?;
            if ob.is_instance(&relativedelta_cls)? {
                return value_from_relativedelta(&ob).map(Wrap);
            }

            Err(PyTypeError::new_err(format!(
                "Cannot convert '{}' to a DuckLake value",
                ob.get_type().qualname()?
            )))
        }
        .map(Wrap)
    }
}

impl<'py> IntoPyObject<'py> for Wrap<Value> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self.0 {
            Value::Boolean(v) => bool_from_value(py, v),
            Value::Int8(v) => int_from_value(py, v as i64),
            Value::Int16(v) => int_from_value(py, v as i64),
            Value::Int32(v) => int_from_value(py, v as i64),
            Value::Int64(v) => int_from_value(py, v),
            Value::Int128(v) => int128_from_value(py, v),
            Value::UInt8(v) => uint_from_value(py, v as u64),
            Value::UInt16(v) => uint_from_value(py, v as u64),
            Value::UInt32(v) => uint_from_value(py, v as u64),
            Value::UInt64(v) => uint_from_value(py, v),
            Value::UInt128(v) => uint128_from_value(py, v),
            Value::Float32(v) => float_from_value(py, v as f64),
            Value::Float64(v) => float_from_value(py, v),
            Value::Decimal(v) => decimal_from_value(py, v),
            Value::Date(date) => date_from_value(py, date),
            Value::Time(time) => time_from_value(py, time, None),
            Value::TimeTz(t) => time_from_value(py, t.time, Some(t.offset)),
            Value::Timestamp(dt) => timestamp_from_value(py, dt),
            Value::TimestampTz(dt) => timestamptz_from_value(py, dt),
            Value::Interval(i) => relativedelta_from_value(py, i.months, i.delta),
            Value::Varchar(v) => varchar_from_value(py, v),
            Value::Json(v) => json_from_value(py, v),
            Value::Blob(v) => blob_from_value(py, v),
            Value::Uuid(v) => uuid_from_value(py, v),
            Value::List(values) => list_from_value(py, values),
            Value::Struct(map) => struct_from_value(py, map),
            Value::Map(entries) => map_from_value(py, entries),
        }
    }
}

/* --------------------------------------- PYTHON -> RUST -------------------------------------- */

fn value_from_bool(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
    Ok(Value::Boolean(ob.extract::<bool>()?))
}

fn value_from_int(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
    if let Ok(v) = ob.extract::<i64>() {
        Ok(Value::Int64(v))
    } else {
        Ok(Value::UInt64(ob.extract::<u64>()?))
    }
}

fn value_from_float(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
    Ok(Value::Float64(ob.extract::<f64>()?))
}

fn value_from_str(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
    Ok(Value::Varchar(ob.extract::<String>()?))
}

fn value_from_bytes(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
    Ok(Value::Blob(ob.extract::<Vec<u8>>()?))
}

fn value_from_datetime(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
    let is_timezone_aware = !ob.getattr("tzinfo")?.is_none();
    if is_timezone_aware {
        Ok(Value::TimestampTz(ob.extract()?))
    } else {
        Ok(Value::Timestamp(ob.extract()?))
    }
}

fn value_from_date(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
    Ok(Value::Date(ob.extract()?))
}

fn value_from_time(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
    let is_timezone_aware = !ob.getattr("tzinfo")?.is_none();
    if is_timezone_aware {
        Ok(Value::TimeTz(TimeWithTimezone {
            time: ob.extract()?,
            offset: ob.extract()?,
        }))
    } else {
        Ok(Value::Time(ob.extract()?))
    }
}

fn value_from_timedelta(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
    let days: i64 = ob.getattr("days")?.extract()?;
    let seconds: i64 = ob.getattr("seconds")?.extract()?;
    let microseconds: i64 = ob.getattr("microseconds")?.extract()?;
    let micros = days * 86_400_000_000 + seconds * 1_000_000 + microseconds;
    Ok(Value::Interval(Interval {
        months: Months::new(0),
        delta: TimeDelta::microseconds(micros),
    }))
}

fn value_from_relativedelta(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
    let years: u32 = ob.getattr("years")?.extract()?;
    let months: u32 = ob.getattr("months")?.extract()?;
    let weeks: i64 = ob.getattr("weeks")?.extract()?;
    let days: i64 = ob.getattr("days")?.extract()?;
    let hours: i64 = ob.getattr("hours")?.extract()?;
    let minutes: i64 = ob.getattr("minutes")?.extract()?;
    let seconds: i64 = ob.getattr("seconds")?.extract()?;
    let microseconds: i64 = ob.getattr("microseconds")?.extract()?;
    Ok(Value::Interval(Interval {
        months: Months::new(years * 12 + months),
        delta: TimeDelta::microseconds(
            ((weeks * 7 + days) * 86_400 + hours * 3_600 + minutes * 60 + seconds) * 1_000_000
                + microseconds,
        ),
    }))
}

fn value_from_list(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
    let list: &Bound<'_, PyList> = ob.cast::<PyList>()?;
    let values: Vec<Option<Value>> = list
        .iter()
        .map(|item| {
            item.extract::<Option<Wrap<Value>>>()
                .map(|w| w.map(|v| v.0))
        })
        .collect::<PyResult<_>>()?;
    Ok(Value::List(values))
}

fn value_from_dict(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
    let dict: &Bound<'_, PyDict> = ob.cast::<PyDict>()?;
    let mut map = indexmap::IndexMap::new();
    for (k, v) in dict.iter() {
        let key: String = k.extract()?;
        let value = v.extract::<Option<Wrap<Value>>>()?;
        map.insert(key, value.map(|w| w.0));
    }
    Ok(Value::Struct(map))
}

fn value_from_decimal(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
    let s: String = ob.str()?.extract()?;
    let dec: rust_decimal::Decimal = s
        .parse()
        .map_err(|e| PyTypeError::new_err(format!("Invalid decimal: {e}")))?;
    Ok(Value::Decimal(dec))
}

fn value_from_uuid(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
    let s: String = ob.str()?.extract()?;
    let uuid: uuid::Uuid = s
        .parse()
        .map_err(|e| PyTypeError::new_err(format!("Invalid UUID: {e}")))?;
    Ok(Value::Uuid(uuid))
}

/* --------------------------------------- RUST -> PYTHON -------------------------------------- */

fn bool_from_value<'py>(py: Python<'py>, v: bool) -> PyResult<Bound<'py, PyAny>> {
    Ok(v.into_pyobject(py)?.to_owned().into_any())
}

fn int_from_value<'py>(py: Python<'py>, v: i64) -> PyResult<Bound<'py, PyAny>> {
    Ok(v.into_pyobject(py)?.to_owned().into_any())
}

fn uint_from_value<'py>(py: Python<'py>, v: u64) -> PyResult<Bound<'py, PyAny>> {
    Ok(v.into_pyobject(py)?.to_owned().into_any())
}

fn int128_from_value<'py>(py: Python<'py>, v: i128) -> PyResult<Bound<'py, PyAny>> {
    Ok(v.into_pyobject(py)?.to_owned().into_any())
}

fn uint128_from_value<'py>(py: Python<'py>, v: u128) -> PyResult<Bound<'py, PyAny>> {
    Ok(v.into_pyobject(py)?.to_owned().into_any())
}

fn float_from_value<'py>(py: Python<'py>, v: f64) -> PyResult<Bound<'py, PyAny>> {
    Ok(v.into_pyobject(py)?.to_owned().into_any())
}

fn decimal_from_value<'py>(
    py: Python<'py>,
    v: rust_decimal::Decimal,
) -> PyResult<Bound<'py, PyAny>> {
    let dm = decimal_module(py).bind(py);
    let decimal_cls = dm.getattr("Decimal")?;
    decimal_cls.call1((v.to_string(),))
}

fn date_from_value<'py>(py: Python<'py>, date: NaiveDate) -> PyResult<Bound<'py, PyAny>> {
    Ok(date.into_pyobject(py)?.as_any().to_owned())
}

fn time_from_value<'py>(
    py: Python<'py>,
    time: NaiveTime,
    offset: Option<FixedOffset>,
) -> PyResult<Bound<'py, PyAny>> {
    let dt = datetime_module(py).bind(py);
    let time_cls = dt.getattr("time")?;

    let offset = match offset {
        Some(offset) => Some(offset.into_pyobject(py)?),
        None => None,
    };
    time_cls.call1((
        time.hour(),
        time.minute(),
        time.second(),
        time.nanosecond() / 1000,
        offset,
    ))
}

fn timestamp_from_value<'py>(
    py: Python<'py>,
    naive: NaiveDateTime,
) -> PyResult<Bound<'py, PyAny>> {
    Ok(naive.into_pyobject(py)?.as_any().to_owned())
}

fn timestamptz_from_value<'py>(py: Python<'py>, dt: DateTime<Utc>) -> PyResult<Bound<'py, PyAny>> {
    Ok(dt.into_pyobject(py)?.as_any().to_owned())
}

fn relativedelta_from_value<'py>(
    py: Python<'py>,
    months: Months,
    delta: TimeDelta,
) -> PyResult<Bound<'py, PyAny>> {
    let rd_mod = relativedelta_module(py).bind(py);
    let relativedelta_cls = rd_mod.getattr("relativedelta")?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("months", months.as_u32())?;
    kwargs.set_item("microseconds", delta.num_microseconds().unwrap_or(0))?;
    relativedelta_cls.call((), Some(&kwargs))
}

fn varchar_from_value<'py>(py: Python<'py>, v: String) -> PyResult<Bound<'py, PyAny>> {
    Ok(v.into_pyobject(py)?.into_any())
}

fn json_from_value<'py>(py: Python<'py>, v: String) -> PyResult<Bound<'py, PyAny>> {
    Ok(v.into_pyobject(py)?.into_any())
}

fn blob_from_value<'py>(py: Python<'py>, v: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
    Ok((&v[..]).into_pyobject(py)?.into_any())
}

fn uuid_from_value<'py>(py: Python<'py>, v: uuid::Uuid) -> PyResult<Bound<'py, PyAny>> {
    let um = uuid_module(py).bind(py);
    let uuid_cls = um.getattr("UUID")?;
    uuid_cls.call1((v.to_string(),))
}

fn list_from_value<'py>(
    py: Python<'py>,
    values: Vec<Option<Value>>,
) -> PyResult<Bound<'py, PyAny>> {
    let items: Vec<Bound<'py, PyAny>> = values
        .into_iter()
        .map(|v| v.map(Wrap).into_pyobject(py))
        .collect::<Result<_, _>>()?;
    Ok(PyList::new(py, items)?.into_any())
}

fn struct_from_value<'py>(
    py: Python<'py>,
    map: indexmap::IndexMap<String, Option<Value>>,
) -> PyResult<Bound<'py, PyAny>> {
    let dict = PyDict::new(py);
    for (k, v) in map {
        dict.set_item(k, v.map(Wrap).into_pyobject(py)?)?;
    }
    Ok(dict.into_any())
}

fn map_from_value<'py>(
    py: Python<'py>,
    entries: Vec<(Value, Option<Value>)>,
) -> PyResult<Bound<'py, PyAny>> {
    let dict = PyDict::new(py);
    for (k, v) in entries {
        dict.set_item(Wrap(k).into_pyobject(py)?, v.map(Wrap).into_pyobject(py)?)?;
    }
    Ok(dict.into_any())
}
