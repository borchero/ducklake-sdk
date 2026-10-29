use ducklake::DatabaseOptions;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::Wrap;

impl FromPyObject<'_, '_> for Wrap<DatabaseOptions> {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let mut options = DatabaseOptions::default();
        for (key, value) in ob.cast::<PyDict>()?.iter() {
            let key: String = key.extract()?;
            match key.as_str() {
                "max_connections" => options.max_connections = value.extract()?,
                "min_connections" => options.min_connections = value.extract()?,
                "acquire_timeout" => options.acquire_timeout = value.extract()?,
                "test_before_acquire" => options.test_before_acquire = value.extract()?,
                "idle_timeout" => options.idle_timeout = value.extract()?,
                "max_lifetime" => options.max_lifetime = value.extract()?,
                _ => {
                    return Err(PyValueError::new_err(format!(
                        "unknown database option '{key}'"
                    )));
                }
            }
        }
        Ok(Wrap(options))
    }
}
