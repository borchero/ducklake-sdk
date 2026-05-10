use ducklake::{FileColumnStats, Value};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::Wrap;
use super::py_modules::*;

impl FromPyObject<'_, '_> for Wrap<FileColumnStats> {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let size_bytes: Option<usize> = ob.getattr("size_bytes")?.extract()?;
        let min_value = ob.getattr("min_value")?.extract::<Option<Wrap<Value>>>()?;
        let max_value = ob.getattr("max_value")?.extract::<Option<Wrap<Value>>>()?;
        let null_count: Option<usize> = ob.getattr("null_count")?.extract()?;
        let contains_nan: Option<bool> = ob.getattr("contains_nan")?.extract()?;
        Ok(Wrap(FileColumnStats {
            size_bytes,
            min_value: min_value.map(|v| v.0),
            max_value: max_value.map(|v| v.0),
            null_count,
            contains_nan,
        }))
    }
}

impl<'py> IntoPyObject<'py> for Wrap<FileColumnStats> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dl = ducklake_module(py).bind(py);
        let cls = dl.getattr("ColumnStats")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("size_bytes", self.0.size_bytes)?;
        kwargs.set_item("min_value", self.0.min_value.map(Wrap).into_pyobject(py)?)?;
        kwargs.set_item("max_value", self.0.max_value.map(Wrap).into_pyobject(py)?)?;
        kwargs.set_item("null_count", self.0.null_count)?;
        kwargs.set_item("contains_nan", self.0.contains_nan)?;
        cls.call((), Some(&kwargs))
    }
}
