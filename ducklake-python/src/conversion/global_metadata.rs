use ducklake::GlobalMetadata;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::Wrap;
use super::py_modules::*;

impl<'py> IntoPyObject<'py> for Wrap<GlobalMetadata> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dl = ducklake_module(py).bind(py);
        let cls = dl.getattr("GlobalMetadata")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("data_path", self.0.data_path)?;
        cls.call((), Some(&kwargs))
    }
}
