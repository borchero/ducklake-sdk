use ducklake::Tag;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use super::Wrap;

impl FromPyObject<'_, '_> for Wrap<Tag> {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let (key, value) = ob.extract::<(String, String)>()?;
        Ok(Wrap(Tag { key, value }))
    }
}

impl<'py> IntoPyObject<'py> for Wrap<Tag> {
    type Target = PyTuple;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let tuple = (self.0.key, self.0.value);
        tuple.into_pyobject(py)
    }
}
