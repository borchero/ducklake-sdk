use std::collections::HashMap;

use ducklake::Tags;
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyDict};

use super::Wrap;

impl<'py> IntoPyObject<'py> for Wrap<Tags> {
    type Target = PyDict;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.0.iter().into_py_dict(py)
    }
}

impl FromPyObject<'_, '_> for Wrap<Tags> {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let tags = ob.extract::<HashMap<String, String>>()?;
        Ok(Wrap(tags.into()))
    }
}
