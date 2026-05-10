use ducklake::ColumnName;
use pyo3::prelude::*;

use super::Wrap;
use crate::error;

#[derive(FromPyObject)]
enum StringOrList {
    #[pyo3(transparent)]
    List(Vec<String>),
    #[pyo3(transparent)]
    Single(String),
}

impl FromPyObject<'_, '_> for Wrap<ColumnName> {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let name = match ob.extract::<StringOrList>()? {
            StringOrList::List(names) => names.into(),
            StringOrList::Single(name) => name.parse().map_err(error::into_pyerr)?,
        };
        Ok(Wrap(name))
    }
}
