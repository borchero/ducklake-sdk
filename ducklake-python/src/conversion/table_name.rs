use ducklake::TableName;
use pyo3::prelude::*;

use super::Wrap;
use crate::error;

#[derive(FromPyObject)]
enum StringOrTuple {
    #[pyo3(transparent)]
    Tuple((String, String)),
    #[pyo3(transparent)]
    Single(String),
}

impl FromPyObject<'_, '_> for Wrap<TableName> {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let name = match ob.extract::<StringOrTuple>()? {
            StringOrTuple::Tuple((schema, name)) => TableName { schema, name },
            StringOrTuple::Single(name) => name.parse().map_err(error::into_pyerr)?,
        };
        Ok(Wrap(name))
    }
}
