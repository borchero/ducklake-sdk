use ducklake::{ColumnDefault, Value};
use pyo3::prelude::*;

use super::Wrap;

impl FromPyObject<'_, '_> for Wrap<ColumnDefault> {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        if let Ok((dialect, expression)) = ob.extract::<(String, String)>() {
            Ok(Wrap(ColumnDefault::Expression {
                dialect,
                expression,
            }))
        } else {
            let val = ob.extract::<Option<Wrap<Value>>>()?;
            Ok(Wrap(ColumnDefault::Literal(val.map(|w| w.0))))
        }
    }
}
