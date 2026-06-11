use ducklake::IfExistsStrategy;
use pyo3::prelude::*;

use super::Wrap;

impl FromPyObject<'_, '_> for Wrap<IfExistsStrategy> {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let strategy_str = ob.extract::<String>()?;
        let strategy = match strategy_str.as_str() {
            "fail" => IfExistsStrategy::Fail,
            "skip" => IfExistsStrategy::Skip,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Invalid IfExistsStrategy: '{}'. Expected 'fail' or 'skip'.",
                    strategy_str
                )));
            }
        };
        Ok(Wrap(strategy))
    }
}
