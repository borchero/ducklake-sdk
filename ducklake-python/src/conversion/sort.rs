use ducklake::{NullOrder, SortDirection, SortExpression};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use super::Wrap;

impl FromPyObject<'_, '_> for Wrap<SortExpression> {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let (expression, dialect, direction, null_order) =
            ob.extract::<(String, String, String, String)>()?;
        let direction = match direction.as_str() {
            "ascending" => SortDirection::Ascending,
            "descending" => SortDirection::Descending,
            _ => return Err(PyValueError::new_err("Invalid sort direction")),
        };
        let null_order = match null_order.as_str() {
            "nulls_first" => NullOrder::NullsFirst,
            "nulls_last" => NullOrder::NullsLast,
            _ => return Err(PyValueError::new_err("Invalid null order")),
        };
        Ok(Wrap(SortExpression {
            expression,
            dialect,
            direction,
            null_order,
        }))
    }
}

impl<'py> IntoPyObject<'py> for Wrap<SortExpression> {
    type Target = PyTuple;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Bound<'py, Self::Target>, Self::Error> {
        (
            self.0.expression,
            self.0.dialect,
            match self.0.direction {
                SortDirection::Ascending => "ascending",
                SortDirection::Descending => "descending",
            },
            match self.0.null_order {
                NullOrder::NullsFirst => "nulls_first",
                NullOrder::NullsLast => "nulls_last",
            },
        )
            .into_pyobject(py)
    }
}
