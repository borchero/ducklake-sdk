use ducklake::{PartitionColumn, PartitionTransform};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use super::Wrap;
use crate::error;

impl FromPyObject<'_, '_> for Wrap<PartitionColumn> {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let (column, transform_str, num_buckets) =
            ob.extract::<(String, Option<String>, Option<u32>)>()?;
        let transform = match transform_str.as_deref() {
            None => PartitionTransform::Identity,
            Some("bucket") => {
                let n = num_buckets.ok_or_else(|| {
                    PyTypeError::new_err(
                        "Bucket transform requires `num_buckets` to be a positive integer.",
                    )
                })?;
                PartitionTransform::Bucket(n)
            }
            Some(s) => s.parse().map_err(error::into_pyerr)?,
        };
        Ok(Wrap(PartitionColumn { column, transform }))
    }
}

impl<'py> IntoPyObject<'py> for Wrap<PartitionColumn> {
    type Target = PyTuple;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Bound<'py, Self::Target>, Self::Error> {
        let (transform_str, num_buckets): (Option<&str>, Option<u32>) = match self.0.transform {
            PartitionTransform::Identity => (None, None),
            PartitionTransform::Bucket(n) => (Some("bucket"), Some(n)),
            PartitionTransform::Year => (Some("year"), None),
            PartitionTransform::Month => (Some("month"), None),
            PartitionTransform::Day => (Some("day"), None),
            PartitionTransform::Hour => (Some("hour"), None),
        };
        let tuple = (self.0.column, transform_str, num_buckets);
        tuple.into_pyobject(py)
    }
}
