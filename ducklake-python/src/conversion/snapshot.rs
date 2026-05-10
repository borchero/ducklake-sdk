use ducklake::SnapshotMetadata;
use pyo3::prelude::*;

use super::Wrap;
use super::py_modules::*;

impl<'py> IntoPyObject<'py> for Wrap<SnapshotMetadata> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dl = ducklake_module(py).bind(py);
        let cls = dl.getattr("SnapshotMetadata")?;
        let snapshot = cls.call0()?;
        snapshot.setattr("id", self.0.id)?;
        snapshot.setattr("timestamp", self.0.timestamp)?;
        Ok(snapshot)
    }
}
