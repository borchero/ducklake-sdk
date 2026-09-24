use ducklake::TableStatistics;
use pyo3::prelude::*;

use super::Wrap;
use super::py_modules::*;

impl<'py> IntoPyObject<'py> for Wrap<TableStatistics> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let cls = ducklake_module(py).bind(py).getattr("TableStatistics")?;
        let stats = cls.call0()?;
        stats.setattr("num_rows", self.0.num_rows)?;
        stats.setattr("num_inline_rows", self.0.num_inline_rows)?;
        stats.setattr("num_deleted_rows", self.0.num_deleted_rows)?;
        stats.setattr("num_data_files", self.0.num_data_files)?;
        stats.setattr("total_file_size_bytes", self.0.total_file_size_bytes)?;
        stats.setattr("min_file_size_bytes", self.0.min_file_size_bytes)?;
        stats.setattr("max_file_size_bytes", self.0.max_file_size_bytes)?;
        Ok(stats)
    }
}
