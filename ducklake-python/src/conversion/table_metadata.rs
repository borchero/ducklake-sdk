use ducklake::TableMetadata;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::Wrap;
use super::py_modules::*;

impl<'py> IntoPyObject<'py> for Wrap<TableMetadata> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dl = ducklake_module(py).bind(py);
        let cls = dl.getattr("TableMetadata")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("data_inlining_row_limit", self.0.data_inlining_row_limit)?;
        kwargs.set_item("target_file_size", self.0.target_file_size)?;
        kwargs.set_item(
            "parquet_row_group_size_bytes",
            self.0.parquet_row_group_size_bytes,
        )?;
        kwargs.set_item("parquet_row_group_size", self.0.parquet_row_group_size)?;
        kwargs.set_item("parquet_compression", self.0.parquet_compression)?;
        kwargs.set_item(
            "parquet_compression_level",
            self.0.parquet_compression_level,
        )?;
        kwargs.set_item("parquet_version", self.0.parquet_version)?;
        kwargs.set_item("hive_file_pattern", self.0.hive_file_pattern)?;
        kwargs.set_item("rewrite_delete_threshold", self.0.rewrite_delete_threshold)?;
        kwargs.set_item("auto_compact", self.0.auto_compact)?;
        cls.call((), Some(&kwargs))
    }
}
