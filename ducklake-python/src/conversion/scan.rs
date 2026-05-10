use std::sync::Arc;

use ducklake::{DataFileStatistics, ScanDataFile, ScanDeleteFile, ScanResult};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_arrow::PyArray as ArrowPyArray;

use super::Wrap;
use super::py_modules::*;

fn statistics_into_pyobject<'py>(
    py: Python<'py>,
    statistics: DataFileStatistics,
) -> PyResult<Bound<'py, PyAny>> {
    let dl = ducklake_module(py).bind(py);
    let stats_cls = dl.getattr("DataFileStatistics")?;
    let stats_kwargs = PyDict::new(py);
    stats_kwargs.set_item("file_size_bytes", statistics.file_size_bytes)?;
    stats_kwargs.set_item("footer_size_bytes", statistics.footer_size_bytes)?;
    let column_stats = PyDict::new(py);
    for (field_id, stats) in statistics.column_stats {
        column_stats.set_item(field_id, Wrap(stats).into_pyobject(py)?)?;
    }
    stats_kwargs.set_item("column_stats", column_stats)?;
    stats_cls.call((statistics.num_rows,), Some(&stats_kwargs))
}

impl<'py> IntoPyObject<'py> for Wrap<ScanDeleteFile> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dl = ducklake_module(py).bind(py);
        let cls = dl.getattr("DeleteFile")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("file_size_bytes", self.0.file_size_bytes)?;
        kwargs.set_item("footer_size_bytes", self.0.footer_size_bytes)?;
        cls.call((self.0.path, self.0.num_deletes), Some(&kwargs))
    }
}

impl<'py> IntoPyObject<'py> for Wrap<ScanDataFile> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dl = ducklake_module(py).bind(py);
        let cls = dl.getattr("ScanDataFile")?;
        let statistics = statistics_into_pyobject(py, self.0.statistics)?;
        let delete_files: Vec<_> = self
            .0
            .delete_files
            .into_iter()
            .map(|df| Wrap(df).into_pyobject(py))
            .collect::<Result<_, _>>()?;
        let inline_deletes = self
            .0
            .inline_deletes
            .as_ref()
            .map(|array| ArrowPyArray::from_array_ref(array.clone()));
        cls.call1((self.0.path, statistics, delete_files, inline_deletes))
    }
}

impl<'py> IntoPyObject<'py> for Wrap<ScanResult> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dl = ducklake_module(py).bind(py);
        let cls = dl.getattr("ScanResult")?;
        let data_files: Vec<_> = self
            .0
            .data_files
            .into_iter()
            .map(|df| Wrap(df).into_pyobject(py))
            .collect::<Result<_, _>>()?;
        let inline_data: Vec<_> = self
            .0
            .inline_data
            .into_iter()
            .map(|batch| {
                let struct_array = Arc::new(arrow_array::StructArray::from(batch));
                ArrowPyArray::from_array_ref(struct_array)
            })
            .collect();
        cls.call1((data_files, inline_data))
    }
}
