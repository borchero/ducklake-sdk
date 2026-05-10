use indexmap::IndexMap;
use pyo3::prelude::*;

use crate::conversion::Wrap;

/* ----------------------------------------- DATA FILE ----------------------------------------- */

#[pyclass]
pub struct PyDataFilePathGenerator(ducklake::DataFilePathGenerator);

impl PyDataFilePathGenerator {
    pub fn new(generator: ducklake::DataFilePathGenerator) -> Self {
        PyDataFilePathGenerator(generator)
    }
}

#[pymethods]
impl PyDataFilePathGenerator {
    #[getter]
    pub fn base_path(&self) -> &str {
        self.0.base_path()
    }

    pub fn generate_relative(
        &self,
        partition_values: Vec<(String, Option<Wrap<ducklake::Value>>)>,
    ) -> String {
        let values: IndexMap<String, Option<ducklake::Value>> = partition_values
            .into_iter()
            .map(|(k, v)| (k, v.map(|v| v.0)))
            .collect();
        self.0.generate_relative(&values)
    }

    pub fn generate_absolute(
        &self,
        partition_values: Vec<(String, Option<Wrap<ducklake::Value>>)>,
    ) -> String {
        let values: IndexMap<String, Option<ducklake::Value>> = partition_values
            .into_iter()
            .map(|(k, v)| (k, v.map(|v| v.0)))
            .collect();
        self.0.generate_absolute(&values)
    }
}
