use ducklake::{PartitionColumn, TableMetadata};
use pyo3::prelude::*;
use pyo3_arrow::PyTable as ArrowPyTable;

use crate::conversion::Wrap;
use crate::error;
use crate::utils::filepath_generator::PyDataFilePathGenerator;
use crate::utils::runtime::block_on;

#[pyclass]
pub struct PyTable(ducklake::Table);

impl PyTable {
    pub fn new(table: ducklake::Table) -> Self {
        PyTable(table)
    }
}

#[pymethods]
impl PyTable {
    #[getter]
    pub fn name(&self, py: Python) -> PyResult<(String, String)> {
        let name = block_on(py, self.0.name()).map_err(error::into_pyerr)?;
        Ok((name.schema, name.name))
    }

    #[getter]
    pub fn columns(&self, py: Python) -> PyResult<Vec<Wrap<ducklake::Column>>> {
        let columns = block_on(py, self.0.columns()).map_err(error::into_pyerr)?;
        Ok(columns.into_iter().map(|col| col.into()).collect())
    }

    #[getter]
    pub fn partitioning(&self, py: Python) -> PyResult<Option<Vec<Wrap<PartitionColumn>>>> {
        let partitioning = block_on(py, self.0.partitioning()).map_err(error::into_pyerr)?;
        Ok(partitioning.map(|p| p.into_iter().map(|col| col.into()).collect()))
    }

    #[getter]
    pub fn tags(&self, py: Python) -> PyResult<Vec<Wrap<ducklake::Tag>>> {
        let tags = block_on(py, self.0.tags()).map_err(error::into_pyerr)?;
        Ok(tags.into_iter().map(|tag| tag.into()).collect())
    }

    #[getter]
    pub fn metadata(&self) -> PyResult<Wrap<TableMetadata>> {
        let metadata = self.0.metadata();
        Ok(metadata.into())
    }

    pub fn rename(&mut self, py: Python, new_name: &str) -> PyResult<()> {
        block_on(py, self.0.rename(new_name)).map_err(error::into_pyerr)
    }

    pub fn update_partitioning(
        &mut self,
        py: Python,
        partitioning: Option<Vec<Wrap<PartitionColumn>>>,
    ) -> PyResult<()> {
        let partitioning = partitioning.map(|cols| cols.into_iter().map(|c| c.0).collect());
        block_on(py, self.0.update_partitioning(partitioning)).map_err(error::into_pyerr)
    }

    pub fn add_column(&mut self, py: Python, column: Wrap<ducklake::Column>) -> PyResult<()> {
        block_on(py, self.0.add_column(column.0)).map_err(error::into_pyerr)
    }

    pub fn rename_column(
        &mut self,
        py: Python,
        column: Wrap<ducklake::ColumnName>,
        new_name: &str,
    ) -> PyResult<()> {
        block_on(py, self.0.rename_column(column.0, new_name)).map_err(error::into_pyerr)
    }

    pub fn remove_column(
        &mut self,
        py: Python,
        column: Wrap<ducklake::ColumnName>,
    ) -> PyResult<()> {
        block_on(py, self.0.remove_column(column.0)).map_err(error::into_pyerr)
    }

    pub fn update_column_dtype(
        &mut self,
        py: Python,
        column: Wrap<ducklake::ColumnName>,
        new_dtype: Wrap<ducklake::DataType>,
    ) -> PyResult<()> {
        block_on(py, self.0.update_column_dtype(column.0, new_dtype.0)).map_err(error::into_pyerr)
    }

    pub fn update_column_default(
        &mut self,
        py: Python,
        column: Wrap<ducklake::ColumnName>,
        default_value: Wrap<ducklake::ColumnDefault>,
    ) -> PyResult<()> {
        block_on(py, self.0.update_column_default(column.0, default_value.0))
            .map_err(error::into_pyerr)
    }

    pub fn update_column_nullability(
        &mut self,
        py: Python,
        column: Wrap<ducklake::ColumnName>,
        nullable: bool,
    ) -> PyResult<()> {
        block_on(py, self.0.update_column_nullability(column.0, nullable))
            .map_err(error::into_pyerr)
    }

    pub fn update_schema(
        &mut self,
        py: Python,
        columns: Vec<Wrap<ducklake::Column>>,
    ) -> PyResult<()> {
        let cols = columns.into_iter().map(|c| c.0).collect();
        block_on(py, self.0.update_schema(cols)).map_err(error::into_pyerr)
    }

    pub fn delete(&mut self, py: Python) -> PyResult<()> {
        block_on(py, self.0.delete()).map_err(error::into_pyerr)
    }

    pub fn add_tag(&mut self, py: Python, key: &str, value: &str) -> PyResult<()> {
        block_on(py, self.0.add_tag(key, value)).map_err(error::into_pyerr)
    }

    pub fn remove_tag(&mut self, py: Python, key: &str) -> PyResult<()> {
        block_on(py, self.0.remove_tag(key)).map_err(error::into_pyerr)
    }

    pub fn add_column_tag(
        &mut self,
        py: Python,
        column_path: Wrap<ducklake::ColumnName>,
        key: &str,
        value: &str,
    ) -> PyResult<()> {
        block_on(py, self.0.add_column_tag(column_path.0, key, value)).map_err(error::into_pyerr)
    }

    pub fn remove_column_tag(
        &mut self,
        py: Python,
        column_path: Wrap<ducklake::ColumnName>,
        key: &str,
    ) -> PyResult<()> {
        block_on(py, self.0.remove_column_tag(column_path.0, key)).map_err(error::into_pyerr)
    }

    pub fn get_write_info(
        &self,
        py: Python,
    ) -> PyResult<(Wrap<TableMetadata>, PyDataFilePathGenerator)> {
        let (metadata, generator) =
            block_on(py, self.0.get_write_info()).map_err(error::into_pyerr)?;
        Ok((metadata.into(), PyDataFilePathGenerator::new(generator)))
    }

    pub fn write_data_files(
        &mut self,
        py: Python,
        data_files: Vec<Wrap<ducklake::WriteDataFile>>,
    ) -> PyResult<()> {
        let data_files = data_files.into_iter().map(|df| df.0).collect();
        block_on(py, self.0.write_data_files(data_files)).map_err(error::into_pyerr)
    }

    pub fn write_inline_data(&mut self, py: Python, data: ArrowPyTable) -> PyResult<()> {
        let (batches, _) = data.into_inner();
        block_on(py, self.0.write_inline_data(batches)).map_err(error::into_pyerr)
    }

    pub fn scan(&self, py: Python) -> PyResult<Wrap<ducklake::ScanResult>> {
        let info = block_on(py, self.0.scan()).map_err(error::into_pyerr)?;
        Ok(Wrap(info))
    }

    pub fn set_metadata(&self, py: Python, key: String, value: Option<String>) -> PyResult<()> {
        if let Some(v) = value {
            block_on(py, self.0.set_metadata(&key, &v)).map_err(error::into_pyerr)
        } else {
            block_on(py, self.0.unset_metadata(&key)).map_err(error::into_pyerr)
        }
    }
}
