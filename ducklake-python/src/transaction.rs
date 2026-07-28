use std::sync::{Arc, MappedMutexGuard, Mutex, MutexGuard};

use ducklake::{PartitionColumn, TableMetadata, Transaction};
use pyo3::prelude::*;
use pyo3_arrow::PyTable as ArrowPyTable;

use super::error;
use crate::conversion::Wrap;
use crate::utils::filepath_generator::PyDataFilePathGenerator;
use crate::utils::runtime::block_on;

#[repr(transparent)]
#[derive(Clone)]
struct TransactionInner(Arc<Mutex<Option<Transaction<'static>>>>);

#[pyclass]
pub struct PyTransaction(TransactionInner);

#[pyclass]
pub struct PyTransactionTable {
    transaction: TransactionInner,
    table: ducklake::TableName,
}

impl PyTransaction {
    pub fn new(tx: Transaction<'static>) -> Self {
        PyTransaction(TransactionInner(Arc::new(Mutex::new(Some(tx)))))
    }

    fn tx(&mut self) -> MappedMutexGuard<'_, Transaction<'static>> {
        self.0.tx()
    }
}

impl PyTransactionTable {
    fn tx(&mut self) -> MappedMutexGuard<'_, Transaction<'static>> {
        self.transaction.tx()
    }
}

impl TransactionInner {
    fn tx(&mut self) -> MappedMutexGuard<'_, Transaction<'static>> {
        MutexGuard::map(self.0.lock().unwrap(), |opt_tx| {
            opt_tx
                .as_mut()
                .expect("transaction has already been committed")
        })
    }
}

/* --------------------------------------- PYTHON METHODS -------------------------------------- */

#[pymethods]
impl PyTransaction {
    fn create_schema(
        &mut self,
        name: String,
        data_path: Option<String>,
        if_exists: Wrap<ducklake::IfExistsStrategy>,
    ) -> PyResult<()> {
        self.tx()
            .create_schema(&name, data_path, if_exists.0)
            .map_err(error::into_pyerr)
    }

    fn delete_schema(&mut self, name: String) -> PyResult<()> {
        self.tx().delete_schema(&name).map_err(error::into_pyerr)
    }

    fn table(&mut self, name: Wrap<ducklake::TableName>) -> PyResult<PyTransactionTable> {
        // NOTE: We don't use the return value here as we'll run into lifetime issues in the
        //  Python bindings. We still use it to check that the table exists.
        self.tx().table(name.0.clone()).map_err(error::into_pyerr)?;
        Ok(PyTransactionTable {
            transaction: self.0.clone(),
            table: name.0,
        })
    }

    fn create_table(
        &mut self,
        name: Wrap<ducklake::TableName>,
        schema: Vec<Wrap<ducklake::Column>>,
        partition: Option<Vec<Wrap<ducklake::PartitionColumn>>>,
        data_path: Option<String>,
        tags: Option<Vec<Wrap<ducklake::Tag>>>,
        if_exists: Wrap<ducklake::IfExistsStrategy>,
    ) -> PyResult<PyTransactionTable> {
        self.tx()
            .create_table(
                name.0.clone(),
                schema.into_iter().map(|c| c.0).collect(),
                partition.map(|v| v.into_iter().map(|p| p.0).collect()),
                data_path,
                tags.map(|v| v.into_iter().map(|t| t.0).collect()),
                if_exists.0,
            )
            .map_err(error::into_pyerr)?;
        Ok(PyTransactionTable {
            transaction: self.0.clone(),
            table: name.0,
        })
    }

    fn commit(&mut self, py: Python) -> PyResult<()> {
        let mut guard = self.0.0.lock().unwrap();
        if let Some(tx) = guard.take() {
            block_on(py, tx.commit()).map_err(error::into_pyerr)
        } else {
            // Transaction has already been committed, we just do nothing
            Ok(())
        }
    }
}

#[pymethods]
impl PyTransactionTable {
    #[getter]
    pub fn columns(&mut self) -> PyResult<Vec<Wrap<ducklake::Column>>> {
        let table = self.table.clone();
        let mut tx_guard = self.tx();
        let tx_table = tx_guard.table(table).map_err(error::into_pyerr)?;
        let columns = tx_table.columns().map_err(error::into_pyerr)?;
        Ok(columns.into_iter().map(|col| col.into()).collect())
    }

    #[getter]
    pub fn partitioning(&mut self) -> PyResult<Option<Vec<Wrap<PartitionColumn>>>> {
        let table = self.table.clone();
        let mut tx_guard = self.tx();
        let tx_table = tx_guard.table(table).map_err(error::into_pyerr)?;
        let partitioning = tx_table.partitioning().map_err(error::into_pyerr)?;
        Ok(partitioning.map(|p| p.into_iter().map(|col| col.into()).collect()))
    }

    fn get_write_info(&mut self) -> PyResult<(Wrap<TableMetadata>, PyDataFilePathGenerator)> {
        let table = self.table.clone();
        let (metadata, generator) = self
            .tx()
            .get_table_write_info(&table)
            .map_err(error::into_pyerr)?;
        Ok((metadata.into(), PyDataFilePathGenerator::new(generator)))
    }

    fn write_data_files(
        &mut self,
        py: Python,
        data_files: Vec<Wrap<ducklake::WriteDataFile>>,
    ) -> PyResult<()> {
        let table = self.table.clone();
        let data_files: Vec<_> = data_files.into_iter().map(|df| df.0).collect();
        let mut tx_guard = self.tx();
        block_on(py, tx_guard.write_table_data_files(&table, data_files))
            .map_err(error::into_pyerr)
    }

    fn write_inline_data(&mut self, data: ArrowPyTable) -> PyResult<()> {
        let table = self.table.clone();
        let (batches, _) = data.into_inner();
        self.tx()
            .write_table_inline_data(&table, batches)
            .map_err(error::into_pyerr)
    }

    fn rename(&mut self, new_name: String) -> PyResult<()> {
        let table = self.table.clone();
        self.tx()
            .rename_table(&table, &new_name)
            .map_err(error::into_pyerr)
    }

    fn update_partitioning(
        &mut self,
        partitioning: Option<Vec<Wrap<ducklake::PartitionColumn>>>,
    ) -> PyResult<()> {
        let table = self.table.clone();
        let partitioning = partitioning.map(|cols| cols.into_iter().map(|c| c.0).collect());
        self.tx()
            .update_table_partitioning(&table, partitioning)
            .map_err(error::into_pyerr)
    }

    fn add_column(&mut self, py: Python, column: Wrap<ducklake::Column>) -> PyResult<()> {
        let table = self.table.clone();
        let mut tx_guard = self.tx();
        block_on(
            py,
            tx_guard.add_table_column(&table, column.0, &Default::default()),
        )
        .map_err(error::into_pyerr)
    }

    fn rename_column(
        &mut self,
        column: Wrap<ducklake::ColumnName>,
        new_name: String,
    ) -> PyResult<()> {
        let table = self.table.clone();
        self.tx()
            .rename_table_column(&table, &column.0, &new_name)
            .map_err(error::into_pyerr)
    }

    fn remove_column(&mut self, column: Wrap<ducklake::ColumnName>) -> PyResult<()> {
        let table = self.table.clone();
        self.tx()
            .remove_table_column(&table, &column.0)
            .map_err(error::into_pyerr)
    }

    fn update_column_dtype(
        &mut self,
        py: Python,
        column: Wrap<ducklake::ColumnName>,
        new_dtype: Wrap<ducklake::DataType>,
    ) -> PyResult<()> {
        let table = self.table.clone();
        let mut tx_guard = self.tx();
        block_on(
            py,
            tx_guard.update_table_column_dtype(&table, &column.0, new_dtype.0),
        )
        .map_err(error::into_pyerr)
    }

    fn update_column_default(
        &mut self,
        column: Wrap<ducklake::ColumnName>,
        default_value: Wrap<ducklake::ColumnDefault>,
    ) -> PyResult<()> {
        let table = self.table.clone();
        self.tx()
            .update_table_column_default(&table, &column.0, default_value.0)
            .map_err(error::into_pyerr)
    }

    fn update_column_nullability(
        &mut self,
        py: Python,
        column: Wrap<ducklake::ColumnName>,
        nullable: bool,
    ) -> PyResult<()> {
        let table = self.table.clone();
        let mut tx_guard = self.tx();
        block_on(
            py,
            tx_guard.update_table_column_nullability(&table, &column.0, nullable),
        )
        .map_err(error::into_pyerr)
    }

    fn update_schema(&mut self, py: Python, columns: Vec<Wrap<ducklake::Column>>) -> PyResult<()> {
        let table = self.table.clone();
        let mut tx_guard = self.tx();
        block_on(
            py,
            tx_guard.update_table_schema(&table, columns.into_iter().map(|c| c.0).collect()),
        )
        .map_err(error::into_pyerr)
    }

    fn delete(&mut self) -> PyResult<()> {
        let table = self.table.clone();
        self.tx().delete_table(&table).map_err(error::into_pyerr)
    }

    fn add_tag(&mut self, key: String, value: String) -> PyResult<()> {
        let table = self.table.clone();
        self.tx()
            .add_table_tag(&table, &key, &value)
            .map_err(error::into_pyerr)
    }

    fn remove_tag(&mut self, key: String) -> PyResult<()> {
        let table = self.table.clone();
        self.tx()
            .remove_table_tag(&table, &key)
            .map_err(error::into_pyerr)
    }

    fn add_column_tag(
        &mut self,
        column_path: Wrap<ducklake::ColumnName>,
        key: String,
        value: String,
    ) -> PyResult<()> {
        let table = self.table.clone();
        self.tx()
            .add_table_column_tag(&table, &column_path.0, &key, &value)
            .map_err(error::into_pyerr)
    }

    fn remove_column_tag(
        &mut self,
        column_path: Wrap<ducklake::ColumnName>,
        key: String,
    ) -> PyResult<()> {
        let table = self.table.clone();
        self.tx()
            .remove_table_column_tag(&table, &column_path.0, &key)
            .map_err(error::into_pyerr)
    }
}
