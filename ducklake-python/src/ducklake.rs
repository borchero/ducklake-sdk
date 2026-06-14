use ducklake::{AuthorInfo, ConnectOptions, CreateOptions, DryRun, Ducklake, SnapshotMetadata};
use pyo3::prelude::*;

use crate::conversion::Wrap;
use crate::utils::runtime::block_on;
use crate::{PyTable, PyTransaction, error};

#[pyclass]
pub struct PyDucklake(Ducklake);

/* ------------------------------------------ CONNECT ------------------------------------------ */

#[pyfunction]
pub(crate) fn create(
    py: Python,
    url: &str,
    data_path: &str,
    storage_options: Vec<(String, String)>,
) -> PyResult<PyDucklake> {
    let options = CreateOptions::new(url, data_path).with_storage_options(storage_options);
    let ducklake = block_on(py, Ducklake::create(options)).map_err(error::into_pyerr)?;
    Ok(PyDucklake(ducklake))
}

#[pyfunction]
pub(crate) fn connect(
    py: Python,
    url: &str,
    snapshot_id: Option<i64>,
    snapshot_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    migrate: bool,
    storage_options: Vec<(String, String)>,
) -> PyResult<PyDucklake> {
    let mut options = ConnectOptions::new(url)
        .with_migrate(migrate)
        .with_storage_options(storage_options);
    if let Some(id) = snapshot_id {
        options = options.with_snapshot_id(id);
    } else if let Some(timestamp) = snapshot_timestamp {
        options = options.with_snapshot_timestamp(timestamp);
    }
    let ducklake = block_on(py, Ducklake::connect(options)).map_err(error::into_pyerr)?;
    Ok(PyDucklake(ducklake))
}

/* --------------------------------------- PYTHON METHODS -------------------------------------- */

#[pymethods]
impl PyDucklake {
    pub fn at_snapshot_id(&self, py: Python, snapshot_id: i64) -> PyResult<PyDucklake> {
        block_on(py, self.0.at_snapshot_id(snapshot_id))
            .map(PyDucklake)
            .map_err(error::into_pyerr)
    }

    pub fn at_snapshot_timestamp(
        &self,
        py: Python,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> PyResult<PyDucklake> {
        block_on(py, self.0.at_snapshot_timestamp(timestamp))
            .map(PyDucklake)
            .map_err(error::into_pyerr)
    }

    pub fn get_latest_snapshot(&self, py: Python) -> PyResult<Wrap<SnapshotMetadata>> {
        block_on(py, self.0.latest_snapshot())
            .map(Wrap)
            .map_err(error::into_pyerr)
    }

    pub fn list_snapshots(&self, py: Python) -> PyResult<Vec<Wrap<SnapshotMetadata>>> {
        block_on(py, self.0.list_snapshots())
            .map(|snapshots| snapshots.into_iter().map(Wrap).collect())
            .map_err(error::into_pyerr)
    }

    pub fn create_schema(
        &self,
        py: Python,
        name: String,
        data_path: Option<String>,
        if_exists: Wrap<ducklake::IfExistsStrategy>,
    ) -> PyResult<()> {
        block_on(py, self.0.create_schema(&name, data_path, if_exists.0))
            .map_err(error::into_pyerr)
    }

    pub fn delete_schema(&self, py: Python, name: String) -> PyResult<()> {
        block_on(py, self.0.delete_schema(&name)).map_err(error::into_pyerr)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_table(
        &self,
        py: Python,
        name: Wrap<ducklake::TableName>,
        schema: Vec<Wrap<ducklake::Column>>,
        partition: Option<Vec<Wrap<ducklake::PartitionColumn>>>,
        data_path: Option<String>,
        tags: Option<Vec<Wrap<ducklake::Tag>>>,
        if_exists: Wrap<ducklake::IfExistsStrategy>,
    ) -> PyResult<PyTable> {
        block_on(
            py,
            self.0.create_table(
                name.0.clone(),
                schema.into_iter().map(|c| c.0).collect(),
                partition.map(|v| v.into_iter().map(|p| p.0).collect()),
                data_path,
                tags.map(|v| v.into_iter().map(|t| t.0).collect()),
                if_exists.0,
            ),
        )
        .map(PyTable::new)
        .map_err(error::into_pyerr)
    }

    pub fn transaction(
        &self,
        py: Python,
        author: Option<String>,
        message: Option<String>,
        extra_info: Option<String>,
    ) -> PyResult<PyTransaction> {
        let tx = if author.is_none() && message.is_none() && extra_info.is_none() {
            block_on(py, self.0.transaction())
        } else {
            let author_info = AuthorInfo {
                author,
                message,
                extra_info,
            };
            block_on(py, self.0.transaction_with_author(author_info))
        }
        .map_err(error::into_pyerr)?
        .into_owned();
        Ok(PyTransaction::new(tx))
    }

    pub fn table(&self, py: Python, name: Wrap<ducklake::TableName>) -> PyResult<PyTable> {
        block_on(py, self.0.table(name.0))
            .map(PyTable::new)
            .map_err(error::into_pyerr)
    }

    pub fn list_tables(&self, py: Python, schema: Option<String>) -> PyResult<Vec<PyTable>> {
        block_on(py, self.0.list_tables(schema.as_deref()))
            .map(|tables| tables.into_iter().map(PyTable::new).collect())
            .map_err(error::into_pyerr)
    }

    pub fn list_schemas(&self, py: Python) -> PyResult<Vec<String>> {
        block_on(py, self.0.list_schemas()).map_err(error::into_pyerr)
    }

    pub fn set_metadata(
        &self,
        py: Python,
        key: String,
        value: Option<String>,
        schema: Option<String>,
    ) -> PyResult<()> {
        if let Some(v) = value {
            block_on(py, self.0.set_metadata(&key, &v, schema.as_deref()))
                .map_err(error::into_pyerr)
        } else {
            block_on(py, self.0.unset_metadata(&key, schema.as_deref())).map_err(error::into_pyerr)
        }
    }

    pub fn disconnect(&mut self, py: Python) {
        block_on(py, self.0.disconnect());
    }

    pub fn expire_snapshots(
        &self,
        py: Python,
        versions: Option<Vec<i64>>,
        older_than: Option<chrono::DateTime<chrono::Utc>>,
        dry_run: bool,
    ) -> PyResult<Vec<Wrap<SnapshotMetadata>>> {
        let dry_run = if dry_run { DryRun::Yes } else { DryRun::No };
        let result = if let Some(versions) = versions {
            block_on(py, self.0.expire_snapshots_versions(&versions, dry_run))
        } else if let Some(timestamp) = older_than {
            block_on(py, self.0.expire_snapshots_older_than(timestamp, dry_run))
        } else {
            block_on(py, self.0.expire_snapshots(dry_run))
        };
        result
            .map(|snapshots| snapshots.into_iter().map(Wrap).collect())
            .map_err(error::into_pyerr)
    }

    pub fn delete_orphaned_files(
        &self,
        py: Python,
        cleanup_all: bool,
        older_than: Option<chrono::DateTime<chrono::Utc>>,
        dry_run: bool,
    ) -> PyResult<Vec<String>> {
        let dry_run = if dry_run { DryRun::Yes } else { DryRun::No };
        block_on(
            py,
            self.0
                .delete_orphaned_files(cleanup_all, older_than, dry_run),
        )
        .map_err(error::into_pyerr)
    }
}
