use std::collections::HashMap;

use arrow_array::RecordBatch;

use crate::ducklake::{DucklakeConnection, SnapshotAccess};
use crate::{
    Ducklake,
    DucklakeError,
    DucklakeResult,
    IfExistsStrategy,
    IntoColumnName,
    TableMetadata,
    TableName,
    WriteDataFile,
    io,
    scan,
    utils,
};

/// Handle to a table in the DuckLake catalog.
#[derive(Clone)]
pub struct Table {
    conn: DucklakeConnection,
    schema_id: i64,
    id: i64,
}

/// A table to transfer, optionally with a new name in the target DuckLake.
pub struct TableTransfer<'a> {
    table: &'a Table,
    name: Option<TableName>,
}

impl<'a> From<&'a Table> for TableTransfer<'a> {
    fn from(table: &'a Table) -> Self {
        Self { table, name: None }
    }
}

impl<'a> From<(TableName, &'a Table)> for TableTransfer<'a> {
    fn from((name, table): (TableName, &'a Table)) -> Self {
        Self {
            table,
            name: Some(name),
        }
    }
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.conn == other.conn
    }
}

impl Eq for Table {}

#[derive(Clone)]
pub(crate) struct TableInfo {
    pub name: TableName,
    pub schema: crate::Schema,
    pub partitioning: Option<crate::Partition>,
    pub tags: Vec<crate::Tag>,
}

impl Table {
    pub(crate) fn new(conn: DucklakeConnection, schema_id: i64, id: i64) -> Self {
        Self {
            conn,
            schema_id,
            id,
        }
    }

    /// Get the name of the table.
    pub async fn name(&self) -> DucklakeResult<crate::TableName> {
        let snapshot = self.conn.current_snapshot();
        let catalog = snapshot.catalog().await?;
        let table = catalog.table(self.id)?;
        Ok(table.name().clone())
    }

    /// Get the schema of the table.
    pub async fn columns(&self) -> DucklakeResult<impl Iterator<Item = crate::Column>> {
        let snapshot = self.conn.current_snapshot();
        let catalog = snapshot.catalog().await?;
        let columns = catalog.table(self.id)?.schema().columns.into_values();
        Ok(columns)
    }

    /// Get the partitioning of the table.
    pub async fn partitioning(&self) -> DucklakeResult<Option<Vec<crate::PartitionColumn>>> {
        let columns = self
            .conn
            .current_snapshot()
            .catalog()
            .await?
            .table(self.id)?
            .partitioning()
            .map(|p| p.0);
        Ok(columns)
    }

    /// Get the tags of the table.
    pub async fn tags(&self) -> DucklakeResult<Vec<crate::Tag>> {
        let tags = self
            .conn
            .current_snapshot()
            .catalog()
            .await?
            .table(self.id)?
            .tags();
        Ok(tags)
    }

    /// Get the metadata set on this table.
    pub fn metadata(&self) -> TableMetadata {
        let meta = self.conn.metadata();
        meta.table_metadata(Some(self.schema_id), Some(self.id))
    }

    /// Get the Arrow schema of the table.
    pub async fn arrow_schema(&self) -> crate::DucklakeResult<arrow_schema::Schema> {
        let schema = self
            .conn
            .current_snapshot()
            .catalog()
            .await?
            .table(self.id)?
            .schema()
            .to_arrow();
        Ok(schema)
    }
}

/* -------------------------------------- TABLE TRANSFER -------------------------------------- */

impl Table {
    async fn transfer_info(&self) -> DucklakeResult<TableTransferInfo> {
        let snapshot = self.conn.snapshot(SnapshotAccess::Any).await?;
        let catalog = snapshot.catalog().await?;
        let table = catalog.table(self.id)?;
        let info = table.info();
        let data_path = table.data_path(&self.conn.metadata().data_path());
        let scan = scan::scan_table(
            self.conn.pool(),
            self.id,
            snapshot,
            self.conn.snapshot_cache(),
            &data_path,
            true,
        )
        .await?;
        Ok(TableTransferInfo { info, scan })
    }
}

impl Ducklake {
    /// Copy tables from this DuckLake into `target` in a single transaction.
    ///
    /// Each copied table owns newly-created copies of the corresponding source table's data files.
    /// All tables are created and populated within one transaction, so the entire batch is
    /// committed as a single snapshot (or fails without any partial changes to the catalog).
    ///
    /// Passing tables directly retains their names. Passing `(TableName, &Table)` pairs renames
    /// them in the target. All tables must belong to this DuckLake and no target table may already
    /// exist.
    pub async fn copy_tables<'a, I, T>(
        &self,
        tables: I,
        target: &Ducklake,
    ) -> DucklakeResult<Vec<Table>>
    where
        I: IntoIterator<Item = T>,
        T: Into<TableTransfer<'a>>,
    {
        let tables = tables.into_iter().map(Into::into).collect::<Vec<_>>();
        self.transfer_tables(&tables, target, true).await
    }

    /// Move tables from this DuckLake into `target` in a single transaction.
    ///
    /// The moved tables re-register the existing data files using absolute paths. Once the target
    /// commit succeeds, the source tables are dropped and their file metadata is detached in a
    /// single source-side transaction, so source maintenance cannot delete files now owned by this
    /// DuckLake.
    ///
    /// The target creation is atomic, but because the source and target are distinct catalogs the
    /// overall move is not: if the source changed since the transfer began, the source drop is
    /// rejected with [`DucklakeError::TableChangedDuringTransfer`] and the (already committed)
    /// target tables are retained.
    ///
    /// Passing tables directly retains their names. Passing `(TableName, &Table)` pairs renames
    /// them in the target. All tables must belong to this DuckLake and no target table may already
    /// exist.
    pub async fn move_tables<'a, I, T>(
        &self,
        tables: I,
        target: &Ducklake,
    ) -> DucklakeResult<Vec<Table>>
    where
        I: IntoIterator<Item = T>,
        T: Into<TableTransfer<'a>>,
    {
        let tables = tables.into_iter().map(Into::into).collect::<Vec<_>>();
        self.transfer_tables(&tables, target, false).await
    }

    async fn transfer_tables(
        &self,
        tables: &[TableTransfer<'_>],
        target: &Ducklake,
        copy_files: bool,
    ) -> DucklakeResult<Vec<Table>> {
        if tables.is_empty() {
            return Ok(Vec::new());
        }

        let source_conn = &self.conn;
        if !tables
            .iter()
            .all(|transfer| transfer.table.conn.is_same(source_conn))
        {
            return Err(DucklakeError::InvalidTableTransfer(
                "all tables must belong to the source DuckLake".to_string(),
            ));
        }
        if target.conn.is_same(source_conn) {
            return Err(DucklakeError::InvalidTableTransfer(
                "the source and target must be different DuckLake catalogs".to_string(),
            ));
        }
        if !copy_files {
            let mut seen = std::collections::HashSet::with_capacity(tables.len());
            if !tables.iter().all(|transfer| seen.insert(transfer.table.id)) {
                return Err(DucklakeError::InvalidTableTransfer(
                    "a table cannot occur more than once in a move".to_string(),
                ));
            }
        }

        // For a move, capture the source's head snapshot up front (also asserting the source is
        // writable) so we can detect concurrent writers before detaching files below.
        let source_snapshot_id = if copy_files {
            None
        } else {
            Some(source_conn.snapshot(SnapshotAccess::Write).await?.info().id)
        };

        // Scan all sources before making any target changes.
        let transfer_info = futures::future::try_join_all(
            tables.iter().map(|transfer| transfer.table.transfer_info()),
        )
        .await?;
        let source_names = transfer_info
            .iter()
            .map(|transfer| transfer.info.name.clone())
            .collect::<Vec<_>>();

        // Resolve the target names, defaulting to the source names.
        let target_names = tables
            .iter()
            .zip(&source_names)
            .map(|(transfer, source_name)| {
                transfer.name.clone().unwrap_or_else(|| source_name.clone())
            })
            .collect::<Vec<_>>();

        // Reject duplicate target names up front so a collision surfaces clearly rather than as a
        // confusing "already exists" error midway through the transaction.
        let mut seen = std::collections::HashSet::with_capacity(target_names.len());
        for name in &target_names {
            if !seen.insert(name) {
                return Err(DucklakeError::InvalidTableTransfer(format!(
                    "multiple tables target the same name '{name}'"
                )));
            }
        }

        // Create every table and write its data within a single target transaction. File copies
        // (for `copy`) happen mid-transaction; the transaction is buffered in memory until commit,
        // so no database lock is held while copying.
        let mut tx = target.conn.transaction(None).await?;
        for ((transfer, info), name) in tables.iter().zip(transfer_info).zip(&target_names) {
            let source = transfer.table;
            let TableTransferInfo { info, scan } = info;
            let source_columns = info.schema.columns.into_values().collect::<Vec<_>>();

            tx.create_schema(&name.schema, None, IfExistsStrategy::Skip)?;
            let mut table = tx.create_table(
                name.clone(),
                source_columns.clone(),
                info.partitioning.map(|partition| partition.0),
                None,
                Some(info.tags),
                IfExistsStrategy::Fail,
            )?;

            // Field IDs are assigned deterministically when the table is created, so the freshly
            // created target columns line up positionally with the source columns.
            let target_columns = table.columns()?.collect::<Vec<_>>();
            let column_ids = transfer_column_ids(&source_columns, &target_columns);

            let (_, generator) = table.get_write_info()?;
            let mut data_files = Vec::with_capacity(scan.result.data_files.len());
            for (data_file, metadata) in scan
                .result
                .data_files
                .into_iter()
                .zip(scan.partition_values)
            {
                let path = copy_transfer_file(
                    source.conn.storage_options(),
                    target.conn.storage_options(),
                    &generator,
                    &data_file.path,
                    copy_files,
                )
                .await?;
                let mut delete_files = data_file.delete_files;
                for delete_file in &mut delete_files {
                    let path = copy_transfer_file(
                        source.conn.storage_options(),
                        target.conn.storage_options(),
                        &generator,
                        &delete_file.path,
                        copy_files,
                    )
                    .await?;
                    delete_file.path = path;
                }
                data_files.push(crate::transaction::TransferDataFile {
                    data_file: WriteDataFile {
                        path,
                        statistics: Some(remap_statistics(data_file.statistics, &column_ids)),
                        partition_values: None,
                    },
                    partition_values: metadata,
                    delete_files,
                    inline_deletes: data_file
                        .inline_deletes
                        .map(|row_ids| row_ids.values().to_vec())
                        .unwrap_or_default(),
                });
            }
            if !data_files.is_empty() {
                table.write_transfer_data_files(data_files).await?;
            }
            if !scan.result.inline_data.is_empty() {
                table.write_inline_data(scan.result.inline_data)?;
            }
        }
        tx.commit().await?;

        // For a move, drop the source tables now that the target commit succeeded. A single
        // snapshot guard suffices because all sources share one catalog: reject the drop if any
        // concurrent writer advanced the source snapshot, since their files were not transferred.
        if let Some(snapshot_id) = source_snapshot_id {
            if source_conn.snapshot(SnapshotAccess::Write).await?.info().id != snapshot_id {
                return Err(DucklakeError::TableChangedDuringTransfer);
            }
            let mut source_tx = source_conn.transaction(None).await?;
            for name in &source_names {
                source_tx.delete_table_transferring_file_ownership(name)?;
            }
            source_tx.commit().await?;
        }

        futures::future::try_join_all(target_names.into_iter().map(|name| target.table(name)))
            .await
    }
}

/// Resolve the path a transferred file should be registered under in the target. When copying, the
/// file is physically copied into the target's data directory and the new absolute path is
/// returned. When moving, the source path is registered as-is (as an absolute path).
async fn copy_transfer_file(
    source_options: &[(String, String)],
    target_options: &[(String, String)],
    generator: &utils::DataFilePathGenerator,
    source_path: &str,
    copy_files: bool,
) -> DucklakeResult<String> {
    if !copy_files {
        return Ok(source_path.to_string());
    }
    let source = source_path.parse::<io::DucklakePath>()?;
    let destination = generator.generate_absolute(&Default::default());
    io::copy_file(
        &source,
        source_options,
        &destination.parse::<io::DucklakePath>()?,
        target_options,
    )
    .await?;
    Ok(destination)
}

struct TableTransferInfo {
    info: TableInfo,
    scan: scan::TableScan,
}

fn transfer_column_ids(source: &[crate::Column], target: &[crate::Column]) -> HashMap<i64, i64> {
    let mut source_ids = Vec::new();
    let mut target_ids = Vec::new();
    for column in source {
        source_ids.extend(
            column
                .flatten()
                .into_iter()
                .map(|column| column.column.field_id),
        );
    }
    for column in target {
        target_ids.extend(
            column
                .flatten()
                .into_iter()
                .map(|column| column.column.field_id),
        );
    }
    source_ids
        .into_iter()
        .zip(target_ids)
        .filter_map(|(source, target)| source.zip(target))
        .collect()
}

fn remap_statistics(
    mut statistics: crate::DataFileStatistics,
    column_ids: &HashMap<i64, i64>,
) -> crate::DataFileStatistics {
    statistics.column_stats = statistics
        .column_stats
        .into_iter()
        .filter_map(|(source_id, stats)| column_ids.get(&source_id).map(|id| (*id, stats)))
        .collect();
    statistics
}

/* --------------------------------------------------------------------------------------------- */
/*                                          TRANSACTIONS                                         */
/* --------------------------------------------------------------------------------------------- */

impl Table {
    fn transaction_table<'tx, 'a>(
        &self,
        tx: &'tx mut crate::Transaction<'a>,
    ) -> DucklakeResult<crate::TransactionTable<'tx, 'a>> {
        let name = tx.catalog().table(self.id)?.name().clone();
        tx.table(name)
    }
}

/* ------------------------------------------- WRITES ------------------------------------------ */

impl Table {
    /// Write data files to the table by invoking the provided closure with the table metadata
    /// and a path generator. The data files returned by the closure are committed to the table.
    pub async fn write_data<F>(
        &self,
        write_fn: impl FnOnce(TableMetadata, utils::DataFilePathGenerator) -> F,
    ) -> DucklakeResult<()>
    where
        F: Future<Output = DucklakeResult<Vec<crate::WriteDataFile>>>,
    {
        let mut tx = self.conn.transaction(None).await?;
        let mut table = self.transaction_table(&mut tx)?;
        table.write_data(write_fn).await?;
        tx.commit().await
    }

    /// Get the table metadata and a path generator that can be used to write new data files.
    pub async fn get_write_info(
        &self,
    ) -> DucklakeResult<(TableMetadata, utils::DataFilePathGenerator)> {
        let snapshot = self.conn.snapshot(SnapshotAccess::Write).await?;
        let catalog = snapshot.catalog().await?;
        let meta = self.conn.metadata();
        let metadata = meta.table_metadata(Some(self.schema_id), Some(self.id));
        let data_path = catalog.table(self.id)?.data_path(&meta.data_path());
        let generator = utils::DataFilePathGenerator::new(data_path, metadata.hive_file_pattern);
        Ok((metadata, generator))
    }

    /// Commit the provided pre-written data files to the table.
    pub async fn write_data_files(
        &self,
        data_files: Vec<crate::WriteDataFile>,
    ) -> DucklakeResult<()> {
        let mut tx = self.conn.transaction(None).await?;
        let mut table = self.transaction_table(&mut tx)?;
        table.write_data_files(data_files).await?;
        tx.commit().await
    }

    /// Write the provided record batches as inline data into the catalog.
    pub async fn write_inline_data(&self, data: Vec<RecordBatch>) -> DucklakeResult<()> {
        let mut tx = self.conn.transaction(None).await?;
        let mut table = self.transaction_table(&mut tx)?;
        table.write_inline_data(data)?;
        tx.commit().await
    }
}

/* --------------------------------------- SCHEMA CHANGES -------------------------------------- */

macro_rules! within_transaction {
    ($(
        $(#[$meta:meta])*
        fn $name:ident($($arg:ident: $ty:ty),*);
    )*) => {
        impl Table {
            $(
            $(#[$meta])*
            pub async fn $name(&self, $($arg: $ty),*) -> DucklakeResult<()> {
                let mut tx = self.conn.transaction(None).await?;
                let mut table = self.transaction_table(&mut tx)?;
                let result = table.$name($($arg),*)?;
                tx.commit().await?;
                Ok(result)
            }
            )*
        }
    };
}

macro_rules! within_transaction_async {
    ($(
        $(#[$meta:meta])*
        fn $name:ident($($arg:ident: $ty:ty),*);
    )*) => {
        impl Table {
            $(
            $(#[$meta])*
            pub async fn $name(&self, $($arg: $ty),*) -> DucklakeResult<()> {
                let mut tx = self.conn.transaction(None).await?;
                let mut table = self.transaction_table(&mut tx)?;
                let result = table.$name($($arg),*).await?;
                tx.commit().await?;
                Ok(result)
            }
            )*
        }
    };
}

within_transaction! {
    /// Rename the table.
    fn rename(new_name: &str);
    /// Update the table's partitioning.
    fn update_partitioning(columns: Option<Vec<crate::PartitionColumn>>);
    /// Rename a column in the table.
    fn rename_column(column: impl IntoColumnName, new_name: &str);
    /// Remove a column from the table.
    fn remove_column(column: impl IntoColumnName);
    /// Update the default value of a column in the table.
    fn update_column_default(column: impl IntoColumnName, default_value: crate::ColumnDefault);
    /// Add a new tag for the table.
    fn add_tag(key: &str, value: &str);
    /// Remove a tag from the table.
    fn remove_tag(key: &str);
    /// Add a new tag to a column of the table.
    fn add_column_tag(column_path: impl IntoColumnName, key: &str, value: &str);
    /// Remove a tag from a column of the table.
    fn remove_column_tag(column_path: impl IntoColumnName, key: &str);
}

within_transaction_async! {
    /// Add a new column to the table.
    fn add_column(column: crate::Column);
    /// Update the dtype of a column in the table.
    fn update_column_dtype(column: impl IntoColumnName, new_dtype: crate::DataType);
    /// Update the nullability of a column in the table.
    fn update_column_nullability(column: impl IntoColumnName, nullable: bool);
    /// Update the full schema of the table.
    fn update_schema(columns: Vec<crate::Column>);
}

impl Table {
    /// Delete the table.
    ///
    /// Once this method returns successfully, this object should no longer be used.
    pub async fn delete(&self) -> DucklakeResult<()> {
        let mut tx = self.conn.transaction(None).await?;
        let table = self.transaction_table(&mut tx)?;
        table.delete()?;
        tx.commit().await
    }

    /// Set a metadata option for this table.
    pub async fn set_metadata(&self, key: &str, value: &str) -> DucklakeResult<()> {
        self.conn.set_table_metadata(key, value, self.id).await
    }

    /// Unset a metadata option for this table.
    pub async fn unset_metadata(&self, key: &str) -> DucklakeResult<()> {
        self.conn.unset_table_metadata(key, self.id).await
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                              READ                                             */
/* --------------------------------------------------------------------------------------------- */

impl Table {
    /// Get all data and delete files for the table in the latest snapshot.
    ///
    /// Currently, this fails if any data is inlined.
    pub async fn scan(&self) -> DucklakeResult<crate::ScanResult> {
        let snapshot = self.conn.snapshot(SnapshotAccess::Any).await?;
        let data_path = snapshot
            .catalog()
            .await?
            .table(self.id)?
            .data_path(&self.conn.metadata().data_path());
        scan::scan_table(
            self.conn.pool(),
            self.id,
            snapshot,
            self.conn.snapshot_cache(),
            &data_path,
            false,
        )
        .await
        .map(|scan| scan.result)
    }
}
