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
    /// Copy this table into another DuckLake.
    ///
    /// The copied table owns newly-created copies of this table's data files. If `name` is not
    /// provided, the source table's name is used.
    ///
    /// This is a convenience wrapper around [`Ducklake::copy_tables_from`] for a single table.
    pub async fn copy_to(
        &self,
        target: &Ducklake,
        name: Option<TableName>,
    ) -> DucklakeResult<Table> {
        let names = name.map(|name| vec![name]);
        Ok(target
            .copy_tables_from(&[self], names)
            .await?
            .pop()
            .expect("transfer of a single table must yield exactly one table"))
    }

    /// Move this table into another DuckLake.
    ///
    /// Moving re-registers the existing data files in the target using absolute paths, then
    /// atomically removes their metadata from the source catalog while dropping the source table.
    /// Consequently, maintenance on the source cannot delete files now owned by the target. If
    /// `name` is not provided, the source table's name is used.
    ///
    /// This is a convenience wrapper around [`Ducklake::move_tables_from`] for a single table.
    pub async fn move_to(
        &self,
        target: &Ducklake,
        name: Option<TableName>,
    ) -> DucklakeResult<Table> {
        let names = name.map(|name| vec![name]);
        Ok(target
            .move_tables_from(&[self], names)
            .await?
            .pop()
            .expect("transfer of a single table must yield exactly one table"))
    }

    async fn transfer_info(&self) -> DucklakeResult<TableTransferInfo> {
        let snapshot = self.conn.snapshot(SnapshotAccess::Any).await?;
        let catalog = snapshot.catalog().await?;
        let table = catalog.table(self.id)?;
        let info = table.info();
        let data_path = table.data_path(&self.conn.metadata().data_path());
        let scan = scan::scan_table(
            self.conn.pool(),
            self.id,
            snapshot.clone(),
            self.conn.snapshot_cache(),
            &data_path,
        )
        .await?;
        if scan
            .data_files
            .iter()
            .any(|file| file.inline_deletes.is_some())
        {
            return Err(DucklakeError::TableTransferWithInlineDeletes);
        }
        Ok(TableTransferInfo { info, scan })
    }
}

impl Ducklake {
    /// Copy the provided tables into this DuckLake in a single transaction.
    ///
    /// Each copied table owns newly-created copies of the corresponding source table's data files.
    /// All tables are created and populated within one transaction, so the entire batch is
    /// committed as a single snapshot (or fails without any partial changes to the catalog).
    ///
    /// If `names` is provided, it must contain exactly one target name per source table; otherwise,
    /// each source table's name is retained. All sources must originate from the same DuckLake and
    /// no target table may already exist.
    pub async fn copy_tables_from(
        &self,
        sources: &[&Table],
        names: Option<Vec<TableName>>,
    ) -> DucklakeResult<Vec<Table>> {
        self.transfer_tables(sources, names, true).await
    }

    /// Move the provided tables into this DuckLake in a single transaction.
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
    /// If `names` is provided, it must contain exactly one target name per source table; otherwise,
    /// each source table's name is retained. All sources must originate from the same DuckLake and
    /// no target table may already exist.
    pub async fn move_tables_from(
        &self,
        sources: &[&Table],
        names: Option<Vec<TableName>>,
    ) -> DucklakeResult<Vec<Table>> {
        self.transfer_tables(sources, names, false).await
    }

    async fn transfer_tables(
        &self,
        sources: &[&Table],
        names: Option<Vec<TableName>>,
        copy_files: bool,
    ) -> DucklakeResult<Vec<Table>> {
        if sources.is_empty() {
            return Ok(Vec::new());
        }

        // All sources must share a single catalog so that a move can guard the source snapshot and
        // drop the source tables in one transaction.
        let source_conn = &sources[0].conn;
        if !sources.iter().all(|table| table.conn.is_same(source_conn)) {
            return Err(DucklakeError::MixedTransferSources);
        }

        // For a move, capture the source's head snapshot up front (also asserting the source is
        // writable) so we can detect concurrent writers before detaching files below.
        let source_snapshot_id = if copy_files {
            None
        } else {
            Some(source_conn.snapshot(SnapshotAccess::Write).await?.info().id)
        };

        // Scan all sources before making any target changes.
        let mut transfers = Vec::with_capacity(sources.len());
        for source in sources {
            transfers.push(source.transfer_info().await?);
        }
        let source_names = transfers
            .iter()
            .map(|transfer| transfer.info.name.clone())
            .collect::<Vec<_>>();

        // Resolve the target names, defaulting to the source names.
        let target_names = match names {
            Some(names) if names.len() != sources.len() => {
                return Err(DucklakeError::TransferNameCountMismatch {
                    expected: sources.len(),
                    actual: names.len(),
                });
            }
            Some(names) => names,
            None => source_names.clone(),
        };

        // Reject duplicate target names up front so a collision surfaces clearly rather than as a
        // confusing "already exists" error midway through the transaction.
        let mut seen = std::collections::HashSet::with_capacity(target_names.len());
        for name in &target_names {
            if !seen.insert(name) {
                return Err(DucklakeError::DuplicateTransferTarget {
                    name: name.to_string(),
                });
            }
        }

        // Create every table and write its data within a single target transaction. File copies
        // (for `copy`) happen mid-transaction; the transaction is buffered in memory until commit,
        // so no database lock is held while copying.
        let mut tx = self.conn.transaction(None).await?;
        for (source, (transfer, name)) in
            sources.iter().zip(transfers.into_iter().zip(&target_names))
        {
            let source_columns = transfer
                .info
                .schema
                .columns
                .values()
                .cloned()
                .collect::<Vec<_>>();

            tx.create_schema(&name.schema, None, IfExistsStrategy::Skip)?;
            let mut table = tx.create_table(
                name.clone(),
                source_columns.clone(),
                transfer
                    .info
                    .partitioning
                    .clone()
                    .map(|partition| partition.0),
                None,
                Some(transfer.info.tags.clone()),
                IfExistsStrategy::Fail,
            )?;

            // Field IDs are assigned deterministically when the table is created, so the freshly
            // created target columns line up positionally with the source columns.
            let target_columns = table.columns()?.collect::<Vec<_>>();
            let column_ids = transfer_column_ids(&source_columns, &target_columns);

            let (_, generator) = table.get_write_info()?;
            let mut data_files = Vec::with_capacity(transfer.scan.data_files.len());
            for data_file in transfer.scan.data_files {
                let path = copy_transfer_file(
                    source.conn.storage_options(),
                    self.conn.storage_options(),
                    &generator,
                    data_file.path,
                    copy_files,
                )
                .await?;
                let mut delete_files = Vec::with_capacity(data_file.delete_files.len());
                for delete_file in data_file.delete_files {
                    let path = copy_transfer_file(
                        source.conn.storage_options(),
                        self.conn.storage_options(),
                        &generator,
                        delete_file.path,
                        copy_files,
                    )
                    .await?;
                    delete_files.push(crate::transaction::TransferDeleteFile {
                        path,
                        num_deletes: delete_file.num_deletes,
                        file_size_bytes: delete_file.file_size_bytes,
                        footer_size_bytes: delete_file.footer_size_bytes,
                    });
                }
                data_files.push(crate::transaction::TransferDataFile {
                    data_file: WriteDataFile {
                        path,
                        statistics: Some(remap_statistics(data_file.statistics, &column_ids)),
                        partition_values: None,
                    },
                    delete_files,
                });
            }
            if !data_files.is_empty() {
                table.write_transfer_data_files(data_files).await?;
            }
            if !transfer.scan.inline_data.is_empty() {
                table.write_inline_data(transfer.scan.inline_data)?;
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

        // Fetch handles to the newly created target tables.
        let mut result = Vec::with_capacity(target_names.len());
        for name in target_names {
            result.push(self.table(name).await?);
        }
        Ok(result)
    }
}

/// Resolve the path a transferred file should be registered under in the target. When copying, the
/// file is physically copied into the target's data directory and the new absolute path is
/// returned. When moving, the source path is registered as-is (as an absolute path).
async fn copy_transfer_file(
    source_options: &[(String, String)],
    target_options: &[(String, String)],
    generator: &utils::DataFilePathGenerator,
    source_path: String,
    copy_files: bool,
) -> DucklakeResult<String> {
    if !copy_files {
        return Ok(source_path);
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
    scan: crate::ScanResult,
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
        )
        .await
    }
}
