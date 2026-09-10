use std::collections::HashSet;

use itertools::Itertools;
use sea_query::{Asterisk, ExprTrait, Order, Query};

use crate::ducklake::SnapshotAccess;
use crate::spec::{DucklakeColumn, ducklake_column};
use crate::table::TableInfo;
use crate::{
    Ducklake,
    DucklakeError,
    DucklakeResult,
    IfExistsStrategy,
    Table,
    TableName,
    WriteDataFile,
    io,
    scan,
    utils,
};

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

/* -------------------------------------- TABLE TRANSFER -------------------------------------- */

struct TableTransferInfo {
    info: TableInfo,
    scan: scan::TableTransferScan,
    retired_columns: Vec<DucklakeColumn>,
}

impl Table {
    async fn transfer_info(&self) -> DucklakeResult<TableTransferInfo> {
        let snapshot = self.conn.snapshot(SnapshotAccess::Any).await?;
        let catalog = snapshot.catalog().await?;
        let table = catalog.table(self.id)?;
        let info = table.info();
        let data_path = table.data_path(&self.conn.metadata().data_path());

        // Find all retired column IDs. These IDs must stay reserved to not expose any IDs.
        let active_column_ids: HashSet<_> = info
            .schema
            .columns
            .values()
            .flat_map(crate::Column::flatten)
            .filter_map(|column| column.column.field_id)
            .collect();
        let query = Query::select()
            .column(Asterisk)
            .from(ducklake_column::Table)
            .and_where(ducklake_column::Column::TableId.col().eq(self.id))
            .order_by(ducklake_column::Column::BeginSnapshot, Order::Desc)
            .to_owned();
        let columns: Vec<DucklakeColumn> = self.conn.pool().fetch_all(&query).await?;
        let retired_columns = columns
            .into_iter()
            .filter(|column| !active_column_ids.contains(&column.column_id))
            .unique_by(|column| column.column_id)
            .collect();

        // Scan the table
        let scan = scan::scan_table_for_transfer(
            self.conn.pool(),
            self.id,
            snapshot,
            self.conn.snapshot_cache(),
            &data_path,
        )
        .await?;
        Ok(TableTransferInfo {
            info,
            scan,
            retired_columns,
        })
    }
}

impl Ducklake {
    /// Copy tables from this DuckLake into `target` in a single transaction.
    ///
    /// Each copied table owns newly-created copies of the corresponding source table's data files.
    /// All tables are created and populated within one transaction, so the entire batch is
    /// committed as a single snapshot (or fails without any partial changes to the catalog).
    /// File copies precede the commit, so a failed copy or commit may leave orphaned target files.
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
    /// DuckLake. Note that this means that time travel on the source DuckLake cannot recover moved
    /// files.
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
            .all(|transfer| transfer.table.conn.is_same_catalog(source_conn))
        {
            return Err(DucklakeError::InvalidTableTransfer(
                "all tables must belong to the source DuckLake".to_string(),
            ));
        }
        if target.conn.is_same_catalog(source_conn) {
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
        // (for `copy`) happen mid-transaction.
        let mut tx = target.conn.transaction(None).await?;
        for ((transfer, info), name) in tables.iter().zip(transfer_info).zip(&target_names) {
            let source = transfer.table;
            let TableTransferInfo {
                info,
                scan,
                retired_columns,
            } = info;

            tx.create_schema(&name.schema, None, IfExistsStrategy::Skip)?;
            let mut table = tx.create_transfer_table(name.clone(), info, retired_columns)?;

            let (_, generator) = table.get_write_info()?;
            let mut data_files = Vec::with_capacity(scan.result.data_files.len());
            for (data_file, metadata) in scan
                .result
                .data_files
                .into_iter()
                .zip(scan.partition_values)
            {
                let mut path = data_file.path;
                let mut delete_files = data_file.delete_files;
                if copy_files {
                    path = copy_transfer_file(
                        source.conn.storage_options(),
                        target.conn.storage_options(),
                        &generator,
                        &path,
                    )
                    .await?;
                    for delete_file in &mut delete_files {
                        delete_file.path = copy_transfer_file(
                            source.conn.storage_options(),
                            target.conn.storage_options(),
                            &generator,
                            &delete_file.path,
                        )
                        .await?;
                    }
                }
                data_files.push(crate::transaction::TransferDataFile {
                    data_file: WriteDataFile {
                        path,
                        statistics: Some(data_file.statistics),
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
            let mut tx = source_conn.transaction(None).await?;
            for name in &source_names {
                tx.delete_table_transferring_file_ownership(name)?;
            }
            tx.commit().await?;
        }

        futures::future::try_join_all(target_names.into_iter().map(|name| target.table(name)))
            .await
    }
}

/// Copy a file into the target's data directory and return its table-relative path.
async fn copy_transfer_file(
    source_options: &[(String, String)],
    target_options: &[(String, String)],
    generator: &utils::DataFilePathGenerator,
    source_path: &str,
) -> DucklakeResult<String> {
    let source = source_path.parse::<io::DucklakePath>()?;
    let relative_path = generator.generate_relative(&Default::default());
    let destination = generator
        .base_path()
        .parse::<io::DucklakePath>()?
        .join_str(&relative_path);
    io::copy_file(&source, source_options, &destination, target_options).await?;
    Ok(relative_path)
}
