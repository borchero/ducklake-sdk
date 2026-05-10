use arrow_array::RecordBatch;

use crate::ducklake::DucklakeConnection;
use crate::{DucklakeResult, IntoColumnName, TableMetadata, TableName, scan, utils};

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
        self.conn
            .current_snapshot()
            .catalog()
            .await?
            .try_table_name_by_id(self.id)
    }

    /// Get the schema of the table.
    pub async fn columns(&self) -> DucklakeResult<impl Iterator<Item = crate::Column>> {
        let columns = self
            .conn
            .current_snapshot()
            .catalog()
            .await?
            .try_table_schema_by_id(self.id)?
            .columns
            .into_values();
        Ok(columns)
    }

    /// Get the partitioning of the table.
    pub async fn partitioning(&self) -> DucklakeResult<Option<Vec<crate::PartitionColumn>>> {
        let columns = self
            .conn
            .current_snapshot()
            .catalog()
            .await?
            .try_table_partitioning_by_id(self.id)?
            .map(|p| p.0);
        Ok(columns)
    }

    /// Get the tags of the table.
    pub async fn tags(&self) -> DucklakeResult<Vec<crate::Tag>> {
        self.conn
            .current_snapshot()
            .catalog()
            .await?
            .try_table_tags_by_id(self.id)
    }

    /// Get the metadata set on this table.
    pub fn metadata(&self) -> TableMetadata {
        let meta = self.conn.metadata();
        meta.table_metadata(self.schema_id, self.id)
    }

    /// Get the Arrow schema of the table.
    pub async fn arrow_schema(&self) -> crate::DucklakeResult<arrow_schema::Schema> {
        let schema = self
            .conn
            .current_snapshot()
            .catalog()
            .await?
            .try_table_schema_by_id(self.id)?
            .to_arrow();
        Ok(schema)
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                          TRANSACTIONS                                         */
/* --------------------------------------------------------------------------------------------- */

impl Table {
    fn transaction_table<'tx, 'a>(
        &self,
        tx: &'tx mut crate::Transaction<'a>,
    ) -> DucklakeResult<crate::TransactionTable<'tx, 'a>> {
        let name = tx.catalog().try_table_name_by_id(self.id)?;
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
        let snapshot = self.conn.latest_snapshot(false).await?;
        let catalog = snapshot.catalog().await?;
        let meta = self.conn.metadata();
        let metadata = meta.table_metadata(self.schema_id, self.id);
        let data_path =
            catalog.try_table_data_path_by_id(self.schema_id, self.id, &meta.data_path())?;
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
    /// Add a new column to the table.
    fn add_column(column: crate::Column);
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
        let snapshot = self.conn.latest_snapshot(true).await?;
        let data_path = snapshot.catalog().await?.try_table_data_path_by_id(
            self.schema_id,
            self.id,
            &self.conn.metadata().data_path(),
        )?;
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
