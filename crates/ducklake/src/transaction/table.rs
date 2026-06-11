use std::sync::Arc;

use arrow_array::RecordBatch;
use ducklake_macros::visibility_if;
use itertools::EitherOrBoth;

use super::changes::Change;
use super::{CommitDataFile, CommitInlineData, IfExistsStrategy, Transaction};
use crate::{
    Column,
    ColumnName,
    DucklakeError,
    DucklakeResult,
    IntoColumnName,
    PartitionColumn,
    Schema,
    TableMetadata,
    TableName,
    Tag,
    io,
    primitives,
    utils,
};

// NOTE: The general idea is that table-level functionality is exposed on the transaction itself
//  as well as on the table object. Users of the Rust crate would typically access table-level
//  functionality via the table object. However, it is much easier to write language bindings for
//  languages without ownership semantics (e.g., Python) if table-level functionality is exposed
//  on the transaction itself.

/// Handle to a table within an active transaction.
pub struct TransactionTable<'tx, 'a> {
    tx: &'tx mut Transaction<'a>,
    name: TableName,
}

impl<'tx, 'a> TransactionTable<'tx, 'a> {
    fn new(tx: &'tx mut Transaction<'a>, name: TableName) -> Self {
        Self { tx, name }
    }
}

impl<'a> Transaction<'a> {
    /// Get a handle to the table with the provided name within this transaction.
    pub fn table(
        &mut self,
        name: impl TryInto<TableName, Error = impl Into<DucklakeError>>,
    ) -> DucklakeResult<TransactionTable<'_, 'a>> {
        let name = name.try_into().map_err(|e| e.into())?;
        // NOTE: We could create the TransactionTable directly here without querying the catalog
        //  first. However, we want to ensure that the table exists at this point.
        self.catalog().table(&name)?;
        Ok(TransactionTable::new(self, name.clone()))
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                           ATTRIBUTES                                          */
/* --------------------------------------------------------------------------------------------- */

impl<'tx, 'a> TransactionTable<'tx, 'a> {
    /// Get the schema of the table within the current transaction.
    pub fn columns(&self) -> DucklakeResult<impl Iterator<Item = crate::Column>> {
        let columns = self
            .tx
            .catalog()
            .table(&self.name)?
            .schema()
            .columns
            .into_values();
        Ok(columns)
    }

    /// Get the partitioning of the table within the current transaction.
    pub fn partitioning(&self) -> DucklakeResult<Option<Vec<crate::PartitionColumn>>> {
        let columns = self
            .tx
            .catalog()
            .table(&self.name)?
            .partitioning()
            .map(|p| p.0);
        Ok(columns)
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                           LIFECYCLE                                           */
/* --------------------------------------------------------------------------------------------- */

/* ------------------------------------------- CREATE ------------------------------------------ */

impl<'a> Transaction<'a> {
    /// Create a new table in the catalog.
    pub fn create_table(
        &mut self,
        name: impl TryInto<TableName, Error = impl Into<DucklakeError>>,
        columns: Vec<Column>,
        partition_columns: Option<Vec<PartitionColumn>>,
        path: Option<String>,
        tags: Option<Vec<Tag>>,
        if_exists: IfExistsStrategy,
    ) -> DucklakeResult<TransactionTable<'_, 'a>> {
        let name = name.try_into().map_err(|e| e.into())?;

        // If the table already exists and the strategy is specified accordingly, simply
        // return the existing table
        if matches!(if_exists, IfExistsStrategy::Skip) && self.catalog().table(&name).is_ok() {
            return Ok(TransactionTable::new(self, name));
        }

        // Prepare the path
        let path: io::DucklakePath = path.unwrap_or_else(|| name.name.clone()).parse()?;
        let path = path.ensure_directory();

        // Insert the table into the catalog along with all its metadata
        let info = crate::TableInfo {
            name: name.clone(),
            schema: columns.clone().try_into()?,
            partitioning: partition_columns.clone().map(|p| p.into()),
            tags: tags.clone().unwrap_or_default(),
        };
        let (schema_ref, table_ref, column_refs, partition_refs) =
            self.catalog_mut().add_table(info, path.clone())?;

        // Create the change object
        let change = Change::CreateTable {
            schema_ref,
            table_ref,
            column_refs,
            partition_column_refs: partition_refs,
            name: name.clone(),
            columns,
            partition_columns,
            path,
            tags,
        };
        self.changes.push(change);
        Ok(TransactionTable::new(self, name.clone()))
    }
}

/* ------------------------------------------- DELETE ------------------------------------------ */

impl<'tx, 'a> TransactionTable<'tx, 'a> {
    /// Delete the table.
    pub fn delete(self) -> DucklakeResult<()> {
        self.tx.delete_table(&self.name)
    }
}

impl<'a> Transaction<'a> {
    #[visibility_if(feature = "python", pub)]
    fn delete_table(&mut self, name: &TableName) -> DucklakeResult<()> {
        let mut table = self.catalog_mut().table_mut(name)?;
        table.delete();
        let change = Change::DeleteTable {
            table_ref: table.ref_(),
        };
        self.changes.push(change);
        Ok(())
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                        DATA OPERATIONS                                        */
/* --------------------------------------------------------------------------------------------- */

/* ------------------------------------------- WRITE ------------------------------------------- */

impl<'tx, 'a> TransactionTable<'tx, 'a> {
    /// Write data files to the table by invoking the provided closure with the table metadata
    /// and a path generator. The data files returned by the closure are committed to the table.
    pub async fn write_data<F>(
        &mut self,
        write_fn: impl FnOnce(TableMetadata, utils::DataFilePathGenerator) -> F,
    ) -> DucklakeResult<()>
    where
        F: Future<Output = DucklakeResult<Vec<crate::WriteDataFile>>>,
    {
        self.tx.write_table_data(&self.name, write_fn).await
    }

    /// Get the table metadata and a path generator that can be used to write new data files.
    pub fn get_write_info(&self) -> DucklakeResult<(TableMetadata, utils::DataFilePathGenerator)> {
        self.tx.get_table_write_info(&self.name)
    }

    /// Commit the provided pre-written data files to the table.
    pub async fn write_data_files(
        &mut self,
        data_files: Vec<crate::WriteDataFile>,
    ) -> DucklakeResult<()> {
        self.tx.write_table_data_files(&self.name, data_files).await
    }

    /// Write the provided record batches as inline data into the catalog.
    pub fn write_inline_data(&mut self, data: Vec<RecordBatch>) -> DucklakeResult<()> {
        self.tx.write_table_inline_data(&self.name, data)
    }
}

impl<'a> Transaction<'a> {
    async fn write_table_data<F>(
        &mut self,
        table_name: &TableName,
        write_fn: impl FnOnce(TableMetadata, utils::DataFilePathGenerator) -> F,
    ) -> DucklakeResult<()>
    where
        F: Future<Output = DucklakeResult<Vec<crate::WriteDataFile>>>,
    {
        let (metadata, generator) = self.get_table_write_info(table_name)?;
        let data_files = write_fn(metadata, generator).await?;
        self.write_table_data_files(table_name, data_files).await
    }

    #[visibility_if(feature = "python", pub)]
    fn get_table_write_info(
        &self,
        table_name: &TableName,
    ) -> DucklakeResult<(TableMetadata, utils::DataFilePathGenerator)> {
        // Derive data path
        let table = self.catalog().table(table_name)?;
        let data_path = table.data_path(&self.metadata.data_path());

        // Derive metadata
        // NOTE: We must not unwrap the IDs here as, otherwise, we run into issues with pending
        //  tables that were created in the current transaction.
        let metadata = self
            .metadata
            .table_metadata(table.parent_schema().id(), table.id());

        // Construct result
        let generator = utils::DataFilePathGenerator::new(data_path, metadata.hive_file_pattern);
        Ok((metadata, generator))
    }

    #[visibility_if(feature = "python", pub)]
    async fn write_table_data_files(
        &mut self,
        table_name: &TableName,
        data_files: Vec<crate::WriteDataFile>,
    ) -> DucklakeResult<()> {
        let table = self.catalog().table(table_name)?;
        let base_path = table.data_path(&self.metadata.data_path());
        let table_info = table.info();
        let schema_columns = table_info.schema.columns_by_id();

        // Ensure that statistics are available for all data files
        let mut data_files = data_files;
        let paths = data_files
            .iter()
            .map(|data_file| data_file.path.parse::<io::DucklakePath>())
            .collect::<Result<Vec<_>, _>>()?;
        let statistics =
            futures::future::try_join_all(data_files.iter_mut().zip(paths.iter()).map(
                |(data_file, path)| {
                    let path = base_path.join(path);
                    let storage_options = self.storage_options.clone();
                    async move {
                        if let Some(stats) = data_file.statistics.take() {
                            Ok(stats)
                        } else {
                            io::parquet::read_file_statistics(
                                path.resolve()?,
                                Some(storage_options),
                            )
                            .await
                        }
                    }
                },
            ))
            .await?;

        // Create the commit data files
        let commit_data_files = data_files
            .into_iter()
            .zip(paths)
            .zip(statistics)
            .map(|((data_file, path), stats)| {
                let commit_data_file = CommitDataFile {
                    path,
                    num_rows: stats.num_rows,
                    file_size_bytes: stats.file_size_bytes,
                    footer_size_bytes: stats.footer_size_bytes,
                    partition_values: match (
                        table_info.partitioning.as_ref(),
                        data_file.partition_values,
                    ) {
                        // If partitioning is defined, and the user-provided data file contains
                        // partition values, we ensure that they match. Otherwise, we simply ignore
                        // the partition values provided by the user.
                        (Some(target), Some(p)) => target
                            .0
                            .iter()
                            .map(|col| p.get(&col.column).cloned().ok_or(()))
                            .collect::<Result<Vec<_>, _>>()
                            .ok(),
                        // - If the table is partitioned but no partitions are provided, this is
                        //   fine. We simply don't add partition values.
                        // - If the table is not partitioned, we simply ignore the partition
                        //   values. Users are free to partition data files regardless.
                        (Some(_), None) | (None, _) => None,
                    },
                    column_stats: stats
                        .column_stats
                        .into_iter()
                        .map(|(column_id, stats)| {
                            if let Some(col) = schema_columns.get(&column_id)
                                && !col.nullable
                                && stats.null_count.unwrap_or(0) > 0
                            {
                                return Err(DucklakeError::InvalidNullValue {
                                    column: col.name.to_string(),
                                });
                            }
                            let Ok(table) = self.catalog().table(table.ref_());
                            let col_ref = table.column(column_id)?.ref_();
                            Ok((col_ref, stats))
                        })
                        .collect::<DucklakeResult<_>>()?,
                };
                Ok(commit_data_file)
            })
            .collect::<DucklakeResult<Vec<_>>>()?;
        let change = Change::WriteTableDataFiles {
            table_ref: table.ref_(),
            data_files: commit_data_files,
        };
        self.changes.push(change);
        Ok(())
    }

    #[visibility_if(feature = "python", pub)]
    fn write_table_inline_data(
        &mut self,
        table_name: &TableName,
        data: Vec<RecordBatch>,
    ) -> DucklakeResult<()> {
        let table = self.catalog_mut().table(table_name)?;
        let table_ref = table.ref_();
        let schema = table.schema();
        let schema_columns = schema.columns_by_id();

        let change = Change::WriteTableInlineData {
            table_ref,
            data: data
                .into_iter()
                .map(|batch| {
                    let statistics = io::arrow::compute_record_batch_statistics(&schema, &batch);
                    let data = CommitInlineData {
                        record_batch: batch.clone(),
                        column_stats: statistics
                            .column_stats
                            .into_iter()
                            .map(|(column_id, stats)| {
                                if let Some(col) = schema_columns.get(&column_id)
                                    && !col.nullable
                                    && stats.null_count.unwrap_or(0) > 0
                                {
                                    return Err(DucklakeError::InvalidNullValue {
                                        column: col.name.to_string(),
                                    });
                                }
                                let Ok(table) = self.catalog().table(table_ref);
                                let col_ref = table.column(column_id)?.ref_();
                                Ok((col_ref, stats))
                            })
                            .collect::<DucklakeResult<_>>()?,
                    };
                    Ok(data)
                })
                .collect::<DucklakeResult<Vec<_>>>()?,
        };
        self.changes.push(change);
        Ok(())
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                      METADATA OPERATIONS                                      */
/* --------------------------------------------------------------------------------------------- */

/* ---------------------------------------- RENAME TABLE --------------------------------------- */

impl<'tx, 'a> TransactionTable<'tx, 'a> {
    /// Rename the table.
    ///
    /// Also see [`crate::Table::rename`].
    pub fn rename(&mut self, new_name: &str) -> DucklakeResult<()> {
        self.tx.rename_table(&self.name, new_name)
    }
}

impl<'a> Transaction<'a> {
    #[visibility_if(feature = "python", pub)]
    fn rename_table(&mut self, old_name: &TableName, new_name: &str) -> DucklakeResult<()> {
        if old_name.name == new_name {
            return Ok(());
        }

        let mut table = self.catalog_mut().table_mut(old_name)?;
        table.rename(new_name)?;
        let change = Change::RenameTable {
            table_ref: table.ref_(),
            name: TableName {
                schema: old_name.schema.clone(),
                name: new_name.to_string(),
            },
        };
        self.changes.push(change);
        Ok(())
    }
}

/* --------------------------------------- UPDATE SCHEMA --------------------------------------- */

impl<'tx, 'a> TransactionTable<'tx, 'a> {
    /// Update the full schema of the table.
    pub async fn update_schema(&mut self, columns: Vec<crate::Column>) -> DucklakeResult<()> {
        self.tx.update_table_schema(&self.name, columns).await
    }
}

impl<'a> Transaction<'a> {
    #[visibility_if(feature = "python", pub)]
    async fn update_table_schema(
        &mut self,
        name: &TableName,
        new_columns: Vec<Column>,
    ) -> DucklakeResult<()> {
        let schema = self.catalog().table(name)?.schema();
        let guard = self.guard();
        // Iterate over the schemas and either update data types, add, or remove columns
        let old_columns = schema.columns;
        let new_columns = Schema::try_from(new_columns)?.columns;
        for item in primitives::iter_index_map_diff(&old_columns, &new_columns) {
            match item {
                EitherOrBoth::Both(_, (col_name, col)) => {
                    let col_name = ColumnName::named(col_name);
                    guard
                        .tx
                        .update_table_column_dtype(name, &col_name, col.dtype.clone())
                        .await?;
                    guard.tx.update_table_column_default(
                        name,
                        &col_name,
                        col.default_value.clone(),
                    )?;
                    guard
                        .tx
                        .update_table_column_nullability(name, &col_name, col.nullable)
                        .await?;
                    // NOTE: We intentionally do not touch tags here. Maybe something to change
                    //  in the future.
                }
                EitherOrBoth::Left((col_name, _)) => {
                    guard
                        .tx
                        .remove_table_column(name, &ColumnName::named(col_name))?;
                }
                EitherOrBoth::Right((_, col)) => {
                    guard
                        .tx
                        .add_table_column(name, col.clone(), &Default::default())
                        .await?;
                }
            }
        }
        guard.commit();
        Ok(())
    }
}

/* ---------------------------------- ADD/REMOVE TABLE COLUMNS --------------------------------- */

impl<'tx, 'a> TransactionTable<'tx, 'a> {
    /// Add a new column to the table.
    pub async fn add_column(&mut self, column: crate::Column) -> DucklakeResult<()> {
        self.tx
            .add_table_column(&self.name, column, &Default::default())
            .await
    }

    /// Remove a column from the table.
    pub fn remove_column(&mut self, column: impl IntoColumnName) -> DucklakeResult<()> {
        self.tx
            .remove_table_column(&self.name, &column.try_into().map_err(|e| e.into())?)
    }
}

impl<'a> Transaction<'a> {
    #[visibility_if(feature = "python", pub)]
    async fn add_table_column(
        &mut self,
        table_name: &TableName,
        column: Column,
        parent_path: &ColumnName,
    ) -> DucklakeResult<()> {
        let mut table = Arc::make_mut(&mut self.catalog).table_mut(table_name)?;
        table.ensure_next_column_id(&self.pool).await?;
        let (parent_column_ref, column_refs) =
            table.add_column(parent_path.as_ref(), column.clone())?;
        self.changes.push(Change::AddTableColumn {
            parent_column_ref,
            column_refs,
            column,
        });
        Ok(())
    }

    #[visibility_if(feature = "python", pub)]
    fn remove_table_column(
        &mut self,
        table_name: &TableName,
        column: &ColumnName,
    ) -> DucklakeResult<()> {
        let mut table = self.catalog_mut().table_mut(table_name)?;
        let mut column = table.column_mut(column.as_ref())?;
        let column_refs = column.remove()?;
        for column_ref in column_refs {
            self.changes.push(Change::RemoveTableColumn { column_ref });
        }
        Ok(())
    }
}

/* --------------------------------------- UPDATE COLUMNS -------------------------------------- */

impl<'tx, 'a> TransactionTable<'tx, 'a> {
    /// Rename a column in the table.
    pub fn rename_column(
        &mut self,
        column: impl IntoColumnName,
        new_name: &str,
    ) -> DucklakeResult<()> {
        self.tx.rename_table_column(
            &self.name,
            &column.try_into().map_err(|e| e.into())?,
            new_name,
        )
    }

    /// Update the dtype of a column in the table.
    pub async fn update_column_dtype(
        &mut self,
        column: impl IntoColumnName,
        new_dtype: crate::DataType,
    ) -> DucklakeResult<()> {
        self.tx
            .update_table_column_dtype(
                &self.name,
                &column.try_into().map_err(|e| e.into())?,
                new_dtype,
            )
            .await
    }

    /// Update the default value of a column in the table.
    pub fn update_column_default(
        &mut self,
        column: impl IntoColumnName,
        default_value: crate::ColumnDefault,
    ) -> DucklakeResult<()> {
        self.tx.update_table_column_default(
            &self.name,
            &column.try_into().map_err(|e| e.into())?,
            default_value,
        )
    }

    /// Update the nullability of a column in the table.
    pub async fn update_column_nullability(
        &mut self,
        column: impl IntoColumnName,
        nullable: bool,
    ) -> DucklakeResult<()> {
        self.tx
            .update_table_column_nullability(
                &self.name,
                &column.try_into().map_err(|e| e.into())?,
                nullable,
            )
            .await
    }
}

impl<'a> Transaction<'a> {
    #[visibility_if(feature = "python", pub)]
    fn rename_table_column(
        &mut self,
        table_name: &TableName,
        column: &ColumnName,
        new_name: &str,
    ) -> DucklakeResult<()> {
        if column.0.last().unwrap() == new_name {
            return Ok(());
        }

        let mut table = self.catalog_mut().table_mut(table_name)?;
        let mut column = table.column_mut(column.as_ref())?;
        column.rename(new_name)?;
        let change = Change::UpdateTableColumn {
            parent_column_ref: column.parent_ref(),
            column_ref: column.ref_(),
            column: column.info(),
        };
        self.changes.push(change);
        Ok(())
    }

    #[visibility_if(feature = "python", pub)]
    async fn update_table_column_dtype(
        &mut self,
        table_name: &TableName,
        column: &ColumnName,
        dtype: crate::DataType,
    ) -> DucklakeResult<()> {
        let table = self.catalog().table(table_name)?;
        let existing_column = table.column(column.as_ref())?.info();
        let guard = self.guard();
        // NOTE: Some "data type updates" may actually be represented as column additions or
        //  deletions in case structs are being modified
        guard
            .tx
            .update_table_column_dtype_recursive(
                table_name,
                column.as_ref(),
                existing_column.dtype,
                dtype,
            )
            .await?;
        guard.commit();
        Ok(())
    }

    #[visibility_if(feature = "python", pub)]
    fn update_table_column_default(
        &mut self,
        table_name: &TableName,
        column: &ColumnName,
        default_value: crate::ColumnDefault,
    ) -> DucklakeResult<()> {
        let mut table = self.catalog_mut().table_mut(table_name)?;
        let mut column = table.column_mut(column.as_ref())?;
        column.update_default_value(default_value);
        let change = Change::UpdateTableColumn {
            parent_column_ref: column.parent_ref(),
            column_ref: column.ref_(),
            column: column.info(),
        };
        self.changes.push(change);
        Ok(())
    }

    #[visibility_if(feature = "python", pub)]
    async fn update_table_column_nullability(
        &mut self,
        table_name: &TableName,
        column: &ColumnName,
        nullable: bool,
    ) -> DucklakeResult<()> {
        let table = self.catalog().table(table_name)?;
        let column_view = table.column(column.as_ref())?;
        if column_view.nullable() == nullable {
            return Ok(());
        }

        // If we want to make the column non-nullable, we need to ensure that it doesn't currently
        // contain null values.
        if !nullable
            && let Some(table_stats) = self.snapshot.table_stats().await?.get(&table.id().unwrap())
            && let Some(column_stats) = table_stats.column_stats(column_view.id())
            && column_stats.contains_null().unwrap_or(false)
        {
            return Err(DucklakeError::InvalidNullabilityChange {
                column: column.to_string(),
            });
        }

        let mut table = self.catalog_mut().table_mut(table_name)?;
        let mut column = table.column_mut(column.as_ref())?;
        column.update_nullability(nullable);
        let change = Change::UpdateTableColumn {
            parent_column_ref: column.parent_ref(),
            column_ref: column.ref_(),
            column: column.info(),
        };
        self.changes.push(change);
        Ok(())
    }

    #[async_recursion::async_recursion]
    async fn update_table_column_dtype_recursive(
        &mut self,
        table_name: &TableName,
        column_name: &[String],
        existing_dtype: crate::DataType,
        target_dtype: crate::DataType,
    ) -> DucklakeResult<()> {
        use crate::DataType::*;

        // If dtypes match, there's nothing to do
        if existing_dtype == target_dtype {
            return Ok(());
        }

        // Otherwise, we update
        match (&existing_dtype, &target_dtype) {
            // Type promotions, see also:
            // https://ducklake.select/docs/stable/duckdb/usage/schema_evolution#type-promotion
            (Int8, Int16 | Int32 | Int64)
            | (Int16, Int32 | Int64)
            | (Int32, Int64)
            | (UInt8, UInt16 | UInt32 | UInt64)
            | (UInt16, UInt32 | UInt64)
            | (UInt32, UInt64)
            | (Float32, Float64) => {
                let mut table = self.catalog_mut().table_mut(table_name)?;
                let mut column = table.column_mut(column_name)?;
                column.update_primitive_data_type(target_dtype);
                let change = Change::UpdateTableColumn {
                    parent_column_ref: column.parent_ref(),
                    column_ref: column.ref_(),
                    column: column.info(),
                };
                self.changes.push(change);
            }
            // Struct "casts"
            (Struct(old_fields), Struct(new_fields)) => {
                // - Update dtype of common fields recursively
                // - Delete fields in 'old' but not in 'new'
                // - Add fields in 'new' but not in 'old'
                for item in
                    primitives::iter_vec_diff(old_fields, new_fields, |col| col.name.clone())
                {
                    match item {
                        EitherOrBoth::Both(old, new) => {
                            self.update_table_column_dtype_recursive(
                                table_name,
                                &[column_name, std::slice::from_ref(&old.name)].concat(),
                                old.dtype.clone(),
                                new.dtype.clone(),
                            )
                            .await?;
                        }
                        EitherOrBoth::Left(old) => self.remove_table_column(
                            table_name,
                            &[column_name, std::slice::from_ref(&old.name)]
                                .concat()
                                .into(),
                        )?,
                        EitherOrBoth::Right(new) => {
                            self.add_table_column(table_name, (*new).clone(), &column_name.into())
                                .await?
                        }
                    }
                }
            }
            (List(old_inner), List(new_inner)) => {
                let nested_column_name = [column_name, &["element".to_owned()]].concat();
                self.update_table_column_dtype_recursive(
                    table_name,
                    &nested_column_name,
                    old_inner.dtype.clone(),
                    new_inner.dtype.clone(),
                )
                .await?;
            }
            (Map(old_key, old_value), Map(new_key, new_value)) => {
                let key_name = [column_name, &["key".to_owned()]].concat();
                let value_name = [column_name, &["value".to_owned()]].concat();
                self.update_table_column_dtype_recursive(
                    table_name,
                    &key_name,
                    old_key.dtype.clone(),
                    new_key.dtype.clone(),
                )
                .await?;
                self.update_table_column_dtype_recursive(
                    table_name,
                    &value_name,
                    old_value.dtype.clone(),
                    new_value.dtype.clone(),
                )
                .await?;
            }
            _ => {
                return Err(DucklakeError::InvalidCast {
                    old: existing_dtype.clone(),
                    new: target_dtype.clone(),
                });
            }
        }
        Ok(())
    }
}

/* ------------------------------------ UPDATE PARTITIONING ------------------------------------ */

impl<'tx, 'a> TransactionTable<'tx, 'a> {
    /// Update the table's partitioning.
    pub fn update_partitioning(
        &mut self,
        columns: Option<Vec<crate::PartitionColumn>>,
    ) -> DucklakeResult<()> {
        self.tx.update_table_partitioning(&self.name, columns)
    }
}

impl<'a> Transaction<'a> {
    #[visibility_if(feature = "python", pub)]
    fn update_table_partitioning(
        &mut self,
        table_name: &TableName,
        partition_columns: Option<Vec<PartitionColumn>>,
    ) -> DucklakeResult<()> {
        let mut table = self.catalog_mut().table_mut(table_name)?;
        let partition_column_refs =
            table.update_partitioning(partition_columns.clone().map(|c| c.into()))?;
        let change = Change::UpdateTablePartitioning {
            table_ref: table.ref_(),
            partition_column_refs,
            partition_columns,
        };
        self.changes.push(change);
        Ok(())
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                              TAGS                                             */
/* --------------------------------------------------------------------------------------------- */

/* ------------------------------------------- TABLE ------------------------------------------- */

impl<'tx, 'a> TransactionTable<'tx, 'a> {
    /// Add a new tag for the table.
    pub fn add_tag(&mut self, key: &str, value: &str) -> DucklakeResult<()> {
        self.tx.add_table_tag(&self.name, key, value)
    }

    /// Remove a tag from the table.
    pub fn remove_tag(&mut self, key: &str) -> DucklakeResult<()> {
        self.tx.remove_table_tag(&self.name, key)
    }
}

impl<'a> Transaction<'a> {
    #[visibility_if(feature = "python", pub)]
    fn add_table_tag(&mut self, name: &TableName, key: &str, value: &str) -> DucklakeResult<()> {
        let tag = Tag {
            key: key.to_string(),
            value: value.to_string(),
        };
        let mut table = self.catalog_mut().table_mut(name)?;
        table.add_tag(tag.clone());
        let change = Change::AddTableTag {
            table_ref: table.ref_(),
            tag,
        };
        self.changes.push(change);
        Ok(())
    }

    #[visibility_if(feature = "python", pub)]
    fn remove_table_tag(&mut self, name: &TableName, key: &str) -> DucklakeResult<()> {
        let mut table = self.catalog_mut().table_mut(name)?;
        table.remove_tag(key)?;
        let change = Change::RemoveTableTag {
            table_ref: table.ref_(),
            key: key.to_string(),
        };
        self.changes.push(change);
        Ok(())
    }
}

/* ------------------------------------------- COLUMN ------------------------------------------ */

impl<'tx, 'a> TransactionTable<'tx, 'a> {
    /// Add a new tag to a column of the table.
    pub fn add_column_tag(
        &mut self,
        column_path: impl TryInto<ColumnName, Error = impl Into<DucklakeError>>,
        key: &str,
        value: &str,
    ) -> DucklakeResult<()> {
        self.tx.add_table_column_tag(
            &self.name,
            &column_path.try_into().map_err(|e| e.into())?,
            key,
            value,
        )
    }

    /// Remove a tag from a column of the table.
    pub fn remove_column_tag(
        &mut self,
        column_path: impl TryInto<ColumnName, Error = impl Into<DucklakeError>>,
        key: &str,
    ) -> DucklakeResult<()> {
        self.tx.remove_table_column_tag(
            &self.name,
            &column_path.try_into().map_err(|e| e.into())?,
            key,
        )
    }
}

impl<'a> Transaction<'a> {
    #[visibility_if(feature = "python", pub)]
    fn add_table_column_tag(
        &mut self,
        table_name: &TableName,
        column_name: &ColumnName,
        key: &str,
        value: &str,
    ) -> DucklakeResult<()> {
        let tag = Tag {
            key: key.to_string(),
            value: value.to_string(),
        };
        let mut table = self.catalog_mut().table_mut(table_name)?;
        let mut column = table.column_mut(column_name.as_ref())?;
        column.add_tag(tag.clone());
        let change = Change::AddTableColumnTag {
            column_ref: column.ref_(),
            tag,
        };
        self.changes.push(change);
        Ok(())
    }

    #[visibility_if(feature = "python", pub)]
    fn remove_table_column_tag(
        &mut self,
        table_name: &TableName,
        column_name: &ColumnName,
        key: &str,
    ) -> DucklakeResult<()> {
        let mut table = self.catalog_mut().table_mut(table_name)?;
        let mut column = table.column_mut(column_name.as_ref())?;
        column.remove_tag(key)?;
        let change = Change::RemoveTableColumnTag {
            column_ref: column.ref_(),
            key: key.to_string(),
        };
        self.changes.push(change);
        Ok(())
    }
}
