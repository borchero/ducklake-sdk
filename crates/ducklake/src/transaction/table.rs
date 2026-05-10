use std::collections::HashMap;

use arrow_array::RecordBatch;
use ducklake_macros::visibility_if;
use itertools::{EitherOrBoth, Itertools};

use super::changes::Change;
use super::{CommitDataFile, CommitInlineData, Transaction};
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
        self.catalog_mut().try_table_id_by_name(&name)?;
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
            .try_table_schema_by_name(&self.name)?
            .columns
            .into_values();
        Ok(columns)
    }

    /// Get the partitioning of the table within the current transaction.
    pub fn partitioning(&self) -> DucklakeResult<Option<Vec<crate::PartitionColumn>>> {
        let columns = self
            .tx
            .catalog()
            .try_table_partitioning_by_name(&self.name)?
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
    ) -> DucklakeResult<TransactionTable<'_, 'a>> {
        let name = name.try_into().map_err(|e| e.into())?;
        let path: io::DucklakePath = path.unwrap_or_else(|| name.name.clone()).parse()?;

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
            path: path.ensure_directory(),
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
        let table_ref = self.catalog_mut().delete_table(name)?;
        let change = Change::DeleteTable { table_ref };
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
        let (table_ref, data_path) = self
            .catalog
            .try_table_data_path_by_name(table_name, &self.metadata.data_path())?;

        // Derive metadata
        let schema_id = self
            .catalog
            .try_schema_id_by_name(&table_name.schema)
            .unwrap();
        let table_id = self.catalog().table_id(table_ref).unwrap();
        let metadata = self.metadata.table_metadata(schema_id, table_id);

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
        let (table_ref, base_path) = self
            .catalog()
            .try_table_data_path_by_name(table_name, &self.metadata.data_path())?;
        let table_info = self.catalog().table_info_by_ref(table_ref);
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
                            let col_ref = self
                                .catalog_mut()
                                .try_column_ref_by_id(table_ref, column_id)?;
                            Ok((col_ref, stats))
                        })
                        .collect::<DucklakeResult<_>>()?,
                };
                Ok(commit_data_file)
            })
            .collect::<DucklakeResult<Vec<_>>>()?;
        let change = Change::WriteTableDataFiles {
            table_ref,
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
        let table_ref = self.catalog_mut().try_table_ref_by_name(table_name)?;
        let schema = self.catalog().table_schema_by_ref(table_ref);
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
                                let col_ref = self
                                    .catalog_mut()
                                    .try_column_ref_by_id(table_ref, column_id)?;
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

        let table_ref = self.catalog_mut().rename_table(old_name, new_name)?;
        let change = Change::RenameTable {
            table_ref,
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
        let table_info = self.catalog_mut().try_table_info_by_name(name)?;
        let guard = self.guard();
        // Iterate over the schemas and either update data types, add, or remove columns
        let old_columns = table_info.schema.columns;
        let new_columns = Schema::try_from(new_columns)?.columns;
        for item in primitives::iter_index_map_diff(&old_columns, &new_columns) {
            match item {
                EitherOrBoth::Both(_, (col_name, col)) => {
                    let col_name = ColumnName::named(col_name);
                    guard
                        .tx
                        .update_table_column_dtype(name, &col_name, col.dtype.clone())?;
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
                        .add_table_column(name, col.clone(), &Default::default())?;
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
    pub fn add_column(&mut self, column: crate::Column) -> DucklakeResult<()> {
        self.tx
            .add_table_column(&self.name, column, &Default::default())
    }

    /// Remove a column from the table.
    pub fn remove_column(&mut self, column: impl IntoColumnName) -> DucklakeResult<()> {
        self.tx
            .remove_table_column(&self.name, &column.try_into().map_err(|e| e.into())?)
    }
}

impl<'a> Transaction<'a> {
    #[visibility_if(feature = "python", pub)]
    fn add_table_column(
        &mut self,
        table_name: &TableName,
        column: Column,
        parent_path: &ColumnName,
    ) -> DucklakeResult<()> {
        let (parent_column_ref, column_refs) = self.catalog_mut().add_table_column(
            table_name,
            parent_path.as_ref(),
            column.clone(),
        )?;
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
        let column_refs = self
            .catalog_mut()
            .remove_table_column(table_name, column.as_ref())?;
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
        self.tx.update_table_column_dtype(
            &self.name,
            &column.try_into().map_err(|e| e.into())?,
            new_dtype,
        )
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

        let column_ref =
            self.catalog_mut()
                .rename_table_column(table_name, column.as_ref(), new_name)?;
        self.changes.push(Change::UpdateTableColumn {
            parent_column_ref: self.catalog().parent_column_ref(column_ref),
            column_ref,
            column: self.catalog().column(column_ref),
        });
        Ok(())
    }

    #[visibility_if(feature = "python", pub)]
    fn update_table_column_dtype(
        &mut self,
        table_name: &TableName,
        column: &ColumnName,
        dtype: crate::DataType,
    ) -> DucklakeResult<()> {
        let (_, existing_column) = self.catalog.try_column_by_name(table_name, column)?;
        let guard = self.guard();
        // NOTE: Some "data type updates" may actually be represented as column additions or
        //  deletions in case structs are being modified
        guard.tx.update_table_column_dtype_recursive(
            table_name,
            column.as_ref(),
            existing_column.dtype,
            dtype,
        )?;
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
        let column_ref = self.catalog_mut().update_table_column_default_value(
            table_name,
            column.as_ref(),
            default_value.clone(),
        )?;
        self.changes.push(Change::UpdateTableColumn {
            parent_column_ref: self.catalog().parent_column_ref(column_ref),
            column_ref,
            column: self.catalog().column(column_ref),
        });
        Ok(())
    }

    #[visibility_if(feature = "python", pub)]
    async fn update_table_column_nullability(
        &mut self,
        table_name: &TableName,
        column: &ColumnName,
        nullable: bool,
    ) -> DucklakeResult<()> {
        let (column_ref, existing_column) = self.catalog.try_column_by_name(table_name, column)?;
        if existing_column.nullable == nullable {
            return Ok(());
        }

        // If we want to make the column non-nullable, we need to ensure that it doesn't currently
        // contain null values.
        if !nullable {
            let table_id = self.catalog().try_table_id_by_name(table_name)?;
            if let Some(table_stats) = self.snapshot.table_stats().await?.get(&table_id) {
                let column_id = self.catalog().column_id(column_ref).unwrap();
                if let Some(column_stats) = table_stats.column_stats(column_id)
                    && column_stats.contains_null().unwrap_or(false)
                {
                    return Err(DucklakeError::InvalidNullabilityChange {
                        column: column.to_string(),
                    });
                }
            }
        }

        let column_ref = self.catalog_mut().update_table_column_nullability(
            table_name,
            column.as_ref(),
            nullable,
        )?;
        self.changes.push(Change::UpdateTableColumn {
            parent_column_ref: self.catalog().parent_column_ref(column_ref),
            column_ref,
            column: self.catalog().column(column_ref),
        });
        Ok(())
    }

    fn update_table_column_dtype_recursive(
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
                let column_ref = self.catalog_mut().update_table_column_primitive_data_type(
                    table_name,
                    column_name,
                    target_dtype.clone(),
                )?;
                self.changes.push(Change::UpdateTableColumn {
                    parent_column_ref: self.catalog().parent_column_ref(column_ref),
                    column_ref,
                    column: self.catalog().column(column_ref),
                });
            }
            // Struct "casts"
            (Struct(old_fields), Struct(new_fields)) => {
                let old_fields_map: HashMap<_, _> = old_fields
                    .iter()
                    .map(|col| (col.name.clone(), col))
                    .collect();
                let new_fields_map: HashMap<_, _> = new_fields
                    .iter()
                    .map(|col| (col.name.clone(), col))
                    .collect();

                // - Update dtype of common fields recursively
                // - Delete fields in 'old' but not in 'new'
                // - Add fields in 'new' but not in 'old'
                for key in old_fields_map.keys().chain(new_fields_map.keys()).unique() {
                    let nested_column_name = [column_name, &[key.to_owned()]].concat();
                    match (old_fields_map.get(key), new_fields_map.get(key)) {
                        (Some(old), Some(new)) => {
                            self.update_table_column_dtype_recursive(
                                table_name,
                                &nested_column_name,
                                old.dtype.clone(),
                                new.dtype.clone(),
                            )?;
                        }
                        (Some(_), None) => {
                            self.remove_table_column(table_name, &nested_column_name.into())?
                        }
                        (None, Some(new)) => {
                            self.add_table_column(table_name, (*new).clone(), &column_name.into())?
                        }
                        (None, None) => unreachable!(),
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
                )?;
            }
            (Map(old_key, old_value), Map(new_key, new_value)) => {
                let key_name = [column_name, &["key".to_owned()]].concat();
                let value_name = [column_name, &["value".to_owned()]].concat();
                self.update_table_column_dtype_recursive(
                    table_name,
                    &key_name,
                    old_key.dtype.clone(),
                    new_key.dtype.clone(),
                )?;
                self.update_table_column_dtype_recursive(
                    table_name,
                    &value_name,
                    old_value.dtype.clone(),
                    new_value.dtype.clone(),
                )?;
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
        let (table_ref, partition_column_refs) = self
            .catalog_mut()
            .update_table_partitioning(table_name, partition_columns.clone().map(|c| c.into()))?;
        let change = Change::UpdateTablePartitioning {
            table_ref,
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
        let table_ref = self.catalog_mut().add_table_tag(name, tag.clone())?;
        let change = Change::AddTableTag { table_ref, tag };
        self.changes.push(change);
        Ok(())
    }

    #[visibility_if(feature = "python", pub)]
    fn remove_table_tag(&mut self, name: &TableName, key: &str) -> DucklakeResult<()> {
        let table_ref = self.catalog_mut().remove_table_tag(name, key)?;
        let change = Change::RemoveTableTag {
            table_ref,
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
        let column_ref = self.catalog_mut().add_table_column_tag(
            table_name,
            column_name.as_ref(),
            tag.clone(),
        )?;
        let change = Change::AddTableColumnTag { column_ref, tag };
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
        let column_ref =
            self.catalog_mut()
                .remove_table_column_tag(table_name, column_name.as_ref(), key)?;
        let change = Change::RemoveTableColumnTag {
            column_ref,
            key: key.to_string(),
        };
        self.changes.push(change);
        Ok(())
    }
}
