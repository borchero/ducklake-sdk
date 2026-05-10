use std::collections::HashMap;

use crate::{DucklakeError, DucklakeResult, io};

mod load;
mod refs;
mod typedefs;
use itertools::Itertools;
pub use refs::{ColumnRef, SchemaRef, TableRef};
use typedefs::*;

/// Point-in-time capture of the DuckLake schema. This includes all schemas, tables, their
/// columns, etc.
///
/// The catalog may be altered within a transaction, hence, this type exposes numerous methods to
/// modify the catalog. Entities (i.e. schemas/tables) which are created "locally" (i.e. within
/// the transaction) are not immediately assigned an ID. Hence, they can only be referred to by
/// their name as opposed to an entity ID.
#[derive(Debug, Clone)]
pub struct Catalog {
    // Unordered list of all entities (=schemas and tables) in the catalog.
    // When modifying the catalog within a transaction, this arena contains pending and deleted
    // entities as well.
    arena: Vec<CatalogEntity>,
    // Mapping from entity ID to arena index for quick lookup by ID. This mapping does not include
    // pending entities as they do not have an ID yet.
    by_id: HashMap<i64, ArenaIdx>,
    // Mapping from schema name to arena index for quick lookup by schema name.
    schemas: HashMap<String, ArenaIdx>,
}

/// Index into the catalog arena.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ArenaIdx(usize);

/// An entity in the catalog, either a schema or a table.
#[derive(Debug, Clone)]
enum CatalogEntity {
    Schema(CatalogSchema),
    Table(CatalogTable),
}

/* --------------------------------------------------------------------------------------------- */
/*                                   TRANSACTION-LEVEL CHANGES                                   */
/* --------------------------------------------------------------------------------------------- */

/* ------------------------------------------- SCHEMA ------------------------------------------ */

impl Catalog {
    /// Insert a new schema with the given name as a pending schema.
    ///
    /// This method returns a reference to the newly created schema and errors if the schema
    /// already exists.
    pub fn add_schema(&mut self, name: &str, path: io::DucklakePath) -> DucklakeResult<SchemaRef> {
        // If the schema exists already, we need to raise some kind of error
        if let Ok((schema, _)) = self.try_schema_by_name(name) {
            return match &schema.state {
                CatalogState::Existing { .. } | CatalogState::Pending => {
                    Err(DucklakeError::schema_already_exists(name))
                }
                CatalogState::Deleted { .. } => Err(DucklakeError::InvalidChanges(format!(
                    "cannot create schema {name} which was deleted in the same transaction"
                ))),
            };
        }

        // If the schema does not yet exist, create a new pending schema
        let schema = CatalogSchema {
            state: CatalogState::Pending,
            name: name.to_string(),
            tables: HashMap::new(),
            path,
        };
        let idx = self.push_schema(schema);
        self.schemas.insert(name.to_string(), idx);
        Ok(idx.into())
    }

    /// Delete the schema with the given name by marking it as deleted and return its reference.
    ///
    /// Deleting a schema that has already been marked as deleted or does not exist will raise an
    /// error.
    pub fn delete_schema(&mut self, name: &str) -> DucklakeResult<SchemaRef> {
        // Find the schema to delete
        let (schema, schema_idx) = self.try_schema_by_name_mut(name)?;
        if !schema.tables.is_empty() {
            return Err(DucklakeError::InvalidChanges(format!(
                "cannot delete schema {name} which is not empty"
            )));
        }

        // Depending on the current state, either mark deleted or raise an error
        match &schema.state {
            CatalogState::Existing { id } => {
                schema.state = CatalogState::Deleted { id: *id };
                Ok(schema_idx.into())
            }
            CatalogState::Pending => Err(DucklakeError::InvalidChanges(format!(
                "cannot delete schema {name} which was created in the same transaction"
            ))),
            CatalogState::Deleted { .. } => Err(DucklakeError::schema_not_found(name)),
        }
    }
}

/* ------------------------------------------- TABLE ------------------------------------------- */

impl Catalog {
    /// Add a new table with the given name as a pending table.
    ///
    /// This method returns references for all relevant entities associated with the insert:
    ///  - A reference to the schema the table was created in
    ///  - A reference to the newly created table
    ///  - References to all created columns (organized by root column)
    ///  - References to all created partition columns (if any)
    ///
    /// Returns an error if the schema does not exist or the table already exists.
    #[allow(clippy::type_complexity)]
    pub fn add_table(
        &mut self,
        table: crate::TableInfo,
        path: io::DucklakePath,
    ) -> DucklakeResult<(
        SchemaRef,
        TableRef,
        Vec<Vec<ColumnRef>>,
        Option<Vec<ColumnRef>>,
    )> {
        // If the table exists already, we need to raise some kind of error
        if let Ok((table, _)) = self.try_table_by_name(&table.name) {
            return match table.state {
                CatalogState::Existing { .. } | CatalogState::Pending => {
                    Err(DucklakeError::table_already_exists(&table.name))
                }
                CatalogState::Deleted { .. } => Err(DucklakeError::InvalidChanges(format!(
                    "cannot create table {} which was deleted in the same transaction",
                    table.name
                ))),
            };
        }

        // If the table does not yet exist, create a new pending table
        // NOTE: We might still return an error at this point as we didn't check explicitly
        //  above whether the schema exists. This is not an issue, however, as it's transparent to
        //  the caller.
        let columns = table.schema.into();
        let partition = table
            .partitioning
            .map(|p| CatalogTablePartition::from_partition(p, &columns))
            .transpose()?;
        let catalog_table = CatalogTable {
            state: CatalogState::Pending,
            name: table.name.clone(),
            columns,
            partition: partition.clone(),
            tags: table.tags,
            path,
        };
        let column_idxs = catalog_table.columns.root_column_indices();
        let table_idx = self.push_table(catalog_table);

        let (schema, schema_idx) = self.try_schema_by_name_mut(&table.name.schema)?;
        schema.tables.insert(table.name.name.clone(), table_idx);

        // Collect all references and return
        let column_refs = column_idxs
            .into_iter()
            .map(|idxs| {
                idxs.into_iter()
                    .map(|idx| (table_idx, idx).into())
                    .collect()
            })
            .collect();
        let partition_refs = partition.map(|p| {
            p.columns
                .iter()
                .map(|col| (table_idx, col.column).into())
                .collect()
        });
        Ok((
            schema_idx.into(),
            table_idx.into(),
            column_refs,
            partition_refs,
        ))
    }

    pub fn rename_table(
        &mut self,
        name: &crate::TableName,
        new_name: &str,
    ) -> DucklakeResult<TableRef> {
        // Find the table to rename
        let (table, _) = self.try_table_by_name(name)?;

        // Depending on the current state, either rename or raise an error
        match table.state {
            CatalogState::Existing { .. } | CatalogState::Pending => {
                // Ensure that the new name does not already exist
                let (schema, _) = self.try_schema_by_name_mut(&name.schema)?;
                if schema.tables.contains_key(new_name) {
                    return Err(DucklakeError::table_already_exists(&crate::TableName {
                        schema: name.schema.clone(),
                        name: new_name.to_string(),
                    }));
                }

                // Rename the table in the schema's table mapping
                let arena_idx = schema.tables.remove(&name.name).unwrap(); // SAFETY: checked above
                schema.tables.insert(new_name.to_string(), arena_idx);

                // Rename the table itself
                let table = self.table_by_arena_idx_mut(arena_idx);
                table.name = crate::TableName {
                    schema: name.schema.clone(),
                    name: new_name.to_string(),
                };
                Ok(TableRef(arena_idx))
            }
            CatalogState::Deleted { .. } => Err(DucklakeError::table_not_found(name)),
        }
    }

    /// Update the partitioning of the table.
    pub fn update_table_partitioning(
        &mut self,
        name: &crate::TableName,
        partitioning: Option<crate::Partition>,
    ) -> DucklakeResult<(TableRef, Option<Vec<ColumnRef>>)> {
        // Find the table
        let (table, table_idx) = self.try_table_by_name_mut(name)?;

        // If the partitioning is already pending, raise an error
        if let Some(partition) = table.partition.as_ref()
            && !matches!(partition.state, CatalogState::Existing { .. })
        {
            return Err(DucklakeError::InvalidChanges(format!(
                "cannot update partitioning for table {} more than once in the same transaction",
                name
            )));
        }

        // Set the new partitioning
        table.partition = partitioning
            .map(|p| CatalogTablePartition::from_partition(p, &table.columns))
            .transpose()?;

        // Derive the partition's column refs
        let partition_refs = table.partition.as_ref().map(|p| {
            p.columns
                .iter()
                .map(|col| (table_idx, col.column).into())
                .collect()
        });
        Ok((table_idx.into(), partition_refs))
    }

    /// Delete the table with the given identifier by marking it as deleted and return its
    /// reference.
    ///
    /// Deleting a table that has already been marked as deleted or does not exist will raise an
    /// error.
    pub fn delete_table(&mut self, name: &crate::TableName) -> DucklakeResult<TableRef> {
        // Find the table to delete
        let (table, table_idx) = self.try_table_by_name_mut(name)?;

        // Depending on the current state, either mark deleted or raise an error
        match &table.state {
            CatalogState::Existing { id } => {
                table.state = CatalogState::Deleted { id: *id };
                Ok(TableRef(table_idx))
            }
            CatalogState::Pending => Err(DucklakeError::InvalidChanges(format!(
                "cannot delete table '{}' which was created in the same transaction",
                name
            ))),
            CatalogState::Deleted { .. } => Err(DucklakeError::table_not_found(name)),
        }
    }

    /// Rename a column in the specified table.
    pub fn rename_table_column(
        &mut self,
        name: &crate::TableName,
        column_path: &[String],
        new_name: &str,
    ) -> DucklakeResult<ColumnRef> {
        let (table, table_idx) = self.try_table_by_name_mut(name)?;
        let column_idx = table.columns.rename_column(column_path, new_name)?;
        Ok((table_idx, column_idx).into())
    }

    /// Remove a column in the specified table.
    ///
    /// This may return multiple column references in the case of (deeply) nested columns being
    /// removed.
    pub fn remove_table_column(
        &mut self,
        name: &crate::TableName,
        column_path: &[String],
    ) -> DucklakeResult<Vec<ColumnRef>> {
        let (table, table_idx) = self.try_table_by_name_mut(name)?;
        if column_path.len() == 1
            && let Some(partition_columns) = table.partition_column_names()
            && partition_columns.contains(&column_path[0])
        {
            return Err(DucklakeError::InvalidChanges(format!(
                "cannot remove column '{}' from table {} as the table is partitioned by it - reset or change the partitioning on this table in order to drop this column",
                column_path[0], name
            )));
        }
        let column_idxs = table.columns.remove_column(column_path)?;
        Ok(column_idxs
            .into_iter()
            .map(|column_idx| (table_idx, column_idx).into())
            .collect())
    }

    pub fn add_table_column(
        &mut self,
        name: &crate::TableName,
        path: &[String],
        column: crate::Column,
    ) -> DucklakeResult<(Option<ColumnRef>, Vec<ColumnRef>)> {
        let (table, table_idx) = self.try_table_by_name_mut(name)?;
        let (parent_idx, column_idxs) = table.columns.add_column(path, column)?;
        let parent_ref = parent_idx.map(|idx| (table_idx, idx).into());
        let column_refs = column_idxs
            .into_iter()
            .map(|idx| (table_idx, idx).into())
            .collect();
        Ok((parent_ref, column_refs))
    }

    pub fn update_table_column_primitive_data_type(
        &mut self,
        name: &crate::TableName,
        path: &[String],
        data_type: crate::DataType,
    ) -> DucklakeResult<ColumnRef> {
        let (table, table_idx) = self.try_table_by_name_mut(name)?;
        let column_idx = table.columns.update_primitive_data_type(path, data_type)?;
        Ok((table_idx, column_idx).into())
    }

    pub fn update_table_column_default_value(
        &mut self,
        name: &crate::TableName,
        path: &[String],
        default_value: crate::ColumnDefault,
    ) -> DucklakeResult<ColumnRef> {
        let (table, table_idx) = self.try_table_by_name_mut(name)?;
        let column_idx = table.columns.update_default_value(path, default_value)?;
        Ok((table_idx, column_idx).into())
    }

    pub fn update_table_column_nullability(
        &mut self,
        name: &crate::TableName,
        path: &[String],
        nullable: bool,
    ) -> DucklakeResult<ColumnRef> {
        let (table, table_idx) = self.try_table_by_name_mut(name)?;
        let column_idx = table.columns.update_nullability(path, nullable)?;
        Ok((table_idx, column_idx).into())
    }

    pub fn add_table_tag(
        &mut self,
        name: &crate::TableName,
        tag: crate::Tag,
    ) -> DucklakeResult<TableRef> {
        let (table, table_idx) = self.try_table_by_name_mut(name)?;
        table.tags.push(tag);
        Ok(table_idx.into())
    }

    pub fn remove_table_tag(
        &mut self,
        name: &crate::TableName,
        key: &str,
    ) -> DucklakeResult<TableRef> {
        let (table, table_idx) = self.try_table_by_name_mut(name)?;
        let removed = table
            .tags
            .extract_if(.., |tag| tag.key == key)
            .collect_vec();
        if removed.is_empty() {
            return Err(DucklakeError::InvalidChanges(format!(
                "no tag with key '{}' found for table {}",
                key, name
            )));
        }
        Ok(table_idx.into())
    }

    pub fn add_table_column_tag(
        &mut self,
        table_name: &crate::TableName,
        column_path: &[String],
        tag: crate::Tag,
    ) -> DucklakeResult<ColumnRef> {
        let (table, table_idx) = self.try_table_by_name_mut(table_name)?;
        let (column, column_idx) = table.columns.try_column_by_path_mut(column_path)?;
        column.tags.push(tag);
        Ok((table_idx, column_idx).into())
    }

    pub fn remove_table_column_tag(
        &mut self,
        table_name: &crate::TableName,
        column_path: &[String],
        key: &str,
    ) -> DucklakeResult<ColumnRef> {
        let (table, table_idx) = self.try_table_by_name_mut(table_name)?;
        let (column, column_idx) = table.columns.try_column_by_path_mut(column_path)?;
        let removed = column
            .tags
            .extract_if(.., |tag| tag.key == key)
            .collect_vec();
        if removed.is_empty() {
            return Err(DucklakeError::InvalidChanges(format!(
                "no tag with key '{}' found for column {} in table {}",
                key,
                crate::ColumnName::from(column_path),
                table_name
            )));
        }
        Ok((table_idx, column_idx).into())
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                      COMMIT-LEVEL CHANGES                                     */
/* --------------------------------------------------------------------------------------------- */

impl Catalog {
    /// Get the ID for the schema with the provided reference, if it exists.
    pub fn schema_id(&self, schema_ref: SchemaRef) -> Option<i64> {
        let schema = self.schema_by_ref(schema_ref);
        schema.id()
    }

    /// Resolve the ID for a pending schema by setting it to the provided one.
    pub fn resolve_schema_id(&mut self, schema_ref: SchemaRef, id: i64) {
        let schema = self.schema_by_ref_mut(schema_ref);
        match schema.state {
            CatalogState::Pending => {
                schema.state = CatalogState::Existing { id };
                self.by_id.insert(id, schema_ref.0);
            }
            _ => panic!("schema must be in state 'pending' to set ID"),
        }
    }

    /// Get the ID for the table with the provided reference, if it exists.
    pub fn table_id(&self, table_ref: TableRef) -> Option<i64> {
        let table = self.table_by_ref(table_ref);
        table.id()
    }

    /// Resolve the ID for a pending table by setting it to the provided one.
    pub fn resolve_table_id(&mut self, table_ref: TableRef, id: i64) {
        let table = self.table_by_ref_mut(table_ref);
        match table.state {
            CatalogState::Pending => {
                table.state = CatalogState::Existing { id };
                self.by_id.insert(id, table_ref.0);
            }
            _ => panic!("table must be in state 'pending' to set ID"),
        }
    }

    /// Get the ID for the column with the provided reference, if it exists.
    pub fn column_id(&self, column_ref: ColumnRef) -> Option<i64> {
        let column = self.column_by_ref(column_ref);
        column.id()
    }

    pub fn parent_column_ref(&self, column_ref: ColumnRef) -> Option<ColumnRef> {
        let column = self.column_by_ref(column_ref);
        column.parent_column.map(|parent_idx| {
            let table_idx = column_ref.table_ref.0;
            (table_idx, parent_idx).into()
        })
    }

    /// Get information of a column by its reference.
    pub fn column(&self, column_ref: ColumnRef) -> crate::Column {
        let catalog_table = self.table_by_ref(column_ref.table_ref);
        catalog_table
            .columns
            .schema_column_from_arena_index(column_ref.column_idx)
            .unwrap()
    }

    /// Resolve the ID for a pending column by setting it to the provided one.
    pub fn resolve_column_id(&mut self, column_ref: ColumnRef, id: i64) {
        let table = self.table_by_ref_mut(column_ref.table_ref);
        let column = table.columns.column_by_arena_idx_mut(column_ref.column_idx);
        match column.state {
            CatalogState::Pending => {
                column.state = CatalogState::Existing { id };
            }
            _ => panic!("column must be in state 'pending' to set ID"),
        }
    }

    /// Get the ID for the partition within the table with the provided reference, if it exists.
    pub fn partition_id(&self, table_ref: TableRef) -> Option<i64> {
        let partition = self
            .table_by_ref(table_ref)
            .partition
            .as_ref()
            .expect("table must have partition info");
        partition.state.id()
    }

    /// Resolve the ID for a pending partition by setting it to the provided one.
    pub fn resolve_partition_id(&mut self, table_ref: TableRef, id: i64) {
        let table = self.table_by_ref_mut(table_ref);
        let partition = table
            .partition
            .as_mut()
            .expect("table must have partition info to set partition ID");
        match partition.state {
            CatalogState::Pending => {
                partition.state = CatalogState::Existing { id };
            }
            _ => panic!("partition must be in state 'pending' to set ID"),
        }
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                           ACCESSORS                                           */
/* --------------------------------------------------------------------------------------------- */

/* ------------------------------------------- PUBLIC ------------------------------------------ */

impl Catalog {
    /// Get a reference to the table by its name.
    /// Returns an error if the table does not exist.
    pub fn try_table_ref_by_name(&self, name: &crate::TableName) -> DucklakeResult<TableRef> {
        let (_, table_idx) = self.try_table_by_name(name)?;
        Ok(table_idx.into())
    }

    pub fn table_info_by_ref(&self, table_ref: TableRef) -> crate::TableInfo {
        let table = self.table_by_ref(table_ref);
        crate::TableInfo {
            name: table.name.clone(),
            schema: crate::Schema::from(&table.columns),
            partitioning: table
                .partition
                .as_ref()
                .map(|p| p.into_partition(&table.columns)),
            tags: table.tags.clone(),
        }
    }

    /// Get the full path for the data files of a table.
    /// Returns an error if the table does not exist.
    pub fn try_table_data_path_by_name(
        &self,
        name: &crate::TableName,
        root_data_path: &io::DucklakePath,
    ) -> DucklakeResult<(TableRef, io::DucklakePath)> {
        let (schema, _) = self.try_schema_by_name(&name.schema)?;
        let data_path = root_data_path.join(&schema.path);
        let (table, table_idx) = self.try_table_by_name(name)?;
        Ok((table_idx.into(), data_path.join(&table.path)))
    }

    pub fn try_table_data_path_by_id(
        &self,
        schema_id: i64,
        table_id: i64,
        root_data_path: &io::DucklakePath,
    ) -> DucklakeResult<io::DucklakePath> {
        let schema = self
            .schema_by_id(schema_id)
            .ok_or(DucklakeError::EntityNotFound { id: schema_id })?;
        let table = self
            .table_by_id(table_id)
            .ok_or(DucklakeError::EntityNotFound { id: table_id })?;
        let data_path = root_data_path.join(&schema.path);
        Ok(data_path.join(&table.path))
    }

    /// Get the IDs of all tables and their schemas, optionally filtered by schema name.
    pub fn list_table_ids(&self, schema: Option<&str>) -> Vec<i64> {
        self.by_id
            .iter()
            .filter_map(|(id, arena_idx)| {
                if let CatalogEntity::Table(table) = &self.arena[arena_idx.0]
                    && (schema.is_none() || schema == Some(table.name.schema.as_str()))
                {
                    return Some(*id);
                }
                None
            })
            .collect()
    }

    /// Get the names of all schemas.
    pub fn list_schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    /// Get the name of the schema with the provided ID, if it exists.
    pub fn try_schema_name_by_id(&self, id: i64) -> DucklakeResult<String> {
        let schema = self
            .schema_by_id(id)
            .ok_or(DucklakeError::EntityNotFound { id })?;
        Ok(schema.name.clone())
    }

    /// Get the name of the table with the provided ID, if it exists.
    pub fn try_table_name_by_id(&self, id: i64) -> DucklakeResult<crate::TableName> {
        let table = self
            .table_by_id(id)
            .ok_or(DucklakeError::EntityNotFound { id })?;
        Ok(table.name.clone())
    }

    /// Get the ID of the schema with the provided name, if it exists. If the schema exists but
    /// does not have an ID yet (i.e. is pending), this panics.
    pub fn try_schema_id_by_name(&self, name: &str) -> DucklakeResult<i64> {
        let (schema, _) = self.try_schema_by_name(name)?;
        Ok(schema.id().unwrap())
    }

    /// Get the ID of the table with the provided name, if it exists. If the table exists but
    /// does not have an ID yet (i.e. is pending), this panics.
    pub fn try_table_id_by_name(&self, name: &crate::TableName) -> DucklakeResult<i64> {
        let (table, _) = self.try_table_by_name(name)?;
        Ok(table.id().unwrap())
    }

    /// Get the table info by its name. Returns an error if the schema or table does not exist.
    pub fn try_table_info_by_name(
        &self,
        name: &crate::TableName,
    ) -> DucklakeResult<crate::TableInfo> {
        let table_idx = self
            .try_schema_by_name(&name.schema)?
            .0
            .tables
            .get(&name.name)
            .ok_or(DucklakeError::table_not_found(name))?;
        Ok(self.table_info_by_ref((*table_idx).into()))
    }

    pub fn try_table_column_data_types_by_id(
        &self,
        id: i64,
    ) -> DucklakeResult<HashMap<i64, crate::DataType>> {
        let catalog_table = self
            .table_by_id(id)
            .ok_or(DucklakeError::EntityNotFound { id })?;
        Ok(HashMap::from(&catalog_table.columns))
    }

    /// Get the schema of the table with the provided name, if it exists.
    pub fn try_table_schema_by_name(
        &self,
        name: &crate::TableName,
    ) -> DucklakeResult<crate::Schema> {
        let table_ref = self.try_table_ref_by_name(name)?;
        Ok(self.table_schema_by_ref(table_ref))
    }

    /// Get the schema of the table with the provided ID, if it exists.
    pub fn try_table_schema_by_id(&self, id: i64) -> DucklakeResult<crate::Schema> {
        let catalog_table = self
            .table_by_id(id)
            .ok_or(DucklakeError::EntityNotFound { id })?;
        Ok(crate::Schema::from(&catalog_table.columns))
    }

    /// Get the schema of the table with the provided ID, if it exists.
    pub fn table_schema_by_ref(&self, table_ref: TableRef) -> crate::Schema {
        let table = self.table_by_ref(table_ref);
        crate::Schema::from(&table.columns)
    }

    /// Get the partitioning of the table with the provided name, if it exists.
    pub fn try_table_partitioning_by_name(
        &self,
        name: &crate::TableName,
    ) -> DucklakeResult<Option<crate::Partition>> {
        let table_ref = self.try_table_ref_by_name(name)?;
        let catalog_table = self.table_by_ref(table_ref);
        Ok(catalog_table
            .partition
            .as_ref()
            .map(|p| p.into_partition(&catalog_table.columns)))
    }

    /// Get the partitioning of the table with the provided ID, if it exists.
    pub fn try_table_partitioning_by_id(
        &self,
        id: i64,
    ) -> DucklakeResult<Option<crate::Partition>> {
        let catalog_table = self
            .table_by_id(id)
            .ok_or(DucklakeError::EntityNotFound { id })?;
        Ok(catalog_table
            .partition
            .as_ref()
            .map(|p| p.into_partition(&catalog_table.columns)))
    }

    /// Get the tags of the table with the provided ID, if it exists.
    pub fn try_table_tags_by_id(&self, id: i64) -> DucklakeResult<Vec<crate::Tag>> {
        let catalog_table = self
            .table_by_id(id)
            .ok_or(DucklakeError::EntityNotFound { id })?;
        Ok(catalog_table.tags.clone())
    }

    pub fn try_column_ref_by_id(
        &self,
        table_ref: TableRef,
        column_id: i64,
    ) -> DucklakeResult<ColumnRef> {
        let catalog_table = self.table_by_ref(table_ref);
        let column_idx = catalog_table
            .columns
            .arena_idx_by_id(column_id)
            .ok_or(DucklakeError::EntityNotFound { id: column_id })?;
        Ok((table_ref.0, column_idx).into())
    }

    pub fn try_column_by_name(
        &self,
        table_name: &crate::TableName,
        column_name: &crate::ColumnName,
    ) -> DucklakeResult<(ColumnRef, crate::Column)> {
        let table_arena_idx = *self
            .try_schema_by_name(&table_name.schema)?
            .0
            .tables
            .get(&table_name.name)
            .ok_or(DucklakeError::table_not_found(table_name))?;
        let catalog_table = self.table_by_arena_idx(table_arena_idx);
        let (_, column_idx) = catalog_table
            .columns
            .try_column_by_path(column_name.as_ref())?;
        Ok((
            (table_arena_idx, column_idx).into(),
            catalog_table
                .columns
                .schema_column_from_arena_index(column_idx)
                .unwrap(),
        ))
    }
}

/* ------------------------------------------- SCHEMA ------------------------------------------ */

impl Catalog {
    // --- NAME ---

    /// Get a schema by its name. Returns an error if the schema does not exist.
    fn try_schema_by_name(&self, name: &str) -> DucklakeResult<(&CatalogSchema, ArenaIdx)> {
        let schema_idx = *self
            .schemas
            .get(name)
            .ok_or(DucklakeError::schema_not_found(name))?;
        Ok((self.schema_by_arena_idx(schema_idx), schema_idx))
    }

    /// Get a mutable schema by its name. Returns an error if the schema does not exist.
    fn try_schema_by_name_mut(
        &mut self,
        name: &str,
    ) -> DucklakeResult<(&mut CatalogSchema, ArenaIdx)> {
        let schema_idx = *self
            .schemas
            .get(name)
            .ok_or(DucklakeError::schema_not_found(name))?;
        Ok((self.schema_by_arena_idx_mut(schema_idx), schema_idx))
    }

    // --- ID ---

    /// Get a schema by its entity ID. Returns `None` if the schema does not exist.
    fn schema_by_id(&self, id: i64) -> Option<&CatalogSchema> {
        let schema_idx = self.by_id.get(&id)?;
        Some(self.schema_by_arena_idx(*schema_idx))
    }

    /// Get a mutable schema by its entity ID. Returns `None` if the schema does not exist.
    fn schema_by_id_mut(&mut self, id: i64) -> Option<&mut CatalogSchema> {
        let schema_idx = self.by_id.get(&id)?;
        Some(self.schema_by_arena_idx_mut(*schema_idx))
    }

    // --- REF ---

    /// Get a schema by its reference. Panics if the schema does not exist.
    fn schema_by_ref(&self, schema_ref: SchemaRef) -> &CatalogSchema {
        self.schema_by_arena_idx(schema_ref.0)
    }

    /// Get a mutable schema by its reference. Panics if the schema does not exist.
    fn schema_by_ref_mut(&mut self, schema_ref: SchemaRef) -> &mut CatalogSchema {
        self.schema_by_arena_idx_mut(schema_ref.0)
    }

    // --- ARENA IDX ---

    /// Get a schema by its arena index. Panics on invalid index.
    fn schema_by_arena_idx(&self, idx: ArenaIdx) -> &CatalogSchema {
        match &self.arena[idx.0] {
            CatalogEntity::Schema(schema) => schema,
            _ => unreachable!("arena index does not point to a schema"),
        }
    }

    /// Get a mutable schema by its arena index. Panics on invalid index.
    fn schema_by_arena_idx_mut(&mut self, idx: ArenaIdx) -> &mut CatalogSchema {
        match &mut self.arena[idx.0] {
            CatalogEntity::Schema(schema) => schema,
            _ => unreachable!("arena index does not point to a schema"),
        }
    }
}

/* ------------------------------------------- TABLE ------------------------------------------- */

impl Catalog {
    // --- NAME ---

    /// Get a table by its name. Returns an error if the schema or table does not exist.
    fn try_table_by_name(
        &self,
        name: &crate::TableName,
    ) -> DucklakeResult<(&CatalogTable, ArenaIdx)> {
        let table_idx = *self
            .try_schema_by_name(&name.schema)?
            .0
            .tables
            .get(&name.name)
            .ok_or(DucklakeError::table_not_found(name))?;
        Ok((self.table_by_arena_idx(table_idx), table_idx))
    }

    /// Get a mutable table by its name. Returns an error if the schema or table does not exist.
    fn try_table_by_name_mut(
        &mut self,
        name: &crate::TableName,
    ) -> DucklakeResult<(&mut CatalogTable, ArenaIdx)> {
        let table_idx = *self
            .try_schema_by_name_mut(&name.schema)?
            .0
            .tables
            .get(&name.name)
            .ok_or(DucklakeError::table_not_found(name))?;
        Ok((self.table_by_arena_idx_mut(table_idx), table_idx))
    }

    // --- ID ---

    /// Get a table by its entity ID. Returns `None` if the table does not exist.
    fn table_by_id(&self, id: i64) -> Option<&CatalogTable> {
        self.by_id.get(&id).map(|idx| self.table_by_arena_idx(*idx))
    }

    // --- REF ---

    /// Get a table by its reference. Panics if the table does not exist.
    fn table_by_ref(&self, table_ref: TableRef) -> &CatalogTable {
        self.table_by_arena_idx(table_ref.0)
    }

    /// Get a mutable table by its reference. Panics if the table does not exist.
    fn table_by_ref_mut(&mut self, table_ref: TableRef) -> &mut CatalogTable {
        self.table_by_arena_idx_mut(table_ref.0)
    }

    // --- ARENA IDX ---

    /// Get a table by its arena index. Panics on invalid index.
    fn table_by_arena_idx(&self, idx: ArenaIdx) -> &CatalogTable {
        match &self.arena[idx.0] {
            CatalogEntity::Table(table) => table,
            _ => unreachable!("arena index does not point to a table"),
        }
    }

    /// Get a mutable table by its arena index. Panics on invalid index.
    fn table_by_arena_idx_mut(&mut self, idx: ArenaIdx) -> &mut CatalogTable {
        match &mut self.arena[idx.0] {
            CatalogEntity::Table(table) => table,
            _ => unreachable!("arena index does not point to a table"),
        }
    }
}

/* ------------------------------------------- COLUMN ------------------------------------------ */

impl Catalog {
    /// Get a column by its reference. Panics if the column does not exist.
    fn column_by_ref(&self, column_ref: ColumnRef) -> &CatalogColumn {
        let table = self.table_by_ref(column_ref.table_ref);
        table.columns.column_by_arena_idx(column_ref.column_idx)
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                           INTERNALS                                           */
/* --------------------------------------------------------------------------------------------- */

impl Catalog {
    fn push_schema(&mut self, schema: CatalogSchema) -> ArenaIdx {
        self.push_entity(CatalogEntity::Schema(schema))
    }

    fn push_table(&mut self, table: CatalogTable) -> ArenaIdx {
        self.push_entity(CatalogEntity::Table(table))
    }

    fn push_entity(&mut self, entity: CatalogEntity) -> ArenaIdx {
        let idx = ArenaIdx(self.arena.len());
        self.arena.push(entity);
        idx
    }
}
