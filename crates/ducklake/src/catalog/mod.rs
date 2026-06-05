use std::collections::HashMap;

use crate::{DucklakeError, DucklakeResult, io};

mod load;
mod refs;
mod typedefs;
mod views;
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

/* ------------------------------------------- SCHEMA ------------------------------------------ */

impl Catalog {
    /// Insert a new schema with the given name as a pending schema.
    ///
    /// This method returns a reference to the newly created schema and errors if the schema
    /// already exists.
    pub fn add_schema(&mut self, name: &str, path: io::DucklakePath) -> DucklakeResult<SchemaRef> {
        // If the schema exists already, we need to raise some kind of error
        if let Ok(schema) = self.schema(name) {
            return match &schema.inner().state {
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

    /// Get the names of all schemas.
    pub fn list_schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
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
        if let Ok(table) = self.table(&table.name) {
            return match table.inner().state {
                CatalogState::Existing { .. } | CatalogState::Pending => {
                    Err(DucklakeError::table_already_exists(table.name()))
                }
                CatalogState::Deleted { .. } => Err(DucklakeError::InvalidChanges(format!(
                    "cannot create table {} which was deleted in the same transaction",
                    table.name()
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

        let mut schema = self.schema_mut(&table.name.schema)?;
        let catalog_schema = schema.inner_mut();
        catalog_schema
            .tables
            .insert(table.name.name.clone(), table_idx);

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
        Ok((schema.ref_(), table_idx.into(), column_refs, partition_refs))
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
