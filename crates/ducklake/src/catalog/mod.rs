use std::collections::HashMap;

use crate::{DucklakeError, DucklakeResult, io};

mod arena;
mod load;
mod refs;
mod typedefs;
mod views;

use arena::{Arena, ArenaIdx};
pub(crate) use refs::{ColumnRef, SchemaRef, TableRef};
use typedefs::*;
pub(crate) use views::SchemaView;

/// Point-in-time capture of the DuckLake schema. This includes all schemas, tables, their
/// columns, etc.
///
/// The catalog may be altered within a transaction, hence, this type exposes numerous methods to
/// modify the catalog. Entities (i.e. schemas/tables) which are created "locally" (i.e. within
/// the transaction) are not immediately assigned an ID. Hence, they can only be referred to by
/// their name as opposed to an entity ID.
#[derive(Debug, Clone)]
pub(crate) struct Catalog {
    // Storage of schemas.
    schema_arena: Arena<CatalogSchema>,
    // Storage of tables across schemas.
    table_arena: Arena<CatalogTable>,
    // Mapping from schema name to arena index for quick lookup by schema name.
    schemas: HashMap<String, ArenaIdx>,
}

/* ------------------------------------------- SCHEMA ------------------------------------------ */

impl Catalog {
    /// Insert a new schema with the given name as a pending schema.
    ///
    /// This method returns a reference to the newly created schema and errors if the schema
    /// already exists.
    pub(crate) fn add_schema(
        &mut self,
        name: &str,
        path: io::DucklakePath,
    ) -> DucklakeResult<SchemaRef> {
        // If the schema exists already, we need to raise some kind of error
        if self.schema(name).is_ok() {
            return Err(DucklakeError::schema_already_exists(name));
        }

        // If the schema does not yet exist, create a new pending schema
        let schema = CatalogSchema {
            id: None,
            name: name.to_string(),
            tables: HashMap::new(),
            path,
        };
        let idx = self.schema_arena.push(schema, None);
        self.schemas.insert(name.to_string(), idx);
        Ok(idx.into())
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
    pub(crate) fn add_table(
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
            return Err(DucklakeError::table_already_exists(table.name()));
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
            id: None,
            name: table.name.clone(),
            columns,
            partition: partition.clone(),
            tags: table.tags,
            path,
        };
        let column_idxs = catalog_table.columns.root_column_indices();
        let table_idx = self.table_arena.push(catalog_table, None);

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
}
