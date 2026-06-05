use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::hash_map::Entry;

use crate::caches::{SnapshotInfo, TableStats};
use crate::catalog::{Catalog, ColumnRef, SchemaRef, TableRef};
use crate::spec::DucklakeSnapshot;
use crate::{DucklakeResult, db};

pub struct CommitState<'a> {
    snapshot_id: i64,
    schema_version: i64,
    next_catalog_id: i64,
    next_file_id: i64,
    catalog: Cow<'a, Catalog>,
    // Map from table ID to next column ID. Only populated on demand.
    next_column_ids: HashMap<i64, i64>,
    // Map from table ID to table stats. Populated only if provided in input.
    table_stats: Option<HashMap<i64, TableStats>>,
}

impl<'a> CommitState<'a> {
    pub fn new(
        snapshot_info: &SnapshotInfo,
        catalog: Cow<'a, Catalog>,
        schema_changed: bool,
        table_stats: Option<HashMap<i64, TableStats>>,
    ) -> Self {
        Self {
            snapshot_id: snapshot_info.id + 1,
            schema_version: snapshot_info.schema_version + (schema_changed as i64),
            next_catalog_id: snapshot_info.next_catalog_id,
            next_file_id: snapshot_info.next_file_id,
            catalog,
            next_column_ids: HashMap::new(),
            table_stats,
        }
    }
}

/* -------------------------------------- CHANGE-LEVEL API ------------------------------------- */

impl<'a> CommitState<'a> {
    /// Obtain the ID for the schema with the provided name. If the schema does not yet
    /// exist, a new ID is generated from the catalog ID sequence.
    pub fn schema_id(&mut self, schema_ref: SchemaRef) -> i64 {
        if let Some(id) = self.catalog.schema(schema_ref).into_ok().id() {
            return id;
        }
        let id = self.catalog_id();
        let Ok(mut schema) = self.catalog.to_mut().schema_mut(schema_ref);
        schema.resolve_id(id);
        id
    }

    /// Obtain the ID for the table with the provided identifier. If the table does not yet
    /// exist, a new ID is generated from the catalog ID sequence.
    pub fn table_id(&mut self, table_ref: TableRef) -> i64 {
        if let Some(id) = self.catalog.table(table_ref).into_ok().id() {
            return id;
        }
        let id = self.catalog_id();
        let Ok(mut table) = self.catalog.to_mut().table_mut(table_ref);
        table.resolve_id(id);
        id
    }

    /// Obtain the IDs for the column with the provided reference.
    pub fn column_id(&mut self, column_ref: ColumnRef) -> i64 {
        let Ok(table) = self.catalog.table(column_ref.table_ref);
        if let Some(id) = table.column(column_ref).into_ok().id() {
            return id;
        }
        let table_id = table
            .id()
            .expect("table ID must be set before resolving column IDs");
        let column_id = self.next_column_id(table_id);
        let Ok(mut table) = self.catalog.to_mut().table_mut(column_ref.table_ref);
        table.column_mut(column_ref).into_ok().resolve_id(column_id);
        column_id
    }

    /// Obtain the ID for a partition within the table with the provided ID. If the partition does
    /// not yet exist, a new ID is generated from the catalog ID sequence. This panics if the
    /// partition has not been defined for the table (i.e. is neither existing nor pending nor
    /// deleted).
    pub fn partition_id(&mut self, table_ref: TableRef) -> i64 {
        if let Some(id) = self.catalog.table(table_ref).into_ok().partition_id() {
            return id;
        }
        let id = self.catalog_id();
        let Ok(mut table) = self.catalog.to_mut().table_mut(table_ref);
        table.resolve_partition_id(id);
        id
    }

    /// Set the next column ID for the specified table ID, if it is not already set.
    /// If it is not set, the provided future is awaited to "initialize" the ID.
    pub async fn ensure_next_column_id_set(
        &mut self,
        table_id: i64,
        fetch_id: impl Future<Output = DucklakeResult<i64>>,
    ) -> DucklakeResult<()> {
        if let Entry::Vacant(entry) = self.next_column_ids.entry(table_id) {
            let id = fetch_id.await?;
            entry.insert(id);
        }
        Ok(())
    }

    /// Obtain the table stats for the specified table ID. If the table stats have not been
    /// queried from the database, they are fetched dynamically. If no stats exist for the table,
    /// a new, empty stats object is created and returned.
    pub async fn table_stats(&mut self, table_id: i64) -> DucklakeResult<&mut TableStats> {
        let stats = self
            .table_stats
            .as_mut()
            .expect("table stats are unset but requested");
        Ok(stats.entry(table_id).or_default())
    }
}

/* ---------------------------------------- STATE ACCESS --------------------------------------- */

impl<'a> CommitState<'a> {
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    pub fn schema_version(&self) -> i64 {
        self.schema_version
    }

    pub fn file_id(&mut self) -> i64 {
        let file_id = self.next_file_id;
        self.next_file_id += 1;
        file_id
    }

    pub fn table_schema(&self, table_ref: TableRef) -> crate::Schema {
        self.catalog.table(table_ref).into_ok().schema()
    }

    fn catalog_id(&mut self) -> i64 {
        let catalog_id = self.next_catalog_id;
        self.next_catalog_id += 1;
        catalog_id
    }

    fn next_column_id(&mut self, table_id: i64) -> i64 {
        let column_id = self
            .next_column_ids
            .get_mut(&table_id)
            .expect("next column ID must be set for table");
        let next_id = *column_id;
        *column_id += 1;
        next_id
    }
}

/* ------------------------------------------ SNAPSHOT ----------------------------------------- */

impl<'a> From<&CommitState<'a>> for DucklakeSnapshot {
    fn from(metadata: &CommitState<'a>) -> Self {
        Self {
            snapshot_id: metadata.snapshot_id,
            snapshot_time: db::UtcDateTime::now(),
            schema_version: metadata.schema_version,
            next_catalog_id: metadata.next_catalog_id,
            next_file_id: metadata.next_file_id,
        }
    }
}

// impl<'a> From<&CommitState<'a>> for SnapshotInfo {
//     fn from(metadata: &CommitState<'a>) -> Self {
//         Self {
//             id: metadata.snapshot_id,
//             schema_version: metadata.schema_version,
//             next_catalog_id: metadata.next_catalog_id,
//             next_file_id: metadata.next_file_id,
//             snapshot_time: chrono::Utc::now(),
//         }
//     }
// }
