use std::collections::HashMap;
use std::ops::Deref;

use super::TryIntoRef;
use crate::catalog::{
    ArenaIdx,
    Catalog,
    CatalogEntity,
    CatalogTable,
    CatalogTablePartition,
    ColumnRef,
    TableRef,
};
use crate::{DucklakeError, DucklakeResult, db, io};

pub struct TableView<'a, C = &'a Catalog> {
    pub(super) catalog: C,
    pub(super) arena_idx: ArenaIdx,
    _marker: std::marker::PhantomData<&'a ()>,
}

pub type TableViewMut<'a> = TableView<'a, &'a mut Catalog>;

/* --------------------------------------------------------------------------------------------- */
/*                                              INIT                                             */
/* --------------------------------------------------------------------------------------------- */

impl<'a, C: Deref<Target = Catalog>> TableView<'a, C> {
    fn new(catalog: C, table_ref: TableRef) -> Self {
        Self {
            catalog,
            arena_idx: table_ref.0,
            _marker: std::marker::PhantomData,
        }
    }
}

impl Catalog {
    pub fn table<R: TryIntoRef<TableRef>>(&self, table_ref: R) -> Result<TableView<'_>, R::Error> {
        let table_ref = table_ref.try_into_ref(self)?;
        Ok(TableView::new(self, table_ref))
    }

    pub fn table_mut<R: TryIntoRef<TableRef>>(
        &mut self,
        table_ref: R,
    ) -> Result<TableViewMut<'_>, R::Error> {
        let table_ref = table_ref.try_into_ref(self)?;
        Ok(TableViewMut::new(self, table_ref))
    }
}

/* ------------------------------------------ INTO REF ----------------------------------------- */

impl TryIntoRef<TableRef> for &crate::TableName {
    type Error = DucklakeError;

    fn try_into_ref(self, catalog: &Catalog) -> Result<TableRef, Self::Error> {
        let idx = *catalog
            .schema(&self.schema)?
            .inner()
            .tables
            .get(&self.name)
            .ok_or(DucklakeError::table_not_found(self))?;
        Ok(idx.into())
    }
}

impl TryIntoRef<TableRef> for i64 {
    type Error = DucklakeError;

    fn try_into_ref(self, catalog: &Catalog) -> Result<TableRef, Self::Error> {
        let idx = *catalog
            .by_id
            .get(&self)
            .ok_or(DucklakeError::EntityNotFound { id: self })?;
        Ok(idx.into())
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                          READ & WRITE                                         */
/* --------------------------------------------------------------------------------------------- */

impl<'a, C: Deref<Target = Catalog>> TableView<'a, C> {
    pub(in crate::catalog) fn inner(&self) -> &CatalogTable {
        self.catalog.table_by_idx(self.arena_idx)
    }

    pub fn parent_schema(&self) -> super::schema::SchemaView<'_> {
        self.catalog.schema(&self.name().schema).unwrap()
    }
}

impl<'a> TableViewMut<'a> {
    pub(in crate::catalog) fn inner_mut(&mut self) -> &mut CatalogTable {
        self.catalog.table_by_idx_mut(self.arena_idx)
    }

    pub fn parent_schema_mut(&mut self) -> super::schema::SchemaViewMut<'_> {
        let schema_view = self.catalog.schema(&self.name().schema).unwrap();
        self.catalog.schema_mut(schema_view.ref_()).unwrap()
    }
}

impl Catalog {
    pub(super) fn table_by_idx(&self, arena_idx: ArenaIdx) -> &CatalogTable {
        match &self.arena[arena_idx.0] {
            CatalogEntity::Table(table) => table,
            _ => unreachable!("arena index does not point to a table"),
        }
    }

    pub(super) fn table_by_idx_mut(&mut self, arena_idx: ArenaIdx) -> &mut CatalogTable {
        match &mut self.arena[arena_idx.0] {
            CatalogEntity::Table(table) => table,
            _ => unreachable!("arena index does not point to a table"),
        }
    }
}

/* ----------------------------------------- ACCESSORS ----------------------------------------- */

impl<'a, C: Deref<Target = Catalog>> TableView<'a, C> {
    pub fn ref_(&self) -> TableRef {
        self.arena_idx.into()
    }

    pub fn id(&self) -> Option<i64> {
        self.inner().id
    }

    pub fn partition_id(&self) -> Option<i64> {
        self.inner().partition.as_ref().and_then(|p| p.id)
    }

    pub fn info(&self) -> crate::TableInfo {
        crate::TableInfo {
            name: self.name().clone(),
            schema: self.schema(),
            partitioning: self.partitioning(),
            tags: self.tags(),
        }
    }

    pub fn name(&self) -> &crate::TableName {
        &self.inner().name
    }

    pub fn schema(&self) -> crate::Schema {
        crate::Schema::from(&self.inner().columns)
    }

    pub fn column_data_types(&self) -> HashMap<i64, crate::DataType> {
        HashMap::from(&self.inner().columns)
    }

    pub fn partitioning(&self) -> Option<crate::Partition> {
        let table = self.inner();
        table
            .partition
            .as_ref()
            .map(|p| p.into_partition(&table.columns))
    }

    pub fn data_path(&self, root_data_path: &io::DucklakePath) -> io::DucklakePath {
        let data_path = root_data_path.join(&self.parent_schema().inner().path);
        data_path.join(&self.inner().path)
    }

    pub fn tags(&self) -> Vec<crate::Tag> {
        self.inner().tags.clone()
    }
}

/* ------------------------------------------ MUTATION ----------------------------------------- */

impl<'a> TableViewMut<'a> {
    pub fn resolve_id(&mut self, id: i64) {
        let table = self.inner_mut();
        match table.id {
            None => {
                table.id = Some(id);
                self.catalog.by_id.insert(id, self.arena_idx);
            }
            _ => panic!("table ID must not be overwritten"),
        }
    }

    pub fn resolve_partition_id(&mut self, id: i64) {
        let table = self.inner_mut();
        let partition = table
            .partition
            .as_mut()
            .expect("table must have partition info to resolve partition ID");
        match partition.id {
            None => {
                partition.id = Some(id);
            }
            _ => panic!("partition ID must not be overwritten"),
        }
    }

    pub async fn ensure_next_column_id(&mut self, pool: &db::Pool) -> DucklakeResult<()> {
        if self.inner().columns.next_column_id.is_none() {
            let next_column_id = self
                .catalog
                .load_next_column_id(pool, self.id().unwrap())
                .await?;
            self.inner_mut().columns.next_column_id = Some(next_column_id);
        }
        Ok(())
    }

    pub fn rename(&mut self, new_name: &str) -> DucklakeResult<()> {
        let name = self.name().clone();

        // Ensure that the new name does not already exist
        let mut schema = self.catalog.schema_mut(&name.schema)?;
        let catalog_schema = schema.inner_mut();
        if catalog_schema.tables.contains_key(new_name) {
            return Err(DucklakeError::table_already_exists(&crate::TableName {
                schema: name.schema.clone(),
                name: new_name.to_string(),
            }));
        }

        // Rename the table in the schema's table mapping
        let arena_idx = catalog_schema.tables.remove(&name.name).unwrap();
        catalog_schema
            .tables
            .insert(new_name.to_string(), arena_idx);

        // Rename the table itself
        let table = self.inner_mut();
        table.name = crate::TableName {
            schema: table.name.schema.clone(),
            name: new_name.to_string(),
        };
        Ok(())
    }

    pub fn add_column(
        &mut self,
        path: &[String],
        column: crate::Column,
    ) -> DucklakeResult<(Option<ColumnRef>, Vec<ColumnRef>)> {
        let parent_idx = if !path.is_empty() {
            Some(self.column(path)?.ref_().column_idx)
        } else {
            None
        };
        let table = self.inner_mut();
        let column_idxs = table.columns.add_column(parent_idx, column)?;
        let parent_ref = parent_idx.map(|idx| (self.arena_idx, idx).into());
        let column_refs = column_idxs
            .into_iter()
            .map(|idx| (self.arena_idx, idx).into())
            .collect();
        Ok((parent_ref, column_refs))
    }

    pub fn update_partitioning(
        &mut self,
        partitioning: Option<crate::Partition>,
    ) -> DucklakeResult<Option<Vec<ColumnRef>>> {
        let arena_idx = self.arena_idx;
        let table = self.inner_mut();

        // Set the new partitioning
        table.partition = partitioning
            .map(|p| CatalogTablePartition::from_partition(p, &table.columns))
            .transpose()?;

        // Derive the partition's column refs
        let partition_refs = table.partition.as_ref().map(|p| {
            p.columns
                .iter()
                .map(|col| (arena_idx, col.column).into())
                .collect()
        });
        Ok(partition_refs)
    }

    pub fn add_tag(&mut self, tag: crate::Tag) {
        let tags = &mut self.inner_mut().tags;
        super::upsert_tag(tags, tag);
    }

    pub fn remove_tag(&mut self, key: &str) -> DucklakeResult<()> {
        let tags = &mut self.inner_mut().tags;
        super::remove_tag(tags, key)?;
        Ok(())
    }

    /// Delete the table with the given identifier.
    pub fn delete(&mut self) {
        let table = self.inner_mut();
        let name = table.name.name.clone();
        if let Some(id) = table.id {
            self.catalog.by_id.remove(&id);
        }
        self.parent_schema_mut().inner_mut().tables.remove(&name);
    }
}
