use std::ops::Deref;

use super::TryIntoRef;
use super::table::{TableView, TableViewMut};
use crate::catalog::typedefs::CatalogDataType;
use crate::catalog::{ArenaIdx, Catalog, CatalogColumn, CatalogTable, ColumnRef, TableRef};
use crate::{DucklakeError, DucklakeResult};

pub(crate) struct ColumnView<'a, C = &'a Catalog> {
    catalog: C,
    table_ref: TableRef,
    column_arena_idx: ArenaIdx,
    _marker: std::marker::PhantomData<&'a ()>,
}

pub(crate) type ColumnViewMut<'a> = ColumnView<'a, &'a mut Catalog>;

/* --------------------------------------------------------------------------------------------- */
/*                                              INIT                                             */
/* --------------------------------------------------------------------------------------------- */

impl<'a, C: Deref<Target = Catalog>> ColumnView<'a, C> {
    fn new(catalog: C, table_ref: TableRef, column_arena_idx: ArenaIdx) -> Self {
        Self {
            catalog,
            table_ref,
            column_arena_idx,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'a, C: Deref<Target = Catalog>> TableView<'a, C> {
    pub(crate) fn column<R: TryIntoRef<ColumnRef, TableView<'a, C>>>(
        &self,
        column_ref: R,
    ) -> Result<ColumnView<'_, &Catalog>, R::Error> {
        let column_idx = column_ref.try_into_ref(self)?.column_idx;
        let view = ColumnView::new(&*self.catalog, self.arena_idx.into(), column_idx);
        Ok(view)
    }
}

impl<'a> TableViewMut<'a> {
    pub(crate) fn column_mut<R: TryIntoRef<ColumnRef, TableViewMut<'a>>>(
        &mut self,
        column_ref: R,
    ) -> Result<ColumnViewMut<'_>, R::Error> {
        let column_idx = column_ref.try_into_ref(self)?.column_idx;
        let view = ColumnView::new(&mut *self.catalog, self.arena_idx.into(), column_idx);
        Ok(view)
    }
}

/* ------------------------------------------ INTO REF ----------------------------------------- */

impl<'a, C: Deref<Target = Catalog>> TryIntoRef<ColumnRef, TableView<'a, C>> for &str {
    type Error = DucklakeError;

    fn try_into_ref(self, container: &TableView<'a, C>) -> Result<ColumnRef, Self::Error> {
        let into_ref: &[String] = &[self.to_string()];
        into_ref.try_into_ref(container)
    }
}

impl<'a, C: Deref<Target = Catalog>> TryIntoRef<ColumnRef, TableView<'a, C>> for &[String] {
    type Error = DucklakeError;

    fn try_into_ref(self, container: &TableView<'a, C>) -> Result<ColumnRef, Self::Error> {
        let columns = &container.inner().columns;
        let idx = columns.arena_idx_by_path(self)?;
        Ok(ColumnRef {
            table_ref: container.arena_idx.into(),
            column_idx: idx,
        })
    }
}

impl<'a, C: Deref<Target = Catalog>> TryIntoRef<ColumnRef, TableView<'a, C>> for i64 {
    type Error = DucklakeError;

    fn try_into_ref(self, container: &TableView<'a, C>) -> Result<ColumnRef, Self::Error> {
        let columns = &container.inner().columns;
        let idx = columns
            .by_id
            .get(&self)
            .ok_or_else(|| DucklakeError::ColumnNotFound { id: self })?;
        Ok(ColumnRef {
            table_ref: container.arena_idx.into(),
            column_idx: *idx,
        })
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                          READ & WRITE                                         */
/* --------------------------------------------------------------------------------------------- */

impl<'a, C: Deref<Target = Catalog>> ColumnView<'a, C> {
    fn table(&self) -> &CatalogTable {
        self.catalog.table_by_idx(self.table_ref.0)
    }

    pub(in crate::catalog) fn inner(&self) -> &CatalogColumn {
        &self.table().columns.arena[self.column_arena_idx.0]
    }
}

impl<'a> ColumnViewMut<'a> {
    fn table_mut(&mut self) -> &mut CatalogTable {
        self.catalog.table_by_idx_mut(self.table_ref.0)
    }

    pub(in crate::catalog) fn inner_mut(&mut self) -> &mut CatalogColumn {
        let idx = self.column_arena_idx.0;
        &mut self.table_mut().columns.arena[idx]
    }
}

/* ----------------------------------------- ACCESSORS ----------------------------------------- */

impl<'a, C: Deref<Target = Catalog>> ColumnView<'a, C> {
    pub(crate) fn ref_(&self) -> ColumnRef {
        ColumnRef {
            table_ref: self.table_ref,
            column_idx: self.column_arena_idx,
        }
    }

    pub(crate) fn id(&self) -> i64 {
        self.inner().id
    }

    pub(crate) fn info(&self) -> crate::Column {
        self.table()
            .columns
            .schema_column_from_arena_index(self.column_arena_idx)
    }

    pub(crate) fn nullable(&self) -> bool {
        self.inner().nullable
    }

    pub(crate) fn parent_ref(&self) -> Option<ColumnRef> {
        self.inner()
            .parent_column
            .as_ref()
            .map(|parent_idx| (self.table_ref.0, *parent_idx).into())
    }
}

/* ------------------------------------------ MUTATION ----------------------------------------- */

impl<'a> ColumnViewMut<'a> {
    pub(crate) fn update_primitive_data_type(&mut self, data_type: crate::DataType) {
        self.inner_mut().dtype = CatalogDataType::Primitive(data_type);
    }

    pub(crate) fn update_default_value(&mut self, default_value: crate::ColumnDefault) {
        self.inner_mut().default_value = default_value;
    }

    pub(crate) fn update_nullability(&mut self, nullable: bool) {
        self.inner_mut().nullable = nullable;
    }

    pub(crate) fn rename(&mut self, new_name: &str) -> DucklakeResult<()> {
        let column_idx = self.column_arena_idx;
        let table = self.table_mut();
        table.columns.rename_column(column_idx, new_name)?;
        Ok(())
    }

    pub(crate) fn add_tag(&mut self, tag: crate::Tag) {
        let tags = &mut self.inner_mut().tags;
        super::upsert_tag(tags, tag);
    }

    pub(crate) fn remove_tag(&mut self, key: &str) -> DucklakeResult<()> {
        let tags = &mut self.inner_mut().tags;
        super::remove_tag(tags, key)?;
        Ok(())
    }

    pub(crate) fn remove(&mut self) -> DucklakeResult<Vec<ColumnRef>> {
        let column_idx = self.column_arena_idx;
        let table = self.table_mut();
        if let Some(partition) = table.partition.as_ref()
            && partition.columns.iter().any(|col| col.column == column_idx)
        {
            return Err(DucklakeError::InvalidChanges(
                "cannot remove column from table as the table is partitioned by it - reset or change the partitioning on this table in order to drop this column".to_string(),
            ));
        }
        let column_idxs = table.columns.remove_column(column_idx)?;
        Ok(column_idxs
            .into_iter()
            .map(|column_idx| (self.table_ref.0, column_idx).into())
            .collect())
    }
}
