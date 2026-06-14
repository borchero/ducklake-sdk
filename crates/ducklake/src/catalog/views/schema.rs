use std::ops::Deref;

use super::TryIntoRef;
use crate::catalog::{ArenaIdx, Catalog, CatalogSchema, SchemaRef};
use crate::{DucklakeError, DucklakeResult};

pub(crate) struct SchemaView<'a, C = &'a Catalog> {
    catalog: C,
    arena_idx: ArenaIdx,
    _marker: std::marker::PhantomData<&'a ()>,
}

pub(crate) type SchemaViewMut<'a> = SchemaView<'a, &'a mut Catalog>;

/* --------------------------------------------------------------------------------------------- */
/*                                              INIT                                             */
/* --------------------------------------------------------------------------------------------- */

impl<'a, C: Deref<Target = Catalog>> SchemaView<'a, C> {
    fn new(catalog: C, schema_ref: SchemaRef) -> Self {
        Self {
            catalog,
            arena_idx: schema_ref.0,
            _marker: std::marker::PhantomData,
        }
    }
}

impl Catalog {
    pub(crate) fn list_schemas(&self) -> Vec<SchemaView<'_>> {
        self.schemas
            .values()
            .map(|arena_idx| SchemaView::new(self, (*arena_idx).into()))
            .collect()
    }

    pub(crate) fn schema<R: TryIntoRef<SchemaRef>>(
        &self,
        schema_ref: R,
    ) -> Result<SchemaView<'_>, R::Error> {
        let schema_ref = schema_ref.try_into_ref(self)?;
        Ok(SchemaView::new(self, schema_ref))
    }

    pub(crate) fn schema_mut<R: TryIntoRef<SchemaRef>>(
        &mut self,
        schema_ref: R,
    ) -> Result<SchemaViewMut<'_>, R::Error> {
        let schema_ref = schema_ref.try_into_ref(self)?;
        Ok(SchemaViewMut::new(self, schema_ref))
    }
}

/* ------------------------------------------ INTO REF ----------------------------------------- */

impl<S: AsRef<str> + ?Sized> TryIntoRef<SchemaRef> for &S {
    type Error = DucklakeError;

    fn try_into_ref(self, catalog: &Catalog) -> Result<SchemaRef, Self::Error> {
        let idx = *catalog
            .schemas
            .get(self.as_ref())
            .ok_or(DucklakeError::schema_not_found(self.as_ref()))?;
        Ok(idx.into())
    }
}

impl TryIntoRef<SchemaRef> for i64 {
    type Error = DucklakeError;

    fn try_into_ref(self, catalog: &Catalog) -> Result<SchemaRef, Self::Error> {
        let idx = catalog
            .schema_arena
            .map_id(self)
            .ok_or(DucklakeError::EntityNotFound { id: self })?;
        Ok(idx.into())
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                          READ & WRITE                                         */
/* --------------------------------------------------------------------------------------------- */

impl<'a, C: Deref<Target = Catalog>> SchemaView<'a, C> {
    pub(in crate::catalog) fn inner(&self) -> &CatalogSchema {
        self.catalog.schema_arena.get(self.arena_idx)
    }
}

impl<'a> SchemaViewMut<'a> {
    pub(in crate::catalog) fn inner_mut(&mut self) -> &mut CatalogSchema {
        self.catalog.schema_arena.get_mut(self.arena_idx)
    }
}

/* ----------------------------------------- ACCESSORS ----------------------------------------- */

impl<'a, C: Deref<Target = Catalog>> SchemaView<'a, C> {
    pub(crate) fn ref_(&self) -> SchemaRef {
        SchemaRef(self.arena_idx)
    }

    pub(crate) fn id(&self) -> Option<i64> {
        self.inner().id
    }

    pub(crate) fn name(&self) -> &str {
        &self.inner().name
    }

    pub(crate) fn list_tables(&self) -> Vec<super::TableView<'_>> {
        let catalog: &Catalog = &self.catalog;
        self.inner()
            .tables
            .values()
            .map(|arena_idx| super::TableView::new(catalog, (*arena_idx).into()))
            .collect()
    }
}

/* ------------------------------------------ MUTATION ----------------------------------------- */

impl<'a> SchemaViewMut<'a> {
    pub(crate) fn resolve_id(&mut self, id: i64) {
        let schema = self.inner_mut();
        match schema.id {
            None => {
                schema.id = Some(id);
                self.catalog.schema_arena.register_id(self.arena_idx, id);
            }
            _ => panic!("schema ID must not be overwritten"),
        }
    }

    pub(crate) fn delete(&mut self) -> DucklakeResult<()> {
        let schema = self.inner_mut();
        if !schema.tables.is_empty() {
            return Err(DucklakeError::InvalidChanges(format!(
                "cannot delete schema {} which is not empty",
                schema.name
            )));
        }
        let name = schema.name.clone();
        self.catalog.schemas.remove(&name);
        Ok(())
    }
}
