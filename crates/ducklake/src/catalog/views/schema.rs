use std::ops::Deref;

use super::TryIntoRef;
use crate::catalog::{ArenaIdx, Catalog, CatalogEntity, CatalogSchema, CatalogState, SchemaRef};
use crate::{DucklakeError, DucklakeResult};

pub struct SchemaView<'a, C = &'a Catalog> {
    catalog: C,
    arena_idx: ArenaIdx,
    _marker: std::marker::PhantomData<&'a ()>,
}

pub type SchemaViewMut<'a> = SchemaView<'a, &'a mut Catalog>;

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
    pub fn schema<R: TryIntoRef<SchemaRef>>(
        &self,
        schema_ref: R,
    ) -> Result<SchemaView<'_>, R::Error> {
        let schema_ref = schema_ref.try_into_ref(self)?;
        Ok(SchemaView::new(self, schema_ref))
    }

    pub fn schema_mut<R: TryIntoRef<SchemaRef>>(
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

impl<'a, C: Deref<Target = Catalog>> SchemaView<'a, C> {
    pub(in crate::catalog) fn inner(&self) -> &CatalogSchema {
        match &self.catalog.arena[self.arena_idx.0] {
            CatalogEntity::Schema(schema) => schema,
            _ => unreachable!("arena index does not point to a schema"),
        }
    }
}

impl<'a> SchemaViewMut<'a> {
    pub(in crate::catalog) fn inner_mut(&mut self) -> &mut CatalogSchema {
        match &mut self.catalog.arena[self.arena_idx.0] {
            CatalogEntity::Schema(schema) => schema,
            _ => unreachable!("arena index does not point to a schema"),
        }
    }
}

/* ----------------------------------------- ACCESSORS ----------------------------------------- */

impl<'a, C: Deref<Target = Catalog>> SchemaView<'a, C> {
    pub fn ref_(&self) -> SchemaRef {
        SchemaRef(self.arena_idx)
    }

    pub fn id(&self) -> Option<i64> {
        self.inner().state.id()
    }

    pub fn name(&self) -> &str {
        &self.inner().name
    }
}

/* ------------------------------------------ MUTATION ----------------------------------------- */

impl<'a> SchemaViewMut<'a> {
    pub fn resolve_id(&mut self, id: i64) {
        let schema = self.inner_mut();
        match schema.state {
            CatalogState::Pending => {
                schema.state = CatalogState::Existing { id };
                self.catalog.by_id.insert(id, self.arena_idx);
            }
            _ => panic!("schema must be in state 'pending' to set ID"),
        }
    }

    pub fn delete(&mut self) -> DucklakeResult<()> {
        let schema = self.inner_mut();
        if !schema.tables.is_empty() {
            return Err(DucklakeError::InvalidChanges(format!(
                "cannot delete schema {} which is not empty",
                schema.name
            )));
        }

        // Depending on the current state, either mark deleted or raise an error
        match &schema.state {
            CatalogState::Existing { id } => {
                schema.state = CatalogState::Deleted { id: *id };
                Ok(())
            }
            CatalogState::Pending => Err(DucklakeError::InvalidChanges(format!(
                "cannot delete schema {} which was created in the same transaction",
                schema.name
            ))),
            CatalogState::Deleted { .. } => Err(DucklakeError::schema_not_found(&schema.name)),
        }
    }
}
