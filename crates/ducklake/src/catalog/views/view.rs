use std::ops::Deref;

use super::TryIntoRef;
use crate::DucklakeError;
use crate::catalog::{ArenaIdx, Catalog, CatalogView, ViewRef};

pub(crate) struct ViewView<'a, C = &'a Catalog> {
    pub(super) catalog: C,
    pub(super) arena_idx: ArenaIdx,
    _marker: std::marker::PhantomData<&'a ()>,
}

pub(crate) type ViewViewMut<'a> = ViewView<'a, &'a mut Catalog>;

/* --------------------------------------------------------------------------------------------- */
/*                                              INIT                                             */
/* --------------------------------------------------------------------------------------------- */

impl<'a, C: Deref<Target = Catalog>> ViewView<'a, C> {
    pub(super) fn new(catalog: C, view_ref: ViewRef) -> Self {
        Self {
            catalog,
            arena_idx: view_ref.0,
            _marker: std::marker::PhantomData,
        }
    }
}

impl Catalog {
    pub(crate) fn view<R: TryIntoRef<ViewRef>>(
        &self,
        view_ref: R,
    ) -> Result<ViewView<'_>, R::Error> {
        let view_ref = view_ref.try_into_ref(self)?;
        Ok(ViewView::new(self, view_ref))
    }

    pub(crate) fn view_mut<R: TryIntoRef<ViewRef>>(
        &mut self,
        view_ref: R,
    ) -> Result<ViewViewMut<'_>, R::Error> {
        let view_ref = view_ref.try_into_ref(self)?;
        Ok(ViewViewMut::new(self, view_ref))
    }
}

/* ------------------------------------------ INTO REF ----------------------------------------- */

impl TryIntoRef<ViewRef> for &crate::TableName {
    type Error = DucklakeError;

    fn try_into_ref(self, catalog: &Catalog) -> Result<ViewRef, Self::Error> {
        let idx = *catalog
            .schema(&self.schema)?
            .inner()
            .views
            .get(&self.name)
            .ok_or(DucklakeError::view_not_found(self))?;
        Ok(idx.into())
    }
}

impl TryIntoRef<ViewRef> for i64 {
    type Error = DucklakeError;

    fn try_into_ref(self, catalog: &Catalog) -> Result<ViewRef, Self::Error> {
        let idx = catalog
            .view_arena
            .map_id(self)
            .ok_or(DucklakeError::EntityNotFound { id: self })?;
        Ok(idx.into())
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                          READ & WRITE                                         */
/* --------------------------------------------------------------------------------------------- */

impl<'a, C: Deref<Target = Catalog>> ViewView<'a, C> {
    pub(in crate::catalog) fn inner(&self) -> &CatalogView {
        self.catalog.view_by_idx(self.arena_idx)
    }
}

impl<'a> ViewViewMut<'a> {
    pub(in crate::catalog) fn inner_mut(&mut self) -> &mut CatalogView {
        self.catalog.view_by_idx_mut(self.arena_idx)
    }

    pub(crate) fn parent_schema_mut(&mut self) -> super::schema::SchemaViewMut<'_> {
        let schema_view = self.catalog.schema(&self.name().schema).unwrap();
        self.catalog.schema_mut(schema_view.ref_()).unwrap()
    }
}

impl Catalog {
    pub(super) fn view_by_idx(&self, arena_idx: ArenaIdx) -> &CatalogView {
        self.view_arena.get(arena_idx)
    }

    pub(super) fn view_by_idx_mut(&mut self, arena_idx: ArenaIdx) -> &mut CatalogView {
        self.view_arena.get_mut(arena_idx)
    }
}

/* ----------------------------------------- ACCESSORS ----------------------------------------- */

impl<'a, C: Deref<Target = Catalog>> ViewView<'a, C> {
    pub(crate) fn ref_(&self) -> ViewRef {
        self.arena_idx.into()
    }

    pub(crate) fn id(&self) -> Option<i64> {
        self.inner().id
    }

    pub(crate) fn name(&self) -> &crate::TableName {
        &self.inner().name
    }

    pub(crate) fn sql(&self) -> &str {
        &self.inner().sql
    }

    pub(crate) fn dialect(&self) -> &str {
        &self.inner().dialect
    }

    pub(crate) fn column_aliases(&self) -> Option<Vec<String>> {
        self.inner().column_aliases.clone()
    }

    pub(crate) fn tags(&self) -> Vec<crate::Tag> {
        self.inner().tags.clone()
    }
}

/* ------------------------------------------ MUTATION ----------------------------------------- */

impl<'a> ViewViewMut<'a> {
    pub(crate) fn resolve_id(&mut self, id: i64) {
        let view = self.inner_mut();
        match view.id {
            None => {
                view.id = Some(id);
                self.catalog.view_arena.register_id(self.arena_idx, id);
            }
            _ => panic!("view ID must not be overwritten"),
        }
    }

    /// Delete the view.
    pub(crate) fn delete(&mut self) {
        let view = self.inner_mut();
        let name = view.name.name.clone();
        self.parent_schema_mut().inner_mut().views.remove(&name);
    }
}
