use std::ops::Deref;

use super::TryIntoRef;
use crate::catalog::{ArenaIdx, Catalog, CatalogMacro, MacroRef};
use crate::{DucklakeError, MacroImplementation, MacroType, TableName};

pub(crate) struct MacroView<'a, C = &'a Catalog> {
    catalog: C,
    arena_idx: ArenaIdx,
    _marker: std::marker::PhantomData<&'a ()>,
}

pub(crate) type MacroViewMut<'a> = MacroView<'a, &'a mut Catalog>;

impl Catalog {
    pub(crate) fn macro_<R: TryIntoRef<MacroRef>>(
        &self,
        macro_ref: R,
    ) -> Result<MacroView<'_>, R::Error> {
        let macro_ref = macro_ref.try_into_ref(self)?;
        Ok(MacroView::new(self, macro_ref))
    }

    pub(crate) fn macro_mut<R: TryIntoRef<MacroRef>>(
        &mut self,
        macro_ref: R,
    ) -> Result<MacroViewMut<'_>, R::Error> {
        let macro_ref = macro_ref.try_into_ref(self)?;
        Ok(MacroViewMut::new(self, macro_ref))
    }
}

impl<'a, C: Deref<Target = Catalog>> MacroView<'a, C> {
    pub(crate) fn ref_(&self) -> MacroRef {
        MacroRef(self.arena_idx)
    }

    pub(crate) fn id(&self) -> Option<i64> {
        self.inner().id
    }

    pub(crate) fn name(&self) -> &TableName {
        &self.inner().name
    }

    pub(crate) fn macro_type(&self) -> MacroType {
        self.inner().macro_type
    }

    pub(crate) fn implementations(&self) -> Vec<MacroImplementation> {
        self.inner().implementations.clone()
    }
}

impl<'a> MacroViewMut<'a> {
    pub(crate) fn resolve_id(&mut self, id: i64) {
        let macro_ = self.inner_mut();
        match macro_.id {
            None => {
                macro_.id = Some(id);
                self.catalog.macro_arena.register_id(self.arena_idx, id);
            }
            _ => panic!("macro ID must not be overwritten"),
        }
    }

    pub(crate) fn delete(&mut self) {
        let name = self.inner().name.clone();
        let macro_type = self.inner().macro_type;
        self.catalog
            .schema_mut(&name.schema)
            .expect("macro schema must exist")
            .inner_mut()
            .macros
            .remove(&(name.name.to_ascii_lowercase(), macro_type));
    }
}
impl<'a, C: Deref<Target = Catalog>> MacroView<'a, C> {
    pub(super) fn new(catalog: C, macro_ref: MacroRef) -> Self {
        Self {
            catalog,
            arena_idx: macro_ref.0,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'a, C: Deref<Target = Catalog>> MacroView<'a, C> {
    pub(in crate::catalog) fn inner(&self) -> &CatalogMacro {
        self.catalog.macro_arena.get(self.arena_idx)
    }
}

impl<'a> MacroViewMut<'a> {
    pub(in crate::catalog) fn inner_mut(&mut self) -> &mut CatalogMacro {
        self.catalog.macro_arena.get_mut(self.arena_idx)
    }
}

impl TryIntoRef<MacroRef> for (&TableName, MacroType) {
    type Error = DucklakeError;

    fn try_into_ref(self, catalog: &Catalog) -> Result<MacroRef, Self::Error> {
        let (name, macro_type) = self;
        let schema = catalog.schema(&name.schema)?;
        let idx = *schema
            .inner()
            .macros
            .get(&(name.name.to_ascii_lowercase(), macro_type))
            .ok_or_else(|| DucklakeError::macro_not_found(name))?;
        Ok(idx.into())
    }
}

impl TryIntoRef<MacroRef> for i64 {
    type Error = DucklakeError;

    fn try_into_ref(self, catalog: &Catalog) -> Result<MacroRef, Self::Error> {
        let idx = catalog
            .macro_arena
            .map_id(self)
            .ok_or(DucklakeError::EntityNotFound { id: self })?;
        Ok(idx.into())
    }
}
