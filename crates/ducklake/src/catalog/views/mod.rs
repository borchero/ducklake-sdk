use crate::catalog::Catalog;
use crate::{DucklakeError, DucklakeResult};

mod column;
mod macro_;
mod schema;
mod table;
mod view;

pub(crate) use macro_::MacroView;
pub(crate) use schema::SchemaView;
pub(crate) use table::TableView;
pub(crate) use view::ViewView;

/* ---------------------------------------- TRY INTO REF --------------------------------------- */

pub(crate) trait TryIntoRef<Ref, Container = Catalog> {
    type Error: std::error::Error;

    fn try_into_ref(self, container: &Container) -> Result<Ref, Self::Error>;
}

impl<Ref: Copy, Container> TryIntoRef<Ref, Container> for Ref {
    type Error = !;

    fn try_into_ref(self, _container: &Container) -> Result<Ref, Self::Error> {
        Ok(self)
    }
}

/* ------------------------------------------- UTILS ------------------------------------------- */

fn upsert_tag(tags: &mut crate::Tags, tag: crate::Tag) {
    tags.insert(tag);
}

fn remove_tag(tags: &mut crate::Tags, key: &str) -> DucklakeResult<()> {
    if tags.remove(key).is_none() {
        return Err(DucklakeError::NotFound {
            entity: "tag",
            name: key.to_string(),
        });
    }
    Ok(())
}
