use crate::catalog::Catalog;
use crate::{DucklakeError, DucklakeResult};

mod column;
mod schema;
mod table;

pub(crate) use schema::SchemaView;
pub(crate) use table::TableView;

/* ---------------------------------------- TRY INTO REF --------------------------------------- */

pub trait TryIntoRef<Ref, Container = Catalog> {
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

fn upsert_tag(tags: &mut Vec<crate::Tag>, tag: crate::Tag) {
    tags.retain(|t| t.key != tag.key);
    tags.push(tag);
}

fn remove_tag(tags: &mut Vec<crate::Tag>, key: &str) -> DucklakeResult<()> {
    if tags.extract_if(.., |t| t.key == key).count() == 0 {
        return Err(DucklakeError::NotFound {
            entity: "tag",
            name: key.to_string(),
        });
    }
    Ok(())
}
