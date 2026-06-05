use crate::catalog::Catalog;

mod column;
mod schema;
mod table;

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
