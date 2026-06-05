/// The state of an entity in the catalog.
///
/// When reading the catalog from DuckLake, all entities are in the `Existing` state.
/// When modifying the catalog in a transaction, new entities are created as `Pending` to indicate
/// that they exist locally without an entity ID assigned yet. Entities that are deleted within the
/// transaction are marked as `Deleted` to keep the information around until committing the
/// changes.
#[derive(Debug, Clone, Copy)]
pub(in crate::catalog) enum CatalogState {
    Pending,
    Existing { id: i64 },
    Deleted { id: i64 },
}

impl CatalogState {
    /// The ID of the entity, if it exists.
    pub fn id(&self) -> Option<i64> {
        match self {
            CatalogState::Existing { id } | CatalogState::Deleted { id } => Some(*id),
            CatalogState::Pending => None,
        }
    }
}
