/* ---------------------------------- CREATE TABLE ---------------------------------- */

use crate::db::Dialect;

/// Simple trait to simplify the creation of ducklake tables.
pub trait CreateTable {
    fn create_entity<E: CreatableEntity>(dialect: Dialect) -> sea_query::TableCreateStatement;
}

pub trait CreatableEntity {
    fn create_table(dialect: Dialect) -> sea_query::TableCreateStatement;
}

impl CreateTable for sea_query::Table {
    fn create_entity<E: CreatableEntity>(dialect: Dialect) -> sea_query::TableCreateStatement {
        E::create_table(dialect)
    }
}

/* ------------------------------------- INSERT ------------------------------------- */

pub trait InsertableEntity {
    /// The number of columns that are inserted for each entity. This is used to determine how
    /// many entities can be inserted within a single statement without exceeding the bind
    /// parameter limit of the underlying database backend.
    const NUM_COLUMNS: usize;

    fn insert_into_table(&self) -> sea_query::InsertStatement;
    fn insert_all_into_table(
        entities: impl IntoIterator<Item = Self>,
    ) -> sea_query::InsertStatement;
}
