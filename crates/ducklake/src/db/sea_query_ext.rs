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

/// Simple trait to simplify the insertion of rows into ducklake tables.
pub trait InsertIntoTable {
    fn insert_entity(entity: impl InsertableEntity) -> sea_query::InsertStatement;
    fn insert_entities<E: InsertableEntity>(
        entities: impl IntoIterator<Item = E>,
    ) -> sea_query::InsertStatement;
}

pub trait InsertableEntity {
    fn insert_into_table(&self) -> sea_query::InsertStatement;
    fn insert_all_into_table(
        entities: impl IntoIterator<Item = Self>,
    ) -> sea_query::InsertStatement;
}

impl InsertIntoTable for sea_query::Query {
    fn insert_entity(entity: impl InsertableEntity) -> sea_query::InsertStatement {
        entity.insert_into_table()
    }

    fn insert_entities<E: InsertableEntity>(
        entities: impl IntoIterator<Item = E>,
    ) -> sea_query::InsertStatement {
        E::insert_all_into_table(entities)
    }
}
