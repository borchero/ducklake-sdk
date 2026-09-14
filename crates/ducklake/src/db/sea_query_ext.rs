/* ---------------------------------- CREATE TABLE ---------------------------------- */

use crate::db::Dialect;

/// Simple trait to simplify the creation of ducklake tables.
pub(crate) trait CreateTable {
    fn create_entity<E: CreatableEntity>(dialect: Dialect) -> sea_query::TableCreateStatement;
}

pub(crate) trait CreatableEntity {
    fn create_table(dialect: Dialect) -> sea_query::TableCreateStatement;
}

impl CreateTable for sea_query::Table {
    fn create_entity<E: CreatableEntity>(dialect: Dialect) -> sea_query::TableCreateStatement {
        E::create_table(dialect)
    }
}

/* ------------------------------------- INSERT ------------------------------------- */

/// An entity's backing table, columns, and values, shared by direct and buffered inserts.
pub(crate) trait InsertableEntity: Sized {
    const TABLE: &'static str;
    const COLUMNS: &'static [&'static str];

    fn into_values(self) -> Vec<sea_query::Value>;

    fn insert_into_table(self) -> sea_query::InsertStatement {
        Self::insert_all_into_table([self])
    }

    fn insert_all_into_table(
        entities: impl IntoIterator<Item = Self>,
    ) -> sea_query::InsertStatement {
        let mut query = sea_query::Query::insert();
        query
            .into_table(Self::TABLE)
            .columns(Self::COLUMNS.iter().copied());
        for entity in entities {
            query.values_panic(entity.into_values().into_iter().map(sea_query::Expr::val));
        }
        query
    }
}
