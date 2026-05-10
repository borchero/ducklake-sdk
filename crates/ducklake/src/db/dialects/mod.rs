#[cfg(feature = "mysql")]
mod mysql;
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

use sea_query::{
    ColumnType,
    DeleteStatement,
    Expr,
    InsertStatement,
    MysqlQueryBuilder,
    PostgresQueryBuilder,
    SelectStatement,
    SqliteQueryBuilder,
    TableAlterStatement,
    TableCreateStatement,
    TableRenameStatement,
    UpdateStatement,
};
use sea_query_sqlx::{SqlxBinder, SqlxValues};

use crate::DucklakeError;

#[derive(Clone, Copy)]
pub enum Dialect {
    #[cfg(feature = "postgres")]
    Postgres,
    #[cfg(feature = "mysql")]
    MySql,
    #[cfg(feature = "sqlite")]
    Sqlite,
}

impl Dialect {
    pub fn column_type_for_data_inlining(&self, data_type: &crate::DataType) -> ColumnType {
        match self {
            #[cfg(feature = "postgres")]
            Dialect::Postgres => postgres::column_type_for_data_type(data_type),
            #[cfg(feature = "mysql")]
            Dialect::MySql => unimplemented!("data inlining is not yet implemented for MySQL"),
            #[cfg(feature = "sqlite")]
            Dialect::Sqlite => sqlite::column_type_for_data_type(data_type),
        }
    }
}

/* ---------------------------------- DUCKDB TYPE EQUIVALENCE ---------------------------------- */
// The DuckDB extensions do not necessarily map data types to the same types as `sea_query` does.
// Since we want to be fully compatible with that the DuckDB DuckLake extension is doing though,
// we replicate the data types here. Equivalence is ensured through differential tests.

impl Dialect {
    pub fn column_type_string(&self) -> ColumnType {
        match self {
            #[cfg(feature = "mysql")]
            Dialect::MySql => ColumnType::Text,
            _ => ColumnType::string(None),
        }
    }

    pub fn column_type_i64(&self) -> ColumnType {
        match self {
            #[cfg(feature = "sqlite")]
            Dialect::Sqlite => ColumnType::custom("bigint"),
            _ => ColumnType::BigInteger,
        }
    }

    pub fn column_type_bool(&self) -> ColumnType {
        match self {
            #[cfg(feature = "sqlite")]
            Dialect::Sqlite => ColumnType::custom("bigint"),
            _ => ColumnType::Boolean,
        }
    }

    pub fn column_type_date_time_with_time_zone(&self) -> ColumnType {
        match self {
            #[cfg(feature = "sqlite")]
            Dialect::Sqlite => ColumnType::string(None),
            _ => ColumnType::TimestampWithTimeZone,
        }
    }

    pub fn column_type_uuid(&self) -> ColumnType {
        match self {
            #[cfg(feature = "sqlite")]
            Dialect::Sqlite => ColumnType::string(None),
            #[cfg(feature = "mysql")]
            Dialect::MySql => ColumnType::Text,
            _ => ColumnType::Uuid,
        }
    }

    pub fn default_value_string(&self, value: &str) -> Expr {
        match self {
            #[cfg(feature = "mysql")]
            Dialect::MySql => Expr::cust(format!("'{}'", value.replace('\'', "''"))),
            _ => Expr::val(value),
        }
    }
}

/* -------------------------------------- SQL CONVERTIBLE -------------------------------------- */

pub trait SqlConvertible: Send + Sync {
    fn to_sql(&self, dialect: Dialect) -> (String, SqlxValues);
}

macro_rules! impl_sql_convertible_sqlx_binder {
    ($s:ident) => {
        impl SqlConvertible for $s {
            fn to_sql(&self, dialect: Dialect) -> (String, SqlxValues) {
                match dialect {
                    #[cfg(feature = "postgres")]
                    Dialect::Postgres => self.build_sqlx(PostgresQueryBuilder),
                    #[cfg(feature = "mysql")]
                    Dialect::MySql => {
                        let (sql, values) = self.build_sqlx(MysqlQueryBuilder);
                        (sql, mysql::adapt_values(values))
                    }
                    #[cfg(feature = "sqlite")]
                    Dialect::Sqlite => {
                        let (sql, values) = self.build_sqlx(SqliteQueryBuilder);
                        (sql, sqlite::adapt_values(values))
                    }
                }
            }
        }
    };
}

macro_rules! impl_sql_convertible_other {
    ($s:ident) => {
        impl SqlConvertible for $s {
            fn to_sql(&self, dialect: Dialect) -> (String, SqlxValues) {
                let sql = match dialect {
                    #[cfg(feature = "postgres")]
                    Dialect::Postgres => self.to_string(PostgresQueryBuilder),
                    #[cfg(feature = "mysql")]
                    Dialect::MySql => self.to_string(MysqlQueryBuilder),
                    #[cfg(feature = "sqlite")]
                    Dialect::Sqlite => self.to_string(SqliteQueryBuilder),
                };
                let values = SqlxValues(sea_query::Values(vec![]));
                (sql, values)
            }
        }
    };
}

impl_sql_convertible_sqlx_binder!(SelectStatement);
impl_sql_convertible_sqlx_binder!(InsertStatement);
impl_sql_convertible_sqlx_binder!(UpdateStatement);
impl_sql_convertible_sqlx_binder!(DeleteStatement);
impl_sql_convertible_other!(TableCreateStatement);
impl_sql_convertible_other!(TableAlterStatement);
impl_sql_convertible_other!(TableRenameStatement);

/* ------------------------------------------- ERRORS ------------------------------------------ */

impl Dialect {
    pub fn is_table_not_found_error(&self, err: &DucklakeError) -> bool {
        match err {
            DucklakeError::Database(db_err) => match self {
                #[cfg(feature = "postgres")]
                Dialect::Postgres => db_err
                    .as_database_error()
                    .is_some_and(|err| err.code() == Some("42P01".into())),
                #[cfg(feature = "mysql")]
                Dialect::MySql => db_err
                    .as_database_error()
                    .is_some_and(|err| err.code() == Some("42S02".into())),
                #[cfg(feature = "sqlite")]
                Dialect::Sqlite => db_err.as_database_error().is_some_and(|err| {
                    err.code() == Some("1".into()) && err.message().contains("no such table")
                }),
            },
            _ => false,
        }
    }

    pub fn is_table_already_exists_error(&self, err: &DucklakeError) -> bool {
        match err {
            DucklakeError::Database(db_err) => match self {
                #[cfg(feature = "postgres")]
                Dialect::Postgres => db_err
                    .as_database_error()
                    .is_some_and(|err| err.code() == Some("42P07".into())),
                #[cfg(feature = "mysql")]
                Dialect::MySql => db_err
                    .as_database_error()
                    .is_some_and(|err| err.code() == Some("42S01".into())),
                #[cfg(feature = "sqlite")]
                Dialect::Sqlite => db_err.as_database_error().is_some_and(|err| {
                    err.code() == Some("1".into()) && err.message().contains("already exists")
                }),
            },
            _ => false,
        }
    }
}
