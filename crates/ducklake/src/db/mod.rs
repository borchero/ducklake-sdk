mod arrow;
mod dialects;
pub mod sea_query_ext;
mod types;

use std::sync::OnceLock;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
pub use dialects::Dialect;
use dialects::SqlConvertible;
use sea_query::Expr;
use sqlx::prelude::*;
pub use types::chrono::UtcDateTime;
pub use types::uuid::UuidText;

use crate::{DucklakeError, DucklakeResult};

/* -------------------------------------------- POOL ------------------------------------------- */

/// Single-connection pool to a dynamic database backend (Postgres, MySQL, SQLite).
#[derive(Clone)]
pub struct Pool(AnyPool);

#[derive(Clone)]
enum AnyPool {
    #[cfg(feature = "postgres")]
    Postgres(sqlx::Pool<sqlx::Postgres>),
    #[cfg(feature = "mysql")]
    MySql(sqlx::Pool<sqlx::MySql>),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::Pool<sqlx::Sqlite>),
}

impl Pool {
    pub fn dialect(&self) -> Dialect {
        match self.0 {
            #[cfg(feature = "postgres")]
            AnyPool::Postgres(_) => Dialect::Postgres,
            #[cfg(feature = "mysql")]
            AnyPool::MySql(_) => Dialect::MySql,
            #[cfg(feature = "sqlite")]
            AnyPool::Sqlite(_) => Dialect::Sqlite,
        }
    }

    pub async fn new(url: &str) -> DucklakeResult<Self> {
        let pool = if url.starts_with("postgresql://") || url.starts_with("postgres://") {
            #[cfg(feature = "postgres")]
            {
                let pool = sqlx::postgres::PgPoolOptions::new()
                    // NOTE: Choose 7 because this allows the highest concurrency query in this
                    //  repo to send all queries simultaneously.
                    .max_connections(7)
                    .connect(url)
                    .await?;
                AnyPool::Postgres(pool)
            }
            #[cfg(not(feature = "postgres"))]
            panic!("Postgres support is not enabled. Enable the 'postgres' feature.");
        } else if url.starts_with("mysql://") {
            #[cfg(feature = "mysql")]
            {
                let pool = sqlx::mysql::MySqlPoolOptions::new()
                    // NOTE: Choose 7 because this allows the highest concurrency query in this
                    //  repo to send all queries simultaneously.
                    .max_connections(7)
                    .connect(url)
                    .await?;
                AnyPool::MySql(pool)
            }
            #[cfg(not(feature = "mysql"))]
            panic!("MySQL support is not enabled. Enable the 'mysql' feature.");
        } else if url.starts_with("sqlite://") {
            #[cfg(feature = "sqlite")]
            {
                use sqlx::sqlite::SqliteConnectOptions;

                let connect_options = url.parse::<SqliteConnectOptions>()?.create_if_missing(true);
                let pool = sqlx::sqlite::SqlitePoolOptions::new()
                    .max_connections(1)
                    .connect_with(connect_options)
                    .await?;
                AnyPool::Sqlite(pool)
            }
            #[cfg(not(feature = "sqlite"))]
            panic!("SQLite support is not enabled. Enable the 'sqlite' feature.");
        } else {
            return Err(DucklakeError::UnsupportedDatabase(url.to_string()));
        };
        Ok(Pool(pool))
    }

    pub async fn close(&self) {
        match &self.0 {
            #[cfg(feature = "postgres")]
            AnyPool::Postgres(pool) => pool.close().await,
            #[cfg(feature = "mysql")]
            AnyPool::MySql(pool) => pool.close().await,
            #[cfg(feature = "sqlite")]
            AnyPool::Sqlite(pool) => pool.close().await,
        }
    }

    pub async fn table_exists(&self, table_name: &str) -> DucklakeResult<bool> {
        let result: (bool,) = match &self.0 {
            #[cfg(feature = "postgres")]
            AnyPool::Postgres(pool) => {
                let sql = "SELECT to_regclass($1) IS NOT NULL";
                log_sql(sql, None);
                sqlx::query_as(sql).bind(table_name).fetch_one(pool).await?
            }
            #[cfg(feature = "mysql")]
            AnyPool::MySql(pool) => {
                let sql = r#"SELECT COUNT(*) > 0
                   FROM information_schema.tables
                   WHERE table_schema = DATABASE() AND table_name = ?"#;
                log_sql(sql, None);
                sqlx::query_as(sql).bind(table_name).fetch_one(pool).await?
            }
            #[cfg(feature = "sqlite")]
            AnyPool::Sqlite(pool) => {
                let sql = r#"SELECT COUNT(*) > 0
                   FROM sqlite_master
                   WHERE type = 'table' AND name = ?"#;
                log_sql(sql, None);
                sqlx::query_as(sql).bind(table_name).fetch_one(pool).await?
            }
        };
        Ok(result.0)
    }

    pub async fn fetch_one<O, Q>(&self, query: &Q) -> DucklakeResult<O>
    where
        O: RowType,
        Q: SqlConvertible,
    {
        let (sql, values) = query.to_sql(self.dialect());
        log_sql(&sql, Some(&values));
        let result = match &self.0 {
            #[cfg(feature = "postgres")]
            AnyPool::Postgres(pool) => sqlx::query_as_with(&sql, values).fetch_one(pool).await?,
            #[cfg(feature = "mysql")]
            AnyPool::MySql(pool) => sqlx::query_as_with(&sql, values).fetch_one(pool).await?,
            #[cfg(feature = "sqlite")]
            AnyPool::Sqlite(pool) => sqlx::query_as_with(&sql, values).fetch_one(pool).await?,
        };
        Ok(result)
    }

    pub async fn fetch_all<O, Q>(&self, query: &Q) -> DucklakeResult<Vec<O>>
    where
        O: RowType,
        Q: SqlConvertible,
    {
        let (sql, values) = query.to_sql(self.dialect());
        log_sql(&sql, Some(&values));
        let result = match &self.0 {
            #[cfg(feature = "postgres")]
            AnyPool::Postgres(pool) => sqlx::query_as_with(&sql, values).fetch_all(pool).await?,
            #[cfg(feature = "mysql")]
            AnyPool::MySql(pool) => sqlx::query_as_with(&sql, values).fetch_all(pool).await?,
            #[cfg(feature = "sqlite")]
            AnyPool::Sqlite(pool) => sqlx::query_as_with(&sql, values).fetch_all(pool).await?,
        };
        Ok(result)
    }

    pub async fn fetch_optional<O, Q>(&self, query: &Q) -> DucklakeResult<Option<O>>
    where
        O: RowType,
        Q: SqlConvertible,
    {
        let (sql, values) = query.to_sql(self.dialect());
        log_sql(&sql, Some(&values));
        let result = match &self.0 {
            #[cfg(feature = "postgres")]
            AnyPool::Postgres(pool) => {
                sqlx::query_as_with(&sql, values)
                    .fetch_optional(pool)
                    .await?
            }
            #[cfg(feature = "mysql")]
            AnyPool::MySql(pool) => {
                sqlx::query_as_with(&sql, values)
                    .fetch_optional(pool)
                    .await?
            }
            #[cfg(feature = "sqlite")]
            AnyPool::Sqlite(pool) => {
                sqlx::query_as_with(&sql, values)
                    .fetch_optional(pool)
                    .await?
            }
        };
        Ok(result)
    }

    pub async fn fetch_all_arrow<Q>(
        &self,
        query: &Q,
        schema: &Schema,
    ) -> DucklakeResult<RecordBatch>
    where
        Q: SqlConvertible,
    {
        let (sql, values) = query.to_sql(self.dialect());
        log_sql(&sql, Some(&values));
        match &self.0 {
            #[cfg(feature = "postgres")]
            AnyPool::Postgres(pool) => {
                let rows = sqlx::query_with(&sql, values).fetch(pool);
                arrow::decode_rows(rows, schema).await
            }
            #[cfg(feature = "mysql")]
            AnyPool::MySql(_) => unimplemented!("data inlining is not yet implemented for MySQL"),
            #[cfg(feature = "sqlite")]
            AnyPool::Sqlite(pool) => {
                let rows = sqlx::query_with(&sql, values).fetch(pool);
                arrow::decode_rows(rows, schema).await
            }
        }
    }

    pub async fn begin(&self) -> DucklakeResult<Transaction> {
        let tx = match &self.0 {
            #[cfg(feature = "postgres")]
            AnyPool::Postgres(pool) => {
                let sql = "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ";
                log_sql(sql, None);
                AnyTransaction::Postgres(pool.begin_with(sql).await?)
            }
            #[cfg(feature = "mysql")]
            AnyPool::MySql(pool) => {
                let sql = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION";
                log_sql(sql, None);
                AnyTransaction::MySql(pool.begin_with(sql).await?)
            }
            #[cfg(feature = "sqlite")]
            AnyPool::Sqlite(pool) => {
                let sql = "BEGIN IMMEDIATE";
                log_sql(sql, None);
                AnyTransaction::Sqlite(pool.begin_with(sql).await?)
            }
        };
        Ok(Transaction(tx))
    }
}

/* ---------------------------------------- TRANSACTION ---------------------------------------- */

pub struct Transaction(AnyTransaction);

enum AnyTransaction {
    #[cfg(feature = "postgres")]
    Postgres(sqlx::Transaction<'static, sqlx::Postgres>),
    #[cfg(feature = "mysql")]
    MySql(sqlx::Transaction<'static, sqlx::MySql>),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::Transaction<'static, sqlx::Sqlite>),
}

impl Transaction {
    pub fn dialect(&self) -> Dialect {
        match self.0 {
            #[cfg(feature = "postgres")]
            AnyTransaction::Postgres(_) => Dialect::Postgres,
            #[cfg(feature = "mysql")]
            AnyTransaction::MySql(_) => Dialect::MySql,
            #[cfg(feature = "sqlite")]
            AnyTransaction::Sqlite(_) => Dialect::Sqlite,
        }
    }

    pub async fn execute<Q>(&mut self, query: &Q) -> DucklakeResult<()>
    where
        Q: SqlConvertible,
    {
        let (sql, values) = query.to_sql(self.dialect());
        log_sql(&sql, Some(&values));
        match &mut self.0 {
            #[cfg(feature = "postgres")]
            AnyTransaction::Postgres(tx) => {
                sqlx::query_with(&sql, values).execute(&mut **tx).await?;
            }
            #[cfg(feature = "mysql")]
            AnyTransaction::MySql(tx) => {
                sqlx::query_with(&sql, values).execute(&mut **tx).await?;
            }
            #[cfg(feature = "sqlite")]
            AnyTransaction::Sqlite(tx) => {
                sqlx::query_with(&sql, values).execute(&mut **tx).await?;
            }
        };
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn insert_all_arrow(
        &mut self,
        table: &str,
        data: RecordBatch,
    ) -> DucklakeResult<()> {
        if data.num_rows() == 0 || data.num_columns() == 0 {
            return Ok(());
        }

        // Build the insertion query
        let mut stmt = sea_query::Query::insert();
        stmt.into_table(table.to_string())
            .columns(data.schema().fields().iter().map(|f| f.name().clone()));
        // NOTE: We use dummy values for the placeholders here and replace them with the Arrow
        //  data below. This way, we are not dependent on data types supported by sea-query.
        //  For example,
        (0..data.num_rows()).for_each(|_| {
            let row = (0..data.num_columns())
                .map(|_| Expr::value(false))
                .collect::<Vec<_>>();
            stmt.values_panic(row);
        });
        let (sql, _) = stmt.to_sql(self.dialect());
        log_sql(&sql, None);

        // Execute the insertion query with the appropriate arguments built from the Arrow data
        match &mut self.0 {
            #[cfg(feature = "postgres")]
            AnyTransaction::Postgres(tx) => {
                let args: sqlx::postgres::PgArguments = arrow::encode_record_batch(&data)?;
                sqlx::query_with(&sql, args).execute(&mut **tx).await?;
            }
            #[cfg(feature = "mysql")]
            AnyTransaction::MySql(_) => {
                unimplemented!("data inlining is not yet implemented for MySQL")
            }
            #[cfg(feature = "sqlite")]
            AnyTransaction::Sqlite(tx) => {
                let args: sqlx::sqlite::SqliteArguments = arrow::encode_record_batch(&data)?;
                sqlx::query_with(&sql, args).execute(&mut **tx).await?;
            }
        };
        Ok(())
    }

    pub async fn fetch_one<O, Q>(&mut self, query: &Q) -> DucklakeResult<O>
    where
        O: RowType,
        Q: SqlConvertible,
    {
        let (sql, values) = query.to_sql(self.dialect());
        log_sql(&sql, Some(&values));
        let result = match &mut self.0 {
            #[cfg(feature = "postgres")]
            AnyTransaction::Postgres(pool) => {
                sqlx::query_as_with(&sql, values)
                    .fetch_one(&mut **pool)
                    .await?
            }
            #[cfg(feature = "mysql")]
            AnyTransaction::MySql(pool) => {
                sqlx::query_as_with(&sql, values)
                    .fetch_one(&mut **pool)
                    .await?
            }
            #[cfg(feature = "sqlite")]
            AnyTransaction::Sqlite(pool) => {
                sqlx::query_as_with(&sql, values)
                    .fetch_one(&mut **pool)
                    .await?
            }
        };
        Ok(result)
    }

    pub async fn commit(self) -> DucklakeResult<()> {
        log_sql("COMMIT", None);
        match self.0 {
            #[cfg(feature = "postgres")]
            AnyTransaction::Postgres(tx) => tx.commit().await?,
            #[cfg(feature = "mysql")]
            AnyTransaction::MySql(tx) => tx.commit().await?,
            #[cfg(feature = "sqlite")]
            AnyTransaction::Sqlite(tx) => tx.commit().await?,
        };
        Ok(())
    }
}

/* ------------------------------------------- UTILS ------------------------------------------- */

fn log_sql(sql: &str, values: Option<&sea_query_sqlx::SqlxValues>) {
    static VERBOSE: OnceLock<bool> = OnceLock::new();
    let verbose =
        *VERBOSE.get_or_init(|| std::env::var("DUCKLAKE_SQL_VERBOSE").as_deref() == Ok("1"));
    if verbose {
        match values {
            Some(values) if !values.0.0.is_empty() => {
                println!("[ducklake sql] {sql} -- values: {:?}", values.0.0)
            }
            _ => println!("[ducklake sql] {sql}"),
        }
    }
}

/* ------------------------------------------ ROW TYPE ----------------------------------------- */

#[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
pub trait RowType = Send + Unpin;

#[cfg(all(feature = "postgres", not(feature = "mysql"), not(feature = "sqlite")))]
pub trait RowType = Send + Unpin + for<'r> FromRow<'r, <sqlx::Postgres as sqlx::Database>::Row>;

#[cfg(all(not(feature = "postgres"), feature = "mysql", not(feature = "sqlite")))]
pub trait RowType = Send + Unpin + for<'r> FromRow<'r, <sqlx::MySql as sqlx::Database>::Row>;

#[cfg(all(not(feature = "postgres"), not(feature = "mysql"), feature = "sqlite"))]
pub trait RowType = Send + Unpin + for<'r> FromRow<'r, <sqlx::Sqlite as sqlx::Database>::Row>;

#[cfg(all(feature = "postgres", feature = "mysql", not(feature = "sqlite")))]
pub trait RowType = Send
    + Unpin
    + for<'r> FromRow<'r, <sqlx::Postgres as sqlx::Database>::Row>
    + for<'r> FromRow<'r, <sqlx::MySql as sqlx::Database>::Row>;

#[cfg(all(feature = "postgres", not(feature = "mysql"), feature = "sqlite"))]
pub trait RowType = Send
    + Unpin
    + for<'r> FromRow<'r, <sqlx::Postgres as sqlx::Database>::Row>
    + for<'r> FromRow<'r, <sqlx::Sqlite as sqlx::Database>::Row>;

#[cfg(all(not(feature = "postgres"), feature = "mysql", feature = "sqlite"))]
pub trait RowType = Send
    + Unpin
    + for<'r> FromRow<'r, <sqlx::MySql as sqlx::Database>::Row>
    + for<'r> FromRow<'r, <sqlx::Sqlite as sqlx::Database>::Row>;

#[cfg(all(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub trait RowType = Send
    + Unpin
    + for<'r> FromRow<'r, <sqlx::Postgres as sqlx::Database>::Row>
    + for<'r> FromRow<'r, <sqlx::MySql as sqlx::Database>::Row>
    + for<'r> FromRow<'r, <sqlx::Sqlite as sqlx::Database>::Row>;
