mod arrow;
mod dialects;
#[cfg(feature = "postgres")]
mod postgres_copy;
pub(crate) mod sea_query_ext;
mod types;

use std::sync::OnceLock;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
pub(crate) use dialects::Dialect;
use dialects::SqlConvertible;
use sea_query::Expr;
use sqlx::prelude::*;
pub(crate) use types::chrono::UtcDateTime;
pub(crate) use types::uuid::UuidText;

use crate::{DucklakeError, DucklakeResult};

/* ------------------------------------------ DISPATCH ----------------------------------------- */

/// Dispatches an operation over the concrete database backend behind a [`Pool`].
macro_rules! dispatch_pool {
    ($self:ident, $pool:ident => $call:expr) => {
        match &$self.0 {
            #[cfg(feature = "postgres")]
            AnyPool::Postgres($pool) => $call,
            #[cfg(feature = "mysql")]
            AnyPool::MySql($pool) => $call,
            #[cfg(feature = "sqlite")]
            AnyPool::Sqlite($pool) => $call,
        }
    };
}

/// Dispatches an operation over the concrete database backend behind a [`Transaction`].
macro_rules! dispatch_tx {
    ($self:ident, $tx:ident => $call:expr) => {
        match &mut $self.0 {
            #[cfg(feature = "postgres")]
            AnyTransaction::Postgres($tx) => $call,
            #[cfg(feature = "mysql")]
            AnyTransaction::MySql($tx) => $call,
            #[cfg(feature = "sqlite")]
            AnyTransaction::Sqlite($tx) => $call,
        }
    };
}

/* -------------------------------------------- POOL ------------------------------------------- */

/// Single-connection pool to a dynamic database backend (Postgres, MySQL, SQLite).
#[derive(Clone)]
pub(crate) struct Pool(AnyPool);

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
    pub(crate) fn dialect(&self) -> Dialect {
        match self.0 {
            #[cfg(feature = "postgres")]
            AnyPool::Postgres(_) => Dialect::Postgres,
            #[cfg(feature = "mysql")]
            AnyPool::MySql(_) => Dialect::MySql,
            #[cfg(feature = "sqlite")]
            AnyPool::Sqlite(_) => Dialect::Sqlite,
        }
    }

    /// Whether two pools connect to the same catalog database.
    pub(crate) fn is_same_catalog(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            #[cfg(feature = "postgres")]
            (AnyPool::Postgres(this), AnyPool::Postgres(other)) => {
                let this = this.connect_options();
                let other = other.connect_options();
                ServerCatalogKey::from_postgres(this.as_ref())
                    == ServerCatalogKey::from_postgres(other.as_ref())
            }
            #[cfg(feature = "mysql")]
            (AnyPool::MySql(this), AnyPool::MySql(other)) => {
                let this = this.connect_options();
                let other = other.connect_options();
                ServerCatalogKey::from_mysql(this.as_ref())
                    == ServerCatalogKey::from_mysql(other.as_ref())
            }
            #[cfg(feature = "sqlite")]
            (AnyPool::Sqlite(this), AnyPool::Sqlite(other)) => {
                let this = this.connect_options();
                let other = other.connect_options();
                normalized_path(this.get_filename()) == normalized_path(other.get_filename())
            }
            _ => false,
        }
    }

    pub(crate) async fn new(url: &str) -> DucklakeResult<Self> {
        // NOTE: Choose 8 because this allows the highest concurrency query in this
        //  repo to send all queries simultaneously.
        #[cfg(any(feature = "postgres", feature = "mysql"))]
        const POOL_SIZE: u32 = 8;

        let pool = if url.starts_with("postgresql://") || url.starts_with("postgres://") {
            #[cfg(feature = "postgres")]
            {
                let pool = sqlx::postgres::PgPoolOptions::new()
                    .max_connections(POOL_SIZE)
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
                    .max_connections(POOL_SIZE)
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

    pub(crate) async fn close(&self) {
        dispatch_pool!(self, pool => {
            pool.close().await
        })
    }

    pub(crate) async fn table_exists(&self, table_name: &str) -> DucklakeResult<bool> {
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

    pub(crate) async fn fetch_one<O>(&self, query: &impl SqlConvertible) -> DucklakeResult<O>
    where
        O: RowType,
    {
        let (sql, values) = query.to_sql(self.dialect());
        log_sql(sql.as_str(), Some(&values));
        let result = dispatch_pool!(self, pool => {
            sqlx::query_as_with(sql, values).fetch_one(pool).await?
        });
        Ok(result)
    }

    pub(crate) async fn fetch_all<O>(&self, query: &impl SqlConvertible) -> DucklakeResult<Vec<O>>
    where
        O: RowType,
    {
        let (sql, values) = query.to_sql(self.dialect());
        log_sql(sql.as_str(), Some(&values));
        let result = dispatch_pool!(self, pool => {
            sqlx::query_as_with(sql, values).fetch_all(pool).await?
        });
        Ok(result)
    }

    pub(crate) async fn fetch_optional<O>(
        &self,
        query: &impl SqlConvertible,
    ) -> DucklakeResult<Option<O>>
    where
        O: RowType,
    {
        let (sql, values) = query.to_sql(self.dialect());
        log_sql(sql.as_str(), Some(&values));
        let result = dispatch_pool!(self, pool => {
            sqlx::query_as_with(sql, values).fetch_optional(pool).await?
        });
        Ok(result)
    }

    pub(crate) async fn fetch_all_arrow(
        &self,
        query: &impl SqlConvertible,
        schema: &Schema,
    ) -> DucklakeResult<RecordBatch> {
        let (sql, values) = query.to_sql(self.dialect());
        log_sql(sql.as_str(), Some(&values));
        match &self.0 {
            #[cfg(feature = "postgres")]
            AnyPool::Postgres(pool) => {
                let rows = sqlx::query_with(sql, values).fetch(pool);
                arrow::decode_rows(rows, schema).await
            }
            #[cfg(feature = "mysql")]
            AnyPool::MySql(_) => unimplemented!("data inlining is not yet implemented for MySQL"),
            #[cfg(feature = "sqlite")]
            AnyPool::Sqlite(pool) => {
                let rows = sqlx::query_with(sql, values).fetch(pool);
                arrow::decode_rows(rows, schema).await
            }
        }
    }

    pub(crate) async fn begin(&self) -> DucklakeResult<Transaction> {
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

pub(crate) struct Transaction(AnyTransaction);

enum AnyTransaction {
    #[cfg(feature = "postgres")]
    Postgres(sqlx::Transaction<'static, sqlx::Postgres>),
    #[cfg(feature = "mysql")]
    MySql(sqlx::Transaction<'static, sqlx::MySql>),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::Transaction<'static, sqlx::Sqlite>),
}

impl Transaction {
    pub(crate) fn dialect(&self) -> Dialect {
        match self.0 {
            #[cfg(feature = "postgres")]
            AnyTransaction::Postgres(_) => Dialect::Postgres,
            #[cfg(feature = "mysql")]
            AnyTransaction::MySql(_) => Dialect::MySql,
            #[cfg(feature = "sqlite")]
            AnyTransaction::Sqlite(_) => Dialect::Sqlite,
        }
    }

    pub(crate) async fn execute(&mut self, query: &impl SqlConvertible) -> DucklakeResult<()> {
        let (sql, values) = query.to_sql(self.dialect());
        log_sql(sql.as_str(), Some(&values));
        dispatch_tx!(self, tx => {
            sqlx::query_with(sql, values).execute(&mut **tx).await?;
        });
        Ok(())
    }

    /// Update rows identified by non-null keys. CASE expressions keep this portable across
    /// backends without requiring unique constraints on the catalog tables.
    pub(crate) async fn update_rows<
        C: sea_query::IntoIden + Copy,
        const K: usize,
        const V: usize,
    >(
        &mut self,
        table: impl sea_query::IntoTableRef,
        keys: [C; K],
        columns: [C; V],
        rows: Vec<([sea_query::Value; K], [sea_query::Value; V])>,
    ) -> DucklakeResult<()> {
        use sea_query::{CaseStatement, Condition, ExprTrait, Query};

        let table = table.into_table_ref();
        // Bound SQL expression depth as well as parameter count (particularly for SQLite).
        let chunk_size = (self.dialect().max_bind_params() / (K * (V + 1) + V)).min(256);
        for chunk in rows.chunks(chunk_size) {
            let conditions: Vec<_> = chunk
                .iter()
                .map(|(values, _)| {
                    keys.iter()
                        .zip(values)
                        .fold(Condition::all(), |condition, (key, value)| {
                            condition.add(Expr::col(*key).eq(value.clone()))
                        })
                })
                .collect();
            let mut query = Query::update();
            query.table(table.clone());
            for (index, column) in columns.iter().enumerate() {
                let mut case = CaseStatement::new();
                for ((_, values), condition) in chunk.iter().zip(&conditions) {
                    case = case.case(condition.clone(), values[index].clone());
                }
                query.value(*column, case.finally(Expr::col(*column)));
            }
            query.cond_where(
                conditions
                    .into_iter()
                    .fold(Condition::any(), Condition::add),
            );
            self.execute(&query).await?;
        }
        Ok(())
    }

    /// Insert buffered catalog rows, using COPY for large PostgreSQL batches and parameter-limited
    /// INSERT statements otherwise.
    pub(crate) async fn insert_rows(
        &mut self,
        table: &str,
        columns: &[&str],
        rows: Vec<Vec<sea_query::Value>>,
    ) -> DucklakeResult<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let chunk_size = self.dialect().max_bind_params() / columns.len();
        // COPY has startup overhead; use it when it replaces multiple INSERT statements.
        #[cfg(feature = "postgres")]
        match &mut self.0 {
            AnyTransaction::Postgres(tx)
                if rows.len() > chunk_size && postgres_copy::supports(&rows) =>
            {
                return postgres_copy::insert(tx, table, columns, rows).await;
            }
            _ => {}
        }
        let mut rows = rows.into_iter();
        loop {
            let chunk: Vec<_> = rows.by_ref().take(chunk_size).collect();
            if chunk.is_empty() {
                break;
            }
            let mut query = sea_query::Query::insert();
            query
                .into_table(table.to_owned())
                .columns(columns.iter().map(|c| (*c).to_owned()));
            for row in chunk {
                query.values_panic(row.into_iter().map(Expr::val));
            }
            self.execute(&query).await?;
        }
        Ok(())
    }

    /// Insert a single entity into its backing table.
    pub(crate) async fn insert_entity(
        &mut self,
        entity: impl sea_query_ext::InsertableEntity,
    ) -> DucklakeResult<()> {
        let query = entity.insert_into_table();
        self.execute(&query).await
    }

    /// Insert the given entities into their backing table.
    ///
    /// Uses the same batching as raw rows to respect the database's bind parameter limit.
    pub(crate) async fn insert_entities<E>(
        &mut self,
        entities: impl IntoIterator<Item = E>,
    ) -> DucklakeResult<()>
    where
        E: sea_query_ext::InsertableEntity,
    {
        self.insert_entities_into(E::TABLE, entities).await
    }

    /// Insert the given entities into a dynamically named table.
    pub(crate) async fn insert_entities_into<E>(
        &mut self,
        table: &str,
        entities: impl IntoIterator<Item = E>,
    ) -> DucklakeResult<()>
    where
        E: sea_query_ext::InsertableEntity,
    {
        // Materialize owned rows before awaiting so borrowing iterators remain supported.
        let rows = entities.into_iter().map(E::into_values).collect();
        self.insert_rows(table, E::COLUMNS, rows).await
    }

    pub(crate) async fn insert_all_arrow(
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
        log_sql(sql.as_str(), None);

        // Execute the insertion query with the appropriate arguments built from the Arrow data
        match &mut self.0 {
            #[cfg(feature = "postgres")]
            AnyTransaction::Postgres(tx) => {
                let args: sqlx::postgres::PgArguments = arrow::encode_record_batch(&data)?;
                sqlx::query_with(sql, args).execute(&mut **tx).await?;
            }
            #[cfg(feature = "mysql")]
            AnyTransaction::MySql(_) => {
                unimplemented!("data inlining is not yet implemented for MySQL")
            }
            #[cfg(feature = "sqlite")]
            AnyTransaction::Sqlite(tx) => {
                let args: sqlx::sqlite::SqliteArguments = arrow::encode_record_batch(&data)?;
                sqlx::query_with(sql, args).execute(&mut **tx).await?;
            }
        };
        Ok(())
    }

    pub(crate) async fn fetch_one<O>(&mut self, query: &impl SqlConvertible) -> DucklakeResult<O>
    where
        O: RowType,
    {
        let (sql, values) = query.to_sql(self.dialect());
        log_sql(sql.as_str(), Some(&values));
        let result = dispatch_tx!(self, tx => {
            sqlx::query_as_with(sql, values).fetch_one(&mut **tx).await?
        });
        Ok(result)
    }

    pub(crate) async fn fetch_all<O>(
        &mut self,
        query: &impl SqlConvertible,
    ) -> DucklakeResult<Vec<O>>
    where
        O: RowType,
    {
        let (sql, values) = query.to_sql(self.dialect());
        log_sql(sql.as_str(), Some(&values));
        let result = dispatch_tx!(self, tx => {
            sqlx::query_as_with(sql, values).fetch_all(&mut **tx).await?
        });
        Ok(result)
    }

    pub(crate) async fn commit(self) -> DucklakeResult<()> {
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

    pub(crate) async fn rollback(self) -> DucklakeResult<()> {
        log_sql("ROLLBACK", None);
        match self.0 {
            #[cfg(feature = "postgres")]
            AnyTransaction::Postgres(tx) => tx.rollback().await?,
            #[cfg(feature = "mysql")]
            AnyTransaction::MySql(tx) => tx.rollback().await?,
            #[cfg(feature = "sqlite")]
            AnyTransaction::Sqlite(tx) => tx.rollback().await?,
        };
        Ok(())
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                             UTILS                                             */
/* --------------------------------------------------------------------------------------------- */

/* ------------------------------------------ LOGGING ------------------------------------------ */

#[allow(clippy::print_stdout)]
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

/* ----------------------------------------- CONNECTION ---------------------------------------- */

#[cfg(any(feature = "postgres", feature = "mysql"))]
#[derive(PartialEq, Eq)]
struct ServerCatalogKey<'a> {
    endpoint: ServerEndpoint,
    port: u16,
    database: Option<&'a str>,
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
#[derive(PartialEq, Eq)]
enum ServerEndpoint {
    Host(String),
    Socket(std::path::PathBuf),
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
impl<'a> ServerCatalogKey<'a> {
    fn new(
        host: &str,
        port: u16,
        socket: Option<&std::path::PathBuf>,
        database: Option<&'a str>,
    ) -> Self {
        let socket = socket
            .map(std::path::PathBuf::as_path)
            .or_else(|| host.starts_with('/').then(|| std::path::Path::new(host)));
        let endpoint = match socket {
            Some(socket) => ServerEndpoint::Socket(normalized_path(socket)),
            None => ServerEndpoint::Host(host.to_ascii_lowercase()),
        };
        Self {
            endpoint,
            port,
            database,
        }
    }

    #[cfg(feature = "postgres")]
    fn from_postgres(options: &'a sqlx::postgres::PgConnectOptions) -> Self {
        Self::new(
            options.get_host(),
            options.get_port(),
            options.get_socket(),
            options.get_database().or(Some(options.get_username())),
        )
    }

    #[cfg(feature = "mysql")]
    fn from_mysql(options: &'a sqlx::mysql::MySqlConnectOptions) -> Self {
        Self::new(
            options.get_host(),
            options.get_port(),
            options.get_socket(),
            options.get_database(),
        )
    }
}

fn normalized_path(path: &std::path::Path) -> std::path::PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/* ------------------------------------------ ROW TYPE ----------------------------------------- */

#[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
pub(crate) trait RowType = Send + Unpin;

#[cfg(all(feature = "postgres", not(feature = "mysql"), not(feature = "sqlite")))]
pub(crate) trait RowType =
    Send + Unpin + for<'r> FromRow<'r, <sqlx::Postgres as sqlx::Database>::Row>;

#[cfg(all(not(feature = "postgres"), feature = "mysql", not(feature = "sqlite")))]
pub(crate) trait RowType =
    Send + Unpin + for<'r> FromRow<'r, <sqlx::MySql as sqlx::Database>::Row>;

#[cfg(all(not(feature = "postgres"), not(feature = "mysql"), feature = "sqlite"))]
pub(crate) trait RowType =
    Send + Unpin + for<'r> FromRow<'r, <sqlx::Sqlite as sqlx::Database>::Row>;

#[cfg(all(feature = "postgres", feature = "mysql", not(feature = "sqlite")))]
pub(crate) trait RowType = Send
    + Unpin
    + for<'r> FromRow<'r, <sqlx::Postgres as sqlx::Database>::Row>
    + for<'r> FromRow<'r, <sqlx::MySql as sqlx::Database>::Row>;

#[cfg(all(feature = "postgres", not(feature = "mysql"), feature = "sqlite"))]
pub(crate) trait RowType = Send
    + Unpin
    + for<'r> FromRow<'r, <sqlx::Postgres as sqlx::Database>::Row>
    + for<'r> FromRow<'r, <sqlx::Sqlite as sqlx::Database>::Row>;

#[cfg(all(not(feature = "postgres"), feature = "mysql", feature = "sqlite"))]
pub(crate) trait RowType = Send
    + Unpin
    + for<'r> FromRow<'r, <sqlx::MySql as sqlx::Database>::Row>
    + for<'r> FromRow<'r, <sqlx::Sqlite as sqlx::Database>::Row>;

#[cfg(all(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub(crate) trait RowType = Send
    + Unpin
    + for<'r> FromRow<'r, <sqlx::Postgres as sqlx::Database>::Row>
    + for<'r> FromRow<'r, <sqlx::MySql as sqlx::Database>::Row>
    + for<'r> FromRow<'r, <sqlx::Sqlite as sqlx::Database>::Row>;
