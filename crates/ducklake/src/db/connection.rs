use sqlx::Connection as _;

use super::{
    AnyPool,
    AnyTransaction,
    Dialect,
    Pool,
    RowType,
    SqlConvertible,
    Transaction,
    log_sql,
};
use crate::DucklakeResult;

/// A connection retained across related operations without returning it to the pool.
pub(crate) enum Connection {
    #[cfg(feature = "postgres")]
    Postgres(sqlx::pool::PoolConnection<sqlx::Postgres>),
    #[cfg(feature = "mysql")]
    MySql(sqlx::pool::PoolConnection<sqlx::MySql>),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::pool::PoolConnection<sqlx::Sqlite>),
}

macro_rules! dispatch_connection {
    ($self:ident, $conn:ident => $call:expr) => {
        match $self {
            #[cfg(feature = "postgres")]
            Connection::Postgres($conn) => $call,
            #[cfg(feature = "mysql")]
            Connection::MySql($conn) => $call,
            #[cfg(feature = "sqlite")]
            Connection::Sqlite($conn) => $call,
        }
    };
}

impl Pool {
    pub(crate) async fn acquire(&self) -> DucklakeResult<Connection> {
        Ok(match &self.0 {
            #[cfg(feature = "postgres")]
            AnyPool::Postgres(pool) => Connection::Postgres(pool.acquire().await?),
            #[cfg(feature = "mysql")]
            AnyPool::MySql(pool) => Connection::MySql(pool.acquire().await?),
            #[cfg(feature = "sqlite")]
            AnyPool::Sqlite(pool) => Connection::Sqlite(pool.acquire().await?),
        })
    }
}

impl Connection {
    pub(crate) fn dialect(&self) -> Dialect {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => Dialect::Postgres,
            #[cfg(feature = "mysql")]
            Self::MySql(_) => Dialect::MySql,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(_) => Dialect::Sqlite,
        }
    }

    pub(crate) async fn fetch_one<O: RowType>(
        &mut self,
        query: &impl SqlConvertible,
    ) -> DucklakeResult<O> {
        let (sql, values) = query.to_sql(self.dialect());
        log_sql(sql.as_str(), Some(&values));
        Ok(dispatch_connection!(self, conn => {
            sqlx::query_as_with(sql, values).fetch_one(&mut **conn).await?
        }))
    }

    pub(crate) async fn fetch_all<O: RowType>(
        &mut self,
        query: &impl SqlConvertible,
    ) -> DucklakeResult<Vec<O>> {
        let (sql, values) = query.to_sql(self.dialect());
        log_sql(sql.as_str(), Some(&values));
        Ok(dispatch_connection!(self, conn => {
            sqlx::query_as_with(sql, values).fetch_all(&mut **conn).await?
        }))
    }

    pub(crate) async fn begin(&mut self) -> DucklakeResult<Transaction<'_>> {
        let sql = self.dialect().begin_statement();
        log_sql(sql, None);
        Ok(Transaction(match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(conn) => AnyTransaction::Postgres(conn.begin_with(sql).await?),
            #[cfg(feature = "mysql")]
            Self::MySql(conn) => AnyTransaction::MySql(conn.begin_with(sql).await?),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(conn) => AnyTransaction::Sqlite(conn.begin_with(sql).await?),
        }))
    }
}
