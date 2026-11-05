use chrono::{DateTime, Utc};
use sea_query::{ColumnDef, ColumnType, Iden, Order, Query, StringLen, TableCreateStatement};

use crate::db::{Dialect, UtcDateTime};
use crate::{Ducklake, DucklakeResult};

/// The maximum length of a migration name, which is the length of the `migration` column.
const MAX_NAME_LENGTH: u32 = 255;

/// The table in the catalog database which records the applied migrations.
///
/// This table belongs to the SDK and is not part of the DuckLake specification. It is therefore
/// created on demand (see [`Ducklake::ensure_migrations_table`]) instead of by
/// [`Ducklake::create`], and catalog upgrades do not touch it.
mod ledger {
    #[derive(sea_query::Iden)]
    #[iden = "ducklake_sdk_migrations"]
    pub(super) struct Table;

    #[derive(sea_query::Iden, Clone, Copy)]
    pub(super) enum Column {
        Migration,
        Batch,
        AppliedAt,
    }
}

/// A migration which has been recorded as applied.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct AppliedMigration {
    /// The unique name of the migration.
    #[sqlx(rename = "migration")]
    pub name: String,
    /// The batch in which the migration was applied.
    pub batch: i64,
    /// The time at which the migration was applied.
    #[sqlx(try_from = "UtcDateTime")]
    pub applied_at: DateTime<Utc>,
}

fn create_ledger_table(dialect: Dialect) -> TableCreateStatement {
    use ledger::Column::*;

    sea_query::Table::create()
        .table(ledger::Table)
        .if_not_exists()
        .col(
            ColumnDef::new_with_type(
                Migration,
                // NOTE: The column is a primary key, hence it requires a fixed length in MySQL
                ColumnType::String(StringLen::N(MAX_NAME_LENGTH)),
            )
            .not_null()
            .primary_key(),
        )
        .col(ColumnDef::new_with_type(Batch, dialect.column_type_i64()).not_null())
        .col(
            ColumnDef::new_with_type(AppliedAt, dialect.column_type_date_time_with_time_zone())
                .not_null(),
        )
        .to_owned()
}

impl Ducklake {
    /// Create the table which records applied migrations in the catalog database, unless it
    /// already exists.
    ///
    /// This is idempotent and safe to call concurrently.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection is read-only.
    pub async fn ensure_migrations_table(&self) -> DucklakeResult<()> {
        self.conn.check_writable()?;

        let pool = self.conn.pool();
        let table_name = ledger::Table.to_string();
        if pool.table_exists(&table_name).await? {
            return Ok(());
        }

        let statement = create_ledger_table(pool.dialect());
        let result = async {
            let mut tx = pool.begin().await?;
            tx.execute(&statement).await?;
            tx.commit().await
        }
        .await;
        if let Err(error) = result {
            // NOTE: `CREATE TABLE IF NOT EXISTS` is not atomic in all databases. When run
            //  concurrently, PostgreSQL may fail with errors such as a unique violation on a system
            //  table or a type that already exists. As the table exists afterwards, the call
            //  nevertheless fulfilled its purpose.
            return match pool.table_exists(&table_name).await {
                Ok(true) => Ok(()),
                _ => Err(error),
            };
        }
        Ok(())
    }

    /// List all applied migrations ordered by batch and name.
    ///
    /// If the table which records applied migrations does not exist, no migration has been applied
    /// and the result is empty. This never creates the table.
    pub async fn applied_migrations(&self) -> DucklakeResult<Vec<AppliedMigration>> {
        use ledger::Column::*;

        let pool = self.conn.pool();
        let query = Query::select()
            .columns([Migration, Batch, AppliedAt])
            .from(ledger::Table)
            .order_by(Batch, Order::Asc)
            .order_by(Migration, Order::Asc)
            .to_owned();
        match pool.fetch_all::<AppliedMigration>(&query).await {
            Ok(migrations) => Ok(migrations),
            Err(error) if pool.dialect().is_table_not_found_error(&error) => Ok(Vec::new()),
            Err(error) => Err(error),
        }
    }
}
