use chrono::{DateTime, Utc};
use ducklake_macros::ducklake_table;
use sea_query::{Expr, ExprTrait, Order, Query};

use crate::db::sea_query_ext::{self, CreateTable};
use crate::{Ducklake, DucklakeError, DucklakeResult, db};

/// The maximum length of a migration name.
const MAX_NAME_LENGTH: u32 = 255;

/// A migration which has been recorded as applied.
///
/// The backing table belongs to the SDK and is not part of the DuckLake specification. It is
/// therefore created on demand (see [`Ducklake::ensure_migrations_table`]) instead of by
/// [`Ducklake::create`], and catalog upgrades do not touch it.
#[ducklake_table(table_name = "_ducklake_sdk_migrations")]
pub struct AppliedMigration {
    /// The unique name of the migration.
    #[not_null]
    #[primary_key]
    #[max_length = 255]
    pub name: String,
    /// The batch in which the migration was applied.
    #[not_null]
    pub batch: i64,
    /// The time at which the migration was applied.
    #[not_null]
    pub applied_at: DateTime<Utc>,
}

/// A migration which is recorded as applied in the same database transaction as a commit.
pub(crate) struct MigrationRecord {
    pub(crate) name: String,
    batch: i64,
}

impl MigrationRecord {
    pub(crate) fn new(name: &str, batch: i64) -> DucklakeResult<Self> {
        let length = name.chars().count();
        if length == 0 || length > MAX_NAME_LENGTH as usize {
            return Err(DucklakeError::InvalidChanges(format!(
                "migration names must have between 1 and {MAX_NAME_LENGTH} characters but '{name}' has {length}"
            )));
        }
        Ok(Self {
            name: name.to_owned(),
            batch,
        })
    }

    /// The entity to insert to record this migration as applied at the provided time.
    pub(crate) fn to_entity(&self, applied_at: DateTime<Utc>) -> AppliedMigration {
        AppliedMigration {
            name: self.name.clone(),
            batch: self.batch,
            applied_at,
        }
    }

    /// Whether a migration with this name has already been recorded as applied.
    pub(crate) async fn is_recorded(&self, pool: &db::Pool) -> DucklakeResult<bool> {
        use applied_migration::Column::*;

        let query = Query::select()
            .column(Name)
            .from(applied_migration::Table)
            .and_where(Expr::col(Name).eq(self.name.as_str()))
            .to_owned();
        Ok(pool.fetch_optional::<(String,)>(&query).await?.is_some())
    }
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
        let mut tx = pool.begin().await?;
        tx.execute(
            sea_query::Table::create_entity::<AppliedMigration>(pool.dialect()).if_not_exists(),
        )
        .await?;
        tx.commit().await
    }

    /// List all applied migrations ordered by batch and name.
    ///
    /// If the table which records applied migrations does not exist, no migration has been applied
    /// and the result is empty. This never creates the table.
    pub async fn applied_migrations(&self) -> DucklakeResult<Vec<AppliedMigration>> {
        use applied_migration::Column::*;

        let pool = self.conn.pool();
        let query = Query::select()
            .columns([Name, Batch, AppliedAt])
            .from(applied_migration::Table)
            .order_by(Batch, Order::Asc)
            .order_by(Name, Order::Asc)
            .to_owned();
        match pool.fetch_all::<AppliedMigration>(&query).await {
            Ok(migrations) => Ok(migrations),
            Err(error) if pool.dialect().is_table_not_found_error(&error) => Ok(Vec::new()),
            Err(error) => Err(error),
        }
    }
}
