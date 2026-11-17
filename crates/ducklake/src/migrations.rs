use chrono::{DateTime, Utc};
use ducklake_macros::ducklake_table;
use sea_query::Iden;
#[cfg(feature = "python")]
use sea_query::{Order, Query};
use sqlx::error::ErrorKind;

#[cfg(feature = "python")]
use crate::Ducklake;
use crate::db::sea_query_ext;
#[cfg(feature = "python")]
use crate::db::sea_query_ext::CreateTable;
use crate::{DucklakeError, DucklakeResult, db};

/// The maximum length of a migration name.
#[cfg(feature = "python")]
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
    #[cfg(feature = "python")]
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

    /// Insert this migration as applied at the provided time into the table of applied migrations
    /// as part of the provided database transaction.
    ///
    /// Fails with [`DucklakeError::MigrationConflict`] if the migration has already been recorded,
    /// e.g. by a concurrent run.
    pub(crate) async fn insert(
        &self,
        tx: &mut db::Transaction<'_>,
        applied_at: DateTime<Utc>,
    ) -> DucklakeResult<()> {
        let entity = AppliedMigration {
            name: self.name.clone(),
            batch: self.batch,
            applied_at,
        };
        tx.insert_entity(entity)
            .await
            .map_err(|error| match &error {
                // NOTE: The message of all supported databases contains the name of the table (or of
                //  the constraint, which includes the table name).
                DucklakeError::Database(sqlx::Error::Database(db_error))
                    if db_error.kind() == ErrorKind::UniqueViolation
                        && db_error
                            .message()
                            .contains(&applied_migration::Table.to_string()) =>
                {
                    DucklakeError::MigrationConflict(self.name.clone())
                }
                _ => error,
            })
    }
}

#[cfg(feature = "python")]
impl Ducklake {
    /// Create the table which records applied migrations in the catalog database, unless it
    /// already exists.
    ///
    /// This is idempotent and safe to call concurrently.
    pub async fn ensure_migrations_table(&self) -> DucklakeResult<()> {
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
