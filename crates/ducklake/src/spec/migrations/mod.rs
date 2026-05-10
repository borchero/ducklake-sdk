use std::str::FromStr;

use itertools::Itertools;
use sea_query::{ExprTrait, Query};
use strum::IntoEnumIterator;

use super::{ducklake_metadata, metadata};
use crate::{DucklakeError, DucklakeResult, db};

macro_rules! execute_migration {
    ($tx:ident, $migration:expr) => {
        let query = $migration.to_owned();
        $tx.execute(&query).await?;
    };
}

macro_rules! create_table {
    ($tx:ident, $table:expr, $($column:expr),+ $(,)?) => {{
        execute_migration!(
            $tx,
            sea_query::Table::create()
                .table($table)
                $(.col($column))+
        );
    }};
}

macro_rules! add_table_columns {
    ($tx:ident, $table:expr, $($column:expr),+ $(,)?) => {{
        $(execute_migration!($tx, sea_query::Table::alter().table($table).add_column($column));)+
    }};
}

mod v0_2;
mod v0_3;
mod v0_4;
mod v1_0;

#[derive(strum::EnumIter)]
enum Migrations {
    V0_2,
    V0_3,
    V0_4,
    V1_0,
}

impl Migrations {
    fn version(&self) -> &'static str {
        use Migrations::*;
        match self {
            V0_2 => "0.2",
            V0_3 => "0.3",
            V0_4 => "0.4",
            V1_0 => "1.0",
        }
    }

    async fn migrate(&self, pool: &db::Pool) -> DucklakeResult<()> {
        let mut tx = pool.begin().await?;
        self.migrate_tables(&mut tx).await?;
        set_version(&mut tx, self.version()).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn migrate_tables(&self, tx: &mut db::Transaction) -> DucklakeResult<()> {
        use Migrations::*;
        match self {
            V0_2 => v0_2::migrate(tx).await,
            V0_3 => v0_3::migrate(tx).await,
            V0_4 => v0_4::migrate(tx).await,
            V1_0 => v1_0::migrate(tx).await,
        }
    }
}

async fn set_version(tx: &mut db::Transaction, version: &str) -> DucklakeResult<()> {
    execute_migration!(
        tx,
        Query::update()
            .table(ducklake_metadata::Table)
            .value(ducklake_metadata::Column::Value, version)
            .and_where(ducklake_metadata::Column::Key.col().eq(metadata::VERSION))
    );
    Ok(())
}

/* ----------------------------------------- MIGRATION ----------------------------------------- */

/// Migrate the catalog database to the most up-to-date DuckLake schema.
pub async fn migrate_catalog(pool: &db::Pool, current_version: &str) -> DucklakeResult<()> {
    let current_semver = current_version.parse::<DucklakeVersion>()?;
    for migration in Migrations::iter() {
        let migration_semver = migration.version().parse::<DucklakeVersion>()?;
        if migration_semver > current_semver {
            migration.migrate(pool).await?;
        }
    }
    Ok(())
}

/* ------------------------------------------ VERSION ------------------------------------------ */

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct DucklakeVersion {
    major: u64,
    minor: u64,
}

impl FromStr for DucklakeVersion {
    type Err = DucklakeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.split('.').collect_vec();
        if parts.len() != 2 {
            return Err(DucklakeError::InvalidVersion(s.to_string()));
        }
        let major = parts[0]
            .parse()
            .map_err(|_| DucklakeError::InvalidVersion(s.to_string()))?;
        let minor = parts[1]
            .parse()
            .map_err(|_| DucklakeError::InvalidVersion(s.to_string()))?;
        Ok(DucklakeVersion { major, minor })
    }
}
