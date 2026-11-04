use std::str::FromStr;

use itertools::Itertools;
use sea_query::{ExprTrait, Query};
use strum::IntoEnumIterator;

use super::{ducklake_metadata, metadata};
use crate::{DucklakeError, DucklakeResult, db};

macro_rules! execute_upgrade {
    ($tx:ident, $upgrade:expr) => {
        let query = $upgrade.to_owned();
        $tx.execute(&query).await?;
    };
}

macro_rules! create_table {
    ($tx:ident, $table:expr, $($column:expr),+ $(,)?) => {{
        execute_upgrade!(
            $tx,
            sea_query::Table::create()
                .table($table)
                $(.col($column))+
        );
    }};
}

macro_rules! add_table_columns {
    ($tx:ident, $table:expr, $($column:expr),+ $(,)?) => {{
        $(execute_upgrade!($tx, sea_query::Table::alter().table($table).add_column($column));)+
    }};
}

mod v0_2;
mod v0_3;
mod v0_4;
mod v1_0;

#[derive(strum::EnumIter)]
enum Upgrades {
    V0_2,
    V0_3,
    V0_4,
    V1_0,
}

impl Upgrades {
    fn version(&self) -> &'static str {
        use Upgrades::*;
        match self {
            V0_2 => "0.2",
            V0_3 => "0.3",
            V0_4 => "0.4",
            V1_0 => "1.0",
        }
    }

    async fn upgrade(&self, pool: &db::Pool) -> DucklakeResult<()> {
        let mut tx = pool.begin().await?;
        self.upgrade_tables(&mut tx).await?;
        set_version(&mut tx, self.version()).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn upgrade_tables(&self, tx: &mut db::Transaction) -> DucklakeResult<()> {
        use Upgrades::*;
        match self {
            V0_2 => v0_2::upgrade(tx).await,
            V0_3 => v0_3::upgrade(tx).await,
            V0_4 => v0_4::upgrade(tx).await,
            V1_0 => v1_0::upgrade(tx).await,
        }
    }
}

async fn set_version(tx: &mut db::Transaction, version: &str) -> DucklakeResult<()> {
    execute_upgrade!(
        tx,
        Query::update()
            .table(ducklake_metadata::Table)
            .value(ducklake_metadata::Column::Value, version)
            .and_where(ducklake_metadata::Column::Key.col().eq(metadata::VERSION))
    );
    Ok(())
}

/* ------------------------------------------ UPGRADE ------------------------------------------ */

/// Upgrade the catalog database to the most up-to-date DuckLake schema.
pub(crate) async fn upgrade_catalog(pool: &db::Pool, current_version: &str) -> DucklakeResult<()> {
    let current_semver = current_version.parse::<DucklakeVersion>()?;
    for upgrade in Upgrades::iter() {
        let upgrade_semver = upgrade.version().parse::<DucklakeVersion>()?;
        if upgrade_semver > current_semver {
            upgrade.upgrade(pool).await?;
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
