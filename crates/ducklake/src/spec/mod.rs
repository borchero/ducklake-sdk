mod entities;
mod init;
pub(crate) mod literals;
pub(crate) mod metadata;
mod query_clauses;
mod upgrades;

pub(crate) use entities::*;
pub(crate) use init::{Config as InitConfig, init_catalog};
pub(crate) use query_clauses::*;
pub(crate) use upgrades::upgrade_catalog;

pub(crate) const SUPPORTED_VERSIONS: [&str; 5] = ["0.1", "0.2", "0.3", "0.4", "1.0"];
pub(crate) const LATEST_VERSION: &str = SUPPORTED_VERSIONS.last().unwrap();
