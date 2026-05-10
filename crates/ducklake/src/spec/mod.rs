mod entities;
mod init;
pub mod literals;
pub mod metadata;
mod migrations;
mod query_clauses;

pub use entities::*;
pub use init::{Config as InitConfig, init_catalog};
pub use migrations::migrate_catalog;
pub use query_clauses::*;

pub const SUPPORTED_VERSIONS: [&str; 5] = ["0.1", "0.2", "0.3", "0.4", "1.0"];
pub const LATEST_VERSION: &str = SUPPORTED_VERSIONS.last().unwrap();
