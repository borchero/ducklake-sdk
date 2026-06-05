#![feature(trait_alias, unwrap_infallible, never_type)]
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

#[macro_use]
mod io;

mod caches;
mod catalog;
mod db;
mod ducklake;
mod error;
mod options;
mod primitives;
mod scan;
mod spec;
mod table;
mod transaction;
mod typedefs;
mod utils;

pub use caches::TableMetadata;
pub use ducklake::{Ducklake, SnapshotMetadata};
pub use error::{DucklakeError, DucklakeResult};
pub use options::*;
pub use primitives::{Interval, TimeWithTimezone};
pub use table::*;
pub use transaction::{AuthorInfo, Transaction, TransactionTable};
pub use typedefs::*;
pub use utils::DataFilePathGenerator;
