use std::collections::HashMap;

use super::*;
use crate::io;

/// A schema in a catalog.
#[derive(Debug, Clone)]
pub(in crate::catalog) struct CatalogSchema {
    pub id: Option<i64>,
    pub name: String,
    pub tables: HashMap<String, ArenaIdx>,
    pub path: io::DucklakePath,
}
