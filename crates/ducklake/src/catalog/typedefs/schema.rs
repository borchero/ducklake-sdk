use std::collections::HashMap;
use std::ops::Deref;

use super::*;
use crate::io;

/// A schema in a catalog.
#[derive(Debug, Clone)]
pub(in crate::catalog) struct CatalogSchema {
    pub state: CatalogState,
    pub name: String,
    pub tables: HashMap<String, ArenaIdx>,
    pub path: io::DucklakePath,
}

impl Deref for CatalogSchema {
    type Target = CatalogState;

    fn deref(&self) -> &CatalogState {
        &self.state
    }
}
