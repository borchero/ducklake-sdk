use super::Transaction;
use super::changes::Change;
use crate::{DucklakeResult, io};

impl<'a> Transaction<'a> {
    /// Create a new schema in the catalog.
    pub fn create_schema(&mut self, name: &str, path: Option<String>) -> DucklakeResult<()> {
        let path: io::DucklakePath = path.unwrap_or_else(|| name.to_string()).parse()?;
        let schema_ref = self.catalog_mut().add_schema(name, path.clone())?;
        let change = Change::CreateSchema {
            schema_ref,
            name: name.to_string(),
            path: path.ensure_directory(),
        };
        self.changes.push(change);
        Ok(())
    }

    /// Delete an existing schema from the catalog.
    pub fn delete_schema(&mut self, name: &str) -> DucklakeResult<()> {
        let mut schema = self.catalog_mut().schema_mut(name)?;
        schema.delete()?;
        let change = Change::DeleteSchema {
            schema_ref: schema.ref_(),
        };
        self.changes.push(change);
        Ok(())
    }
}
