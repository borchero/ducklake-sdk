use super::changes::Change;
use super::{IfExistsStrategy, Transaction};
use crate::{DucklakeError, DucklakeResult, TableName, Tag, ViewInfo, view};

/* --------------------------------------------------------------------------------------------- */
/*                                           LIFECYCLE                                           */
/* --------------------------------------------------------------------------------------------- */

/* ------------------------------------------- CREATE ------------------------------------------ */

impl<'a> Transaction<'a> {
    /// Create a new view in the catalog.
    ///
    /// The provided SQL must be a single `SELECT` query (not a `CREATE VIEW` statement).
    pub fn create_view(
        &mut self,
        name: impl TryInto<TableName, Error = impl Into<DucklakeError>>,
        sql: String,
        column_aliases: Option<Vec<String>>,
        tags: Option<Vec<Tag>>,
        if_exists: IfExistsStrategy,
    ) -> DucklakeResult<()> {
        let name = name.try_into().map_err(|e| e.into())?;

        // If the view already exists and the strategy is specified accordingly, simply
        // return without making any changes
        if matches!(if_exists, IfExistsStrategy::Skip) && self.catalog().view(&name).is_ok() {
            return Ok(());
        }

        // Validate that the provided SQL is a single SELECT query and normalize it
        let statement = view::parse_select_query(&sql, "duckdb")?;
        let sql = format!("{statement:#}");

        // Insert the view into the catalog
        let dialect = "duckdb".to_string();
        let info = ViewInfo {
            name: name.clone(),
            sql: sql.clone(),
            dialect: dialect.clone(),
            column_aliases: column_aliases.clone(),
            tags: tags.clone().unwrap_or_default(),
        };
        let (schema_ref, view_ref) = self.catalog_mut().add_view(info)?;

        // Create the change object
        let change = Change::CreateView {
            schema_ref,
            view_ref,
            name,
            sql,
            dialect,
            column_aliases,
            tags,
        };
        self.changes.push(change);
        Ok(())
    }
}

/* ------------------------------------------- DELETE ------------------------------------------ */

impl<'a> Transaction<'a> {
    /// Delete the view with the given name from the catalog.
    pub fn delete_view(
        &mut self,
        name: impl TryInto<TableName, Error = impl Into<DucklakeError>>,
        if_not_exists: IfExistsStrategy,
    ) -> DucklakeResult<()> {
        let name = name.try_into().map_err(|e| e.into())?;
        // If the view does not exist and the strategy is specified accordingly, simply return
        // without making any changes
        if matches!(if_not_exists, IfExistsStrategy::Skip) && self.catalog().view(&name).is_err() {
            return Ok(());
        }

        let mut view = self.catalog_mut().view_mut(&name)?;
        view.delete();
        let change = Change::DeleteView {
            view_ref: view.ref_(),
        };
        self.changes.push(change);
        Ok(())
    }
}
