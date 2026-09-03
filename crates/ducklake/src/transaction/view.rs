use ducklake_macros::visibility_if;

use super::changes::Change;
use super::{IfExistsStrategy, Transaction};
use crate::{DucklakeError, DucklakeResult, TableName, Tag, ViewInfo, view};

// NOTE: The general idea is that view-level functionality is exposed on the transaction itself
//  as well as on the view object. Users of the Rust crate would typically access view-level
//  functionality via the view object. However, it is much easier to write language bindings for
//  languages without ownership semantics (e.g., Python) if view-level functionality is exposed
//  on the transaction itself.

/// Handle to a view within an active transaction.
pub struct TransactionView<'tx, 'a> {
    tx: &'tx mut Transaction<'a>,
    name: TableName,
}

impl<'tx, 'a> TransactionView<'tx, 'a> {
    fn new(tx: &'tx mut Transaction<'a>, name: TableName) -> Self {
        Self { tx, name }
    }
}

impl<'a> Transaction<'a> {
    /// Get a handle to the view with the provided name within this transaction.
    pub fn view(
        &mut self,
        name: impl TryInto<TableName, Error = impl Into<DucklakeError>>,
    ) -> DucklakeResult<TransactionView<'_, 'a>> {
        let name = name.try_into().map_err(|e| e.into())?;
        // NOTE: We could create the TransactionView directly here without querying the catalog
        //  first. However, we want to ensure that the view exists at this point.
        self.catalog().view(&name)?;
        Ok(TransactionView::new(self, name))
    }
}

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
    ) -> DucklakeResult<TransactionView<'_, 'a>> {
        let name = name.try_into().map_err(|e| e.into())?;

        // If the view already exists and the strategy is specified accordingly, simply return the
        // existing view
        if matches!(if_exists, IfExistsStrategy::Skip) && self.catalog().view(&name).is_ok() {
            return Ok(TransactionView::new(self, name));
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
            name: name.clone(),
            sql,
            dialect,
            column_aliases,
            tags,
        };
        self.changes.push(change);
        Ok(TransactionView::new(self, name))
    }
}

/* ------------------------------------------- DELETE ------------------------------------------ */

impl<'tx, 'a> TransactionView<'tx, 'a> {
    /// Delete the view.
    pub fn delete(self) -> DucklakeResult<()> {
        self.tx.delete_view(&self.name)
    }
}

impl<'a> Transaction<'a> {
    /// Delete the view with the given name from the catalog.
    #[visibility_if(feature = "python", pub)]
    pub(crate) fn delete_view(&mut self, name: &TableName) -> DucklakeResult<()> {
        let mut view = self.catalog_mut().view_mut(name)?;
        view.delete();
        let change = Change::DeleteView {
            view_ref: view.ref_(),
        };
        self.changes.push(change);
        Ok(())
    }
}
