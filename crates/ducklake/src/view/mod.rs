mod sql;

pub(crate) use sql::{find_referenced_tables, parse_select_query};

use crate::ducklake::DucklakeConnection;
use crate::{DucklakeResult, Table, TableName};

/// Handle to a view in the DuckLake catalog.
#[derive(Clone)]
pub struct View {
    conn: DucklakeConnection,
    id: i64,
}

/// Information required to create a view in the catalog.
#[derive(Clone)]
pub(crate) struct ViewInfo {
    pub name: TableName,
    pub sql: String,
    pub dialect: String,
    pub column_aliases: Option<Vec<String>>,
    pub tags: Vec<crate::Tag>,
}

/// The definition of a view, consisting of its SQL query, dialect, and referenced tables.
#[derive(Clone)]
pub struct ViewDefinition {
    /// The SQL `SELECT` query defining the view.
    pub sql: String,
    /// The SQL dialect used by the view definition.
    pub dialect: String,
    /// Names and handles of all catalog tables referenced by the query. References to non-catalog
    /// tables are omitted.
    pub tables: Vec<(TableName, Table)>,
}

impl View {
    pub(crate) fn new(conn: DucklakeConnection, id: i64) -> Self {
        Self { conn, id }
    }

    /// Get the name of the view.
    pub async fn name(&self) -> DucklakeResult<TableName> {
        let snapshot = self.conn.current_snapshot();
        let catalog = snapshot.catalog().await?;
        let view = catalog.view(self.id)?;
        Ok(view.name().clone())
    }

    /// Get the SQL query defining the view.
    pub async fn sql(&self) -> DucklakeResult<String> {
        let snapshot = self.conn.current_snapshot();
        let catalog = snapshot.catalog().await?;
        Ok(catalog.view(self.id)?.sql().to_string())
    }

    /// Get the explicit column aliases of the view, if any were provided at creation time.
    pub async fn column_aliases(&self) -> DucklakeResult<Option<Vec<String>>> {
        let snapshot = self.conn.current_snapshot();
        let catalog = snapshot.catalog().await?;
        Ok(catalog.view(self.id)?.column_aliases())
    }

    /// Get the tags of the view.
    pub async fn tags(&self) -> DucklakeResult<Vec<crate::Tag>> {
        let snapshot = self.conn.current_snapshot();
        let catalog = snapshot.catalog().await?;
        Ok(catalog.view(self.id)?.tags())
    }

    /// Get the definition of the view, consisting of its SQL query and the tables referenced by
    /// the query (excluding common table expressions).
    pub async fn definition(&self) -> DucklakeResult<ViewDefinition> {
        let snapshot = self.conn.current_snapshot();
        let catalog = snapshot.catalog().await?;
        let view = catalog.view(self.id)?;
        let sql = view.sql().to_string();
        let dialect = view.dialect().to_string();

        let tables = find_referenced_tables(&sql, &dialect)?
            .into_iter()
            .filter_map(|name| {
                let schema_id = catalog.schema(&name.schema).ok()?.id()?;
                let table_id = catalog.table(&name).ok()?.id()?;
                Some((name, Table::new(self.conn.clone(), schema_id, table_id)))
            })
            .collect();
        Ok(ViewDefinition {
            sql,
            dialect,
            tables,
        })
    }

    /// Delete the view.
    ///
    /// Once this method returns successfully, this object should no longer be used.
    pub async fn delete(&self) -> DucklakeResult<()> {
        let mut tx = self.conn.transaction(None).await?;
        let name = tx.catalog().view(self.id)?.name().clone();
        tx.delete_view(&name)?;
        tx.commit().await
    }
}
