mod sql;

use itertools::{Either, Itertools};
pub(crate) use sql::{find_referenced_tables_in_schema, parse_select_query};

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

/// The definition of a view, consisting of its SQL query, dialect, and referenced relations.
#[derive(Clone)]
pub struct ViewDefinition {
    /// The SQL `SELECT` query defining the view.
    pub sql: String,
    /// The SQL dialect used by the view definition.
    pub dialect: String,
    /// Schema against which unqualified relation names are resolved.
    pub default_schema: String,
    /// Names and handles of all catalog tables referenced by the query. References to non-catalog
    /// tables are omitted.
    pub tables: Vec<(TableName, Table)>,
    /// Names and handles of all catalog views referenced by the query.
    pub views: Vec<(TableName, View)>,
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

    /// Get the definition of the view, consisting of its SQL query and the catalog relations
    /// referenced by the query (excluding common table expressions).
    pub async fn definition(&self) -> DucklakeResult<ViewDefinition> {
        let snapshot = self.conn.current_snapshot();
        let catalog = snapshot.catalog().await?;
        let view = catalog.view(self.id)?;
        let sql = view.sql().to_string();
        let dialect = view.dialect().to_string();
        let default_schema = view.name().schema.clone();

        let (tables, views) = find_referenced_tables_in_schema(&sql, &dialect, &default_schema)?
            .into_iter()
            .partition_map(|name| match catalog.table(&name) {
                Ok(table) => {
                    let schema_id = table
                        .parent_schema()
                        .id()
                        .expect("referenced table schema must have an ID");
                    let table_id = table.id().expect("referenced table must have an ID");
                    Either::Left((name, Table::new(self.conn.clone(), schema_id, table_id)))
                }
                Err(_) => {
                    let view_id = catalog
                        .view(&name)
                        .expect("referenced relation must be a catalog table or view")
                        .id()
                        .expect("referenced view must have an ID");
                    Either::Right((name, View::new(self.conn.clone(), view_id)))
                }
            });
        Ok(ViewDefinition {
            sql,
            dialect,
            default_schema,
            tables,
            views,
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
