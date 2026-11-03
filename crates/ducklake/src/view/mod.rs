mod sql;

use itertools::{Either, Itertools};
pub(crate) use sql::{
    SqlMacro,
    expand_macros,
    find_referenced_tables_in_schema,
    parse_scalar_macro_expression,
    parse_select_query,
};

use crate::ducklake::DucklakeConnection;
use crate::{DucklakeResult, QualifiedName, Table};

/// Handle to a view in the DuckLake catalog.
#[derive(Clone)]
pub struct View {
    conn: DucklakeConnection,
    id: i64,
}

/// Information required to create a view in the catalog.
#[derive(Clone)]
pub(crate) struct ViewInfo {
    pub name: QualifiedName,
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
    pub tables: Vec<(QualifiedName, Table)>,
    /// Names and handles of all catalog views referenced by the query.
    pub views: Vec<(QualifiedName, View)>,
}

impl View {
    pub(crate) fn new(conn: DucklakeConnection, id: i64) -> Self {
        Self { conn, id }
    }

    /// Get the name of the view.
    pub async fn name(&self) -> DucklakeResult<QualifiedName> {
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
        self.resolve_definition(false).await
    }

    /// Get a view definition with catalog macros expanded for client-side SQL execution.
    #[doc(hidden)]
    pub async fn expanded_definition(&self) -> DucklakeResult<ViewDefinition> {
        self.resolve_definition(true).await
    }

    async fn resolve_definition(&self, expand: bool) -> DucklakeResult<ViewDefinition> {
        let snapshot = self.conn.current_snapshot();
        let catalog = snapshot.catalog().await?;
        let view = catalog.view(self.id)?;
        let dialect = view.dialect().to_string();
        let default_schema = view.name().schema.clone();
        let sql = if expand {
            let macros = catalog
                .list_schemas()
                .into_iter()
                .flat_map(|schema| {
                    schema
                        .list_macros()
                        .into_iter()
                        .map(|macro_| SqlMacro {
                            name: macro_.name().clone(),
                            macro_type: macro_.macro_type(),
                            implementations: macro_.implementations(),
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            expand_macros(view.sql(), &dialect, &default_schema, &macros)?
        } else {
            view.sql().to_string()
        };

        let (tables, views) = find_referenced_tables_in_schema(&sql, &dialect, &default_schema)?
            .into_iter()
            .filter_map(|name| match catalog.table(&name) {
                Ok(table) => {
                    let schema_id = table.parent_schema().id()?;
                    let table_id = table.id()?;
                    Some(Either::Left((
                        name,
                        Table::new(self.conn.clone(), schema_id, table_id),
                    )))
                }
                Err(_) => catalog.view(&name).ok().and_then(|view| {
                    view.id().map(|view_id| {
                        Either::Right((name, View::new(self.conn.clone(), view_id)))
                    })
                }),
            })
            .partition_map(|relation| relation);
        Ok(ViewDefinition {
            sql,
            dialect,
            default_schema,
            tables,
            views,
        })
    }
}
