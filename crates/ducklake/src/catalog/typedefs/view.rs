/// A view in a catalog.
#[derive(Debug, Clone)]
pub(in crate::catalog) struct CatalogView {
    pub id: Option<i64>,
    pub name: crate::TableName,
    pub sql: String,
    pub dialect: String,
    pub column_aliases: Option<Vec<String>>,
    pub tags: Vec<crate::Tag>,
}
