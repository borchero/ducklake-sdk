use std::collections::{HashSet, VecDeque};
use std::ops::ControlFlow;

use sqlparser::ast::{ObjectName, SetExpr, Statement, Visit, Visitor};
use sqlparser::dialect::dialect_from_str;
use sqlparser::parser::Parser;

use crate::{DucklakeError, DucklakeResult, TableName};

/// Parse and validate a SELECT query.
pub(crate) fn parse_select_query(sql: &str, dialect: &str) -> DucklakeResult<Statement> {
    let dialect = dialect_from_str(dialect).ok_or_else(|| DucklakeError::InvalidView {
        reason: format!("invalid SQL dialect '{dialect}'"),
    })?;
    let mut statements =
        Parser::parse_sql(&*dialect, sql).map_err(|e| DucklakeError::InvalidView {
            reason: format!("failed to parse SQL: {e}"),
        })?;

    if statements.len() != 1 {
        return Err(DucklakeError::InvalidView {
            reason: format!(
                "expected exactly one SQL statement but found {}",
                statements.len()
            ),
        });
    }

    let statement = statements.pop().unwrap();
    match &statement {
        Statement::Query(query) => match query.body.as_ref() {
            SetExpr::Select(_) | SetExpr::Query(_) | SetExpr::SetOperation { .. } => {}
            _ => {
                return Err(DucklakeError::InvalidView {
                    reason: "view definition must be a SELECT query".to_string(),
                });
            }
        },
        _ => {
            return Err(DucklakeError::InvalidView {
                reason: "view definition must be a SELECT query".to_string(),
            });
        }
    }
    Ok(statement)
}

/// Extract the names of all tables referenced by the provided SELECT query, resolving
/// unqualified names against `default_schema`.
pub(crate) fn find_referenced_tables_in_schema(
    sql: &str,
    dialect: &str,
    default_schema: &str,
) -> DucklakeResult<Vec<TableName>> {
    let statement = parse_select_query(sql, dialect)?;

    let mut visitor = RelationVisitor::default();
    let _ = statement.visit(&mut visitor);

    let mut tables = HashSet::new();
    for relation in visitor.relations {
        let Some(name) = TableName::from_object_name(&relation, default_schema) else {
            continue;
        };
        tables.insert(name);
    }
    Ok(tables.into_iter().collect())
}

/// Visitor collecting catalog relations while tracking the CTEs visible in each query scope.
#[derive(Default)]
struct RelationVisitor {
    relations: Vec<ObjectName>,
    scopes: Vec<QueryScope>,
}

struct QueryScope {
    visible_ctes: HashSet<String>,
    pending_ctes: VecDeque<String>,
}

impl Visitor for RelationVisitor {
    type Break = ();

    fn pre_visit_query(&mut self, query: &sqlparser::ast::Query) -> ControlFlow<Self::Break> {
        let mut visible_ctes = self
            .scopes
            .last()
            .map(|scope| scope.visible_ctes.clone())
            .unwrap_or_default();
        let pending_ctes = query
            .with
            .iter()
            .flat_map(|with| &with.cte_tables)
            .map(|cte| cte.alias.name.value.clone())
            .collect::<VecDeque<_>>();
        if query.with.as_ref().is_some_and(|with| with.recursive) {
            visible_ctes.extend(pending_ctes.iter().cloned());
        }
        self.scopes.push(QueryScope {
            visible_ctes,
            pending_ctes,
        });
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &sqlparser::ast::Query) -> ControlFlow<Self::Break> {
        self.scopes.pop().expect("query scope must exist");
        if let Some(parent) = self.scopes.last_mut()
            && let Some(cte) = parent.pending_ctes.pop_front()
        {
            parent.visible_ctes.insert(cte);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        if let [name] = relation.0.as_slice()
            && self.scopes.last().is_some_and(|scope| {
                name.as_ident()
                    .is_some_and(|name| scope.visible_ctes.contains(&name.value))
            })
        {
            return ControlFlow::Continue(());
        }
        self.relations.push(relation.clone());
        ControlFlow::Continue(())
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                             TESTS                                             */
/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("CREATE VIEW v AS SELECT 1")]
    #[case("INSERT INTO t VALUES (1)")]
    #[case("SELECT 1; SELECT 2")]
    fn test_parse_rejects_non_select(#[case] sql: &str) {
        assert!(parse_select_query(sql, "duckdb").is_err());
    }

    #[rstest]
    #[case("SELECT * FROM users")]
    #[case("SELECT a FROM x UNION SELECT b FROM y")]
    fn test_parse_accepts_select(#[case] sql: &str) {
        assert!(parse_select_query(sql, "duckdb").is_ok());
    }

    #[test]
    fn test_referenced_tables_simple() {
        let tables =
            find_referenced_tables_in_schema("SELECT * FROM users", "duckdb", "main").unwrap();
        assert_eq!(tables, vec!["main.users".try_into().unwrap()]);
    }

    #[test]
    fn test_referenced_tables_qualified() {
        let tables = find_referenced_tables_in_schema(
            "SELECT * FROM my_schema.orders o JOIN main.users u ON o.uid = u.id",
            "duckdb",
            "main",
        )
        .unwrap();
        assert!(tables.contains(&"my_schema.orders".try_into().unwrap()));
        assert!(tables.contains(&"main.users".try_into().unwrap()));
    }

    #[test]
    fn test_referenced_tables_uses_default_schema() {
        let tables =
            find_referenced_tables_in_schema("SELECT * FROM users", "duckdb", "analytics")
                .unwrap();
        assert_eq!(tables, vec!["analytics.users".try_into().unwrap()]);
    }

    #[test]
    fn test_referenced_tables_excludes_ctes() {
        let sql = "WITH recent AS (SELECT * FROM events) SELECT * FROM recent JOIN users ON true";
        let tables = find_referenced_tables_in_schema(sql, "duckdb", "main").unwrap();
        // `recent` is a CTE and must be excluded; `events` and `users` remain.
        assert!(tables.contains(&"main.events".try_into().unwrap()));
        assert!(tables.contains(&"main.users".try_into().unwrap()));
        assert!(!tables.iter().any(|t| t.name == "recent"));
    }

    #[rstest]
    #[case("events")]
    #[case("main.events")]
    fn test_referenced_tables_resolves_base_table_inside_same_named_cte(#[case] relation: &str) {
        // Arrange
        let sql = format!("WITH events AS (SELECT * FROM {relation}) SELECT * FROM events");

        // Act
        let tables = find_referenced_tables_in_schema(&sql, "duckdb", "main").unwrap();

        // Assert
        assert_eq!(tables, vec!["main.events".try_into().unwrap()]);
    }
}
