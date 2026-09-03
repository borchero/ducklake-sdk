use std::collections::HashSet;
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

    // Subtract unqualified CTE names from the referenced relations. Qualified relations always
    // refer to catalog objects, even if their final component matches a CTE name.
    let mut tables = HashSet::new();
    for relation in visitor.relations {
        let Some(name) = TableName::from_object_name(&relation, default_schema) else {
            continue;
        };
        if relation.0.len() == 1 && visitor.cte_names.contains(&name.name) {
            continue;
        }
        tables.insert(name);
    }
    Ok(tables.into_iter().collect())
}

/// Visitor collecting both referenced relations and CTE names.
#[derive(Default)]
struct RelationVisitor {
    relations: Vec<ObjectName>,
    cte_names: HashSet<String>,
}

impl Visitor for RelationVisitor {
    type Break = ();

    fn pre_visit_query(&mut self, query: &sqlparser::ast::Query) -> ControlFlow<Self::Break> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.cte_names.insert(cte.alias.name.value.clone());
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
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
}
