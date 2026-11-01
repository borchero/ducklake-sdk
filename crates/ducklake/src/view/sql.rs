use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::ControlFlow;

use sqlparser::ast::{
    Expr,
    Function,
    FunctionArg,
    FunctionArgExpr,
    FunctionArguments,
    ObjectName,
    SetExpr,
    Statement,
    TableAlias,
    TableFactor,
    Visit,
    VisitMut,
    Visitor,
    VisitorMut,
};
use sqlparser::dialect::dialect_from_str;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::{
    DucklakeError,
    DucklakeResult,
    MacroImplementation,
    MacroParameter,
    MacroType,
    TableName,
};

pub(crate) struct SqlMacro {
    pub name: TableName,
    pub macro_type: MacroType,
    pub implementations: Vec<MacroImplementation>,
}

/// Expand catalog macro calls in a SELECT query for engines that do not load DuckLake macros.
pub(crate) fn expand_macros(
    sql: &str,
    dialect: &str,
    default_schema: &str,
    macros: &[SqlMacro],
) -> DucklakeResult<String> {
    let mut statement = parse_select_query(sql, dialect)?;
    let mut expander = MacroExpander {
        dialect,
        default_schema,
        macros,
        active: Vec::new(),
    };
    match VisitMut::visit(&mut statement, &mut expander) {
        ControlFlow::Continue(()) => Ok(statement.to_string()),
        ControlFlow::Break(reason) => Err(DucklakeError::InvalidMacro { reason }),
    }
}

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

/// Parse a macro body as exactly one scalar expression.
pub(crate) fn parse_scalar_macro_expression(sql: &str, dialect: &str) -> Result<Expr, String> {
    let dialect =
        dialect_from_str(dialect).ok_or_else(|| format!("invalid SQL dialect '{dialect}'"))?;
    let mut parser = Parser::new(&*dialect)
        .try_with_sql(sql)
        .map_err(|error| format!("failed to parse SQL: {error}"))?;
    let expression = parser
        .parse_expr()
        .map_err(|error| format!("failed to parse SQL: {error}"))?;
    if !matches!(parser.peek_token().token, Token::EOF) {
        return Err("macro SQL must contain exactly one scalar expression".to_string());
    }
    Ok(expression)
}

struct MacroExpander<'a> {
    dialect: &'a str,
    default_schema: &'a str,
    macros: &'a [SqlMacro],
    active: Vec<(TableName, MacroType)>,
}

impl<'a> MacroExpander<'a> {
    fn expand_scalar(&mut self, function: &Function) -> Result<Option<Expr>, String> {
        let Some(macro_) = self.find_macro(&function.name, MacroType::Scalar) else {
            return Ok(None);
        };
        let FunctionArguments::List(arguments) = &function.args else {
            return Err(format!("unsupported arguments for macro {}", macro_.name));
        };
        if arguments.duplicate_treatment.is_some()
            || function.over.is_some()
            || function.filter.is_some()
        {
            return Err(format!(
                "unsupported call modifiers for macro {}",
                macro_.name
            ));
        }
        let (implementation, bindings) =
            bind_implementation(macro_, &arguments.args, self.dialect)?;
        let mut expression =
            parse_scalar_macro_expression(&implementation.sql, &implementation.dialect)?;
        self.expand_body(macro_, &mut expression, bindings)?;
        Ok(Some(expression))
    }

    fn expand_table(
        &mut self,
        name: &ObjectName,
        arguments: &[FunctionArg],
    ) -> Result<Option<Box<sqlparser::ast::Query>>, String> {
        let Some(macro_) = self.find_macro(name, MacroType::Table) else {
            return Ok(None);
        };
        let (implementation, bindings) = bind_implementation(macro_, arguments, self.dialect)?;
        let dialect = dialect_from_str(&implementation.dialect)
            .ok_or_else(|| format!("invalid SQL dialect '{}'", implementation.dialect))?;
        let mut statements = Parser::parse_sql(&*dialect, &implementation.sql)
            .map_err(|error| error.to_string())?;
        let Some(Statement::Query(mut query)) = statements.pop() else {
            return Err(format!("table macro {} is not a query", macro_.name));
        };
        self.expand_body(macro_, &mut query, bindings)?;
        Ok(Some(query))
    }

    fn expand_body<T: VisitMut>(
        &mut self,
        macro_: &SqlMacro,
        body: &mut T,
        bindings: HashMap<String, Expr>,
    ) -> Result<(), String> {
        let key = (macro_.name.clone(), macro_.macro_type);
        if self.active.contains(&key) {
            return Err(format!("recursive macro {}", macro_.name));
        }
        self.active.push(key);
        substitute_parameters(body, bindings);
        let result = VisitMut::visit(body, self);
        self.active.pop();
        match result {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(reason) => Err(reason),
        }
    }

    fn find_macro(&self, name: &ObjectName, macro_type: MacroType) -> Option<&'a SqlMacro> {
        let is_unqualified = name.0.len() == 1;
        let name = TableName::from_object_name(name, self.default_schema)?;
        let find_in_schema = |schema: &str| {
            self.macros.iter().find(|macro_| {
                macro_.macro_type == macro_type
                    && macro_.name.schema.eq_ignore_ascii_case(schema)
                    && macro_.name.name.eq_ignore_ascii_case(&name.name)
            })
        };
        find_in_schema(&name.schema)
            .or_else(|| is_unqualified.then(|| find_in_schema("main")).flatten())
    }
}

impl VisitorMut for MacroExpander<'_> {
    type Break = String;

    fn post_visit_expr(&mut self, expression: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::Function(function) = expression else {
            return ControlFlow::Continue(());
        };
        match self.expand_scalar(function) {
            Ok(Some(expanded)) => *expression = Expr::Nested(Box::new(expanded)),
            Ok(None) => {}
            Err(reason) => return ControlFlow::Break(reason),
        }
        ControlFlow::Continue(())
    }

    fn post_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        let TableFactor::Table {
            name,
            alias,
            args: Some(arguments),
            sample,
            ..
        } = table_factor
        else {
            return ControlFlow::Continue(());
        };
        match self.expand_table(name, &arguments.args) {
            Ok(Some(subquery)) => {
                let alias = alias.clone().or_else(|| {
                    Some(TableAlias {
                        explicit: true,
                        name: name.0.last()?.as_ident()?.clone(),
                        columns: Vec::new(),
                        at: None,
                    })
                });
                *table_factor = TableFactor::Derived {
                    lateral: false,
                    subquery,
                    alias,
                    sample: sample.clone(),
                };
            }
            Ok(None) => {}
            Err(reason) => return ControlFlow::Break(reason),
        }
        ControlFlow::Continue(())
    }
}

fn bind_implementation<'a>(
    macro_: &'a SqlMacro,
    arguments: &[FunctionArg],
    dialect: &str,
) -> Result<(&'a MacroImplementation, HashMap<String, Expr>), String> {
    let mut candidates = macro_
        .implementations
        .iter()
        .filter(|implementation| implementation.dialect.eq_ignore_ascii_case(dialect))
        .filter_map(|implementation| {
            bind_arguments(
                &implementation.parameters,
                arguments,
                &implementation.dialect,
            )
            .map(|bindings| (implementation, bindings))
        });
    let (implementation, bindings) = candidates.next().ok_or_else(|| {
        format!(
            "no implementation of macro {} accepts the supplied arguments",
            macro_.name
        )
    })?;
    if candidates.next().is_some() {
        return Err(format!(
            "client-side macro expansion cannot resolve overloaded macro {} without SQL type binding",
            macro_.name
        ));
    }
    if implementation
        .parameters
        .iter()
        .any(|parameter| parameter.data_type != "unknown")
    {
        return Err(format!(
            "client-side macro expansion cannot enforce typed parameters for macro {}",
            macro_.name
        ));
    }
    Ok((implementation, bindings))
}

fn bind_arguments(
    parameters: &[MacroParameter],
    arguments: &[FunctionArg],
    dialect: &str,
) -> Option<HashMap<String, Expr>> {
    let mut result = HashMap::new();
    let mut saw_named = false;
    for (index, argument) in arguments.iter().enumerate() {
        let (parameter, expression) = match argument {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) if !saw_named => {
                (parameters.get(index)?, expression)
            }
            FunctionArg::Named {
                name,
                arg: FunctionArgExpr::Expr(expression),
                ..
            } => {
                saw_named = true;
                let parameter = parameters
                    .iter()
                    .find(|parameter| parameter.name.eq_ignore_ascii_case(&name.value))?;
                (parameter, expression)
            }
            _ => return None,
        };
        if result
            .insert(parameter.name.to_ascii_lowercase(), expression.clone())
            .is_some()
        {
            return None;
        }
    }

    for parameter in parameters {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            result.entry(parameter.name.to_ascii_lowercase())
        {
            entry.insert(default_expression(parameter, dialect)?);
        }
    }
    Some(result)
}

fn substitute_parameters<T: VisitMut>(value: &mut T, bindings: HashMap<String, Expr>) {
    let _ = sqlparser::ast::visit_expressions_mut(value, |expression| {
        if let Expr::Identifier(identifier) = expression
            && let Some(value) = bindings.get(&identifier.value.to_ascii_lowercase())
        {
            *expression = Expr::Nested(Box::new(value.clone()));
        }
        ControlFlow::<()>::Continue(())
    });
}

fn default_expression(parameter: &MacroParameter, dialect: &str) -> Option<Expr> {
    let default = parameter.default_value.as_ref()?;
    if parameter.default_value_type == "list" {
        return parse_scalar_macro_expression(default, dialect).ok();
    }
    let data_type = crate::DataType::parse_primitive(&parameter.default_value_type).ok()?;
    let literal = match parameter.default_value_type.as_str() {
        "varchar" => format!("'{}'", default.replace('\'', "''")),
        _ if default == crate::spec::literals::NULL_STRING => default.clone(),
        "boolean" => match default.to_ascii_lowercase().as_str() {
            "true" | "1" => "TRUE".to_string(),
            "false" | "0" => "FALSE".to_string(),
            _ => return None,
        },
        _ => format!("'{}'", default.replace('\'', "''")),
    };
    parse_scalar_macro_expression(&format!("CAST({literal} AS {data_type})"), dialect).ok()
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
    #[case(MacroType::Scalar, "m(x)", "SELECT m(1)")]
    #[case(MacroType::Table, "SELECT * FROM m(x)", "SELECT * FROM m(1)")]
    fn test_recursive_macro_expansion_is_rejected(
        #[case] macro_type: MacroType,
        #[case] body: &str,
        #[case] query: &str,
    ) {
        // Arrange
        let macros = [SqlMacro {
            name: "main.m".parse().unwrap(),
            macro_type,
            implementations: vec![MacroImplementation::new(
                body,
                vec![MacroParameter::new("x")],
            )],
        }];

        // Act
        let result = expand_macros(query, "duckdb", "main", &macros);

        // Assert
        assert!(
            matches!(result, Err(DucklakeError::InvalidMacro { reason }) if reason.contains("recursive"))
        );
    }

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
