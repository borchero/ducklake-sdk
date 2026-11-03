use std::fmt::Display;
use std::str::FromStr;

use ducklake_macros::visibility_if;
use sqlparser::ast::ObjectName;

use crate::utils::{format_identifier, parse_identifier};
use crate::{DucklakeError, DucklakeResult};

/// Fully-qualified name of an object, consisting of a schema and an object name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedName {
    /// The name of the schema containing the object.
    pub schema: String,
    /// The name of the object within its schema.
    pub name: String,
}

impl QualifiedName {
    /// Create a qualified name from a SQL object name, resolving an unqualified name against the
    /// provided default schema.
    #[visibility_if(feature = "python", pub)]
    pub(crate) fn from_object_name(name: &ObjectName, default_schema: &str) -> Option<Self> {
        match name.0.as_slice() {
            [name] => Some(Self {
                schema: default_schema.to_string(),
                name: name.as_ident()?.value.clone(),
            }),
            [schema, name] => Some(Self {
                schema: schema.as_ident()?.value.clone(),
                name: name.as_ident()?.value.clone(),
            }),
            _ => None,
        }
    }
}

impl TryFrom<&str> for QualifiedName {
    type Error = DucklakeError;

    fn try_from(s: &str) -> DucklakeResult<Self> {
        s.parse()
    }
}

impl TryFrom<&String> for QualifiedName {
    type Error = DucklakeError;

    fn try_from(s: &String) -> DucklakeResult<Self> {
        s.parse()
    }
}

impl TryFrom<String> for QualifiedName {
    type Error = DucklakeError;

    fn try_from(s: String) -> DucklakeResult<Self> {
        s.parse()
    }
}

impl FromStr for QualifiedName {
    type Err = crate::DucklakeError;

    fn from_str(s: &str) -> DucklakeResult<Self> {
        if let Some(components) = parse_identifier(s) {
            match components.len() {
                1 => {
                    return Ok(QualifiedName {
                        schema: "main".to_string(),
                        name: components[0].clone(),
                    });
                }
                2 => {
                    return Ok(QualifiedName {
                        schema: components[0].clone(),
                        name: components[1].clone(),
                    });
                }
                _ => {}
            }
        }
        Err(DucklakeError::InvalidQualifiedName {
            name: s.into(),
            reason: "expected format 'name' or 'schema.name' (potentially escaped via \")",
        })
    }
}

impl Display for QualifiedName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format_identifier(&[&self.schema, &self.name]))
    }
}

/* ------------------------------------------- COLUMN ------------------------------------------ */

/// Convenience trait alias for any type that can be converted into a [`ColumnName`].
pub trait IntoColumnName: TryInto<ColumnName, Error: Into<DucklakeError>> {}
impl<T: TryInto<ColumnName, Error: Into<DucklakeError>>> IntoColumnName for T {}

/// Path to a column in a table, consisting of a top-level column name and zero or more nested
/// component names.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
#[repr(transparent)]
pub struct ColumnName(pub Vec<String>);

impl ColumnName {
    /// Create a new column name referencing the top-level column with the provided name.
    pub fn named(name: &str) -> Self {
        ColumnName(vec![name.to_string()])
    }

    /// Append a nested component to the column name and return the resulting column name.
    pub fn add_component(&self, component: &str) -> Self {
        let mut vec = Vec::with_capacity(self.0.len() + 1);
        vec.extend_from_slice(&self.0);
        vec.push(component.to_string());
        ColumnName(vec)
    }
}

impl TryFrom<&str> for ColumnName {
    type Error = DucklakeError;

    fn try_from(s: &str) -> DucklakeResult<Self> {
        s.parse()
    }
}

impl TryFrom<&String> for ColumnName {
    type Error = DucklakeError;

    fn try_from(s: &String) -> DucklakeResult<Self> {
        s.parse()
    }
}

impl TryFrom<String> for ColumnName {
    type Error = DucklakeError;

    fn try_from(s: String) -> DucklakeResult<Self> {
        s.parse()
    }
}

impl From<&[String]> for ColumnName {
    fn from(v: &[String]) -> Self {
        ColumnName(v.to_vec())
    }
}

impl From<Vec<String>> for ColumnName {
    fn from(v: Vec<String>) -> Self {
        ColumnName(v)
    }
}

impl AsRef<[String]> for ColumnName {
    fn as_ref(&self) -> &[String] {
        &self.0
    }
}

impl FromStr for ColumnName {
    type Err = crate::DucklakeError;

    fn from_str(s: &str) -> DucklakeResult<Self> {
        if let Some(components) = parse_identifier(s)
            && !components.is_empty()
        {
            return Ok(ColumnName(components));
        }
        Err(DucklakeError::InvalidColumnName {
            name: s.into(),
            reason: "expected format 'column' or 'struct_column.nested_column' (with arbitrary nesting and potentially escaped via \")",
        })
    }
}

impl Display for ColumnName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format_identifier(&self.0))
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
    #[case("users", "main", "users")]
    #[case("my_table", "main", "my_table")]
    #[case("_table123", "main", "_table123")]
    #[case("\"users\"", "main", "users")]
    #[case("\"my table\"", "main", "my table")]
    #[case("\"table\"\"with\"\"quotes\"", "main", "table\"with\"quotes")]
    #[case("schema.table", "schema", "table")]
    #[case("my_schema.my_table", "my_schema", "my_table")]
    #[case("\"schema\".\"table\"", "schema", "table")]
    #[case("\"my schema\".\"my table\"", "my schema", "my table")]
    #[case("\"schema\"\"x\".\"table\"\"y\"", "schema\"x", "table\"y")]
    fn test_qualified_name_parsing(
        #[case] input: &str,
        #[case] expected_schema: &str,
        #[case] expected_name: &str,
    ) {
        let table: QualifiedName = input.parse().unwrap();
        assert_eq!(table.schema, expected_schema);
        assert_eq!(table.name, expected_name);
    }

    #[rstest]
    #[case("main", "users", "\"main\".\"users\"")]
    #[case("my_schema", "my_table", "\"my_schema\".\"my_table\"")]
    #[case(
        "schema with spaces",
        "table with spaces",
        "\"schema with spaces\".\"table with spaces\""
    )]
    #[case("schema\"x", "table\"y", "\"schema\"\"x\".\"table\"\"y\"")]
    fn test_qualified_name_display(
        #[case] schema: &str,
        #[case] name: &str,
        #[case] expected: &str,
    ) {
        let table = QualifiedName {
            schema: schema.to_string(),
            name: name.to_string(),
        };
        assert_eq!(table.to_string(), expected);
    }
}
