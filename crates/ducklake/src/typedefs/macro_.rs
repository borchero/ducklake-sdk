use std::fmt::{Display, Formatter};
use std::str::FromStr;

use crate::DucklakeError;

/// The result kind of a DuckLake macro.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MacroType {
    /// A macro that expands to a scalar SQL expression.
    Scalar,
    /// A macro that expands to a table-producing SQL query.
    Table,
}

/// A parameter accepted by one macro implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MacroParameter {
    /// Parameter name.
    pub name: String,
    /// DuckLake type name, or `unknown` when the parameter is untyped.
    pub data_type: String,
    /// Serialized DuckLake default value. `None` means that the parameter is required.
    pub default_value: Option<String>,
    /// DuckLake type name of the default value, or `unknown` when there is no default.
    pub default_value_type: String,
}

impl MacroParameter {
    /// Create an untyped, required macro parameter.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type: "unknown".to_string(),
            default_value: None,
            default_value_type: "unknown".to_string(),
        }
    }

    /// Set the DuckLake type of the parameter.
    pub fn with_type(mut self, data_type: impl Into<String>) -> Self {
        self.data_type = data_type.into();
        self
    }

    /// Set the serialized DuckLake default and its DuckLake type.
    pub fn with_default(mut self, value: impl Into<String>, data_type: impl Into<String>) -> Self {
        self.default_value = Some(value.into());
        self.default_value_type = data_type.into();
        self
    }
}

/// One implementation (overload) of a macro.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MacroImplementation {
    /// SQL dialect of the implementation.
    pub dialect: String,
    /// SQL expression or query defining the implementation.
    pub sql: String,
    /// Ordered implementation parameters.
    pub parameters: Vec<MacroParameter>,
}

impl MacroImplementation {
    /// Create a DuckDB macro implementation.
    pub fn new(sql: impl Into<String>, parameters: Vec<MacroParameter>) -> Self {
        Self {
            dialect: "duckdb".to_string(),
            sql: sql.into(),
            parameters,
        }
    }
}

impl Display for MacroType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scalar => f.write_str("scalar"),
            Self::Table => f.write_str("table"),
        }
    }
}

impl FromStr for MacroType {
    type Err = DucklakeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "scalar" => Ok(Self::Scalar),
            "table" => Ok(Self::Table),
            value => Err(DucklakeError::InvalidMacro {
                reason: format!("unknown macro type '{value}'"),
            }),
        }
    }
}
