use std::fmt::Display;
use std::str::FromStr;

use crate::{DucklakeError, DucklakeResult};

#[derive(Debug, Clone)]
pub(crate) struct SortInfo(pub Vec<SortExpression>);

impl From<Vec<SortExpression>> for SortInfo {
    fn from(expressions: Vec<SortExpression>) -> Self {
        Self(expressions)
    }
}

/// An ordered column in a table's sort configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortExpression {
    /// Name of the column to sort by.
    pub expression: String,
    /// SQL dialect used to interpret the expression.
    pub dialect: String,
    /// Direction in which values are ordered.
    pub direction: SortDirection,
    /// Placement of null values.
    pub null_order: NullOrder,
}

/* --------------------------------------- SORT DIRECTION -------------------------------------- */

/// Direction of a sort expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    /// Sort from smallest to largest.
    Ascending,
    /// Sort from largest to smallest.
    Descending,
}

impl FromStr for SortDirection {
    type Err = DucklakeError;

    fn from_str(s: &str) -> DucklakeResult<Self> {
        match s {
            "ASC" => Ok(Self::Ascending),
            "DESC" => Ok(Self::Descending),
            _ => Err(DucklakeError::InvalidSortDirection(s.to_string())),
        }
    }
}

impl Display for SortDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Ascending => "ASC",
            Self::Descending => "DESC",
        };
        write!(f, "{s}")
    }
}

/* ----------------------------------------- NULL ORDER ---------------------------------------- */

/// Placement of null values in a sort expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullOrder {
    /// Place null values before non-null values.
    NullsFirst,
    /// Place null values after non-null values.
    NullsLast,
}

impl FromStr for NullOrder {
    type Err = DucklakeError;

    fn from_str(s: &str) -> DucklakeResult<Self> {
        match s {
            "NULLS_FIRST" => Ok(Self::NullsFirst),
            "NULLS_LAST" => Ok(Self::NullsLast),
            _ => Err(DucklakeError::InvalidNullOrder(s.to_string())),
        }
    }
}

impl Display for NullOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::NullsFirst => "NULLS_FIRST",
            Self::NullsLast => "NULLS_LAST",
        };
        write!(f, "{s}")
    }
}

impl SortExpression {
    /// Sort a column in ascending order with nulls last.
    pub fn column(name: impl Into<String>) -> Self {
        Self {
            expression: name.into(),
            dialect: "duckdb".to_string(),
            direction: SortDirection::Ascending,
            null_order: NullOrder::NullsLast,
        }
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                             TESTS                                             */
/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_sort_values() {
        assert!(matches!(
            "SIDEWAYS".parse::<SortDirection>(),
            Err(DucklakeError::InvalidSortDirection(value)) if value == "SIDEWAYS"
        ));
        assert!(matches!(
            "NULLS_MIDDLE".parse::<NullOrder>(),
            Err(DucklakeError::InvalidNullOrder(value)) if value == "NULLS_MIDDLE"
        ));
    }
}
