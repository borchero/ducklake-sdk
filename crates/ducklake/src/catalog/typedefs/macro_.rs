use std::collections::HashMap;

use itertools::Itertools;

use crate::spec::{DucklakeMacroImpl, DucklakeMacroParameters};
use crate::{
    DucklakeError,
    DucklakeResult,
    MacroImplementation,
    MacroParameter,
    MacroType,
    QualifiedName,
};

/// A macro in a catalog.
#[derive(Debug, Clone)]
pub(in crate::catalog) struct CatalogMacro {
    pub id: Option<i64>,
    pub name: QualifiedName,
    pub macro_type: MacroType,
    pub implementations: Vec<MacroImplementation>,
}

impl CatalogMacro {
    pub(in crate::catalog) fn from_ducklake(
        id: i64,
        name: QualifiedName,
        mut implementations: Vec<DucklakeMacroImpl>,
        parameters: &mut HashMap<(i64, i64), Vec<DucklakeMacroParameters>>,
    ) -> DucklakeResult<Self> {
        implementations.sort_by_key(|implementation| implementation.impl_id);
        let first = implementations
            .first()
            .ok_or_else(|| DucklakeError::InvalidMacro {
                reason: format!("macro {name} has no implementations"),
            })?;
        let macro_type = first.r#type.parse::<MacroType>()?;
        let implementations = implementations
            .into_iter()
            .map(|implementation| {
                if implementation.r#type.parse::<MacroType>()? != macro_type {
                    return Err(DucklakeError::InvalidMacro {
                        reason: format!("macro {name} mixes scalar and table implementations"),
                    });
                }
                Ok(MacroImplementation {
                    dialect: implementation.dialect,
                    sql: implementation.sql,
                    parameters: parameters
                        .remove(&(id, implementation.impl_id))
                        .unwrap_or_default()
                        .into_iter()
                        .sorted_by_key(|parameter| parameter.column_id)
                        .map(parameter_from_ducklake)
                        .collect(),
                })
            })
            .collect::<DucklakeResult<Vec<_>>>()?;
        Ok(Self {
            id: Some(id),
            name,
            macro_type,
            implementations,
        })
    }
}

fn parameter_from_ducklake(parameter: DucklakeMacroParameters) -> MacroParameter {
    let default_value = match parameter.default_value {
        Some(value)
            if parameter.default_value_type == "unknown"
                && value.eq_ignore_ascii_case(crate::spec::literals::NULL_STRING) =>
        {
            None
        }
        value => value,
    };
    MacroParameter {
        name: parameter.parameter_name,
        data_type: parameter.parameter_type,
        default_value,
        default_value_type: parameter.default_value_type,
    }
}
