use std::collections::HashSet;

use sqlparser::ast::Statement;
use sqlparser::dialect::dialect_from_str;
use sqlparser::parser::Parser;

use super::changes::Change;
use super::{IfExistsStrategy, Transaction};
use crate::{DucklakeError, DucklakeResult, MacroInfo, MacroType, TableName};

impl<'a> Transaction<'a> {
    /// Create a macro in the catalog.
    pub fn create_macro(
        &mut self,
        name: impl TryInto<TableName, Error = impl Into<DucklakeError>>,
        macro_type: MacroType,
        mut implementations: Vec<crate::MacroImplementation>,
        if_exists: IfExistsStrategy,
    ) -> DucklakeResult<()> {
        let name = name.try_into().map_err(|error| error.into())?;
        if matches!(if_exists, IfExistsStrategy::Skip)
            && self.catalog().macro_((&name, macro_type)).is_ok()
        {
            return Ok(());
        }
        validate_implementations(macro_type, &mut implementations)?;

        let info = MacroInfo {
            name: name.clone(),
            macro_type,
            implementations: implementations.clone(),
        };
        let (schema_ref, macro_ref) = self.catalog_mut().add_macro(info)?;
        self.changes.push(Change::CreateMacro {
            schema_ref,
            macro_ref,
            name: name.clone(),
            macro_type,
            implementations,
        });
        Ok(())
    }

    /// Delete a macro with the given name from the catalog.
    pub fn delete_macro(
        &mut self,
        name: impl TryInto<TableName, Error = impl Into<DucklakeError>>,
        macro_type: MacroType,
        if_not_exists: IfExistsStrategy,
    ) -> DucklakeResult<()> {
        let name = name.try_into().map_err(|error| error.into())?;
        if matches!(if_not_exists, IfExistsStrategy::Skip)
            && self.catalog().macro_((&name, macro_type)).is_err()
        {
            return Ok(());
        }
        let mut macro_ = self.catalog_mut().macro_mut((&name, macro_type))?;
        let macro_ref = macro_.ref_();
        macro_.delete();
        self.changes.push(Change::DeleteMacro {
            macro_ref,
            macro_type,
        });
        Ok(())
    }
}

fn validate_implementations(
    macro_type: MacroType,
    implementations: &mut [crate::MacroImplementation],
) -> DucklakeResult<()> {
    if implementations.is_empty() {
        return Err(DucklakeError::InvalidMacro {
            reason: "a macro must have at least one implementation".to_string(),
        });
    }

    let mut signatures = HashSet::new();
    for implementation in implementations {
        validate_implementation(macro_type, implementation)?;
        let signature = (
            implementation.dialect.to_ascii_lowercase(),
            implementation
                .parameters
                .iter()
                .map(|parameter| parameter.data_type.clone())
                .collect::<Vec<_>>(),
        );
        if !signatures.insert(signature) {
            return Err(DucklakeError::InvalidMacro {
                reason: "macro implementations must have unique parameter type signatures"
                    .to_string(),
            });
        }
    }
    Ok(())
}

fn validate_implementation(
    macro_type: MacroType,
    implementation: &mut crate::MacroImplementation,
) -> DucklakeResult<()> {
    let dialect =
        dialect_from_str(&implementation.dialect).ok_or_else(|| DucklakeError::InvalidMacro {
            reason: format!("invalid SQL dialect '{}'", implementation.dialect),
        })?;
    let valid = match macro_type {
        MacroType::Scalar => crate::view::parse_scalar_macro_expression(
            &implementation.sql,
            &implementation.dialect,
        )
        .is_ok(),
        MacroType::Table => Parser::parse_sql(&*dialect, &implementation.sql)
            .is_ok_and(|statements| matches!(statements.as_slice(), [Statement::Query(_)])),
    };
    if !valid {
        return Err(DucklakeError::InvalidMacro {
            reason: "macro SQL must be one scalar expression or one table query".to_string(),
        });
    }

    let mut names = HashSet::new();
    let mut saw_default = false;
    for parameter in &mut implementation.parameters {
        if parameter.name.is_empty() || !names.insert(parameter.name.to_ascii_lowercase()) {
            return Err(DucklakeError::InvalidMacro {
                reason: format!("invalid or duplicate parameter '{}'", parameter.name),
            });
        }
        if saw_default && parameter.default_value.is_none() {
            return Err(DucklakeError::InvalidMacro {
                reason: "required parameters must precede default parameters".to_string(),
            });
        }
        normalize_parameter(parameter)?;
        saw_default |= parameter.default_value.is_some();
    }
    Ok(())
}

fn normalize_parameter(parameter: &mut crate::MacroParameter) -> DucklakeResult<()> {
    parameter.data_type = if parameter.data_type.eq_ignore_ascii_case("unknown") {
        "unknown".to_string()
    } else {
        normalize_data_type(&parameter.data_type, &parameter.name)?
    };

    let Some(default_value) = &parameter.default_value else {
        if !parameter.default_value_type.eq_ignore_ascii_case("unknown") {
            return Err(DucklakeError::InvalidMacro {
                reason: format!(
                    "parameter '{}' has a default type but no default value",
                    parameter.name
                ),
            });
        }
        parameter.default_value_type = "unknown".to_string();
        return Ok(());
    };

    if parameter.default_value_type == "unknown" {
        return Err(DucklakeError::InvalidMacro {
            reason: format!(
                "default value for parameter '{}' requires a default value type",
                parameter.name
            ),
        });
    }
    parameter.default_value_type =
        normalize_data_type(&parameter.default_value_type, &parameter.name)?;
    for data_type in [&parameter.default_value_type, &parameter.data_type] {
        if data_type == "unknown" {
            continue;
        }
        validate_default_value(data_type, default_value).map_err(|error| {
            DucklakeError::InvalidMacro {
                reason: format!(
                    "invalid default value for parameter '{}' as {data_type}: {error}",
                    parameter.name
                ),
            }
        })?;
    }
    Ok(())
}

fn normalize_data_type(data_type: &str, parameter_name: &str) -> DucklakeResult<String> {
    let normalized = data_type.to_ascii_lowercase();
    if matches!(normalized.as_str(), "list" | "struct" | "map") {
        return Err(DucklakeError::InvalidMacro {
            reason: format!(
                "nested type '{data_type}' for parameter '{parameter_name}' cannot be safely reconstructed from DuckLake macro metadata"
            ),
        });
    }
    crate::DataType::parse_primitive(&normalized)
        .and_then(|data_type| {
            if let crate::DataType::Decimal { precision, scale } = data_type
                && (!(1..=38).contains(&precision) || scale > precision)
            {
                return Err(DucklakeError::InvalidDataType(normalized.clone()));
            }
            Ok(data_type.to_string())
        })
        .map_err(|_| DucklakeError::InvalidMacro {
            reason: format!(
                "invalid DuckLake type '{data_type}' for parameter '{parameter_name}'"
            ),
        })
}

fn validate_default_value(data_type: &str, value: &str) -> DucklakeResult<()> {
    match data_type {
        "varchar" => {}
        "boolean" if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") => {
        }
        "blob" if is_macro_blob(value) => {}
        "interval" | "json" => {
            return Err(DucklakeError::InvalidMacro {
                reason: format!(
                    "defaults of type '{data_type}' are not supported because their macro serialization cannot be validated safely"
                ),
            });
        }
        _ => {
            let data_type = crate::DataType::parse_primitive(data_type)?;
            let parsed = crate::Value::parse(&data_type, value)?.ok_or_else(|| {
                DucklakeError::InvalidMacro {
                    reason: format!("NULL is not a valid default for type '{data_type}'"),
                }
            })?;
            if let (crate::DataType::Decimal { precision, scale }, crate::Value::Decimal(value)) =
                (data_type, parsed)
            {
                let fractional_digits = value.scale();
                let mantissa_digits = value.mantissa().unsigned_abs().to_string().len() as u32;
                let integer_digits = if value.is_zero() {
                    0
                } else {
                    mantissa_digits.saturating_sub(fractional_digits)
                };
                if fractional_digits > u32::from(scale)
                    || integer_digits > u32::from(precision - scale)
                {
                    return Err(DucklakeError::InvalidMacro {
                        reason: format!("value is out of range for decimal({precision}, {scale})"),
                    });
                }
            }
        }
    }
    Ok(())
}

fn is_macro_blob(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            if index + 3 >= bytes.len()
                || bytes[index + 1] != b'x'
                || !bytes[index + 2].is_ascii_hexdigit()
                || !bytes[index + 3].is_ascii_hexdigit()
            {
                return false;
            }
            index += 4;
        } else if bytes[index].is_ascii_graphic() || bytes[index] == b' ' {
            index += 1;
        } else {
            return false;
        }
    }
    true
}
