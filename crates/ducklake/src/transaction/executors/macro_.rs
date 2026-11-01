use crate::catalog::{MacroRef, SchemaRef};
use crate::spec::*;
use crate::transaction::{CommitState, TransactionChanges};
use crate::{DucklakeResult, MacroImplementation, MacroType, TableName};

pub(crate) fn create_macro<'a>(
    changes: &mut TransactionChanges,
    state: &mut CommitState<'a>,
    schema_ref: &SchemaRef,
    macro_ref: &MacroRef,
    name: &TableName,
    macro_type: MacroType,
    implementations: &[MacroImplementation],
) -> DucklakeResult<()> {
    let macro_id = state.macro_id(*macro_ref);
    changes.new_macros.push(DucklakeMacro {
        schema_id: state.schema_id(*schema_ref),
        macro_id,
        macro_name: name.name.clone(),
        begin_snapshot: state.snapshot_id(),
        end_snapshot: None,
    });

    for (impl_id, implementation) in implementations.iter().enumerate() {
        changes.new_macro_impls.push(DucklakeMacroImpl {
            macro_id,
            impl_id: impl_id as i64,
            dialect: implementation.dialect.clone(),
            sql: implementation.sql.clone(),
            r#type: macro_type.to_string(),
        });

        let parameters =
            implementation
                .parameters
                .iter()
                .enumerate()
                .map(|(column_id, parameter)| DucklakeMacroParameters {
                    macro_id,
                    impl_id: impl_id as i64,
                    column_id: column_id as i64,
                    parameter_name: parameter.name.clone(),
                    parameter_type: parameter.data_type.clone(),
                    default_value: Some(literals::format(parameter.default_value.as_ref())),
                    default_value_type: parameter.default_value_type.clone(),
                });
        changes.new_macro_parameters.extend(parameters);
    }
    Ok(())
}

pub(crate) fn delete_macro<'a>(
    changes: &mut TransactionChanges,
    state: &mut CommitState<'a>,
    macro_ref: &MacroRef,
) -> DucklakeResult<()> {
    let macro_id = state.macro_id(*macro_ref);
    changes.dropped_macros.insert(macro_id);
    Ok(())
}
