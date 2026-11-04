use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array,
    ArrayRef,
    LargeListArray,
    MapArray,
    RecordBatch,
    RecordBatchOptions,
    StructArray,
};
use arrow_schema::{DataType as ArrowType, Field, Fields};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::{Column, ColumnDefault, DataType, DucklakeError, DucklakeResult};

#[derive(Clone, Copy)]
pub(crate) enum Defaults {
    InitialDefault,
    DefaultValue,
}

pub(crate) fn match_to_schema(
    batch: &RecordBatch,
    schema: &crate::Schema,
    defaults: Defaults,
) -> DucklakeResult<RecordBatch> {
    let arrays = match_fields(
        batch.schema().fields(),
        batch.columns(),
        schema.columns.values(),
        batch.num_rows(),
        defaults,
    )?;
    for (column, array) in schema.columns.values().zip(&arrays) {
        check_nullability(column, array, None)?;
    }
    Ok(RecordBatch::try_new_with_options(
        Arc::new(schema.to_arrow()),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?)
}

fn check_nullability(
    column: &Column,
    array: &ArrayRef,
    parent: Option<&StructArray>,
) -> DucklakeResult<()> {
    if !column.nullable
        && let Some(nulls) = array.logical_nulls()
        && nulls.null_count() > 0
        && !parent
            .and_then(|parent| parent.nulls())
            .is_some_and(|parent_nulls| parent_nulls.contains(&nulls))
    {
        return Err(DucklakeError::InvalidNullValue {
            column: column.name.clone(),
        });
    }
    Ok(())
}

fn match_fields<'a>(
    fields: &Fields,
    arrays: &[ArrayRef],
    columns: impl IntoIterator<Item = &'a Column>,
    len: usize,
    defaults: Defaults,
) -> DucklakeResult<Vec<ArrayRef>> {
    let mut source = HashMap::with_capacity(fields.len());
    for (field, array) in fields.iter().zip(arrays) {
        let key = match defaults {
            Defaults::InitialDefault => field.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Defaults::DefaultValue => Some(field.name()),
        };
        if let Some(key) = key
            && source
                .insert(key.as_str(), (field.as_ref(), array))
                .is_some()
        {
            return Err(DucklakeError::SchemaError(format!(
                "duplicate field: {key}"
            )));
        }
    }
    let result = columns
        .into_iter()
        .map(|column| {
            let key = match defaults {
                Defaults::InitialDefault => column.field_id.map(|id| id.to_string()),
                Defaults::DefaultValue => Some(column.name.clone()),
            };
            match key.and_then(|key| source.remove(key.as_str())) {
                Some((field, array)) => match_column(field, array, column, defaults),
                None => generate_column(column, len, defaults),
            }
        })
        .collect::<DucklakeResult<_>>()?;
    if matches!(defaults, Defaults::DefaultValue) && !source.is_empty() {
        return Err(DucklakeError::SchemaError(
            "unexpected columns in inline data".into(),
        ));
    }
    Ok(result)
}

fn match_column(
    field: &Field,
    array: &ArrayRef,
    column: &Column,
    defaults: Defaults,
) -> DucklakeResult<ArrayRef> {
    let target = column.to_arrow_field();
    // Nested Arrow types include child names, field IDs, and nullability.
    match (array.data_type(), &column.dtype) {
        (ArrowType::Struct(_) | ArrowType::LargeList(_) | ArrowType::Map(_, _), _)
            if array.data_type() == target.data_type() =>
        {
            Ok(array.clone())
        }
        (ArrowType::Struct(_), DataType::Struct(columns)) => {
            let source = array.as_struct();
            let arrays = match_fields(
                source.fields(),
                source.columns(),
                columns,
                source.len(),
                defaults,
            )?;
            for (column, array) in columns.iter().zip(&arrays) {
                check_nullability(column, array, Some(source))?;
            }
            let ArrowType::Struct(fields) = target.data_type() else {
                unreachable!()
            };
            Ok(Arc::new(StructArray::try_new_with_length(
                fields.clone(),
                arrays,
                source.nulls().cloned(),
                source.len(),
            )?))
        }
        (ArrowType::List(inner) | ArrowType::LargeList(inner), DataType::List(column)) => {
            let array = arrow_cast::cast(array, &ArrowType::LargeList(inner.clone()))?;
            let source = array.as_list::<i64>();
            let values = match_child(inner, source.values(), column, defaults)?;
            check_nullability(column, &values, None)?;
            Ok(Arc::new(LargeListArray::try_new(
                Arc::new(column.to_arrow_field()),
                source.offsets().clone(),
                values,
                source.nulls().cloned(),
            )?))
        }
        (ArrowType::Map(_, _), DataType::Map(key, value)) => {
            let source = array.as_map();
            let fields = source.entries().fields();
            let keys = match_child(&fields[0], source.keys(), key, defaults)?;
            let values = match_child(&fields[1], source.values(), value, defaults)?;
            check_nullability(key, &keys, None)?;
            check_nullability(value, &values, None)?;
            let ArrowType::Map(target_entries, sorted) = target.data_type() else {
                unreachable!()
            };
            let ArrowType::Struct(target_fields) = target_entries.data_type() else {
                unreachable!()
            };
            let entries_array =
                StructArray::try_new(target_fields.clone(), vec![keys, values], None)?;
            Ok(Arc::new(MapArray::try_new(
                target_entries.clone(),
                source.offsets().clone(),
                entries_array,
                source.nulls().cloned(),
                *sorted,
            )?))
        }
        (ArrowType::Null, _) => Ok(arrow_array::new_null_array(target.data_type(), array.len())),
        _ => {
            let source = Column::try_from(field)?;
            if source.dtype == column.dtype
                || source.dtype.can_promote_to(&column.dtype)
                || matches!(
                    (&source.dtype, &column.dtype),
                    (DataType::Varchar, DataType::Json)
                )
            {
                if array.data_type() == target.data_type() {
                    Ok(array.clone())
                } else {
                    Ok(arrow_cast::cast(array, target.data_type())?)
                }
            } else {
                Err(DucklakeError::InvalidCast {
                    old: source.dtype,
                    new: column.dtype.clone(),
                })
            }
        }
    }
}

fn match_child(
    field: &Field,
    array: &ArrayRef,
    column: &Column,
    defaults: Defaults,
) -> DucklakeResult<ArrayRef> {
    if matches!(defaults, Defaults::InitialDefault)
        && field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .and_then(|id| id.parse::<i64>().ok())
            != column.field_id
    {
        generate_column(column, array.len(), defaults)
    } else {
        match_column(field, array, column, defaults)
    }
}

fn generate_column(column: &Column, len: usize, defaults: Defaults) -> DucklakeResult<ArrayRef> {
    let value = match defaults {
        Defaults::InitialDefault => column.initial_default.as_ref(),
        Defaults::DefaultValue => match &column.default_value {
            ColumnDefault::Literal(value) => value.as_ref(),
            ColumnDefault::Expression { .. } => {
                return Err(DucklakeError::SchemaError(format!(
                    "expression default for '{}' is unsupported",
                    column.name,
                )));
            }
        },
    };
    if let Some(value) = value {
        value.broadcast(&column.dtype, len)
    } else {
        let field = column.to_arrow_field();
        Ok(arrow_array::new_null_array(field.data_type(), len))
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                             TESTS                                             */
/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
mod tests {
    use arrow_array::Int64Array;
    use arrow_array::types::Int64Type;

    use super::*;
    use crate::{Schema, Value};

    #[test]
    fn read_matches_renamed_column_by_id() {
        // Arrange
        let source = Schema::try_from(vec![
            Column::new("old".into(), DataType::Int64).field_id(Some(1)),
        ])
        .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(source.to_arrow()),
            vec![Arc::new(Int64Array::from(vec![7]))],
        )
        .unwrap();
        let target = Schema::try_from(vec![
            Column::new("new".into(), DataType::Int64).field_id(Some(1)),
        ])
        .unwrap();

        // Act
        let result = match_to_schema(&batch, &target, Defaults::InitialDefault).unwrap();

        // Assert
        assert_eq!(result.schema().field(0).name(), "new");
        assert_eq!(result.column(0).as_primitive::<Int64Type>().value(0), 7);
    }

    #[test]
    fn write_fills_missing_column_with_default() {
        // Arrange
        let values: ArrayRef = Arc::new(Int64Array::from(vec![7]));
        let batch = RecordBatch::try_from_iter([("x", values)]).unwrap();
        let schema = Schema::try_from(vec![
            Column::new("x".into(), DataType::Int64),
            Column::new("y".into(), DataType::Int64)
                .default_value(ColumnDefault::Literal(Some(Value::Int64(42)))),
        ])
        .unwrap();

        // Act
        let result = match_to_schema(&batch, &schema, Defaults::DefaultValue).unwrap();

        // Assert
        assert_eq!(result.column(0).as_primitive::<Int64Type>().value(0), 7);
        assert_eq!(result.column(1).as_primitive::<Int64Type>().value(0), 42);
    }
}
