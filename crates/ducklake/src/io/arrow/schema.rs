use std::collections::HashMap;
use std::sync::Arc;

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
        if !column.nullable && array.null_count() > 0 {
            return Err(DucklakeError::InvalidNullValue {
                column: column.name.clone(),
            });
        }
    }
    Ok(RecordBatch::try_new_with_options(
        Arc::new(schema.to_arrow()),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?)
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
            let source = array.as_any().downcast_ref::<StructArray>().unwrap();
            let arrays = match_fields(
                source.fields(),
                source.columns(),
                columns,
                source.len(),
                defaults,
            )?;
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
            let source = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let values = match_child(inner, source.values(), column, defaults)?;
            Ok(Arc::new(LargeListArray::try_new(
                Arc::new(column.to_arrow_field()),
                source.offsets().clone(),
                values,
                source.nulls().cloned(),
            )?))
        }
        (ArrowType::Map(_, _), DataType::Map(key, value)) => {
            let source = array.as_any().downcast_ref::<MapArray>().unwrap();
            let fields = source.entries().fields();
            let keys = match_child(&fields[0], source.keys(), key, defaults)?;
            let values = match_child(&fields[1], source.values(), value, defaults)?;
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
            if source.dtype != column.dtype
                && !allows_primitive_promotion(&source.dtype, &column.dtype)
            {
                return Err(DucklakeError::InvalidCast {
                    old: source.dtype,
                    new: column.dtype.clone(),
                });
            }
            if array.data_type() == target.data_type() {
                Ok(array.clone())
            } else {
                Ok(arrow_cast::cast(array, target.data_type())?)
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

fn allows_primitive_promotion(source: &DataType, target: &DataType) -> bool {
    use DataType::*;
    matches!(
        (source, target),
        (Int8, Int16 | Int32 | Int64)
            | (Int16, Int32 | Int64)
            | (Int32, Int64)
            | (UInt8, UInt16 | UInt32 | UInt64)
            | (UInt16, UInt32 | UInt64)
            | (UInt32, UInt64)
            | (Float32, Float64)
            | (Varchar, Json)
    )
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
        return value.broadcast_into_array(&column.dtype, len);
    }
    let field = column.to_arrow_field();
    if matches!(defaults, Defaults::InitialDefault)
        && let DataType::Struct(columns) = &column.dtype
    {
        let arrays = columns
            .iter()
            .map(|column| generate_column(column, len, defaults))
            .collect::<DucklakeResult<_>>()?;
        let ArrowType::Struct(fields) = field.data_type() else {
            unreachable!()
        };
        return Ok(Arc::new(StructArray::try_new_with_length(
            fields.clone(),
            arrays,
            None,
            len,
        )?));
    }
    Ok(arrow_array::new_null_array(field.data_type(), len))
}

#[cfg(test)]
mod tests {
    use arrow_array::builder::{Int32Builder, ListBuilder, StructBuilder};
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_array::{Int32Array, Int64Array, StringArray};
    use rstest::rstest;

    use super::*;
    use crate::{Schema, Value};

    #[test]
    fn read_matches_ids_and_preserves_target_order() {
        // Arrange
        let old = Schema::try_from(vec![
            Column::new("a".into(), DataType::Int32).field_id(Some(1)),
            Column::new("b".into(), DataType::Int64).field_id(Some(2)),
        ])
        .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(old.to_arrow()),
            vec![
                Arc::new(Int32Array::from(vec![Some(7), None])),
                Arc::new(Int64Array::from(vec![100, 200])),
            ],
        )
        .unwrap();
        let target = Schema::try_from(vec![
            Column::new("z".into(), DataType::Int64).field_id(Some(1)),
            Column::new("b".into(), DataType::Int64)
                .field_id(Some(3))
                .initial_default(Some(Value::Int64(42))),
        ])
        .unwrap();

        // Act
        let result = match_to_schema(&batch, &target, Defaults::InitialDefault).unwrap();

        // Assert
        assert_eq!(result.schema().as_ref(), &target.to_arrow());
        assert_eq!(
            result.column(0).as_primitive::<Int64Type>(),
            &Int64Array::from(vec![Some(7), None])
        );
        assert_eq!(
            result.column(1).as_primitive::<Int64Type>(),
            &Int64Array::from(vec![42, 42])
        );
    }

    #[test]
    fn write_reorders_struct_fields_and_normalizes_strings() {
        // Arrange
        let fields: Fields = vec![
            Field::new("z", ArrowType::Utf8, true),
            Field::new("a", ArrowType::Int32, true),
        ]
        .into();
        let array = StructArray::new(
            fields,
            vec![
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(Int32Array::from(vec![7])),
            ],
            None,
        );
        let source = Field::new("s", array.data_type().clone(), true);
        let target = Column::new(
            "s".into(),
            DataType::Struct(vec![
                Column::new("a".into(), DataType::Int64),
                Column::new("z".into(), DataType::Varchar),
            ]),
        );

        // Act
        let result = match_column(
            &source,
            &(Arc::new(array) as ArrayRef),
            &target,
            Defaults::DefaultValue,
        )
        .unwrap();

        // Assert
        assert_eq!(result.data_type(), target.to_arrow_field().data_type());
        assert_eq!(
            result
                .as_struct()
                .column(0)
                .as_primitive::<Int64Type>()
                .value(0),
            7
        );
        assert_eq!(
            result.as_struct().column(1).as_string_view().value(0),
            "hello"
        );
    }

    #[test]
    fn read_evolves_sliced_lists_and_preserves_nested_nulls() {
        // Arrange
        let old_child = Column::new("a".into(), DataType::Int32).field_id(Some(3));
        let fields: Fields = vec![old_child.to_arrow_field()].into();
        let mut builder = ListBuilder::new(StructBuilder::new(
            fields.clone(),
            vec![Box::new(Int32Builder::new())],
        ))
        .with_field(
            Column::new("element".into(), DataType::Struct(vec![old_child]))
                .field_id(Some(2))
                .to_arrow_field(),
        );
        for (value, valid) in [(100, true), (7, true), (0, false)] {
            builder
                .values()
                .field_builder::<Int32Builder>(0)
                .unwrap()
                .append_value(value);
            builder.values().append(valid);
            builder.append(true);
        }
        builder.append(false);
        let array: ArrayRef = Arc::new(builder.finish().slice(1, 3));
        let source = Field::new("list", array.data_type().clone(), true);
        let target = Column::new(
            "list".into(),
            DataType::List(Box::new(
                Column::new(
                    "element".into(),
                    DataType::Struct(vec![
                        Column::new("z".into(), DataType::Int64).field_id(Some(3)),
                        Column::new("a".into(), DataType::Int64)
                            .field_id(Some(4))
                            .initial_default(Some(Value::Int64(42))),
                    ]),
                )
                .field_id(Some(2)),
            )),
        );

        // Act
        let result = match_column(&source, &array, &target, Defaults::InitialDefault).unwrap();

        // Assert
        let list = result.as_list::<i64>();
        assert_eq!(list.len(), 3);
        assert!(list.is_null(2));
        let first = list.value(0);
        assert_eq!(
            first
                .as_struct()
                .column(0)
                .as_primitive::<Int64Type>()
                .value(0),
            7
        );
        assert_eq!(
            first
                .as_struct()
                .column(1)
                .as_primitive::<Int64Type>()
                .value(0),
            42
        );
        assert!(list.value(1).as_struct().is_null(0));
    }

    #[test]
    fn read_evolves_sliced_maps() {
        // Arrange
        let key = Column::new("key".into(), DataType::Int32)
            .nullable(false)
            .field_id(Some(2));
        let value = Column::new("value".into(), DataType::Int32).field_id(Some(3));
        let mut builder =
            arrow_array::builder::MapBuilder::new(None, Int32Builder::new(), Int32Builder::new())
                .with_keys_field(key.to_arrow_field())
                .with_values_field(value.to_arrow_field());
        for (key, value) in [(100, Some(200)), (7, None)] {
            builder.keys().append_value(key);
            builder.values().append_option(value);
            builder.append(true).unwrap();
        }
        builder.append(false).unwrap();
        let array: ArrayRef = Arc::new(builder.finish().slice(1, 2));
        let field = Field::new("map", array.data_type().clone(), true);
        let target = Column::new(
            "map".into(),
            DataType::Map(
                Box::new(Column {
                    dtype: DataType::Int64,
                    ..key
                }),
                Box::new(Column {
                    dtype: DataType::Int64,
                    ..value
                }),
            ),
        );

        // Act
        let result = match_column(&field, &array, &target, Defaults::InitialDefault).unwrap();

        // Assert
        let map = result.as_map();
        assert_eq!(map.len(), 2);
        assert!(map.is_null(1));
        assert_eq!(
            map.value(0).column(0).as_primitive::<Int64Type>().value(0),
            7
        );
        assert!(map.value(0).column(1).is_null(0));
    }

    #[rstest]
    #[case::read(Defaults::InitialDefault)]
    #[case::write(Defaults::DefaultValue)]
    fn defaults_only_fill_missing_fields(#[case] defaults: Defaults) {
        // Arrange
        let column = Column::new("x".into(), DataType::Int64)
            .field_id(Some(1))
            .initial_default(Some(Value::Int64(42)))
            .default_value(ColumnDefault::Literal(Some(Value::Int64(43))));
        let schema = Schema::try_from(vec![column]).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(schema.to_arrow()),
            vec![Arc::new(Int64Array::from(vec![None, Some(7)]))],
        )
        .unwrap();
        let empty = RecordBatch::try_new_with_options(
            Arc::new(arrow_schema::Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(2)),
        )
        .unwrap();

        // Act
        let supplied = match_to_schema(&batch, &schema, defaults).unwrap();
        let missing = match_to_schema(&empty, &schema, defaults).unwrap();

        // Assert
        assert!(supplied.column(0).is_null(0));
        assert!(Arc::ptr_eq(batch.column(0), supplied.column(0)));
        let expected = match defaults {
            Defaults::InitialDefault => 42,
            Defaults::DefaultValue => 43,
        };
        assert_eq!(
            missing
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .as_ref(),
            &[expected, expected]
        );
    }

    #[rstest]
    fn struct_nullability_respects_parent(#[values(false, true)] parent_valid: bool) {
        // Arrange
        let array: ArrayRef = Arc::new(StructArray::new(
            vec![Field::new("x", ArrowType::Int64, true)].into(),
            vec![Arc::new(Int64Array::from(vec![None]))],
            Some(vec![parent_valid].into()),
        ));
        let field = Field::new("s", array.data_type().clone(), true);
        let column = Column::new(
            "s".into(),
            DataType::Struct(vec![
                Column::new("x".into(), DataType::Int64).nullable(false),
            ]),
        );

        // Act
        let result = match_column(&field, &array, &column, Defaults::DefaultValue);

        // Assert
        assert_eq!(result.is_ok(), !parent_valid);
    }

    #[test]
    fn omitted_struct_write_is_null_even_with_nonnullable_children() {
        // Arrange
        let column = Column::new(
            "s".into(),
            DataType::Struct(vec![
                Column::new("x".into(), DataType::Int64).nullable(false),
            ]),
        );

        // Act
        let array = generate_column(&column, 2, Defaults::DefaultValue).unwrap();

        // Assert
        assert_eq!(array.null_count(), 2);
        assert_eq!(array.data_type(), column.to_arrow_field().data_type());
    }

    #[rstest]
    #[case::narrowing(DataType::Int8, true, Some(300))]
    #[case::null(DataType::Int64, false, None)]
    fn invalid_writes_return_errors(
        #[case] dtype: DataType,
        #[case] nullable: bool,
        #[case] value: Option<i64>,
    ) {
        // Arrange
        let batch = RecordBatch::try_from_iter(vec![(
            "x",
            Arc::new(Int64Array::from(vec![value])) as ArrayRef,
        )])
        .unwrap();
        let schema =
            Schema::try_from(vec![Column::new("x".into(), dtype).nullable(nullable)]).unwrap();

        // Act
        let result = match_to_schema(&batch, &schema, Defaults::DefaultValue);

        // Assert
        assert!(matches!(
            result,
            Err(DucklakeError::InvalidCast { .. } | DucklakeError::InvalidNullValue { .. })
        ));
    }
}
