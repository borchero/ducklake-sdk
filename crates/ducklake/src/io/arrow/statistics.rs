use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType as ArrowDataType;

use super::aggregate;
use crate::{ArrayColumnStats, DataType, RecordBatchStatistics};

pub fn compute_record_batch_statistics(
    schema: &crate::Schema,
    record_batch: &RecordBatch,
) -> RecordBatchStatistics {
    let mut column_stats = HashMap::new();
    for (field, data) in record_batch
        .schema()
        .fields()
        .iter()
        .zip(record_batch.columns())
    {
        let Some(column) = schema.columns.get(field.name()) else {
            continue;
        };
        compute_array_statistics(column, data, &mut column_stats);
    }
    RecordBatchStatistics { column_stats }
}

fn compute_array_statistics(
    column: &crate::Column,
    array: &Arc<dyn Array>,
    result: &mut HashMap<i64, ArrayColumnStats>,
) {
    // Nested types do not have table stats on their own. Instead, we simply recurse into their
    // children. Primitive leaves always generate stats.
    match column.dtype {
        DataType::List(ref inner) => {
            // For a sliced list array, `.values()` returns the full underlying child array,
            // ignoring the slice. We must restrict the child to the range actually covered by
            // the (possibly sliced) parent's offsets.
            macro_rules! sliced_list_values {
                ($ty:ty) => {{
                    let list = array.as_any().downcast_ref::<$ty>().unwrap();
                    let offsets = list.offsets();
                    let start = offsets[0] as usize;
                    let end = offsets[list.len()] as usize;
                    list.values().slice(start, end - start)
                }};
            }
            let child = match array.data_type() {
                ArrowDataType::List(_) => sliced_list_values!(arrow_array::ListArray),
                ArrowDataType::LargeList(_) => sliced_list_values!(arrow_array::LargeListArray),
                _ => unreachable!(),
            };
            compute_array_statistics(inner, &child, result);
        }
        DataType::Struct(ref fields) => {
            let struct_array = array
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .unwrap();
            for (field, column) in fields.iter().zip(struct_array.columns()) {
                compute_array_statistics(field, column, result);
            }
        }
        DataType::Map(ref key, ref value) => {
            let map_array = array
                .as_any()
                .downcast_ref::<arrow_array::MapArray>()
                .unwrap();
            let entries = map_array.entries();
            let key_column = entries
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .unwrap()
                .column(0);
            let value_column = entries
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .unwrap()
                .column(1);
            compute_array_statistics(key, key_column, result);
            compute_array_statistics(value, value_column, result);
        }
        _ => {
            let Some(id) = column.field_id else { return };
            let stats = ArrayColumnStats {
                min_value: aggregate::find_min(&column.dtype, array),
                max_value: aggregate::find_max(&column.dtype, array),
                null_count: Some(array.null_count()),
                contains_nan: None,
            };
            result.insert(id, stats);
        }
    }
}
