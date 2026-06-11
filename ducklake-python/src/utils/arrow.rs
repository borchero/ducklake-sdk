use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType as ArrowDataType, Field, Schema};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use pyo3::prelude::*;
use pyo3_arrow::PySchema as ArrowPySchema;

use crate::conversion::Wrap;
use crate::error;

/// Convert a list of columns to an object implementing the Arrow PyCapsule.
#[pyfunction]
pub fn schema_to_arrow(columns: Vec<Wrap<ducklake::Column>>) -> PyResult<ArrowPySchema> {
    let fields: Vec<_> = columns.into_iter().map(|c| c.0.to_arrow_field()).collect();
    let schema = Schema::new(fields);
    Ok(Arc::new(schema).into())
}

#[pyfunction]
pub fn schema_from_arrow(schema: ArrowPySchema) -> PyResult<Vec<Wrap<ducklake::Column>>> {
    let schema = schema.into_inner();
    let columns = schema
        .fields()
        .iter()
        .map(|f| ducklake::Column::try_from(&**f).map(Wrap))
        .collect::<Result<Vec<_>, _>>()
        .map_err(error::into_pyerr)?;
    Ok(columns)
}

/// Extract a mapping from parquet field IDs to column names from an Arrow schema.
#[pyfunction]
pub fn arrow_schema_field_ids(schema: ArrowPySchema) -> PyResult<HashMap<i64, String>> {
    let schema = schema.into_inner();
    let mut result = HashMap::new();
    for field in schema.fields() {
        collect_field_ids(field, &mut result)?;
    }
    Ok(result)
}

fn collect_field_ids(field: &Field, result: &mut HashMap<i64, String>) -> PyResult<()> {
    if let Some(raw) = field.metadata().get(PARQUET_FIELD_ID_META_KEY)
        && let Ok(id) = raw.parse::<i64>()
    {
        result.insert(id, field.name().clone());
    }
    match field.data_type() {
        ArrowDataType::LargeList(inner) | ArrowDataType::Map(inner, _) => {
            collect_field_ids(inner, result)?
        }
        ArrowDataType::Struct(fields) => {
            for f in fields {
                collect_field_ids(f, result)?;
            }
        }
        _ => {}
    }
    Ok(())
}
