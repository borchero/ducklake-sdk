use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType as ArrowDataType, Field, Schema};
use ducklake::DucklakeResult;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use pyo3::prelude::*;
use pyo3_arrow::PySchema as ArrowPySchema;
use serde::{Deserialize, Serialize};

use crate::conversion::Wrap;
use crate::error;

const COMMENT_TAG: &str = "comment";
const SOURCE_KEY: &str = "ducklake-sdk:arrow-field-metadata:v1";

#[derive(Serialize, Deserialize)]
struct ArrowFieldMetadata {
    /// The true "key" of the tag. Currently, the official DuckLake DuckDB extension does not
    /// support any key but 'comment'.
    source: String,
    /// Optional data type for dictionary columns which are not currently supported by DuckLake.
    /// Stores the full data type and whether it is ordered.
    dictionary: Option<(ArrowDataType, bool)>,
    /// Arbitrary metadata attached to the field.
    metadata: HashMap<String, String>,
}

/* -------------------------------------- SCHEMA -> ARROW -------------------------------------- */

/// Convert a list of columns to an object implementing the Arrow PyCapsule interface.
#[pyfunction]
pub(crate) fn schema_to_arrow(columns: Vec<Wrap<ducklake::Column>>) -> PyResult<ArrowPySchema> {
    let fields: Vec<_> = columns
        .into_iter()
        .map(|c| arrow_field_from_column(&c.0))
        .collect();
    let schema = Schema::new(fields);
    Ok(Arc::new(schema).into())
}

fn arrow_field_from_column(column: &ducklake::Column) -> Field {
    // First, we translate the column into its direct Arrow representation
    let field = column.to_arrow_field();

    // Then, we check for the comment tag to see if we need to attach metadata and/or convert to
    // a dictionary type
    let comment_tag = column.tags.iter().find(|&t| t.key == COMMENT_TAG);
    if let Some(tag) = comment_tag
        && let Ok(metadata) = serde_json::from_str::<ArrowFieldMetadata>(&tag.value)
        && metadata.source == SOURCE_KEY
    {
        let field = if metadata.metadata.is_empty() {
            field
        } else {
            let mut new_metadata = field.metadata().clone();
            new_metadata.extend(metadata.metadata);
            field.with_metadata(new_metadata)
        };
        if let Some((data_type, ordered)) = metadata.dictionary {
            return field
                .with_data_type(data_type)
                .with_dict_is_ordered(ordered);
        }
        return field;
    }
    field
}

/* -------------------------------------- ARROW -> SCHEMA -------------------------------------- */

#[pyfunction]
pub(crate) fn schema_from_arrow(schema: ArrowPySchema) -> PyResult<Vec<Wrap<ducklake::Column>>> {
    let schema = schema.into_inner();
    let columns = schema
        .fields()
        .iter()
        .map(|f| column_from_arrow_field(f).map(Wrap))
        .collect::<Result<Vec<_>, _>>()
        .map_err(error::into_pyerr)?;
    Ok(columns)
}

fn column_from_arrow_field(field: &Field) -> DucklakeResult<ducklake::Column> {
    // If we encounter a dictionary type, we want to treat it just like the value type
    // (typically a string), but persist metadata about it
    let (mut column, dictionary) = match field.data_type() {
        ArrowDataType::Dictionary(_, value_type) => {
            let original_dtype = (field.data_type().clone(), field.dict_is_ordered().unwrap());
            let field = field.clone().with_data_type((**value_type).clone());
            (ducklake::Column::try_from(&field)?, Some(original_dtype))
        }
        _ => (ducklake::Column::try_from(field)?, None),
    };

    // In any case, we persist field metadata if there is any
    if dictionary.is_some() || !field.metadata().is_empty() {
        let metadata = ArrowFieldMetadata {
            source: SOURCE_KEY.to_string(),
            dictionary,
            metadata: field.metadata().clone(),
        };
        column.tags.push(ducklake::Tag {
            key: COMMENT_TAG.to_string(),
            value: serde_json::to_string(&metadata).unwrap(),
        });
    }
    Ok(column)
}

/* ----------------------------------------- FIELD IDS ----------------------------------------- */

/// Extract a mapping from parquet field IDs to column names from an Arrow schema.
#[pyfunction]
pub(crate) fn arrow_schema_field_ids(schema: ArrowPySchema) -> PyResult<HashMap<i64, String>> {
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
