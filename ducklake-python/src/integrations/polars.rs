use std::sync::Arc;

use arrow_schema::{DataType, Schema};
use ducklake::DucklakeResult;
use pyo3::prelude::*;
use pyo3_arrow::PySchema as ArrowPySchema;
use serde::{Deserialize, Serialize};

use crate::conversion::Wrap;
use crate::error;

const COMMENT_TAG: &str = "comment";
const SOURCE_KEY: &str = "ducklake-sdk:polars";

const POLARS_ENUM_METADATA_KEY: &str = "_PL_ENUM_VALUES2";
const POLARS_CATEGORICAL_METADATA_KEY: &str = "_PL_CATEGORICAL2";

#[derive(Deserialize, Serialize, Debug)]
struct Metadata {
    source: String,
    index_type: IndexType,
    polars_metadata: PolarsMetadata,
}
#[derive(Deserialize, Serialize, Copy, Clone, Debug)]
enum IndexType {
    UInt8,
    UInt16,
    UInt32,
}

impl From<&DataType> for IndexType {
    fn from(value: &DataType) -> Self {
        match value {
            DataType::UInt8 => IndexType::UInt8,
            DataType::UInt16 => IndexType::UInt16,
            DataType::UInt32 => IndexType::UInt32,
            _ => panic!(
                "invalid index type for Polars dictionary column: {:?}",
                value
            ),
        }
    }
}

impl From<IndexType> for DataType {
    fn from(value: IndexType) -> Self {
        match value {
            IndexType::UInt8 => DataType::UInt8,
            IndexType::UInt16 => DataType::UInt16,
            IndexType::UInt32 => DataType::UInt32,
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
enum PolarsMetadata {
    Enum(String),
    Categorical(String),
}

/* -------------------------------------- ARROW -> SCHEMA -------------------------------------- */

#[pyfunction]
pub(crate) fn schema_from_arrow_with_polars_metadata(
    schema: ArrowPySchema,
) -> PyResult<Vec<Wrap<ducklake::Column>>> {
    let schema = schema.into_inner();
    let columns = schema
        .fields()
        .iter()
        .map(|f| column_from_arrow_field(f).map(Wrap))
        .collect::<Result<Vec<_>, _>>()
        .map_err(error::into_pyerr)?;
    Ok(columns)
}

fn column_from_arrow_field(field: &arrow_schema::Field) -> DucklakeResult<ducklake::Column> {
    // First, we need to check whether we received a dictionary. This cannot be represented as a
    // ducklake column. Instead, we represent it as a varchar and add column metadata.
    let column = match field.data_type() {
        DataType::Dictionary(key_type, _) => {
            let field = field.clone().with_data_type(DataType::Utf8View);
            let mut column = ducklake::Column::try_from(&field)?;

            let index_type = IndexType::from(&**key_type);
            let polars_metadata =
                if let Some(enum_values) = field.metadata().get(POLARS_ENUM_METADATA_KEY) {
                    PolarsMetadata::Enum(enum_values.clone())
                } else if let Some(cat) = field.metadata().get(POLARS_CATEGORICAL_METADATA_KEY) {
                    PolarsMetadata::Categorical(cat.clone())
                } else {
                    unreachable!("encountered non-enum/non-categorical dictionary column")
                };
            let metadata = Metadata {
                source: SOURCE_KEY.to_string(),
                index_type,
                polars_metadata,
            };

            column.tags.push(ducklake::Tag {
                key: COMMENT_TAG.to_string(),
                value: serde_json::to_string(&metadata).unwrap(),
            });
            column
        }
        _ => ducklake::Column::try_from(field)?,
    };
    Ok(column)
}

/* -------------------------------------- SCHEMA -> ARROW -------------------------------------- */

#[pyfunction]
pub(crate) fn schema_to_arrow_with_polars_metadata(
    columns: Vec<Wrap<ducklake::Column>>,
) -> PyResult<ArrowPySchema> {
    let fields: Vec<_> = columns
        .into_iter()
        .map(|c| arrow_field_from_column(&c.0))
        .collect();
    let schema = Schema::new(fields);
    Ok(Arc::new(schema).into())
}

fn arrow_field_from_column(column: &ducklake::Column) -> arrow_schema::Field {
    // First, we translate the column into its direct Arrow representation
    let field = column.to_arrow_field();

    // Then, we check for the comment tag to see if we should attach enum/categorical metadata
    let comment_tag = column.tags.iter().find(|&t| t.key == COMMENT_TAG);
    if let Some(tag) = comment_tag
        && let Ok(metadata) = serde_json::from_str::<Metadata>(&tag.value)
    {
        let mut field_metadata = field.metadata().clone();
        let mut ordered = false;
        match metadata.polars_metadata {
            PolarsMetadata::Enum(enum_values) => {
                field_metadata.insert(POLARS_ENUM_METADATA_KEY.to_string(), enum_values);
                ordered = true;
            }
            PolarsMetadata::Categorical(categorical) => {
                field_metadata.insert(POLARS_CATEGORICAL_METADATA_KEY.to_string(), categorical);
            }
        }
        return field
            .with_data_type(DataType::Dictionary(
                Box::new(metadata.index_type.into()),
                Box::new(DataType::Utf8View),
            ))
            .with_dict_is_ordered(ordered)
            .with_metadata(field_metadata);
    }

    // If we did not find any valid metadata, we just return the field as-is
    field
}
