use super::DataType;
use crate::Value;

/* ------------------------------------------- COLUMN ------------------------------------------ */

/// A column in a table schema.
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    /// The name of the column.
    pub name: String,
    /// The data type of the column.
    pub dtype: DataType,
    /// Whether the column may contain null values.
    pub nullable: bool,
    /// Tags attached to the column.
    pub tags: Vec<crate::Tag>,
    /// The default value used when reading the column from data files written before the column
    /// was added.
    pub initial_default: Option<Value>,
    /// The default value used when inserting new rows that do not specify a value for the column.
    pub default_value: ColumnDefault,
    /// The internal field ID of the column. Set automatically when the column is added to a
    /// table.
    pub field_id: Option<i64>,
}

/// The default value of a column.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnDefault {
    /// A literal value used as the default.
    Literal(Option<Value>),
    /// An expression in the given SQL dialect that is evaluated to derive the default.
    Expression { dialect: String, expression: String },
}

impl Column {
    /// Create a new column with the provided name and data type. By default, the column is
    /// nullable, has no tags, and has no default value.
    pub fn new(name: String, dtype: DataType) -> Self {
        Self {
            name,
            dtype,
            nullable: true,
            tags: Vec::new(),
            initial_default: None,
            default_value: ColumnDefault::Literal(None),
            field_id: None,
        }
    }

    /// Set the nullability of the column.
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Set the tags of the column.
    pub fn tags(mut self, tags: Vec<crate::Tag>) -> Self {
        self.tags = tags;
        self
    }

    /// Set the initial default value of the column.
    pub fn initial_default(mut self, default: Option<Value>) -> Self {
        self.initial_default = default;
        self
    }

    /// Set the default value of the column.
    pub fn default_value(mut self, default: ColumnDefault) -> Self {
        self.default_value = default;
        self
    }

    /// Set the internal field ID of the column.
    pub fn field_id(mut self, field_id: Option<i64>) -> Self {
        self.field_id = field_id;
        self
    }
}

/* ----------------------------------------- FLATTENING ---------------------------------------- */

pub(crate) struct FlattenedColumn {
    /// The flattened column.
    pub column: Column,
    /// The parent index in a vector of flattened columns *for a single original column*.
    pub parent_index: Option<usize>,
}

impl Column {
    /// "Flatten" this column by turning nested types into multiple columns. If the column
    /// references a primitive type, this returns a vector with just this column.
    pub(crate) fn flatten(&self) -> Vec<FlattenedColumn> {
        let mut result = Vec::new();
        Column::flatten_into(self, None, &mut result);
        result
    }

    fn flatten_into(
        column: &Column,
        parent_index: Option<usize>,
        flattened: &mut Vec<FlattenedColumn>,
    ) {
        flattened.push(FlattenedColumn {
            column: column.clone(),
            parent_index,
        });
        let parent_index = flattened.len() - 1;

        match &column.dtype {
            DataType::List(inner) => {
                Column::flatten_into(inner, Some(parent_index), flattened);
            }
            DataType::Struct(fields) => {
                for field in fields {
                    Column::flatten_into(field, Some(parent_index), flattened);
                }
            }
            DataType::Map(key, value) => {
                Column::flatten_into(key, Some(parent_index), flattened);
                Column::flatten_into(value, Some(parent_index), flattened);
            }
            _ => {}
        }
    }
}
