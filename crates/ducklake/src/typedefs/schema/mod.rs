mod arrow;
mod column;
mod dtype;

use std::collections::HashMap;

pub use column::{Column, ColumnDefault};
pub use dtype::{DataType, TimestampPrecision};
use indexmap::IndexMap;
use indexmap::map::Entry;

use crate::{DucklakeError, DucklakeResult};

#[derive(Debug, Clone)]
pub(crate) struct Schema {
    pub columns: IndexMap<String, Column>,
}

impl Schema {
    pub(crate) fn columns_by_id(&self) -> HashMap<i64, &Column> {
        fn walk<'a>(column: &'a Column, result: &mut HashMap<i64, &'a Column>) {
            if let Some(id) = column.field_id {
                result.insert(id, column);
            }
            match column.dtype {
                DataType::List(ref inner) => walk(inner, result),
                DataType::Struct(ref inner) => {
                    for child in inner {
                        walk(child, result);
                    }
                }
                DataType::Map(ref key, ref value) => {
                    walk(key, result);
                    walk(value, result);
                }
                _ => {}
            }
        }

        let mut result = HashMap::new();
        for column in self.columns.values() {
            walk(column, &mut result);
        }
        result
    }
}

impl TryFrom<Vec<Column>> for Schema {
    type Error = DucklakeError;

    fn try_from(columns: Vec<Column>) -> DucklakeResult<Self> {
        let mut result = IndexMap::new();
        for col in columns {
            match result.entry(col.name.clone()) {
                Entry::Occupied(_) => {
                    return Err(DucklakeError::DuplicateColumnName(col.name));
                }
                Entry::Vacant(entry) => {
                    entry.insert(col);
                }
            }
        }
        Ok(Self { columns: result })
    }
}
