use std::collections::HashMap;
use std::sync::LazyLock;

use indexmap::IndexMap;
use itertools::Itertools;
use regex::Regex;

use super::*;
use crate::spec::*;
use crate::{DucklakeError, DucklakeResult};

#[derive(Debug, Clone)]
pub(in crate::catalog) struct CatalogColumns {
    /// Arena holding all columns, including nested ones, in a flat structure.
    pub arena: Vec<CatalogColumn>,
    /// Mapping from column ID to arena index for quick lookup by ID.
    /// Does not include pending columns as they do not have an ID yet.
    pub by_id: HashMap<i64, ArenaIdx>,
    /// Arena indices of the root (top-level) columns, ordered by the order in the schema.
    pub root_columns: IndexMap<String, ArenaIdx>,
}

/// A column within a table in a catalog.
///
/// The column might either be a root column or a child column of a nested type.
#[derive(Debug, Clone)]
pub(in crate::catalog) struct CatalogColumn {
    pub state: CatalogState,
    pub name: String,
    pub dtype: CatalogDataType,
    pub parent_column: Option<ArenaIdx>,
    pub nullable: bool,
    pub tags: Vec<crate::Tag>,
    pub initial_default: Option<crate::Value>,
    pub default_value: crate::ColumnDefault,
}

#[derive(Debug, Clone)]
pub(in crate::catalog) enum CatalogDataType {
    Primitive(crate::DataType),
    List(ArenaIdx),
    Struct(IndexMap<String, ArenaIdx>),
    Map(ArenaIdx, ArenaIdx),
}

/* -------------------------------------------- INFO ------------------------------------------- */

impl CatalogColumns {
    pub fn root_column_indices(&self) -> Vec<Vec<ArenaIdx>> {
        let mut result: Vec<Vec<ArenaIdx>> = Vec::new();
        for idx in self.root_columns.values() {
            let mut column_indices = Vec::new();
            self.aggregate_column_indices(*idx, &mut column_indices);
            result.push(column_indices);
        }
        result
    }

    fn aggregate_column_indices(&self, idx: ArenaIdx, result: &mut Vec<ArenaIdx>) {
        result.push(idx);
        match &self.arena[idx.0].dtype {
            CatalogDataType::Struct(fields) => {
                for field_idx in fields.values() {
                    self.aggregate_column_indices(*field_idx, result);
                }
            }
            CatalogDataType::List(item_idx) => {
                self.aggregate_column_indices(*item_idx, result);
            }
            CatalogDataType::Map(key_idx, value_idx) => {
                self.aggregate_column_indices(*key_idx, result);
                self.aggregate_column_indices(*value_idx, result);
            }
            _ => {}
        }
    }

    pub fn arena_idx_by_path(&self, path: &[String]) -> DucklakeResult<ArenaIdx> {
        let mut idx = *self
            .root_columns
            .get(&path[0])
            .ok_or_else(|| DucklakeError::column_path_not_found(path))?;
        for part in &path[1..] {
            let col = &self.arena[idx.0];
            match &col.dtype {
                CatalogDataType::Struct(fields) => {
                    idx = *fields
                        .get(part)
                        .ok_or_else(|| DucklakeError::column_path_not_found(path))?;
                }
                CatalogDataType::List(list_idx) if part == "element" => idx = *list_idx,
                CatalogDataType::Map(key_idx, _) if part == "key" => idx = *key_idx,
                CatalogDataType::Map(_, value_idx) if part == "value" => idx = *value_idx,
                _ => {
                    return Err(DucklakeError::column_path_not_found(path));
                }
            }
        }
        Ok(idx)
    }
}

/* ------------------------------------------ CHANGES ------------------------------------------ */

impl CatalogColumns {
    // --- RENAME ---

    pub fn rename_column(&mut self, idx: ArenaIdx, new_name: &str) -> DucklakeResult<ArenaIdx> {
        // Check that the new name is unique among the column's siblings
        let parent = self.arena[idx.0].parent_column;
        self.ensure_column_unique_at_parent(parent, new_name)?;

        // If unique, rename
        let column = &mut self.arena[idx.0];
        column.name = new_name.to_string();
        Ok(idx)
    }

    fn ensure_column_unique_at_parent(
        &self,
        parent: Option<ArenaIdx>,
        name: &str,
    ) -> DucklakeResult<()> {
        let exists = match parent {
            None => self.root_columns.contains_key(name),
            Some(idx) => match &self.arena[idx.0].dtype {
                CatalogDataType::Struct(fields) => fields.contains_key(name),
                _ => unreachable!(),
            },
        };
        if exists {
            return Err(DucklakeError::column_already_exists(name));
        }
        Ok(())
    }

    // --- REMOVE ---

    pub fn remove_column(&mut self, idx: ArenaIdx) -> DucklakeResult<Vec<ArenaIdx>> {
        let mut deletions = Vec::new();
        self.mark_column_deleted(idx, &mut deletions)?;
        Ok(deletions)
    }

    fn mark_column_deleted(
        &mut self,
        idx: ArenaIdx,
        deletions: &mut Vec<ArenaIdx>,
    ) -> DucklakeResult<()> {
        // Get the column and mark it as deleted
        let column = &mut self.arena[idx.0];
        match &column.state {
            CatalogState::Existing { id } => {
                self.by_id.remove(id);
                column.state = CatalogState::Deleted { id: *id };
            }
            CatalogState::Pending => {
                return Err(DucklakeError::InvalidChanges(
                    "cannot delete column which was created in the same transaction".to_string(),
                ));
            }
            CatalogState::Deleted { .. } => {
                return Err(DucklakeError::InvalidChanges(
                    "cannot delete column which is already deleted".to_string(),
                ));
            }
        }
        deletions.push(idx);

        // Then, recursively mark child columns as deleted
        let dtype = column.dtype.clone();
        match dtype {
            CatalogDataType::Struct(fields) => {
                for field_idx in fields.values() {
                    self.mark_column_deleted(*field_idx, deletions)?;
                }
            }
            CatalogDataType::List(item_idx) => {
                self.mark_column_deleted(item_idx, deletions)?;
            }
            CatalogDataType::Map(key_idx, value_idx) => {
                self.mark_column_deleted(key_idx, deletions)?;
                self.mark_column_deleted(value_idx, deletions)?;
            }
            _ => {}
        }
        Ok(())
    }

    // --- ADD ---

    pub fn add_column(
        &mut self,
        parent_idx: Option<ArenaIdx>,
        column: crate::Column,
    ) -> DucklakeResult<Vec<ArenaIdx>> {
        // Check that the column name is unique at the given path
        self.ensure_column_unique_at_parent(parent_idx, &column.name)?;

        // If so, add the column to the arena - this does not "register" the column in the "tree"
        // of columns yet
        let column_name = column.name.clone();
        let arena_idxs = self.add_column_to_arena(column, parent_idx);

        // We either add the column as a root column or as a struct field
        if let Some(pidx) = parent_idx {
            // The new column is a struct field, add it there
            let parent = &mut self.arena[pidx.0];
            match parent.dtype {
                CatalogDataType::Struct(ref mut fields) => {
                    fields.insert(column_name, arena_idxs[0]);
                }
                // SAFETY: Cannot be reached as `ensure_column_unique_at_parent` already makes sure
                //  we can only encounter a struct
                _ => unreachable!("parent column is not a struct"),
            }
        } else {
            // The new column is a root column, add it to the root columns
            self.root_columns.insert(column_name, arena_idxs[0]);
        }

        // Return the arena indices of all added columns
        Ok(arena_idxs)
    }

    fn add_column_to_arena(
        &mut self,
        column: crate::Column,
        parent: Option<ArenaIdx>,
    ) -> Vec<ArenaIdx> {
        use crate::DataType::*;

        let mut result: Vec<ArenaIdx> = Vec::new();
        let first_idx = self.arena.len();

        // IMPORTANT: Keep using `flatten` here to ensure that the series of indices matches the
        //  order of the columns during the commit apply. This also eliminates duplication wrt.
        //  column name assignment.
        let flattened_columns = column.flatten();

        let mut children_by_parent: HashMap<usize, Vec<_>> = HashMap::new();
        for (i, col) in flattened_columns.iter().enumerate() {
            if let Some(idx) = col.parent_index {
                children_by_parent
                    .entry(idx)
                    .or_default()
                    .push(ArenaIdx(first_idx + i));
            }
        }

        for (idx, flat_column) in flattened_columns.into_iter().enumerate() {
            let dtype = match flat_column.column.dtype {
                List(_) => CatalogDataType::List(children_by_parent[&idx][0]),
                Struct(columns) => CatalogDataType::Struct(
                    columns
                        .into_iter()
                        .zip(children_by_parent[&idx].iter().cloned())
                        .map(|(field, idx)| (field.name, idx))
                        .collect(),
                ),
                Map(_, _) => {
                    let children = &children_by_parent[&idx];
                    CatalogDataType::Map(children[0], children[1])
                }
                dtype => CatalogDataType::Primitive(dtype),
            };
            let parent_column = flat_column
                .parent_index
                .map(|i| ArenaIdx(first_idx + i))
                .or(parent);
            let catalog_column = CatalogColumn {
                state: CatalogState::Pending,
                parent_column,
                name: flat_column.column.name,
                dtype,
                nullable: flat_column.column.nullable,
                tags: flat_column.column.tags,
                initial_default: flat_column.column.initial_default,
                default_value: flat_column.column.default_value,
            };
            let idx = self.push_column(catalog_column);
            result.push(idx);
        }
        result
    }
}

/* ----------------------------------------- INTERNALS ----------------------------------------- */

impl CatalogColumns {
    fn push_column(&mut self, column: CatalogColumn) -> ArenaIdx {
        let idx = ArenaIdx(self.arena.len());
        self.arena.push(column);
        idx
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                           TRANSFORM                                           */
/* --------------------------------------------------------------------------------------------- */

/* ------------------------------------------ COLUMNS ------------------------------------------ */

impl CatalogColumns {
    fn new() -> Self {
        Self {
            arena: Vec::new(),
            by_id: HashMap::new(),
            root_columns: IndexMap::new(),
        }
    }

    pub fn from_ducklake(
        mut columns: Vec<DucklakeColumn>,
        mut tags: Vec<DucklakeColumnTag>,
    ) -> DucklakeResult<Self> {
        // First, we want to sort the columns to make sure that root columns are ordered correctly
        columns.sort_by_key(|col| col.column_order);

        // Pre-compute a mapping from column ID to arena index. After sorting, the position in the
        // input vector matches the column's eventual position in the arena.
        let id_to_arena_idx: HashMap<i64, ArenaIdx> = columns
            .iter()
            .enumerate()
            .map(|(idx, col)| (col.column_id, ArenaIdx(idx)))
            .collect();

        // Group child columns by their parent to easily assign arena indices in data types
        let mut children_by_parent: HashMap<i64, Vec<_>> = HashMap::new();
        for (idx, col) in columns.iter().enumerate() {
            if let Some(parent_id) = col.parent_column {
                children_by_parent
                    .entry(parent_id)
                    .or_default()
                    .push((col.clone(), ArenaIdx(idx)));
            }
        }

        // Iterate over all columns and turn them into catalog columns. While doing so, we
        // populate all the fields of the return type. As we mirror the iteration order from
        // above, the `idx` matches the one used for grouping children.
        let mut result = Self::new();
        for (idx, col) in columns.into_iter().enumerate() {
            let column_id = col.column_id;
            let column_name = col.column_name.clone();
            let parent_column = col.parent_column.map(|pid| id_to_arena_idx[&pid]);
            let is_root = parent_column.is_none();

            let catalog_column = CatalogColumn::from_ducklake(
                col,
                parent_column,
                &mut children_by_parent,
                &mut tags,
            )?;

            result.arena.push(catalog_column);
            // NOTE: `idx` is exactly the index in the arena at this point
            let arena_idx = ArenaIdx(idx);
            result.by_id.insert(column_id, arena_idx);
            if is_root {
                if result.root_columns.contains_key(&column_name) {
                    return Err(DucklakeError::column_already_exists(&column_name));
                }
                result.root_columns.insert(column_name, arena_idx);
            }
        }
        Ok(result)
    }
}

impl From<crate::Schema> for CatalogColumns {
    fn from(value: crate::Schema) -> Self {
        let mut result = Self::new();
        for column in value.columns.into_values() {
            let column_name = column.name.clone();
            let idxs = result.add_column_to_arena(column, None);
            result.root_columns.insert(column_name, idxs[0]);
        }
        result
    }
}

impl From<&CatalogColumns> for crate::Schema {
    fn from(value: &CatalogColumns) -> Self {
        let mut columns = Vec::new();
        for idx in value.root_columns.values() {
            if let Some(column) = value.schema_column_from_arena_index(*idx) {
                columns.push(column);
            }
        }
        columns
            .try_into()
            .expect("unexpectedly found duplicate column names in catalog")
    }
}

impl From<&CatalogColumns> for HashMap<i64, crate::DataType> {
    fn from(value: &CatalogColumns) -> Self {
        let mut result = HashMap::new();
        for idx in value.root_columns.values() {
            value.collect_existing_dtypes(*idx, &mut result);
        }
        result
    }
}

impl CatalogColumns {
    fn collect_existing_dtypes(&self, idx: ArenaIdx, result: &mut HashMap<i64, crate::DataType>) {
        let column = &self.arena[idx.0];
        if let CatalogState::Existing { id } = column.state {
            result.insert(id, self.schema_dtype(&column.dtype));
        }
        match &column.dtype {
            CatalogDataType::Struct(fields) => {
                for field_idx in fields.values() {
                    self.collect_existing_dtypes(*field_idx, result);
                }
            }
            CatalogDataType::List(item_idx) => {
                self.collect_existing_dtypes(*item_idx, result);
            }
            CatalogDataType::Map(key_idx, value_idx) => {
                self.collect_existing_dtypes(*key_idx, result);
                self.collect_existing_dtypes(*value_idx, result);
            }
            CatalogDataType::Primitive(_) => {}
        }
    }
}

impl CatalogColumns {
    pub fn schema_column_from_arena_index(&self, idx: ArenaIdx) -> Option<crate::Column> {
        if let CatalogState::Deleted { .. } = self.arena[idx.0].state {
            return None;
        }
        let col = &self.arena[idx.0];
        let column = crate::Column {
            name: col.name.clone(),
            dtype: self.schema_dtype(&col.dtype),
            nullable: col.nullable,
            tags: col.tags.clone(),
            initial_default: col.initial_default.clone(),
            default_value: col.default_value.clone(),
            field_id: col.state.id(),
        };
        Some(column)
    }

    fn schema_dtype(&self, dtype: &CatalogDataType) -> crate::DataType {
        match dtype {
            CatalogDataType::Primitive(p) => p.clone(),
            CatalogDataType::List(item_idx) => crate::DataType::List(Box::new(
                // SAFETY: We can unwrap here because the list element cannot be deleted
                self.schema_column_from_arena_index(*item_idx).unwrap(),
            )),
            CatalogDataType::Struct(fields) => {
                let field_columns = fields
                    .values()
                    .flat_map(|idx| self.schema_column_from_arena_index(*idx))
                    .collect();
                crate::DataType::Struct(field_columns)
            }
            CatalogDataType::Map(key_idx, value_idx) => crate::DataType::Map(
                // SAFETY: We can unwrap here because map key and value cannot be deleted
                Box::new(self.schema_column_from_arena_index(*key_idx).unwrap()),
                Box::new(self.schema_column_from_arena_index(*value_idx).unwrap()),
            ),
        }
    }
}

/* ------------------------------------------- COLUMN ------------------------------------------ */

impl CatalogColumn {
    fn from_ducklake(
        col: DucklakeColumn,
        parent_column: Option<ArenaIdx>,
        children_by_parent: &mut HashMap<i64, Vec<(DucklakeColumn, ArenaIdx)>>,
        tags: &mut Vec<DucklakeColumnTag>,
    ) -> DucklakeResult<Self> {
        let column_id = col.column_id;
        let column_type = col.column_type;

        let dtype = match column_type.as_str() {
            "list" => {
                let children = children_by_parent.remove(&column_id).unwrap_or_default();
                if children.len() != 1 {
                    return Err(DucklakeError::InvalidDataType(format!(
                        "list must have exactly one child element but found {}",
                        children.len()
                    )));
                }
                CatalogDataType::List(children[0].1)
            }
            "struct" => {
                let children = children_by_parent.remove(&column_id).unwrap_or_default();
                // NOTE: Sort to preserve field order
                let sorted_children = children
                    .into_iter()
                    .sorted_by_key(|(field, _)| field.column_order)
                    .collect_vec();
                let mut fields: IndexMap<String, ArenaIdx> = IndexMap::new();
                for (field, idx) in sorted_children {
                    let field_name = field.column_name;
                    if fields.contains_key(&field_name) {
                        return Err(DucklakeError::column_already_exists(&field_name));
                    }
                    fields.insert(field_name, idx);
                }
                CatalogDataType::Struct(fields)
            }
            "map" => {
                let children = children_by_parent.remove(&column_id).unwrap_or_default();
                if children.len() != 2 {
                    return Err(DucklakeError::InvalidDataType(format!(
                        "map must have exactly two child elements (key and value) but found {}",
                        children.len()
                    )));
                }
                let (mut keys, mut values): (Vec<_>, Vec<_>) = children
                    .into_iter()
                    .partition(|(c, _)| c.column_name == "key");
                let key_child = keys.pop().ok_or(DucklakeError::InvalidDataType(
                    "map must have a child element named 'key'".into(),
                ))?;
                let value_child = values.pop().ok_or(DucklakeError::InvalidDataType(
                    "map must have a child element named 'value'".into(),
                ))?;
                CatalogDataType::Map(key_child.1, value_child.1)
            }
            s => CatalogDataType::Primitive(parse_primitive_dtype(s)?),
        };

        // Parse initial_default
        let initial_default = match (col.initial_default, &dtype) {
            (Some(val), CatalogDataType::Primitive(p)) => crate::Value::parse(&p.clone(), &val)?,
            (Some(_), _) => {
                return Err(DucklakeError::InvalidDefault {
                    column: col.column_name,
                    reason: "default value for non-primitive type is not supported",
                });
            }
            _ => None,
        };

        // Parse default_value
        let default_value = match col.default_value_type.as_deref() {
            Some("literal") => match (col.default_value, &dtype) {
                (Some(val), CatalogDataType::Primitive(p)) => {
                    crate::ColumnDefault::Literal(crate::Value::parse(&p.clone(), &val)?)
                }
                (Some(_), _) => {
                    return Err(DucklakeError::InvalidDefault {
                        column: col.column_name,
                        reason: "default value for non-primitive type is not supported",
                    });
                }
                _ => crate::ColumnDefault::Literal(None),
            },
            Some("expression") => match col.default_value {
                Some(expression) => crate::ColumnDefault::Expression {
                    dialect: col.default_value_dialect.unwrap_or_default(),
                    expression,
                },
                None => {
                    return Err(DucklakeError::InvalidDefault {
                        column: col.column_name,
                        reason: "expression default value is missing expression",
                    });
                }
            },
            // NOTE: This branch is triggered for nested types
            _ => crate::ColumnDefault::Literal(None),
        };

        Ok(Self {
            state: CatalogState::Existing { id: column_id },
            parent_column,
            name: col.column_name,
            dtype,
            nullable: col.nulls_allowed,
            tags: tags
                .extract_if(.., |tag| tag.column_id == column_id)
                .map(|tag| tag.into())
                .collect(),
            initial_default,
            default_value,
        })
    }
}

/* ----------------------------------------- DATA TYPE ----------------------------------------- */

fn parse_primitive_dtype(s: &str) -> DucklakeResult<crate::DataType> {
    use crate::DataType::*;
    match s {
        "boolean" => Ok(Boolean),
        "int8" => Ok(Int8),
        "int16" => Ok(Int16),
        "int32" => Ok(Int32),
        "int64" => Ok(Int64),
        "int128" => Ok(Int128),
        "uint8" => Ok(UInt8),
        "uint16" => Ok(UInt16),
        "uint32" => Ok(UInt32),
        "uint64" => Ok(UInt64),
        "uint128" => Ok(UInt128),
        "float32" => Ok(Float32),
        "float64" => Ok(Float64),
        "time" => Ok(Time),
        "timetz" => Ok(TimeTz),
        "date" => Ok(Date),
        "timestamp" => Ok(Timestamp {
            precision: crate::TimestampPrecision::Microseconds,
        }),
        "timestamp_s" => Ok(Timestamp {
            precision: crate::TimestampPrecision::Seconds,
        }),
        "timestamp_ms" => Ok(Timestamp {
            precision: crate::TimestampPrecision::Milliseconds,
        }),
        "timestamp_ns" => Ok(Timestamp {
            precision: crate::TimestampPrecision::Nanoseconds,
        }),
        "timestamptz" => Ok(TimestampTz),
        "interval" => Ok(Interval),
        "varchar" => Ok(Varchar),
        "blob" => Ok(Blob),
        "json" => Ok(Json),
        "uuid" => Ok(Uuid),
        s => {
            static RE_DECIMAL: LazyLock<Regex> =
                LazyLock::new(|| Regex::new(r"^decimal\((\d+),\s*(\d+)\)$").unwrap());
            if let Some(caps) = RE_DECIMAL.captures(s)
                && let Ok(precision) = caps[1].parse::<u8>()
                && let Ok(scale) = caps[2].parse::<u8>()
            {
                Ok(Decimal { precision, scale })
            } else {
                Err(DucklakeError::InvalidDataType(s.into()))
            }
        }
    }
}
