use super::ArenaIdx;

/// Opaque reference to a schema in the catalog. This is static for the lifetime of a catalog.
/// It is different to the schema ID in the DuckLake and also exists for transaction-local
/// (i.e. pending) schemas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SchemaRef(pub(super) ArenaIdx);

impl From<ArenaIdx> for SchemaRef {
    fn from(value: ArenaIdx) -> Self {
        Self(value)
    }
}

/// Opaque reference to a table in the catalog. This is static for the lifetime of a catalog.
/// It is different to the table ID in the DuckLake and also exists for transaction-local
/// (i.e. pending) tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableRef(pub(super) ArenaIdx);

#[cfg(test)]
impl TableRef {
    pub(crate) fn mock(i: usize) -> Self {
        Self(ArenaIdx(i))
    }
}

impl From<ArenaIdx> for TableRef {
    fn from(value: ArenaIdx) -> Self {
        Self(value)
    }
}

/// Opaque reference to a column within a table in the catalog. This is static for the lifetime
/// of a catalog. It is different to the column ID in the DuckLake and also exists for
/// transaction-local (i.e. pending) columns, potentially of transaction-local tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    pub table_ref: TableRef,
    pub(super) column_idx: ArenaIdx,
}

impl From<(ArenaIdx, ArenaIdx)> for ColumnRef {
    fn from(value: (ArenaIdx, ArenaIdx)) -> Self {
        Self {
            table_ref: TableRef(value.0),
            column_idx: value.1,
        }
    }
}
