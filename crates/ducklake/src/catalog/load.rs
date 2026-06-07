use std::borrow::Cow;
use std::collections::HashMap;

use itertools::Itertools;
use sea_query::{Asterisk, ExprTrait, Query};

use super::*;
use crate::spec::*;
use crate::{db, io};

macro_rules! snapshot_query {
    ($entity:ident, $snapshot_id:expr) => {
        Query::select()
            .column(Asterisk)
            .from($entity::Table)
            .filter_for_snapshot(
                $entity::Column::BeginSnapshot.col(),
                $entity::Column::EndSnapshot.col(),
                $snapshot_id,
            )
            .to_owned()
    };
}

impl Catalog {
    /// Create a new, empty catalog.
    ///
    /// This is reserved for unit testing. Consumers should always use `Catalog::load`.
    pub(super) fn new() -> Self {
        Self {
            schema_arena: Arena::new(),
            table_arena: Arena::new(),
            schemas: HashMap::new(),
        }
    }

    /// Load the current catalog from the DuckLake catalog database referenced by the pool, at the
    /// given snapshot.
    pub async fn load(pool: &db::Pool, snapshot_id: i64) -> DucklakeResult<Self> {
        // Fetch all relevant data in parallel. Since we're using snapshot filtering,
        // we don't need a transaction for consistency - each query filters by the same
        // snapshot_id which ensures we get a consistent view of the data.
        let schemas_query = snapshot_query!(ducklake_schema, snapshot_id);
        let tables_query = snapshot_query!(ducklake_table, snapshot_id);
        let columns_query = snapshot_query!(ducklake_column, snapshot_id);
        let tags_query = snapshot_query!(ducklake_tag, snapshot_id);
        let column_tags_query = snapshot_query!(ducklake_column_tag, snapshot_id);
        let partition_infos_query = snapshot_query!(ducklake_partition_info, snapshot_id);
        let partition_columns_query = Query::select()
            .column(Asterisk)
            .from(ducklake_partition_column::Table)
            .to_owned();

        #[allow(clippy::type_complexity)]
        let (
            fetched_schemas,
            fetched_tables,
            fetched_columns,
            fetched_tags,
            fetched_column_tags,
            fetched_partition_infos,
            fetched_partition_columns,
        ): (
            Vec<DucklakeSchema>,
            Vec<DucklakeTable>,
            Vec<DucklakeColumn>,
            Vec<DucklakeTag>,
            Vec<DucklakeColumnTag>,
            Vec<DucklakePartitionInfo>,
            Vec<DucklakePartitionColumn>,
        ) = tokio::try_join!(
            pool.fetch_all(&schemas_query),
            pool.fetch_all(&tables_query),
            pool.fetch_all(&columns_query),
            pool.fetch_all(&tags_query),
            pool.fetch_all(&column_tags_query),
            pool.fetch_all(&partition_infos_query),
            pool.fetch_all(&partition_columns_query),
        )?;

        // Group all relevant data by the keys we need to filter by below. This avoids a bunch
        // of linear searches and memcopies later on.
        let mut grouped_columns = fetched_columns
            .into_iter()
            .into_group_map_by(|c| c.table_id);
        let mut grouped_tags = fetched_tags.into_iter().into_group_map_by(|t| t.object_id);
        let mut grouped_column_tags = fetched_column_tags
            .into_iter()
            .into_group_map_by(|ct| ct.table_id);
        let mut grouped_partition_infos = fetched_partition_infos
            .into_iter()
            .into_group_map_by(|pi| pi.table_id);
        let mut grouped_partition_columns = fetched_partition_columns
            .into_iter()
            .into_group_map_by(|pc| pc.table_id);

        // Initialize a new catalog and populate it with the fetched data
        let mut catalog = Catalog::new();
        catalog.set_schemas(fetched_schemas);
        catalog.set_tables(
            fetched_tables,
            &mut grouped_columns,
            &mut grouped_column_tags,
            &mut grouped_partition_infos,
            &mut grouped_partition_columns,
            &mut grouped_tags,
        )?;

        Ok(catalog)
    }

    fn set_schemas(&mut self, schemas: Vec<DucklakeSchema>) {
        for schema in schemas {
            let schema_name = schema.schema_name;

            // 1) Create the catalog schema
            let catalog_schema = CatalogSchema {
                id: Some(schema.schema_id),
                name: schema_name.clone(),
                tables: HashMap::new(),
                path: io::DucklakePath::new(
                    &default_empty_string(&schema.path, || format!("{}/", schema_name)),
                    schema.path_is_relative,
                ),
            };

            // 2) Add the schema to the catalog
            let idx = self
                .schema_arena
                .push(catalog_schema, Some(schema.schema_id));
            self.schemas.insert(schema_name, idx);
        }
    }

    fn set_tables(
        &mut self,
        tables: Vec<DucklakeTable>,
        columns: &mut HashMap<i64, Vec<DucklakeColumn>>,
        column_tags: &mut HashMap<i64, Vec<DucklakeColumnTag>>,
        partition_infos: &mut HashMap<i64, Vec<DucklakePartitionInfo>>,
        partition_columns: &mut HashMap<i64, Vec<DucklakePartitionColumn>>,
        tags: &mut HashMap<i64, Vec<DucklakeTag>>,
    ) -> DucklakeResult<()> {
        for table in tables {
            let table_name = table.table_name;

            // 1) Get the schema this table belongs to
            let schema = self
                .schema(table.schema_id)
                .expect("table references the ID of non-existent schema");

            // 2) Collect the columns for this table along with their tags
            let table_columns = columns.remove(&table.table_id).unwrap_or_default();
            let table_column_tags = column_tags.remove(&table.table_id).unwrap_or_default();
            let table_columns = CatalogColumns::from_ducklake(table_columns, table_column_tags)?;

            // 3) Collect the partition info for this table
            let mut table_partition_info =
                partition_infos.remove(&table.table_id).unwrap_or_default();
            if table_partition_info.len() > 1 {
                return Err(DucklakeError::InvalidPartitions(format!(
                    "expected at most one partition for table {table_name} but found {}",
                    table_partition_info.len()
                )));
            }
            let table_partition = if let Some(partition_info) = table_partition_info.pop() {
                let table_partition_columns: Vec<_> = partition_columns
                    .remove(&table.table_id)
                    .map(|cols| {
                        cols.into_iter()
                            .filter(|col| col.partition_id == partition_info.partition_id)
                            .collect()
                    })
                    .unwrap_or_default();
                if table_partition_columns.is_empty() {
                    return Err(DucklakeError::InvalidPartitions(format!(
                        "partition info exists for table {table_name} but no partition columns found"
                    )));
                }
                Some(CatalogTablePartition::from_ducklake(
                    partition_info,
                    table_partition_columns,
                    &table_columns,
                )?)
            } else {
                None
            };

            // 4) Construct the full table catalog object
            let catalog_table = CatalogTable {
                id: Some(table.table_id),
                name: crate::TableName {
                    schema: schema.name().to_string(),
                    name: table_name.clone(),
                },
                columns: table_columns,
                partition: table_partition,
                tags: tags
                    .remove(&table.table_id)
                    .map(|v| v.into_iter().map(|tag| tag.into()).collect())
                    .unwrap_or_default(),
                path: io::DucklakePath::new(
                    &default_empty_string(&table.path, || format!("{}/", table_name)),
                    table.path_is_relative,
                ),
            };

            // 5) Add the table to the catalog
            let arena_idx = self.table_arena.push(catalog_table, Some(table.table_id));
            self.schema_mut(table.schema_id)
                .unwrap() // SAFETY: we already verified existence above
                .inner_mut()
                .tables
                .insert(table_name, arena_idx);
        }
        Ok(())
    }
}

fn default_empty_string(s: &str, on_empty: impl FnOnce() -> String) -> Cow<'_, str> {
    if s.is_empty() {
        Cow::Owned(on_empty())
    } else {
        Cow::Borrowed(s)
    }
}

/* ----------------------------------------- ON-DEMAND ----------------------------------------- */

impl Catalog {
    pub(super) async fn load_next_column_id(
        &self,
        pool: &db::Pool,
        table_id: i64,
    ) -> DucklakeResult<i64> {
        let query = Query::select()
            .expr(ducklake_column::Column::ColumnId.col().max())
            .from(ducklake_column::Table)
            .and_where(ducklake_column::Column::TableId.col().eq(table_id))
            .to_owned();
        let (max_col,): (i64,) = pool.fetch_one(&query).await?;
        Ok(max_col + 1)
    }
}
