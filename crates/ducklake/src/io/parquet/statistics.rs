use std::collections::HashMap;
use std::sync::Arc;

use arrow_arith::aggregate;
use arrow_schema::{DataType, Field, Schema};
use object_store::ObjectStoreExt;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, parquet_to_arrow_schema};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::schema::types::{SchemaDescriptor, Type};

use super::async_reader::ObjectStoreReader;
use crate::{DucklakeResult, FileColumnStats, io};

pub(crate) async fn read_file_statistics(
    path: io::Path,
    io_options: Option<Vec<(String, String)>>,
) -> DucklakeResult<crate::DataFileStatistics> {
    // Get the store to read the file
    let store = path.object_store(io_options);
    let object_path = path.path();
    let file_meta = store.head(&object_path).await?;

    // Read the parquet metadata using the async reader
    let mut reader = ObjectStoreReader::new(store, object_path);
    let metadata = reader.get_metadata(None).await?;

    // Read column statistics from the metadata
    let file_metadata = metadata.file_metadata();
    let arrow_schema = parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    )?;

    let mut column_stats = HashMap::new();
    let parquet_schema = file_metadata.schema_descr();

    let fields_by_id = fields_by_id(&arrow_schema);
    for (col_idx, col_desc) in parquet_schema.columns().iter().enumerate() {
        // Get Arrow field via the ID
        let basic_info = col_desc.self_type().get_basic_info();
        if !basic_info.has_id() {
            continue;
        }
        let field_id = basic_info.id() as i64;
        let Some(&field) = fields_by_id.get(&field_id) else {
            continue;
        };

        // Compute column statistics for the column
        let stats = if col_desc.path().parts().len() == 1 {
            // Fast path: top-level primitive parquet columns can be looked up directly in
            // the original arrow/parquet schemas, and statistics can be read straight off
            // the original row groups.
            let converter =
                StatisticsConverter::try_new(col_desc.name(), &arrow_schema, parquet_schema)?;
            derive_column_stats(field, &converter, metadata.row_groups(), &metadata, col_idx)?
        } else {
            // `StatisticsConverter::try_new` only resolves top-level columns and
            // explicitly refuses nested arrow types. Work around that by giving it a
            // synthetic flat schema (one arrow leaf, one parquet leaf) and synthetic
            // single-column row groups so that the rest of its machinery just works
            // for nested leaves too.
            let fake_schema = Schema::new(vec![field.clone().with_name("c")]);
            let fake_parquet_descr = Arc::new(SchemaDescriptor::new(Arc::new(
                Type::group_type_builder("schema")
                    .with_fields(vec![col_desc.self_type_ptr()])
                    .build()?,
            )));
            let converter = StatisticsConverter::try_new("c", &fake_schema, &fake_parquet_descr)?;
            let fake_row_groups = metadata
                .row_groups()
                .iter()
                .map(|rg| {
                    RowGroupMetaData::builder(fake_parquet_descr.clone())
                        .set_num_rows(rg.num_rows())
                        .set_column_metadata(vec![rg.column(col_idx).clone()])
                        .build()
                })
                .collect::<parquet::errors::Result<Vec<_>>>()?;
            derive_column_stats(field, &converter, &fake_row_groups, &metadata, col_idx)?
        };

        column_stats.insert(field_id, stats);
    }

    // Return the statistics
    Ok(crate::DataFileStatistics {
        num_rows: metadata.file_metadata().num_rows() as usize,
        file_size_bytes: Some(file_meta.size as usize),
        footer_size_bytes: reader.footer_size(),
        column_stats,
    })
}

fn fields_by_id(schema: &Schema) -> HashMap<i64, &Field> {
    fn walk<'a>(field: &'a Field, out: &mut HashMap<i64, &'a Field>) {
        match field.data_type() {
            DataType::Struct(fields) => {
                for f in fields {
                    walk(f, out);
                }
            }
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::FixedSizeList(f, _)
            | DataType::ListView(f)
            | DataType::LargeListView(f)
            | DataType::Map(f, _) => walk(f, out),
            _ => {
                if let Some(id) = field
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    out.insert(id, field);
                }
            }
        }
    }

    let mut out = HashMap::new();
    for f in schema.fields() {
        walk(f, &mut out);
    }
    out
}

fn derive_column_stats(
    field: &Field,
    converter: &StatisticsConverter<'_>,
    row_groups: &[RowGroupMetaData],
    metadata: &ParquetMetaData,
    column_index: usize,
) -> DucklakeResult<FileColumnStats> {
    // Compute the size (always available)
    let size_bytes: i64 = metadata
        .row_groups()
        .iter()
        .map(|rg| rg.column(column_index).compressed_size())
        .sum();

    // Compute min, max, null count statistics
    let data_type = crate::Column::try_from(field)?.dtype;
    let min_value = converter
        .row_group_mins(row_groups)
        .ok()
        .and_then(|mins| io::arrow::aggregate::find_min(&data_type, &mins));
    let max_value = converter
        .row_group_maxes(row_groups)
        .ok()
        .and_then(|maxes| io::arrow::aggregate::find_max(&data_type, &maxes));
    let null_count = converter
        .row_group_null_counts(row_groups)
        .ok()
        .and_then(|counts| aggregate::sum(&counts));

    // Aggregate everything
    Ok(FileColumnStats {
        size_bytes: Some(size_bytes as usize),
        min_value,
        max_value,
        null_count: null_count.map(|c| c as usize),
        contains_nan: None,
    })
}
