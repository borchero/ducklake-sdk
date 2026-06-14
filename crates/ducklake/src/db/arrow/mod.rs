mod decoding;
mod encoding;

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
pub(super) use decoding::*;
pub(super) use encoding::*;
use futures::{Stream, TryStreamExt};

use crate::DucklakeResult;

pub(super) async fn decode_rows<R, S>(mut rows: S, schema: &Schema) -> DucklakeResult<RecordBatch>
where
    R: DecodableRow,
    S: Stream<Item = sqlx::Result<R>> + Unpin,
{
    // Create a column builder for each field in the schema
    let mut builders: Vec<Box<dyn ArrayAppender<R::Decoder>>> = schema
        .fields()
        .iter()
        .map(|f| decoding::make_array_appender::<R::Decoder>(f))
        .collect::<DucklakeResult<_>>()?;

    // Iterate over the rows and extract via the builders
    while let Some(row) = rows.try_next().await? {
        for (field, a) in schema.fields().iter().zip(builders.iter_mut()) {
            a.append(&row, field.name())?;
        }
    }

    // Finalize all builders and create the record batch
    let arrays = builders.iter_mut().map(|a| a.finish()).collect();
    Ok(RecordBatch::try_new(Arc::new(schema.clone()), arrays)?)
}

pub(super) fn encode_record_batch<A: EncodableArguments>(
    batch: &RecordBatch,
) -> DucklakeResult<A> {
    // Create an extractor for each column in the batch
    let extractors: Vec<Box<dyn ArrayExtractor<A::Encoder>>> = batch
        .schema_ref()
        .fields()
        .iter()
        .zip(batch.columns().iter())
        .map(|(field, array)| encoding::make_column_encoder::<A::Encoder>(field, array.clone()))
        .collect::<DucklakeResult<_>>()?;

    // Extract all data row-wise
    let mut args = A::default();
    for r in 0..batch.num_rows() {
        for ext in &extractors {
            ext.extract(&mut args, r)?;
        }
    }
    Ok(args)
}
