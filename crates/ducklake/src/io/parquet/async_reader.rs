use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::FOOTER_SIZE;
use parquet::file::metadata::{FooterTail, ParquetMetaData, ParquetMetaDataReader};

#[derive(Clone, Debug)]
pub(super) struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    path: ObjectStorePath,
    file_size: u64,
    footer_size: Option<usize>,
}

impl ObjectStoreReader {
    pub(super) fn new(store: Arc<dyn ObjectStore>, path: ObjectStorePath, file_size: u64) -> Self {
        Self {
            store,
            path,
            file_size,
            footer_size: None,
        }
    }

    /// Size of the parquet footer (thrift metadata excluding the 8-byte trailer).
    ///
    /// This is only available after [`AsyncFileReader::get_metadata`] has been called.
    pub(super) fn footer_size(&self) -> Option<usize> {
        self.footer_size
    }

    /// Derives the footer size from the footer tail contained in the last 8 bytes of `bytes`.
    fn record_footer_size(&mut self, bytes: &Bytes) {
        if let Some(start) = bytes.len().checked_sub(FOOTER_SIZE)
            && let Ok(footer) = FooterTail::try_from(&bytes[start..])
        {
            self.footer_size = Some(footer.metadata_length());
        }
    }
}

impl AsyncFileReader for ObjectStoreReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        // A read reaching the end of the file ends with the footer tail.
        let at_eof = range.end == self.file_size;
        async move {
            let bytes = self
                .store
                .get_range(&self.path, range)
                .await
                .map_err(to_parquet_err)?;
            if at_eof && self.footer_size.is_none() {
                self.record_footer_size(&bytes);
            }
            Ok(bytes)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        // Bounded range requests (rather than suffix requests) are supported by all backends.
        let file_size = self.file_size;
        async move {
            // FIXME: Enable `.with_arrow_reader_options(options)` once removing the
            // `patch.crates-io` section in `Cargo.toml`.
            let metadata = ParquetMetaDataReader::new()
                .load_and_finish(self, file_size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

fn to_parquet_err(e: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(e))
}
