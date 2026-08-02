use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use object_store::path::Path as ObjectStorePath;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, MetadataSuffixFetch};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::FOOTER_SIZE;
use parquet::file::metadata::{FooterTail, ParquetMetaData, ParquetMetaDataReader};

#[derive(Clone, Debug)]
pub(super) struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    path: ObjectStorePath,
    footer_size: Option<usize>,
}

impl ObjectStoreReader {
    pub(super) fn new(store: Arc<dyn ObjectStore>, path: ObjectStorePath) -> Self {
        Self {
            store,
            path,
            footer_size: None,
        }
    }

    /// Size of the parquet footer (thrift metadata excluding the 8-byte trailer).
    ///
    /// This is only available after [`AsyncFileReader::get_metadata`] has been called.
    pub(super) fn footer_size(&self) -> Option<usize> {
        self.footer_size
    }
}

impl AsyncFileReader for ObjectStoreReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        async move {
            self.store
                .get_range(&self.path, range)
                .await
                .map_err(to_parquet_err)
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        async move {
            self.store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(to_parquet_err)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        async move {
            let metadata = ParquetMetaDataReader::new()
                // FIXME: Enable once removing the `patch.crates-io` section in `Cargo.toml`
                // .with_arrow_reader_options(options)
                .load_via_suffix_and_finish(self)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

impl MetadataSuffixFetch for &mut ObjectStoreReader {
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let options = GetOptions {
            range: Some(GetRange::Suffix(suffix as u64)),
            ..Default::default()
        };
        async move {
            let resp = self
                .store
                .get_opts(&self.path, options)
                .await
                .map_err(to_parquet_err)?;
            let bytes = resp.bytes().await.map_err(to_parquet_err)?;

            // The last 8 bytes of a suffix are the footer tail, from which the footer size can
            // be derived without an additional request.
            let start = bytes.len() - FOOTER_SIZE;
            if let Ok(footer) = FooterTail::try_from(&bytes[start..]) {
                self.footer_size = Some(footer.metadata_length());
            }

            Ok(bytes)
        }
        .boxed()
    }
}

fn to_parquet_err(e: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(e))
}
