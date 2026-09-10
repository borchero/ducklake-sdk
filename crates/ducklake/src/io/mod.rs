#[macro_use]
pub(crate) mod arrow;
pub(crate) mod parquet;
mod path;

use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use object_store::buffered::BufWriter;
use object_store::{ObjectStore, ObjectStoreExt};
pub(crate) use path::{DucklakePath, Path};
use tokio::io::AsyncWriteExt;

/// Copy a file between two object stores.
pub(crate) async fn copy_file(
    source: &DucklakePath,
    source_options: &[(String, String)],
    destination: &DucklakePath,
    destination_options: &[(String, String)],
) -> crate::DucklakeResult<()> {
    let source = source.resolve()?;
    let destination = destination.resolve()?;

    let source_store = source.object_store(Some(source_options.to_vec()));
    let destination_store = destination.object_store(Some(destination_options.to_vec()));

    let source_path = source.path();
    let destination_path = destination.path();

    if std::sync::Arc::ptr_eq(&source_store, &destination_store) {
        source_store.copy(&source_path, &destination_path).await?;
    } else {
        let contents = source_store.get(&source_path).await?.into_stream();
        copy_stream(contents, destination_store, destination_path).await?;
    }

    Ok(())
}

async fn copy_stream(
    mut contents: BoxStream<'static, object_store::Result<Bytes>>,
    destination_store: Arc<dyn ObjectStore>,
    destination_path: object_store::path::Path,
) -> object_store::Result<()> {
    const CHUNK_SIZE: usize = 8 * 1024 * 1024;
    let mut writer = BufWriter::with_capacity(destination_store, destination_path, CHUNK_SIZE)
        .with_max_concurrency(2);
    let result = async {
        while let Some(bytes) = contents.try_next().await? {
            // Bound queued uploads even when a store yields an unusually large chunk.
            for offset in (0..bytes.len()).step_by(CHUNK_SIZE) {
                writer
                    .put(bytes.slice(offset..(offset + CHUNK_SIZE).min(bytes.len())))
                    .await?;
            }
        }
        Ok::<_, object_store::Error>(())
    }
    .await;
    if let Err(error) = result {
        // Keep the original copy error if cleaning up the incomplete upload also fails.
        let _ = writer.abort().await;
        return Err(error);
    }
    writer
        .shutdown()
        .await
        .map_err(|error| object_store::Error::Generic {
            store: "copy",
            source: Box::new(error),
        })
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(0)]
    #[case(1024)]
    #[case(25 * 1024 * 1024)]
    #[tokio::test]
    async fn test_copy_stream_between_stores(#[case] size: usize) {
        // Arrange
        let source = InMemory::new();
        let destination = Arc::new(InMemory::new());
        let path = Path::from("file.parquet");
        let contents = Bytes::from((0..size).map(|i| (i % 251) as u8).collect::<Vec<_>>());
        source.put(&path, contents.clone().into()).await.unwrap();
        let stream = source.get(&path).await.unwrap().into_stream();

        // Act
        copy_stream(stream, destination.clone(), path.clone())
            .await
            .unwrap();

        // Assert
        let copied = destination.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(copied, contents);
        assert_eq!(
            source.get(&path).await.unwrap().bytes().await.unwrap(),
            contents
        );
    }

    #[tokio::test]
    async fn test_copy_stream_aborts_on_read_error() {
        // Arrange
        let destination = Arc::new(InMemory::new());
        let path = Path::from("file.parquet");
        let stream = futures::stream::iter([
            Ok(Bytes::from(vec![42; 9 * 1024 * 1024])),
            Err(object_store::Error::Generic {
                store: "source",
                source: "read failed".into(),
            }),
        ])
        .boxed();

        // Act
        let result = copy_stream(stream, destination.clone(), path.clone()).await;

        // Assert
        assert!(result.unwrap_err().to_string().contains("read failed"));
        assert!(matches!(
            destination.head(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }
}
