#[macro_use]
pub(crate) mod arrow;
pub(crate) mod parquet;
mod path;

use object_store::ObjectStoreExt;
pub(crate) use path::{DucklakePath, Path};

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
        let contents = source_store.get(&source_path).await?.bytes().await?;
        destination_store
            .put(&destination_path, contents.into())
            .await?;
    }

    Ok(())
}
