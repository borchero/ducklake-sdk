#[macro_use]
pub(crate) mod arrow;
pub(crate) mod parquet;
mod path;

pub(crate) use path::{DucklakePath, Path};

/// Copy a file between two object stores.
pub(crate) async fn copy_file(
    source: &DucklakePath,
    source_options: &[(String, String)],
    destination: &DucklakePath,
    destination_options: &[(String, String)],
) -> crate::DucklakeResult<()> {
    use object_store::ObjectStoreExt;

    let source = source.resolve()?;
    let destination = destination.resolve()?;
    let source_store = source.object_store(Some(source_options.to_vec()));
    let destination_store = destination.object_store(Some(destination_options.to_vec()));
    let contents = source_store.get(&source.path()).await?.bytes().await?;
    destination_store
        .put(&destination.path(), contents.into())
        .await?;
    Ok(())
}
