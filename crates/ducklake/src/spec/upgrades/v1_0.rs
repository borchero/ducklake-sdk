use crate::{DucklakeResult, db};

pub(super) async fn upgrade(_tx: &mut db::Transaction<'_>) -> DucklakeResult<()> {
    Ok(())
}
