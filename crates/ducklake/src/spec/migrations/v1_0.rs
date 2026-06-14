use crate::{DucklakeResult, db};

pub(super) async fn migrate(_tx: &mut db::Transaction) -> DucklakeResult<()> {
    Ok(())
}
