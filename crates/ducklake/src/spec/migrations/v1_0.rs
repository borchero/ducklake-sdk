use crate::{DucklakeResult, db};

pub async fn migrate(_tx: &mut db::Transaction) -> DucklakeResult<()> {
    Ok(())
}
