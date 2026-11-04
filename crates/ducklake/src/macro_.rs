use crate::ducklake::DucklakeConnection;
use crate::{DucklakeResult, MacroImplementation, MacroType, QualifiedName};

/// Handle to a macro in the DuckLake catalog.
#[derive(Clone)]
pub struct Macro {
    conn: DucklakeConnection,
    id: i64,
}

/// Information required to create a macro in the catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MacroInfo {
    /// Fully-qualified macro name.
    pub name: QualifiedName,
    /// Whether this is a scalar or table macro.
    pub macro_type: MacroType,
    /// All implementations (overloads) of the macro.
    pub implementations: Vec<MacroImplementation>,
}

impl Macro {
    /// Get the fully-qualified macro name.
    pub async fn name(&self) -> DucklakeResult<QualifiedName> {
        let snapshot = self.conn.current_snapshot();
        let catalog = snapshot.catalog().await?;
        Ok(catalog.macro_(self.id)?.name().clone())
    }

    /// Get whether this is a scalar or table macro.
    pub async fn macro_type(&self) -> DucklakeResult<MacroType> {
        let snapshot = self.conn.current_snapshot();
        let catalog = snapshot.catalog().await?;
        Ok(catalog.macro_(self.id)?.macro_type())
    }

    /// Get all implementations (overloads) of the macro.
    pub async fn implementations(&self) -> DucklakeResult<Vec<MacroImplementation>> {
        let snapshot = self.conn.current_snapshot();
        let catalog = snapshot.catalog().await?;
        Ok(catalog.macro_(self.id)?.implementations())
    }

    pub(crate) fn new(conn: DucklakeConnection, id: i64) -> Self {
        Self { conn, id }
    }
}
