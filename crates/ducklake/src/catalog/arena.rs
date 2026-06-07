use std::collections::HashMap;

/// Index into the catalog arena.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct ArenaIdx(pub usize);

#[derive(Debug, Clone)]
pub(super) struct Arena<T> {
    /// Append-only vector of entities.
    arena: Vec<T>,
    /// Append-only mapping from entity IDs to arena indices.
    by_id: HashMap<i64, ArenaIdx>,
}

impl<T> Arena<T> {
    pub fn new() -> Self {
        Self {
            arena: Vec::new(),
            by_id: HashMap::new(),
        }
    }

    pub fn push(&mut self, entity: T, id: Option<i64>) -> ArenaIdx {
        let idx = ArenaIdx(self.arena.len());
        self.arena.push(entity);
        if let Some(id) = id {
            self.by_id.insert(id, idx);
        }
        idx
    }

    pub fn register_id(&mut self, idx: ArenaIdx, id: i64) {
        self.by_id.insert(id, idx);
    }

    pub fn map_id(&self, id: i64) -> Option<ArenaIdx> {
        self.by_id.get(&id).copied()
    }

    pub fn get(&self, idx: ArenaIdx) -> &T {
        &self.arena[idx.0]
    }

    pub fn get_mut(&mut self, idx: ArenaIdx) -> &mut T {
        &mut self.arena[idx.0]
    }
}
