use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, Weak};

/// Shares values by key without keeping them alive after their callers release them.
pub(crate) struct WeakCache<K, V> {
    values: Mutex<HashMap<K, Weak<V>>>,
}

impl<K: Eq + Hash, V> WeakCache<K, V> {
    pub(crate) fn new() -> Self {
        Self {
            values: Mutex::new(HashMap::new()),
        }
    }

    /// Reuse a live value or create one, pruning expired entries on a miss.
    /// The initializer runs under the cache lock and should only do cheap, synchronous work.
    pub(crate) fn get_or_insert_with(&self, key: K, init: impl FnOnce() -> V) -> Arc<V> {
        let mut values = self.values.lock().unwrap();
        if let Some(value) = values.get(&key).and_then(Weak::upgrade) {
            return value;
        }
        values.retain(|_, value| value.strong_count() > 0);
        let value = Arc::new(init());
        values.insert(key, Arc::downgrade(&value));
        value
    }
}
