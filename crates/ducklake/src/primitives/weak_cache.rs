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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier, Weak};

    use super::WeakCache;

    #[rstest::rstest]
    #[case(true)]
    #[case(false)]
    fn values_are_reused_only_while_a_caller_retains_them(#[case] retain: bool) {
        // Arrange
        let cache = WeakCache::new();
        let value = cache.get_or_insert_with(0, || "original");
        let original = Arc::downgrade(&value);
        let _retained = retain.then_some(value);

        // Act
        let value = cache.get_or_insert_with(0, || "replacement");

        // Assert
        assert_eq!(Weak::ptr_eq(&original, &Arc::downgrade(&value)), retain);
        assert_eq!(*value, if retain { "original" } else { "replacement" });
    }

    #[test]
    fn concurrent_callers_share_one_value() {
        // Arrange
        let cache = WeakCache::new();
        let barrier = Barrier::new(8);

        // Act
        let values = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|id| {
                    let cache = &cache;
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        cache.get_or_insert_with(0, || id)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });

        // Assert
        assert!(values.iter().all(|value| Arc::ptr_eq(value, &values[0])));
    }
}
