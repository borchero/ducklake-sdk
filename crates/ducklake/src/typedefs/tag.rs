use std::collections::HashMap;
use std::sync::Arc;

use crate::spec::*;

/// A collection of tags indexed by key.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tags(Arc<HashMap<String, String>>);

impl Tags {
    /// Get a tag's value by key without copying it.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// Iterate over the keys and values without copying them. Iteration order is unspecified.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }

    /// Copy all tags into an owned vector, cloning each key and value. Order is unspecified.
    pub fn to_vec(&self) -> Vec<Tag> {
        self.0
            .iter()
            .map(|(key, value)| Tag {
                key: key.clone(),
                value: value.clone(),
            })
            .collect()
    }

    /// Get the number of tags in the collection.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check whether the collection contains no tags.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn insert(&mut self, tag: Tag) {
        Arc::make_mut(&mut self.0).insert(tag.key, tag.value);
    }

    pub(crate) fn remove(&mut self, key: &str) -> Option<String> {
        Arc::make_mut(&mut self.0).remove(key)
    }
}

impl FromIterator<Tag> for Tags {
    fn from_iter<T: IntoIterator<Item = Tag>>(iter: T) -> Self {
        Self(Arc::new(
            iter.into_iter().map(|tag| (tag.key, tag.value)).collect(),
        ))
    }
}

impl From<HashMap<String, String>> for Tags {
    fn from(tags: HashMap<String, String>) -> Self {
        Self(Arc::new(tags))
    }
}

/// Collect key-value pairs, keeping the last value for each key.
impl<K: Into<String>, V: Into<String>> FromIterator<(K, V)> for Tags {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        Self(Arc::new(
            iter.into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        ))
    }
}

/// A key-value tag attached to a table or column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tag {
    /// The tag key.
    pub key: String,
    /// The tag value.
    pub value: String,
}

impl From<DucklakeTag> for Tag {
    fn from(value: DucklakeTag) -> Self {
        Tag {
            key: value.key,
            value: value.value,
        }
    }
}

impl From<DucklakeColumnTag> for Tag {
    fn from(value: DucklakeColumnTag) -> Self {
        Tag {
            key: value.key,
            value: value.value,
        }
    }
}
