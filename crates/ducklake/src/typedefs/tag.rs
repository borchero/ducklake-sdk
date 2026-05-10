use crate::spec::*;

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
