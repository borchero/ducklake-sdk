mod date;
mod nested;
mod primitive;

use crate::DucklakeResult;

pub trait Literal: Sized {
    fn parse(s: &str) -> DucklakeResult<Self>;
    fn format(&self) -> String;
}

pub const NULL_STRING: &str = "NULL";

// Overview for type encodings: https://ducklake.select/docs/stable/specification/data_types#type-encoding-for-statistics

pub fn parse<T: Literal>(s: &str) -> DucklakeResult<Option<T>> {
    if s == NULL_STRING {
        Ok(None)
    } else {
        Ok(Some(T::parse(s)?))
    }
}

pub fn format<T: Literal>(value: Option<&T>) -> String {
    match value {
        Some(v) => v.format(),
        None => NULL_STRING.to_string(),
    }
}
