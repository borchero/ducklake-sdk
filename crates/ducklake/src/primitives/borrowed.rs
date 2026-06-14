#[cfg(feature = "python")]
use std::borrow::Cow;
use std::ops::Deref;

/// Zero-cost abstraction for an immutable reference type that can be owned when writing language
/// bindings that do not support lifetimes (e.g. Python).
#[cfg(feature = "python")]
pub(crate) struct Borrowed<'a, T: Clone>(Cow<'a, T>);
#[cfg(not(feature = "python"))]
pub(crate) struct Borrowed<'a, T: Clone>(&'a T);

impl<'a, T: Clone> Borrowed<'a, T> {
    pub(crate) fn new(value: &'a T) -> Self {
        #[cfg(feature = "python")]
        return Borrowed(Cow::Borrowed(value));
        #[cfg(not(feature = "python"))]
        return Borrowed(value);
    }
}

impl<'a, T: Clone> Deref for Borrowed<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        #[cfg(feature = "python")]
        return self.0.deref();
        #[cfg(not(feature = "python"))]
        return self.0;
    }
}

#[cfg(feature = "python")]
impl<'a, T: Clone> Borrowed<'a, T> {
    pub(crate) fn into_owned(self) -> Borrowed<'static, T> {
        Borrowed(Cow::Owned(self.0.into_owned()))
    }
}
