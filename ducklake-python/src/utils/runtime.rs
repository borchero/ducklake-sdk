use std::sync::LazyLock;

use pyo3::marker::Ungil;
use pyo3::prelude::*;

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .build()
        .unwrap()
});

pub fn block_on<F>(py: Python, fut: F) -> F::Output
where
    F: std::future::Future + Send,
    F::Output: Ungil,
{
    py.detach(|| RUNTIME.block_on(fut))
}
