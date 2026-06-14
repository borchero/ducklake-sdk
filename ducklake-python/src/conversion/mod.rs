mod column;
mod column_default;
mod column_name;
mod column_stats;
mod data_type;
mod nominal_enums;
mod partition;
mod scan;
mod snapshot;
mod table_metadata;
mod table_name;
mod tag;
mod value;
mod write_data_file;

#[repr(transparent)]
pub struct Wrap<T>(pub T);

impl<T> From<T> for Wrap<T> {
    fn from(t: T) -> Self {
        Wrap(t)
    }
}

/* ------------------------------------------ MODULES ------------------------------------------ */

mod py_modules {
    use pyo3::prelude::*;
    use pyo3::sync::PyOnceLock;

    static PY_DUCKLAKE: PyOnceLock<Py<PyModule>> = PyOnceLock::new();
    static PY_DATETIME: PyOnceLock<Py<PyModule>> = PyOnceLock::new();
    static PY_DECIMAL_MOD: PyOnceLock<Py<PyModule>> = PyOnceLock::new();
    static PY_UUID_MOD: PyOnceLock<Py<PyModule>> = PyOnceLock::new();
    static PY_RELATIVEDELTA_MOD: PyOnceLock<Py<PyModule>> = PyOnceLock::new();

    pub(super) fn ducklake_module(py: Python<'_>) -> &Py<PyModule> {
        PY_DUCKLAKE.get_or_init(py, || py.import("ducklake").unwrap().unbind())
    }

    pub(super) fn datetime_module(py: Python<'_>) -> &Py<PyModule> {
        PY_DATETIME.get_or_init(py, || py.import("datetime").unwrap().unbind())
    }

    pub(super) fn decimal_module(py: Python<'_>) -> &Py<PyModule> {
        PY_DECIMAL_MOD.get_or_init(py, || py.import("decimal").unwrap().unbind())
    }

    pub(super) fn uuid_module(py: Python<'_>) -> &Py<PyModule> {
        PY_UUID_MOD.get_or_init(py, || py.import("uuid").unwrap().unbind())
    }

    pub(super) fn relativedelta_module(py: Python<'_>) -> &Py<PyModule> {
        PY_RELATIVEDELTA_MOD
            .get_or_init(py, || py.import("dateutil.relativedelta").unwrap().unbind())
    }
}
