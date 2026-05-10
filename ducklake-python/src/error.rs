use ducklake::DucklakeError;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;

create_exception!(ducklake.exceptions, NotInitializedError, PyException);
create_exception!(ducklake.exceptions, AlreadyInitializedError, PyException);
create_exception!(ducklake.exceptions, OutdatedVersionError, PyException);
create_exception!(ducklake.exceptions, AlreadyExistsError, PyException);
create_exception!(ducklake.exceptions, NotFoundError, PyException);
create_exception!(ducklake.exceptions, InvalidCastError, PyException);
create_exception!(ducklake.exceptions, InvalidNullValueError, PyException);
create_exception!(
    ducklake.exceptions,
    InvalidNullabilityChangeError,
    PyException
);
create_exception!(ducklake.exceptions, TransactionConflictError, PyException);
create_exception!(ducklake.exceptions, ImmutableDucklakeError, PyException);

pub fn into_pyerr(error: DucklakeError) -> PyErr {
    use DucklakeError::*;
    match error {
        CatalogNotInitialized => NotInitializedError::new_err(
            "catalog is not initialized yet, call `ducklake.create()` first",
        ),
        CatalogAlreadyInitialized => AlreadyInitializedError::new_err(
            "catalog is already initialized, call `ducklake.connect()` instead",
        ),
        OutdatedVersion(_, _) => OutdatedVersionError::new_err(error.to_string()),
        AlreadyExists { .. } => AlreadyExistsError::new_err(error.to_string()),
        NotFound { .. } => NotFoundError::new_err(error.to_string()),
        InvalidTableName { .. } => PyValueError::new_err(error.to_string()),
        InvalidPartitionTransform { .. } => PyValueError::new_err(error.to_string()),
        ReadOnlyMetadata { .. } => PyValueError::new_err(error.to_string()),
        InvalidCast { .. } => InvalidCastError::new_err(error.to_string()),
        InvalidNullValue { .. } => InvalidNullValueError::new_err(error.to_string()),
        InvalidNullabilityChange { .. } => {
            InvalidNullabilityChangeError::new_err(error.to_string())
        }
        TransactionConflict(_) => TransactionConflictError::new_err(error.to_string()),
        ImmutableDucklake => ImmutableDucklakeError::new_err(error.to_string()),
        InvalidChanges(_) => PyValueError::new_err(error.to_string()),
        _ => pyo3::exceptions::PyRuntimeError::new_err(error.to_string()),
    }
}
