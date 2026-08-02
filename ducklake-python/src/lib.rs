#![feature(mapped_lock_guards)]

use pyo3::prelude::*;

mod conversion;
mod ducklake;
mod error;
mod table;
mod transaction;
mod utils;

use ducklake::PyDucklake;
use table::PyTable;
use transaction::{PyTransaction, PyTransactionTable};

#[pymodule]
#[pyo3(name = "_native")]
mod ducklake_module {
    #[pymodule_export]
    use super::ducklake::{connect, create};
    #[pymodule_export]
    use super::{PyDucklake, PyTable, PyTransaction, PyTransactionTable};
    #[pymodule_export]
    use crate::error::{
        AlreadyExistsError,
        AlreadyInitializedError,
        ImmutableDucklakeError,
        InvalidCastError,
        InvalidNullValueError,
        InvalidNullabilityChangeError,
        NotFoundError,
        NotInitializedError,
        OutdatedVersionError,
        TransactionConflictError,
    };
    #[pymodule_export]
    use crate::utils::arrow::{arrow_schema_field_ids, schema_from_arrow, schema_to_arrow};
    #[pymodule_export]
    use crate::utils::filepath_generator::PyDataFilePathGenerator;
}
