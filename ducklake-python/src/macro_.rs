use ducklake::{MacroImplementation, MacroParameter};
use pyo3::prelude::*;

use crate::error;
use crate::utils::runtime::block_on;

pub(crate) type PyMacroParameter = (String, String, Option<String>, String);
pub(crate) type PyMacroImplementation = (String, String, Vec<PyMacroParameter>);

#[pyclass]
pub struct PyMacro(ducklake::Macro);

#[pymethods]
impl PyMacro {
    #[getter]
    pub fn name(&self, py: Python) -> PyResult<(String, String)> {
        let name = block_on(py, self.0.name()).map_err(error::into_pyerr)?;
        Ok((name.schema, name.name))
    }

    #[getter]
    pub fn macro_type(&self, py: Python) -> PyResult<String> {
        block_on(py, self.0.macro_type())
            .map(|macro_type| macro_type.to_string())
            .map_err(error::into_pyerr)
    }

    #[getter]
    pub fn implementations(&self, py: Python) -> PyResult<Vec<PyMacroImplementation>> {
        block_on(py, self.0.implementations())
            .map(|implementations| {
                implementations
                    .into_iter()
                    .map(|implementation| {
                        (
                            implementation.dialect,
                            implementation.sql,
                            implementation
                                .parameters
                                .into_iter()
                                .map(|parameter| {
                                    (
                                        parameter.name,
                                        parameter.data_type,
                                        parameter.default_value,
                                        parameter.default_value_type,
                                    )
                                })
                                .collect(),
                        )
                    })
                    .collect()
            })
            .map_err(error::into_pyerr)
    }
}

impl PyMacro {
    pub fn new(macro_: ducklake::Macro) -> Self {
        Self(macro_)
    }
}

pub(crate) fn macro_implementations_from_python(
    implementations: Vec<PyMacroImplementation>,
) -> Vec<MacroImplementation> {
    implementations
        .into_iter()
        .map(|(dialect, sql, parameters)| MacroImplementation {
            dialect,
            sql,
            parameters: parameters
                .into_iter()
                .map(
                    |(name, data_type, default_value, default_value_type)| MacroParameter {
                        name,
                        data_type,
                        default_value,
                        default_value_type,
                    },
                )
                .collect(),
        })
        .collect()
}
