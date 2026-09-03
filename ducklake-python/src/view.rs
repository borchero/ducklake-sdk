use std::collections::HashMap;
use std::ops::ControlFlow;

use pyo3::prelude::*;
use sqlparser::ast::{Ident, ObjectName, visit_relations_mut};
use sqlparser::dialect::dialect_from_str;
use sqlparser::parser::Parser;

use crate::conversion::Wrap;
use crate::utils::runtime::block_on;
use crate::{PyTable, error};

#[pyclass]
pub struct PyView(ducklake::View);

impl PyView {
    pub fn new(view: ducklake::View) -> Self {
        PyView(view)
    }
}

#[pymethods]
impl PyView {
    #[getter]
    pub fn name(&self, py: Python) -> PyResult<(String, String)> {
        let name = block_on(py, self.0.name()).map_err(error::into_pyerr)?;
        Ok((name.schema, name.name))
    }

    #[getter]
    pub fn sql(&self, py: Python) -> PyResult<String> {
        block_on(py, self.0.sql()).map_err(error::into_pyerr)
    }

    #[getter]
    pub fn column_aliases(&self, py: Python) -> PyResult<Option<Vec<String>>> {
        block_on(py, self.0.column_aliases()).map_err(error::into_pyerr)
    }

    #[getter]
    pub fn tags(&self, py: Python) -> PyResult<Vec<Wrap<ducklake::Tag>>> {
        let tags = block_on(py, self.0.tags()).map_err(error::into_pyerr)?;
        Ok(tags.into_iter().map(|tag| tag.into()).collect())
    }

    pub fn polars_query(&self, py: Python) -> PyResult<(String, Vec<(String, PyTable)>)> {
        let definition = block_on(py, self.0.definition()).map_err(error::into_pyerr)?;
        let (sql, tables) = normalize_query(definition);
        Ok((
            sql,
            tables
                .into_iter()
                .map(|(alias, table)| (alias, PyTable::new(table)))
                .collect(),
        ))
    }

    pub fn delete(&self, py: Python) -> PyResult<()> {
        block_on(py, self.0.delete()).map_err(error::into_pyerr)
    }
}

fn normalize_query(
    definition: ducklake::ViewDefinition,
) -> (String, Vec<(String, ducklake::Table)>) {
    let dialect = dialect_from_str(&definition.dialect).unwrap();
    let mut statement = Parser::parse_sql(&*dialect, &definition.sql)
        .unwrap()
        .pop()
        .unwrap();

    let mut tables = Vec::with_capacity(definition.tables.len());
    let aliases = definition
        .tables
        .into_iter()
        .enumerate()
        .map(|(index, (name, table))| {
            let alias = format!("__ducklake_table_{index}");
            tables.push((alias.clone(), table));
            (name, alias)
        })
        .collect::<HashMap<_, _>>();

    let _ = visit_relations_mut(&mut statement, |relation| -> ControlFlow<()> {
        // Polars SQLContext does not support schemas. Replace known DuckLake table names with
        // unique aliases that can be registered as flat frame names.
        let Ok(name) = ducklake::TableName::try_from(relation.to_string()) else {
            return ControlFlow::Continue(());
        };
        let Some(alias) = aliases.get(&name) else {
            return ControlFlow::Continue(());
        };
        *relation = ObjectName::from(Ident::new(alias));
        ControlFlow::Continue(())
    });

    (statement.to_string(), tables)
}
