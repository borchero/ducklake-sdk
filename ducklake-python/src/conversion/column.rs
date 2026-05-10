use ducklake::{Column, ColumnDefault, DataType, Tag, Value};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::Wrap;
use super::py_modules::*;

impl FromPyObject<'_, '_> for Wrap<Column> {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let name: String = ob.getattr("name")?.extract()?;
        let dtype = ob.getattr("data_type")?.extract::<Wrap<DataType>>()?;
        let nullable: bool = ob.getattr("nullable")?.extract()?;
        let tags_obj = ob.getattr("tags")?;
        let tags_dict: Bound<'_, PyDict> = tags_obj.to_owned().cast_into()?;
        let tags: Vec<_> = tags_dict
            .iter()
            .map(|(k, v)| {
                Ok(Tag {
                    key: k.extract()?,
                    value: v.extract()?,
                })
            })
            .collect::<PyResult<_>>()?;
        let field_id: Option<i64> = ob.getattr("field_id")?.extract()?;

        let initial_default: Option<Wrap<Value>> = ob.getattr("initial_default")?.extract()?;
        let default_value: Wrap<ColumnDefault> = ob.getattr("default_value")?.extract()?;

        let col = Column::new(name, dtype.0)
            .nullable(nullable)
            .tags(tags)
            .field_id(field_id)
            .initial_default(initial_default.map(|w| w.0))
            .default_value(default_value.0);
        Ok(Wrap(col))
    }
}

impl<'py> IntoPyObject<'py> for Wrap<Column> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dl = ducklake_module(py).bind(py);
        let column_cls = dl.getattr("Column")?;
        let data_type = Wrap(self.0.dtype.clone()).into_pyobject(py)?;
        let tags: Option<Bound<'py, PyDict>> = if self.0.tags.is_empty() {
            None
        } else {
            let dict = PyDict::new(py);
            for tag in &self.0.tags {
                dict.set_item(&tag.key, &tag.value)?;
            }
            Some(dict)
        };
        let args = (&self.0.name, data_type);
        let kwargs = PyDict::new(py);
        kwargs.set_item("nullable", self.0.nullable)?;
        kwargs.set_item("tags", tags)?;
        kwargs.set_item("field_id", self.0.field_id)?;
        let py_val = self.0.initial_default.map(Wrap).into_pyobject(py)?;
        kwargs.set_item("initial_default", py_val)?;
        let py_dv = match self.0.default_value {
            ColumnDefault::Literal(v) => v.map(Wrap).into_pyobject(py)?,
            ColumnDefault::Expression {
                dialect,
                expression,
            } => (dialect, expression).into_pyobject(py)?.into_any(),
        };
        kwargs.set_item("default_value", py_dv)?;
        column_cls.call(args, Some(&kwargs))
    }
}
