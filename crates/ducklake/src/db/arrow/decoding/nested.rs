use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, builder as arrow_builder};
use indexmap::IndexMap;

use super::{ArrayAppender, TypeDecoder, factory};
use crate::DucklakeResult;
use crate::spec::literals;

/* -------------------------------------------- LIST ------------------------------------------- */

pub struct LargeListArrayAppender<D: TypeDecoder> {
    builder: arrow_builder::LargeListBuilder<ColumnArrayBuilder<D>>,
}

impl<D: TypeDecoder> LargeListArrayAppender<D> {
    pub fn new(field: arrow_schema::FieldRef) -> DucklakeResult<Self> {
        let inner_builder = ColumnArrayBuilder::new(factory::make_array_appender::<D>(&field)?);
        let builder = Self {
            builder: arrow_builder::LargeListBuilder::new(inner_builder).with_field(field),
        };
        Ok(builder)
    }
}

impl<D: TypeDecoder> ArrayAppender<D> for LargeListArrayAppender<D> {
    fn append(&mut self, row: &D::Row, name: &str) -> DucklakeResult<()> {
        if let Some(text) = D::decode_text(row, name)? {
            self.append_text(&text)?;
        } else {
            self.append_null();
        }
        Ok(())
    }

    fn append_text(&mut self, text: &str) -> DucklakeResult<()> {
        if let Some(elements) = literals::parse::<Vec<String>>(text)? {
            for elem in elements {
                self.builder.values().append_text(&elem)?;
            }
            self.builder.append(true);
        } else {
            self.append_null();
        }
        Ok(())
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

/* ------------------------------------------- STRUCT ------------------------------------------ */

pub struct StructArrayAppender<D: TypeDecoder> {
    builder: arrow_builder::StructBuilder,
    field_indices: HashMap<String, usize>,
    _marker: std::marker::PhantomData<D>,
}

impl<D: TypeDecoder> StructArrayAppender<D> {
    pub fn new(fields: &arrow_schema::Fields) -> DucklakeResult<Self> {
        let field_indices = fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().clone(), i))
            .collect();
        let inner_builders = fields
            .iter()
            .map(|f| {
                factory::make_array_appender::<D>(f)
                    .map(ColumnArrayBuilder::new)
                    .map(|arr| Box::new(arr) as Box<dyn arrow_builder::ArrayBuilder>)
            })
            .collect::<DucklakeResult<_>>()?;
        let builder = Self {
            builder: arrow_builder::StructBuilder::new(fields.to_vec(), inner_builders),
            field_indices,
            _marker: std::marker::PhantomData,
        };
        Ok(builder)
    }
}

impl<D: TypeDecoder> ArrayAppender<D> for StructArrayAppender<D> {
    fn append(&mut self, row: &D::Row, name: &str) -> DucklakeResult<()> {
        if let Some(text) = D::decode_text(row, name)? {
            self.append_text(&text)?;
        } else {
            self.append_null();
        }
        Ok(())
    }

    fn append_text(&mut self, text: &str) -> DucklakeResult<()> {
        if let Some(entries) = literals::parse::<IndexMap<String, String>>(text)? {
            for (field_name, value) in entries {
                self.builder
                    .field_builder::<ColumnArrayBuilder<D>>(
                        self.field_indices.get(&field_name).copied().unwrap(),
                    )
                    .unwrap()
                    .append_text(&value)?;
            }
            self.builder.append(true);
        } else {
            self.append_null();
        }
        Ok(())
    }

    fn append_null(&mut self) {
        self.builder.field_builders_mut().iter_mut().for_each(|b| {
            b.as_any_mut()
                .downcast_mut::<ColumnArrayBuilder<D>>()
                .unwrap()
                .append_null()
        });
        self.builder.append_null();
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

/* -------------------------------------------- MAP -------------------------------------------- */

pub struct MapArrayAppender<D: TypeDecoder> {
    builder: arrow_builder::MapBuilder<ColumnArrayBuilder<D>, ColumnArrayBuilder<D>>,
}

impl<D: TypeDecoder> MapArrayAppender<D> {
    pub fn new(entries_field: arrow_schema::FieldRef) -> DucklakeResult<Self> {
        let arrow_schema::DataType::Struct(fields) = entries_field.data_type() else {
            panic!("map entries field must have a struct data type")
        };
        let key_field = fields[0].clone();
        let value_field = fields[1].clone();
        let field_names = arrow_builder::MapFieldNames {
            entry: entries_field.name().clone(),
            key: key_field.name().clone(),
            value: value_field.name().clone(),
        };
        let keys = ColumnArrayBuilder::new(factory::make_array_appender::<D>(&key_field)?);
        let values = ColumnArrayBuilder::new(factory::make_array_appender::<D>(&value_field)?);
        let builder = arrow_builder::MapBuilder::new(Some(field_names), keys, values)
            .with_keys_field(key_field)
            .with_values_field(value_field);
        Ok(Self { builder })
    }
}

impl<D: TypeDecoder> ArrayAppender<D> for MapArrayAppender<D> {
    fn append(&mut self, row: &D::Row, name: &str) -> DucklakeResult<()> {
        if let Some(text) = D::decode_text(row, name)? {
            self.append_text(&text)?;
        } else {
            self.append_null();
        }
        Ok(())
    }

    fn append_text(&mut self, text: &str) -> DucklakeResult<()> {
        if let Some(entries) = literals::parse::<Vec<(String, String)>>(text)? {
            for (key, value) in entries {
                let (keys, values) = self.builder.entries();
                keys.append_text(&key)?;
                values.append_text(&value)?;
            }
            self.builder.append(true)?;
        } else {
            self.append_null();
        }
        Ok(())
    }

    fn append_null(&mut self) {
        self.builder.append(false).unwrap();
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                         COLUMN BUILDER                                        */
/* --------------------------------------------------------------------------------------------- */

/// Adapter that lets `Box<dyn ArrayAppender<D>>` be used as the values builder
/// of a nested Arrow builder.
struct ColumnArrayBuilder<D: TypeDecoder> {
    inner: Box<dyn ArrayAppender<D>>,
    len: usize,
}

impl<D: TypeDecoder> ColumnArrayBuilder<D> {
    fn new(inner: Box<dyn ArrayAppender<D>>) -> Self {
        Self { inner, len: 0 }
    }

    fn append_text(&mut self, text: &str) -> DucklakeResult<()> {
        self.inner.append_text(text)?;
        self.len += 1;
        Ok(())
    }

    fn append_null(&mut self) {
        self.inner.append_null();
        self.len += 1;
    }
}

impl<D: TypeDecoder> arrow_builder::ArrayBuilder for ColumnArrayBuilder<D> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }

    fn len(&self) -> usize {
        self.len
    }

    fn finish(&mut self) -> ArrayRef {
        self.len = 0;
        self.inner.finish()
    }

    fn finish_cloned(&self) -> ArrayRef {
        unimplemented!()
    }
}
