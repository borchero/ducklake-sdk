use arrow_array::{Array, ArrayRef, LargeListArray, MapArray, StructArray};
use arrow_schema::Field;
use indexmap::IndexMap;
use sqlx::Arguments;

use super::{ArrayExtractor, TypeEncoder, factory};
use crate::DucklakeResult;
use crate::spec::literals;

/* -------------------------------------------- LIST ------------------------------------------- */

pub struct LargeListArrayExtractor<E: TypeEncoder> {
    array: LargeListArray,
    inner: Box<dyn ArrayExtractor<E>>,
}

impl<E: TypeEncoder> LargeListArrayExtractor<E> {
    pub fn new(array: &ArrayRef, inner_field: &Field) -> DucklakeResult<Self> {
        let array = array
            .as_any()
            .downcast_ref::<LargeListArray>()
            .unwrap()
            .clone();
        let inner = factory::make_column_encoder::<E>(inner_field, array.values().clone())?;
        Ok(Self { array, inner })
    }

    fn items(&self, row_idx: usize) -> Option<Vec<String>> {
        if self.array.is_null(row_idx) {
            return None;
        }
        let offsets = self.array.value_offsets();
        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        let elements = (start..end).map(|i| self.inner.extract_text(i)).collect();
        Some(elements)
    }
}

impl<E: TypeEncoder> ArrayExtractor<E> for LargeListArrayExtractor<E> {
    fn extract(&self, args: &mut E::Arguments, row_idx: usize) -> DucklakeResult<()> {
        let value = self.items(row_idx);
        let text = value.as_ref().map(|v| literals::format(Some(v)));
        args.add(E::encode_text(text.as_deref()))
            .map_err(sqlx::Error::Encode)?;
        Ok(())
    }

    fn extract_text(&self, row_idx: usize) -> String {
        literals::format(self.items(row_idx).as_ref())
    }
}

/* ------------------------------------------- STRUCT ------------------------------------------ */

pub struct StructArrayExtractor<E: TypeEncoder> {
    array: StructArray,
    children: Vec<(String, Box<dyn ArrayExtractor<E>>)>,
}

impl<E: TypeEncoder> StructArrayExtractor<E> {
    pub fn new(array: &ArrayRef, fields: &[arrow_schema::Field]) -> DucklakeResult<Self> {
        let array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone();
        let children = fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                factory::make_column_encoder::<E>(f, array.column(i).clone())
                    .map(|enc| (f.name().clone(), enc))
            })
            .collect::<DucklakeResult<Vec<_>>>()?;
        Ok(Self { array, children })
    }

    fn entries(&self, row_idx: usize) -> Option<IndexMap<String, String>> {
        if self.array.is_null(row_idx) {
            return None;
        }
        let entries = self
            .children
            .iter()
            .map(|(name, child)| (name.clone(), child.extract_text(row_idx)))
            .collect();
        Some(entries)
    }
}

impl<E: TypeEncoder> ArrayExtractor<E> for StructArrayExtractor<E> {
    fn extract(&self, args: &mut E::Arguments, row_idx: usize) -> DucklakeResult<()> {
        let value = self.entries(row_idx);
        let text = value.as_ref().map(|v| literals::format(Some(v)));
        args.add(E::encode_text(text.as_deref()))
            .map_err(sqlx::Error::Encode)?;
        Ok(())
    }

    fn extract_text(&self, row_idx: usize) -> String {
        literals::format(self.entries(row_idx).as_ref())
    }
}

/* -------------------------------------------- MAP -------------------------------------------- */

pub struct MapArrayExtractor<E: TypeEncoder> {
    array: MapArray,
    key_encoder: Box<dyn ArrayExtractor<E>>,
    value_encoder: Box<dyn ArrayExtractor<E>>,
}

impl<E: TypeEncoder> MapArrayExtractor<E> {
    pub fn new(array: &ArrayRef, key_field: &Field, value_field: &Field) -> DucklakeResult<Self> {
        let array = array.as_any().downcast_ref::<MapArray>().unwrap().clone();
        let key_encoder = factory::make_column_encoder::<E>(key_field, array.keys().clone())?;
        let value_encoder =
            factory::make_column_encoder::<E>(value_field, array.values().clone())?;
        Ok(Self {
            array,
            key_encoder,
            value_encoder,
        })
    }

    fn formatted(&self, row_idx: usize) -> Option<Vec<(String, String)>> {
        if self.array.is_null(row_idx) {
            return None;
        }
        let offsets = self.array.value_offsets();
        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        let entries = (start..end)
            .map(|i| {
                let key = self.key_encoder.extract_text(i);
                let value = self.value_encoder.extract_text(i);
                (key, value)
            })
            .collect();
        Some(entries)
    }
}

impl<E: TypeEncoder> ArrayExtractor<E> for MapArrayExtractor<E> {
    fn extract(&self, args: &mut E::Arguments, row_idx: usize) -> DucklakeResult<()> {
        let value = self.formatted(row_idx);
        let text = value.as_ref().map(|v| literals::format(Some(v)));
        args.add(E::encode_text(text.as_deref()))
            .map_err(sqlx::Error::Encode)?;
        Ok(())
    }

    fn extract_text(&self, row_idx: usize) -> String {
        literals::format(self.formatted(row_idx).as_ref())
    }
}
