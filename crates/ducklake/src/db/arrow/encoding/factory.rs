use arrow_array::ArrayRef;
use arrow_schema::Field;
use itertools::Itertools;

use super::nested::*;
use super::primitive::*;
use super::{ArrayExtractor, TypeEncoder};
use crate::{DataType, DucklakeResult, TimestampPrecision};

pub(in crate::db) fn make_column_encoder<E: TypeEncoder>(
    field: &Field,
    array: ArrayRef,
) -> DucklakeResult<Box<dyn ArrayExtractor<E>>> {
    let column = crate::Column::try_from(field)?;
    let encoder: Box<dyn ArrayExtractor<E>> = match column.dtype {
        DataType::Boolean => Box::new(BooleanArrayExtractor::new(&array)),
        DataType::Int8 => Box::new(Int8ArrayExtractor::new(&array)),
        DataType::Int16 => Box::new(Int16ArrayExtractor::new(&array)),
        DataType::Int32 => Box::new(Int32ArrayExtractor::new(&array)),
        DataType::Int64 => Box::new(Int64ArrayExtractor::new(&array)),
        DataType::Int128 => Box::new(Int128ArrayExtractor::new(&array)),
        DataType::UInt8 => Box::new(UInt8ArrayExtractor::new(&array)),
        DataType::UInt16 => Box::new(UInt16ArrayExtractor::new(&array)),
        DataType::UInt32 => Box::new(UInt32ArrayExtractor::new(&array)),
        DataType::UInt64 => Box::new(UInt64ArrayExtractor::new(&array)),
        DataType::UInt128 => Box::new(UInt128ArrayExtractor::new(&array)),
        DataType::Float32 => Box::new(Float32ArrayExtractor::new(&array)),
        DataType::Float64 => Box::new(Float64ArrayExtractor::new(&array)),
        DataType::Decimal {
            precision: _,
            scale,
        } => Box::new(DecimalArrayExtractor::new(&array, scale)),
        DataType::Date => Box::new(DateArrayExtractor::new(&array)),
        DataType::Time => arrow_match_time!(array.data_type(),
            microsecond => Box::new(TimeMicrosecondArrayExtractor::new(&array)),
            nanosecond => Box::new(TimeNanosecondArrayExtractor::new(&array))
        ),
        DataType::TimeTz => Box::new(TimeTzArrayExtractor::new(&array)),
        DataType::Timestamp { precision } => {
            use TimestampPrecision::*;
            match precision {
                Seconds => Box::new(TimestampSecondArrayExtractor::new(&array)),
                Milliseconds => Box::new(TimestampMillisecondArrayExtractor::new(&array)),
                Microseconds => Box::new(TimestampMicrosecondArrayExtractor::new(&array)),
                Nanoseconds => Box::new(TimestampNanosecondArrayExtractor::new(&array)),
            }
        }
        DataType::TimestampTz => Box::new(TimestampTzArrayExtractor::new(&array)),
        DataType::Interval => Box::new(IntervalArrayExtractor::new(&array)),
        DataType::Varchar => Box::new(StringArrayExtractor::new(&array)),
        DataType::Blob => Box::new(BinaryArrayExtractor::new(&array)),
        DataType::Json => Box::new(StringArrayExtractor::new(&array)),
        DataType::Uuid => Box::new(UuidArrayExtractor::new(&array)),
        DataType::List(inner) => Box::new(LargeListArrayExtractor::<E>::new(
            &array,
            &inner.to_arrow_field(),
        )?),
        DataType::Struct(fields) => Box::new(StructArrayExtractor::<E>::new(
            &array,
            &fields.iter().map(|f| f.to_arrow_field()).collect_vec(),
        )?),
        DataType::Map(key, value) => Box::new(MapArrayExtractor::<E>::new(
            &array,
            &key.to_arrow_field(),
            &value.to_arrow_field(),
        )?),
    };
    Ok(encoder)
}
