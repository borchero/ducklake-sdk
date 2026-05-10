use std::sync::Arc;

use arrow_schema::Field;
use itertools::Itertools;

use super::nested::*;
use super::primitive::*;
pub use super::{ArrayAppender, TypeDecoder};
use crate::{DataType, DucklakeResult, TimestampPrecision};

pub fn make_array_appender<D: TypeDecoder>(
    field: &Field,
) -> DucklakeResult<Box<dyn ArrayAppender<D>>> {
    let column = crate::Column::try_from(field)?;
    let appender: Box<dyn ArrayAppender<D>> = match &column.dtype {
        DataType::Boolean => Box::new(BooleanArrayAppender::new()),
        DataType::Int8 => Box::new(Int8ArrayAppender::new()),
        DataType::Int16 => Box::new(Int16ArrayAppender::new()),
        DataType::Int32 => Box::new(Int32ArrayAppender::new()),
        DataType::Int64 => Box::new(Int64ArrayAppender::new()),
        DataType::Int128 => Box::new(Int128ArrayAppender::new()),
        DataType::UInt8 => Box::new(UInt8ArrayAppender::new()),
        DataType::UInt16 => Box::new(UInt16ArrayAppender::new()),
        DataType::UInt32 => Box::new(UInt32ArrayAppender::new()),
        DataType::UInt64 => Box::new(UInt64ArrayAppender::new()),
        DataType::UInt128 => Box::new(UInt128ArrayAppender::new()),
        DataType::Float32 => Box::new(Float32ArrayAppender::new()),
        DataType::Float64 => Box::new(Float64ArrayAppender::new()),
        DataType::Decimal { precision, scale } => {
            Box::new(DecimalArrayAppender::new(*precision, *scale as i8)?)
        }
        DataType::Date => Box::new(DateArrayAppender::new()),
        DataType::Time => Box::new(TimeArrayAppender::new()),
        DataType::TimeTz => Box::new(TimeTzArrayAppender::new()),
        DataType::Timestamp { precision } => match precision {
            TimestampPrecision::Seconds => Box::new(TimestampSecondArrayAppender::new()),
            TimestampPrecision::Milliseconds => Box::new(TimestampMillisecondArrayAppender::new()),
            TimestampPrecision::Microseconds => Box::new(TimestampMicrosecondArrayAppender::new()),
            TimestampPrecision::Nanoseconds => Box::new(TimestampNanosecondArrayAppender::new()),
        },
        DataType::TimestampTz => Box::new(TimestampTzArrayAppender::new()),
        DataType::Interval => Box::new(IntervalArrayAppender::new()),
        DataType::Varchar => Box::new(StringViewArrayAppender::new()),
        DataType::Blob => Box::new(LargeBinaryArrayAppender::new()),
        DataType::Json => Box::new(StringViewArrayAppender::new()),
        DataType::Uuid => Box::new(UuidArrayAppender::new()),
        DataType::List(inner) => Box::new(LargeListArrayAppender::<D>::new(Arc::new(
            inner.to_arrow_field(),
        ))?),
        DataType::Struct(fields) => Box::new(StructArrayAppender::<D>::new(
            &fields
                .iter()
                .map(|f| f.to_arrow_field())
                .collect_vec()
                .into(),
        )?),
        DataType::Map(_, _) => {
            let entries = column.to_arrow_field();
            Box::new(MapArrayAppender::<D>::new(Arc::new(entries))?)
        }
    };
    Ok(appender)
}
