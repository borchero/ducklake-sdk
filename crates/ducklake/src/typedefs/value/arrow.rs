use std::sync::Arc;

use arrow_array::*;
use arrow_schema::TimeUnit;

use super::Value;
use crate::io::arrow::conversion::{IntoPhysical, IntoPhysicalWithContext};
use crate::{Column, DataType, DucklakeError, DucklakeResult};

impl Value {
    pub(crate) fn broadcast_into_array(
        &self,
        dtype: &DataType,
        len: usize,
    ) -> DucklakeResult<ArrayRef> {
        // Build one scalar first, so view arrays share large default strings across all rows.
        let scalar: ArrayRef = match self.clone() {
            Value::Boolean(v) => Arc::new(BooleanArray::from(vec![v])),
            Value::Int8(v) => Arc::new(Int8Array::from_value(v, 1)),
            Value::Int16(v) => Arc::new(Int16Array::from_value(v, 1)),
            Value::Int32(v) => Arc::new(Int32Array::from_value(v, 1)),
            Value::Int64(v) => Arc::new(Int64Array::from_value(v, 1)),
            Value::UInt8(v) => Arc::new(UInt8Array::from_value(v, 1)),
            Value::UInt16(v) => Arc::new(UInt16Array::from_value(v, 1)),
            Value::UInt32(v) => Arc::new(UInt32Array::from_value(v, 1)),
            Value::UInt64(v) => Arc::new(UInt64Array::from_value(v, 1)),
            Value::Float32(v) => Arc::new(Float32Array::from_value(v, 1)),
            Value::Float64(v) => Arc::new(Float64Array::from_value(v, 1)),
            Value::Int128(v) => Arc::new(FixedSizeBinaryArray::try_from_iter(std::iter::once(
                v.into_physical(),
            ))?),
            Value::UInt128(v) => Arc::new(FixedSizeBinaryArray::try_from_iter(std::iter::once(
                v.into_physical(),
            ))?),
            Value::TimeTz(v) => Arc::new(FixedSizeBinaryArray::try_from_iter(std::iter::once(
                v.into_physical(),
            ))?),
            Value::Uuid(v) => Arc::new(FixedSizeBinaryArray::try_from_iter(std::iter::once(
                v.into_physical(),
            ))?),
            Value::Decimal(v) => Arc::new(
                Decimal128Array::from_value(v.mantissa(), 1)
                    .with_precision_and_scale(38, v.scale() as i8)?,
            ),
            Value::Date(v) => Arc::new(Date32Array::from_value(v.into_physical(), 1)),
            Value::Time(v) => Arc::new(Time64MicrosecondArray::from_value(v.into_physical(), 1)),
            Value::Timestamp(v) => {
                let DataType::Timestamp { precision } = dtype else {
                    return Err(DucklakeError::SchemaError(
                        "timestamp default requires a timestamp column".into(),
                    ));
                };
                match precision {
                    crate::TimestampPrecision::Seconds => {
                        Arc::new(TimestampSecondArray::from_value(
                            v.into_physical_with_context(&TimeUnit::Second),
                            1,
                        ))
                    }
                    crate::TimestampPrecision::Milliseconds => {
                        Arc::new(TimestampMillisecondArray::from_value(
                            v.into_physical_with_context(&TimeUnit::Millisecond),
                            1,
                        ))
                    }
                    crate::TimestampPrecision::Microseconds => {
                        Arc::new(TimestampMicrosecondArray::from_value(
                            v.into_physical_with_context(&TimeUnit::Microsecond),
                            1,
                        ))
                    }
                    crate::TimestampPrecision::Nanoseconds => {
                        Arc::new(TimestampNanosecondArray::from_value(
                            v.into_physical_with_context(&TimeUnit::Nanosecond),
                            1,
                        ))
                    }
                }
            }
            Value::TimestampTz(v) => Arc::new(
                TimestampMicrosecondArray::from_value(v.into_physical(), 1).with_timezone("UTC"),
            ),
            Value::Interval(v) => {
                Arc::new(IntervalMonthDayNanoArray::from_value(v.into_physical(), 1))
            }
            Value::Varchar(v) | Value::Json(v) => {
                Arc::new(StringViewArray::from(vec![v.as_str()]))
            }
            Value::Blob(v) => Arc::new(LargeBinaryArray::from_iter_values([v.as_slice()])),
            Value::List(_) | Value::Struct(_) | Value::Map(_) => {
                return Err(DucklakeError::SchemaError(
                    "nested literal defaults are unsupported".into(),
                ));
            }
        };
        let field = Column::new(String::new(), dtype.clone()).to_arrow_field();
        let scalar = arrow_cast::cast_with_options(
            &scalar,
            field.data_type(),
            &arrow_cast::CastOptions {
                safe: false,
                ..Default::default()
            },
        )?;
        Ok(arrow_select::take::take(
            scalar.as_ref(),
            &UInt32Array::from_value(0, len),
            None,
        )?)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::decimal(DataType::Decimal { precision: 6, scale: 2 }, Value::Decimal(rust_decimal::Decimal::new(123, 1)))]
    #[case::uuid(DataType::Uuid, Value::Uuid(uuid::Uuid::nil()))]
    #[case::timestamp_tz(DataType::TimestampTz, Value::TimestampTz(chrono::DateTime::from_timestamp(123, 0).unwrap()))]
    #[case::int128(DataType::Int128, Value::Int128(-123))]
    #[case::uint128(DataType::UInt128, Value::UInt128(123))]
    #[case::string(DataType::Varchar, Value::Varchar("a default string longer than twelve bytes".into()))]
    fn broadcast_defaults(
        #[case] dtype: DataType,
        #[case] value: Value,
        #[values(0, 3)] len: usize,
    ) {
        // Arrange
        let field = Column::new("x".into(), dtype.clone())
            .field_id(Some(1))
            .to_arrow_field();

        // Act
        let array = value.broadcast_into_array(&dtype, len).unwrap();

        // Assert
        assert_eq!(array.len(), len);
        assert_eq!(array.data_type(), field.data_type());
        assert_eq!(Column::try_from(&field).unwrap().dtype, dtype);
        assert_eq!(
            crate::io::arrow::aggregate::find_min(&dtype, &array),
            (len > 0).then_some(value)
        );
    }
}
