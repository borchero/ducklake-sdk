macro_rules! arrow_match_varchar {
    ($dtype:expr, utf8 => $utf8:expr, large_utf8 => $large_utf8:expr, utf8_view => $utf8_view:expr) => {
        match $dtype {
            arrow_schema::DataType::Utf8 => $utf8,
            arrow_schema::DataType::LargeUtf8 => $large_utf8,
            arrow_schema::DataType::Utf8View => $utf8_view,
            _ => unreachable!(),
        }
    };
}

macro_rules! arrow_match_binary {
    ($dtype:expr, binary => $binary:expr, large_binary => $large_binary:expr, binary_view => $binary_view:expr) => {
        match $dtype {
            arrow_schema::DataType::Binary => $binary,
            arrow_schema::DataType::LargeBinary => $large_binary,
            arrow_schema::DataType::BinaryView => $binary_view,
            _ => unreachable!(),
        }
    };
}

macro_rules! arrow_match_time {
    ($dtype:expr, microsecond => $microsecond:expr, nanosecond => $nanosecond:expr) => {
        match $dtype {
            arrow_schema::DataType::Time64(arrow_schema::TimeUnit::Microsecond) => $microsecond,
            arrow_schema::DataType::Time64(arrow_schema::TimeUnit::Nanosecond) => $nanosecond,
            _ => unreachable!(),
        }
    };
}

pub(crate) mod aggregate;
pub(crate) mod conversion;
mod statistics;

pub(crate) use statistics::compute_record_batch_statistics;
