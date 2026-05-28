// Literal encoding/decoding follows the ducklake specs statistics encoding
// https://ducklake.select/docs/stable/specification/data_types#type-encoding-for-statistics
use chrono::{NaiveDate, NaiveTime};
use rust_decimal::Decimal;
use uuid::Uuid;

use super::Literal;
use crate::{DucklakeError, DucklakeResult};

macro_rules! str_literal {
    ($name:ident) => {
        impl Literal for $name {
            fn parse(s: &str) -> DucklakeResult<Self> {
                Ok(s.parse()?)
            }

            fn format(&self) -> String {
                self.to_string()
            }
        }
    };
}

str_literal!(i8);
str_literal!(i16);
str_literal!(i32);
str_literal!(i64);
str_literal!(i128);
str_literal!(u8);
str_literal!(u16);
str_literal!(u32);
str_literal!(u64);
str_literal!(u128);
str_literal!(f32);
str_literal!(f64);
str_literal!(Decimal);
str_literal!(NaiveTime);
str_literal!(NaiveDate);
str_literal!(String);
str_literal!(Uuid);

impl Literal for bool {
    fn parse(s: &str) -> DucklakeResult<Self> {
        match s.to_ascii_lowercase().as_str() {
            "1" => Ok(true),
            "0" => Ok(false),
            _ => Err(DucklakeError::Parsing(s.to_string())),
        }
    }

    fn format(&self) -> String {
        if *self {
            "1".to_string()
        } else {
            "0".to_string()
        }
    }
}

/* -------------------------------------------- BLOB ------------------------------------------- */

impl Literal for Vec<u8> {
    fn parse(s: &str) -> DucklakeResult<Self> {
        Ok(hex::decode(s)?)
    }

    fn format(&self) -> String {
        hex::encode(self)
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                             TESTS                                             */
/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::str::FromStr;

    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("1", true)]
    #[case("0", false)]
    fn test_bool_roundtrip(#[case] input: &str, #[case] expected: bool) {
        let parsed = <bool as Literal>::parse(input).unwrap();
        assert_eq!(parsed, expected);
        assert_eq!(parsed.format(), input);
    }

    #[rstest]
    #[case("0", 0i32)]
    #[case("42", 42i32)]
    #[case("-7", -7i32)]
    #[case("2147483647", i32::MAX)]
    #[case("-2147483648", i32::MIN)]
    fn test_i32_roundtrip(#[case] input: &str, #[case] expected: i32) {
        let parsed = <i32 as Literal>::parse(input).unwrap();
        assert_eq!(parsed, expected);
        assert_eq!(parsed.format(), input);
    }

    #[rstest]
    #[case("0", 0u64)]
    #[case("18446744073709551615", u64::MAX)]
    fn test_u64_roundtrip(#[case] input: &str, #[case] expected: u64) {
        let parsed = <u64 as Literal>::parse(input).unwrap();
        assert_eq!(parsed, expected);
        assert_eq!(parsed.format(), input);
    }

    #[rstest]
    #[case("0", 0.0f64)]
    #[case("3.14", 3.14f64)]
    #[case("-2.5", -2.5f64)]
    fn test_f64_roundtrip(#[case] input: &str, #[case] expected: f64) {
        let parsed = <f64 as Literal>::parse(input).unwrap();
        assert_eq!(parsed, expected);
        assert_eq!(parsed.format().parse::<f64>().unwrap(), expected);
    }

    #[rstest]
    #[case("0")]
    #[case("3.14")]
    #[case("-123.456")]
    #[case("1000000000000.000001")]
    fn test_decimal_roundtrip(#[case] input: &str) {
        let parsed = <Decimal as Literal>::parse(input).unwrap();
        assert_eq!(parsed, Decimal::from_str(input).unwrap());
        assert_eq!(parsed.format(), input);
    }

    #[rstest]
    #[case("2024-01-15")]
    #[case("1999-12-31")]
    fn test_naive_date_roundtrip(#[case] input: &str) {
        let parsed = <NaiveDate as Literal>::parse(input).unwrap();
        assert_eq!(Literal::format(&parsed), input);
    }

    #[rstest]
    #[case("12:34:56")]
    #[case("00:00:00")]
    #[case("23:59:59")]
    fn test_naive_time_roundtrip(#[case] input: &str) {
        let parsed = <NaiveTime as Literal>::parse(input).unwrap();
        assert_eq!(Literal::format(&parsed), input);
    }

    #[rstest]
    #[case("hello")]
    #[case("")]
    #[case("with spaces and symbols !@#")]
    fn test_string_roundtrip(#[case] input: &str) {
        let parsed = <String as Literal>::parse(input).unwrap();
        assert_eq!(parsed, input);
        assert_eq!(parsed.format(), input);
    }

    #[rstest]
    #[case("550e8400-e29b-41d4-a716-446655440000")]
    #[case("00000000-0000-0000-0000-000000000000")]
    fn test_uuid_roundtrip(#[case] input: &str) {
        let parsed = <Uuid as Literal>::parse(input).unwrap();
        assert_eq!(parsed, Uuid::from_str(input).unwrap());
        assert_eq!(parsed.format(), input);
    }

    #[rstest]
    #[case("", vec![])]
    #[case("00", vec![0x00])]
    #[case("deadbeef", vec![0xde, 0xad, 0xbe, 0xef])]
    #[case("0123456789abcdef", vec![0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef])]
    fn test_blob_roundtrip(#[case] input: &str, #[case] expected: Vec<u8>) {
        let parsed = <Vec<u8> as Literal>::parse(input).unwrap();
        assert_eq!(parsed, expected);
        assert_eq!(parsed.format(), input);
    }

    #[test]
    fn test_blob_parse_invalid_hex() {
        assert!(<Vec<u8> as Literal>::parse("zz").is_err());
        assert!(<Vec<u8> as Literal>::parse("0").is_err());
    }

    #[test]
    fn test_bool_parse_invalid() {
        assert!(<bool as Literal>::parse("yes").is_err());
    }

    #[test]
    fn test_i32_parse_invalid() {
        assert!(<i32 as Literal>::parse("not_a_number").is_err());
    }
}
