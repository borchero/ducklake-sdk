use std::num::ParseIntError;
use std::str::FromStr;
use std::sync::LazyLock;

use chrono::{DateTime, FixedOffset, Months, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta};

use super::Literal;
use crate::primitives::{Interval, TimeWithTimezone};
use crate::{DucklakeError, DucklakeResult};

/* ------------------------------------------- TIMETZ ------------------------------------------ */

impl Literal for TimeWithTimezone {
    fn parse(s: &str) -> DucklakeResult<Self> {
        // Parse a `HH[:MM]` time-zone offset preceded by a `NaiveTime` separated by a `+`.
        static RE_OFFSET: LazyLock<regex::Regex> =
            LazyLock::new(|| regex::Regex::new(r"(\d{2})(?::(\d{2}))?").unwrap());

        if let Some((time, offset)) = s.split_once('+') {
            let offset = RE_OFFSET
                .captures(offset)
                .and_then(|caps| {
                    let hours = caps[1].parse::<i32>().unwrap_or(0);
                    let minutes = caps
                        .get(2)
                        .map(|m| m.as_str().parse::<i32>().unwrap_or(0))
                        .unwrap_or(0);
                    FixedOffset::east_opt(hours * 3600 + minutes * 60)
                })
                .ok_or(DucklakeError::Parsing(format!(
                    "invalid time zone offset: {offset}"
                )))?;
            Ok(TimeWithTimezone {
                time: time.parse()?,
                offset,
            })
        } else {
            Ok(TimeWithTimezone {
                time: s.parse()?,
                offset: FixedOffset::east_opt(0).unwrap(),
            })
        }
    }

    fn format(&self) -> String {
        let offset_hours = self.offset.local_minus_utc() / 3600;
        let offset_minutes = (self.offset.local_minus_utc() % 3600) / 60;
        let mut result = format!("{}+{:02}", self.time, offset_hours);
        if offset_minutes != 0 {
            result.push_str(&format!(":{:02}", offset_minutes));
        }
        result
    }
}

/* ------------------------------------------ DATETIME ----------------------------------------- */

impl Literal for DateTime<chrono::Utc> {
    fn parse(s: &str) -> DucklakeResult<Self> {
        // The following code is copied and adapted from sqlx:
        // https://github.com/launchbadge/sqlx/blob/452da1acf549e94a6358a770e7513433f15b5f0a/sqlx-sqlite/src/types/chrono.rs#L125-L160
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Ok(dt.to_utc());
        }
        if let Ok(dt) = NaiveDate::from_str(s) {
            let dt = dt.and_time(NaiveTime::default());
            return Ok(dt.and_utc());
        }

        // Loop over common date time patterns, inspired by Diesel
        // https://github.com/diesel-rs/diesel/blob/93ab183bcb06c69c0aee4a7557b6798fd52dd0d8/diesel/src/sqlite/types/date_and_time/chrono.rs#L56-L97
        let sqlite_datetime_formats = &[
            // Most likely format
            "%F %T%.f",
            // Other formats in order of appearance in docs
            "%F %R",
            "%F %RZ",
            "%F %R%:z",
            "%F %T%.fZ",
            "%F %T%.f%:z",
            "%FT%R",
            "%FT%RZ",
            "%FT%R%:z",
            "%FT%T%.f",
            "%FT%T%.fZ",
            "%FT%T%.f%:z",
            // Special format for DuckDB
            "%F %T%.f%#z",
        ];

        for format in sqlite_datetime_formats {
            if let Ok(dt) = DateTime::parse_from_str(s, format) {
                return Ok(dt.to_utc());
            }
            if let Ok(dt) = NaiveDateTime::parse_from_str(s, format) {
                return Ok(dt.and_utc());
            }
        }
        Err(DucklakeError::Parsing(format!(
            "failed to parse datetime '{}'",
            s
        )))
    }

    fn format(&self) -> String {
        self.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, false)
    }
}

impl Literal for NaiveDateTime {
    fn parse(s: &str) -> DucklakeResult<Self> {
        let dt = DateTime::<chrono::Utc>::parse(s)?;
        Ok(dt.naive_utc())
    }

    fn format(&self) -> String {
        Literal::format(&self.and_utc())
    }
}

/* ------------------------------------------ INTERVAL ----------------------------------------- */

impl Literal for Interval {
    fn parse(s: &str) -> DucklakeResult<Self> {
        // Parse an interval string of the form `[Y years] [M months] [D days] [HH:MM:SS[.ffffff]]`
        // into the number of months and the time delta of the remaining components.

        static RE_YEARS: LazyLock<regex::Regex> =
            LazyLock::new(|| regex::Regex::new(r"(\d+) years?").unwrap());
        static RE_MONTHS: LazyLock<regex::Regex> =
            LazyLock::new(|| regex::Regex::new(r"(\d+) months?").unwrap());
        static RE_DAYS: LazyLock<regex::Regex> =
            LazyLock::new(|| regex::Regex::new(r"(\d+) days?").unwrap());
        static RE_DELTA: LazyLock<regex::Regex> = LazyLock::new(|| {
            regex::Regex::new(r"(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?").unwrap()
        });

        let num_years = RE_YEARS
            .captures(s)
            .map(|caps| caps[1].parse::<u32>())
            .transpose()?
            .unwrap_or(0);
        let num_months = RE_MONTHS
            .captures(s)
            .map(|caps| caps[1].parse::<u32>())
            .transpose()?
            .unwrap_or(0);
        let num_days = RE_DAYS
            .captures(s)
            .map(|caps| caps[1].parse::<i64>())
            .transpose()?
            .unwrap_or(0);
        let num_microseconds = RE_DELTA
            .captures(s)
            .map(|caps| -> Result<i64, ParseIntError> {
                let hours = caps[1].parse::<i64>()?;
                let minutes = caps[2].parse::<i64>()?;
                let secs = caps[3].parse::<i64>()?;
                let micros = caps
                    .get(4)
                    .map(|m| {
                        let f_padded = format!("{:0<6}", m.as_str());
                        f_padded.parse::<i64>()
                    })
                    .transpose()?
                    .unwrap_or(0);
                Ok((hours * 3600 + minutes * 60 + secs) * 1_000_000 + micros)
            })
            .transpose()?
            .unwrap_or(0);

        Ok(Interval {
            months: Months::new(num_years * 12 + num_months),
            delta: TimeDelta::microseconds(num_days * 86_400_000_000 + num_microseconds),
        })
    }

    fn format(&self) -> String {
        let mut result = format_interval_months(self.months.as_u32());
        if !self.delta.is_zero() {
            if !result.is_empty() {
                result.push(' ');
            }
            result.push_str(&format_interval_microseconds(
                self.delta.num_microseconds().unwrap_or(0) as u64,
            ));
        }
        result
    }
}

fn format_interval_months(months: u32) -> String {
    let num_years = months / 12;
    let num_months = months % 12;
    let mut result = String::new();
    if num_years > 0 {
        result.push_str(&format_singular_plural("year", num_years));
    }
    if num_months > 0 {
        if !result.is_empty() {
            result.push(' ');
        }
        result.push_str(&format_singular_plural("month", num_months));
    }
    result
}

fn format_interval_microseconds(micros: u64) -> String {
    let hours = micros / 3_600_000_000;
    let minutes = (micros % 3_600_000_000) / 60_000_000;
    let seconds = (micros % 60_000_000) / 1_000_000;
    let mut result = format!("{:02}:{:02}:{:02}", hours, minutes, seconds);

    let micros = micros % 1_000_000;
    if micros > 0 {
        let frac = format!("{:06}", micros);
        let frac = frac.trim_end_matches('0');
        result.push_str(&format!(".{}", frac));
    }
    result
}

fn format_singular_plural(name: &str, count: u32) -> String {
    if count > 1 {
        format!("{} {}s", count, name)
    } else {
        format!("{} {}", count, name)
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                             TESTS                                             */
/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use chrono::Utc;
    use rstest::rstest;

    use super::*;

    /* ------------------------------------------ TIMETZ ----------------------------------------- */

    #[rstest]
    #[case("12:34:56+00", 12, 34, 56, 0)]
    #[case("12:34:56+02", 12, 34, 56, 2 * 3600)]
    #[case("00:00:00+14", 0, 0, 0, 14 * 3600)]
    #[case("08:15:30+05:30", 8, 15, 30, 5 * 3600 + 30 * 60)]
    fn test_time_with_timezone_parse(
        #[case] input: &str,
        #[case] h: u32,
        #[case] m: u32,
        #[case] s: u32,
        #[case] offset_secs: i32,
    ) {
        let parsed = TimeWithTimezone::parse(input).unwrap();
        assert_eq!(parsed.time, NaiveTime::from_hms_opt(h, m, s).unwrap());
        assert_eq!(parsed.offset, FixedOffset::east_opt(offset_secs).unwrap());
    }

    #[rstest]
    #[case(12, 34, 56, 0, "12:34:56+00")]
    #[case(12, 34, 56, 2 * 3600, "12:34:56+02")]
    #[case(8, 15, 30, 5 * 3600 + 30 * 60, "08:15:30+05:30")]
    fn test_time_with_timezone_format(
        #[case] h: u32,
        #[case] m: u32,
        #[case] s: u32,
        #[case] offset_secs: i32,
        #[case] expected: &str,
    ) {
        let value = TimeWithTimezone {
            time: NaiveTime::from_hms_opt(h, m, s).unwrap(),
            offset: FixedOffset::east_opt(offset_secs).unwrap(),
        };
        assert_eq!(value.format(), expected);
    }

    #[rstest]
    #[case("12:34:56+00")]
    #[case("08:15:30+05:30")]
    fn test_time_with_timezone_roundtrip(#[case] input: &str) {
        let parsed = TimeWithTimezone::parse(input).unwrap();
        assert_eq!(parsed.format(), input);
    }

    /* ----------------------------------------- DATETIME ---------------------------------------- */

    #[rstest]
    #[case("2024-01-15T12:34:56Z")]
    #[case("2024-01-15T12:34:56+00:00")]
    #[case("2024-01-15 12:34:56")]
    #[case("2024-01-15 12:34:56.123")]
    #[case("2024-01-15")]
    fn test_datetime_parses(#[case] input: &str) {
        assert!(<DateTime<Utc> as Literal>::parse(input).is_ok());
    }

    #[test]
    fn test_datetime_parse_invalid() {
        assert!(<DateTime<Utc> as Literal>::parse("not a date").is_err());
    }

    #[test]
    fn test_datetime_roundtrip() {
        let input = "2024-01-15T12:34:56+00:00";
        let parsed = <DateTime<Utc> as Literal>::parse(input).unwrap();
        let formatted = Literal::format(&parsed);
        let reparsed = <DateTime<Utc> as Literal>::parse(&formatted).unwrap();
        assert_eq!(parsed, reparsed);
    }

    #[rstest]
    #[case("2024-01-15T12:34:56Z")]
    #[case("2024-01-15 12:34:56")]
    fn test_naive_datetime_parses(#[case] input: &str) {
        assert!(<NaiveDateTime as Literal>::parse(input).is_ok());
    }

    #[test]
    fn test_naive_datetime_roundtrip() {
        let input = "2024-01-15T12:34:56Z";
        let parsed = <NaiveDateTime as Literal>::parse(input).unwrap();
        let reparsed = <NaiveDateTime as Literal>::parse(&Literal::format(&parsed)).unwrap();
        assert_eq!(parsed, reparsed);
    }

    /* ----------------------------------------- INTERVAL ---------------------------------------- */

    #[rstest]
    #[case("1 year", 12, 0)]
    #[case("2 years", 24, 0)]
    #[case("1 month", 1, 0)]
    #[case("6 months", 6, 0)]
    #[case("1 year 3 months", 15, 0)]
    #[case("1 day", 0, 86_400_000_000)]
    #[case("3 days", 0, 3 * 86_400_000_000)]
    #[case("01:02:03", 0, (3600 + 2 * 60 + 3) * 1_000_000)]
    #[case("01:02:03.456", 0, (3600 + 2 * 60 + 3) * 1_000_000 + 456_000)]
    #[case("1 year 2 months 3 days 04:05:06", 14, 3 * 86_400_000_000 + (4 * 3600 + 5 * 60 + 6) * 1_000_000)]
    fn test_interval_parse(
        #[case] input: &str,
        #[case] expected_months: u32,
        #[case] expected_micros: i64,
    ) {
        let parsed = Interval::parse(input).unwrap();
        assert_eq!(parsed.months.as_u32(), expected_months);
        assert_eq!(parsed.delta.num_microseconds().unwrap(), expected_micros);
    }

    #[rstest]
    #[case(12, 0, "1 year")]
    #[case(24, 0, "2 years")]
    #[case(1, 0, "1 month")]
    #[case(6, 0, "6 months")]
    #[case(15, 0, "1 year 3 months")]
    #[case(0, 86_400_000_000, "24:00:00")]
    #[case(0, (3600 + 2 * 60 + 3) * 1_000_000, "01:02:03")]
    #[case(0, (3600 + 2 * 60 + 3) * 1_000_000 + 456_000, "01:02:03.456")]
    #[case(14, (4 * 3600 + 5 * 60 + 6) * 1_000_000, "1 year 2 months 04:05:06")]
    fn test_interval_format(#[case] months: u32, #[case] micros: i64, #[case] expected: &str) {
        let value = Interval {
            months: Months::new(months),
            delta: TimeDelta::microseconds(micros),
        };
        assert_eq!(value.format(), expected);
    }

    #[rstest]
    #[case("1 year")]
    #[case("2 years")]
    #[case("1 year 3 months")]
    #[case("01:02:03")]
    #[case("01:02:03.456")]
    #[case("1 year 2 months 04:05:06")]
    fn test_interval_roundtrip(#[case] input: &str) {
        let parsed = Interval::parse(input).unwrap();
        assert_eq!(parsed.format(), input);
    }

    #[test]
    fn test_interval_empty() {
        let value = Interval {
            months: Months::new(0),
            delta: TimeDelta::zero(),
        };
        assert_eq!(value.format(), "");
    }
}
