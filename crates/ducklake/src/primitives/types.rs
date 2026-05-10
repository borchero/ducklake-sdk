use chrono::{FixedOffset, Months, NaiveTime, TimeDelta};

/// A wall-clock time with an associated UTC offset.
#[derive(Debug, Clone, PartialEq)]
pub struct TimeWithTimezone {
    /// The wall-clock time.
    pub time: NaiveTime,
    /// The UTC offset associated with the time.
    pub offset: FixedOffset,
}

/// A calendar interval composed of a number of months and a sub-month time delta.
#[derive(Debug, Clone, PartialEq)]
pub struct Interval {
    /// The number of months in the interval.
    pub months: Months,
    /// The sub-month time delta of the interval.
    pub delta: TimeDelta,
}
