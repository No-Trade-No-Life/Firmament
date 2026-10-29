//! The calendar semantics of a dataset: how often the hot window rolls over.
//!
//! A dataset is one piece of information with two storage shapes: the hot window
//! (a writable `SQLite` used for online collection) and the cold tree (Parquet
//! used for analysis). Cooling never empties the hot window; it clears out the
//! rows that belong to a period which has already rolled over.
//!
//! Every instant is UTC: writers send RFC 3339 and the write path normalises it
//! to UTC microseconds, so periods and partitions are cut in UTC too. A dataset
//! therefore carries no timezone at all — rolling the window over at a UTC
//! midnight instead of a local one only shifts a few hours of rows between one
//! cooling pass and the next, which does not change what the data means.

use chrono::{DateTime, Datelike, Days, NaiveTime, Utc};
use serde::{Deserialize, Serialize};

use crate::schema::Granularity;

/// How often the hot window rolls over and the previous period cools.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Cadence {
    /// The window is the current UTC day.
    Daily,
    /// The window is the current ISO week, which starts on Monday.
    Weekly,
    /// The window is the current UTC month.
    Monthly,
}

impl Cadence {
    /// The stored text form.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Daily => "daily",
            Self::Weekly => "weekly",
            Self::Monthly => "monthly",
        }
    }

    /// Reads the stored text form.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "daily" => Some(Self::Daily),
            "weekly" => Some(Self::Weekly),
            "monthly" => Some(Self::Monthly),
            _ => None,
        }
    }
}

/// The instant where the current period started.
///
/// Every row older than this belongs to a period that rolled over, so it is the
/// boundary between the hot window and the data waiting to cool. The hot window
/// keeps the current period; it is never emptied.
pub fn boundary(now: DateTime<Utc>, cadence: Cadence) -> DateTime<Utc> {
    let today = now.date_naive();
    let start = match cadence {
        Cadence::Daily => today,
        Cadence::Weekly => today - Days::new(u64::from(now.weekday().num_days_from_monday())),
        Cadence::Monthly => today.with_day(1).unwrap_or(today),
    };
    start.and_time(NaiveTime::MIN).and_utc()
}

/// Width of the bucket key inside a normalised timestamp.
///
/// The key is simply the prefix of the UTC timestamp, which keeps the cold tree,
/// the hot query and the manifest all naming a bucket the same way: `2026-10-01`
/// for a day, `2026-10-01T13` for an hour and `2026-10` for a month.
pub fn key_length(granularity: Granularity) -> usize {
    match granularity {
        Granularity::Hour => 13,
        Granularity::Day => 10,
        Granularity::Month => 7,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use super::{Cadence, boundary, key_length};
    use crate::schema::Granularity;

    fn at(raw: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(raw)
            .expect("parses")
            .with_timezone(&Utc)
    }

    #[test]
    fn the_window_rolls_over_at_utc_midnight() {
        assert_eq!(
            boundary(at("2026-10-05T03:00:00Z"), Cadence::Daily),
            at("2026-10-05T00:00:00Z")
        );
        // Late UTC evening is still the same UTC day, whatever the local date.
        assert_eq!(
            boundary(at("2026-10-05T23:59:59Z"), Cadence::Daily),
            at("2026-10-05T00:00:00Z")
        );
    }

    #[test]
    fn weeks_start_on_monday_and_months_on_the_first() {
        // 2026-10-07 is a Wednesday.
        assert_eq!(
            boundary(at("2026-10-07T12:00:00Z"), Cadence::Weekly),
            at("2026-10-05T00:00:00Z")
        );
        assert_eq!(
            boundary(at("2026-10-07T12:00:00Z"), Cadence::Monthly),
            at("2026-10-01T00:00:00Z")
        );
    }

    fn key(granularity: Granularity, instant: &str) -> String {
        let normalized = crate::schema::normalize_timestamp(instant).expect("normalises");
        normalized[..key_length(granularity)].to_owned()
    }

    #[test]
    fn partitions_are_cut_in_utc() {
        let instant = "2026-09-30T18:00:00.000000Z";
        assert_eq!(key(Granularity::Day, instant), "2026-09-30");
        assert_eq!(key(Granularity::Hour, instant), "2026-09-30T18");
        assert_eq!(key(Granularity::Month, instant), "2026-09");
        // An offset is normalised before the bucket is cut, not truncated.
        assert_eq!(
            key(Granularity::Day, "2026-10-01T02:00:00+08:00"),
            "2026-09-30"
        );
    }

    #[test]
    fn cadence_round_trips_through_its_stored_form() {
        for cadence in [Cadence::Daily, Cadence::Weekly, Cadence::Monthly] {
            assert_eq!(Cadence::parse(cadence.as_str()), Some(cadence));
        }
        assert_eq!(Cadence::parse("hourly"), None);
    }
}
