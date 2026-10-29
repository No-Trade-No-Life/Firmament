//! The calendar semantics of a dataset: which timezone its days are days in,
//! and how often the hot window rolls over.
//!
//! A dataset is one piece of information with two storage shapes: the hot window
//! (a writable `SQLite` used for online collection) and the cold tree (Parquet
//! used for analysis). Cooling never empties the hot window; it clears out the
//! rows that belong to a period which has already rolled over.

use chrono::{DateTime, Datelike, Days, NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

use crate::schema::{Granularity, parse_timestamp};

/// How often the hot window rolls over and the previous period cools.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Cadence {
    /// The window is the current calendar day.
    Daily,
    /// The window is the current ISO week, which starts on Monday.
    Weekly,
    /// The window is the current calendar month.
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

/// When the hot window rolls over, and in whose calendar.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CoolingSpec {
    /// Roll-over cadence.
    pub cadence: Cadence,
    /// IANA timezone name, e.g. `Asia/Shanghai`.
    pub timezone: String,
}

impl Default for CoolingSpec {
    fn default() -> Self {
        Self {
            cadence: Cadence::Daily,
            timezone: "UTC".to_owned(),
        }
    }
}

impl CoolingSpec {
    /// The timezone this dataset's calendar is expressed in.
    ///
    /// # Errors
    ///
    /// Returns an error when the stored name is not an IANA timezone.
    pub fn zone(&self) -> Result<Tz, String> {
        self.timezone
            .parse::<Tz>()
            .map_err(|_| format!("unknown timezone {}", self.timezone))
    }
}

/// Validates a cooling spec.
///
/// # Errors
///
/// Returns an error when the timezone is not an IANA name.
pub fn validate(cooling: &CoolingSpec) -> Result<(), String> {
    cooling.zone().map(|_| ())
}

/// The instant where the current period started.
///
/// Every row older than this belongs to a period that rolled over, so it is the
/// boundary between the hot window and the data waiting to cool. The hot window
/// keeps the current period; it is never emptied.
///
/// # Errors
///
/// Returns an error when the dataset carries an unknown timezone.
pub fn boundary(now: DateTime<Utc>, cooling: &CoolingSpec) -> Result<DateTime<Utc>, String> {
    let zone = cooling.zone()?;
    let local = now.with_timezone(&zone);
    let today = local.date_naive();
    let start = match cooling.cadence {
        Cadence::Daily => today,
        Cadence::Weekly => today - Days::new(u64::from(local.weekday().num_days_from_monday())),
        Cadence::Monthly => today.with_day(1).unwrap_or(today),
    };
    let midnight = start.and_time(NaiveTime::MIN);
    // ASSUMPTION: a period boundary is a civil midnight. In the rare zones where
    // a DST transition skips that exact minute the local time does not exist, so
    // the pass falls back to "now" — nothing cools this round, every row simply
    // stays writable in the hot window until the next period rolls over.
    Ok(zone
        .from_local_datetime(&midnight)
        .earliest()
        .map_or(now, |instant| instant.with_timezone(&Utc)))
}

/// The partition bucket of an instant, as named in the cold tree.
///
/// Buckets are cut in the dataset's timezone, so `day=2026-10-01` always means
/// that calendar day where the data lives, not a UTC day.
///
/// # Errors
///
/// Returns an error when the dataset carries an unknown timezone or the instant
/// cannot be parsed.
pub fn bucket(
    granularity: Granularity,
    cooling: &CoolingSpec,
    instant: &str,
) -> Result<String, String> {
    let zone = cooling.zone()?;
    let moment = parse_timestamp(instant)
        .ok_or_else(|| format!("unparsable timestamp {instant}"))?
        .with_timezone(&zone);
    let date = moment.date_naive();
    Ok(match granularity {
        Granularity::Hour => format!("{}T{:02}", date.format("%Y-%m-%d"), moment.hour()),
        Granularity::Day => date.format("%Y-%m-%d").to_string(),
        Granularity::Month => date.format("%Y-%m").to_string(),
    })
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use super::{Cadence, CoolingSpec, boundary, bucket};
    use crate::schema::Granularity;

    fn at(raw: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(raw)
            .expect("parses")
            .with_timezone(&Utc)
    }

    fn spec(cadence: Cadence, timezone: &str) -> CoolingSpec {
        CoolingSpec {
            cadence,
            timezone: timezone.to_owned(),
        }
    }

    #[test]
    fn the_window_rolls_over_at_civil_midnight_in_its_own_timezone() {
        let shanghai = spec(Cadence::Daily, "Asia/Shanghai");
        // 2026-10-05T03:00Z is 11:00 in Shanghai, so the window started at
        // 2026-10-05T00:00+08:00 == 2026-10-04T16:00Z.
        assert_eq!(
            boundary(at("2026-10-05T03:00:00Z"), &shanghai).expect("boundary"),
            at("2026-10-04T16:00:00Z")
        );
        assert_eq!(
            boundary(at("2026-10-05T03:00:00Z"), &spec(Cadence::Daily, "UTC")).expect("boundary"),
            at("2026-10-05T00:00:00Z")
        );
    }

    #[test]
    fn weeks_start_on_monday_and_months_on_the_first() {
        // 2026-10-07 is a Wednesday.
        assert_eq!(
            boundary(at("2026-10-07T12:00:00Z"), &spec(Cadence::Weekly, "UTC")).expect("weekly"),
            at("2026-10-05T00:00:00Z")
        );
        assert_eq!(
            boundary(at("2026-10-07T12:00:00Z"), &spec(Cadence::Monthly, "UTC")).expect("monthly"),
            at("2026-10-01T00:00:00Z")
        );
    }

    #[test]
    fn partitions_are_cut_in_the_dataset_timezone() {
        let shanghai = spec(Cadence::Daily, "Asia/Shanghai");
        // 18:00Z on Sep 30 is already Oct 1 in Shanghai.
        assert_eq!(
            bucket(Granularity::Day, &shanghai, "2026-09-30T18:00:00.000000Z").expect("bucket"),
            "2026-10-01"
        );
        assert_eq!(
            bucket(Granularity::Hour, &shanghai, "2026-09-30T18:00:00.000000Z").expect("bucket"),
            "2026-10-01T02"
        );
        assert_eq!(
            bucket(Granularity::Month, &shanghai, "2026-09-30T18:00:00.000000Z").expect("bucket"),
            "2026-10"
        );
    }

    #[test]
    fn unknown_timezones_are_rejected() {
        assert!(spec(Cadence::Daily, "Mars/Olympus").zone().is_err());
        assert!(spec(Cadence::Daily, "Asia/Shanghai").zone().is_ok());
    }
}
