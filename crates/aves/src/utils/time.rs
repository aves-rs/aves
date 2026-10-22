//! Generic UTC timestamp, calendar, and retention utilities.
use std::time::SystemTime;

use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, Months, NaiveDate, NaiveDateTime, ParseError, TimeZone, Utc,
};
use thiserror::Error;

/// A strict month-only retention horizon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionHorizon {
    /// Retention does not expire.
    Forever,
    /// Retention expires after the specified number of UTC calendar months.
    Months(u32),
}

/// Failure to parse or apply a strict retention horizon.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RetentionHorizonError {
    /// The label is outside the supported retention grammar.
    #[error("expected a positive integer followed by month or months, or forever")]
    InvalidLabel,
    /// The requested subtraction cannot be represented by Chrono's calendar.
    #[error("calendar-month cutoff is outside the supported UTC date range")]
    CalendarOutOfRange,
}

/// Parse a retention horizon without trimming, normalizing, or coercing it.
///
/// Accepted labels are exactly `forever`, or a positive base-10 integer followed
/// by `month` or `months` (for example, `1month` and `12months`).
pub fn parse_retention_horizon(label: &str) -> Result<RetentionHorizon, RetentionHorizonError> {
    if label == "forever" {
        return Ok(RetentionHorizon::Forever);
    }

    let count = label
        .strip_suffix("months")
        .or_else(|| label.strip_suffix("month"))
        .ok_or(RetentionHorizonError::InvalidLabel)?;
    if count.is_empty() || !count.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(RetentionHorizonError::InvalidLabel);
    }

    let months = count
        .parse::<u32>()
        .map_err(|_| RetentionHorizonError::InvalidLabel)?;
    if months == 0 {
        return Err(RetentionHorizonError::InvalidLabel);
    }
    Ok(RetentionHorizon::Months(months))
}

/// Calculate a UTC cutoff by subtracting a retention horizon in calendar months.
///
/// Finite horizons preserve the UTC clock time and clamp the day to the last day
/// of the target month (for example, March 31 minus one month is February 28).
pub fn retention_cutoff_utc(
    now: DateTime<Utc>,
    horizon: RetentionHorizon,
) -> Result<Option<DateTime<Utc>>, RetentionHorizonError> {
    let RetentionHorizon::Months(months) = horizon else {
        return Ok(None);
    };

    now.checked_sub_months(Months::new(months))
        .map(Some)
        .ok_or(RetentionHorizonError::CalendarOutOfRange)
}

/// Failure to parse timestamp text, preserving its input context.
#[derive(Debug, Error)]
pub enum TimeParseError {
    /// RFC3339 input could not be parsed.
    #[error("invalid RFC3339 timestamp: {source}")]
    InvalidRfc3339 {
        #[from]
        source: ParseError,
    },
    /// A persisted period value was not RFC3339.
    #[error("invalid persisted period timestamp: {source}")]
    InvalidPersistedPeriodTimestamp { source: ParseError },
    /// Neither RFC3339 nor SQLite timestamp parsing accepted a field.
    #[error("invalid {field}: {source}")]
    InvalidTimestampField { field: String, source: ParseError },
}

/// Convert a millisecond Unix timestamp to an ISO 8601 string.
///
/// Formats as `YYYY-MM-DDTHH:MM:SS.mmmZ`, falling back to raw milliseconds when out of range.
pub fn ms_to_iso(ms: i64) -> String {
    let secs = ms / 1000;
    let nanos = ((ms % 1000) * 1_000_000) as u32;
    Utc.timestamp_opt(secs, nanos)
        .single()
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        .unwrap_or_else(|| format!("{ms}"))
}

/// Monday-start ISO-style week bounds containing `now`.
///
/// Returns `(monday_00:00_utc, next_monday_00:00_utc)`.
pub fn week_bounds(now: DateTime<Utc>) -> (String, String) {
    let date = now.date_naive();
    let days_since_monday = i64::from(date.weekday().num_days_from_monday());
    let monday = date - ChronoDuration::days(days_since_monday);
    let next_monday = monday + ChronoDuration::days(7);
    (
        naive_date_to_utc(monday).to_rfc3339(),
        naive_date_to_utc(next_monday).to_rfc3339(),
    )
}

/// Calendar month bounds containing `now`.
///
/// Returns `(first_of_month_00:00_utc, first_of_next_month_00:00_utc)`.
pub fn month_bounds(now: DateTime<Utc>) -> (String, String) {
    let date = now.date_naive();
    let year = date.year();
    let month = date.month();
    let start = NaiveDate::from_ymd_opt(year, month, 1).expect("valid first-of-month date");
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1u32)
    } else {
        (year, month + 1)
    };
    let end = NaiveDate::from_ymd_opt(next_year, next_month, 1).expect("valid first-of-next-month date");
    (
        naive_date_to_utc(start).to_rfc3339(),
        naive_date_to_utc(end).to_rfc3339(),
    )
}

/// Calendar quarter bounds containing `now`.
///
/// Quarters are Q1 (Jan-Mar), Q2 (Apr-Jun), Q3 (Jul-Sep), Q4 (Oct-Dec).
/// Returns `(first_of_quarter_00:00_utc, first_of_next_quarter_00:00_utc)`.
pub fn quarter_bounds(now: DateTime<Utc>) -> (String, String) {
    let date = now.date_naive();
    let year = date.year();
    let month = date.month();
    let (q_start_month, q_end_month) = match month {
        1..=3 => (1u32, 4u32),
        4..=6 => (4, 7),
        7..=9 => (7, 10),
        _ => (10, 1),
    };
    let start = NaiveDate::from_ymd_opt(year, q_start_month, 1).expect("valid quarter-start date");
    let end = if q_end_month == 1 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(year, q_end_month, 1)
    }
    .expect("valid quarter-end date");
    (
        naive_date_to_utc(start).to_rfc3339(),
        naive_date_to_utc(end).to_rfc3339(),
    )
}

/// Lift a [`NaiveDate`] at midnight to a UTC [`DateTime`].
pub fn naive_date_to_utc(date: NaiveDate) -> DateTime<Utc> {
    Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).expect("valid midnight"))
}

/// Normalize a persisted period timestamp to canonical RFC3339 UTC text.
pub fn normalize_period_timestamp(value: &str) -> Result<String, TimeParseError> {
    let timestamp = parse_persisted_period_timestamp(value)?;
    Ok(timestamp.to_rfc3339())
}

/// Normalize an optional timestamp to canonical RFC3339 UTC text.
pub fn normalize_optional_timestamp(value: Option<String>, field: &str) -> Result<Option<String>, TimeParseError> {
    value
        .map(|timestamp| normalize_timestamp(&timestamp, field))
        .transpose()
}

/// Normalize RFC3339 or SQLite-style timestamp text to canonical RFC3339 UTC text.
pub fn normalize_timestamp(value: &str, field: &str) -> Result<String, TimeParseError> {
    if let Ok(timestamp) = parse_rfc3339_utc(value) {
        return Ok(timestamp.to_rfc3339());
    }

    let timestamp = parse_sqlite_timestamp(value, field)?;
    Ok(Utc.from_utc_datetime(&timestamp).to_rfc3339())
}

/// Parse RFC3339 timestamp text into UTC.
pub fn parse_rfc3339_utc(value: &str) -> Result<DateTime<Utc>, TimeParseError> {
    let timestamp = DateTime::parse_from_rfc3339(value)?;
    Ok(timestamp.with_timezone(&Utc))
}

/// Parse RFC3339 timestamp text into UTC, returning `None` on failure.
///
/// Returns `None` when parsing fails, for callers treating invalid values as absent.
pub fn parse_rfc3339_optional(value: &str) -> Option<DateTime<Utc>> {
    parse_rfc3339_utc(value).ok()
}

/// Format a [`SystemTime`] as canonical RFC3339 UTC text.
pub fn system_time_to_rfc3339(value: SystemTime) -> String {
    DateTime::<Utc>::from(value).to_rfc3339()
}

/// Check whether an optional timestamp falls within optional RFC3339 bounds.
///
/// `after` is inclusive lower bound; `before` is exclusive upper bound. Any
/// unparseable input (the value or either bound) is treated as absent, so a
/// missing or malformed value never fails the bounds check.
pub fn timestamp_in_bounds(value: Option<&str>, after: Option<&str>, before: Option<&str>) -> bool {
    let Some(value) = value.and_then(parse_rfc3339_optional) else {
        return true;
    };
    if let Some(after) = after.and_then(parse_rfc3339_optional)
        && value < after
    {
        return false;
    }
    if let Some(before) = before.and_then(parse_rfc3339_optional)
        && value >= before
    {
        return false;
    }
    true
}

fn parse_persisted_period_timestamp(value: &str) -> Result<DateTime<Utc>, TimeParseError> {
    match DateTime::parse_from_rfc3339(value) {
        Ok(timestamp) => Ok(timestamp.with_timezone(&Utc)),
        Err(source) => Err(TimeParseError::InvalidPersistedPeriodTimestamp { source }),
    }
}

fn parse_sqlite_timestamp(value: &str, field: &str) -> Result<NaiveDateTime, TimeParseError> {
    match NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        Ok(timestamp) => Ok(timestamp),
        Err(source) => Err(TimeParseError::InvalidTimestampField {
            field: field.to_owned(),
            source,
        }),
    }
}

/// Parse an ISO 8601 / RFC 3339 timestamp string into milliseconds since Unix epoch.
pub fn iso_to_ms(iso: &str) -> Result<i64, TimeParseError> {
    parse_rfc3339_utc(iso).map(|dt| dt.timestamp_millis())
}

/// Returns the current UTC timestamp in the same millisecond format as [`ms_to_iso`].
pub fn now_iso() -> String {
    ms_to_iso(Utc::now().timestamp_millis())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_horizon_accepts_only_month_labels_or_forever() {
        assert_eq!(parse_retention_horizon("1month"), Ok(RetentionHorizon::Months(1)));
        assert_eq!(parse_retention_horizon("12months"), Ok(RetentionHorizon::Months(12)));
        assert_eq!(
            parse_retention_horizon("4294967295months"),
            Ok(RetentionHorizon::Months(u32::MAX))
        );
        assert_eq!(parse_retention_horizon("forever"), Ok(RetentionHorizon::Forever));

        for label in [
            "0month",
            "-1month",
            "1day",
            "1m",
            "month",
            "1 month",
            "forevers",
            "4294967296months",
            " 1month",
            "1month ",
            "FOREVER",
        ] {
            assert_eq!(parse_retention_horizon(label), Err(RetentionHorizonError::InvalidLabel));
        }
    }

    #[test]
    fn retention_cutoff_uses_utc_calendar_months() {
        let now = utc("2026-03-31T12:34:56Z");
        assert_eq!(
            retention_cutoff_utc(now, RetentionHorizon::Months(1)),
            Ok(Some(utc("2026-02-28T12:34:56Z")))
        );
        assert_eq!(retention_cutoff_utc(now, RetentionHorizon::Forever), Ok(None));
    }

    #[test]
    fn retention_cutoff_clamps_to_leap_day() {
        let now = utc("2024-03-31T12:34:56Z");
        assert_eq!(
            retention_cutoff_utc(now, RetentionHorizon::Months(1)),
            Ok(Some(utc("2024-02-29T12:34:56Z")))
        );
    }

    #[test]
    fn retention_cutoff_rejects_out_of_range_calendar() {
        let now = utc("2026-03-31T12:34:56Z");
        assert_eq!(
            retention_cutoff_utc(now, RetentionHorizon::Months(u32::MAX)),
            Err(RetentionHorizonError::CalendarOutOfRange)
        );
    }

    #[test]
    fn retention_cutoff_preserves_zero_months_and_calendar_edges() {
        let cases = [
            ("2000-03-31T12:34:56Z", 1, Some("2000-02-29T12:34:56Z")),
            ("1900-03-31T12:34:56Z", 1, Some("1900-02-28T12:34:56Z")),
            ("0001-01-31T12:34:56Z", 1, Some("0000-12-31T12:34:56Z")),
        ];
        for (input, months, expected) in cases {
            let result = retention_cutoff_utc(utc(input), RetentionHorizon::Months(months));
            assert_eq!(
                result,
                expected
                    .map(utc)
                    .map(Some)
                    .ok_or(RetentionHorizonError::CalendarOutOfRange)
            );
        }
        let now = utc("2024-02-29T12:34:56Z");
        assert_eq!(retention_cutoff_utc(now, RetentionHorizon::Months(0)), Ok(Some(now)));
        let min = Utc.from_utc_datetime(&NaiveDate::MIN.and_hms_opt(0, 0, 0).expect("valid midnight"));
        assert_eq!(
            retention_cutoff_utc(min, RetentionHorizon::Months(1)),
            Err(RetentionHorizonError::CalendarOutOfRange)
        );
    }

    #[test]
    fn retention_labels_keep_ascii_and_leading_zero_grammar() {
        assert_eq!(parse_retention_horizon("0002months"), Ok(RetentionHorizon::Months(2)));
        for label in ["０month", "１months", "0000month"] {
            assert_eq!(parse_retention_horizon(label), Err(RetentionHorizonError::InvalidLabel));
        }
    }

    #[test]
    fn ms_to_iso_epoch() {
        let s = ms_to_iso(0);
        assert!(s.starts_with("1970-01-01"), "got: {s}");
    }

    #[test]
    fn ms_to_iso_known_timestamp() {
        // 2026-01-01T00:00:00.000Z = 1767225600000 ms
        let s = ms_to_iso(1_767_225_600_000);
        assert!(s.starts_with("2026-01-01"), "got: {s}");
    }

    #[test]
    fn millisecond_conversion_preserves_negative_and_range_fallbacks() {
        assert_eq!(ms_to_iso(-1), "-1");
        assert_eq!(ms_to_iso(-1000), "1969-12-31T23:59:59.000Z");
        assert_eq!(ms_to_iso(-1001), "-1001");
        assert_eq!(ms_to_iso(i64::MAX), i64::MAX.to_string());
    }

    fn utc(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s)
            .unwrap_or_else(|e| panic!("invalid RFC3339 test timestamp {s:?}: {e}"))
            .with_timezone(&Utc)
    }

    #[test]
    fn week_midweek_returns_monday_to_monday() {
        let (start, end) = week_bounds(utc("2026-06-24T13:37:00Z"));
        assert_eq!(start, "2026-06-22T00:00:00+00:00");
        assert_eq!(end, "2026-06-29T00:00:00+00:00");
    }

    #[test]
    fn week_monday_midnight_is_inclusive_start() {
        let (start, end) = week_bounds(utc("2026-06-22T00:00:00Z"));
        assert_eq!(start, "2026-06-22T00:00:00+00:00");
        assert_eq!(end, "2026-06-29T00:00:00+00:00");
    }

    #[test]
    fn week_sunday_late_is_still_same_week() {
        let (start, end) = week_bounds(utc("2026-06-28T23:59:59Z"));
        assert_eq!(start, "2026-06-22T00:00:00+00:00");
        assert_eq!(end, "2026-06-29T00:00:00+00:00");
    }

    #[test]
    fn week_exclusive_end_is_next_monday() {
        let (start, end) = week_bounds(utc("2026-06-29T00:00:00Z"));
        assert_eq!(start, "2026-06-29T00:00:00+00:00");
        assert_eq!(end, "2026-07-06T00:00:00+00:00");
    }

    #[test]
    fn week_year_boundary_wraps_correctly() {
        let (start, end) = week_bounds(utc("2026-12-31T12:00:00Z"));
        assert_eq!(start, "2026-12-28T00:00:00+00:00");
        assert_eq!(end, "2027-01-04T00:00:00+00:00");
    }

    #[test]
    fn month_mid_month_returns_first_to_first() {
        let (start, end) = month_bounds(utc("2026-06-15T08:30:00Z"));
        assert_eq!(start, "2026-06-01T00:00:00+00:00");
        assert_eq!(end, "2026-07-01T00:00:00+00:00");
    }

    #[test]
    fn month_first_day_midnight_is_inclusive_start() {
        let (start, end) = month_bounds(utc("2026-06-01T00:00:00Z"));
        assert_eq!(start, "2026-06-01T00:00:00+00:00");
        assert_eq!(end, "2026-07-01T00:00:00+00:00");
    }

    #[test]
    fn month_last_day_late_is_still_same_month() {
        let (start, end) = month_bounds(utc("2026-06-30T23:59:59Z"));
        assert_eq!(start, "2026-06-01T00:00:00+00:00");
        assert_eq!(end, "2026-07-01T00:00:00+00:00");
    }

    #[test]
    fn month_exclusive_end_is_first_of_next_month() {
        let (start, end) = month_bounds(utc("2026-07-01T00:00:00Z"));
        assert_eq!(start, "2026-07-01T00:00:00+00:00");
        assert_eq!(end, "2026-08-01T00:00:00+00:00");
    }

    #[test]
    fn month_year_boundary_december_to_january() {
        let (start, end) = month_bounds(utc("2026-12-31T23:59:59Z"));
        assert_eq!(start, "2026-12-01T00:00:00+00:00");
        assert_eq!(end, "2027-01-01T00:00:00+00:00");
    }

    #[test]
    fn quarter_q1_january() {
        let (start, end) = quarter_bounds(utc("2026-01-15T00:00:00Z"));
        assert_eq!(start, "2026-01-01T00:00:00+00:00");
        assert_eq!(end, "2026-04-01T00:00:00+00:00");
    }

    #[test]
    fn quarter_q2_june() {
        let (start, end) = quarter_bounds(utc("2026-06-15T00:00:00Z"));
        assert_eq!(start, "2026-04-01T00:00:00+00:00");
        assert_eq!(end, "2026-07-01T00:00:00+00:00");
    }

    #[test]
    fn quarter_q3_september() {
        let (start, end) = quarter_bounds(utc("2026-09-15T00:00:00Z"));
        assert_eq!(start, "2026-07-01T00:00:00+00:00");
        assert_eq!(end, "2026-10-01T00:00:00+00:00");
    }

    #[test]
    fn quarter_q4_december() {
        let (start, end) = quarter_bounds(utc("2026-12-15T00:00:00Z"));
        assert_eq!(start, "2026-10-01T00:00:00+00:00");
        assert_eq!(end, "2027-01-01T00:00:00+00:00");
    }

    #[test]
    fn quarter_exclusive_end_is_first_of_next_quarter() {
        let (start, end) = quarter_bounds(utc("2026-04-01T00:00:00Z"));
        assert_eq!(start, "2026-04-01T00:00:00+00:00");
        assert_eq!(end, "2026-07-01T00:00:00+00:00");
    }

    #[test]
    fn quarter_year_boundary_q4_to_next_year_q1() {
        let (start, end) = quarter_bounds(utc("2026-12-31T23:59:59Z"));
        assert_eq!(start, "2026-10-01T00:00:00+00:00");
        assert_eq!(end, "2027-01-01T00:00:00+00:00");
    }

    #[test]
    fn iso_to_ms_valid_rfc3339() {
        // Unix epoch
        assert_eq!(iso_to_ms("1970-01-01T00:00:00Z").unwrap(), 0);
    }

    #[test]
    fn iso_to_ms_known_timestamp() {
        // 2024-01-01T00:00:00Z = 1704067200000 ms
        assert_eq!(iso_to_ms("2024-01-01T00:00:00Z").unwrap(), 1_704_067_200_000);
    }

    #[test]
    fn iso_to_ms_invalid_returns_error() {
        assert!(iso_to_ms("not-a-timestamp").is_err());
    }

    #[test]
    fn now_iso_matches_ms_to_iso_format() {
        let now = now_iso();
        // Canonical format: YYYY-MM-DDTHH:MM:SS.mmmZ
        assert!(now.ends_with('Z'), "now_iso should end with Z, got: {now}");
        assert!(
            now.len() == 24,
            "now_iso should be 24 chars (YYYY-MM-DDTHH:MM:SS.mmmZ), got: {now}"
        );
    }

    #[test]
    fn parse_rfc3339_utc_valid_returns_utc_datetime() {
        let dt = parse_rfc3339_utc("2026-07-07T13:37:00Z").unwrap();
        assert_eq!(dt, utc("2026-07-07T13:37:00Z"));
    }

    #[test]
    fn parse_rfc3339_utc_with_offset_normalizes_to_utc() {
        let dt = parse_rfc3339_utc("2026-07-07T15:37:00+02:00").unwrap();
        assert_eq!(dt, utc("2026-07-07T13:37:00Z"));
    }

    #[test]
    fn rfc3339_parser_preserves_fractional_offset_and_leap_second_inputs() {
        assert_eq!(
            parse_rfc3339_utc("2026-07-07T15:37:00.125+02:00").unwrap(),
            utc("2026-07-07T13:37:00.125Z")
        );
        assert!(parse_rfc3339_utc("2016-12-31T23:59:60Z").is_ok());
    }

    #[test]
    fn parse_rfc3339_utc_invalid_returns_typed_error() {
        let err = parse_rfc3339_utc("not-a-timestamp").unwrap_err();
        assert!(matches!(err, TimeParseError::InvalidRfc3339 { .. }));
    }

    #[test]
    fn parse_rfc3339_utc_sqlite_style_returns_typed_error() {
        // SQLite-style timestamps are not RFC3339; parse_rfc3339_utc rejects them.
        let err = parse_rfc3339_utc("2026-07-07 13:37:00").unwrap_err();
        assert!(matches!(err, TimeParseError::InvalidRfc3339 { .. }));
    }

    #[test]
    fn parse_rfc3339_utc_empty_returns_typed_error() {
        let err = parse_rfc3339_utc("").unwrap_err();
        assert!(matches!(err, TimeParseError::InvalidRfc3339 { .. }));
    }

    #[test]
    fn parse_rfc3339_optional_valid_returns_some_utc() {
        let dt = parse_rfc3339_optional("2026-07-07T13:37:00Z").expect("valid RFC3339 should parse");
        assert_eq!(dt, utc("2026-07-07T13:37:00Z"));
    }

    #[test]
    fn parse_rfc3339_optional_invalid_returns_none() {
        assert!(parse_rfc3339_optional("not-a-timestamp").is_none());
        assert!(parse_rfc3339_optional("").is_none());
    }

    #[test]
    fn system_time_to_rfc3339_epoch_is_canonical() {
        let s = system_time_to_rfc3339(std::time::UNIX_EPOCH);
        assert_eq!(s, "1970-01-01T00:00:00+00:00");
    }

    #[test]
    fn system_time_before_epoch_and_fractional_seconds_are_canonical() {
        assert_eq!(
            system_time_to_rfc3339(std::time::UNIX_EPOCH - std::time::Duration::from_millis(500)),
            "1969-12-31T23:59:59.500+00:00"
        );
        assert_eq!(
            system_time_to_rfc3339(std::time::UNIX_EPOCH + std::time::Duration::from_millis(125)),
            "1970-01-01T00:00:00.125+00:00"
        );
    }

    #[test]
    fn timestamp_in_bounds_no_bounds_passes() {
        assert!(timestamp_in_bounds(Some("2026-07-07T13:37:00Z"), None, None));
    }

    #[test]
    fn timestamp_in_bounds_unparseable_value_passes() {
        assert!(timestamp_in_bounds(Some("garbage"), None, None));
        assert!(timestamp_in_bounds(None, None, None));
    }

    #[test]
    fn timestamp_in_bounds_after_inclusive_lower() {
        let value = "2026-07-07T13:37:00Z";
        assert!(timestamp_in_bounds(Some(value), Some(value), None));
        assert!(!timestamp_in_bounds(Some("2026-07-07T13:36:59Z"), Some(value), None));
    }

    #[test]
    fn timestamp_in_bounds_before_exclusive_upper() {
        let before = "2026-07-07T13:37:00Z";
        assert!(!timestamp_in_bounds(Some(before), None, Some(before)));
        assert!(timestamp_in_bounds(Some("2026-07-07T13:36:59Z"), None, Some(before)));
    }

    #[test]
    fn timestamp_in_bounds_unparseable_bounds_ignored() {
        assert!(timestamp_in_bounds(
            Some("2026-07-07T13:37:00Z"),
            Some("garbage"),
            Some("garbage"),
        ));
    }

    #[test]
    fn timestamp_bounds_fail_open_for_missing_or_malformed_values_and_conflicts() {
        assert!(timestamp_in_bounds(None, Some("bad"), Some("bad")));
        assert!(timestamp_in_bounds(
            Some("bad"),
            Some("2026-07-07T00:00:00Z"),
            Some("2026-07-08T00:00:00Z")
        ));
        assert!(!timestamp_in_bounds(
            Some("2026-07-07T00:00:00Z"),
            Some("2026-07-08T00:00:00Z"),
            Some("2026-07-07T00:00:00Z")
        ));
        assert!(!timestamp_in_bounds(
            Some("2026-07-07T00:00:00Z"),
            None,
            Some("2026-07-07T00:00:00Z")
        ));
    }

    #[test]
    fn normalize_timestamp_rfc3339_input_returns_canonical_rfc3339() {
        // Non-UTC offset should be normalized to +00:00.
        let out = normalize_timestamp("2026-07-07T15:37:00+02:00", "test").unwrap();
        assert_eq!(out, "2026-07-07T13:37:00+00:00");
    }

    #[test]
    fn normalize_timestamp_utc_rfc3339_passes_through() {
        let out = normalize_timestamp("2026-07-07T13:37:00Z", "test").unwrap();
        assert_eq!(out, "2026-07-07T13:37:00+00:00");
    }

    #[test]
    fn normalize_timestamp_sqlite_style_falls_back_to_utc() {
        let out = normalize_timestamp("2026-07-07 13:37:00", "test").unwrap();
        assert_eq!(out, "2026-07-07T13:37:00+00:00");
    }

    #[test]
    fn normalize_timestamp_invalid_returns_typed_error_with_field() {
        let err = normalize_timestamp("not-a-timestamp", "myField").unwrap_err();
        assert!(matches!(err, TimeParseError::InvalidTimestampField { field, .. } if field == "myField"));
    }

    #[test]
    fn normalize_timestamp_invalid_sqlite_format_returns_typed_error() {
        // Wrong SQLite format (missing time component) should fail both RFC3339
        // and SQLite fallback, surfacing the contextual SQLite parse error.
        let err = normalize_timestamp("2026-07-07", "createdAt").unwrap_err();
        assert!(matches!(err, TimeParseError::InvalidTimestampField { field, .. } if field == "createdAt"));
    }

    #[test]
    fn normalize_period_timestamp_valid_returns_canonical_rfc3339() {
        let out = normalize_period_timestamp("2026-07-07T13:37:00Z").unwrap();
        assert_eq!(out, "2026-07-07T13:37:00+00:00");
    }

    #[test]
    fn normalize_period_timestamp_with_offset_normalizes_to_utc() {
        let out = normalize_period_timestamp("2026-07-07T15:37:00+02:00").unwrap();
        assert_eq!(out, "2026-07-07T13:37:00+00:00");
    }

    #[test]
    fn normalize_period_timestamp_invalid_returns_typed_error() {
        let err = normalize_period_timestamp("not-a-timestamp").unwrap_err();
        assert!(matches!(err, TimeParseError::InvalidPersistedPeriodTimestamp { .. }));
    }

    #[test]
    fn normalize_period_timestamp_sqlite_style_returns_typed_error() {
        // Unlike normalize_timestamp, normalize_period_timestamp only accepts RFC3339.
        let err = normalize_period_timestamp("2026-07-07 13:37:00").unwrap_err();
        assert!(matches!(err, TimeParseError::InvalidPersistedPeriodTimestamp { .. }));
    }

    #[test]
    fn normalize_optional_timestamp_none_returns_none() {
        assert_eq!(normalize_optional_timestamp(None, "test").unwrap(), None);
    }

    #[test]
    fn normalize_optional_timestamp_some_valid_returns_canonical() {
        let out = normalize_optional_timestamp(Some("2026-07-07T13:37:00Z".to_string()), "test").unwrap();
        assert_eq!(out, Some("2026-07-07T13:37:00+00:00".to_string()));
    }

    #[test]
    fn normalize_optional_timestamp_some_sqlite_falls_back_to_utc() {
        let out = normalize_optional_timestamp(Some("2026-07-07 13:37:00".to_string()), "test").unwrap();
        assert_eq!(out, Some("2026-07-07T13:37:00+00:00".to_string()));
    }

    #[test]
    fn normalize_optional_timestamp_some_invalid_propagates_typed_error_with_field() {
        let err = normalize_optional_timestamp(Some("not-a-timestamp".to_string()), "updatedAt").unwrap_err();
        assert!(matches!(err, TimeParseError::InvalidTimestampField { field, .. } if field == "updatedAt"));
    }
}
