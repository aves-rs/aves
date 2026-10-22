//! Checked conversions between SQLite's signed `INTEGER` and unsigned Rust integers.
//! Conversion failures retain the source field and value in [`IntegerCastError`].

use thiserror::Error;

/// Error returned by checked integer conversions.
///
/// This internal utility error carries only serializable context so callers can
/// map it onto their domain-specific error enums before any RPC boundary.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
#[error("invalid integer for {field}: {message}")]
pub struct IntegerCastError {
    /// Field/column label identifying the offending value.
    pub field: &'static str,
    /// Human-readable reason (e.g. "negative value -1 cannot fit in u64").
    pub message: String,
}

/// Encode a `u64` into an `i64` for DB storage. Fails if the value exceeds
/// `i64::MAX`.
pub fn encode_u64_to_i64(value: u64, field: &'static str) -> Result<i64, IntegerCastError> {
    i64::try_from(value).map_err(|_| IntegerCastError {
        field,
        message: format!("u64 value {value} exceeds i64::MAX and cannot be stored as INTEGER"),
    })
}

/// Decode an `i64` from the DB into a `u64`. Fails on negative values.
pub fn decode_i64_to_u64(value: i64, field: &'static str) -> Result<u64, IntegerCastError> {
    u64::try_from(value).map_err(|_| IntegerCastError {
        field,
        message: format!("negative or out-of-range i64 {value} cannot decode to u64"),
    })
}

/// Decode an `i64` from the DB into a `u32`. Fails on negative values or
/// values exceeding `u32::MAX`.
pub fn decode_i64_to_u32(value: i64, field: &'static str) -> Result<u32, IntegerCastError> {
    u32::try_from(value).map_err(|_| IntegerCastError {
        field,
        message: format!("i64 {value} out of range for u32"),
    })
}

/// Decode an `i64` from the DB into a `u16`. Fails on negative values or
/// values exceeding `u16::MAX`.
pub fn decode_i64_to_u16(value: i64, field: &'static str) -> Result<u16, IntegerCastError> {
    u16::try_from(value).map_err(|_| IntegerCastError {
        field,
        message: format!("i64 {value} out of range for u16"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_u64_to_i64_ok() {
        assert_eq!(encode_u64_to_i64(0, "f").unwrap(), 0);
        assert_eq!(encode_u64_to_i64(1_000_000, "f").unwrap(), 1_000_000);
        assert_eq!(encode_u64_to_i64(i64::MAX as u64, "f").unwrap(), i64::MAX);
    }

    #[test]
    fn encode_u64_to_i64_overflow_rejected() {
        let err = encode_u64_to_i64((i64::MAX as u64) + 1, "fuel_limit").unwrap_err();
        assert_eq!(err.field, "fuel_limit");
        assert!(err.message.contains("exceeds i64::MAX"));
    }

    #[test]
    fn decode_i64_to_u64_ok() {
        assert_eq!(decode_i64_to_u64(0, "f").unwrap(), 0);
        assert_eq!(decode_i64_to_u64(42, "f").unwrap(), 42);
    }

    #[test]
    fn decode_i64_to_u64_negative_rejected() {
        let err = decode_i64_to_u64(-1, "timeout_secs").unwrap_err();
        assert_eq!(err.field, "timeout_secs");
        assert!(err.message.contains("negative"));
    }

    #[test]
    fn decode_i64_to_u32_ok() {
        assert_eq!(decode_i64_to_u32(0, "f").unwrap(), 0);
        assert_eq!(decode_i64_to_u32(100, "f").unwrap(), 100);
    }

    #[test]
    fn decode_i64_to_u32_overflow_rejected() {
        let err = decode_i64_to_u32((u32::MAX as i64) + 1, "id").unwrap_err();
        assert_eq!(err.field, "id");
        assert!(err.message.contains("u32"));
    }

    #[test]
    fn decode_i64_to_u32_negative_rejected() {
        assert!(decode_i64_to_u32(-5, "id").is_err());
    }

    #[test]
    fn decode_i64_to_u16_ok() {
        assert_eq!(decode_i64_to_u16(0, "f").unwrap(), 0);
        assert_eq!(decode_i64_to_u16(8080, "f").unwrap(), 8080);
    }

    #[test]
    fn decode_i64_to_u16_overflow_rejected() {
        assert!(decode_i64_to_u16((u16::MAX as i64) + 1, "port").is_err());
    }

    #[test]
    fn decode_i64_to_u16_negative_rejected() {
        assert!(decode_i64_to_u16(-1, "port").is_err());
    }

    #[test]
    fn decode_unsigned_target_maxima_succeed() {
        let cases = [
            (
                "u16",
                u16::MAX as i64,
                decode_i64_to_u16(u16::MAX as i64, "u16").unwrap() as u64,
            ),
            (
                "u32",
                u32::MAX as i64,
                decode_i64_to_u32(u32::MAX as i64, "u32").unwrap() as u64,
            ),
            ("u64", i64::MAX, decode_i64_to_u64(i64::MAX, "u64").unwrap()),
        ];

        assert_eq!(
            cases,
            [
                ("u16", u16::MAX as i64, u16::MAX as u64),
                ("u32", u32::MAX as i64, u32::MAX as u64),
                ("u64", i64::MAX, i64::MAX as u64),
            ]
        );
    }

    #[test]
    fn conversion_failures_preserve_full_contextual_display() {
        let cases = [
            (
                "u64 minimum",
                decode_i64_to_u64(i64::MIN, "u64").unwrap_err(),
                "invalid integer for u64: negative or out-of-range i64 -9223372036854775808 cannot decode to u64",
            ),
            (
                "u64 maximum encode",
                encode_u64_to_i64(u64::MAX, "id").unwrap_err(),
                "invalid integer for id: u64 value 18446744073709551615 exceeds i64::MAX and cannot be stored as INTEGER",
            ),
            (
                "u32 overflow",
                decode_i64_to_u32(u32::MAX as i64 + 1, "count").unwrap_err(),
                "invalid integer for count: i64 4294967296 out of range for u32",
            ),
            (
                "u32 minimum",
                decode_i64_to_u32(i64::MIN, "count").unwrap_err(),
                "invalid integer for count: i64 -9223372036854775808 out of range for u32",
            ),
            (
                "u16 overflow",
                decode_i64_to_u16(u16::MAX as i64 + 1, "port").unwrap_err(),
                "invalid integer for port: i64 65536 out of range for u16",
            ),
            (
                "u16 minimum",
                decode_i64_to_u16(i64::MIN, "port").unwrap_err(),
                "invalid integer for port: i64 -9223372036854775808 out of range for u16",
            ),
        ];

        for (case, error, expected) in cases {
            assert_eq!(error.to_string(), expected, "{case}");
        }
    }
}
