/// Convert a boolean into a SQLite-compatible integer representation.
pub fn bool_to_integer(value: bool) -> i64 {
    i64::from(value)
}

/// Convert a SQLite-compatible integer representation into a boolean.
pub fn integer_to_bool(value: i64) -> bool {
    value != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- bool_to_integer ----------------------------------------------------

    #[test]
    fn bool_to_integer_true_is_one() {
        assert_eq!(bool_to_integer(true), 1);
    }

    #[test]
    fn bool_to_integer_false_is_zero() {
        assert_eq!(bool_to_integer(false), 0);
    }

    // --- integer_to_bool ----------------------------------------------------

    #[test]
    fn integer_to_bool_zero_is_false() {
        assert!(!integer_to_bool(0));
    }

    #[test]
    fn integer_to_bool_one_is_true() {
        assert!(integer_to_bool(1));
    }

    #[test]
    fn integer_to_bool_negative_is_true() {
        // Any nonzero value, including negatives, is truthy.
        assert!(integer_to_bool(-1));
    }

    #[test]
    fn integer_to_bool_large_positive_is_true() {
        assert!(integer_to_bool(i64::MAX));
    }

    #[test]
    fn integer_to_bool_large_negative_is_true() {
        assert!(integer_to_bool(i64::MIN));
    }

    // --- roundtrip ----------------------------------------------------------

    #[test]
    fn bool_integer_roundtrip_true() {
        assert_eq!(integer_to_bool(bool_to_integer(true)), true);
    }

    #[test]
    fn bool_integer_roundtrip_false() {
        assert_eq!(integer_to_bool(bool_to_integer(false)), false);
    }
}
