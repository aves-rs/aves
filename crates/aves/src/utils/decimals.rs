use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

use rust_decimal::Decimal;
use rust_decimal::Error as DecimalError;
use rust_decimal::prelude::ToPrimitive;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use specta::datatype::reference::Reference;
use specta::{DataType, Generics, Type, TypeCollection};

/// Scale a `Decimal` to an `i64`, truncating fractional remainders.
///
/// Returns `None` when the scaled value is outside the `i64` range.
pub fn decimal_to_scaled_i64(value: Decimal, scale: i64) -> Option<i64> {
    let scaled = value * Decimal::from(scale);
    scaled.to_i64()
}

/// Scale a `Decimal` to a `u64`, truncating fractional remainders.
///
/// Returns `None` when the scaled value is negative or outside the `u64` range.
pub fn decimal_to_scaled_u64(value: Decimal, scale: i64) -> Option<u64> {
    let scaled = value * Decimal::from(scale);
    scaled.to_u64()
}

/// Scale a non-negative `f64` to an unsigned integer at the given precision scale.
///
/// Non-finite or non-positive values return `0`.
pub fn f64_to_scaled_u64(value: f64, scale: i64) -> u64 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    let scaled = value * scale as f64;
    if !scaled.is_finite() || scaled <= 0.0 {
        return 0;
    }
    scaled as u64
}

/// Scale the absolute value of a `Decimal` to an unsigned integer at the given precision scale.
///
/// Returns `0` when the scaled value overflows `u64`.
pub fn decimal_abs_to_scaled_u64(value: Decimal, scale: i64) -> u64 {
    let scaled = value.abs() * Decimal::from(scale);
    scaled.to_u64().unwrap_or(0)
}

/// Convert an `f64` to a `Decimal` only when the value is finite.
pub fn safe_decimal_from_f64(val: f64) -> Option<Decimal> {
    if val.is_finite() {
        Decimal::from_f64_retain(val)
    } else {
        None
    }
}

/// A decimal serialized and projected to schema consumers as a string.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct SpectaDecimal(#[schemars(with = "String")] pub Decimal);

impl Type for SpectaDecimal {
    fn inline(type_map: &mut TypeCollection, generics: Generics<'_>) -> DataType {
        String::inline(type_map, generics)
    }

    fn reference(type_map: &mut TypeCollection, generics: &[DataType]) -> Reference {
        String::reference(type_map, generics)
    }
}

impl Deref for SpectaDecimal {
    type Target = Decimal;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Decimal> for SpectaDecimal {
    fn from(decimal: Decimal) -> Self {
        Self(decimal)
    }
}

impl From<SpectaDecimal> for Decimal {
    fn from(wrapper: SpectaDecimal) -> Self {
        wrapper.0
    }
}

impl fmt::Display for SpectaDecimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for SpectaDecimal {
    type Err = DecimalError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Decimal::from_str(s).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    // --- decimal_to_scaled_i64 ------------------------------------------------

    #[test]
    fn decimal_to_scaled_i64_positive_value() {
        let value = Decimal::new(123_456_789, 4); // 12345.6789
        assert_eq!(decimal_to_scaled_i64(value, 100), Some(1_234_567));
    }

    #[test]
    fn decimal_to_scaled_i64_negative_value() {
        let value = Decimal::new(-123_456_789, 4); // -12345.6789
        assert_eq!(decimal_to_scaled_i64(value, 100), Some(-1_234_567));
    }

    #[test]
    fn decimal_to_scaled_i64_zero() {
        assert_eq!(decimal_to_scaled_i64(Decimal::ZERO, 1_000), Some(0));
    }

    #[test]
    fn decimal_to_scaled_i64_truncates_fractional_remainder() {
        // 1.2345 scaled by 100 truncates to 123 (rust_decimal ToPrimitive truncates).
        let value = Decimal::new(12345, 4); // 1.2345
        assert_eq!(decimal_to_scaled_i64(value, 100), Some(123));
    }

    #[test]
    fn decimal_to_scaled_i64_overflow_returns_none() {
        // i64::MAX is ~9.22e18; scale by 1e6 to push past it.
        let huge = Decimal::new(i64::MAX, 0);
        assert_eq!(decimal_to_scaled_i64(huge, 1_000_000), None);
    }

    #[test]
    fn decimal_to_scaled_i64_negative_overflow_returns_none() {
        let huge = Decimal::new(i64::MIN, 0);
        assert_eq!(decimal_to_scaled_i64(huge, 1_000_000), None);
    }

    // --- decimal_to_scaled_u64 ------------------------------------------------

    #[test]
    fn decimal_to_scaled_u64_positive_value() {
        let value = Decimal::new(123_456_789, 4); // 12345.6789
        assert_eq!(decimal_to_scaled_u64(value, 100), Some(1_234_567));
    }

    #[test]
    fn decimal_to_scaled_u64_zero() {
        assert_eq!(decimal_to_scaled_u64(Decimal::ZERO, 1_000), Some(0));
    }

    #[test]
    fn decimal_to_scaled_u64_negative_returns_none() {
        // Negative decimals cannot be represented as u64.
        let value = Decimal::new(-1, 0);
        assert_eq!(decimal_to_scaled_u64(value, 1), None);
    }

    #[test]
    fn decimal_to_scaled_u64_overflow_returns_none() {
        // u64::MAX is ~1.84e19; scale by 1e6 to push past it.
        let huge = Decimal::new(i64::MAX, 0);
        assert_eq!(decimal_to_scaled_u64(huge, 1_000_000), None);
    }

    // --- f64_to_scaled_u64 ----------------------------------------------------

    #[test]
    fn f64_to_scaled_u64_positive_finite() {
        assert_eq!(f64_to_scaled_u64(1.5, 100), 150);
    }

    #[test]
    fn f64_to_scaled_u64_zero_returns_zero() {
        assert_eq!(f64_to_scaled_u64(0.0, 100), 0);
    }

    #[test]
    fn f64_to_scaled_u64_negative_returns_zero() {
        assert_eq!(f64_to_scaled_u64(-1.5, 100), 0);
    }

    #[test]
    fn f64_to_scaled_u64_nan_returns_zero() {
        assert_eq!(f64_to_scaled_u64(f64::NAN, 100), 0);
    }

    #[test]
    fn f64_to_scaled_u64_infinity_returns_zero() {
        assert_eq!(f64_to_scaled_u64(f64::INFINITY, 100), 0);
    }

    #[test]
    fn f64_to_scaled_u64_neg_infinity_returns_zero() {
        assert_eq!(f64_to_scaled_u64(f64::NEG_INFINITY, 100), 0);
    }

    #[test]
    fn f64_to_scaled_u64_scaled_overflow_returns_zero() {
        // value * scale overflows f64 to infinity, which is non-finite -> 0.
        assert_eq!(f64_to_scaled_u64(f64::MAX, 1_000_000), 0);
    }

    #[test]
    fn f64_to_scaled_u64_scaled_to_negative_via_overflow_returns_zero() {
        // Large negative scaled value should still be caught by the <= 0 guard.
        assert_eq!(f64_to_scaled_u64(-f64::MAX, 1_000_000), 0);
    }

    // --- decimal_abs_to_scaled_u64 --------------------------------------------

    #[test]
    fn decimal_abs_to_scaled_u64_positive() {
        let value = Decimal::new(123_456_789, 4); // 12345.6789
        assert_eq!(decimal_abs_to_scaled_u64(value, 100), 1_234_567);
    }

    #[test]
    fn decimal_abs_to_scaled_u64_negative_takes_absolute_value() {
        let value = Decimal::new(-123_456_789, 4); // -12345.6789
        assert_eq!(decimal_abs_to_scaled_u64(value, 100), 1_234_567);
    }

    #[test]
    fn decimal_abs_to_scaled_u64_zero() {
        assert_eq!(decimal_abs_to_scaled_u64(Decimal::ZERO, 1_000), 0);
    }

    #[test]
    fn decimal_abs_to_scaled_u64_overflow_returns_zero() {
        let huge = Decimal::new(i64::MAX, 0);
        assert_eq!(decimal_abs_to_scaled_u64(huge, 1_000_000), 0);
    }

    #[test]
    fn decimal_abs_to_scaled_u64_negative_overflow_returns_zero() {
        let huge = Decimal::new(i64::MIN, 0);
        assert_eq!(decimal_abs_to_scaled_u64(huge, 1_000_000), 0);
    }

    // --- safe_decimal_from_f64 ------------------------------------------------

    #[test]
    fn safe_decimal_from_f64_finite_value() {
        let result = safe_decimal_from_f64(1.5);
        assert_eq!(result, Some(Decimal::new(15, 1)));
    }

    #[test]
    fn safe_decimal_from_f64_negative_finite_value() {
        let result = safe_decimal_from_f64(-2.25);
        assert_eq!(result, Some(Decimal::new(-225, 2)));
    }

    #[test]
    fn safe_decimal_from_f64_zero() {
        assert_eq!(safe_decimal_from_f64(0.0), Some(Decimal::ZERO));
    }

    #[test]
    fn safe_decimal_from_f64_nan_returns_none() {
        assert_eq!(safe_decimal_from_f64(f64::NAN), None);
    }

    #[test]
    fn safe_decimal_from_f64_infinity_returns_none() {
        assert_eq!(safe_decimal_from_f64(f64::INFINITY), None);
    }

    #[test]
    fn safe_decimal_from_f64_neg_infinity_returns_none() {
        assert_eq!(safe_decimal_from_f64(f64::NEG_INFINITY), None);
    }

    // --- SpectaDecimal --------------------------------------------------------

    #[test]
    fn specta_decimal_from_str_valid() {
        let parsed: SpectaDecimal = "3.14".parse().unwrap();
        assert_eq!(parsed.0, Decimal::new(314, 2));
    }

    #[test]
    fn specta_decimal_from_str_invalid() {
        assert!("not-a-number".parse::<SpectaDecimal>().is_err());
    }

    #[test]
    fn specta_decimal_from_and_into_decimal_roundtrip() {
        let d = Decimal::new(42, 0);
        let wrapped: SpectaDecimal = d.into();
        let back: Decimal = wrapped.into();
        assert_eq!(back, d);
    }

    #[test]
    fn specta_decimal_display_matches_inner() {
        let d = Decimal::new(123, 2); // 1.23
        let wrapped = SpectaDecimal(d);
        assert_eq!(format!("{wrapped}"), format!("{d}"));
    }

    #[test]
    fn specta_decimal_deref_exposes_inner() {
        let d = Decimal::new(7, 0);
        let wrapped = SpectaDecimal(d);
        assert_eq!(*wrapped, d);
    }

    #[test]
    fn decimal_scaled_conversions_accept_zero_and_negative_scales() {
        let value = Decimal::new(125, 2);
        assert_eq!(decimal_to_scaled_i64(value, 0), Some(0));
        assert_eq!(decimal_to_scaled_i64(value, -10), Some(-12));
        assert_eq!(decimal_to_scaled_u64(value, 0), Some(0));
        assert_eq!(decimal_to_scaled_u64(value, -10), None);
    }

    #[test]
    fn decimal_to_scaled_u64_truncates_fractional_remainder() {
        assert_eq!(decimal_to_scaled_u64(Decimal::new(12345, 4), 100), Some(123));
    }

    #[test]
    fn decimal_scaled_conversions_accept_exact_integer_boundaries() {
        assert_eq!(decimal_to_scaled_i64(Decimal::from(i64::MAX), 1), Some(i64::MAX));
        assert_eq!(decimal_to_scaled_i64(Decimal::from(i64::MIN), 1), Some(i64::MIN));
        assert_eq!(decimal_to_scaled_u64(Decimal::from(u64::MAX), 1), Some(u64::MAX));
    }

    #[test]
    #[should_panic]
    fn decimal_scaled_multiplication_overflow_panics() {
        let _ = decimal_to_scaled_i64(Decimal::MAX, i64::MAX);
    }

    #[test]
    fn f64_scaled_conversion_saturates_finite_values_and_truncates_subunits() {
        assert_eq!(f64_to_scaled_u64(f64::MAX, 1), u64::MAX);
        assert_eq!(f64_to_scaled_u64(0.9, 1), 0);
        assert_eq!(f64_to_scaled_u64(-0.0, 10), 0);
    }

    #[test]
    fn safe_decimal_from_f64_rejects_finite_out_of_range_and_retains_precision() {
        assert_eq!(safe_decimal_from_f64(f64::MAX), None);
        let retained = safe_decimal_from_f64(0.1).unwrap();
        assert_eq!(retained, Decimal::from_f64_retain(0.1).unwrap());
        assert_ne!(retained, Decimal::new(1, 1));
    }

    #[test]
    fn specta_decimal_serde_serializes_as_string_and_roundtrips_values() {
        let wrapped = SpectaDecimal(Decimal::new(314, 2));
        let json = serde_json::to_string(&wrapped).unwrap();
        assert_eq!(json, "\"3.14\"");
        assert_eq!(serde_json::from_str::<SpectaDecimal>(&json).unwrap(), wrapped);
        assert_eq!(serde_json::from_str::<SpectaDecimal>("3.14").unwrap(), wrapped);
        assert!(serde_json::from_str::<SpectaDecimal>("\"not-a-decimal\"").is_err());
        assert!(serde_json::from_str::<SpectaDecimal>("null").is_err());
        assert!(serde_json::from_str::<SpectaDecimal>("{}").is_err());
    }

    #[test]
    fn specta_decimal_declares_string_schema_and_type() {
        let schema = schemars::schema_for!(SpectaDecimal);
        assert_eq!(schema.as_value()["type"], "string");

        let projected = SpectaDecimal::inline(&mut TypeCollection::default(), Generics::NONE);
        let string_type = String::inline(&mut TypeCollection::default(), Generics::NONE);
        assert_eq!(projected, string_type);
    }
}
