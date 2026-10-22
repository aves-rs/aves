//! Element-wise arithmetic helpers.

/// Converts a boolean trend series to `±1` sign values.
pub fn bools_to_signs(flags: &[bool]) -> Vec<f64> {
    flags
        .iter()
        .map(|flag| if *flag { 1.0 } else { -1.0 })
        .collect()
}

/// Element-wise addition. Panics if lengths differ.
pub fn add(a: &[f64], b: &[f64]) -> Vec<f64> {
    assert_eq!(a.len(), b.len(), "length mismatch");
    a.iter().zip(b).map(|(x, y)| x + y).collect()
}

/// Element-wise subtraction. Panics if lengths differ.
pub fn subtract(a: &[f64], b: &[f64]) -> Vec<f64> {
    assert_eq!(a.len(), b.len(), "length mismatch");
    a.iter().zip(b).map(|(x, y)| x - y).collect()
}

/// Trims the front of `a` and `b` so both end-align to the same length
/// (the shorter of the two). Returns the aligned pair as slices.
pub fn align_right<'a>(a: &'a [f64], b: &'a [f64]) -> (&'a [f64], &'a [f64]) {
    let n = a.len().min(b.len());
    (&a[a.len() - n..], &b[b.len() - n..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bools_to_signs_true_is_positive_one() {
        assert_eq!(bools_to_signs(&[true]), vec![1.0]);
    }

    #[test]
    fn bools_to_signs_false_is_negative_one() {
        assert_eq!(bools_to_signs(&[false]), vec![-1.0]);
    }

    #[test]
    fn bools_to_signs_mixed_sequence_preserves_order() {
        assert_eq!(bools_to_signs(&[true, false, true, false]), vec![1.0, -1.0, 1.0, -1.0]);
    }

    #[test]
    fn bools_to_signs_empty_input_is_empty() {
        assert!(bools_to_signs(&[]).is_empty());
    }

    #[test]
    fn bools_to_signs_all_true() {
        assert_eq!(bools_to_signs(&[true, true, true]), vec![1.0, 1.0, 1.0]);
    }

    #[test]
    fn bools_to_signs_all_false() {
        assert_eq!(bools_to_signs(&[false, false, false]), vec![-1.0, -1.0, -1.0]);
    }

    // --- add / subtract -----------------------------------------------------

    #[test]
    fn add_basic() {
        assert_eq!(add(&[1.0, 2.0, 3.0], &[10.0, 20.0, 30.0]), vec![11.0, 22.0, 33.0]);
    }

    #[test]
    fn add_empty() {
        assert!(add(&[], &[]).is_empty());
    }

    #[test]
    #[should_panic(expected = "length mismatch")]
    fn add_length_mismatch_panics() {
        let _ = add(&[1.0, 2.0], &[1.0]);
    }

    #[test]
    #[should_panic(expected = "length mismatch")]
    fn add_length_mismatch_when_second_input_is_longer_panics() {
        let _ = add(&[1.0], &[1.0, 2.0]);
    }

    #[test]
    fn add_overflow_to_infinity() {
        // f64 addition overflow saturates to +infinity.
        assert_eq!(add(&[f64::MAX], &[f64::MAX]), vec![f64::INFINITY]);
    }

    #[test]
    fn add_infinity_and_nan_propagates() {
        assert!(add(&[f64::INFINITY], &[f64::NEG_INFINITY])[0].is_nan());
        assert!(add(&[f64::NAN], &[1.0])[0].is_nan());
    }

    #[test]
    fn add_preserves_negative_zero_from_ieee_addition() {
        let zeros = [-0.0];
        assert_eq!(add(&zeros, &zeros)[0].to_bits(), (-0.0_f64).to_bits());
    }

    #[test]
    fn subtract_basic() {
        assert_eq!(subtract(&[10.0, 20.0], &[1.0, 2.0]), vec![9.0, 18.0]);
    }

    #[test]
    fn subtract_empty() {
        assert!(subtract(&[], &[]).is_empty());
    }

    #[test]
    #[should_panic(expected = "length mismatch")]
    fn subtract_length_mismatch_panics() {
        let _ = subtract(&[1.0], &[1.0, 2.0]);
    }

    #[test]
    #[should_panic(expected = "length mismatch")]
    fn subtract_length_mismatch_when_first_input_is_longer_panics() {
        let _ = subtract(&[1.0, 2.0], &[1.0]);
    }

    #[test]
    fn subtract_overflow_to_neg_infinity() {
        // f64 subtraction overflow saturates to -infinity.
        assert_eq!(subtract(&[-f64::MAX], &[f64::MAX]), vec![f64::NEG_INFINITY]);
    }

    #[test]
    fn subtract_infinity_yields_nan() {
        assert!(subtract(&[f64::INFINITY], &[f64::INFINITY])[0].is_nan());
    }

    #[test]
    fn subtract_propagates_nan_and_preserves_signed_zero() {
        let values = [f64::NAN, -0.0];
        let zeros = [1.0, 0.0];
        let result = subtract(&values, &zeros);

        assert!(result[0].is_nan());
        assert_eq!(result[1].to_bits(), (-0.0_f64).to_bits());
    }

    // --- align_right --------------------------------------------------------

    #[test]
    fn align_right_equal_lengths() {
        let (a, b) = align_right(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]);
        assert_eq!(a, &[1.0, 2.0, 3.0]);
        assert_eq!(b, &[4.0, 5.0, 6.0]);
    }

    #[test]
    fn align_right_first_longer_trims_front() {
        let (a, b) = align_right(&[1.0, 2.0, 3.0, 4.0], &[5.0, 6.0]);
        assert_eq!(a, &[3.0, 4.0]);
        assert_eq!(b, &[5.0, 6.0]);
    }

    #[test]
    fn align_right_second_longer_trims_front() {
        let (a, b) = align_right(&[1.0, 2.0], &[3.0, 4.0, 5.0, 6.0]);
        assert_eq!(a, &[1.0, 2.0]);
        assert_eq!(b, &[5.0, 6.0]);
    }

    #[test]
    fn align_right_both_empty() {
        let (a, b) = align_right(&[], &[]);
        assert!(a.is_empty());
        assert!(b.is_empty());
    }

    #[test]
    fn align_right_one_empty_clamps_to_zero_length() {
        // The min-length clamp reduces both to empty when either side is empty.
        let (a, b) = align_right(&[1.0, 2.0, 3.0], &[]);
        assert!(a.is_empty());
        assert!(b.is_empty());

        let (a, b) = align_right(&[], &[1.0, 2.0, 3.0]);
        assert!(a.is_empty());
        assert!(b.is_empty());
    }

    #[test]
    fn align_right_single_element_each() {
        let (a, b) = align_right(&[1.0], &[9.0]);
        assert_eq!(a, &[1.0]);
        assert_eq!(b, &[9.0]);
    }
}
