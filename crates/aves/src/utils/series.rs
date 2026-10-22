//! Series transforms and ranks.

/// Difference between each value and the value `lag` periods before it.
/// Output length: `len - lag`.
pub fn change(values: &[f64], lag: usize) -> Vec<f64> {
    if values.len() <= lag {
        return Vec::new();
    }
    values[lag..]
        .iter()
        .zip(values)
        .map(|(&current, &previous)| current - previous)
        .collect()
}

/// Ratio change between each value and the value `lag` periods before it,
/// minus one. Output length: `len - lag`.
pub fn change_ratio(values: &[f64], lag: usize) -> Vec<f64> {
    if values.len() <= lag {
        return Vec::new();
    }
    values[lag..]
        .iter()
        .zip(values)
        .map(|(&current, &previous)| current / previous - 1.0)
        .collect()
}

/// Counts the number of periods since the last change of value.
/// Output length matches the input.
pub fn since(values: &[f64]) -> Vec<usize> {
    let mut out = Vec::with_capacity(values.len());
    let mut count = 0usize;
    let mut last: Option<f64> = None;

    for &v in values {
        match last {
            Some(l) if l == v => count += 1,
            _ => {
                last = Some(v);
                count = 0;
            }
        }
        out.push(count);
    }

    out
}

/// Percentile rank of each value compared to the previous `period - 1`
/// values, as a percentage. The first output corresponds to input index
/// `period`. Output length: `len - period`.
pub fn percent_rank(values: &[f64], period: usize) -> Vec<f64> {
    assert!(period > 1, "period must be greater than 1");
    if values.len() <= period {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(values.len() - period);
    for i in period..values.len() {
        let window = &values[i + 1 - period..i];
        let less = window.iter().filter(|&&v| v < values[i]).count();
        out.push(less as f64 * 100.0 / (period - 1) as f64);
    }

    out
}

/// Computes an index from the ratio of positive to negative values.
pub fn ratio_index(positive: f64, negative: f64) -> f64 {
    let ratio = positive / negative;
    100.0 - (100.0 / (1.0 + ratio))
}

#[cfg(test)]
mod tests {
    use super::super::math_test_support::{EPS, assert_vec_close};
    use super::{change, change_ratio, percent_rank, ratio_index, since};

    #[test]
    fn change_basic() {
        let out = change(&[1.0, 2.0, 3.0, 4.0, 5.0], 2);
        assert_vec_close(&out, &[2.0, 2.0, 2.0], EPS);
    }

    #[test]
    fn change_ratio_basic() {
        let out = change_ratio(&[1.0, 2.0, 4.0, 3.0], 1);
        assert_vec_close(&out, &[1.0, 1.0, -0.25], EPS);
    }

    #[test]
    fn changes_cover_empty_short_equal_lag_and_lag_zero() {
        let cases: [(&[f64], usize, &[f64]); 6] = [
            (&[], 0, &[]),
            (&[1.0], 1, &[]),
            (&[1.0, 2.0], 2, &[]),
            (&[1.0, 2.0], 1, &[1.0]),
            (&[1.0, 2.0], 0, &[0.0, 0.0]),
            (&[2.0, 4.0], 0, &[0.0, 0.0]),
        ];
        for (values, lag, expected) in cases {
            assert_eq!(change(values, lag), expected);
            assert_eq!(change_ratio(values, lag).len(), expected.len());
        }
        assert_eq!(change_ratio(&[2.0, 4.0], 0), [0.0, 0.0]);
    }

    #[test]
    fn since_basic() {
        let out = since(&[1.0, 1.0, 2.0, 2.0, 2.0, 3.0]);
        assert_eq!(out, vec![0, 1, 0, 1, 2, 0]);
    }

    #[test]
    fn since_keeps_equal_runs_separate_across_nan() {
        let values = [1.0, 1.0, f64::NAN, f64::NAN, 1.0, 1.0];
        assert_eq!(since(&values), [0, 1, 0, 0, 0, 1]);
    }

    #[test]
    fn percent_rank_basic() {
        let out = percent_rank(&[1.0, 2.0, 3.0, 0.0, 4.0], 3);
        assert_vec_close(&out, &[0.0, 100.0], EPS);
    }

    #[test]
    #[should_panic(expected = "period must be greater than 1")]
    fn percent_rank_period_zero_panics_before_short_input_gate() {
        percent_rank(&[], 0);
    }

    #[test]
    #[should_panic(expected = "period must be greater than 1")]
    fn percent_rank_period_one_panics_before_short_input_gate() {
        percent_rank(&[], 1);
    }

    #[test]
    fn percent_rank_oversized_period_is_empty() {
        assert!(percent_rank(&[1.0, 2.0], 3).is_empty());
    }

    #[test]
    fn percent_rank_preserves_ties_descending_and_nonfinite_comparisons() {
        let cases: [(&[f64], usize, &[f64]); 5] = [
            (&[2.0, 2.0, 2.0, 2.0, 2.0], 3, &[0.0, 0.0]),
            (&[4.0, 3.0, 2.0, 1.0, 0.0], 3, &[0.0, 0.0]),
            (&[1.0, 2.0, 3.0, f64::NAN], 3, &[0.0]),
            (&[1.0, 2.0, 3.0, f64::INFINITY], 3, &[100.0]),
            (&[f64::NEG_INFINITY, 1.0, 2.0, 3.0], 3, &[100.0]),
        ];
        for (values, period, expected) in cases {
            assert_eq!(percent_rank(values, period), expected);
        }
    }

    #[test]
    fn ratio_index_equal_positive_negative_is_fifty() {
        // ratio = 1.0 → 100 - 100/2 = 50
        assert!((ratio_index(50.0, 50.0) - 50.0).abs() <= EPS);
    }

    #[test]
    fn ratio_index_zero_positive_is_zero() {
        // ratio = 0 → 100 - 100/1 = 0
        assert!((ratio_index(0.0, 100.0) - 0.0).abs() <= EPS);
    }

    #[test]
    fn ratio_index_zero_negative_is_one_hundred() {
        // ratio = +inf → 1 + inf = inf → 100/inf = 0 → 100 - 0 = 100
        assert!((ratio_index(100.0, 0.0) - 100.0).abs() <= EPS);
    }

    #[test]
    fn ratio_index_positive_greater_than_negative_above_fifty() {
        // ratio = 3 → 100 - 100/4 = 75
        assert!((ratio_index(75.0, 25.0) - 75.0).abs() <= EPS);
    }

    #[test]
    fn ratio_index_positive_less_than_negative_below_fifty() {
        // ratio = 1/3 → 100 - 100/(4/3) = 100 - 75 = 25
        assert!((ratio_index(25.0, 75.0) - 25.0).abs() <= EPS);
    }

    #[test]
    fn ratio_index_both_zero_is_nan() {
        // 0/0 = NaN; 100 - 100/(1+NaN) = NaN
        assert!(ratio_index(0.0, 0.0).is_nan());
    }

    #[test]
    fn ratio_index_negative_equals_negative_positive_is_infinite() {
        // ratio = positive / (-positive) = -1 → 1 + ratio = 0 → division by zero
        let result = ratio_index(50.0, -50.0);
        assert!(result.is_infinite());
    }

    #[test]
    fn ratio_index_negative_denominator_positive_numerator_below_zero() {
        // ratio = positive / negative < 0 when signs differ; result < 0
        // ratio = 100 / -50 = -2 → 100 - 100/(-1) = 100 + 100 = 200
        assert!((ratio_index(100.0, -50.0) - 200.0).abs() <= EPS);
    }

    #[test]
    fn ratio_index_both_negative_above_fifty() {
        // ratio = -50 / -50 = 1 → 100 - 100/2 = 50
        assert!((ratio_index(-50.0, -50.0) - 50.0).abs() <= EPS);
    }

    #[test]
    fn ratio_index_large_equal_values_is_fifty() {
        assert!((ratio_index(1e12, 1e12) - 50.0).abs() <= EPS);
    }

    #[test]
    fn ratio_index_symmetric_around_fifty() {
        // ratio_index(a, b) + ratio_index(b, a) = 100 when both positive
        let a = 80.0;
        let b = 20.0;
        let sum = ratio_index(a, b) + ratio_index(b, a);
        assert!((sum - 100.0).abs() <= EPS);
    }

    #[test]
    fn ratio_index_tiny_positive_tiny_negative_is_fifty() {
        assert!((ratio_index(1e-10, 1e-10) - 50.0).abs() <= EPS);
    }

    #[test]
    fn ratio_index_extreme_positive_dominance_approaches_one_hundred() {
        // ratio = 1e9 / 1 → 100 - 100/(1+1e9) ≈ 100
        let result = ratio_index(1e9, 1.0);
        assert!((result - 100.0).abs() <= 1e-6);
    }

    #[test]
    fn ratio_index_extreme_negative_dominance_approaches_zero() {
        // ratio = 1 / 1e9 → 100 - 100/(1+1e9) ≈ 0
        let result = ratio_index(1.0, 1e9);
        assert!((result - 0.0).abs() <= 1e-6);
    }

    #[test]
    fn ratio_index_preserves_signed_zero_and_nonfinite_results() {
        let cases: [(f64, f64, fn(f64) -> bool); 6] = [
            (0.0, -1.0, f64::is_sign_positive),
            (-0.0, 1.0, f64::is_sign_positive),
            (f64::INFINITY, f64::INFINITY, f64::is_nan),
            (f64::NEG_INFINITY, f64::INFINITY, f64::is_nan),
            (1.0, f64::INFINITY, |value| value == 0.0),
            (1.0, f64::NEG_INFINITY, |value| value == 0.0),
        ];
        for (positive, negative, matches) in cases {
            assert!(matches(ratio_index(positive, negative)));
        }
    }
}
