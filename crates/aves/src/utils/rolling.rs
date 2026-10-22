//! Rolling numerical calculations.

use std::collections::VecDeque;

/// Moving sum over `period`. Output length: `len - period + 1`.
pub fn moving_sum(values: &[f64], period: usize) -> Vec<f64> {
    assert!(period > 0, "period must be greater than 0");
    if values.len() < period {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(values.len() - period + 1);
    let mut sum: f64 = values[..period].iter().sum();
    out.push(sum);

    for i in period..values.len() {
        sum += values[i] - values[i - period];
        out.push(sum);
    }

    out
}

/// Moving maximum over `period`. Output length: `len - period + 1`.
pub fn moving_max(values: &[f64], period: usize) -> Vec<f64> {
    moving_extreme(values, period, |back, current| back <= current)
}

/// Moving minimum over `period`. Output length: `len - period + 1`.
pub fn moving_min(values: &[f64], period: usize) -> Vec<f64> {
    moving_extreme(values, period, |back, current| back >= current)
}

fn moving_extreme(values: &[f64], period: usize, should_remove_back: impl Fn(f64, f64) -> bool) -> Vec<f64> {
    assert!(period > 0, "period must be greater than 0");
    if values.len() < period {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(values.len() - period + 1);
    let mut deque: VecDeque<usize> = VecDeque::new();

    for i in 0..values.len() {
        while deque
            .back()
            .is_some_and(|&back| should_remove_back(values[back], values[i]))
        {
            deque.pop_back();
        }
        deque.push_back(i);

        if let Some(&front) = deque.front()
            && front + period <= i
        {
            deque.pop_front();
        }

        if i + 1 >= period {
            out.push(values[*deque.front().unwrap()]);
        }
    }

    out
}

/// Moving population standard deviation over `period`.
/// Output length: `len - period + 1`.
pub fn moving_std(values: &[f64], period: usize) -> Vec<f64> {
    assert!(period > 0, "period must be greater than 0");
    if values.len() < period {
        return Vec::new();
    }

    values
        .windows(period)
        .map(|window| {
            let mean = window.iter().sum::<f64>() / period as f64;
            let var = window.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / period as f64;
            var.sqrt()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::math_test_support::{EPS, assert_vec_close};
    use super::{moving_max, moving_min, moving_std, moving_sum};

    #[test]
    fn moving_sum_basic() {
        let out = moving_sum(&[1.0, 2.0, 3.0, 4.0, 5.0], 3);
        assert_vec_close(&out, &[6.0, 9.0, 12.0], EPS);
    }

    #[test]
    fn moving_sum_period_one_is_identity() {
        let out = moving_sum(&[3.0, -1.0, 2.0], 1);
        assert_vec_close(&out, &[3.0, -1.0, 2.0], EPS);
    }

    #[test]
    fn moving_sum_short_input_is_empty() {
        let out = moving_sum(&[1.0, 2.0], 3);
        assert!(out.is_empty());
    }

    #[test]
    fn moving_max_basic() {
        let out = moving_max(&[1.0, 3.0, 2.0, 5.0, 4.0], 2);
        assert_vec_close(&out, &[3.0, 3.0, 5.0, 5.0], EPS);
    }

    #[test]
    fn moving_max_decreasing() {
        let out = moving_max(&[5.0, 4.0, 3.0, 2.0], 3);
        assert_vec_close(&out, &[5.0, 4.0], EPS);
    }

    #[test]
    fn moving_min_basic() {
        let out = moving_min(&[1.0, 3.0, 2.0, 5.0, 4.0], 2);
        assert_vec_close(&out, &[1.0, 2.0, 2.0, 4.0], EPS);
    }

    #[test]
    fn moving_std_basic() {
        let out = moving_std(&[1.0, 3.0, 5.0, 9.0], 2);
        assert_vec_close(&out, &[1.0, 1.0, 2.0], EPS);
    }

    #[test]
    fn moving_std_constant_is_zero() {
        let out = moving_std(&[4.0, 4.0, 4.0, 4.0], 3);
        assert_vec_close(&out, &[0.0, 0.0], EPS);
    }

    #[test]
    fn moving_sum_empty_equal_window_and_nonfinite_values() {
        assert!(moving_sum(&[], 1).is_empty());
        assert_eq!(moving_sum(&[2.0, 3.0], 2), vec![5.0]);
        let out = moving_sum(&[f64::INFINITY, f64::NEG_INFINITY, 1.0], 2);
        assert!(out.iter().all(|value| value.is_nan()));
    }

    #[test]
    fn moving_sum_zero_period_panics_before_empty_gate() {
        assert!(std::panic::catch_unwind(|| moving_sum(&[], 0)).is_err());
    }

    #[test]
    fn moving_sum_incremental_signed_zero_behavior() {
        let out = moving_sum(&[-0.0, 0.0], 1);
        assert!(out[0].is_sign_negative());
        assert!(!out[1].is_sign_negative());
    }

    #[test]
    fn moving_max_equal_window_ties_expiration_and_period_one() {
        assert_eq!(moving_max(&[2.0, 2.0], 2), vec![2.0]);
        assert_eq!(moving_max(&[5.0, 1.0, 4.0], 2), vec![5.0, 4.0]);
        assert_eq!(moving_max(&[3.0, 1.0], 1), vec![3.0, 1.0]);
        assert!(moving_max(&[], 1).is_empty());
        assert!(moving_max(&[1.0], 2).is_empty());
    }

    #[test]
    fn moving_max_zero_period_and_ieee_ordering() {
        assert!(std::panic::catch_unwind(|| moving_max(&[], 0)).is_err());
        assert!(moving_max(&[f64::NAN, 2.0], 2)[0].is_nan());
        assert_eq!(moving_max(&[f64::NEG_INFINITY, f64::INFINITY], 2), vec![f64::INFINITY]);
        let zeros = moving_max(&[-0.0, 0.0], 2);
        assert!(!zeros[0].is_sign_negative());
        let zeros = moving_max(&[0.0, -0.0], 2);
        assert!(zeros[0].is_sign_negative());
    }

    #[test]
    fn moving_min_equal_window_ties_expiration_and_period_one() {
        assert_eq!(moving_min(&[2.0, 2.0], 2), vec![2.0]);
        assert_eq!(moving_min(&[1.0, 5.0, 2.0], 2), vec![1.0, 2.0]);
        assert_eq!(moving_min(&[3.0, 1.0], 1), vec![3.0, 1.0]);
        assert!(moving_min(&[], 1).is_empty());
        assert!(moving_min(&[1.0], 2).is_empty());
    }

    #[test]
    fn moving_min_zero_period_and_ieee_ordering() {
        assert!(std::panic::catch_unwind(|| moving_min(&[], 0)).is_err());
        assert!(moving_min(&[f64::NAN, 2.0], 2)[0].is_nan());
        assert_eq!(
            moving_min(&[f64::INFINITY, f64::NEG_INFINITY], 2),
            vec![f64::NEG_INFINITY]
        );
        let zeros = moving_min(&[-0.0, 0.0], 2);
        assert!(!zeros[0].is_sign_negative());
        let zeros = moving_min(&[0.0, -0.0], 2);
        assert!(zeros[0].is_sign_negative());
    }

    #[test]
    fn moving_std_empty_equal_window_period_one_and_nonfinite() {
        assert!(moving_std(&[], 1).is_empty());
        assert_eq!(moving_std(&[2.0, 4.0], 2), vec![1.0]);
        assert_eq!(moving_std(&[2.0, 4.0], 1), vec![0.0, 0.0]);
        assert!(moving_std(&[f64::INFINITY], 1)[0].is_nan());
        assert!(moving_std(&[f64::NAN, 1.0], 2)[0].is_nan());
        assert!(moving_std(&[1.0], 2).is_empty());
    }

    #[test]
    fn moving_std_zero_period_panics_before_empty_gate() {
        assert!(std::panic::catch_unwind(|| moving_std(&[], 0)).is_err());
    }
}
