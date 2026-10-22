//! Floating-point assertions for this crate's numerical unit tests.

/// Default tolerance for approximate floating-point comparisons.
pub(super) const EPS: f64 = 1e-9;

/// Asserts that vectors have equal lengths and corresponding values within `eps`.
pub(super) fn assert_vec_close(actual: &[f64], expected: &[f64], eps: f64) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "length mismatch: actual={:?} expected={:?}",
        actual,
        expected
    );
    for (i, (a, e)) in actual.iter().zip(expected.iter()).enumerate() {
        assert!(
            (a - e).abs() <= eps,
            "index {}: actual={} expected={} (eps={})\nactual={:?}\nexpected={:?}",
            i,
            a,
            e,
            eps,
            actual,
            expected
        );
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::{EPS, assert_vec_close};

    fn panic_message(assertion: impl FnOnce()) -> String {
        let payload = catch_unwind(AssertUnwindSafe(assertion)).expect_err("assertion should fail");
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| {
                payload
                    .downcast_ref::<&str>()
                    .map(|message| (*message).to_owned())
            })
            .expect("assertion panic should have a string message")
    }

    #[test]
    fn equal_vectors_are_close() {
        assert_eq!(EPS, 1e-9);
        assert_vec_close(&[1.0, -2.5], &[1.0, -2.5], EPS);
    }

    #[test]
    fn values_at_tolerance_are_close() {
        assert_vec_close(&[EPS], &[0.0], EPS);
    }

    #[test]
    fn values_outside_tolerance_report_index_and_vectors() {
        let message = panic_message(|| assert_vec_close(&[1.0 + 2.0 * EPS], &[1.0], EPS));

        assert!(message.contains("index 0: actual="));
        assert!(message.contains("expected=1 (eps="));
        assert!(message.contains("actual=["));
        assert!(message.contains("expected=[1.0]"));
    }

    #[test]
    fn length_mismatch_reports_both_vectors() {
        let message = panic_message(|| assert_vec_close(&[1.0], &[1.0, 2.0], EPS));

        assert!(message.contains("length mismatch: actual=[1.0] expected=[1.0, 2.0]"));
    }

    #[test]
    fn nan_values_are_rejected() {
        let message = panic_message(|| assert_vec_close(&[f64::NAN], &[0.0], EPS));

        assert!(message.contains("index 0: actual=NaN expected=0 (eps="));
    }
}
