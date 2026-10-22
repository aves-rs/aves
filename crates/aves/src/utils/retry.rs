use std::future::Future;
use std::time::Duration;

use tokio::sync::oneshot::Receiver;
use tokio::time::sleep;

/// Bounded jittered exponential backoff for provider reconnection loops.
/// Each delay is clamped to the configured bounds; the base doubles up to max.
pub struct RetryBackoff {
    current: Duration,
    min: Duration,
    max: Duration,
}

impl RetryBackoff {
    /// Create a new backoff starting at `min` and capped at `max`.
    pub fn new(min: Duration, max: Duration) -> Self {
        Self { current: min, min, max }
    }

    /// Return the next sleep duration with ±25% jitter, then advance the backoff.
    pub fn next_delay(&mut self) -> Duration {
        let base_ms = self.current.as_millis() as u64;
        let jitter_ms = (base_ms / 4).saturating_sub(1);
        let jitter_sample = if jitter_ms > 0 {
            rand::random_range(0..jitter_ms * 2)
        } else {
            0
        };
        let delay = jittered_delay(base_ms, self.min.as_millis() as u64, self.max, jitter_sample);
        self.current = std::cmp::min(self.current * 2, self.max);
        delay
    }

    /// Reset the backoff to `min` after a successful connection.
    pub fn reset(&mut self) {
        self.current = self.min;
    }
}

fn jittered_delay(base_ms: u64, min_ms: u64, max: Duration, jitter_sample: u64) -> Duration {
    let jitter_ms = (base_ms / 4).saturating_sub(1);
    let jitter_offset = if jitter_ms > 0 {
        jitter_sample.wrapping_sub(jitter_ms) as i64
    } else {
        0
    };
    let delay_ms = (base_ms as i64 + jitter_offset).max(min_ms as i64) as u64;
    Duration::from_millis(delay_ms).min(max)
}

/// Retry an async operation while its errors remain retryable.
pub async fn retry_with_backoff<T, E, Operation, OperationFuture, ShouldRetry>(
    max_attempts: u32,
    mut backoff: RetryBackoff,
    mut operation: Operation,
    should_retry: ShouldRetry,
) -> Result<T, E>
where
    Operation: FnMut() -> OperationFuture,
    OperationFuture: Future<Output = Result<T, E>>,
    ShouldRetry: Fn(&E) -> bool,
{
    let max_attempts = max_attempts.max(1);
    for attempt in 1..=max_attempts {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) if attempt < max_attempts && should_retry(&error) => {
                sleep(backoff.next_delay()).await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("bounded retry loop always returns")
}

/// Sleep until the next delay elapses or cancellation is signaled.
pub async fn sleep_with_backoff(backoff: &mut RetryBackoff, cancel: &mut Receiver<()>) -> bool {
    tokio::select! {
        _ = sleep(backoff.next_delay()) => true,
        _ = cancel => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    #[test]
    fn doubles_and_caps_at_max() {
        let mut policy = RetryBackoff::new(Duration::from_millis(500), Duration::from_secs(30));
        for _ in 0..20 {
            let d = policy.next_delay();
            assert!(d <= Duration::from_secs(30));
        }
    }

    #[test]
    fn reset_returns_to_min() {
        let mut policy = RetryBackoff::new(Duration::from_millis(500), Duration::from_secs(30));
        for _ in 0..10 {
            policy.next_delay();
        }
        policy.reset();
        assert_eq!(policy.current, policy.min);
    }

    #[test]
    fn delay_never_below_min() {
        let min = Duration::from_millis(500);
        let mut policy = RetryBackoff::new(min, Duration::from_secs(30));
        for _ in 0..20 {
            let d = policy.next_delay();
            assert!(d >= min);
        }
    }

    #[test]
    fn jitter_arithmetic_preserves_both_clamp_edges() {
        let max = Duration::from_millis(1_000);
        assert_eq!(jittered_delay(100, 90, max, 0), Duration::from_millis(90));
        assert_eq!(jittered_delay(100, 1, max, 48), Duration::from_millis(124));
        assert_eq!(
            jittered_delay(100, 1, Duration::from_millis(105), 98),
            Duration::from_millis(105)
        );
        assert_eq!(jittered_delay(100, 1, max, 49), Duration::from_millis(125));
    }

    #[test]
    fn zero_submillisecond_and_equal_bounds_are_stable() {
        let mut zero = RetryBackoff::new(Duration::ZERO, Duration::ZERO);
        assert_eq!(zero.next_delay(), Duration::ZERO);

        let mut submillisecond = RetryBackoff::new(Duration::from_nanos(999), Duration::from_millis(10));
        assert_eq!(submillisecond.next_delay(), Duration::ZERO);

        let fixed = Duration::from_millis(40);
        let mut equal = RetryBackoff::new(fixed, fixed);
        assert_eq!(equal.next_delay(), fixed);
        assert_eq!(equal.current, fixed);
    }

    #[test]
    fn doubling_caps_and_reset_restores_minimum() {
        let mut policy = RetryBackoff::new(Duration::from_millis(10), Duration::from_millis(40));
        policy.next_delay();
        assert_eq!(policy.current, Duration::from_millis(20));
        policy.next_delay();
        assert_eq!(policy.current, Duration::from_millis(40));
        policy.next_delay();
        assert_eq!(policy.current, Duration::from_millis(40));
        policy.reset();
        assert_eq!(policy.current, Duration::from_millis(10));
    }

    #[tokio::test]
    async fn retries_only_retryable_async_failures() {
        let attempts = AtomicU32::new(0);
        let result = retry_with_backoff(
            3,
            RetryBackoff::new(Duration::ZERO, Duration::ZERO),
            || async {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt < 3 { Err("retry") } else { Ok("done") }
            },
            |error| *error == "retry",
        )
        .await;

        assert_eq!(result, Ok("done"));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn returns_non_retryable_async_failure_immediately() {
        let attempts = AtomicU32::new(0);
        let result: Result<(), &str> = retry_with_backoff(
            5,
            RetryBackoff::new(Duration::ZERO, Duration::ZERO),
            || async {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err("terminal")
            },
            |error| *error == "retry",
        )
        .await;

        assert_eq!(result, Err("terminal"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn zero_attempts_still_runs_once_and_returns_typed_error() {
        let attempts = AtomicU32::new(0);
        let result: Result<(), &str> = retry_with_backoff(
            0,
            RetryBackoff::new(Duration::ZERO, Duration::ZERO),
            || async {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err("last")
            },
            |_| true,
        )
        .await;
        assert_eq!(result, Err("last"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn exhausted_retries_return_last_error_without_final_predicate() {
        let attempts = AtomicU32::new(0);
        let predicates = AtomicU32::new(0);
        let result: Result<(), &str> = retry_with_backoff(
            3,
            RetryBackoff::new(Duration::ZERO, Duration::ZERO),
            || async {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                Err(if attempt == 3 { "last" } else { "retry" })
            },
            |_| {
                predicates.fetch_add(1, Ordering::SeqCst);
                true
            },
        )
        .await;
        assert_eq!(result, Err("last"));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert_eq!(predicates.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn immediate_success_skips_retry_predicate() {
        let predicates = AtomicU32::new(0);
        let result = retry_with_backoff(
            4,
            RetryBackoff::new(Duration::ZERO, Duration::ZERO),
            || async { Ok::<_, &str>("ok") },
            |_| {
                predicates.fetch_add(1, Ordering::SeqCst);
                true
            },
        )
        .await;
        assert_eq!(result, Ok("ok"));
        assert_eq!(predicates.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn elapsed_zero_delay_wins_with_live_cancellation_sender() {
        let mut policy = RetryBackoff::new(Duration::ZERO, Duration::ZERO);
        let (_sender, mut receiver) = tokio::sync::oneshot::channel();
        assert!(sleep_with_backoff(&mut policy, &mut receiver).await);
    }

    #[tokio::test]
    async fn queued_or_dropped_cancellation_stops_nonzero_delay() {
        let mut policy = RetryBackoff::new(Duration::from_secs(60), Duration::from_secs(60));
        let (sender, mut receiver) = tokio::sync::oneshot::channel();
        sender.send(()).unwrap();
        assert!(!sleep_with_backoff(&mut policy, &mut receiver).await);

        let mut dropped_policy = RetryBackoff::new(Duration::from_secs(60), Duration::from_secs(60));
        let (sender, mut receiver) = tokio::sync::oneshot::channel();
        drop(sender);
        assert!(!sleep_with_backoff(&mut dropped_policy, &mut receiver).await);
    }
}
