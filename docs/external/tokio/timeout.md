---
source: docs.rs (official rustdoc)
library: tokio
package: tokio
version: 1.54.1
topic: tokio::time::timeout semantics, cancellation, panics
fetched: 2026-11-14
official_docs: https://docs.rs/tokio/1.54.1/tokio/time/fn.timeout.html
---

# tokio::time::timeout

```rust
pub fn timeout<F>(duration: Duration, future: F) -> Timeout<F::IntoFuture>
where
    F: IntoFuture,
```

Requires crate feature `time`.

## Semantics

- Requires the future to complete before the specified duration elapses.
- If it completes in time, the completed value is returned; otherwise an error is
  returned and the future is canceled.
- Returns `Result<T, tokio::time::error::Elapsed>` where `T` is the future's output.
- If the provided future completes immediately, the returned future is guaranteed to
  complete immediately with `Ok` regardless of the duration.

## Cancellation

Cancelling a timeout is done by dropping the future; no additional cleanup or other work
is required. The original future can be recovered with `Timeout::into_inner(self) -> T`,
which consumes the `Timeout`.

## Important timing caveat

The future is polled before the timeout is checked, so if the future does not yield during
execution it is possible for it to complete and exceed the timeout **without** returning
an error. A timeout bounds wall-clock waiting, not a single long-running poll.

## Panics

Panics if there is no current timer. This happens when the runtime was built without
`Builder::enable_time()` or `Builder::enable_all()`, or when a timer is created outside a
tokio runtime (e.g. `rt.block_on(sleep(...))` panics, while
`rt.block_on(async { sleep(...).await })` does not).

## Example

```rust
use tokio::time::timeout;
use tokio::sync::oneshot;

use std::time::Duration;

let (tx, rx) = oneshot::channel();

// Wrap the future with a `Timeout` set to expire in 10 milliseconds.
if let Err(_) = timeout(Duration::from_millis(10), rx).await {
    println!("did not receive value within 10 ms");
}
```
