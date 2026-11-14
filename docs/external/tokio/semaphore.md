---
source: docs.rs (official rustdoc)
library: tokio
package: tokio
version: 1.54.1
topic: tokio::sync::Semaphore
fetched: 2026-11-14
official_docs: https://docs.rs/tokio/1.54.1/tokio/sync/struct.Semaphore.html
---

# `tokio::sync::Semaphore`

Available on crate feature **`sync`** only.

A counting semaphore performing asynchronous permit acquisition. It differs from a mutex in
that it allows more than one concurrent caller to access the shared resource. When `acquire`
is called and permits remain, the permit is returned immediately; otherwise the caller waits
until an outstanding permit is dropped.

The semaphore is **fair**: permits are handed out in request order. Fairness also applies to
`acquire_many`, so a front-of-queue `acquire_many` that needs more permits than are available
can block a later `acquire` even when enough permits exist for it.

## Construction and inspection

```rust
pub const MAX_PERMITS: usize = usize::MAX >> 3;

pub fn new(permits: usize) -> Self;
pub const fn const_new(permits: usize) -> Self;

pub fn available_permits(&self) -> usize;
pub fn add_permits(&self, n: usize);
pub fn forget_permits(&self, n: usize) -> usize;
```

- `new` panics if `permits > MAX_PERMITS`.
- `const_new` supports statics. With the tracing unstable feature it is **not instrumented**
  (invisible to tokio-console); use `new` when instrumentation is required.
- `add_permits` panics if the total would exceed `MAX_PERMITS`.
- `forget_permits` reduces permits by at most `n` and returns how many were actually removed.

## Acquisition

```rust
pub async fn acquire(&self) -> Result<SemaphorePermit<'_>, AcquireError>;
pub async fn acquire_many(&self, n: u32) -> Result<SemaphorePermit<'_>, AcquireError>;

pub async fn acquire_owned(self: Arc<Self>) -> Result<OwnedSemaphorePermit, AcquireError>;
pub async fn acquire_many_owned(self: Arc<Self>, n: u32)
    -> Result<OwnedSemaphorePermit, AcquireError>;

pub fn try_acquire(&self) -> Result<SemaphorePermit<'_>, TryAcquireError>;
pub fn try_acquire_many(&self, n: u32) -> Result<SemaphorePermit<'_>, TryAcquireError>;
pub fn try_acquire_owned(self: Arc<Self>) -> Result<OwnedSemaphorePermit, TryAcquireError>;
pub fn try_acquire_many_owned(self: Arc<Self>, n: u32)
    -> Result<OwnedSemaphorePermit, TryAcquireError>;
```

- `Result::Err` from `acquire` is `AcquireError` (only when the semaphore is closed).
- `try_acquire*` distinguishes `TryAcquireError::NoPermits` from `TryAcquireError::Closed`.
- The borrowed `SemaphorePermit<'_>` cannot outlive the semaphore borrow. Use an `_owned`
  variant to move a permit into a spawned task; `acquire_owned` requires the semaphore in an
  `Arc`.
- Dropping a permit releases it back to the semaphore. `SemaphorePermit::forget` (shown in
  the token-bucket example) releases a permit without making it available again.

Blocking variants exist for synchronous code: `blocking_acquire`, `blocking_acquire_many`,
`blocking_acquire_owned`, `blocking_acquire_many_owned`. They **panic inside an asynchronous
execution context**; use them from `spawn_blocking`.

## Close

```rust
pub fn close(&self);
pub fn is_closed(&self) -> bool;
```

Closing prevents new permits from being issued and wakes all pending waiters with
`AcquireError`. `try_acquire*` then returns `TryAcquireError::Closed`.

## Cancellation safety

`acquire`, `acquire_many`, `acquire_owned`, and `acquire_many_owned` are **not cancel safe**
in the queue-position sense: cancelling a call makes the caller lose its place in the fair
queue. The docs classify `Semaphore::acquire` under "not cancellation safe because they use a
queue for fairness" in `tokio::select!`.

## Memory ordering

Acquiring a permit (`acquire`, `acquire_many`, `try_acquire`, `try_acquire_many`, and the
`_owned` variants), releasing one (dropping a permit, `add_permits`, `forget_permits`), and
`close` are `AcqRel` operations, totally ordered and synchronizing-with prior such
operations. This makes a semaphore safe for handing data between tasks through shared state.
Failed acquisitions, `available_permits`, and `is_closed` behave like an `Acquire` load.

## Actor-relevant caveats

- A permit held across `.await` bounds concurrency for the duration of that await; acquire
  close to the work, not at task start, when the goal is limiting actual in-flight work.
- Holding a borrowed permit across `tokio::spawn` is not possible; use `acquire_owned` and
  move the `OwnedSemaphorePermit` into the task.
- `acquire` in a `select!` loop is not cancel safe; a losing branch loses queue position.
  Prefer acquiring outside the `select!` or use `try_acquire` when the branch must be
  cancellable without queue loss.
- `close()` is the shutdown signal for a semaphore-backed worker admission gate: waiters
  receive `AcquireError`.

## Example: bound in-flight work

```rust
use std::sync::Arc;
use tokio::sync::Semaphore;

let semaphore = Arc::new(Semaphore::new(5));
let mut join_handles = Vec::new();

for task_id in 0..50 {
    let permit = semaphore.clone().acquire_owned().await.unwrap();
    join_handles.push(tokio::spawn(async move {
        // work here while owning the permit
        drop(permit);
    }));
}

for handle in join_handles {
    handle.await.unwrap();
}
```
