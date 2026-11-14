---
source: docs.rs (official rustdoc)
library: tokio
package: tokio
version: 1.54.1
topic: Task cancellation (JoinHandle::abort, abort_handle, CancellationToken)
fetched: 2026-11-14
official_docs: https://docs.rs/tokio/1.54.1/tokio/task/struct.JoinHandle.html
---

# Task cancellation

## Dropping a future

Dropping a future cancels it; cancellation takes effect at an `.await` point. This is the
mechanism behind `tokio::select!` cancelling losing branches and behind `tokio::time::timeout`
cancelling its inner future. No extra cleanup is required to cancel a future, but any
cancellation-sensitive partial work is simply abandoned.

## `JoinHandle` and `AbortHandle`

```rust
impl<T> JoinHandle<T> {
    pub fn abort(&self);
    pub fn is_finished(&self) -> bool;
    pub fn abort_handle(&self) -> AbortHandle;
    pub fn id(&self) -> Id;
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;
}
```

- `abort()` requests cancellation. Awaiting the handle then usually yields a `JoinError` for
  which `is_cancelled()` is true. If the task had already completed, awaiting may still
  succeed normally.
- `spawn_blocking` tasks **cannot be aborted** because they are not async. Calling `abort`
  has no effect once the blocking task has started; it may only prevent a not-yet-started
  task from running.
- `is_finished()` can remain `false` briefly after `abort()` because cancellation is
  asynchronous; it becomes `true` only after the task actually terminates.
- `abort_handle()` returns an owned `AbortHandle` that can be moved elsewhere and used to
  abort the task remotely.
- Awaiting `&mut JoinHandle<T>` is **cancel safe**: if another `select!` branch wins, the
  task's output is not lost.
- Dropping a `JoinHandle` **detaches** the task rather than cancelling it: the task keeps
  running in the background and its return value is lost. Dropping the handle is not a
  cancellation mechanism.
- The spawned task's destructor is guaranteed to have finished before completion is observed
  through `await`, `is_finished`, or `AbortHandle::is_finished`.

## `tokio_util::sync::CancellationToken`

Crate: `tokio-util` 0.7.20 (requires tokio `^1.51.0`).

```rust
pub struct CancellationToken { /* ... */ }

impl CancellationToken {
    pub fn new() -> CancellationToken;
    pub fn child_token(&self) -> CancellationToken;
    pub fn cancel(&self);
    pub fn is_cancelled(&self) -> bool;
    pub fn cancelled(&self) -> WaitForCancellationFuture<'_>;
    pub fn cancelled_owned(self) -> WaitForCancellationFutureOwned;
    pub fn drop_guard(self) -> DropGuard;
    pub fn drop_guard_ref(&self) -> DropGuardRef<'_>;
    pub async fn run_until_cancelled<F>(&self, fut: F) -> Option<F::Output>
    where F: Future;
    pub async fn run_until_cancelled_owned<F>(self, fut: F) -> Option<F::Output>
    where F: Future;
}
```

- The token is `Clone` and `Send + Sync`. Cloning produces a token cancelled whenever the
  original is cancelled, and vice versa. `PartialEq` compares cancellation identity, true for
  clones but not for parent/child tokens.
- `child_token()` creates a token cancelled when the parent is cancelled; cancelling a child
  does **not** cancel the parent.
- `cancel()` cancels the token and all child tokens and wakes all waiters. Cancellation is not
  atomic across threads: one child may report `is_cancelled() == true` while another still
  reports `false`; once `cancel()` returns, all children are fully cancelled.
- `cancelled()` returns a future that resolves on cancellation, immediately if already
  cancelled. It is **cancel safe**. `cancelled_owned()` owns the token for use in
  `'static` tasks.
- `drop_guard()` / `drop_guard_ref()` return a guard that cancels the token on drop unless
  disarmed — useful so a dropped owner stops its subtree.
- `run_until_cancelled(fut)` returns `Some(output)` if `fut` completes first, otherwise
  `None` and drops `fut`. On an already-cancelled token it returns `None` immediately; on
  later polls it is biased toward future completion. It is cancel safe only if `fut` is
  cancel safe.

## Actor-relevant caveats

- Graceful shutdown is cooperative: use a `CancellationToken` (or a watch channel) fanned out
  to tasks and select on `token.cancelled()`; use `JoinHandle::abort()` only for hard,
  immediate termination where abandoned work is acceptable.
- Because dropping a `JoinHandle` detaches, retain handles (or `AbortHandle`s) for any task
  that must be stopped at shutdown.
- A cancelled task is dropped at an `.await`; cleanup that must run should be in a `Drop`
  impl / drop guard or in an explicit shutdown branch, not after the cancellation point.
- `spawn_blocking` work cannot be force-cancelled; design blocking tasks to observe the
  token themselves if they need to stop early.
