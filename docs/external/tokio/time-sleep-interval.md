---
source: docs.rs (official rustdoc)
library: tokio
package: tokio
version: 1.54.1
topic: tokio::time sleep, sleep_until, Sleep and Interval
fetched: 2026-11-15
official_docs: https://docs.rs/tokio/1.54.1/tokio/time/struct.Sleep.html
---

# `tokio::time` timers: `sleep`, `sleep_until`, `Sleep`, `Interval`

Available on crate feature **`time`** only.

## `sleep` / `sleep_until`

```rust
pub fn sleep(duration: Duration) -> Sleep;
pub fn sleep_until(deadline: Instant) -> Sleep;
```

`Sleep` is a `Future<Output = ()>`. No work is performed while awaiting it. Timers operate at
**millisecond granularity** and are not suitable for high-resolution timing. For periodic work
use `interval` rather than a loop over `sleep`.

Both panic if no timer is set on the runtime (`Builder::enable_time` / `enable_all`) or if the
timer is created outside a Tokio runtime. `rt.block_on(sleep(d))` panics because the call
happens outside the runtime; `rt.block_on(async { sleep(d).await })` does not, because the
call is deferred until it runs inside the runtime.

Cancellation: **drop the returned future**. No cleanup is required.

## `Sleep`

```rust
impl Sleep {
    pub fn deadline(&self) -> Instant;
    pub fn is_elapsed(&self) -> bool;
    pub fn reset(self: Pin<&mut Self>, deadline: Instant);
}
```

- `Sleep` does **not** implement `Unpin`. To use it with `select!` or `poll` it must be pinned
  (`tokio::pin!`, `Box::pin`, or pin projection); `.await`-ing it directly needs no pinning.
- `reset` sets a new deadline on existing state. It works **both before and after** the future
  has completed — this is the supported way to reuse one timer across loop iterations.
- `is_elapsed` reports whether the requested instant has passed. Never use it as a `select!`
  precondition: it can be true at the test and false when the branch is polled, silently
  skipping the timeout branch. Poll the timer as a normal branch instead.

## `Interval`

```rust
impl Interval {
    pub async fn tick(&mut self) -> Instant;
    pub fn poll_tick(&mut self, cx: &mut Context<'_>) -> Poll<Instant>;

    pub fn reset(&mut self);                       // now + period
    pub fn reset_immediately(&mut self);           // now
    pub fn reset_after(&mut self, after: Duration); // now + after
    pub fn reset_at(&mut self, deadline: Instant);

    pub fn period(&self) -> Duration;
    pub fn missed_tick_behavior(&self) -> MissedTickBehavior;
    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior);
}
```

- `tick()` is **cancel safe**: if another `select!` branch wins, no tick was consumed.
- The **first** `tick()` completes immediately after the interval is created.
- Unlike `sleep` in a loop, `Interval` accounts for the time spent between calls.
- Every `reset*` variant **ignores** `MissedTickBehavior`. `reset_at` with a past instant
  catches up using the missed-tick strategy; with a future instant it may sleep longer than one
  period; previously missed ticks are discarded.
- `poll_tick` returns `Pending` / `Ready(instant)`; only the most recent `Waker` is registered.

## `MissedTickBehavior`

```rust
pub enum MissedTickBehavior { Burst, Delay, Skip }
```

Default is `Burst`. Strategies only take effect for delays greater than 5 ms, because the
executor cannot guarantee exact timer precision.

| Variant | On a missed tick |
|---|---|
| `Burst` (default) | Fire as fast as possible until caught up; tick instants are unchanged, spacing may be shortened. |
| `Delay` | Reschedule the next tick one full `period` **from the moment `tick` was called**; never shortens spacing, no longer aligned to `start`. |
| `Skip` | Jump to the next multiple of `period` measured from `start`; skipped ticks are lost, spacing may be shortened. |

For a periodic supervisor heartbeat, `Skip` or `Delay` avoids the `Burst` catch-up storm after
a long blocking section.

## Supervision-relevant caveats

- **Hot-loop bug**: a `Sleep` or `Interval` polled repeatedly must be reset when its branch
  fires. A completed `Sleep` stays ready, so the branch resolves immediately on every subsequent
  poll and the loop spins. The documented pattern is `tokio::pin!` + `sleep.as_mut().reset(..)`
  inside the branch handler.
- **Sliding deadline**: `sleep(Duration::from_secs(n))` recreated at the top of each loop
  iteration measures from *now*, so a slow iteration silently extends the deadline. When a
  budget is meant to be absolute (a process kill deadline, a total-timeout), compute the
  `Instant` once and use `sleep_until(deadline)` — or keep the deadline in a variable you do
  not recompute.
- **Precondition race**: do not gate a timer branch with `if !sleep.is_elapsed()`; the
  precondition is evaluated before polling and the branch can be disabled for the whole call.
- A `select!` that drops a `sleep_until` deadline branch does not leak the timer, but dropping
  the only handle to a kill-deadline means the deadline is forgotten — keep the deadline data
  separate from the future.
- `Interval` starting with an immediate first tick means a `tick()` branch is ready at loop
  entry; combine it with `MissedTickBehavior::Skip` and a `biased;` ordering when a shutdown
  branch must win over the first tick.

## Example: reusable pinned deadline inside `select!`

```rust
use tokio::time::{self, Duration, Instant};

let sleep = time::sleep(Duration::from_millis(10));
tokio::pin!(sleep);

loop {
    tokio::select! {
        () = &mut sleep => {
            println!("timer elapsed");
            sleep.as_mut().reset(Instant::now() + Duration::from_millis(50));
        }
    }
}
```
