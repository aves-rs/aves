---
source: docs.rs (official rustdoc)
library: tokio
package: tokio
version: 1.54.1
topic: Paused time testing (pause, advance, resume, start_paused)
fetched: 2026-11-14
official_docs: https://docs.rs/tokio/1.54.1/tokio/time/fn.pause.html
---

# Paused time testing

Requires crate features **`test-util` and `time`**. Pausing time requires the
**`current_thread`** runtime, which is the default for `#[tokio::test]`.

Related cached notes: [test-attribute.md](test-attribute.md) (`#[tokio::test(start_paused = true)]`,
`Builder::start_paused`), [timeout.md](timeout.md) (`time::timeout` result and caveats).

## Pause

```rust
pub fn pause();
```

Saves the current `Instant::now()` value; all subsequent `tokio::time::Instant::now()` calls
return the saved value until `advance` changes it or the runtime auto-advances. Only
**`tokio::time::Instant`** is affected — `std::time::Instant` continues to use the system
clock.

`pause()` panics if time is already frozen or if called outside a `current_thread` Tokio
runtime.

Prefer pausing at runtime construction rather than calling `pause()` mid-test:

```rust
#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() { /* ... */ }
```

or:

```rust
#[tokio::test(start_paused = true)]
async fn test_something() { /* ... */ }
```

## Auto-advance

When time is paused and the runtime has no work to do, the clock **auto-advances to the next
pending timer**. Awaiting `Sleep` or any timer-backed primitive therefore completes without
real waiting, and timers fire in deadline order.

Auto-advance is temporarily inhibited while a `spawn_blocking` task is running. This lets a
test wait for real (blocking) I/O while keeping the paused clock stationary:

```rust
use tokio::time::{Duration, Instant};
use tokio::task;

#[tokio::test(start_paused = true)]
async fn test_with_io() {
    let start = Instant::now();
    let result = task::spawn_blocking(|| {
        std::thread::sleep(std::time::Duration::from_millis(10));
        42
    }).await.unwrap();
    assert_eq!(start.elapsed(), Duration::ZERO);
}
```

## Advance

```rust
pub async fn advance(duration: Duration);
```

Adds `duration` to the saved `Instant::now()` value in one jump. All `sleep` calls whose
deadline falls before the new time become ready "at the same time", and the runtime may poll
them in any order. `advance` does **not** wait for those sleeps to complete; it returns after
moving the clock.

- `advance` is for instantly jumping forward, not for reliably triggering a timeout.
- For a reliable timeout, prefer `sleep(duration).await` and let auto-advance process timers
  in order.
- Batch semantics make `advance` unsuitable for simulating a freeze-then-resume with ordered
  timer effects.
- A sleep advanced past is not guaranteed to complete on its first poll after `advance` (the
  timer driver may not have been touched yet); if it does not, the runtime polls the task
  again shortly.
- Panics if the clock is not frozen (call `pause` first), if called outside the Tokio
  runtime, or if `duration` would overflow the current time (e.g. `Duration::MAX`).

## Resume

```rust
pub fn resume();
```

Clears the saved `Instant::now()` value; subsequent `Instant::now()` calls return the system
clock. Panics if time is not frozen or if called outside the Tokio runtime.

## Interaction with `tokio::time::timeout`

`timeout` is timer-backed and works deterministically under paused time. When the wrapped
future is pending, the runtime auto-advances to the timeout deadline and returns
`Err(Elapsed)` without real waiting. See [timeout.md](timeout.md) for the poll-before-deadline
caveat and `Timeout::into_inner`.

## Actor-testing notes

- Idle timeouts, heartbeat/interval ticks, and retry/backoff schedules can be tested with
  `start_paused = true`: send a command, `sleep` past the deadline, assert the actor acted.
- `time::advance` is appropriate when several timers must expire together, but assertions
  about their relative order after a jump are unsound.
- A paused clock does not make a busy actor idle; the runtime only auto-advances when there
  is no work. An actor loop that never awaits will hang the test, just as in production.
- Real network or blocking I/O stops auto-advance while `spawn_blocking` runs; keep paused
  tests hermetic and do not depend on wall-clock durations.
- `#[tokio::test]` defaults to current-thread, so `start_paused` needs no flavor change;
  multi-thread tests cannot pause time.
