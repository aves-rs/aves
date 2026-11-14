---
source: docs.rs (official rustdoc)
library: tokio
package: tokio
version: 1.54.1
topic: tokio::select! macro
fetched: 2026-11-14
official_docs: https://docs.rs/tokio/1.54.1/tokio/macro.select.html
---

# `tokio::select!`

Available on crate feature **`macros`** only. Must be used inside async functions, closures,
or blocks.

## Syntax

```rust
tokio::select! {
    biased;                                  // optional
    <pattern> = <async expr>, if <cond> => <handler>,
    <pattern> = <async expr> => <handler>,
    // ...
    else => <expression>,                    // optional
}
```

The macro runs all `<async expression>`s concurrently on the **current task** and returns when
the **first** branch completes with a value matching its `<pattern>`, cancelling the rest.
The handler may be a block or any expression.

## Evaluation lifecycle (verbatim semantics)

1. Evaluate every `if <cond>` precondition. If `false`, disable that branch for the remainder
   of this `select!` call. Re-entering `select!` (for example in a loop) clears disabled state.
2. Aggregate the async expressions from all branches, including disabled ones. A disabled
   branch's async expression is still evaluated, but its future is never polled.
3. If **all** branches are disabled, go to step 6.
4. Concurrently await the remaining async expressions.
5. When one returns a value, try to match its `<pattern>`. On match, evaluate the handler and
   return. On mismatch, disable that branch for the rest of this call and go back to step 3.
6. Evaluate the `else` expression. **If no `else` is provided, panic.**

## Runtime characteristics

- Branches run **concurrently but not in parallel** — all on the same thread/task. A branch
  that blocks the thread prevents every other branch from progressing.
- For parallelism, `tokio::spawn` the work and select on the join handles.

## Fairness

By default `select!` randomly picks a branch to check first, providing fairness when looping
over always-ready branches. `biased;` polls futures top-to-bottom instead. Biased mode removes
the RNG cost and makes polling order deterministic, but fairness becomes the caller's
responsibility: in a loop selecting a busy channel and a shutdown future, put the shutdown
future first.

## Panics

Panics when all branches are disabled **and** no `else` branch exists. A branch is disabled
when its `if` precondition is `false` or when its pattern does not match the returned value.

## Cancellation safety

`select!` cancels losing branches by dropping their futures. Cancellation safety means: if a
future has not completed, dropping it and recreating it must be a no-op; otherwise progress is
lost on every loop iteration.

Cancellation-safe methods (per the docs):

- `tokio::sync::mpsc::Receiver::recv` / `UnboundedReceiver::recv`
- `tokio::sync::broadcast::Receiver::recv`
- `tokio::sync::watch::Receiver::changed`
- `tokio::net::TcpListener::accept` / `UnixListener::accept`
- `tokio::signal::unix::Signal::recv`
- `tokio::io::AsyncReadExt::read` / `read_buf`, `AsyncWriteExt::write` / `write_buf`
- `tokio_stream::StreamExt::next` / `futures::stream::StreamExt::next`
- `tokio::sync::oneshot::Receiver` when awaited via `&mut`
- `tokio::task::JoinHandle` when awaited via `&mut` (task output is not lost)

**Not** cancellation safe (data loss possible):

- `AsyncReadExt::read_exact`, `read_to_end`, `read_to_string`
- `AsyncWriteExt::write_all`

**Not** cancellation safe because of fair queuing (losing branch loses its place):

- `tokio::sync::Mutex::lock`
- `tokio::sync::RwLock::read` / `write`
- `tokio::sync::Semaphore::acquire`
- `tokio::sync::Notify::notified`

A custom future is cancel safe if restarting it after an `.await` is harmless. Cancellation
always happens at an `.await`. Cancelling something that is not cancel safe is not
necessarily wrong — for example during application shutdown, losing partial data may be
acceptable.

## Usage patterns

- Reusing the same future across `select!` calls requires it to be `Unpin`; use `Box::pin` or
  `tokio::pin!`. The docs example pins a `sleep` and selects on `&mut sleep`.
- `&mut rx` is required for `oneshot` receivers and other `Future` handles that are polled
  repeatedly.
- Precondition race to avoid: `if !sleep.is_elapsed()` can be true at the check and false by
  the time `select!` polls, so the timeout branch is skipped. Poll the timer as a normal
  branch and `break` in its handler instead.

## Actor-relevant caveats

- A typical actor loop is `loop { select! { cmd = rx.recv() => ..., _ = shutdown.cancelled() => ... } }`.
  Keep only cancel-safe receive branches; `mpsc::Receiver::recv` and `CancellationToken::cancelled`
  qualify.
- State mutation should happen in the winning branch's handler, after the other branches have
  been dropped; do not hold a `MutexGuard` across the `select!` await.
- When shutdown must win over a saturated mailbox, use `biased;` with the shutdown branch
  first, or verify fairness another way.

## Ecosystem alternatives

For merging streams, `tokio_stream` / `futures::stream` / `futures-concurrency` provide
cancellation-safe merging (`StreamExt::merge`, `select_all`). For racing futures,
`futures_concurrency::future::Race`, `futures::select!`, `futures_lite::future::race`/`or`
are alternatives that reduce manual cancel-safety handling.
