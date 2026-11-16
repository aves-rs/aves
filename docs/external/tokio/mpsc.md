---
source: docs.rs (official rustdoc)
library: tokio
package: tokio
version: 1.54.1
topic: tokio::sync::mpsc
fetched: 2026-11-15
official_docs: https://docs.rs/tokio/1.54.1/tokio/sync/mpsc/index.html
---

# `tokio::sync::mpsc`

Available on crate feature **`sync`** only.

Multi-producer, single-consumer queue. The bounded variant (`channel(capacity)`) applies
backpressure: once the buffer holds `capacity` values, `send` waits until a value is received.
The unbounded variant (`unbounded_channel()`) never waits, so its `send` is synchronous and
callable from non-async code.

The channel is runtime agnostic (moves freely between runtimes and works from non-Tokio
runtimes). Under a Tokio runtime it participates in cooperative scheduling, so a tight
`recv` loop yields. Methods ending in `_timeout` are the exception — they need the Tokio timer.

## Disconnection and clean shutdown

- All `Sender` handles dropped → the channel is closed; after buffered values are drained,
  `recv` returns `None`.
- `Receiver` dropped → all sends fail and unread messages are drained and dropped.
- **Clean shutdown**: `Receiver::close()` prevents further sends while still allowing the
  receiver to drain the buffer, then `recv().await` until `None` before dropping. Outstanding
  `Permit`/`OwnedPermit` values may still send, and `recv` will not return `None` until they
  are released — a leaked permit keeps the channel "open" for drain purposes.
- `WeakSender` (via `Sender::downgrade`) does not count toward keeping the channel open;
  use it so a retained handle cannot prevent channel closure.

## `Sender`

```rust
pub async fn send(&self, value: T) -> Result<(), SendError<T>>;
pub async fn send_timeout(&self, value: T, timeout: Duration) -> Result<(), SendTimeoutError<T>>;
pub fn try_send(&self, message: T) -> Result<(), TrySendError<T>>;
pub fn blocking_send(&self, value: T) -> Result<(), SendError<T>>;
pub async fn closed(&self);
pub fn is_closed(&self) -> bool;

pub async fn reserve(&self) -> Result<Permit<'_, T>, SendError<()>>;
pub async fn reserve_many(&self, n: usize) -> Result<PermitIterator<'_, T>, SendError<()>>;
pub async fn reserve_owned(self) -> Result<OwnedPermit<T>, SendError<()>>;
pub fn try_reserve(&self) -> Result<Permit<'_, T>, TrySendError<()>>;
pub fn try_reserve_many(&self, n: usize) -> Result<PermitIterator<'_, T>, TrySendError<()>>;
pub fn try_reserve_owned(self) -> Result<OwnedPermit<T>, TrySendError<Self>>;

pub fn capacity(&self) -> usize;
pub fn max_capacity(&self) -> usize;
pub fn strong_count(&self) -> usize;
pub fn weak_count(&self) -> usize;
pub fn same_channel(&self, other: &Self) -> bool;
pub fn downgrade(&self) -> WeakSender<T>;
```

- `send` errors only on a closed/closing receiver; the value is returned in the error. `Ok(())`
  does **not** guarantee receipt — the receiver may close immediately after.
- `try_send` has two failure modes: `TrySendError::Full(T)` (buffer at capacity) and
  `TrySendError::Closed(T)`.
- `try_reserve_owned` takes the sender by value, so on failure the `TrySendError` carries the
  sender back and it can be reused.
- `send_timeout` panics outside a Tokio runtime with time enabled.
- `blocking_send` panics inside an async context.
- `capacity()` is the *current* free capacity (reservations lower it); `max_capacity()` is the
  value passed to `channel()` and never changes.
- `closed().await` resolves when the receiver is dropped or closed; it is cancel safe and
  permanently true thereafter.

### Cancel safety of sending

If `send` is a `select!` branch and another branch completes first, the message was **not**
sent — **but it is dropped and lost**. The channel queues `send`/`reserve` callers to preserve
order, so cancelling either one loses the caller's place in the queue.

To avoid losing a message, `reserve` capacity first and send through the returned `Permit`.

## `OwnedPermit` and `Permit`

```rust
impl<T> OwnedPermit<T> {
    pub fn send(self, value: T) -> Sender<T>;
    pub fn release(self) -> Sender<T>;
    pub fn same_channel(&self, other: &Self) -> bool;
    pub fn same_channel_as_sender(&self, sender: &Sender<T>) -> bool;
}
```

- An owned permit reserves one slot in advance and is `'static`, so it can be moved into a
  spawned task. `send` consumes it and returns the `Sender`; `release` returns the sender
  without sending.
- `OwnedPermit::send` **succeeds even if the receiver half has been closed** (the reserved
  slot exists); `Sender::send` would fail there.
- Dropping a `Permit`/`OwnedPermit` without sending releases the capacity back to the channel.
- `reserve_owned`/`try_reserve_owned` move the sender by value; clone it first when several
  owned permits or continued use of `tx` are needed (`Sender::clone` is a refcount bump).

## `Receiver`

```rust
pub async fn recv(&mut self) -> Option<T>;
pub async fn recv_many(&mut self, buffer: &mut Vec<T>, limit: usize) -> usize;
pub fn try_recv(&mut self) -> Result<T, TryRecvError>;
pub fn blocking_recv(&mut self) -> Option<T>;
pub fn blocking_recv_many(&mut self, buffer: &mut Vec<T>, limit: usize) -> usize;
pub fn close(&mut self);
pub fn is_closed(&self) -> bool;
pub fn is_empty(&self) -> bool;
pub fn len(&self) -> usize;
pub fn capacity(&self) -> usize;
pub fn max_capacity(&self) -> usize;
pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>>;
pub fn poll_recv_many(&mut self, cx: &mut Context<'_>, buffer: &mut Vec<T>, limit: usize) -> Poll<usize>;
pub fn sender_strong_count(&self) -> usize;
pub fn sender_weak_count(&self) -> usize;
```

- `recv` returns `None` only when closed **and** drained. It is **cancel safe** — a losing
  `select!` branch receives nothing.
- `recv_many` extends `buffer` by at most `limit` and returns the count; `limit == 0` returns
  `0` immediately; for `limit > 0` it never returns `0` unless closed and drained. Cancelling
  a non-zero-limit call is cancel safe.
- `try_recv` returns `TryRecvError::Empty` only when messages may still arrive (senders or
  permits outstanding) and `Disconnected` when they cannot; unlike `poll_recv` it never
  reports a spurious `Empty`.
- `blocking_recv` panics inside an async context. `poll_recv` may return `Pending` spuriously.
- `is_empty`/`len` are observation only; do not build logic that assumes they stay stable
  across an await.

## Supervision-relevant caveats

- **Completion reports**: a supervised task that reports through `send` can silently lose its
  message if that `send` is inside a `select!`. Reserve a slot (`reserve_owned`) *before*
  spawning, move the `OwnedPermit` into the task, and have it `permit.send(report)` — that path
  also succeeds after the supervisor has closed the receiver, so a late exit is still observable.
- **Admission vs backpressure**: `try_reserve_owned()` gives a non-blocking decision —
  `Full` means the supervisor is saturated, `Closed` means shutdown started. `try_send` merges
  both cases into one error type that still distinguishes them.
- **Shutdown ordering**: call `rx.close()` first (stops new sends, keeps drain working), then
  `while let Some(msg) = rx.recv().await {}` to observe in-flight completions, then drop.
  Skipping `close()` and dropping the receiver discards buffered messages.
- **Channel-never-closes bug**: every clone of `Sender` retained in a supervising struct keeps
  the channel open forever; `recv` never returns `None`. Keep exactly one sender where
  possible, or hold `WeakSender`.
- `Sender::closed()` in the supervisor loop and `Receiver::recv() == None` are two views of the
  same event, from opposite ends.

## Example: bounded admission with an owned permit

```rust
use tokio::sync::mpsc;

let (tx, mut rx) = mpsc::channel::<String>(4);

let permit = tx.clone().reserve_owned().await.unwrap();
tokio::spawn(async move {
    let report = "exit: 0".to_string();
    permit.send(report); // cannot be lost, cannot block
});

if let Some(msg) = rx.recv().await {
    println!("{msg}");
}
```
