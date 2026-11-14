---
source: docs.rs (official rustdoc)
library: tokio
package: tokio
version: 1.54.1
topic: tokio::sync::oneshot
fetched: 2026-11-14
official_docs: https://docs.rs/tokio/1.54.1/tokio/sync/oneshot/index.html
---

# `tokio::sync::oneshot`

Available on crate feature **`sync`** only.

A one-shot channel sends a single message between asynchronous tasks.
`oneshot::channel()` creates a `(Sender<T>, Receiver<T>)` pair. Each handle can live on a
separate task.

Because `Sender::send` is **not async**, it can be used anywhere, including between two
runtimes and from non-async code. A message sent before the receiver is closed remains in
the channel until the receiver is dropped, at which point it is dropped immediately.

## Channel and sender

```rust
pub fn channel<T>() -> (Sender<T>, Receiver<T>);

impl<T> Sender<T> {
    pub fn send(self, t: T) -> Result<(), T>;
    pub async fn closed(&mut self);
    pub fn is_closed(&self) -> bool;
    pub fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<()>;
}
```

- `send` consumes `self` because only one value may ever be sent. `Err(value)` means the
  receiver was already dropped; the value is handed back. `Ok(())` does **not** guarantee the
  receiver will observe the value — it may drop immediately afterward.
- `closed().await` resolves when the receiver is dropped or `Receiver::close` is called.
  Pair it with a computation in `select!` to abandon work when the requester is gone.
- `is_closed` is true once the receiver is dropped or closed; a subsequent `send` always
  fails.
- To send from a destructor, store the `Sender` in an `Option` and `Option::take` it in
  `Drop`.

## Receiver

The receiver has no `recv` method; it implements `Future` directly.

```rust
impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;
}

impl<T> Receiver<T> {
    pub fn close(&mut self);
    pub fn try_recv(&mut self) -> Result<T, TryRecvError>;
    pub fn blocking_recv(self) -> Result<T, RecvError>;
    pub fn is_empty(&self) -> bool;
    pub fn is_terminated(&self) -> bool;
}
```

- `.await` yields `Ok(T)` or `Err(RecvError)` when the sender was dropped without sending.
- `close` prevents the sender from sending; sends already completed **before** `close` can
  still be retrieved with `try_recv`.
- `try_recv` returns `Ok(T)`, `Err(TryRecvError::Empty)`, or `Err(TryRecvError::Closed)`
  (sender dropped without sending, or the value was already received). It does not register
  a waker.
- `blocking_recv` is for synchronous code and **panics inside an async context**.
- `is_empty` is not a safe pollability check; use `is_terminated` to know whether polling
  again is valid.
- `poll` may spuriously return `Pending` even after a send; the task is woken again, and the
  message is not lost.

## Cancel safety

Awaiting `&mut Receiver<T>` is **cancel safe**. If it is a `select!` branch and another
branch wins, no message was consumed. To use a receiver repeatedly in `select!`, write
`&mut rx` in the branch.

## Actor-relevant caveats

- Standard request/response mailbox shape: the caller creates `oneshot::channel()`, sends
  `(payload, reply_tx)` to the actor mailbox, and awaits `reply_rx`. If the actor is gone,
  the caller gets `RecvError`; map that to a typed "actor unavailable" error rather than
  unwrapping.
- The actor should treat `send` failure (`Err(value)`) as "requester cancelled" and usually
  discard the reply.
- For a slow computation the actor can `select!` on `tx.closed()` and abandon the work,
  then `send` (which may still race with receiver drop; ignore the `Err`).
- A `oneshot` reply is single-use by design; do not model streaming or multiple replies with
  it.

## Example: reply channel in `select!`

```rust
use tokio::sync::oneshot;
use tokio::time::{interval, Duration};

let (send, mut recv) = oneshot::channel();
let mut interval = interval(Duration::from_millis(100));

tokio::spawn(async move {
    send.send("shut down").unwrap();
});

loop {
    tokio::select! {
        _ = interval.tick() => { /* periodic work */ }
        msg = &mut recv => {
            // msg: Result<&str, RecvError>
            break;
        }
    }
}
```
