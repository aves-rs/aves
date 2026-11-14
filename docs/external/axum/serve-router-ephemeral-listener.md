---
source: docs.rs (official rustdoc)
library: axum
package: axum
version: 0.8.9
topic: serve a Router on an ephemeral listener, graceful shutdown, bounding the server future
fetched: 2026-11-14
official_docs: https://docs.rs/axum/0.8.9/axum/serve/
---

# axum 0.8: serving a Router on an ephemeral listener

Source pages:
- https://docs.rs/axum/0.8.9/axum/serve/fn.serve.html
- https://docs.rs/axum/0.8.9/axum/serve/struct.Serve.html
- https://docs.rs/tokio/1.54.1/tokio/net/struct.TcpListener.html

## `axum::serve`

```rust
pub fn serve<L, M, S>(listener: L, make_service: M) -> Serve<L, M, S>
where
    L: Listener,
    M: for<'a> Service<IncomingStream<'a, L>, Error = Infallible, Response = S>,
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send,
```

- Available only with crate feature `tokio` and (`http1` or `http2`).
- Supports both HTTP/1 and HTTP/2.
- Deliberately simple with no configuration; the docs direct you to hyper or hyper-util if
  configuration is needed.
- A `Router` satisfies the make-service bound directly, so the common call is:

```rust
use axum::{Router, routing::get};

let router = Router::new().route("/", get(|| async { "Hello, World!" }));

let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
axum::serve(listener, router).await.unwrap();
```

`MethodRouter` and `Handler` (via `into_make_service()`) can be passed the same way.

### Return value

The `Serve` future resolves to `io::Result<()>` but, per the docs, it will **never
actually complete or return an error**: TCP socket errors are handled by sleeping for a
short while (currently one second). The server therefore ends only by being dropped,
cancelled, or stopped through graceful shutdown.

### `Serve` methods

- `local_addr(&self) -> io::Result<L::Addr>` — local address the server is bound to.
- `with_graceful_shutdown(signal)` where `signal: Future<Output = ()> + Send + 'static` —
  returns `WithGracefulShutdown`. It resolves `Ok(())` only after the signal future
  completes; like `serve`, it does not error.

```rust
let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

axum::serve(listener, router)
    .with_graceful_shutdown(shutdown_signal())
    .await
    .unwrap();
```

## Ephemeral / OS-assigned port

Axum does not allocate ports; the `Listener` is a `tokio::net::TcpListener` (or another
`Listener` implementor). Tokio's docs state: binding with port number **0** requests that
the OS assign a port, and the allocated port can be queried via `local_addr()`.

```rust
use axum::{Router, routing::get};

let app = Router::new().route("/health", get(|| async { "ok" }));

let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
let addr = listener.local_addr()?;   // actual bound SocketAddr, e.g. 127.0.0.1:54321
println!("listening on http://{addr}");

// Hand the address to the test/client before the server future is awaited.
tokio::spawn(async move {
    axum::serve(listener, app).await.unwrap();
});
```

`Serve::local_addr()` is also available, but it requires the `Serve` value to exist
(before awaiting it), which is why binding the `TcpListener` yourself and reading
`listener.local_addr()` is the simpler pattern when the port must be published first.

For an ephemeral internal service, bind loopback (`127.0.0.1:0`) rather than
`0.0.0.0:0`; binding the wildcard address exposes the port on every interface.

## Bounding the server with a timeout

`axum::serve(...)` never resolves on its own, so `tokio::time::timeout` around it is a
hard upper bound: on elapse the future is dropped, which cancels the server and closes
the listener. Because dropping cancels in-flight work, prefer graceful shutdown when
in-flight requests must finish, and use the timeout as a backstop.

```rust
use std::time::Duration;

tokio::time::timeout(
    Duration::from_secs(30),
    axum::serve(listener, app),
)
.await
.ok(); // Err(Elapsed) if the bound was hit
```

Notes from tokio's `timeout` docs that matter here:
- The future is polled before the timeout is checked; a future that never yields can
  complete and exceed the timeout without returning an error.
- Cancellation is performed by dropping the future; no additional cleanup is required.
