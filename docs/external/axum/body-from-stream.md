---
source: docs.rs (official API reference)
library: axum
package: axum
topic: Body::from_stream with tokio_util ReaderStream
version: axum 0.8.7, tokio-util 0.7.20
fetched: 2026-11-14T00:00:00Z
official_docs: https://docs.rs/axum/0.8.7/axum/body/struct.Body.html
---

# `axum::body::Body::from_stream`

Verified against axum 0.8.7 and tokio-util 0.7.20.

## Signatures

```rust
impl Body {
    pub fn from_stream<S>(stream: S) -> Body
    where
        S: TryStream + Send + 'static,
        S::Ok: Into<Bytes>,
        S::Error: Into<Box<dyn Error + Sync + Send>>;
}
```

No feature gate on `Body::from_stream` itself (the `Body` page shows no "Available on crate feature ..." note; `Body` is in `axum-core` 0.5.5).

`tokio_util::io::ReaderStream`:

```rust
// Available on crate feature "io" only.
impl<R: AsyncRead> ReaderStream<R> {
    pub fn new(reader: R) -> Self;                       // item: Result<Bytes, std::io::Error>
    pub fn with_capacity(reader: R, capacity: usize) -> Self;
}
// impl<R: AsyncRead> Stream for ReaderStream<R> { type Item = Result<Bytes, std::io::Error>; }
```

Default read buffer capacity is 4096 bytes (not semver-stable). The stream is fused.

## Compatibility

`ReaderStream<R>` yields `Result<Bytes, std::io::Error>`, which satisfies
`from_stream`:

- `S::Ok = Bytes` → `Into<Bytes>` ✔
- `S::Error = std::io::Error` → `Into<Box<dyn Error + Send + Sync>>` ✔
- `Send + 'static` holds when `R: AsyncRead + Send + 'static` (e.g. `tokio::fs::File`).

So `Body::from_stream(ReaderStream::new(file))` compiles and streams.

## Usage pattern

```rust
use axum::{body::Body, http::{StatusCode, header}, response::Response};
use tokio_util::io::ReaderStream;

async fn download() -> Response {
    let file = tokio::fs::File::open("data.bin").await.unwrap();
    let body = Body::from_stream(ReaderStream::new(file));
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(body)
        .unwrap()
}

// tuple form also works because Body: IntoResponse and HeaderMap: IntoResponseParts
async fn tuple() -> (StatusCode, [(header::HeaderName, &'static str); 1], Body) {
    let file = tokio::fs::File::open("data.bin").await.unwrap();
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/octet-stream")],
        Body::from_stream(ReaderStream::new(file)),
    )
}
```

## Gotchas

- `Content-Length` is unknown for a streamed body; set `Content-Type` (and `Content-Length` if known) yourself. Without `Content-Length` the response is chunked/close-delimited.
- Any `Stream` works, not just `ReaderStream`; an error item terminates the body and surfaces as `axum::Error` (`Body::Error = axum::Error`).
- `tokio::fs::File` needs tokio's `fs` feature; `ReaderStream` needs `tokio-util`'s `io` feature.
- For an in-memory body prefer `Body::from(Bytes/Vec<u8>/String)`; `from_stream` is for incremental I/O.
- `Body::into_data_stream()` is the inverse direction (Body → `BodyDataStream`), discarding trailers.
