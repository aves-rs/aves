---
source: docs.rs (official API reference)
library: axum
package: axum
topic: HeaderMap/Json extractors and tuple responses
version: axum 0.8.7
fetched: 2026-11-14T00:00:00Z
official_docs: https://docs.rs/axum/0.8.7/axum/extract/index.html
---

# axum 0.8 handlers: `HeaderMap`, `Json`, tuple responses

Verified against axum 0.8.7 (axum-core 0.5.5).

## Extractor ordering

- Extractors run left to right.
- Exactly one body-consuming extractor is allowed and it must be the **last** argument (`FromRequest`); all earlier arguments must be `FromRequestParts`.
- `HeaderMap`, `Method`, `State`, `Extension`, `Path`, `Query` do not consume the body, so they may appear anywhere before a body extractor.
- `Json` consumes the body → must be last. `Json` and `String`/`Bytes`/`Body` cannot both be used.

```rust
use axum::{extract::State, http::{HeaderMap, Method}, Json};

async fn handler(
    method: Method,
    headers: HeaderMap,          // FromRequestParts; infallible
    State(state): State<AppState>,
    Json(payload): Json<Payload>, // must be last
) { /* ... */ }
```

`HeaderMap` is `axum::http::header::HeaderMap` (i.e. `http::HeaderMap`); as an extractor it clones the request's header map (infallible `FromRequestParts`). To read a single header, use `headers.get(header::AUTHORIZATION)`.

## `Json`

- `pub struct Json<T>(pub T);` — feature `json`.
- Extractor: requires `T: DeserializeOwned`; rejects with `JsonRejection` on missing/incorrect `Content-Type: application/json`, invalid JSON, or deserialization failure. Subject to `DefaultBodyLimit` (2 MB default).
- Response: `impl<T: Serialize> IntoResponse for Json<T>` sets `Content-Type: application/json`; a failing `Serialize` yields a 500 with the error text as the body.
- Handle rejections in-handler with `Result<Json<T>, JsonRejection>`.

## Tuple responses (any element order constraints)

`IntoResponse` is implemented for tuples of these shapes:

- `(StatusCode, impl IntoResponse)`
- `(Parts, impl IntoResponse)`
- `(Response<()>, impl IntoResponse)`
- `(T1, .., Tn, impl IntoResponse)` where each `Ti: IntoResponseParts`
- `(StatusCode, T1, .., Tn, impl IntoResponse)` where each `Ti: IntoResponseParts`
- `(Parts, T1, .., Tn, impl IntoResponse)`
- `(Response<()>, T1, .., Tn, impl IntoResponse)`

`IntoResponseParts` is implemented for `StatusCode`, `HeaderMap`, `Extension<_>`, arrays/iterators of `(HeaderName, value)` pairs, and `Parts`. Consequence: you cannot override the status or body via a "parts" element — parts only set headers/extensions. The last element is always the body.

```rust
// status + headers + JSON body
async fn ok() -> (StatusCode, HeaderMap, Json<Payload>) {
    let mut headers = HeaderMap::new();
    headers.insert("x-request-id", "abc".parse().unwrap());
    (StatusCode::OK, headers, Json(payload))
}

// status + header array + JSON body
async fn ok2() -> (StatusCode, [(&'static str, &'static str); 1], Json<Payload>) {
    (StatusCode::CREATED, [("x-trace", "1")], Json(payload))
}

// returning two different response types requires .into_response()
async fn branch(flag: bool) -> axum::response::Response {
    if flag { "ok".into_response() } else { (StatusCode::NOT_FOUND, "no").into_response() }
}
```

Notes:

- `HeaderMap` alone is a valid response (headers-only, empty body); `(HeaderMap, impl IntoResponse)` adds headers to that body.
- Prefer concrete return types or `Response`; `impl IntoResponse` breaks with `?` unless the error type is concrete (`Result<impl IntoResponse, StatusCode>`).
- `Json` as a *response* element has no ordering restriction — ordering rules apply only to extractor parameters.
