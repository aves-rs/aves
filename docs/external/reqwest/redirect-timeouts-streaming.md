---
source: docs.rs (official rustdoc)
library: reqwest
package: reqwest
version: 0.13.5
topic: redirect policy, response chunk streaming, client/request timeouts
fetched: 2026-11-14
official_docs: https://docs.rs/reqwest/0.13.5/reqwest/
---

# reqwest: redirect policy, chunked responses, and timeouts

Source pages:
- https://docs.rs/reqwest/0.13.5/reqwest/redirect/struct.Policy.html
- https://docs.rs/reqwest/0.13.5/reqwest/struct.ClientBuilder.html
- https://docs.rs/reqwest/0.13.5/reqwest/struct.Response.html
- https://docs.rs/reqwest/0.13.5/reqwest/struct.RequestBuilder.html

## Redirect policy (`reqwest::redirect::Policy`)

`Policy` is a struct with private fields (not an enum). Default behavior: catches redirect
loops and follows a maximum of **10** redirects in a chain before returning an error.

| Constructor | Behavior |
|---|---|
| `Policy::default()` | Same as the default: loop detection, max 10 hops |
| `Policy::limited(max)` | Same behavior with an adjusted maximum; an `Error` is returned if the max is reached |
| `Policy::none()` | Disables all redirect following |
| `Policy::custom(f)` | Custom policy from `Fn(Attempt<'_>) -> Action + Send + Sync + 'static` |

`ClientBuilder::redirect(policy)` sets the policy; the default follows redirects up to a
maximum of 10.

Docs note on `custom`: the default policy handles a maximum loop chain, but a custom
variant does **not** do that automatically — the custom policy must handle loops itself.

`Attempt` exposes:
- `status() -> StatusCode` — type of redirect
- `url() -> &Url` — the next URL to redirect to
- `previous() -> &[Url]` — URLs already requested in this chain
- `follow() -> Action` — follow the next URL
- `stop() -> Action` — do not follow; the 30x response is returned as the `Ok` result
- `error(e) -> Action` — fail the request with the given error

`Policy::redirect(&self, attempt)` applies a policy to an attempt, which lets a custom
policy wrap another (e.g. log, then delegate to the default).

```rust
use reqwest::redirect;

// No redirects at all (typical for a locked-down internal client).
let client = reqwest::Client::builder()
    .redirect(redirect::Policy::none())
    .build()?;

// Bounded hops with an allowlist on the destination host.
let custom = redirect::Policy::custom(|attempt| {
    if attempt.previous().len() > 5 {
        attempt.error("too many redirects")
    } else if attempt.url().host_str() == Some("example.domain") {
        attempt.stop()
    } else {
        attempt.follow()
    }
});
let client = reqwest::Client::builder().redirect(custom).build()?;
```

For a bounded client, choose `Policy::none()` when the caller controls URLs, or
`Policy::custom` when redirects must be allowed but destination hosts and hop counts
must stay constrained. `stop()` returns the 3xx response as success, so callers that
require a final 2xx must check `status()` themselves.

## Response body streaming

- `Response::chunk(&mut self) -> Result<Option<Bytes>>` — streams a chunk of the body;
  returns `None` when the body is exhausted. Each call awaits the next chunk, so the
  consumer naturally applies backpressure per chunk.
- `Response::bytes_stream(self) -> impl Stream<Item = Result<Bytes>>` — converts the body
  into a stream (`stream` feature required).
- `Response::content_length() -> Option<u64>` — size of the response body when known. It
  is not necessarily the `Content-Length` header value (read the header via
  `Response::headers()` if that is what you need). It may be unknown when the response
  has no body (e.g. a `HEAD` response) or when the response is gzip-compressed and
  automatically decoded, since decoding changes the decoded length.
- `bytes()` / `text()` / `json()` consume and buffer the entire body — unsuitable for
  untrusted or unbounded remote input.

```rust
let mut res = reqwest::get("https://hyper.rs").await?;

while let Some(chunk) = res.chunk().await? {
    println!("Chunk: {chunk:?}");
}
```

```rust
use futures_util::StreamExt;

let mut stream = reqwest::get("http://httpbin.org/ip").await?.bytes_stream();

while let Some(item) = stream.next().await {
    println!("Chunk: {:?}", item?);
}
```

Bounded accumulation pattern (cap enforced while streaming, not after buffering):

```rust
let mut res = client.get(url).send().await?;
let mut body = Vec::new();

while let Some(chunk) = res.chunk().await? {
    if body.len() + chunk.len() > MAX_BODY_BYTES {
        return Err(anyhow!("response body exceeds {MAX_BODY_BYTES} bytes"));
    }
    body.extend_from_slice(&chunk);
}
```

Do not rely on `content_length()` alone as an admission check: it is `Option<u64>` and is
absent for chunked/decoded responses. Enforce the cap on bytes actually received.

## Timeouts

| Method | Scope | Default |
|---|---|---|
| `ClientBuilder::timeout(Duration)` | Total request timeout, applied from when the request starts connecting until the response body has finished (a total deadline) | No timeout |
| `ClientBuilder::read_timeout(Duration)` | Each read operation; resets after a successful read; appropriate for detecting stalled connections when the size is not known beforehand | No timeout |
| `ClientBuilder::connect_timeout(Duration)` | Connect phase only; requires execution in a tokio runtime with a tokio timer enabled | `None` |
| `ClientBuilder::pool_idle_timeout(D)` | Idle keep-alive sockets; pass `None` to disable | 90 seconds |
| `RequestBuilder::timeout(Duration)` | Per-request total timeout, same window (connect start until body finished); overrides `ClientBuilder::timeout()` for that request | Inherits client setting |

Because the total timeout covers the response body, it also bounds a slow chunked stream;
`read_timeout` is the narrower control for per-read stalls. Per-request timeout is the
right lever when one call needs a tighter bound than the shared client.

```rust
use std::time::Duration;

let client = reqwest::Client::builder()
    .connect_timeout(Duration::from_secs(3))
    .timeout(Duration::from_secs(15))       // includes reading the body
    .read_timeout(Duration::from_secs(5))   // per-read stall detection
    .redirect(reqwest::redirect::Policy::none())
    .build()?;

let res = client
    .get(url)
    .timeout(Duration::from_secs(8))        // tighter bound for this call only
    .send()
    .await?;
```
