# External Documentation

Third-party library documentation with versioned official source URLs. Verify freshness before reuse; pinned dependency versions in Cargo.lock are authoritative. External docs do not cover all provider auth APIs — see versioned source links in [Tyto Connection Architecture](../architecture/tyto/connection.md) for Slack and Telegram auth details.

- [async-trait/async-trait.md](async-trait/async-trait.md) — `#[async_trait]` macro: dyn-compatible async trait methods, `Pin<Box<dyn Future + Send>>` expansion, `?Send` mode, lifetime-elision rules
- [axum/body-from-stream.md](axum/body-from-stream.md) — axum 0.8.7 `Body::from_stream` bounds and use with `tokio_util::io::ReaderStream` (`Result<Bytes, io::Error>`, 4 KiB default chunks, no Content-Length)
- [axum/handlers-extractors-responses.md](axum/handlers-extractors-responses.md) — axum 0.8.7 `HeaderMap`/`Json` extractor ordering, `Json` rejection/limits, and tuple `IntoResponse` forms incl. `(StatusCode, HeaderMap, Json<T>)`
- [axum/serve-router-ephemeral-listener.md](axum/serve-router-ephemeral-listener.md) — axum 0.8 `serve` with a Router, OS-assigned ephemeral port via `TcpListener::bind("127.0.0.1:0")` + `local_addr`, graceful shutdown, and bounding the never-resolving serve future with `tokio::time::timeout`
- [lettre/test_connection.md](lettre/test_connection.md) — AsyncSmtpTransport::test_connection (lettre 0.11, authentication during connection establishment, NOOP, return type, errors)
- [ollama/systemone-decision-api.md](ollama/systemone-decision-api.md) — Ollama `POST /v1/systemone` (System One — Clef/Clef Flash): request/answer schemas for choice/noul/score, local vs cloud base URLs, auth, limits, errors, and differences from TypeSafe Jev (Ollama v0.35.0+; OpenAPI 0.1.0)
- [reqwest/redirect-timeouts-streaming.md](reqwest/redirect-timeouts-streaming.md) — reqwest 0.13 `redirect::Policy` (default 10 hops, `none`/`limited`/`custom`), `Response::chunk`/`bytes_stream`/`content_length`, and total vs read vs connect vs per-request timeouts
- [serde_json/from-value-to-value.md](serde_json/from-value-to-value.md) — serde_json 1.0 `from_value` (`DeserializeOwned`) / `to_value` (`Serialize`) signatures, error conditions (structure mismatch, non-string map keys), and round-trip caveats
- [serde_json/json-macro-borrowing.md](serde_json/json-macro-borrowing.md) — serde_json 1.0.154 `json!`: values interpolated via `to_value(&expr)` (borrowed), object keys consumed via `.into()` (`Into<String>`), panic conditions
- [slack-morphism/slackclienthttpconnector-mock.md](slack-morphism/slackclienthttpconnector-mock.md) — SlackClientHttpConnector trait + base URI for mock impls (slack-morphism 2.x)
- [tokio/cancellation.md](tokio/cancellation.md) — Task cancellation: dropping futures, `JoinHandle::abort`/`abort_handle`/`is_finished`, detach-on-drop, `spawn_blocking` non-abortability, and `tokio_util::sync::CancellationToken` (tokio 1.54.1, tokio-util 0.7.20)
- [tokio/oneshot.md](tokio/oneshot.md) — `tokio::sync::oneshot` channel: `Sender::send`/`closed`, `Receiver` as `Future`, `try_recv`/`close`/`blocking_recv`, `RecvError` vs `TryRecvError`, cancel safety
- [tokio/paused-time-testing.md](tokio/paused-time-testing.md) — Paused time: `pause`/`advance`/`resume`, `start_paused`, idle auto-advance, `spawn_blocking` inhibition, `advance` batching caveats
- [tokio/select.md](tokio/select.md) — `tokio::select!`: branch/`else`/`biased` syntax, lifecycle, current-task concurrency, fairness, panics, and the cancel-safe vs not-cancel-safe method lists
- [tokio/semaphore.md](tokio/semaphore.md) — `tokio::sync::Semaphore`: acquire/`_owned`/`_many`/`try_*`, fairness, permits, `close`, blocking variants, queue-position cancel safety
- [tokio/test-attribute.md](tokio/test-attribute.md) — `#[tokio::test]` (tokio 1.54, features `macros`+`rt`): default current-thread runtime, `flavor`/`worker_threads`, `start_paused` (`test-util`), `name`, `crate`, `unhandled_panic`
- [tokio/timeout.md](tokio/timeout.md) — `tokio::time::timeout` result type, drop-based cancellation, `into_inner`, poll-before-deadline caveat, timer panic requirements
- [typesafe-jev/decision-api.md](typesafe-jev/decision-api.md) — Jev/TypeSafe System One API: question and answer schemas, confidence formulas, image transport, models, errors (API 0.2.0, Jev 1.14/1.13)
