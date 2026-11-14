---
source: docs.rs (official rustdoc)
library: tokio
package: tokio
version: 1.54.1
topic: #[tokio::test] attribute macro
fetched: 2026-11-14
official_docs: https://docs.rs/tokio/latest/tokio/attr.test.html
---

# `#[tokio::test]` attribute macro

Checked against docs.rs tokio **1.54.1** (published 2026-10-21). Requires crate features
**`macros` and `rt`**; the multi-thread flavor additionally requires `rt-multi-thread`,
and `start_paused` requires `test-util`. The pinned version in `Cargo.lock` is authoritative.

Marks an `async fn` to be executed by a runtime, suitable for a test environment. It sets
up a `Runtime` without the user calling `Runtime` or `Builder` directly. It is deliberately
simplistic; for complex setup use `Builder` directly.

## Default: current-thread runtime

The default test runtime is single-threaded. Each test gets a separate current-thread runtime.

```rust
#[tokio::test]
async fn my_test() {
    assert!(true);
}
```

Equivalent hand-written form:

```rust
#[test]
fn my_test() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            assert!(true);
        })
}
```

## Multi-threaded runtime

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn my_test() {
    assert!(true);
}
```

- `worker_threads` configures the number of worker threads; defaults to the number of CPUs.
- The multi-threaded runtime requires the `rt-multi-thread` feature flag.

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn my_test() { /* ... */ }
```

## Options

### `name` — set the runtime name

```rust
#[tokio::test(name = "my-test-runtime")]
async fn my_test() { /* ... */ }
```

### `start_paused` — start with time paused

```rust
#[tokio::test(start_paused = true)]
async fn my_test() { /* ... */ }
```

Requires the **`test-util`** feature. Paused time makes `tokio::time` advance only when the
runtime is idle, so `sleep`/`timeout` tests run instantly and deterministically.

### `crate` — rename the tokio package

```rust
use tokio as tokio1;

#[tokio1::test(crate = "tokio1")]
async fn my_test() {
    println!("Hello world");
}
```

### `unhandled_panic` — runtime behavior on unhandled panic

Available values are `shutdown_runtime` and `ignore` (see `Builder::unhandled_panic`).
Only compatible with the `current_thread` runtime; requires `tokio_unstable`.

```rust
#[cfg(tokio_unstable)]
#[tokio::test(flavor = "current_thread", unhandled_panic = "shutdown_runtime")]
async fn my_test() {
    let _ = tokio::spawn(async { panic!("This panic will shutdown the runtime."); }).await;
}
```

## Practical notes

- The macro implies `#[test]`; do not add `#[test]` alongside it.
- `flavor` accepts `"current_thread"` (default) and `"multi_thread"`.
- Tests needing a paused clock for timeout assertions should use
  `#[tokio::test(start_paused = true)]` with `test-util` enabled in dev-dependencies.
- The macro-generated runtime calls `enable_all()`, so timers and I/O work without extra setup.
