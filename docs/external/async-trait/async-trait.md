---
source: docs.rs (official crate docs)
library: async-trait
package: async-trait
topic: async-trait macro semantics
version: async-trait 0.1.92
fetched: 2026-11-14T00:00:00Z
official_docs: https://docs.rs/async-trait/latest/async_trait/
---

# `async-trait` — verified behavior

Verified against async-trait 0.1.92.

## What it is

An attribute macro that makes `async fn` in traits usable with `dyn Trait`. Native async-fn-in-trait (stable since Rust 1.75) is **not** dyn compatible — `Box<dyn Trait>` with `async fn f(&self)` fails with E0038; `#[async_trait]` solves that by type erasure.

## Expansion

```rust
#[async_trait]
impl Advertisement for AutoplayingVideo {
    async fn run(&self) { /* body */ }
}

// expands to:
impl Advertisement for AutoplayingVideo {
    fn run<'async_trait>(
        &'async_trait self,
    ) -> Pin<Box<dyn core::future::Future<Output = ()> + Send + 'async_trait>>
    where
        Self: Sync + 'async_trait,
    {
        Box::pin(async move { /* original body */ })
    }
}
```

Consequences: one heap allocation per call, and `Self: Send`/`Sync` bounds are required by default (futures are `Send`).

## Usage rules

- Put `#[async_trait]` on **both** the trait definition and every impl block.
- Default mode requires the futures to be `Send`: implementors must be `Send`/`Sync` as the bounds demand (the macro emits `Self: Sync + 'async_trait`).
- For non-`Send` futures (e.g. `Rc`, single-threaded executors) use `#[async_trait(?Send)]` on trait and impl alike.
- Supported: self by value/ref/&mut/absent, any arg count and return type, generic and lifetime params, associated types, mixed async/non-async methods, default impls, elided lifetimes.
- `async fn` syntax does not allow lifetime elision except for `&`/`&mut` params: name the lifetime or use `'_` (e.g. `fn f(x: Elided<'_>)`), otherwise E0726.
- No `unsafe` in the expansion.
- Not needed — and should be avoided — for traits used only statically (no `dyn`) on Rust ≥ 1.75; native `async fn` in traits is enough there.

## Relevance to axum 0.8 handlers

axum requires handler futures to be `Send` by default, which matches `#[async_trait]`'s default mode. Do not use `#[async_trait(?Send)]` for trait objects that produce axum handlers or serve futures — the resulting non-`Send` futures will not satisfy `Handler`/`tower::Service` bounds.
