---
source: docs.rs (official API reference + macro source)
library: serde_json
package: serde_json
topic: json! macro borrowing semantics
version: serde_json 1.0.154
fetched: 2026-11-14T00:00:00Z
official_docs: https://docs.rs/serde_json/latest/serde_json/macro.json.html
---

# `serde_json::json!` — borrowing vs moving

Verified against serde_json 1.0.154 (macro source `serde_json/macros.rs`).

## Rule

- **Object/array values are borrowed, not moved.** Interpolated value expressions expand to `$crate::to_value(&$other).unwrap()` (macro rule `($other:expr) => { to_value(&$other).unwrap() }`). `to_value` takes `&T where T: Serialize`, so an existing variable stays usable after the macro. Types must implement `Serialize`.
- **Object keys are converted by value.** Key expansion is `object.insert(($($key)+).into(), $value)`, so a non-literal key expression must implement `Into<String>` and is consumed by `.into()` (a `&str`/`String` variable used as a key is moved/converted; a literal is fine).

```rust
let code = 200;
let features = vec!["serde", "json"];

let v = json!({
    "code": code,            // borrows `code` (&code -> to_value)
    "success": code == 200,  // expression, no binding moved
    "payload": { features[0]: features[1] }, // key: features[0].into(); value: &features[1]
});
// `code` and `features` are still usable here.
```

## Panics

The macro panics if:

- the interpolated type's `Serialize` impl returns an error, or
- the interpolated type contains a map with non-string keys.

This is a runtime panic (`unwrap()` inside the expansion), not a compile error.

## Other facts

- Trailing commas are allowed in both arrays and objects.
- `null`, `true`, `false`, arrays, and objects are parsed at macro-expansion time into `Value::Null` / `Value::Bool` / `Value::Array` / `Value::Object`.
- Because values are borrowed, one owned value can be interpolated into several `json!` calls without cloning; wrap in `&`-friendly expressions or pre-serialize to `Value` if ownership must transfer.
- To move a value in deliberately, use `to_value(v)` (by value) or `.into()`/`Value::from` instead of `json!`.
