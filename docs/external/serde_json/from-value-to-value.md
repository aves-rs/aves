---
source: docs.rs (official rustdoc)
library: serde_json
package: serde_json
version: 1.0.154
topic: serde_json::from_value / serde_json::to_value
fetched: 2026-11-14
official_docs: https://docs.rs/serde_json/latest/serde_json/fn.from_value.html
---

# serde_json::from_value / to_value

Checked against docs.rs serde_json **1.0.154** (published 2026-10-11). The pinned
version in `Cargo.lock` is authoritative; both functions have been stable across the
1.0.x line.

## Signatures

```rust
pub fn from_value<T>(value: Value) -> Result<T, Error>
where
    T: DeserializeOwned;

pub fn to_value<T>(value: T) -> Result<Value, Error>
where
    T: Serialize;
```

`Value` is `serde_json::Value`, the crate's JSON value enum.

## `from_value`

Interpret a `serde_json::Value` as an instance of type `T`.

```rust
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize, Debug)]
struct User {
    fingerprint: String,
    location: String,
}

let j = json!({
    "fingerprint": "0xF9BA143B95FF6D82",
    "location": "Menlo Park, CA"
});

let u: User = serde_json::from_value(j).unwrap();
```

Errors: fails if the `Value`'s structure does not match what `T` expects (e.g. `T` is
a struct but the `Value` is not a JSON map), or if `T`'s `Deserialize` impl rejects the
data (missing required field, number too large for the target primitive, etc.).

Note the bound is `DeserializeOwned`, not `Deserialize<'de>`: the value is owned, so
borrowed deserialization from the `Value` is not possible.

## `to_value`

Convert a `T` into `serde_json::Value`, the enum that can represent any valid JSON data.

```rust
use serde::Serialize;
use serde_json::json;

#[derive(Serialize)]
struct User {
    fingerprint: String,
    location: String,
}

let u = User {
    fingerprint: "0xF9BA143B95FF6D82".to_owned(),
    location: "Menlo Park, CA".to_owned(),
};

let expected = json!({
    "fingerprint": "0xF9BA143B95FF6D82",
    "location": "Menlo Park, CA",
});

let v = serde_json::to_value(u).unwrap();
assert_eq!(v, expected);
```

Errors: fails if `T`'s `Serialize` impl decides to fail, **or if `T` contains a map with
non-string keys**.

```rust
use std::collections::BTreeMap;

let mut map = BTreeMap::new();
map.insert(vec![32, 64], "x86");
println!("{}", serde_json::to_value(map).unwrap_err());
```

## Round-trip note

`to_value` then `from_value` is a lossy round trip for types with non-string map keys
(`to_value` errors) and for types whose `Deserialize` is stricter than their `Serialize`
output. For plain `#[derive(Serialize, Deserialize)]` structs and enums it round-trips.

## Related

- `serde_json::to_string` / `from_str` — the string-based equivalents (same error type).
- `serde_json::json!` — macro for constructing `Value` literals for assertions.
- `Value::as_object`, `Value::get`, `Value::pointer` — inspecting a `Value` without
  deserializing into a typed struct.
