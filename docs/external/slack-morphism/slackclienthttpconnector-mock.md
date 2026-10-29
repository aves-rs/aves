---
source: Context7 API + docs.rs
library: Slack Morphism
package: slack-morphism
version: 2.31.1
verified: 2026-10-29
official_docs: https://docs.rs/crate/slack-morphism/2.31.1/source/src/client.rs
---

### SlackClientHttpConnector trait (for custom/mock connectors)

```rust
pub trait SlackClientHttpConnector {
    // Required methods
    fn http_get_uri<'a, RS>(...) -> BoxFuture<...>
    fn http_get_with_client_secret<'a, RS>(...) -> BoxFuture<...>
    fn http_post_uri<'a, RQ, RS>(...) -> BoxFuture<...>
    fn http_post_uri_multipart_form<...>(...) -> BoxFuture<...>
    fn http_post_uri_binary<...>(...) -> BoxFuture<...>
    ...
}
```

SlackClient::new takes any impl of SlackClientHttpConnector (Send + Sync).

### Base URI constant

```rust
pub const SLACK_API_URI_STR: &'static str = "https://slack.com/api";
```

(from SlackClientHttpApiUri)

Viable mock: implement the trait to return fixed deterministic responses (e.g. for api.test / auth.test) without real network calls. Full URIs are passed to the methods, so mocks control responses directly.