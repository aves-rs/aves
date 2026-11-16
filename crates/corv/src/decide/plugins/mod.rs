//! Stateless provider boundary and shared bounded HTTP transport.

mod jev;
mod ollama;
mod systemone;

use std::{sync::LazyLock, time::Duration};

use async_trait::async_trait;
use reqwest::{
    Url,
    header::{CONTENT_TYPE, HeaderMap, RETRY_AFTER},
};
use serde_json::Value;

use super::types::{DecideAskRequest, DecideCredential, DecideError, DecideOutcome};

#[async_trait]
pub(crate) trait DecidePlugin: Send + Sync {
    async fn ask(
        &self,
        request: &DecideAskRequest,
        credential: Option<&DecideCredential>,
    ) -> Result<DecideOutcome, DecideError>;
}

/// API-key requirements for a Decide provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecideAuth {
    /// An API key is required.
    Required,
    /// An API key may be omitted.
    Optional,
}

/// Public metadata for a supported Decide provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecideProviderDescriptor {
    /// Stable provider identifier.
    pub id: &'static str,
    /// Human-readable provider name.
    pub display_name: &'static str,
    /// API-key requirement.
    pub credential: DecideAuth,
}

struct ProviderRow {
    id: &'static str,
    display_name: &'static str,
    credential: DecideAuth,
    plugin: &'static dyn DecidePlugin,
}

static PROVIDERS: [ProviderRow; 2] = [
    ProviderRow {
        id: jev::PROVIDER_ID,
        display_name: "Jev",
        credential: DecideAuth::Required,
        plugin: &jev::PLUGIN,
    },
    ProviderRow {
        id: ollama::PROVIDER_ID,
        display_name: "Ollama",
        credential: DecideAuth::Optional,
        plugin: &ollama::PLUGIN,
    },
];

/// Enumerate the supported Decide providers and their credential requirements.
pub fn decide_providers() -> impl Iterator<Item = DecideProviderDescriptor> {
    PROVIDERS.iter().map(|row| DecideProviderDescriptor {
        id: row.id,
        display_name: row.display_name,
        credential: row.credential,
    })
}

pub(crate) fn for_provider(provider_id: &str) -> Result<&'static dyn DecidePlugin, DecideError> {
    PROVIDERS
        .iter()
        .find(|row| row.id == provider_id)
        .map(|row| row.plugin)
        .ok_or_else(|| DecideError::UnknownProvider {
            provider: provider_id.to_owned(),
        })
}

static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("shared HTTP client configuration is valid")
});
const MAX_BODY: usize = 4 * 1024 * 1024;

pub(super) fn systemone_url(base: &Url) -> Url {
    let mut url = base.clone();
    url.set_path(&format!("{}/v1/systemone", base.path().trim_end_matches('/')));
    url
}

pub(super) fn parse_retry_after(headers: &HeaderMap) -> Option<u64> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn retry(message: &str) -> DecideError {
    DecideError::ProviderRetryable {
        retry_after_seconds: None,
        message: message.to_owned(),
    }
}

fn invalid(message: &str) -> DecideError {
    DecideError::ProviderResponseInvalid {
        message: message.to_owned(),
    }
}

pub(super) async fn post_json(
    base: &Url,
    bearer: Option<&str>,
    body: &Value,
    timeout: Duration,
    retryable: fn(u16) -> bool,
) -> Result<Value, DecideError> {
    tokio::time::timeout(timeout, async {
        let bytes = serde_json::to_vec(body).map_err(|_| DecideError::ProviderRequestFailed {
            message: "request serialization failed".into(),
        })?;
        let mut request = CLIENT
            .post(systemone_url(base))
            .header(CONTENT_TYPE, "application/json")
            .body(bytes);
        if let Some(key) = bearer {
            request = request.bearer_auth(key);
        }
        let mut response = request.send().await.map_err(|error| {
            if error.is_timeout() {
                retry("provider request timed out")
            } else if error.is_connect() {
                retry("provider connection failed")
            } else {
                retry("provider transport failed")
            }
        })?;
        let status = response.status();
        if !status.is_success() {
            let code = status.as_u16();
            if retryable(code) {
                return Err(DecideError::ProviderRetryable {
                    retry_after_seconds: parse_retry_after(response.headers()),
                    message: format!("provider temporarily unavailable ({code})"),
                });
            }
            let message = if code == 401 {
                "credential rejected (401)".to_owned()
            } else {
                format!("provider request rejected ({code})")
            };
            return Err(DecideError::ProviderRequestFailed { message });
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_BODY as u64)
        {
            return Err(invalid("response body exceeds limit"));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| retry("response body interrupted"))?
        {
            if chunk.len() > MAX_BODY - bytes.len() {
                return Err(invalid("response body exceeds limit"));
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| invalid("response is not valid JSON"))
    })
    .await
    .unwrap_or_else(|_| Err(retry("provider request timed out")))
}

#[cfg(test)]
pub(super) mod test_server {
    use axum08::Router;
    use reqwest::Url;
    use tokio::task::JoinHandle;

    pub(super) struct TestServer {
        pub(super) base: Url,
        task: JoinHandle<()>,
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    pub(super) async fn start(router: Router) -> TestServer {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock server");
        let base =
            Url::parse(&format!("http://{}/", listener.local_addr().expect("listener address"))).expect("loopback URL");
        let task = tokio::spawn(async move {
            axum08::serve(listener, router).await.expect("mock server");
        });
        TestServer { base, task }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum08::{
        Router,
        http::{StatusCode, header},
        routing::post,
    };

    #[test]
    fn fixed_path_and_retry_after() {
        for (base, expected) in [
            ("https://example.org", "/v1/systemone"),
            ("https://example.org/", "/v1/systemone"),
            ("https://example.org/api/", "/api/v1/systemone"),
            ("https://example.org/api", "/api/v1/systemone"),
        ] {
            assert_eq!(systemone_url(&Url::parse(base).unwrap()).path(), expected);
        }
        let mut headers = HeaderMap::new();
        for (value, expected) in [
            ("42", Some(42)),
            ("Wed, 21 Oct 2015 07:28:00 GMT", None),
            ("garbage", None),
            ("+3", None),
        ] {
            headers.insert(RETRY_AFTER, value.parse().unwrap());
            assert_eq!(parse_retry_after(&headers), expected);
        }
        assert!(matches!(
            for_provider("unknown"),
            Err(DecideError::UnknownProvider { .. })
        ));
    }

    #[tokio::test]
    async fn status_never_exposes_body_and_redirect_is_terminal() {
        for status in [
            StatusCode::UNAUTHORIZED,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::IM_A_TEAPOT,
            StatusCode::TEMPORARY_REDIRECT,
        ] {
            let app = Router::new().route(
                "/v1/systemone",
                post(move || async move {
                    (
                        status,
                        [(header::RETRY_AFTER, "7"), (header::LOCATION, "/other")],
                        "sentinel-secret",
                    )
                }),
            );
            let server = test_server::start(app).await;
            let error = post_json(
                &server.base,
                None,
                &serde_json::json!({}),
                Duration::from_secs(2),
                |s| s == 429,
            )
            .await
            .unwrap_err();
            assert!(!format!("{error:?}").contains("sentinel-secret"));
            match status.as_u16() {
                429 => assert!(
                    matches!(error, DecideError::ProviderRetryable { retry_after_seconds: Some(7), message } if message == "provider temporarily unavailable (429)")
                ),
                401 => assert!(
                    matches!(error, DecideError::ProviderRequestFailed { message } if message == "credential rejected (401)")
                ),
                code => assert!(
                    matches!(error, DecideError::ProviderRequestFailed { message } if message == format!("provider request rejected ({code})"))
                ),
            }
        }
    }

    #[tokio::test]
    async fn rejects_oversized_content_length() {
        let app = Router::new().route("/v1/systemone", post(|| async { "x".repeat(MAX_BODY + 1) }));
        let server = test_server::start(app).await;
        assert!(matches!(
            post_json(
                &server.base,
                None,
                &serde_json::json!({}),
                Duration::from_secs(5),
                |_| false
            )
            .await,
            Err(DecideError::ProviderResponseInvalid { .. })
        ));
    }

    #[tokio::test]
    async fn deadline_covers_body_and_chunked_body_is_bounded() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for stalled in [true, false] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
            let task = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut buffer = [0; 4096];
                let _ = stream.read(&mut buffer).await;
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                    .await
                    .unwrap();
                if stalled {
                    stream.write_all(b"1\r\n{\r\n").await.unwrap();
                    std::future::pending::<()>().await;
                } else {
                    let chunk = vec![b'x'; 65536];
                    for _ in 0..65 {
                        if stream.write_all(b"10000\r\n").await.is_err()
                            || stream.write_all(&chunk).await.is_err()
                            || stream.write_all(b"\r\n").await.is_err()
                        {
                            break;
                        }
                    }
                }
            });
            let result = post_json(
                &base,
                None,
                &serde_json::json!({}),
                Duration::from_secs(if stalled { 1 } else { 10 }),
                |_| false,
            )
            .await;
            if stalled {
                assert!(
                    matches!(result, Err(DecideError::ProviderRetryable { message, .. }) if message == "provider request timed out")
                );
            } else {
                assert!(matches!(result, Err(DecideError::ProviderResponseInvalid { .. })));
            }
            task.abort();
        }
    }
}
