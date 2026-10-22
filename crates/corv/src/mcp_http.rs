use axum08::{
    extract::Request,
    http::{HeaderMap as McpHeaderMap, StatusCode, header::ToStrError},
    middleware::{self, Next},
    response::{IntoResponse, Response as Axum08Response},
};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use serde_json::Value;
use std::{error::Error, io::Error as IoError, sync::Arc};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use aves::auth::Jwt;
use aves::bus::{BusControlError, BusRpcContext, DomainPublication, OpaqueRequestEnvelope};

use super::mcp::{ApplicationMcpService, HttpMcpBearer, collate_mcp_specs};

fn mcp_session_manager() -> LocalSessionManager {
    let mut manager = LocalSessionManager::default();
    manager.session_config.keep_alive = None;
    manager
}

/// Serve application MCP publications over HTTP with bearer admission before bus ingress.
pub async fn run_mcp<E: Error + Send + Sync + 'static>(
    listener: TcpListener,
    bus: mpsc::Sender<OpaqueRequestEnvelope>,
    shutdown: oneshot::Receiver<()>,
    publications: fn() -> Vec<DomainPublication<BusRpcContext, E>>,
    error_details: fn(&E) -> Value,
    application_name: &'static str,
) -> Result<(), BusControlError> {
    let publications = publications();
    let domains = collate_mcp_specs(&publications).map_err(|error| BusControlError::Serve {
        protocol: "MCP",
        source: Box::new(error),
    })?;
    let cancellation = CancellationToken::new();
    let mut router = axum08::Router::new();
    for domain in domains {
        let route = format!("/mcp/{}", domain.route_slug);
        let spec = Arc::clone(&domain.spec);
        let context =
            BusRpcContext::authenticated(bus.clone(), Jwt::from_opaque("http-mcp-request-boundary".to_owned()));
        let mut config = StreamableHttpServerConfig::default();
        config.cancellation_token = cancellation.clone();
        let service = StreamableHttpService::new(
            move || {
                Ok::<_, IoError>(ApplicationMcpService::new(
                    Arc::clone(&spec),
                    context.clone(),
                    Jwt::from_opaque("http-mcp-request-boundary".to_owned()),
                    error_details,
                    application_name,
                ))
            },
            mcp_session_manager().into(),
            config,
        );
        router = router
            .nest_service(&route, service)
            .layer(middleware::from_fn(require_http_mcp_bearer));
    }
    axum08::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = shutdown.await;
            cancellation.cancel();
        })
        .await
        .map_err(|source| BusControlError::Serve {
            protocol: "MCP",
            source: Box::new(source),
        })
}

async fn require_http_mcp_bearer(request: Request, next: Next) -> Axum08Response {
    let token = match extract_http_mcp_bearer(request.headers()) {
        Ok(token) => token,
        Err(error) => {
            return (
                StatusCode::UNAUTHORIZED,
                axum08::Json(serde_json::json!({ "error": error.code() })),
            )
                .into_response();
        }
    };

    let mut request = request;
    request.extensions_mut().insert(HttpMcpBearer(token));
    next.run(request).await
}

fn extract_http_mcp_bearer(headers: &McpHeaderMap) -> Result<Jwt, HttpMcpBearerError> {
    let mut authorization = headers.get_all(axum08::http::header::AUTHORIZATION).iter();
    let value = authorization
        .next()
        .ok_or(HttpMcpBearerError::MissingAuthorization)?;
    if authorization.next().is_some() {
        return Err(HttpMcpBearerError::DuplicateAuthorization);
    }
    let value = value.to_str()?;
    let token = value
        .strip_prefix("Bearer ")
        .ok_or(HttpMcpBearerError::MalformedScheme)?;
    if token.is_empty() {
        return Err(HttpMcpBearerError::EmptyBearer);
    }
    if !is_compact_jwt(token) {
        return Err(HttpMcpBearerError::MalformedCompactJwt);
    }
    Ok(Jwt::from_opaque(token.to_owned()))
}

impl From<ToStrError> for HttpMcpBearerError {
    fn from(_: ToStrError) -> Self {
        Self::MalformedScheme
    }
}

fn is_compact_jwt(token: &str) -> bool {
    let mut segments = token.split('.');
    let valid_segment = |segment: &str| {
        !segment.is_empty()
            && segment
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    };

    match (segments.next(), segments.next(), segments.next(), segments.next()) {
        (Some(header), Some(claims), Some(signature), None) => {
            valid_segment(header) && valid_segment(claims) && valid_segment(signature)
        }
        _ => false,
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
enum HttpMcpBearerError {
    #[error("missing Authorization header")]
    MissingAuthorization,
    #[error("duplicate Authorization headers")]
    DuplicateAuthorization,
    #[error("malformed Authorization scheme")]
    MalformedScheme,
    #[error("empty bearer token")]
    EmptyBearer,
    #[error("malformed compact JWT")]
    MalformedCompactJwt,
}

impl HttpMcpBearerError {
    fn code(self) -> &'static str {
        match self {
            Self::MissingAuthorization => "missing_authorization",
            Self::DuplicateAuthorization => "duplicate_authorization",
            Self::MalformedScheme => "malformed_authorization_scheme",
            Self::EmptyBearer => "empty_bearer",
            Self::MalformedCompactJwt => "malformed_compact_jwt",
        }
    }
}

#[cfg(test)]
mod tests {
    use tower::ServiceExt;

    use super::McpHeaderMap as HeaderMap;
    use super::*;

    #[test]
    fn http_mcp_session_has_no_independent_idle_expiry() {
        let manager = mcp_session_manager();

        assert_eq!(manager.session_config.keep_alive, None);
    }

    #[test]
    fn http_mcp_bearer_requires_one_nonempty_bearer_header() {
        let mut headers = HeaderMap::new();
        assert_eq!(
            extract_http_mcp_bearer(&headers),
            Err(HttpMcpBearerError::MissingAuthorization)
        );

        headers.insert(axum08::http::header::AUTHORIZATION, "Basic token".parse().unwrap());
        assert_eq!(
            extract_http_mcp_bearer(&headers),
            Err(HttpMcpBearerError::MalformedScheme)
        );

        headers.insert(axum08::http::header::AUTHORIZATION, "Bearer ".parse().unwrap());
        assert_eq!(extract_http_mcp_bearer(&headers), Err(HttpMcpBearerError::EmptyBearer));

        headers.insert(
            axum08::http::header::AUTHORIZATION,
            "Bearer eyJhbGciOiJub25lIn0.eyJzdWIiOiJ0ZXN0In0.signature"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            extract_http_mcp_bearer(&headers)
                .expect("well-formed compact JWT")
                .as_str(),
            "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ0ZXN0In0.signature"
        );

        headers.append(
            axum08::http::header::AUTHORIZATION,
            "Bearer eyJhbGciOiJub25lIn0.eyJzdWIiOiJ0ZXN0In0.signature"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            extract_http_mcp_bearer(&headers),
            Err(HttpMcpBearerError::DuplicateAuthorization)
        );
    }

    #[test]
    fn http_mcp_bearer_rejects_whitespace_inside_token() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum08::http::header::AUTHORIZATION,
            "Bearer opaque jwt".parse().unwrap(),
        );

        assert_eq!(
            extract_http_mcp_bearer(&headers),
            Err(HttpMcpBearerError::MalformedCompactJwt)
        );
    }

    #[test]
    fn http_mcp_bearer_requires_compact_jwt_segments() {
        let mut headers = HeaderMap::new();
        for token in ["token", "a..c", "a.b.c.d", "a.b.c="] {
            headers.insert(
                axum08::http::header::AUTHORIZATION,
                format!("Bearer {token}").parse().unwrap(),
            );
            assert_eq!(
                extract_http_mcp_bearer(&headers),
                Err(HttpMcpBearerError::MalformedCompactJwt),
                "token {token:?} should be rejected"
            );
        }
    }

    #[test]
    fn malformed_http_bearer_is_rejected_before_a_bus_request_is_created() {
        let mut headers = HeaderMap::new();
        headers.insert(axum08::http::header::AUTHORIZATION, "Basic not-a-jwt".parse().unwrap());

        let boundary_result = extract_http_mcp_bearer(&headers);

        assert_eq!(boundary_result, Err(HttpMcpBearerError::MalformedScheme));
        assert!(boundary_result.is_err());
    }

    #[tokio::test]
    async fn malformed_bearer_is_rejected_before_the_handler() {
        let handler_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls = Arc::clone(&handler_calls);
        let app = axum08::Router::new()
            .route(
                "/",
                axum08::routing::get(move |request: Request| {
                    let calls = Arc::clone(&calls);
                    async move {
                        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        request
                            .extensions()
                            .get::<HttpMcpBearer>()
                            .unwrap()
                            .0
                            .as_str()
                            .to_owned()
                    }
                }),
            )
            .layer(middleware::from_fn(require_http_mcp_bearer));
        for (headers, expected_code) in [
            (&[][..], "missing_authorization"),
            (&["Basic invalid"][..], "malformed_authorization_scheme"),
            (&["Bearer "][..], "empty_bearer"),
            (&["Bearer a.b.c", "Bearer d.e.f"][..], "duplicate_authorization"),
        ] {
            let mut request = axum08::http::Request::builder().uri("/");
            for &header in headers {
                request = request.header(axum08::http::header::AUTHORIZATION, header);
            }
            let response = app
                .clone()
                .oneshot(request.body(axum08::body::Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            let body = axum08::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&body).unwrap(),
                serde_json::json!({"error": expected_code})
            );
        }
        assert_eq!(handler_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn valid_bearer_is_added_to_request_extensions_before_the_handler() {
        let app = axum08::Router::new()
            .route(
                "/",
                axum08::routing::get(|request: Request| async move {
                    if request
                        .extensions()
                        .get::<HttpMcpBearer>()
                        .unwrap()
                        .0
                        .as_str()
                        == "a.b.c"
                    {
                        StatusCode::NO_CONTENT
                    } else {
                        StatusCode::BAD_REQUEST
                    }
                }),
            )
            .layer(middleware::from_fn(require_http_mcp_bearer));
        let response = app
            .oneshot(
                axum08::http::Request::builder()
                    .uri("/")
                    .header(axum08::http::header::AUTHORIZATION, "Bearer a.b.c")
                    .body(axum08::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
}
