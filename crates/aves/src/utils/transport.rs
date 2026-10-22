//! RPC serving and endpoint catalog projection, independent of MCP implementations.

use std::net::IpAddr;

use axum::{
    Router,
    body::Body,
    extract::Request as AxumRequest,
    http::{
        HeaderMap as RpcHeaderMap, HeaderValue, Method, header::ACCESS_CONTROL_ALLOW_HEADERS,
        header::ACCESS_CONTROL_ALLOW_METHODS, header::ACCESS_CONTROL_ALLOW_ORIGIN, header::AUTHORIZATION,
    },
    middleware::{self as axum_middleware, Next as AxumNext},
    response::Response,
};
use rspc::Router as RspcRouter;
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use crate::auth::Jwt;
use crate::bus::{BusControlError, BusRpcContext, DomainPublication, OpaqueRequestEnvelope};

/// One MCP domain endpoint derived from the canonical MCP registry.
#[derive(Clone, Debug, Serialize)]
pub struct DeveloperMcpEndpoint {
    /// Human-readable server name.
    pub server_name: String,
    /// Domain route segment.
    pub domain: String,
    /// Complete loopback URL.
    pub url: String,
}

/// Projects registered publications into the developer MCP endpoint catalog.
pub fn developer_mcp_endpoints<E: 'static>(
    publications: &[DomainPublication<BusRpcContext, E>],
    host: &IpAddr,
    port: u16,
) -> Vec<DeveloperMcpEndpoint> {
    publications
        .iter()
        .map(|domain| DeveloperMcpEndpoint {
            server_name: domain.server_name.to_string(),
            domain: domain.route_slug.to_string(),
            url: format!("http://{}:{port}/mcp/{}", host, domain.route_slug),
        })
        .collect()
}

/// Serve the application RPC router with the existing developer browser CORS policy.
pub async fn run_rpc(
    listener: TcpListener,
    bus: mpsc::Sender<OpaqueRequestEnvelope>,
    shutdown: oneshot::Receiver<()>,
    rpc_router: fn() -> RspcRouter<BusRpcContext>,
) -> Result<(), BusControlError> {
    let (procedures, _) = rpc_router()
        .build()
        .map_err(|errors| BusControlError::Serve {
            protocol: "RPC",
            source: Box::new(std::io::Error::other(format!("{errors:?}"))),
        })?;
    let rspc = rspc_axum::endpoint(procedures, move |headers: axum::http::HeaderMap| {
        BusRpcContext::authenticated(bus.clone(), jwt_from_authorization(&headers))
    });
    axum::serve(
        listener,
        Router::new()
            .nest("/", rspc)
            .layer(axum_middleware::from_fn(developer_rpc_cors)),
    )
    .with_graceful_shutdown(async move {
        let _ = shutdown.await;
    })
    .await
    .map_err(|source| BusControlError::Serve {
        protocol: "RPC",
        source: Box::new(source),
    })
}

fn jwt_from_authorization(headers: &RpcHeaderMap) -> Jwt {
    let token = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| "missing-desktop-authentication".to_owned());

    Jwt::from_opaque(token)
}

async fn developer_rpc_cors(request: AxumRequest, next: AxumNext) -> Response {
    let mut response = if request.method() == Method::OPTIONS {
        developer_rpc_preflight_response()
    } else {
        next.run(request).await
    };
    add_developer_rpc_cors_headers(&mut response);
    response
}

fn developer_rpc_preflight_response() -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = axum::http::StatusCode::NO_CONTENT;
    response
}

fn add_developer_rpc_cors_headers(response: &mut Response) {
    for (name, value) in [
        (
            ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_static("http://127.0.0.1:5001"),
        ),
        (ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET, OPTIONS")),
        (
            ACCESS_CONTROL_ALLOW_HEADERS,
            HeaderValue::from_static("Authorization, Content-Type"),
        ),
    ] {
        response.headers_mut().insert(name, value);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use tower::ServiceExt;

    #[test]
    fn developer_rpc_preflight_allows_bearer_authentication() {
        let mut response = developer_rpc_preflight_response();
        add_developer_rpc_cors_headers(&mut response);

        assert_eq!(response.status(), axum::http::StatusCode::NO_CONTENT);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&HeaderValue::from_static("http://127.0.0.1:5001"))
        );
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::ACCESS_CONTROL_ALLOW_METHODS),
            Some(&HeaderValue::from_static("GET, OPTIONS"))
        );
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::ACCESS_CONTROL_ALLOW_HEADERS),
            Some(&HeaderValue::from_static("Authorization, Content-Type"))
        );
    }

    #[test]
    fn endpoint_catalog_projects_registered_publications_in_order() {
        static PUBLICATIONS: [DomainPublication<BusRpcContext, std::io::Error>; 2] = [
            DomainPublication {
                server_name: "First",
                route_slug: "first",
                operations: &[],
            },
            DomainPublication {
                server_name: "Second",
                route_slug: "second",
                operations: &[],
            },
        ];
        let endpoints = developer_mcp_endpoints(&PUBLICATIONS, &"::1".parse().unwrap(), 9000);
        assert_eq!(
            endpoints
                .iter()
                .map(|endpoint| endpoint.domain.as_str())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
        assert_eq!(
            endpoints
                .iter()
                .map(|endpoint| endpoint.server_name.as_str())
                .collect::<Vec<_>>(),
            ["First", "Second"]
        );
        assert_eq!(endpoints[0].url, "http://::1:9000/mcp/first");
        assert_eq!(
            serde_json::to_value(&endpoints[0]).unwrap(),
            serde_json::json!({ "server_name": "First", "domain": "first", "url": "http://::1:9000/mcp/first" })
        );
        assert!(developer_mcp_endpoints::<std::io::Error>(&[], &"127.0.0.1".parse().unwrap(), 9000).is_empty());
    }

    #[tokio::test]
    async fn rpc_cors_short_circuits_options_and_preserves_get_handler_path() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let get_calls = Arc::clone(&calls);
        let app = Router::new()
            .route(
                "/",
                axum::routing::get(move || {
                    let get_calls = Arc::clone(&get_calls);
                    async move {
                        get_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        "ok"
                    }
                }),
            )
            .layer(axum_middleware::from_fn(developer_rpc_cors));
        let preflight = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(preflight.status(), axum::http::StatusCode::NO_CONTENT);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        let get = app
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::GET)
                    .uri("/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get.status(), axum::http::StatusCode::OK);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn rpc_authorization_reader_keeps_lenient_fallback() {
        let headers = axum::http::HeaderMap::new();
        assert_eq!(
            jwt_from_authorization(&headers).as_str(),
            "missing-desktop-authentication"
        );
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(axum::http::header::AUTHORIZATION, "Basic opaque".parse().unwrap());
        assert_eq!(
            jwt_from_authorization(&headers).as_str(),
            "missing-desktop-authentication"
        );
        headers.insert(AUTHORIZATION, "Bearer valid-token".parse().unwrap());
        assert_eq!(jwt_from_authorization(&headers).as_str(), "valid-token");
        headers.insert(AUTHORIZATION, "Bearer ".parse().unwrap());
        assert_eq!(
            jwt_from_authorization(&headers).as_str(),
            "missing-desktop-authentication"
        );
        headers.append(AUTHORIZATION, "Bearer second-token".parse().unwrap());
        assert_eq!(
            jwt_from_authorization(&headers).as_str(),
            "missing-desktop-authentication"
        );
    }
}
