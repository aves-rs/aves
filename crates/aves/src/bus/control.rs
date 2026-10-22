use std::collections::HashMap;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use crate::auth::AvesAuthError;
use crate::runtime::ServicePublisher;
use crate::utils::errors::SharedInfraError;

use super::event_bus::OpaqueRequestEnvelope;
use super::iroh::PeerConnectivity;
use super::messages::BusRpcContext;

const LOOPBACK: IpAddr = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// An opaque application listener future owned by the bus lifecycle manager.
pub type ListenerFuture = Pin<Box<dyn Future<Output = Result<(), BusControlError>> + Send + 'static>>;

type ListenerFactory = Arc<
    dyn Fn(TcpListener, mpsc::Sender<OpaqueRequestEnvelope>, oneshot::Receiver<()>) -> ListenerFuture + Send + Sync,
>;

aves::domain! {
    request BusControlRequestPayload => BusControlResponse
    error = BusControlError
    transport = SharedInfraError
    target = "bus";
    pub service BusSudoService {
        OpenRpc => RpcOpened (operation = "openRpc"),
        CloseRpc => RpcClosed (operation = "closeRpc"),
        OpenMcp => McpOpened (operation = "openMcp"),
        CloseMcp => McpClosed (operation = "closeMcp"),
        SetPeerConnectivity => PeerConnectivitySet (operation = "setPeerConnectivity"),
    }
}

/// Private request envelope consumed by the bus control actor.
pub struct BusControlRequest {
    pub payload: BusControlRequestPayload,
    pub reply_to: oneshot::Sender<Result<BusControlResponse, BusControlError>>,
}

type BusControlHandles = (
    oneshot::Sender<()>,
    mpsc::Sender<BusControlRequest>,
    tokio::task::JoinHandle<()>,
);

/// Fixed metadata for one application-mounted MCP route.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct McpRouteMetadata {
    pub server_name: String,
    pub domain: String,
}

/// Application-provided serving adapters. The bus owns only their lifecycle.
#[derive(Clone)]
pub struct TransportBindings {
    rpc_factory: ListenerFactory,
    mcp_factory: ListenerFactory,
    mcp_routes: Vec<McpRouteMetadata>,
}

impl TransportBindings {
    /// Creates generic factories and the fixed MCP route catalog supplied by the application.
    pub fn new(
        rpc_factory: impl Fn(TcpListener, mpsc::Sender<OpaqueRequestEnvelope>, oneshot::Receiver<()>) -> ListenerFuture
        + Send
        + Sync
        + 'static,
        mcp_factory: impl Fn(TcpListener, mpsc::Sender<OpaqueRequestEnvelope>, oneshot::Receiver<()>) -> ListenerFuture
        + Send
        + Sync
        + 'static,
        mcp_routes: Vec<McpRouteMetadata>,
    ) -> Self {
        Self {
            rpc_factory: Arc::new(rpc_factory),
            mcp_factory: Arc::new(mcp_factory),
            mcp_routes,
        }
    }
}

/// Private listener-control operations. Every variant is bus-sudo only.
#[aves::bus_requests(
    context = BusRpcContext,
    root_error = BusControlError,
    domain_error = BusControlError,
    domain = "bus"
)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BusControlRequestPayload {
    #[privileged(reason = "Listener control is internal bus-only infrastructure")]
    OpenRpc { host: Option<IpAddr>, port: u16 },
    #[privileged(reason = "Listener control is internal bus-only infrastructure")]
    CloseRpc { host: IpAddr, port: u16 },
    #[privileged(reason = "Listener control is internal bus-only infrastructure")]
    OpenMcp { host: Option<IpAddr>, port: u16 },
    #[privileged(reason = "Listener control is internal bus-only infrastructure")]
    CloseMcp { host: IpAddr, port: u16 },
    #[privileged(reason = "Native peer transport control is internal bus-only infrastructure")]
    SetPeerConnectivity { enabled: bool },
}

/// Metadata returned for each mounted application MCP route.
#[derive(Debug, Clone, Serialize, Deserialize, specta::Type, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BusMcpEndpoint {
    pub server_name: String,
    pub domain: String,
    pub url: String,
}

struct ListenerEntry {
    address: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<Result<(), BusControlError>>>,
}

impl Drop for ListenerEntry {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.as_ref()
            && !task.is_finished()
        {
            tracing::debug!(address = %self.address, "aborting bus listener safety guard");
            task.abort();
        }
    }
}

/// Responses returned by private listener-control operations.
#[aves::bus_responses]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BusControlResponse {
    RpcOpened {
        host: IpAddr,
        port: u16,
    },
    RpcClosed,
    McpOpened {
        host: IpAddr,
        port: u16,
        endpoints: Vec<BusMcpEndpoint>,
    },
    McpClosed,
    PeerConnectivitySet,
}

struct ListenerManager {
    rpc: HashMap<SocketAddr, ListenerEntry>,
    mcp: HashMap<SocketAddr, ListenerEntry>,
}

impl ListenerManager {
    async fn open(
        registry: &mut HashMap<SocketAddr, ListenerEntry>,
        protocol: &'static str,
        host: Option<IpAddr>,
        port: u16,
        factory: ListenerFactory,
        sender: mpsc::Sender<OpaqueRequestEnvelope>,
        routes: Option<&[McpRouteMetadata]>,
    ) -> Result<(SocketAddr, Option<Vec<BusMcpEndpoint>>), BusControlError> {
        let host = host.unwrap_or(LOOPBACK);
        let requested = SocketAddr::new(host, port);
        if registry.contains_key(&requested) {
            return Err(BusControlError::DuplicateListener { protocol, host, port });
        }
        let bind_error = |source| BusControlError::Bind {
            protocol,
            host,
            port,
            source,
        };
        let listener = TcpListener::bind(requested).await.map_err(bind_error)?;
        let actual = listener.local_addr().map_err(bind_error)?;
        if registry.contains_key(&actual) {
            return Err(BusControlError::DuplicateListener {
                protocol,
                host: actual.ip(),
                port: actual.port(),
            });
        }
        let endpoints = routes.map(|routes| {
            routes
                .iter()
                .map(|route| BusMcpEndpoint {
                    server_name: route.server_name.clone(),
                    domain: route.domain.clone(),
                    url: format!("http://{actual}/mcp/{}", route.domain),
                })
                .collect()
        });
        let (shutdown, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(factory(listener, sender, shutdown_rx));
        registry.insert(
            actual,
            ListenerEntry {
                address: actual,
                shutdown: Some(shutdown),
                task: Some(task),
            },
        );
        Ok((actual, endpoints))
    }

    async fn close(
        registry: &mut HashMap<SocketAddr, ListenerEntry>,
        protocol: &'static str,
        host: IpAddr,
        port: u16,
    ) -> Result<(), BusControlError> {
        let address = SocketAddr::new(host, port);
        let Some(mut entry) = registry.remove(&address) else {
            return Err(BusControlError::ListenerNotFound { protocol, host, port });
        };
        if let Some(shutdown) = entry.shutdown.take() {
            shutdown
                .send(())
                .map_err(|_| BusControlError::ChannelSend("listener shutdown receiver dropped".to_string()))?;
        }
        if let Some(mut task) = entry.task.take() {
            match tokio::time::timeout(SHUTDOWN_TIMEOUT, &mut task).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => return Err(error),
                Ok(Err(source)) => return Err(BusControlError::Join { protocol, source }),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    return Err(BusControlError::ShutdownTimeout { protocol });
                }
            }
        }
        Ok(())
    }

    async fn shutdown(&mut self) {
        for (registry, protocol) in [(&mut self.rpc, "RPC"), (&mut self.mcp, "MCP")] {
            let addresses = registry.keys().copied().collect::<Vec<_>>();
            for address in addresses {
                if let Err(error) = Self::close(registry, protocol, address.ip(), address.port()).await {
                    tracing::warn!(%error, "bus listener shutdown failed");
                }
            }
        }
    }
}

pub(crate) fn start_control(
    sender: mpsc::Sender<OpaqueRequestEnvelope>,
    bindings: TransportBindings,
    publisher: ServicePublisher<dyn BusSudoService>,
    peer_connectivity: PeerConnectivity,
) -> Result<BusControlHandles, BusControlError> {
    let (service, receiver) = BusSudoServiceImpl::channel(32);
    let requests = service.sender.clone();
    let (shutdown, shutdown_receiver) = oneshot::channel();
    let task = tokio::spawn(run_control_actor(
        receiver,
        shutdown_receiver,
        sender,
        bindings,
        peer_connectivity,
    ));
    publisher
        .publish(service)
        .map_err(|_| BusControlError::ServiceAlreadyPublished)?;
    Ok((shutdown, requests, task))
}

async fn run_control_actor(
    mut requests: mpsc::Receiver<BusControlRequest>,
    mut shutdown: oneshot::Receiver<()>,
    sender: mpsc::Sender<OpaqueRequestEnvelope>,
    bindings: TransportBindings,
    peer_connectivity: PeerConnectivity,
) {
    let mut manager = ListenerManager {
        rpc: HashMap::new(),
        mcp: HashMap::new(),
    };
    tokio::select! {
        _ = async {
            while let Some(request) = requests.recv().await {
                let result = match request.payload {
                    BusControlRequestPayload::OpenRpc { host, port } => ListenerManager::open(
                        &mut manager.rpc, "RPC", host, port, bindings.rpc_factory.clone(), sender.clone(), None,
                    ).await.map(|(address, _)| BusControlResponse::RpcOpened {
                        host: address.ip(),
                        port: address.port(),
                    }),
                    BusControlRequestPayload::CloseRpc { host, port } => ListenerManager::close(&mut manager.rpc, "RPC", host, port)
                        .await.map(|_| BusControlResponse::RpcClosed),
                    BusControlRequestPayload::OpenMcp { host, port } => ListenerManager::open(
                        &mut manager.mcp, "MCP", host, port, bindings.mcp_factory.clone(), sender.clone(), Some(&bindings.mcp_routes),
                    ).await.map(|(address, endpoints)| BusControlResponse::McpOpened {
                        host: address.ip(),
                        port: address.port(),
                        endpoints: endpoints.unwrap_or_default(),
                    }),
                    BusControlRequestPayload::CloseMcp { host, port } => ListenerManager::close(&mut manager.mcp, "MCP", host, port)
                        .await.map(|_| BusControlResponse::McpClosed),
                    BusControlRequestPayload::SetPeerConnectivity { enabled } => peer_connectivity
                        .set_enabled(enabled)
                        .await
                        .map(|()| BusControlResponse::PeerConnectivitySet),
                };
                let _ = request.reply_to.send(result);
            }
        } => {}
        _ = &mut shutdown => manager.shutdown().await,
    }
}

/// Errors returned by the bus-owned listener controller.
#[derive(Debug, Error)]
pub enum BusControlError {
    #[error("Auth initialization failed: {0}")]
    AuthInitialization(#[from] AvesAuthError),
    #[error("keyring service namespace cannot be empty")]
    EmptyKeyringServiceNamespace,
    #[error("a {protocol} listener is already bound at {host}:{port}")]
    DuplicateListener {
        protocol: &'static str,
        host: IpAddr,
        port: u16,
    },
    #[error("no {protocol} listener is bound at {host}:{port}")]
    ListenerNotFound {
        protocol: &'static str,
        host: IpAddr,
        port: u16,
    },
    #[error("could not bind {protocol} listener at {host}:{port}: {source}")]
    Bind {
        protocol: &'static str,
        host: IpAddr,
        port: u16,
        source: std::io::Error,
    },
    #[error("{protocol} listener failed while serving: {source}")]
    Serve {
        protocol: &'static str,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("{protocol} listener did not stop before the shutdown deadline")]
    ShutdownTimeout { protocol: &'static str },
    #[error("{protocol} listener task failed: {source}")]
    Join {
        protocol: &'static str,
        source: tokio::task::JoinError,
    },
    #[error("bus control channel send failed: {0}")]
    ChannelSend(String),
    #[error("bus control channel receive failed: {0}")]
    ChannelReceive(String),
    #[error("bus sudo service was already published")]
    ServiceAlreadyPublished,
}

impl From<SharedInfraError> for BusControlError {
    fn from(error: SharedInfraError) -> Self {
        match error {
            SharedInfraError::ChannelSend { cause, .. } => Self::ChannelSend(cause),
            SharedInfraError::ChannelRecv { cause, .. } => Self::ChannelReceive(cause),
            SharedInfraError::OneshotReplyMissing { cause, .. } => Self::ChannelReceive(cause),
            error @ SharedInfraError::OneshotReplyTimeout { .. } => Self::ChannelReceive(error.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_listener_factory(completions: mpsc::Sender<()>) -> ListenerFactory {
        Arc::new(move |listener, _sender, shutdown| {
            let completions = completions.clone();
            Box::pin(async move {
                let _listener = listener;
                let _ = shutdown.await;
                let _ = completions.send(()).await;
                Ok(())
            })
        })
    }

    #[tokio::test]
    async fn omitted_host_defaults_to_loopback_and_port_zero_returns_bound_port() {
        let (completions, _completion_receiver) = mpsc::channel(1);
        let mut listeners = HashMap::new();

        let (address, _) = ListenerManager::open(
            &mut listeners,
            "RPC",
            None,
            0,
            test_listener_factory(completions),
            mpsc::channel(1).0,
            None,
        )
        .await
        .expect("ephemeral loopback listener should open");

        assert_eq!(address.ip(), LOOPBACK);
        assert_ne!(address.port(), 0);

        ListenerManager::close(&mut listeners, "RPC", address.ip(), address.port())
            .await
            .expect("listener should close");
    }

    #[tokio::test]
    async fn mcp_endpoint_metadata_uses_bound_address_and_separate_domain_routes() {
        let (completions, _completion_receiver) = mpsc::channel(1);
        let mut listeners = HashMap::new();
        let routes = [
            McpRouteMetadata {
                server_name: "first".to_string(),
                domain: "first".to_string(),
            },
            McpRouteMetadata {
                server_name: "second".to_string(),
                domain: "second".to_string(),
            },
        ];

        let (address, endpoints) = ListenerManager::open(
            &mut listeners,
            "MCP",
            None,
            0,
            test_listener_factory(completions),
            mpsc::channel(1).0,
            Some(&routes),
        )
        .await
        .expect("ephemeral MCP listener should open");
        let endpoints = endpoints.expect("MCP routes should produce endpoint metadata");

        assert_eq!(endpoints[0].url, format!("http://{address}/mcp/first"));
        assert_eq!(endpoints[1].url, format!("http://{address}/mcp/second"));
        assert_ne!(endpoints[0].url, endpoints[1].url);

        ListenerManager::close(&mut listeners, "MCP", address.ip(), address.port())
            .await
            .expect("listener should close");
    }

    #[tokio::test]
    async fn exact_returned_tuple_closes_and_joins_listener() {
        let (completions, mut completion_receiver) = mpsc::channel(1);
        let mut listeners = HashMap::new();

        let (address, _) = ListenerManager::open(
            &mut listeners,
            "RPC",
            Some(LOOPBACK),
            0,
            test_listener_factory(completions),
            mpsc::channel(1).0,
            None,
        )
        .await
        .expect("listener should open");

        ListenerManager::close(&mut listeners, "RPC", address.ip(), address.port())
            .await
            .expect("exact listener tuple should close");

        tokio::time::timeout(Duration::from_secs(1), completion_receiver.recv())
            .await
            .expect("listener task should join promptly")
            .expect("listener factory should report completion");
        assert!(listeners.is_empty());
    }

    #[tokio::test]
    async fn unknown_exact_tuple_returns_listener_not_found() {
        let mut listeners = HashMap::new();

        let error = ListenerManager::close(&mut listeners, "RPC", LOOPBACK, 1)
            .await
            .expect_err("unknown listener tuple should fail");

        assert!(matches!(
            error,
            BusControlError::ListenerNotFound { protocol: "RPC", host, port: 1 }
                if host == LOOPBACK
        ));
    }

    #[tokio::test]
    async fn duplicate_fixed_tuple_is_rejected() {
        let (completions, _completion_receiver) = mpsc::channel(2);
        let mut listeners = HashMap::new();
        let probe = TcpListener::bind(SocketAddr::new(LOOPBACK, 0))
            .await
            .expect("ephemeral probe should bind");
        let port = probe
            .local_addr()
            .expect("probe should have an address")
            .port();
        drop(probe);
        let factory = test_listener_factory(completions);

        ListenerManager::open(
            &mut listeners,
            "RPC",
            Some(LOOPBACK),
            port,
            factory.clone(),
            mpsc::channel(1).0,
            None,
        )
        .await
        .expect("first fixed listener should open");

        let error = ListenerManager::open(
            &mut listeners,
            "RPC",
            Some(LOOPBACK),
            port,
            factory,
            mpsc::channel(1).0,
            None,
        )
        .await
        .expect_err("duplicate fixed tuple should fail");

        assert!(matches!(
            error,
            BusControlError::DuplicateListener { protocol: "RPC", host, port: duplicate_port }
                if host == LOOPBACK && duplicate_port == port
        ));
        let mut manager = ListenerManager {
            rpc: listeners,
            mcp: HashMap::new(),
        };
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn manager_shutdown_closes_all_remaining_rpc_and_mcp_listeners() {
        let (completions, mut completion_receiver) = mpsc::channel(2);
        let mut manager = ListenerManager {
            rpc: HashMap::new(),
            mcp: HashMap::new(),
        };
        let factory = test_listener_factory(completions);

        ListenerManager::open(
            &mut manager.rpc,
            "RPC",
            None,
            0,
            factory.clone(),
            mpsc::channel(1).0,
            None,
        )
        .await
        .expect("RPC listener should open");
        ListenerManager::open(&mut manager.mcp, "MCP", None, 0, factory, mpsc::channel(1).0, None)
            .await
            .expect("MCP listener should open");

        manager.shutdown().await;

        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(1), completion_receiver.recv())
                .await
                .expect("all listener tasks should stop promptly")
                .expect("listener factory should report completion");
        }
        assert!(manager.rpc.is_empty());
        assert!(manager.mcp.is_empty());
    }
}
