use std::{
    collections::HashMap,
    error::Error as StdError,
    fmt,
    future::Future,
    io::{Error as IoError, ErrorKind},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use iroh::{
    Endpoint, EndpointAddr, RelayMode, SecretKey,
    endpoint::{Connection, ConnectionError, PortmapperConfig, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
use iroh_blobs::{BlobsProtocol, api::Store, store::fs::FsStore};
use iroh_docs::NamespaceId;
use iroh_docs::protocol::Docs;
use iroh_gossip::net::Gossip;
use thiserror::Error;

use crate::auth::{AUTH_ALPN, AuthFirstContactHandler, PairingOwner};
use crate::flow::{FlowNativeServices, FlowSyncServices};

use super::remote;

const MAX_REQUEST_SIZE: usize = 1024 * 1024;
const MAX_RESPONSE_SIZE: usize = 1024 * 1024;

pub(crate) type RequestFuture = Pin<Box<dyn Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send>>;
pub(crate) type RequestHandler = dyn Fn(iroh::PublicKey, Vec<u8>) -> RequestFuture + Send + Sync;

/// Process-wide gate for native Iroh peer connectivity.
#[derive(Clone)]
pub(crate) struct PeerConnectivity(Arc<ConnectivityState>);

struct ConnectivityState {
    enabled: AtomicBool,
    controller: OnceLock<Weak<ConnectivityController>>,
}

impl fmt::Debug for PeerConnectivity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PeerConnectivity")
            .field("enabled", &self.is_enabled())
            .finish()
    }
}

impl PeerConnectivity {
    /// Creates a connectivity gate with the requested initial state.
    pub(crate) fn new(enabled: bool) -> Self {
        Self(Arc::new(ConnectivityState {
            enabled: AtomicBool::new(enabled),
            controller: OnceLock::new(),
        }))
    }

    /// Applies a live transport transition before acknowledging the new state.
    pub(crate) async fn set_enabled(&self, enabled: bool) -> Result<(), super::BusControlError> {
        let controller = self
            .0
            .controller
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| super::BusControlError::Serve {
                protocol: "iroh",
                source: Box::new(IrohSubstrateError::Stopped),
            })?;
        tokio::spawn(async move { controller.transition(enabled).await })
            .await
            .map_err(|source| super::BusControlError::Join {
                protocol: "iroh",
                source,
            })?
            .map_err(|source| super::BusControlError::Serve {
                protocol: "iroh",
                source: Box::new(source),
            })
    }

    /// Returns whether native peer networking is currently enabled.
    pub(crate) fn is_enabled(&self) -> bool {
        self.0.enabled.load(Ordering::Acquire)
    }
}

#[derive(Clone)]
struct GatedGossip {
    gossip: Gossip,
    connectivity: PeerConnectivity,
}

impl fmt::Debug for GatedGossip {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("GatedGossip").finish()
    }
}

impl ProtocolHandler for GatedGossip {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        if !self.connectivity.is_enabled() {
            connection.close(0u32.into(), b"peer connectivity disabled");
            return Ok(());
        }
        self.gossip.accept(connection).await
    }

    async fn shutdown(&self) {
        let _ = self.gossip.shutdown().await;
    }
}

#[derive(Clone)]
pub(crate) struct IrohProtocolHandler {
    request_handler: Arc<RequestHandler>,
}

impl IrohProtocolHandler {
    pub(crate) fn new(request_handler: Arc<RequestHandler>) -> Self {
        Self { request_handler }
    }
}

impl std::fmt::Debug for IrohProtocolHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("IrohProtocolHandler").finish()
    }
}

impl ProtocolHandler for IrohProtocolHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let remote_node_id = connection.remote_id();

        loop {
            let (mut send, mut recv) = match connection.accept_bi().await {
                Ok(streams) => streams,
                Err(ConnectionError::LocallyClosed) | Err(ConnectionError::ApplicationClosed(_)) => return Ok(()),
                Err(source) => return Err(AcceptError::from_err(source)),
            };

            let request = recv
                .read_to_end(MAX_REQUEST_SIZE)
                .await
                .map_err(AcceptError::from_err)?;
            let response = (self.request_handler)(remote_node_id, request)
                .await
                .map_err(AcceptError::from_boxed)?;
            if response.len() > MAX_RESPONSE_SIZE {
                return Err(AcceptError::from_err(IoError::new(
                    ErrorKind::InvalidData,
                    "iroh Bus response exceeds the configured limit",
                )));
            }

            send.write_all(&response)
                .await
                .map_err(AcceptError::from_err)?;

            send.finish().map_err(AcceptError::from_err)?;
        }
    }
}

/// The single native iroh substrate owned by the bus layer.
pub(crate) struct IrohSubstrate {
    endpoint: Endpoint,
    gossip: Gossip,
    blob_protocol: BlobsProtocol,
    docs_protocol: Docs,
    connectivity: PeerConnectivity,
    root: PathBuf,
    secret_key: SecretKey,
    services: FlowSyncServices,
}

pub(crate) struct RunningIrohSubstrate {
    controller: Arc<ConnectivityController>,
}

struct ConnectivityController {
    root: PathBuf,
    secret_key: SecretKey,
    services: FlowSyncServices,
    bus: Option<IrohProtocolHandler>,
    auth: AuthFirstContactHandler,
    pairing: PairingOwner,
    lifecycle: tokio::sync::Mutex<ConnectivityLifecycle>,
}

struct ConnectivityLifecycle {
    router: Option<Router>,
    stopped: bool,
}

#[derive(Clone, Debug)]
struct PersistentBlobs(BlobsProtocol);

impl ProtocolHandler for PersistentBlobs {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        self.0.accept(connection).await
    }
}

impl IrohSubstrate {
    pub(crate) async fn prepare(
        root: &Path,
        secret_key: &SecretKey,
        connectivity: PeerConnectivity,
    ) -> Result<Self, IrohSubstrateError> {
        let blobs = FsStore::load(root.join("flow-workspace-blobs"))
            .await
            .map_err(|source| IrohSubstrateError::Blob {
                source: Box::new(source),
            })?;
        std::fs::create_dir_all(root.join("flow-workspace-docs")).map_err(|source| IrohSubstrateError::DocsIo {
            source: Box::new(source),
        })?;

        let endpoint = bind_endpoint(secret_key, connectivity.is_enabled()).await?;
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let blob_store: Store = blobs.clone().into();
        let docs = Docs::persistent(root.join("flow-workspace-docs"))
            .spawn(endpoint.clone(), blob_store.clone(), gossip.clone())
            .await
            .map_err(|error| IrohSubstrateError::Docs { error })?;
        let docs_api = docs.api().clone();
        let blob_protocol = BlobsProtocol::new(&blob_store, None);
        let services = FlowSyncServices::new(blobs.clone(), docs_api.clone(), endpoint.clone(), connectivity.clone());

        Ok(Self {
            endpoint,
            gossip,
            blob_protocol,
            docs_protocol: docs,
            connectivity,
            root: root.to_path_buf(),
            secret_key: secret_key.clone(),
            services,
        })
    }

    pub(crate) fn flow_services(&self) -> FlowSyncServices {
        self.services.clone()
    }

    pub(crate) fn start(
        self,
        bus: Option<IrohProtocolHandler>,
        auth: AuthFirstContactHandler,
        pairing: PairingOwner,
    ) -> RunningIrohSubstrate {
        let Self {
            endpoint,
            blob_protocol,
            docs_protocol,
            gossip,
            connectivity,
            root,
            secret_key,
            services,
            ..
        } = self;
        let router = start_router(
            endpoint,
            bus.clone(),
            auth.clone(),
            blob_protocol,
            docs_protocol,
            gossip,
            connectivity.clone(),
        );
        let controller = Arc::new(ConnectivityController {
            root,
            secret_key,
            services,
            bus,
            auth,
            pairing,
            lifecycle: tokio::sync::Mutex::new(ConnectivityLifecycle {
                router: Some(router),
                stopped: false,
            }),
        });
        let _ = connectivity.0.controller.set(Arc::downgrade(&controller));
        RunningIrohSubstrate { controller }
    }
}

impl RunningIrohSubstrate {
    pub(crate) async fn endpoint(&self) -> Endpoint {
        self.controller
            .services
            .native
            .read()
            .await
            .endpoint
            .clone()
    }

    pub(crate) async fn remote_endpoint(&self) -> Result<Endpoint, super::RemoteOperationError> {
        let endpoint = self.endpoint().await;
        if !self.controller.services.connectivity.is_enabled() {
            return Err(super::RemoteOperationError::ConnectivityDisabled);
        }
        Ok(endpoint)
    }

    pub(crate) async fn shutdown_router(&self) -> Result<(), IrohSubstrateError> {
        let mut lifecycle = self.controller.lifecycle.lock().await;
        lifecycle.stopped = true;
        self.controller
            .services
            .connectivity
            .0
            .enabled
            .store(false, Ordering::Release);
        if let Some(router) = &lifecycle.router {
            shutdown_router(router).await?;
        }
        lifecycle.router = None;
        self.controller
            .services
            .blobs
            .shutdown()
            .await
            .map_err(|source| IrohSubstrateError::Blob {
                source: Box::new(source),
            })
    }

    pub(crate) async fn close_endpoint(&self) {
        self.endpoint().await.close().await;
    }
}

impl ConnectivityController {
    async fn transition(&self, enabled: bool) -> Result<(), IrohSubstrateError> {
        let mut lifecycle = self.lifecycle.lock().await;
        if lifecycle.stopped {
            return Err(IrohSubstrateError::Stopped);
        }
        let old_endpoint = self.services.native.read().await.endpoint.clone();
        if self.services.connectivity.is_enabled() == enabled && !old_endpoint.is_closed() {
            return Ok(());
        }
        self.services
            .connectivity
            .0
            .enabled
            .store(false, Ordering::Release);
        let mut native = self.services.native.write().await;
        self.pairing
            .lock()
            .expect("Auth pairing coordinator lock poisoned")
            .detach_endpoint();
        old_endpoint.close().await;
        if let Some(router) = &lifecycle.router {
            shutdown_router(router).await?;
        }
        lifecycle.router = None;
        native.open_documents.lock().await.clear();
        let sync_peers = native.sync_peers.lock().await.clone();

        match self.reopen(enabled, sync_peers.clone()).await {
            Ok((replacement, router)) => {
                if enabled {
                    self.pairing
                        .lock()
                        .expect("Auth pairing coordinator lock poisoned")
                        .attach_endpoint(replacement.endpoint.clone(), replacement.endpoint.addr());
                }
                *native = replacement;
                lifecycle.router = Some(router);
                self.services
                    .connectivity
                    .0
                    .enabled
                    .store(enabled, Ordering::Release);
                tracing::info!(enabled, "Iroh peer connectivity changed");
                Ok(())
            }
            Err(error) => {
                // A failed enable must leave local storage usable without opening networking.
                if enabled {
                    match self.reopen(false, sync_peers).await {
                        Ok((replacement, router)) => {
                            *native = replacement;
                            lifecycle.router = Some(router);
                        }
                        Err(recovery) => tracing::error!(%recovery, "Failed to restore local Iroh services"),
                    }
                }
                Err(error)
            }
        }
    }

    async fn reopen(
        &self,
        enabled: bool,
        sync_peers: HashMap<NamespaceId, EndpointAddr>,
    ) -> Result<(FlowNativeServices, Router), IrohSubstrateError> {
        let endpoint = bind_endpoint(&self.secret_key, enabled).await?;
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let blob_store: Store = self.services.blobs.clone().into();
        let docs = match Docs::persistent(self.root.join("flow-workspace-docs"))
            .spawn(endpoint.clone(), blob_store.clone(), gossip.clone())
            .await
        {
            Ok(docs) => docs,
            Err(error) => {
                let _ = gossip.shutdown().await;
                endpoint.close().await;
                return Err(IrohSubstrateError::Docs { error });
            }
        };
        let native = FlowNativeServices {
            docs_api: docs.api().clone(),
            endpoint: endpoint.clone(),
            open_documents: tokio::sync::Mutex::new(HashMap::new()),
            sync_peers: tokio::sync::Mutex::new(sync_peers.clone()),
        };
        let router = start_router(
            endpoint,
            self.bus.clone(),
            self.auth.clone(),
            BlobsProtocol::new(&blob_store, None),
            docs,
            gossip,
            self.services.connectivity.clone(),
        );
        if enabled {
            let result = async {
                for (namespace, source) in sync_peers {
                    if let Some(document) = native.docs_api.open(namespace).await? {
                        document.start_sync(vec![source]).await?;
                        native
                            .open_documents
                            .lock()
                            .await
                            .insert(namespace, document);
                    }
                }
                Ok::<(), wasmtime::anyhow::Error>(())
            }
            .await;
            if let Err(error) = result {
                native.endpoint.close().await;
                shutdown_router(&router).await?;
                return Err(IrohSubstrateError::Docs { error });
            }
        }
        Ok((native, router))
    }
}

fn start_router(
    endpoint: Endpoint,
    bus: Option<IrohProtocolHandler>,
    auth: AuthFirstContactHandler,
    blobs: BlobsProtocol,
    docs: Docs,
    gossip: Gossip,
    connectivity: PeerConnectivity,
) -> Router {
    let mut router = Router::builder(endpoint);
    if let Some(bus) = bus {
        router = router.accept(remote::BUS_ALPN, bus);
    }
    router
        .accept(AUTH_ALPN, auth)
        .accept(iroh_blobs::protocol::ALPN, PersistentBlobs(blobs))
        .accept(iroh_gossip::net::GOSSIP_ALPN, GatedGossip { gossip, connectivity })
        .accept(iroh_docs::net::ALPN, docs)
        .spawn()
}

async fn bind_endpoint(secret_key: &SecretKey, connectivity_enabled: bool) -> Result<Endpoint, IrohSubstrateError> {
    let endpoint_builder = if connectivity_enabled {
        Endpoint::builder(presets::N0)
    } else {
        Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .portmapper_config(PortmapperConfig::Disabled)
            .clear_ip_transports()
            .clear_relay_transports()
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
    };
    endpoint_builder
        .secret_key(secret_key.clone())
        .bind()
        .await
        .map_err(IrohSubstrateError::Endpoint)
}

async fn shutdown_router(router: &Router) -> Result<(), IrohSubstrateError> {
    tokio::time::timeout(Duration::from_secs(30), router.shutdown())
        .await
        .map_err(|_| IrohSubstrateError::ShutdownTimeout)?
        .map_err(|source| IrohSubstrateError::RouterShutdown {
            source: Box::new(source),
        })
}

#[derive(Debug, Error)]
pub(crate) enum IrohSubstrateError {
    #[error("failed to open blob store: {source}")]
    Blob {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("failed to open docs store: {error}")]
    Docs { error: wasmtime::anyhow::Error },
    #[error("failed to create docs store directory: {source}")]
    DocsIo {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("failed to bind iroh endpoint: {0}")]
    Endpoint(#[source] iroh::endpoint::BindError),
    #[error("invalid local-only iroh bind address: {0}")]
    LocalBindAddress(#[from] iroh::endpoint::InvalidSocketAddr),
    #[error("failed to shut down iroh router: {source}")]
    RouterShutdown {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("timed out shutting down iroh substrate")]
    ShutdownTimeout,
    #[error("Iroh runtime is shutting down")]
    Stopped,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn disabled_peer_connectivity_binds_without_relays() {
        let endpoint = bind_endpoint(&SecretKey::generate(), false).await.unwrap();
        assert_eq!(endpoint.addr().relay_urls().count(), 0);
        assert!(
            endpoint
                .addr()
                .ip_addrs()
                .all(|address| address.ip().is_loopback())
        );
        endpoint.close().await;
    }
}
