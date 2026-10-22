use std::{collections::HashMap, sync::Arc};

use iroh::{Endpoint, EndpointAddr};
use iroh_blobs::store::fs::FsStore;
use iroh_docs::{
    NamespaceId,
    api::{Doc, DocsApi},
};
use tokio::sync::{Mutex, RwLock};

use crate::bus::PeerConnectivity;

/// Native Iroh handles rebuilt together when peer connectivity changes.
pub(crate) struct FlowNativeServices {
    pub(crate) docs_api: DocsApi,
    pub(crate) endpoint: Endpoint,
    pub(crate) open_documents: Mutex<HashMap<NamespaceId, Doc>>,
    pub(crate) sync_peers: Mutex<HashMap<NamespaceId, EndpointAddr>>,
}

/// Shared Flow projection of the bus-owned native sync services.
#[derive(Clone)]
pub(crate) struct FlowSyncServices {
    pub(crate) blobs: FsStore,
    pub(crate) native: Arc<RwLock<FlowNativeServices>>,
    pub(crate) connectivity: PeerConnectivity,
}

impl FlowSyncServices {
    pub(crate) fn new(blobs: FsStore, docs_api: DocsApi, endpoint: Endpoint, connectivity: PeerConnectivity) -> Self {
        Self {
            blobs,
            native: Arc::new(RwLock::new(FlowNativeServices {
                docs_api,
                endpoint,
                open_documents: Mutex::new(HashMap::new()),
                sync_peers: Mutex::new(HashMap::new()),
            })),
            connectivity,
        }
    }
}
