use std::fs::create_dir_all;
use std::path::Path;
use std::sync::{Arc, Mutex};

use iroh::{Endpoint, SecretKey, endpoint::presets, protocol::Router};
use iroh_blobs::{BlobsProtocol, api::Store as BlobsStore, protocol::ALPN as BLOBS_ALPN, store::fs::FsStore};
use iroh_docs::{api::DocsApi, net::ALPN as DOCS_ALPN, protocol::Docs};
use iroh_gossip::net::{GOSSIP_ALPN, Gossip};

use crate::machine::{MachinePeerServices, PeerConnectivity};

use super::hook::{
    FlowAppHookContext, FlowAppHookFuture, FlowAppLandingContext, FlowAppTakeoffContext, FlowAppTakeoffFuture,
    FlowAppTakeoffResult, FlowAppWorkspaceHook, FlowWorkspaceHookBoot, FlowWorkspaceHookFuture,
};

#[derive(Default)]
struct RecordingWorkspaceHook {
    calls: Mutex<Vec<FlowAppHookContext>>,
}

impl FlowAppWorkspaceHook for RecordingWorkspaceHook {
    fn takeoff(&self, context: FlowAppTakeoffContext) -> FlowAppTakeoffFuture<'_> {
        self.calls.lock().unwrap().push(context.execution.clone());
        Box::pin(async move {
            std::fs::create_dir_all(&context.execution.execution_root)?;
            Ok(FlowAppTakeoffResult {
                execution_root: context.execution.execution_root,
                imported_workspace_root: None,
            })
        })
    }

    fn landing(&self, context: FlowAppLandingContext) -> FlowAppHookFuture<'_> {
        self.calls.lock().unwrap().push(context.execution);
        Box::pin(async { Ok(()) })
    }

    fn release_artifacts(&self, context: FlowAppHookContext) -> FlowAppHookFuture<'_> {
        self.calls.lock().unwrap().push(context);
        Box::pin(async { Ok(()) })
    }
}

pub(super) fn recording_workspace_hook(_boot: FlowWorkspaceHookBoot) -> FlowWorkspaceHookFuture {
    Box::pin(async { Arc::new(RecordingWorkspaceHook::default()) as Arc<dyn FlowAppWorkspaceHook> })
}

pub(super) struct NativeDocsFixture {
    _endpoint: Endpoint,
    _router: Router,
    pub(super) blobs: FsStore,
    pub(super) docs_api: DocsApi,
}

impl NativeDocsFixture {
    pub(super) async fn open(data_root: &Path) -> Self {
        let root = data_root.join(".native-docs");

        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(SecretKey::generate())
            .bind()
            .await
            .unwrap();
        let gossip = Gossip::builder().spawn(endpoint.clone());

        let blobs = FsStore::load(root.join("flow-workspace-blobs"))
            .await
            .unwrap();
        let blob_store: BlobsStore = blobs.clone().into();

        create_dir_all(root.join("flow-workspace-docs")).unwrap();
        let docs = Docs::persistent(root.join("flow-workspace-docs"))
            .spawn(endpoint.clone(), blob_store.clone(), gossip.clone())
            .await
            .unwrap();
        let docs_api = docs.api().clone();

        let router = Router::builder(endpoint.clone())
            .accept(BLOBS_ALPN, BlobsProtocol::new(&blob_store, None))
            .accept(GOSSIP_ALPN, gossip.clone())
            .accept(DOCS_ALPN, docs)
            .spawn();

        Self {
            _endpoint: endpoint,
            _router: router,
            blobs,
            docs_api,
        }
    }

    pub(super) fn sync_services(&self) -> MachinePeerServices {
        MachinePeerServices::new(
            self.blobs.clone(),
            self.docs_api.clone(),
            self._endpoint.clone(),
            PeerConnectivity::new(true),
        )
    }
}
