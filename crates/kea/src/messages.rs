//! Typed RPC, actor, and broadcast messages for the Knowledge domain.

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use aves::bus::BusRpcContext;
use aves::utils::uuid::SpectaUuid;

use super::KnowledgeDomain;
use super::types::{
    CanvasDashboard, CanvasDashboardDeleteResult, CanvasDashboardLookup, CanvasDashboardSummaryList,
    CreateCanvasDashboardRequest, CreateKnowledgeRequest, DataList, DataObject, DeleteCanvasDashboardRequest,
    DeleteKnowledgeRequest, DeleteKnowledgeResult, GetCanvasDashboardRequest, GetKnowledgeRequest, KnowledgeError,
    KnowledgeRecord, KnowledgeRecordList, KnowledgeSearchOutcome, ListCanvasDashboardsRequest, ListDataRequest,
    ListKnowledgeRequest, SearchKnowledgeRequest, UpdateCanvasDashboardRequest, UpdateKnowledgeRequest,
    UpsertDataRequest, VaultKnowledgeTree,
};

/// Knowledge domain request wrapper.
///
/// Wraps a payload with telemetry context and reply channel for
/// request/response patterns through the event bus.
pub struct KnowledgeRequest {
    /// The request payload.
    pub payload: KnowledgeRequestPayload,
    /// Reply channel for the response.
    pub reply_to: oneshot::Sender<Result<KnowledgeResponse, KnowledgeError>>,
}

/// Payload variants for knowledge requests.
#[aves::bus_requests(marker = KnowledgeDomain, context = BusRpcContext, root_error = KnowledgeError, domain_error = KnowledgeError)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KnowledgeRequestPayload {
    /// Create a Markdown note in the active directory.
    #[operation(name = "createKnowledge", kind = "mutation", response = KnowledgeCreatedOutput, response_variant = "KnowledgeCreated")]
    CreateKnowledge { request: CreateKnowledgeRequest },

    /// List the directory's lightweight navigation tree.
    #[operation(name = "listKnowledgeTree", kind = "query", response = KnowledgeTreeListedOutput, response_variant = "KnowledgeTreeListed")]
    ListKnowledgeTree,

    /// List knowledge with optional filters and query text.
    #[operation(name = "listKnowledge", kind = "query", response = KnowledgeListedOutput, response_variant = "KnowledgeListed")]
    ListKnowledge { request: ListKnowledgeRequest },

    /// Get a note by its relative path.
    #[operation(name = "getKnowledge", kind = "query", response = KnowledgeFoundOutput, response_variant = "KnowledgeFound")]
    GetKnowledge { request: GetKnowledgeRequest },

    /// Update an existing directory note.
    #[operation(name = "updateKnowledge", kind = "mutation", response = KnowledgeUpdatedOutput, response_variant = "KnowledgeUpdated")]
    UpdateKnowledge { request: UpdateKnowledgeRequest },

    /// Delete a note by its relative path.
    #[operation(name = "deleteKnowledge", kind = "mutation", response = KnowledgeDeletedOutput, response_variant = "KnowledgeDeleted")]
    DeleteKnowledge { request: DeleteKnowledgeRequest },

    /// Search directory notes with TurboVault.
    #[operation(name = "searchKnowledge", kind = "query", response = KnowledgeSearchedOutput, response_variant = "KnowledgeSearched")]
    SearchKnowledge { request: SearchKnowledgeRequest },

    /// Replace the JSON object at an exact, case-sensitive key or create it.
    #[operation(name = "upsertData", kind = "mutation", response = DataUpsertedOutput, response_variant = "DataUpserted")]
    UpsertData { request: UpsertDataRequest },

    /// List keys by last-write date with pagination.
    #[operation(name = "listData", kind = "query", response = DataListedOutput, response_variant = "DataListed")]
    ListData { request: ListDataRequest },

    /// Read the JSON object at an exact key.
    #[operation(name = "getData", kind = "query", response = DataFoundOutput, response_variant = "DataFound")]
    GetData { key: String },

    /// Delete an existing JSON object by exact key.
    #[operation(name = "deleteData", kind = "mutation", response = DataDeletedOutput, response_variant = "DataDeleted")]
    DeleteData { key: String },

    /// Create a file-backed Canvas dashboard.
    #[operation(name = "createCanvasDashboard", kind = "mutation", response = CanvasDashboardCreatedOutput, response_variant = "CanvasDashboardCreated")]
    CreateCanvasDashboard { request: CreateCanvasDashboardRequest },

    /// Get a file-backed Canvas dashboard by id.
    #[operation(name = "getCanvasDashboard", kind = "query", response = CanvasDashboardFoundOutput, response_variant = "CanvasDashboardFound")]
    GetCanvasDashboard { request: GetCanvasDashboardRequest },

    /// Replace a file-backed Canvas dashboard.
    #[operation(name = "updateCanvasDashboard", kind = "mutation", response = CanvasDashboardUpdatedOutput, response_variant = "CanvasDashboardUpdated")]
    UpdateCanvasDashboard { request: UpdateCanvasDashboardRequest },

    /// Delete a file-backed Canvas dashboard by id.
    #[operation(name = "deleteCanvasDashboard", kind = "mutation", response = CanvasDashboardDeletedOutput, response_variant = "CanvasDashboardDeleted")]
    DeleteCanvasDashboard { request: DeleteCanvasDashboardRequest },

    /// List file-backed Canvas dashboard summaries.
    #[operation(name = "listCanvasDashboards", kind = "query", response = CanvasDashboardListedOutput, response_variant = "CanvasDashboardListed")]
    ListCanvasDashboards { request: ListCanvasDashboardsRequest },

    /// Reopens the active directory after a Knowledge root change.
    #[serde(skip)]
    KnowledgeConfigChanged,

    /// Runs maintenance on the already-open Knowledge Data database during shutdown.
    #[serde(skip)]
    StopRuntime,
}

/// Response types for knowledge operations.
#[aves::bus_responses]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KnowledgeResponse {
    /// Internal signal that the directory's visible records changed.
    #[serde(skip)]
    KnowledgeRecordsChanged,
    /// Internal acknowledgement for Knowledge database maintenance.
    #[serde(skip)]
    RuntimeStopped,
    /// Created knowledge.
    KnowledgeCreated { knowledge: KnowledgeRecord },
    /// Retrieved knowledge, if present.
    KnowledgeFound { knowledge: KnowledgeRecord },
    /// Updated knowledge.
    KnowledgeUpdated { knowledge: KnowledgeRecord },
    /// Hard-delete result.
    KnowledgeDeleted { result: DeleteKnowledgeResult },
    /// Listed knowledge.
    KnowledgeListed(KnowledgeRecordList),
    /// Listed lightweight navigation tree entries.
    KnowledgeTreeListed { entries: VaultKnowledgeTree },
    /// Vault search response.
    KnowledgeSearched(KnowledgeSearchOutcome),
    /// Stored JSON object after a successful replacement.
    DataUpserted { data: DataObject },
    /// Stored JSON object for an exact key.
    DataFound { data: DataObject },
    /// Page of JSON object keys and write timestamps.
    DataListed { data: DataList },
    /// Confirmed deletion of an existing key.
    DataDeleted { key: String },
    /// Created Canvas dashboard.
    CanvasDashboardCreated { dashboard: CanvasDashboard },
    /// Retrieved Canvas dashboard, if present.
    CanvasDashboardFound { dashboard: CanvasDashboardLookup },
    /// Updated Canvas dashboard.
    CanvasDashboardUpdated { dashboard: CanvasDashboard },
    /// Canvas dashboard deletion result.
    CanvasDashboardDeleted {
        id: SpectaUuid,
        result: CanvasDashboardDeleteResult,
    },
    /// Listed Canvas dashboard summaries.
    CanvasDashboardListed { dashboards: CanvasDashboardSummaryList },
}

/// Events broadcast by the knowledge domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KnowledgeBroadcast {
    /// The visible Knowledge record set changed and clients should refresh it.
    RecordsChanged,
    /// A Canvas dashboard was created.
    CanvasDashboardCreated { dashboard_id: SpectaUuid },
    /// A Canvas dashboard was updated.
    CanvasDashboardUpdated { dashboard_id: SpectaUuid },
    /// A Canvas dashboard was deleted.
    CanvasDashboardDeleted { dashboard_id: SpectaUuid },
}
