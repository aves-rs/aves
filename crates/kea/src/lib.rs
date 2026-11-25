//! Knowledge notes, search, JSON Data, and Canvas dashboards on the Aves runtime.

mod actor;
mod data;
mod documents;
mod domain;
mod full_text;
mod handle;
mod messages;
mod migrations;
mod types;

pub use domain::KnowledgeDomain;
pub use messages::knowledge_operation_descriptors_with_root_error;
pub use messages::{
    CreateKnowledgeInput, KnowledgeBroadcast, UpsertDataInput, dispatch_create_knowledge, dispatch_upsert_data,
    rpc_router,
};
pub use messages::{
    DeleteKnowledgeInput, GetKnowledgeInput, GetVaultStatsInput, KnowledgeCreatedOutput, KnowledgeDeletedOutput,
    KnowledgeFoundOutput, KnowledgeListedOutput, KnowledgeTreeListedOutput, KnowledgeUpdatedOutput, ListKnowledgeInput,
    ListKnowledgeTreeInput, UpdateKnowledgeInput, VaultRegisteredOutput, VaultStatsFoundOutput,
    VaultUnregisteredOutput, dispatch_delete_knowledge, dispatch_get_knowledge, dispatch_get_vault_stats,
    dispatch_list_knowledge, dispatch_list_knowledge_tree, dispatch_register_vault, dispatch_unregister_vault,
    dispatch_update_knowledge,
};
pub use messages::{KnowledgeSearchedOutput, SearchKnowledgeInput, dispatch_search_knowledge};
pub use messages::{RegisterVaultInput, UnregisterVaultInput};
pub use types::{CreateKnowledgeRequest, KnowledgeConfig, KnowledgeError, UpsertDataRequest};
pub use types::{
    DeleteKnowledgeResult, KnowledgeContentType, VaultCreateKnowledgeRequest, VaultIndexState, VaultKnowledge,
    VaultKnowledgeFilters, VaultKnowledgeTreeEntry, VaultKnowledgeTreeEntryKind, VaultListKnowledgeRequest, VaultStats,
    VaultUpdateKnowledgeRequest,
};
pub use types::{SearchMode, VaultKnowledgeSearchResponse, VaultKnowledgeSearchResult, VaultSearchKnowledgeRequest};
