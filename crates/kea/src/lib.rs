//! Knowledge notes, search, JSON Data, and Canvas dashboards on the Aves runtime.

mod actor;
mod data;
mod documents;
mod domain;
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
pub use types::{CreateKnowledgeRequest, KnowledgeConfig, KnowledgeError, UpsertDataRequest};
