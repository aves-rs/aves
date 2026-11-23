use aves::auth::{Jwt, MachineIssuerId};
use aves::flow::NodeExecutionId;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Deserialize;
use uuid::Uuid;

use super::error::DiscussError;
use super::messages::DiscussResponse;
use super::storage::{DiscussPersistence, INSERT_IDENTITY_BINDING_SQL, READ_IDENTITY_BINDING_SQL};
use super::types::DiscussSessionId;

const FLOW_NODE_SUBJECT_PREFIX: &str = "flow-node:";

#[derive(Deserialize)]
struct IssuerClaim {
    iss: MachineIssuerId,
}

/// Reads a trusted just-issued token's claim; this does not verify a token.
fn issuer_claim(token: &str) -> Result<MachineIssuerId, DiscussError> {
    let payload = token
        .split('.')
        .nth(1)
        .ok_or(DiscussError::SessionIdentityInvalidIssuer)?;
    let decoded = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| DiscussError::SessionIdentityInvalidIssuer)?;
    let claim: IssuerClaim =
        serde_json::from_slice(&decoded).map_err(|_| DiscussError::SessionIdentityInvalidIssuer)?;
    Ok(claim.iss)
}

pub(super) fn flow_node_subject(id: NodeExecutionId) -> String {
    let mut subject = FLOW_NODE_SUBJECT_PREFIX.to_owned();
    subject.push_str(&id.to_string());
    subject
}

pub(super) struct Binding<'a> {
    pub(super) token: &'a Jwt,
    pub(super) subject: &'a str,
    pub(super) session_id: DiscussSessionId,
    pub(super) provider: Option<&'a str>,
    pub(super) agent: Option<&'a str>,
    pub(super) node_id: Option<NodeExecutionId>,
}

pub(super) async fn persist_binding(
    persistence: &DiscussPersistence,
    binding: Binding<'_>,
) -> Result<(), DiscussError> {
    let issuer = issuer_claim(binding.token.as_str())?;
    let conn = persistence.acquire_connection().await?;
    conn.execute(
        INSERT_IDENTITY_BINDING_SQL,
        (
            issuer.to_string(),
            binding.subject.to_owned(),
            binding.session_id.to_string(),
            binding.provider.map(str::to_owned),
            binding.agent.map(str::to_owned),
            binding.node_id.map(|id| id.to_string()),
            chrono::Utc::now().timestamp_millis(),
        ),
    )
    .await?;
    let mut rows = conn
        .query(
            READ_IDENTITY_BINDING_SQL,
            (issuer.to_string(), binding.subject.to_owned()),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or(DiscussError::SessionIdentityUnknown)?;
    if row.get::<i64>(4)? != 0 {
        return Err(DiscussError::SessionIdentityAmbiguous);
    }
    Ok(())
}

pub(super) async fn resolve(
    persistence: &DiscussPersistence,
    issuer: MachineIssuerId,
    subject: String,
) -> Result<DiscussResponse, DiscussError> {
    let conn = persistence.acquire_connection().await?;
    let mut rows = conn
        .query(READ_IDENTITY_BINDING_SQL, (issuer.to_string(), subject))
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or(DiscussError::SessionIdentityUnknown)?;
    if row.get::<i64>(4)? != 0 {
        return Err(DiscussError::SessionIdentityAmbiguous);
    }
    let provider: Option<String> = row.get(1)?;
    let agent: Option<String> = row.get(2)?;
    let (Some(agent_provider), Some(agent_name)) = (provider, agent) else {
        return Err(DiscussError::SessionIdentityUnassigned);
    };
    let invalid_row = || DiscussError::Persistence {
        code: "db_query".to_owned(),
        message: "Invalid session identity binding".to_owned(),
    };
    let session: String = row.get(0)?;
    let node: Option<String> = row.get(3)?;
    Ok(DiscussResponse::SessionIdentityResolved {
        discuss_session_id: session.parse().map_err(|_| invalid_row())?,
        agent_provider,
        agent_name,
        node_execution_id: node
            .map(|value| Uuid::parse_str(&value).map(NodeExecutionId))
            .transpose()
            .map_err(|_| invalid_row())?,
    })
}
