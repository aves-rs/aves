use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use aves::auth::Jwt;
use aves::bus::BusRpcContext;
use aves::config::{
    ConfigError, GetConfigSectionInput, SaveConfigSectionInput, dispatch_get_config_section,
    dispatch_save_config_section,
};

use super::library::AssetLibrary;
use super::mcp_direct::McpAcpTransport;
use super::types::{AcpProvider, DiscussError, DiscussStreamEvent};

pub struct DiscussMcpDomain {
    pub server_name: &'static str,
    pub service: Arc<dyn DiscussMcpService>,
}

#[async_trait]
pub trait DiscussMcpService: Send + Sync {
    async fn start(
        &self,
        transport: McpAcpTransport,
        cancellation: CancellationToken,
        context: BusRpcContext,
        worker_jwt: Jwt,
    ) -> Option<Pin<Box<dyn Future<Output = ()> + Send>>>;
}

#[derive(Clone)]
pub struct DiscussBootOptions {
    /// Required host-owned definitions for agents, skills, commands, and teams.
    pub library: AssetLibrary,
    pub config_dir: PathBuf,
    /// Embedding application's MCP-domain catalog; Config is read directly from Aves.
    pub mcp_domains: fn() -> Result<Vec<DiscussMcpDomain>, DiscussError>,
    pub stream_tx: mpsc::Sender<DiscussStreamEvent>,
}

/// Discuss-owned persisted settings, including installed Provider definitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DiscussSettings {
    pub agents: HashMap<String, AcpProvider>,
    pub agents_workspace: Option<PathBuf>,
    pub subagent_retention_limit: u32,
}

impl Default for DiscussSettings {
    fn default() -> Self {
        Self {
            agents: HashMap::new(),
            agents_workspace: None,
            subagent_retention_limit: 10,
        }
    }
}

impl From<ConfigError> for DiscussError {
    fn from(error: ConfigError) -> Self {
        Self::Config {
            message: error.to_string(),
        }
    }
}

/// Loads the Discuss-owned section through the normal Config bus query.
pub(super) async fn load_discuss_settings(context: &BusRpcContext) -> Result<DiscussSettings, DiscussError> {
    let section = dispatch_get_config_section(
        context,
        GetConfigSectionInput {
            section: "discuss".to_owned(),
        },
    )
    .await?;
    Ok(serde_json::from_value(section.value)?)
}

/// Saves the complete Provider map as one field of the Discuss section.
pub(super) async fn persist_discuss_agents(
    context: &BusRpcContext,
    agents: &HashMap<String, AcpProvider>,
) -> Result<(), DiscussError> {
    let response = dispatch_save_config_section(
        context,
        SaveConfigSectionInput {
            section: "discuss".to_owned(),
            value: serde_json::json!({ "agents": agents }),
        },
    )
    .await?;
    if response.section != "discuss" {
        return Err(DiscussError::Config {
            message: format!("unexpected config section persisted: {}", response.section),
        });
    }
    Ok(())
}
