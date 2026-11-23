use std::path::{Component, Path, PathBuf};

use agent_client_protocol::v1::schema::McpServer;
use serde::{Deserialize, Serialize};
use serde_json::{Error as JsonError, Value, to_value};
use specta::{DataType, Generics, Type, TypeCollection};

use aves::flow::AgentNodeConfig;

use super::error::DiscussError;
/// A broad operation understood by permission translators.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Read,
    Write,
    Execute,
}

/// Permission effects in increasing order of restriction.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Effect {
    Allow,
    Ask,
    Deny,
}

/// A whole-operation rule.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, ::schemars::JsonSchema)]
pub struct Rule {
    pub operation: Operation,
    pub effect: Effect,
}

/// One resolved source of operation rules.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, ::schemars::JsonSchema)]
pub struct RuleSet {
    pub rules: Vec<Rule>,
}

/// Explicit command-line tool identity, without credentials or process arguments.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ::schemars::JsonSchema)]
pub struct SessionCliTool {
    /// Executable name or path selected by the user.
    pub command: PathBuf,
    /// Whether this session requires a separately provisioned login.
    pub login: SessionCliLogin,
}

/// Authentication requirement; credential material is never part of the launch configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, ::schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SessionCliLogin {
    /// No per-session login is requested.
    None,
    /// A per-session login must be provisioned securely.
    Required,
}

/// Session-specific inputs for a future provider launch.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ::schemars::JsonSchema)]
pub struct SessionLaunchConfig {
    #[serde(default)]
    pub rule_sources: Vec<RuleSet>,
    #[serde(default)]
    pub mcp_servers: Vec<McpServer>,
    #[serde(default)]
    pub plugin_references: Vec<String>,
    /// Explicit CLI tool requests; empty unless supplied by the caller.
    #[serde(default)]
    pub cli_tools: Vec<SessionCliTool>,
}

impl SessionLaunchConfig {
    /// Set the typed launch configuration on a Flow AgentNode definition.
    pub fn set_on_agent_node(&self, node: &mut AgentNodeConfig) -> Result<(), JsonError> {
        node.launch_config = Some(to_value(self)?);
        Ok(())
    }
}

impl Type for SessionLaunchConfig {
    fn inline(type_map: &mut TypeCollection, generics: Generics<'_>) -> DataType {
        Value::inline(type_map, generics)
    }
}

/// A limitation encountered when applying a session's launch configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LaunchLimitation {
    /// The requested setting affected by the limitation.
    pub subject: LaunchLimitationSubject,
    /// Why the setting could not be applied exactly.
    pub reason: LaunchLimitationReason,
}

/// The setting affected by a launch limitation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LaunchLimitationSubject {
    /// A requested permission and the native outcome.
    Permission {
        /// The affected operation.
        operation: Operation,
        /// The requested permission effect.
        requested: Effect,
        /// The native or inherited outcome.
        actual: LaunchPermissionOutcome,
    },
    /// Session-only configuration isolation is unavailable.
    ConfigurationIsolation,
    /// An affected MCP server, identified by its requested name.
    McpServer {
        /// The requested server name.
        name: String,
    },
    /// An affected plugin reference, identified by its request index.
    PluginReference {
        /// Index into `SessionLaunchConfig::plugin_references`.
        index: usize,
    },
}

/// The native permission outcome, or unverified inherited settings.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LaunchPermissionOutcome {
    /// A native permission effect.
    Effect(Effect),
    /// A native mode identifier.
    Mode(String),
    /// Inherited settings whose effect has not been verified.
    Inherited,
}

/// The reason a requested launch setting could not be applied exactly.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LaunchLimitationReason {
    /// The native harness lacks the requested control.
    UnsupportedNativeControl,
    /// Native mode selection couples multiple permissions.
    ModeCoupling,
    /// Inherited configuration remains unverified.
    InheritedUnverifiedConfiguration,
    /// The required option was not advertised.
    UnavailableAdvertisedOption,
    /// The requested transport is unsupported.
    UnsupportedTransport,
    /// The requested plugin reference is unsupported.
    UnsupportedPluginReference,
}

/// Persisted launch specification and session defaults for an installed ACP Provider.
#[derive(Debug, Clone, Serialize, Deserialize, ::schemars::JsonSchema)]
pub struct AcpProvider {
    #[serde(flatten)]
    server: McpServer,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    default_session_config: Value,
    #[serde(default, alias = "sluice_mcp_enabled")]
    application_mcp_enabled: bool,
    /// Editable paths relative to the agents workspace; None uses provider defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    workspace_copy_sources: Option<Vec<String>>,
    /// Provider-owned managed configuration document, if customized.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    managed_document: Option<String>,
}

impl AcpProvider {
    pub(crate) fn managed_document(&self) -> Option<&str> {
        self.managed_document.as_deref()
    }

    pub(crate) fn set_managed_document(&mut self, document: Option<String>) {
        self.managed_document = document;
    }
    pub fn server(&self) -> &McpServer {
        &self.server
    }

    pub fn application_mcp_enabled(&self) -> bool {
        self.application_mcp_enabled
    }

    pub fn default_session_config(&self) -> &Value {
        &self.default_session_config
    }

    pub fn replace_default_session_config(&mut self, value: Value) {
        self.default_session_config = value;
    }

    pub(crate) fn workspace_copy_sources(&self) -> Option<&[String]> {
        self.workspace_copy_sources.as_deref()
    }

    pub(crate) fn validate_workspace_copy_sources(&self) -> Result<(), DiscussError> {
        for source in self.workspace_copy_sources.as_deref().unwrap_or_default() {
            let path = Path::new(source);
            let windows_drive = source.as_bytes().get(1) == Some(&b':')
                && source
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphabetic);
            let invalid = if source.is_empty() || source.contains('\\') || source.contains('\0') || windows_drive {
                Some("must be a nonempty portable relative path")
            } else if path.is_absolute() {
                Some("must be relative to the agents workspace")
            } else if path.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            }) {
                Some("must not contain parent or root components")
            } else if !path
                .components()
                .any(|component| matches!(component, Component::Normal(_)))
            {
                Some("must not select the whole agents workspace")
            } else if path
                .components()
                .filter(|component| matches!(component, Component::Normal(_)))
                .count()
                == 1
                && path
                    .components()
                    .any(|component| component.as_os_str() == ".agents")
            {
                Some(".agents is always copied and cannot be edited")
            } else {
                None
            };
            if let Some(violation) = invalid {
                return Err(DiscussError::InvalidWorkspaceCopySource {
                    path: source.clone(),
                    violation: violation.to_string(),
                });
            }
        }
        Ok(())
    }
}

impl Type for AcpProvider {
    fn inline(type_map: &mut TypeCollection, generics: Generics<'_>) -> DataType {
        Value::inline(type_map, generics)
    }
}

impl From<AcpProvider> for McpServer {
    fn from(val: AcpProvider) -> Self {
        val.server
    }
}

impl From<McpServer> for AcpProvider {
    fn from(server: McpServer) -> Self {
        Self {
            server,
            default_session_config: Value::Null,
            application_mcp_enabled: false,
            workspace_copy_sources: None,
            managed_document: None,
        }
    }
}
