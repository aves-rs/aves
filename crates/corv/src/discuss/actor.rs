use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::Agent;
use agent_client_protocol::v1::{ConnectionTo, schema::SessionId};
use quota_core::model::ProviderUsage;
use semver::Version;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use aves::auth::{AuthSudoService, Jwt};
use aves::bus::{BusMcpEndpoint, BusRpcContext, BusSudoService};
use aves::flow::NodeExecutionId;
use aves::runtime::ServiceReader;
use aves::utils::buffers::DEFAULT_BUFFER_SIZE;
use aves::utils::database::{MigrationRunner, maintain_database, open_database_at_path};

use super::allowance::AllowanceService;
use super::assets::{ImportCompletion, PendingImport};
use super::boot::{DiscussBootOptions, DiscussMcpDomain, load_discuss_settings};
use super::config::config_settings_from_value;
use super::database::persist_accounting_prune;
use super::library::AssetLibrary;
use super::messages::{DiscussRequest, DiscussRequestPayload, DiscussResponse};
use super::migration::MIGRATIONS as DISCUSS_MIGRATIONS;
use super::plugins;
use super::pricing::PriceIndex;
use super::provider::effective_workspace_copy_sources;
use super::session::{
    PendingElicitationCancel, PendingElicitationRegistration, PendingPermissionCancel, PendingPermissionRegistration,
    SessionCapabilitiesCache, WorkerReady, revoke_worker_jti,
};
use super::storage::{
    CustomInstallationKind, DiscussAgentConfigRow, DiscussPersistence, list_recent_discuss_sessions,
    load_discuss_agent_config, load_discuss_team_installation, present_acp_session_id, upsert_discuss_agent_config,
};
use super::supervisor::{CallbackRouteRegistry, ProviderSupervisor};
use super::types::{
    AcpProvider, ActorInboundPorts, AgentNodeTerminalResult, DiscussConfigOption, DiscussElicitationAnswer,
    DiscussError, DiscussImageAttachment, DiscussMemberOverrides, DiscussPermissionOption, DiscussSessionId,
    DiscussSessionSetup, DiscussStreamEvent, ElicitationRequestId, InspectorBinding, PendingElicitation,
    PendingPermission, PendingPermissionRegistry, SessionLaunchConfig, WorkerEntry, WorkerRegistry,
    WorkspaceCopySourceRequest, WorkspaceCopySources,
};

const DISCUSS_DOMAIN: &str = "discuss";
const MAX_USER_TURN_BYTES: usize = 256 * 1024;
pub(super) const CANCEL_TURN_DEADLINE: Duration = Duration::from_secs(1);
pub(super) const PROVIDER_CHILD_REAP_DEADLINE: Duration = Duration::from_secs(1);

// ProviderSessionInfo, WorkerCommand, WorkerEntry, WorkerRegistry, PendingPermission,
// PendingPermissionRegistry, DiscussSessionId, PermissionRequestId, DiscussStreamEvent,
// DiscussPermissionOption are now defined in types.rs and imported via `use super::types::*`.

pub(super) struct SpawnSessionOptions {
    pub(super) launch_config: Option<SessionLaunchConfig>,
    pub(super) name: Option<String>,
    pub(super) setup: Option<DiscussSessionSetup>,
    pub(super) session_config_overrides: BTreeMap<String, String>,
    pub(super) acp_session_id: Option<String>,
    pub(super) announce_desktop_association: bool,
    pub(super) transient_connectivity: bool,
    pub(super) session_cwd: Option<PathBuf>,
    pub(super) application_mcp_enabled: Option<bool>,
    pub(super) node_token: Option<Jwt>,
    pub(super) flow_mcp_endpoints: Option<Vec<BusMcpEndpoint>>,
}

pub(super) struct TransientWorkerEvent {
    pub(super) discuss_session_id: DiscussSessionId,
    pub(super) result: Result<(SessionId, Option<Vec<DiscussConfigOption>>), DiscussError>,
}

pub(super) struct SharedProviderEvent {
    pub(super) provider_name: String,
    pub(super) generation: u64,
}

pub(super) struct ProviderRestartReady {
    pub(super) provider_name: String,
    pub(super) generation: u64,
}

/// Result of establishing the one connection owned by a shared Provider.
pub(super) struct SharedConnectionReady {
    pub(super) connection: ConnectionTo<Agent>,
    pub(super) owner: JoinHandle<()>,
    pub(super) exit_rx: oneshot::Receiver<()>,
    pub(super) mcp_acp: bool,
    pub(super) mcp_http: bool,
    pub(super) system_prompt: bool,
    pub(super) title_supported: bool,
    pub(super) callback_routes: Arc<Mutex<CallbackRouteRegistry>>,
}

pub(super) struct PendingProviderTest {
    pub(super) provider_name: String,
    pub(super) candidate: AcpProvider,
    pub(super) previous_provider: Option<AcpProvider>,
    pub(super) previous_config: Option<DiscussAgentConfigRow>,
    pub(super) reply_to: oneshot::Sender<Result<DiscussResponse, DiscussError>>,
}

/// Discuss actor that owns session state.
pub(super) struct Actor {
    pub(super) library: AssetLibrary,
    /// The receiver for the discuss requests.
    pub(super) receiver: mpsc::Receiver<DiscussRequest>,
    pub(super) agent_completions: HashMap<NodeExecutionId, oneshot::Receiver<AgentNodeTerminalResult>>,
    pub(super) workspace_copy_source_rx: mpsc::Receiver<WorkspaceCopySourceRequest>,

    /// The map of agent names to their acp instances.
    pub(super) agents: HashMap<String, AcpProvider>,

    /// The single bounded stream sender to Zeus for `DiscussStreamEvent`.
    /// Cloned into every spawned/recovered session worker.
    pub(super) stream_tx: mpsc::Sender<DiscussStreamEvent>,

    /// Root-authenticated context for actor-owned internal bus requests.
    pub(super) root_context: BusRpcContext,
    pub(super) mcp_domains: fn() -> Result<Vec<DiscussMcpDomain>, DiscussError>,
    pub(super) bus_service: ServiceReader<dyn BusSudoService>,
    pub(super) auth_service: Option<ServiceReader<dyn AuthSudoService>>,

    /// Broadcaster (actor→handle→bus broadcast path).
    pub(super) broadcast_tx: mpsc::Sender<DiscussResponse>,

    /// Private cloneable DB-connection factory.
    pub(super) persistence: DiscussPersistence,
    pub(super) database_path: PathBuf,

    /// Config-owned filesystem working directory (`cwd`) used when opening
    /// Discuss agent sessions. Resolved once at boot from the config domain
    /// and threaded into every session worker.
    pub(super) agents_workspace: PathBuf,

    /// Live worker entries keyed by `discuss_session_id`.
    pub(super) worker_registry: WorkerRegistry,

    /// Actor-owned Provider supervisors keyed by the exact installed Provider ID.
    /// All lifecycle claims occur on this actor's serialized request loop.
    pub(super) provider_supervisors: HashMap<String, ProviderSupervisor>,

    /// Monotonically increasing identity for each shared connection owner.
    /// Events from an owner that was replaced or stopped are ignored.
    pub(super) shared_provider_generations: HashMap<String, u64>,

    /// Unexpected-exit notifications from shared connection watchers.
    pub(super) shared_provider_event_rx: mpsc::Receiver<SharedProviderEvent>,
    pub(super) shared_provider_event_tx: mpsc::Sender<SharedProviderEvent>,
    pub(super) provider_restart_ready_rx: mpsc::Receiver<ProviderRestartReady>,
    pub(super) provider_restart_ready_tx: mpsc::Sender<ProviderRestartReady>,

    /// Cancellable automatic-restart delays, keyed by exact Provider ID.
    pub(super) provider_restart_tasks: HashMap<String, JoinHandle<()>>,
    pub(super) provider_restart_cancellations: HashMap<String, oneshot::Sender<()>>,

    /// Ephemeral inspector bindings, held only for the viewer lifetime.
    pub(super) inspector_bindings: HashMap<DiscussSessionId, InspectorBinding>,

    /// Inspector viewers whose parent attachment is being disposed.
    pub(super) disposing_inspectors: HashSet<DiscussSessionId>,

    /// Server-side capability bits captured from `initialize.agentCapabilities`.
    pub(super) worker_capabilities: HashMap<DiscussSessionId, SessionCapabilitiesCache>,

    /// Receiver for capability updates from workers.
    pub(super) capabilities_update_rx: mpsc::Receiver<(DiscussSessionId, SessionCapabilitiesCache)>,

    /// Sender for capability updates, cloned into each worker.
    pub(super) capabilities_update_tx: mpsc::Sender<(DiscussSessionId, SessionCapabilitiesCache)>,

    pub(super) activity_update_rx: mpsc::Receiver<(DiscussSessionId, bool)>,
    pub(super) activity_update_tx: mpsc::Sender<(DiscussSessionId, bool)>,

    /// Receiver for workers that have completed setup and are ready.
    pub(super) worker_ready_rx: mpsc::Receiver<WorkerReady>,

    /// Sender for ready-worker notifications, cloned into each worker.
    pub(super) worker_ready_tx: mpsc::Sender<WorkerReady>,

    /// Receiver for internal transient provider connectivity results.
    pub(super) transient_worker_rx: mpsc::Receiver<TransientWorkerEvent>,

    /// Sender cloned into transient workers.
    pub(super) transient_worker_tx: mpsc::Sender<TransientWorkerEvent>,
    pub(super) import_completion_tx: mpsc::Sender<ImportCompletion>,
    pub(super) import_completion_rx: mpsc::Receiver<ImportCompletion>,
    pub(super) next_import_key: u64,
    pub(super) pending_imports: HashMap<u64, PendingImport>,

    /// Receiver for pending-permission registrations from workers.
    /// Workers send registrations **before** publishing stream events so the
    /// actor's registry has the entry before an early answer can arrive.
    pub(super) pending_permission_rx: mpsc::Receiver<PendingPermissionRegistration>,

    /// Sender for pending-permission registrations, cloned into each worker.
    pub(super) pending_permission_tx: mpsc::Sender<PendingPermissionRegistration>,

    /// Receiver for pending-permission cancellations from workers (stream
    /// send failure path).
    pub(super) pending_permission_cancel_rx: mpsc::Receiver<PendingPermissionCancel>,

    /// Sender for pending-permission cancellations, cloned into each worker.
    pub(super) pending_permission_cancel_tx: mpsc::Sender<PendingPermissionCancel>,

    /// Receiver for pending-elicitation registrations from workers.
    pub(super) pending_elicitation_rx: mpsc::Receiver<PendingElicitationRegistration>,

    /// Sender for pending-elicitation registrations, cloned into each worker.
    pub(super) pending_elicitation_tx: mpsc::Sender<PendingElicitationRegistration>,

    /// Receiver for pending-elicitation cancellations from workers.
    pub(super) pending_elicitation_cancel_rx: mpsc::Receiver<PendingElicitationCancel>,

    /// Sender for pending-elicitation cancellations, cloned into each worker.
    pub(super) pending_elicitation_cancel_tx: mpsc::Sender<PendingElicitationCancel>,

    /// Pending permission requests keyed by `(discuss_session_id, permission_request_id)`.
    /// Live-only state; not persisted.
    pub(super) pending_permissions: PendingPermissionRegistry,

    /// Pending elicitation requests keyed by `(discuss_session_id, elicitation_request_id)`.
    pub(super) pending_elicitations: HashMap<(DiscussSessionId, ElicitationRequestId), PendingElicitation>,

    /// Deferred AddProvider replies keyed by their transient worker identity.
    pub(super) pending_provider_tests: HashMap<DiscussSessionId, PendingProviderTest>,

    /// Authoritative lifecycle state for ACP sessions.
    pub(super) server_running: bool,
    pub(super) price_index: PriceIndex,
    pub(super) allowance_snapshot: Option<Vec<ProviderUsage>>,
    accounting_cancel: Option<CancellationToken>,
    allowance: Option<AllowanceService>,
    pricing_task: Option<JoinHandle<()>>,
    accounting_retention_days: u32,
}

impl Actor {
    /// Return the most recent observed allowance snapshot while the server is running.
    pub(super) fn current_allowance(&self) -> Result<&[ProviderUsage], DiscussError> {
        if !self.server_running {
            return Err(DiscussError::AccountingUnavailable);
        }
        self.allowance_snapshot
            .as_deref()
            .ok_or(DiscussError::AccountingUnavailable)
    }

    pub async fn new(
        receiver: mpsc::Receiver<DiscussRequest>,
        inbound_ports: ActorInboundPorts,
        root_context: BusRpcContext,
        bus_service: ServiceReader<dyn BusSudoService>,
        auth_service: ServiceReader<dyn AuthSudoService>,
        broadcast_tx: mpsc::Sender<DiscussResponse>,
        boot: DiscussBootOptions,
    ) -> Self {
        let ActorInboundPorts {
            workspace_copy_source_rx,
        } = inbound_ports;
        let DiscussBootOptions {
            library,
            config_dir,
            mcp_domains,
            stream_tx,
        } = boot;

        // ACP requires its workspace config from the config domain. Following
        // the knowledge-domain startup pattern, a failed config-domain request
        // panics loudly at boot rather than silently falling back — the config
        // domain is the single source of truth for the ACP agents workspace.
        let config = load_discuss_settings(&root_context)
            .await
            .expect("ACP actor boot failed to request config from Config domain");
        let accounting_retention_days = config.accounting_retention_days;
        let agents = config.agents;

        // Config-owned session cwd. The config default resolves to
        // `config_dir()/discuss`; a user override is honored verbatim as a path
        // string. ACP never hard-codes a home path or reads the process cwd for
        // session creation.
        let agents_workspace = config
            .agents_workspace
            .unwrap_or_else(|| config_dir.join("discuss"));

        // Discuss owns a separate database. Legacy tables in corv's data.db
        // remain untouched until the explicit data migration.
        let database_path = config_dir.join("discuss.db");
        let database = open_database_at_path(&database_path)
            .await
            .expect("could not open ACP database");

        MigrationRunner::new("__discuss_migrations", DISCUSS_MIGRATIONS)
            .apply(&database)
            .await
            .expect("could not migrate Discuss database");

        maintain_database(&database, &database_path).await;

        let database = Arc::new(database);
        let persistence = DiscussPersistence {
            database: database.clone(),
        };

        for (agent_name, agent_spec) in &agents {
            let conn = persistence
                .acquire_connection()
                .await
                .expect("could not open Discuss agent config database connection");
            let existing = load_discuss_agent_config(&conn, agent_name)
                .await
                .expect("could not load Discuss agent config");
            if existing.is_none() {
                upsert_discuss_agent_config(
                    &conn,
                    agent_name,
                    None,
                    &config_settings_from_value(agent_spec.default_session_config()),
                )
                .await
                .expect("could not seed Discuss agent defaults");
            }
        }

        // Channel for workers to report server-side capabilities back to the actor.
        // Workers send `(session_name, agent_name, SessionCapabilitiesCache)` after
        // capturing `initialize.agentCapabilities`; the actor stores them in the
        // matching worker capability cache entry via `run_actor`.
        let (capabilities_update_tx, capabilities_update_rx) =
            mpsc::channel::<(DiscussSessionId, SessionCapabilitiesCache)>(DEFAULT_BUFFER_SIZE);
        let (activity_update_tx, activity_update_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let (worker_ready_tx, worker_ready_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let (transient_worker_tx, transient_worker_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let (import_completion_tx, import_completion_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let (pending_permission_tx, pending_permission_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let (pending_permission_cancel_tx, pending_permission_cancel_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let (pending_elicitation_tx, pending_elicitation_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let (pending_elicitation_cancel_tx, pending_elicitation_cancel_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let (shared_provider_event_tx, shared_provider_event_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let (provider_restart_ready_tx, provider_restart_ready_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let provider_supervisors = agents
            .keys()
            .map(|provider_name| {
                (
                    provider_name.clone(),
                    ProviderSupervisor::new(
                        provider_name.clone(),
                        plugins::for_agent(provider_name).connection_strategy(),
                    ),
                )
            })
            .collect();

        let actor = Self {
            library,
            receiver,
            agent_completions: HashMap::new(),
            workspace_copy_source_rx,
            stream_tx,
            root_context,
            mcp_domains,
            bus_service,
            auth_service: Some(auth_service),
            broadcast_tx,
            agents,
            persistence,
            database_path,
            agents_workspace,
            worker_registry: HashMap::new(),
            provider_supervisors,
            shared_provider_generations: HashMap::new(),
            shared_provider_event_rx,
            shared_provider_event_tx,
            provider_restart_ready_rx,
            provider_restart_ready_tx,
            provider_restart_tasks: HashMap::new(),
            provider_restart_cancellations: HashMap::new(),
            inspector_bindings: HashMap::new(),
            disposing_inspectors: HashSet::new(),
            worker_capabilities: HashMap::new(),
            capabilities_update_rx,
            capabilities_update_tx,
            activity_update_rx,
            activity_update_tx,
            worker_ready_rx,
            worker_ready_tx,
            transient_worker_rx,
            transient_worker_tx,
            import_completion_tx,
            import_completion_rx,
            next_import_key: 0,
            pending_imports: HashMap::new(),
            pending_permission_rx,
            pending_permission_tx,
            pending_permission_cancel_rx,
            pending_permission_cancel_tx,
            pending_permissions: HashMap::new(),
            pending_elicitation_rx,
            pending_elicitation_tx,
            pending_elicitation_cancel_rx,
            pending_elicitation_cancel_tx,
            pending_elicitations: HashMap::new(),
            pending_provider_tests: HashMap::new(),
            server_running: false,
            price_index: PriceIndex::default(),
            allowance_snapshot: None,
            accounting_cancel: None,
            allowance: None,
            pricing_task: None,
            accounting_retention_days,
        };

        actor.upgrade_installed_teams().await;
        actor.upgrade_custom_installations().await;
        actor
    }

    /// Handle incoming messages.
    pub async fn handle_message(&mut self, request: DiscussRequest) {
        let DiscussRequest { payload, reply_to, .. } = request;

        tracing::debug!(
            target: "application_acp",
            operation = acp_request_kind(&payload),
            "actor handling request"
        );

        if !self.server_running && payload.requires_running_server() {
            let _ = reply_to.send(Err(DiscussError::ShuttingDown));
            return;
        }

        if let DiscussRequestPayload::AddProvider {
            provider_name,
            provider_spec,
        } = &payload
        {
            if let Err(error) = self
                .handle_add_provider_request(provider_name.clone(), provider_spec.clone(), reply_to, false)
                .await
            {
                tracing::debug!(target: "application_acp", ?error, "provider request failed");
            }
            return;
        }

        if let DiscussRequestPayload::RefreshProviderConfig { provider_name } = &payload {
            let Some(provider_spec) = self.agents.get(provider_name).cloned() else {
                let _ = reply_to.send(Err(DiscussError::UnknownAgent {
                    agent_key: provider_name.clone(),
                }));
                return;
            };
            if let Err(error) = self
                .handle_add_provider_request(provider_name.clone(), provider_spec, reply_to, true)
                .await
            {
                tracing::debug!(target: "application_acp", ?error, "provider config refresh failed");
            }
            return;
        }

        if matches!(
            &payload,
            DiscussRequestPayload::ImportCustomAgentFromUrl { .. }
                | DiscussRequestPayload::ImportCustomSkillFromUrl { .. }
        ) {
            self.start_custom_import(&payload, reply_to);
            return;
        }
        let result: Result<DiscussResponse, DiscussError> = match payload {
            DiscussRequestPayload::ResolveSessionIdentity { issuer, subject } => {
                super::identity::resolve(&self.persistence, issuer, subject).await
            }
            DiscussRequestPayload::GetTokenUsageReport {
                from_ms,
                to_ms,
                provider_name,
                model_id,
                group_by,
            } => {
                self.usage_report(from_ms, to_ms, provider_name, model_id, group_by, false)
                    .await
            }
            DiscussRequestPayload::GetSpendReport {
                from_ms,
                to_ms,
                provider_name,
                model_id,
                group_by,
            } => {
                self.usage_report(from_ms, to_ms, provider_name, model_id, group_by, true)
                    .await
            }
            DiscussRequestPayload::GetAllowanceReport {
                from_ms,
                to_ms,
                provider_name,
                group_by,
            } => {
                self.allowance_report(from_ms, to_ms, provider_name, group_by)
                    .await
            }
            DiscussRequestPayload::GetCurrentAllowance => self.current_allowance_response(),
            DiscussRequestPayload::StartAgent {
                correlation,
                launch_config,
                provider_name,
                agent_name,
                session_name,
                config_overrides,
                application_mcp_enabled,
                prompt,
                session_cwd,
                max_turns,
                max_tool_calls,
                max_duration,
                node_token,
                flow_mcp_endpoints,
            } => {
                self.start_agent(
                    correlation,
                    launch_config,
                    provider_name,
                    agent_name,
                    session_name,
                    config_overrides,
                    application_mcp_enabled,
                    prompt,
                    session_cwd,
                    max_turns,
                    max_tool_calls,
                    max_duration,
                    node_token,
                    flow_mcp_endpoints,
                )
                .await
            }
            DiscussRequestPayload::TickAgent { correlation } => self.tick_agent(correlation),
            DiscussRequestPayload::CancelAgentExecution { node_execution_id } => {
                self.cancel_agent(node_execution_id);
                Ok(DiscussResponse::Success)
            }
            DiscussRequestPayload::StartServer => self.handle_start_server().await,
            DiscussRequestPayload::GetServerStatus => Ok(DiscussResponse::ServerStatus {
                running: self.server_running,
            }),
            DiscussRequestPayload::StartProvider { provider_name } => self.handle_start_provider(&provider_name).await,
            DiscussRequestPayload::StopProvider { provider_name } => self.handle_stop_provider(&provider_name).await,
            DiscussRequestPayload::AddProvider { .. } => unreachable!("AddProvider is handled above"),
            DiscussRequestPayload::RefreshProviderConfig { .. } => {
                unreachable!("RefreshProviderConfig is handled above")
            }
            DiscussRequestPayload::ListProviders => self.handle_list_providers().await,
            DiscussRequestPayload::GetProviderConfig { provider_name } => {
                self.handle_get_provider_config(&provider_name).await
            }
            DiscussRequestPayload::GetProviderManagedConfig { provider_name } => {
                self.handle_get_provider_managed_config(&provider_name)
            }
            DiscussRequestPayload::SetProviderManagedConfig {
                provider_name,
                document,
            } => {
                self.handle_set_provider_managed_config(&provider_name, document)
                    .await
            }
            DiscussRequestPayload::RemoveProvider { provider_name } => {
                self.handle_remove_provider(&provider_name).await
            }
            DiscussRequestPayload::InstallProviderConfig { provider_name } => {
                self.handle_install_provider_config(&provider_name)
            }
            DiscussRequestPayload::RemoveProviderConfig { provider_name } => {
                self.handle_remove_provider_config(&provider_name)
            }
            DiscussRequestPayload::ListCustomMcps => self.handle_list_custom_mcps().await,
            DiscussRequestPayload::CreateCustomMcp { name, descriptor } => {
                self.handle_write_custom_mcp(&name, descriptor, true).await
            }
            DiscussRequestPayload::UpdateCustomMcp { name, descriptor } => {
                self.handle_write_custom_mcp(&name, descriptor, false).await
            }
            DiscussRequestPayload::DeleteCustomMcp { name } => self.handle_delete_custom_mcp(&name).await,
            DiscussRequestPayload::AssignMcp {
                scope_kind,
                scope_id,
                mcp_name,
            } => {
                self.handle_mcp_assignment(scope_kind, &scope_id, &mcp_name, true)
                    .await
            }
            DiscussRequestPayload::UnassignMcp {
                scope_kind,
                scope_id,
                mcp_name,
            } => {
                self.handle_mcp_assignment(scope_kind, &scope_id, &mcp_name, false)
                    .await
            }
            DiscussRequestPayload::ListBuiltinSkills => self.handle_list_builtin_skills(),
            DiscussRequestPayload::ListProviderInstalledAssets => self.handle_list_provider_installed_assets().await,
            DiscussRequestPayload::ListProviderMcpAssignments { provider_name } => {
                self.handle_list_provider_mcp_assignments(&provider_name)
                    .await
            }
            DiscussRequestPayload::ListCustomAgents => self.handle_list_custom_agents().await,
            DiscussRequestPayload::ListCustomSkills => self.handle_list_custom_skills().await,
            DiscussRequestPayload::GetCustomAgent { agent_id } => self.handle_get_custom_agent(&agent_id).await,
            DiscussRequestPayload::GetCustomSkill { skill_id } => self.handle_get_custom_skill(&skill_id).await,
            DiscussRequestPayload::ImportCustomAgentFromUrl { .. }
            | DiscussRequestPayload::ImportCustomSkillFromUrl { .. } => unreachable!("imports are handled above"),
            DiscussRequestPayload::SetInstalledAssetInstructions {
                provider_name,
                asset_kind,
                asset_id,
                instructions,
            } => {
                self.installed_asset_instructions(&provider_name, asset_kind, &asset_id, Some(instructions))
                    .await
            }
            DiscussRequestPayload::GetInstalledAssetInstructions {
                provider_name,
                asset_kind,
                asset_id,
            } => {
                self.installed_asset_instructions(&provider_name, asset_kind, &asset_id, None)
                    .await
            }
            DiscussRequestPayload::CreateCustomAgent {
                agent_id,
                name,
                job_title,
                description,
                body,
            } => {
                self.handle_create_custom_agent(agent_id, name, job_title, description, body)
                    .await
            }
            DiscussRequestPayload::CreateCustomSkill {
                skill_id,
                name,
                description,
                files,
            } => {
                self.handle_create_custom_skill(skill_id, name, description, files)
                    .await
            }
            DiscussRequestPayload::UpdateCustomAgent {
                agent_id,
                name,
                job_title,
                description,
                body,
            } => {
                self.handle_update_custom_agent(agent_id, name, job_title, description, body)
                    .await
            }
            DiscussRequestPayload::UpdateCustomSkill {
                skill_id,
                name,
                description,
                files,
            } => {
                self.handle_update_custom_skill(skill_id, name, description, files)
                    .await
            }
            DiscussRequestPayload::DeleteCustomAgent { agent_id } => self.handle_delete_custom_agent(&agent_id).await,
            DiscussRequestPayload::DeleteCustomSkill { skill_id } => self.handle_delete_custom_skill(&skill_id).await,
            DiscussRequestPayload::InstallCustomAgent {
                provider_name,
                agent_id,
            } => {
                self.install_custom_asset(CustomInstallationKind::Agent, &provider_name, &agent_id)
                    .await
            }
            DiscussRequestPayload::UninstallCustomAgent {
                provider_name,
                agent_id,
            } => {
                self.uninstall_custom_asset(CustomInstallationKind::Agent, &provider_name, &agent_id)
                    .await
            }
            DiscussRequestPayload::InstallCustomSkill {
                provider_name,
                skill_id,
            } => {
                self.install_custom_asset(CustomInstallationKind::Skill, &provider_name, &skill_id)
                    .await
            }
            DiscussRequestPayload::UninstallCustomSkill {
                provider_name,
                skill_id,
            } => {
                self.uninstall_custom_asset(CustomInstallationKind::Skill, &provider_name, &skill_id)
                    .await
            }
            DiscussRequestPayload::CreateCustomTeam {
                leader_id,
                display_name,
                followers,
                skills,
            } => {
                self.handle_create_custom_team(leader_id, display_name, followers, skills)
                    .await
            }
            DiscussRequestPayload::UpdateCustomTeam {
                leader_id,
                display_name,
                followers,
                skills,
            } => {
                self.handle_update_custom_team(leader_id, display_name, followers, skills)
                    .await
            }
            DiscussRequestPayload::DeleteCustomTeam { leader_id } => self.handle_delete_custom_team(leader_id).await,
            DiscussRequestPayload::ListTeamCatalog => self.handle_list_team_catalog().await,
            DiscussRequestPayload::InstallTeam {
                leader_id,
                provider_name,
                member_overrides,
                theme_color,
            } => {
                self.handle_install_team(leader_id, provider_name, member_overrides, theme_color)
                    .await
            }
            DiscussRequestPayload::EditTeam {
                leader_id,
                provider_name,
                member_overrides,
                theme_color,
            } => {
                self.handle_edit_team(leader_id, provider_name, member_overrides, theme_color)
                    .await
            }
            DiscussRequestPayload::UninstallTeam { leader_id } => self.handle_uninstall_team(leader_id).await,
            DiscussRequestPayload::InstallTeamAndStartSession {
                leader_id,
                provider_name,
                discuss_session_id,
                setup,
                session_name,
                launch_config,
                session_cwd,
                application_mcp_enabled,
            } => {
                async {
                    if !matches!(&setup, DiscussSessionSetup::Team { leader_id: selected } if selected == &leader_id) {
                        return Err(DiscussError::Validation {
                            message: "Session setup must select the installed Team".to_string(),
                        });
                    }
                    let conn = self.persistence.acquire_connection().await?;
                    let installation = load_discuss_team_installation(&conn, &leader_id).await?;
                    drop(conn);
                    let (member_overrides, theme_color): (DiscussMemberOverrides, _) = match installation {
                        Some(row) => (
                            serde_json::from_str(&row.member_overrides_json)?,
                            row.theme_color_json
                                .as_deref()
                                .map(serde_json::from_str)
                                .transpose()?,
                        ),
                        None => (BTreeMap::new(), None),
                    };
                    let (setup, session_config_overrides) = if session_cwd.is_some() {
                        let settings = if self
                            .resolve_persisted_acp_session_id(discuss_session_id, None)
                            .await?
                            .is_none()
                        {
                            member_overrides
                                .get(&leader_id)
                                .cloned()
                                .unwrap_or_default()
                        } else {
                            BTreeMap::new()
                        };
                        (DiscussSessionSetup::TeamAtRoot { leader_id }, settings)
                    } else {
                        self.handle_install_team(leader_id, provider_name.clone(), member_overrides, theme_color)
                            .await?;
                        (setup, BTreeMap::new())
                    };
                    self.spawn_session(
                        discuss_session_id,
                        &provider_name,
                        SpawnSessionOptions {
                            launch_config,
                            name: session_name,
                            setup: Some(setup),
                            session_config_overrides,
                            acp_session_id: None,
                            announce_desktop_association: true,
                            transient_connectivity: false,
                            session_cwd,
                            application_mcp_enabled,
                            node_token: None,
                            flow_mcp_endpoints: None,
                        },
                    )
                    .await
                }
                .await
            }
            DiscussRequestPayload::NewSession {
                launch_config,
                discuss_session_id,
                agent_key,
                session_name,
                setup,
                session_cwd,
                application_mcp_enabled,
            } => {
                self.spawn_session(
                    discuss_session_id,
                    &agent_key,
                    SpawnSessionOptions {
                        launch_config,
                        name: session_name,
                        setup,
                        session_config_overrides: BTreeMap::new(),
                        acp_session_id: None,
                        announce_desktop_association: true,
                        transient_connectivity: false,
                        session_cwd,
                        application_mcp_enabled,
                        node_token: None,
                        flow_mcp_endpoints: None,
                    },
                )
                .await
            }
            DiscussRequestPayload::ListSessions => self.handle_list_sessions().await,
            DiscussRequestPayload::LoadSession { discuss_session_id } => {
                self.handle_load_session(discuss_session_id).await
            }
            DiscussRequestPayload::GetConfigOptions { discuss_session_id } => {
                self.handle_get_config_options(discuss_session_id).await
            }
            DiscussRequestPayload::SetConfigOption {
                discuss_session_id,
                config_id,
                value,
            } => {
                self.handle_set_config_option(discuss_session_id, config_id, value)
                    .await
            }
            DiscussRequestPayload::RemoveSession { discuss_session_id } => {
                self.handle_remove_session(discuss_session_id).await
            }
            DiscussRequestPayload::RenameSession {
                discuss_session_id,
                display_title,
            } => {
                self.handle_rename_session(discuss_session_id, &display_title)
                    .await
            }
            DiscussRequestPayload::SendUserTurn {
                discuss_session_id,
                text,
                images,
            } => {
                self.handle_send_user_turn(discuss_session_id, text, images)
                    .await
            }
            DiscussRequestPayload::AnswerPermission {
                discuss_session_id,
                permission_request_id,
                option,
            } => {
                self.handle_answer_permission(discuss_session_id, permission_request_id, option)
                    .await
            }
            DiscussRequestPayload::AnswerElicitation {
                discuss_session_id,
                elicitation_request_id,
                answer,
            } => {
                self.handle_answer_elicitation(discuss_session_id, elicitation_request_id, answer)
                    .await
            }
            DiscussRequestPayload::CancelElicitation {
                discuss_session_id,
                elicitation_request_id,
            } => {
                self.handle_cancel_elicitation(discuss_session_id, elicitation_request_id)
                    .await
            }
            DiscussRequestPayload::CancelTurn { discuss_session_id } => {
                self.handle_cancel_turn(discuss_session_id).await
            }
            DiscussRequestPayload::CancelSession { discuss_session_id } => {
                self.handle_cancel_session(discuss_session_id).await
            }
            DiscussRequestPayload::ReserveInspector {
                parent_session_id,
                child_session_id,
            } => {
                self.handle_reserve_inspector(parent_session_id, child_session_id)
                    .await
            }
            DiscussRequestPayload::StartInspector { viewer_session_id } => {
                self.handle_start_inspector(viewer_session_id).await
            }
            DiscussRequestPayload::DisposeInspector { viewer_session_id } => {
                self.handle_dispose_inspector(viewer_session_id).await
            }
            DiscussRequestPayload::StopServer => self.handle_stop_server().await,
        };

        if let Err(ref error) = result {
            tracing::debug!(
                target: "application_acp",
                ?error,
                "actor request failed"
            );
        }

        let _ = reply_to.send(result);
    }

    /// Reconnect every recoverable persisted session without announcing a
    /// desktop association. Stale anchors are skipped independently so one bad
    /// provider does not prevent the remaining sessions from warming.
    async fn handle_start_server(&mut self) -> Result<DiscussResponse, DiscussError> {
        let was_running = self.server_running;
        let rows = if was_running {
            Vec::new()
        } else {
            let conn = self.persistence.acquire_connection().await?;
            list_recent_discuss_sessions(&conn).await?
        };

        if !was_running {
            let cancel = CancellationToken::new();
            self.pricing_task = Some(super::pricing::spawn(
                self.persistence.clone(),
                cancel.clone(),
                self.price_index.clone(),
            ));
            self.allowance = Some(super::allowance::spawn(self.persistence.clone(), cancel.clone()));
            self.accounting_cancel = Some(cancel);
            self.allowance_snapshot = None;
            self.server_running = true;
        }

        let provider_names = self.agents.keys().cloned().collect::<Vec<_>>();
        for provider_name in provider_names {
            self.provider_supervisors
                .entry(provider_name.clone())
                .or_insert_with(|| {
                    ProviderSupervisor::new(
                        provider_name.clone(),
                        plugins::for_agent(&provider_name).connection_strategy(),
                    )
                });
            match self.start_provider_supervisor(&provider_name, true).await {
                Ok(true) => {
                    self.publish_provider_runtime(&provider_name).await;
                    if was_running {
                        self.recover_provider_sessions(&provider_name).await;
                    }
                }
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(
                        target: "application_acp",
                        ?error,
                        provider_name,
                        "Provider failed to start during Discuss startup"
                    );
                    self.publish_provider_runtime(&provider_name).await;
                }
            }
        }

        for row in rows {
            if row.is_task_session {
                continue;
            }
            let Some(acp_session_id) = present_acp_session_id(row.acp_session_id.as_deref()) else {
                continue;
            };
            if row.discuss_session_id.trim().is_empty() {
                tracing::warn!(target: "application_acp", "skipping Discuss anchor without a usable discuss_session_id");
                continue;
            }
            if !self.agents.contains_key(&row.agent_name) {
                tracing::warn!(
                    target: "application_acp",
                    discuss_session_id = %row.discuss_session_id,
                    agent_name = %row.agent_name,
                    "skipping Discuss anchor whose agent is no longer installed"
                );
                continue;
            }

            let Ok(discuss_session_id) = DiscussSessionId::from_str(&row.discuss_session_id) else {
                tracing::warn!(
                    target: "application_acp",
                    discuss_session_id = %row.discuss_session_id,
                    "skipping Discuss anchor with unparseable discuss_session_id"
                );
                continue;
            };

            if let Err(error) = self
                .spawn_session(
                    discuss_session_id,
                    &row.agent_name,
                    SpawnSessionOptions {
                        launch_config: None,
                        name: Some(row.session_name),
                        setup: None,
                        session_config_overrides: BTreeMap::new(),
                        acp_session_id: Some(acp_session_id),
                        announce_desktop_association: false,
                        transient_connectivity: false,
                        session_cwd: None,
                        application_mcp_enabled: None,
                        node_token: None,
                        flow_mcp_endpoints: None,
                    },
                )
                .await
            {
                tracing::warn!(target: "application_acp", ?error, "skipping unrecoverable Discuss session during startup");
            }
        }

        if !was_running {
            self.publish_server_status(DiscussResponse::ServerStarted)
                .await;
        }
        Ok(DiscussResponse::Success)
    }

    async fn publish_server_status(&self, response: DiscussResponse) {
        if let Err(error) = self.broadcast_tx.send(response).await {
            tracing::warn!(target: "application_acp", ?error, "could not publish Discuss server status");
        }
    }

    fn resolve_workspace_copy_sources(
        &self,
        provider_names: BTreeSet<String>,
    ) -> Result<WorkspaceCopySources, DiscussError> {
        let sources_by_provider = provider_names
            .into_iter()
            .map(|provider_name| {
                let provider = self
                    .agents
                    .get(&provider_name)
                    .ok_or_else(|| DiscussError::UnknownAgent {
                        agent_key: provider_name.clone(),
                    })?;
                provider.validate_workspace_copy_sources()?;
                Ok((
                    provider_name.clone(),
                    effective_workspace_copy_sources(&provider_name, provider),
                ))
            })
            .collect::<Result<BTreeMap<_, _>, DiscussError>>()?;
        Ok(WorkspaceCopySources {
            agents_workspace: self.agents_workspace.clone(),
            sources_by_provider,
        })
    }

    fn register_pending_permission(&mut self, registration: PendingPermissionRegistration) {
        let PendingPermissionRegistration {
            discuss_session_id,
            permission_request_id,
            pending,
            registration_ack,
        } = registration;
        let key = (discuss_session_id, permission_request_id);

        if !self.server_running {
            let _ = pending.responder_tx.send(DiscussPermissionOption::Cancel);
            let _ = registration_ack.send(Err(DiscussError::ShuttingDown));
            return;
        }

        if self.disposing_inspectors.contains(&discuss_session_id) {
            let _ = pending.responder_tx.send(DiscussPermissionOption::Cancel);
            let _ = registration_ack.send(Err(DiscussError::SessionNotFound { discuss_session_id }));
            return;
        }

        if let Some(previous) = self.pending_permissions.insert(key, pending) {
            let _ = previous.responder_tx.send(DiscussPermissionOption::Cancel);
        }

        if registration_ack.send(Ok(())).is_err()
            && let Some(pending) = self.pending_permissions.remove(&key)
        {
            let _ = pending.responder_tx.send(DiscussPermissionOption::Cancel);
        }
    }

    fn cancel_pending_permission(&mut self, cancellation: PendingPermissionCancel) {
        let key = (cancellation.discuss_session_id, cancellation.permission_request_id);
        if let Some(pending) = self.pending_permissions.remove(&key) {
            let _ = pending.responder_tx.send(DiscussPermissionOption::Cancel);
        }
        let _ = cancellation.cancellation_ack.send(());
    }

    fn register_pending_elicitation(&mut self, registration: PendingElicitationRegistration) {
        let PendingElicitationRegistration {
            discuss_session_id,
            elicitation_request_id,
            pending,
            registration_ack,
        } = registration;
        let key = (discuss_session_id, elicitation_request_id);

        if !self.server_running {
            self.persist_cancelled_elicitation(discuss_session_id, elicitation_request_id, &pending);
            let _ = pending.responder_tx.send(DiscussElicitationAnswer::Cancel);
            let _ = registration_ack.send(Err(DiscussError::ShuttingDown));
            return;
        }

        if self.disposing_inspectors.contains(&discuss_session_id) {
            self.persist_cancelled_elicitation(discuss_session_id, elicitation_request_id, &pending);
            let _ = pending.responder_tx.send(DiscussElicitationAnswer::Cancel);
            let _ = registration_ack.send(Err(DiscussError::SessionNotFound { discuss_session_id }));
            return;
        }

        if let Some(previous) = self.pending_elicitations.insert(key, pending) {
            self.persist_cancelled_elicitation(discuss_session_id, elicitation_request_id, &previous);
            let _ = previous.responder_tx.send(DiscussElicitationAnswer::Cancel);
        }

        if registration_ack.send(Ok(())).is_err()
            && let Some(pending) = self.pending_elicitations.remove(&key)
        {
            self.persist_cancelled_elicitation(discuss_session_id, elicitation_request_id, &pending);
            let _ = pending.responder_tx.send(DiscussElicitationAnswer::Cancel);
        }
    }

    fn cancel_pending_elicitation(&mut self, cancellation: PendingElicitationCancel) {
        if let Some(elicitation_request_id) = cancellation.elicitation_request_id {
            let key = (cancellation.discuss_session_id, elicitation_request_id);
            if let Some(pending) = self.pending_elicitations.remove(&key) {
                self.persist_cancelled_elicitation(cancellation.discuss_session_id, elicitation_request_id, &pending);
                let _ = pending.responder_tx.send(DiscussElicitationAnswer::Cancel);
            }
        } else {
            self.cancel_pending_elicitations_for_session(&cancellation.discuss_session_id);
        }
        let _ = cancellation.cancellation_ack.send(());
    }

    /// Stop the Discuss server: ordered shutdown.
    async fn handle_stop_server(&mut self) -> Result<DiscussResponse, DiscussError> {
        self.cancel_custom_imports();
        let was_running = self.server_running;
        self.server_running = false;
        if let Some(cancel) = self.accounting_cancel.take() {
            cancel.cancel();
        }
        if let Some(service) = self.allowance.take() {
            service.stop().await;
        }
        if let Some(task) = self.pricing_task.take()
            && let Err(error) = task.await
        {
            tracing::warn!(?error, "Pricing task failed");
        }
        self.allowance_snapshot = None;
        self.agent_completions.clear();

        let provider_names = self
            .provider_supervisors
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for provider_name in provider_names {
            self.cancel_provider_restart(&provider_name);
            let should_stop = self
                .provider_supervisors
                .get_mut(&provider_name)
                .is_some_and(ProviderSupervisor::claim_stop);
            if should_stop {
                self.stop_provider_supervisor_resources(&provider_name)
                    .await;
                if let Some(supervisor) = self.provider_supervisors.get_mut(&provider_name) {
                    let _ = supervisor.complete_stop();
                }
                self.publish_provider_runtime(&provider_name).await;
            }
        }

        // (b) Cancel all pending permissions.
        let pending: Vec<(_, PendingPermission)> = self.pending_permissions.drain().collect();
        for (_, perm) in pending {
            let _ = perm.responder_tx.send(DiscussPermissionOption::Cancel);
        }
        self.cancel_pending_elicitations_for_session_all();

        // (c) Abort every live worker immediately. Dropping each worker future
        // closes its ACP connection while leaving persisted sessions intact.
        let entries: Vec<(DiscussSessionId, WorkerEntry)> = self.worker_registry.drain().collect();
        for (_, entry) in entries {
            entry.join_handle.abort();
            revoke_worker_jti(self.auth_service.as_ref(), entry.worker_jti, "transient").await;
        }
        self.worker_capabilities.clear();
        let pending_tests: Vec<_> = self
            .pending_provider_tests
            .drain()
            .map(|(_, pending)| pending)
            .collect();
        for pending in pending_tests {
            let reply = self.restore_provider_test_snapshot(pending, false).await;
            let _ = reply.send(Err(DiscussError::ShuttingDown));
        }

        if was_running {
            self.publish_server_status(DiscussResponse::ServerStopped)
                .await;
        }
        maintain_database(&self.persistence.database, &self.database_path).await;
        Ok(DiscussResponse::Success)
    }
}

/// Static kind label for an [`DiscussRequestPayload`], for structured tracing only.
///
/// Returns a stable `&'static str` discriminant so debug logs can trace which
/// ACP operation the actor is handling without ever serializing prompt text,
/// config values, agent specs, or provider ids.
fn acp_request_kind(payload: &DiscussRequestPayload) -> &'static str {
    match payload {
        DiscussRequestPayload::ResolveSessionIdentity { .. } => "resolveSessionIdentity",
        DiscussRequestPayload::GetTokenUsageReport { .. } => "getTokenUsageReport",
        DiscussRequestPayload::GetSpendReport { .. } => "getSpendReport",
        DiscussRequestPayload::GetAllowanceReport { .. } => "getAllowanceReport",
        DiscussRequestPayload::GetCurrentAllowance => "getCurrentAllowance",
        DiscussRequestPayload::StartAgent { .. } => "startAgent",
        DiscussRequestPayload::TickAgent { .. } => "tickAgent",
        DiscussRequestPayload::CancelAgentExecution { .. } => "cancelAgentExecution",
        DiscussRequestPayload::StartServer => "startServer",
        DiscussRequestPayload::StopServer => "stopServer",
        DiscussRequestPayload::GetServerStatus => "getServerStatus",
        DiscussRequestPayload::StartProvider { .. } => "startProvider",
        DiscussRequestPayload::StopProvider { .. } => "stopProvider",
        DiscussRequestPayload::SendUserTurn { .. } => "sendUserTurn",
        DiscussRequestPayload::AnswerPermission { .. } => "answerPermission",
        DiscussRequestPayload::AnswerElicitation { .. } => "answerElicitation",
        DiscussRequestPayload::CancelElicitation { .. } => "cancelElicitation",
        DiscussRequestPayload::CancelTurn { .. } => "cancelTurn",
        DiscussRequestPayload::CancelSession { .. } => "cancelSession",
        DiscussRequestPayload::ReserveInspector { .. } => "reserveInspector",
        DiscussRequestPayload::StartInspector { .. } => "startInspector",
        DiscussRequestPayload::DisposeInspector { .. } => "disposeInspector",
        DiscussRequestPayload::AddProvider { .. } => "addProvider",
        DiscussRequestPayload::ListProviders => "listProviders",
        DiscussRequestPayload::GetProviderConfig { .. } => "getProviderConfig",
        DiscussRequestPayload::GetProviderManagedConfig { .. } => "getProviderManagedConfig",
        DiscussRequestPayload::SetProviderManagedConfig { .. } => "setProviderManagedConfig",
        DiscussRequestPayload::RefreshProviderConfig { .. } => "refreshProviderConfig",
        DiscussRequestPayload::RemoveProvider { .. } => "removeProvider",
        DiscussRequestPayload::InstallProviderConfig { .. } => "installProviderConfig",
        DiscussRequestPayload::RemoveProviderConfig { .. } => "removeProviderConfig",
        DiscussRequestPayload::ListCustomMcps => "listCustomMcps",
        DiscussRequestPayload::CreateCustomMcp { .. } => "createCustomMcp",
        DiscussRequestPayload::UpdateCustomMcp { .. } => "updateCustomMcp",
        DiscussRequestPayload::DeleteCustomMcp { .. } => "deleteCustomMcp",
        DiscussRequestPayload::AssignMcp { .. } => "assignMcp",
        DiscussRequestPayload::UnassignMcp { .. } => "unassignMcp",
        DiscussRequestPayload::ListBuiltinSkills => "listBuiltinSkills",
        DiscussRequestPayload::ListProviderInstalledAssets => "listProviderInstalledAssets",
        DiscussRequestPayload::ListProviderMcpAssignments { .. } => "listProviderMcpAssignments",
        DiscussRequestPayload::ListCustomAgents => "listCustomAgents",
        DiscussRequestPayload::ListCustomSkills => "listCustomSkills",
        DiscussRequestPayload::GetCustomAgent { .. } => "getCustomAgent",
        DiscussRequestPayload::GetCustomSkill { .. } => "getCustomSkill",
        DiscussRequestPayload::ImportCustomAgentFromUrl { .. } => "importCustomAgentFromUrl",
        DiscussRequestPayload::ImportCustomSkillFromUrl { .. } => "importCustomSkillFromUrl",
        DiscussRequestPayload::SetInstalledAssetInstructions { .. } => "setInstalledAssetInstructions",
        DiscussRequestPayload::GetInstalledAssetInstructions { .. } => "getInstalledAssetInstructions",
        DiscussRequestPayload::CreateCustomAgent { .. } => "createCustomAgent",
        DiscussRequestPayload::CreateCustomSkill { .. } => "createCustomSkill",
        DiscussRequestPayload::UpdateCustomAgent { .. } => "updateCustomAgent",
        DiscussRequestPayload::UpdateCustomSkill { .. } => "updateCustomSkill",
        DiscussRequestPayload::DeleteCustomAgent { .. } => "deleteCustomAgent",
        DiscussRequestPayload::DeleteCustomSkill { .. } => "deleteCustomSkill",
        DiscussRequestPayload::InstallCustomAgent { .. } => "installCustomAgent",
        DiscussRequestPayload::UninstallCustomAgent { .. } => "uninstallCustomAgent",
        DiscussRequestPayload::InstallCustomSkill { .. } => "installCustomSkill",
        DiscussRequestPayload::UninstallCustomSkill { .. } => "uninstallCustomSkill",
        DiscussRequestPayload::CreateCustomTeam { .. } => "createCustomTeam",
        DiscussRequestPayload::UpdateCustomTeam { .. } => "updateCustomTeam",
        DiscussRequestPayload::DeleteCustomTeam { .. } => "deleteCustomTeam",
        DiscussRequestPayload::ListTeamCatalog => "listTeamCatalog",
        DiscussRequestPayload::InstallTeam { .. } => "installTeam",
        DiscussRequestPayload::InstallTeamAndStartSession { .. } => "installTeamAndStartSession",
        DiscussRequestPayload::EditTeam { .. } => "editTeam",
        DiscussRequestPayload::UninstallTeam { .. } => "uninstallTeam",
        DiscussRequestPayload::NewSession { .. } => "newSession",
        DiscussRequestPayload::ListSessions => "listSessions",
        DiscussRequestPayload::LoadSession { .. } => "loadSession",
        DiscussRequestPayload::GetConfigOptions { .. } => "getConfigOptions",
        DiscussRequestPayload::SetConfigOption { .. } => "setConfigOption",
        DiscussRequestPayload::RemoveSession { .. } => "removeSession",
        DiscussRequestPayload::RenameSession { .. } => "renameSession",
    }
}

pub(super) fn validate_user_turn(text: &str, images: &[DiscussImageAttachment]) -> Result<(), DiscussError> {
    let actual_bytes = text.len();
    if actual_bytes > MAX_USER_TURN_BYTES {
        return Err(DiscussError::UserTurnTooLarge {
            actual_bytes: actual_bytes as u64,
            max_bytes: MAX_USER_TURN_BYTES as u64,
        });
    }
    for image in images {
        let valid_mime = image
            .mime_type
            .split_once('/')
            .is_some_and(|(kind, subtype)| {
                !kind.is_empty()
                    && !subtype.is_empty()
                    && !subtype.contains('/')
                    && !kind.chars().any(char::is_whitespace)
                    && !subtype.chars().any(char::is_whitespace)
            });
        if image.data.is_empty() || !valid_mime {
            return Err(DiscussError::Validation {
                message: "image must have non-empty data and a type/subtype MIME type".to_string(),
            });
        }
    }
    Ok(())
}

pub(super) fn team_versions_match(
    installed_version: &str,
    current_version: &Version,
) -> Result<bool, AgentVersionParseError> {
    let installed = Version::parse(installed_version).map_err(AgentVersionParseError::Installed)?;

    Ok(installed.cmp_precedence(current_version) == Ordering::Equal)
}

pub(super) fn channel_send_error(operation: &'static str, cause: impl Into<String>) -> DiscussError {
    DiscussError::Infrastructure {
        kind: "channel_send".to_string(),
        target_domain: DISCUSS_DOMAIN.to_string(),
        operation: operation.to_string(),
        message: cause.into(),
    }
}

pub(super) async fn run_actor(mut actor: Actor) {
    let mut retention = tokio::time::interval(Duration::from_secs(86_400));
    loop {
        tokio::select! {
            _ = retention.tick() => {
                if actor.accounting_retention_days != 0 {
                    let cutoff = chrono::Utc::now().timestamp_millis().saturating_sub(i64::from(actor.accounting_retention_days) * 86_400_000);
                    if let Err(error) = persist_accounting_prune(&actor.persistence, cutoff).await {
                        tracing::warn!(?error, "Accounting retention prune failed");
                    }
                }
            }
            Some(snapshot) = async {
                match actor.allowance.as_mut() {
                    Some(service) => service.observations.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(service) = &actor.allowance { let _ = service.writer.try_send(snapshot.clone()); }
                actor.allowance_snapshot = Some(snapshot);
            }
            Some(request) = actor.workspace_copy_source_rx.recv() => {
                let result = actor.resolve_workspace_copy_sources(request.provider_names);
                let _ = request.reply_to.send(result);
            }
            // Incoming request from the bus.
            Some(msg) = actor.receiver.recv() => {
                let _ = actor.handle_message(msg).await;
            }
            Some(completion) = actor.import_completion_rx.recv() => {
                actor.finish_custom_import(completion).await;
            }
            // Capability update from a worker.
            Some((discuss_session_id, mut caps)) = actor.capabilities_update_rx.recv() => {
                if actor.worker_registry.contains_key(&discuss_session_id) {
                    caps.set_config_option |= actor
                        .worker_capabilities
                        .get(&discuss_session_id)
                        .is_some_and(|current| current.set_config_option);
                    actor.worker_capabilities.insert(discuss_session_id, caps);
                }
            }
            Some((discuss_session_id, active)) = actor.activity_update_rx.recv() => {
                if let Some(entry) = actor.worker_registry.get_mut(&discuss_session_id).filter(|entry| !entry.join_handle.is_finished()) {
                    entry.active_turn = active;
                    if let Err(error) = actor.broadcast_tx.send(DiscussResponse::SessionActivity { discuss_session_id, active }).await {
                        tracing::warn!(%discuss_session_id, %error, "Failed to broadcast session activity");
                    }
                } else {
                    tracing::debug!(%discuss_session_id, "Ignoring activity from absent or finished worker");
                }
            }
            Some(event) = actor.shared_provider_event_rx.recv() => {
                actor.handle_shared_provider_exit(event).await;
            }
            Some(event) = actor.provider_restart_ready_rx.recv() => {
                actor.handle_shared_provider_restart_ready(event).await;
            }
            Some(WorkerReady { discuss_session_id, acp_session_id, announce_desktop_association, transient_connectivity, config_options, supports_config_updates }) = actor.worker_ready_rx.recv() => {
                if !actor.server_running {
                    continue;
                }
                if transient_connectivity {
                    actor
                        .finish_provider_test(discuss_session_id, Ok(config_options))
                        .await;
                    continue;
                }
                // Update the worker entry with the provider ACP session id.
                let mut should_announce_desktop_association = announce_desktop_association;
                if let Some(entry) = actor.worker_registry.get_mut(&discuss_session_id) {
                    entry.acp_session_id = Some(acp_session_id);
                    should_announce_desktop_association |= entry.desktop_association_requested;
                    entry.desktop_association_requested = false;
                }
                actor
                    .worker_capabilities
                    .entry(discuss_session_id)
                    .or_default()
                    .set_config_option |= supports_config_updates;
                if should_announce_desktop_association
                    && let Err(error) = actor.announce_desktop_association(discuss_session_id).await
                {
                    tracing::warn!(target: "application_acp", ?error, "could not announce ready Discuss desktop association");
                }
            }
            Some(TransientWorkerEvent { discuss_session_id, result }) = actor.transient_worker_rx.recv() => {
                actor
                    .finish_provider_test(discuss_session_id, result.map(|(_, options)| options))
                    .await;
            }
            // Pending permission registration from a worker. The worker sends
            // this before publishing the stream event so the actor's registry
            // has the entry before an early answer can arrive.
            Some(registration) = actor.pending_permission_rx.recv() => {
                actor.register_pending_permission(registration);
            }
            // Pending permission cancellation from a worker (stream send
            // failure path). Remove the entry and resolve the provider
            // responder as cancelled.
            Some(cancel) = actor.pending_permission_cancel_rx.recv() => {
                actor.cancel_pending_permission(cancel);
            }
            // Pending elicitation registration from a worker. The worker sends
            // this before the later stream event is published.
            Some(registration) = actor.pending_elicitation_rx.recv() => {
                actor.register_pending_elicitation(registration);
            }
            // Pending elicitation cancellation from a worker when stream
            // delivery fails.
            Some(cancel) = actor.pending_elicitation_cancel_rx.recv() => {
                actor.cancel_pending_elicitation(cancel);
            }
            else => break,
        }
    }
}

#[derive(Debug)]
pub(super) enum AgentVersionParseError {
    Installed(semver::Error),
}

#[cfg(test)]
mod tests;
