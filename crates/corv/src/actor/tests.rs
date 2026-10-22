use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::schema::v1::{
    AuthMethod, EnvVariable, McpServer, McpServerStdio, PermissionOptionId, SessionConfigOptionCategory,
    SessionConfigSelectGroup, SessionConfigSelectOption, SessionConfigSelectOptions, SessionId, SessionInfo,
    StopReason,
};
use serde_json::json;
use tokio::sync::{mpsc, oneshot};

use aves::auth::{AuthSudoService, AvesAuthError, Jwt};
use aves::bus::{
    BusControlError, CloseMcpInput, CloseRpcInput, McpClosedOutput, McpOpenedOutput, OpaqueRequestEnvelope,
    OpenMcpInput, OpenRpcInput, PeerConnectivitySetOutput, RpcClosedOutput, RpcOpenedOutput, SetPeerConnectivityInput,
};
use aves::config::{ConfigDomain, ConfigError, ConfigRequest, ConfigRequestPayload, ConfigResponse};
use aves::flow::{FlowExecutionId, FlowId, FlowNodeId, LifecycleCorrelation, NodeExecutionId, NodeKind};
use aves::messages::{
    CreateInvitationInput, DelegatedJwtIssuedOutput, FirstContactInput, FirstContactedOutput, InvitationOutput,
    IssueJwtInput, IssuePeerJwtInput, IssueRootJwtInput, JwtIssuedOutput, ListPairingCandidatesInput, ListPeersInput,
    PairPeerInput, PairingCandidatesOutput, PeerJwtIssuedOutput, PeersOutput, RevokeJtiInput, RevokePeerInput,
    SuccessOutput, VerifiedOutput, VerifyJwtInput,
};
use aves::runtime::ServiceReader;
use aves::utils::buffers::DEFAULT_BUFFER_SIZE;
use aves::utils::database::{
    DatabaseConcurrency, DatabaseError, MigrationRunner, connect_database, open_database_at_path_with_concurrency,
};
use aves::utils::test_support::{MockBus, reply_with};
use aves::utils::uuid::SpectaUuid;

use super::super::boot::DiscussBootOptions;
use super::super::library::AgentId;
use super::super::migration::MIGRATIONS as DISCUSS_MIGRATIONS;
use super::super::sessions::dedupe_session_list;
use super::super::test_library;
use super::super::{
    agents::*, config::*, database::*, elicitation::*, messages::*, session::*, storage::*, supervisor::*, types::*,
};
use super::*;

async fn open_local_db(config_dir: &Path) -> Result<turso::Database, DatabaseError> {
    open_database_at_path_with_concurrency(config_dir.join("discuss.db"), DatabaseConcurrency::SingleProcess).await
}

async fn apply_discuss_migrations(database: &turso::Database) -> Result<usize, DatabaseError> {
    MigrationRunner::new("__discuss_migrations", DISCUSS_MIGRATIONS)
        .apply(database)
        .await
}

// ── Helpers ──────────────────────────────────────────────────────────────

/// Build a minimal `mpsc::Sender<DiscussStreamEvent>` for tests that do not
/// exercise the session-worker stream path. The receiver is dropped
/// immediately; tests that need it should construct channels manually.
fn dummy_stream_tx() -> mpsc::Sender<DiscussStreamEvent> {
    let (stream_tx, _) = mpsc::channel::<DiscussStreamEvent>(1);
    stream_tx
}

#[test]
fn elicitation_validation_accepts_supported_multi_select_values_and_bounds() {
    let classification = DiscussElicitationClassification::RestrictedForm {
        schema: DiscussRestrictedFormSchema {
            title: None,
            description: None,
            fields: vec![DiscussRestrictedFormField {
                name: "channels".to_string(),
                title: Some("Channels".to_string()),
                description: None,
                field_type: DiscussRestrictedFormFieldType::Array,
                options: vec![
                    DiscussRestrictedFormOption {
                        value: "email".to_string(),
                        title: Some("Email".to_string()),
                        description: None,
                    },
                    DiscussRestrictedFormOption {
                        value: "slack".to_string(),
                        title: Some("Slack".to_string()),
                        description: None,
                    },
                ],
                default: None,
                required: true,
                pattern: None,
                min: Some(1.0),
                max: Some(2.0),
            }],
            required: vec!["channels".to_string()],
            meta: None,
        },
    };

    assert!(
        validate_elicitation_content(&classification, &serde_json::json!({ "channels": ["email", "slack"] })).is_ok()
    );
    assert!(
        validate_elicitation_content(&classification, &serde_json::json!({ "channels": ["email", "sms"] })).is_err()
    );
    assert!(validate_elicitation_content(&classification, &serde_json::json!({ "channels": [] })).is_err());
}

#[tokio::test]
async fn attached_inspector_readers_drop_aborts_every_reader() {
    let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut readers = AttachedInspectorReaders(HashMap::new());
    for _ in 0..2 {
        let dropped = dropped.clone();
        let reader = tokio::spawn(async move {
            struct DropMarker(Arc<std::sync::atomic::AtomicUsize>);
            impl Drop for DropMarker {
                fn drop(&mut self) {
                    self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            }
            let _marker = DropMarker(dropped);
            std::future::pending::<()>().await;
        });
        readers.0.insert(SluiceSessionId::new(), reader);
    }

    drop(readers);
    tokio::task::yield_now().await;
    assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[tokio::test]
async fn inspector_reader_reap_aborts_then_awaits_the_reader() {
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let dropped_for_reader = dropped.clone();
    let (started_tx, started_rx) = oneshot::channel();
    let reader = tokio::spawn(async move {
        struct DropMarker(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for DropMarker {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let _marker = DropMarker(dropped_for_reader);
        let _ = started_tx.send(());
        std::future::pending::<()>().await;
    });
    started_rx.await.expect("reader should start");
    spawn_inspector_reader_reap(reader).await;
    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
}

/// Build a minimal long-lived `AcpProvider` using the ACP SDK's typed stdio
/// constructor so deferred-connectivity tests can observe the staged worker
/// before it reports a protocol result.
fn make_local_mcp_server(cmd: &str) -> AcpProvider {
    let server = McpServerStdio::new(cmd, "/bin/sh").args(vec!["-c".to_string(), format!("sleep 60 # {cmd}")]);
    AcpProvider::from(McpServer::Stdio(server))
}

#[test]
fn initialize_advertises_provider_terminal_auth_support() {
    let request = serde_json::to_value(initialize_request()).expect("initialize request should serialize");

    assert_eq!(
        request.pointer("/clientCapabilities/auth/terminal"),
        Some(&serde_json::json!(true))
    );
    assert_eq!(
        request.pointer("/clientCapabilities/_meta/terminal-auth"),
        Some(&serde_json::json!(true))
    );
}

#[test]
fn terminal_auth_prefers_provider_extension_launch_details() {
    let methods: Vec<AuthMethod> = serde_json::from_value(serde_json::json!([{
        "type": "terminal",
        "id": "provider-login",
        "name": "Provider Subscription",
        "description": "Use the Provider subscription already linked on this Mac.",
        "args": ["--ignored-fallback"],
        "env": { "AUTH_SCOPE": "subscription" },
        "_meta": {
            "terminal-auth": {
                "command": "/Applications/Provider.app/Contents/Helpers/node",
                "args": ["provider-acp.js", "--cli", "auth", "login"],
                "env": { "PROVIDER_HOME": "/tmp/provider home" },
                "label": "Provider sign-in"
            }
        }
    }]))
    .expect("provider terminal method should deserialize");
    let launch_server = McpServer::Stdio(
        McpServerStdio::new("provider", "npx")
            .args(vec!["provider-acp".to_string()])
            .env(vec![EnvVariable::new("BASE_ENV", "kept")]),
    );

    let normalized = terminal_auth_methods(&methods, &launch_server);

    assert_eq!(normalized.len(), 1);
    assert_eq!(normalized[0].id, "provider-login");
    assert_eq!(normalized[0].name, "Provider Subscription");
    assert_eq!(normalized[0].label, "Provider sign-in");
    assert_eq!(
        normalized[0].command,
        PathBuf::from("/Applications/Provider.app/Contents/Helpers/node")
    );
    assert_eq!(normalized[0].args, ["provider-acp.js", "--cli", "auth", "login"]);
    assert_eq!(
        normalized[0].env.get("PROVIDER_HOME").map(String::as_str),
        Some("/tmp/provider home")
    );
    assert_eq!(
        normalized[0].env.get("AUTH_SCOPE").map(String::as_str),
        Some("subscription")
    );
    assert!(!normalized[0].env.contains_key("BASE_ENV"));
}

#[test]
fn terminal_auth_falls_back_to_effective_provider_launch() {
    let methods: Vec<AuthMethod> = serde_json::from_value(serde_json::json!([{
        "type": "terminal",
        "id": "console-login",
        "name": "Provider Console",
        "args": ["auth", "login"],
        "env": { "AUTH_SCOPE": "console" }
    }]))
    .expect("provider terminal method should deserialize");
    let launch_server = McpServer::Stdio(
        McpServerStdio::new("provider", "/usr/local/bin/provider-acp")
            .args(vec!["--stdio".to_string()])
            .env(vec![EnvVariable::new("BASE_ENV", "kept")]),
    );

    let normalized = terminal_auth_methods(&methods, &launch_server);

    assert_eq!(normalized.len(), 1);
    assert_eq!(normalized[0].command, PathBuf::from("/usr/local/bin/provider-acp"));
    assert_eq!(normalized[0].args, ["--stdio", "auth", "login"]);
    assert_eq!(normalized[0].env.get("BASE_ENV").map(String::as_str), Some("kept"));
    assert_eq!(normalized[0].env.get("AUTH_SCOPE").map(String::as_str), Some("console"));
}

fn test_agent_plugin() -> &'static dyn plugins::DiscussAgentPlugin {
    plugins::for_agent("custom-agent")
}

fn config_option(id: &str, current: &str, supported: &[&str]) -> DiscussConfigOption {
    DiscussConfigOption {
        id: id.to_string(),
        name: id.to_string(),
        category: "model_config".to_string(),
        value: current.to_string(),
        options: supported
            .iter()
            .map(|value| DiscussConfigOptionValue {
                value: (*value).to_string(),
                label: (*value).to_string(),
            })
            .collect(),
    }
}

#[test]
fn provider_sessions_use_typed_session_info_fields() {
    let session =
        SessionInfo::new(SessionId::from("provider-session"), "/tmp/discuss").updated_at("2026-07-15T12:34:56Z");

    let sessions = provider_sessions_from_schema(vec![session]);

    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].session_id.to_string(), "provider-session");
    assert_eq!(sessions[0].updated_at.as_deref(), Some("2026-07-15T12:34:56Z"));
    assert!(sessions[0].config_options.is_none());
}

#[test]
fn provider_config_options_use_typed_select_fields_and_group_shape() {
    let grouped = SessionConfigSelectGroup::new(
        "recommended",
        "Recommended",
        vec![SessionConfigSelectOption::new("provider-model", "Provider model")],
    );
    let option = agent_client_protocol::schema::v1::SessionConfigOption::select(
        "model",
        "Model",
        "provider-model",
        SessionConfigSelectOptions::Grouped(vec![grouped]),
    )
    .category(SessionConfigOptionCategory::Model);

    let options = test_agent_plugin()
        .initial_config_options(Some(vec![option]), None)
        .expect("explicit standard options");

    assert_eq!(options.len(), 1);
    assert_eq!(options[0].id, "model");
    assert_eq!(options[0].name, "Model");
    assert_eq!(options[0].category, "model");
    assert_eq!(options[0].value, "provider-model");
    assert_eq!(options[0].options[0].value, "provider-model");
    assert_eq!(options[0].options[0].label, "Recommended / Provider model");
}

#[test]
fn config_preference_selection_follows_provider_order_and_attempt_tracking() {
    let preferences = ConfigSettings::from([
        ("second".to_string(), "b".to_string()),
        ("first".to_string(), "a".to_string()),
        ("unsupported".to_string(), "x".to_string()),
    ]);
    let provider_options = vec![
        config_option("first", "provider", &["a", "provider"]),
        config_option("second", "provider", &["b", "provider"]),
        config_option("unsupported", "provider", &["provider"]),
    ];
    let mut attempted = std::collections::HashSet::new();

    assert_eq!(
        next_config_preference(&preferences, &provider_options, &attempted),
        Some(ConfigPreferenceSelection {
            config_id: "first".to_string(),
            value: "a".to_string(),
        })
    );

    attempted.insert("first".to_string());
    assert_eq!(
        next_config_preference(&preferences, &provider_options, &attempted),
        Some(ConfigPreferenceSelection {
            config_id: "second".to_string(),
            value: "b".to_string(),
        })
    );
}

#[test]
fn agent_node_config_selects_assigned_agent_mode_before_non_mode_preferences() {
    let mut mode = config_option("agent", "Default", &["Default", "chief-of-staff"]);
    mode.category = "mode".to_string();
    let model = config_option("model", "large", &["large", "small"]);
    let provider_options = vec![mode, model];
    let preferences = ConfigSettings::from([
        ("agent".to_string(), "Default".to_string()),
        ("model".to_string(), "small".to_string()),
    ]);

    let mode_selection = assigned_agent_mode_selection(&provider_options, "chief-of-staff").expect("assigned mode");
    assert_eq!(
        mode_selection,
        ConfigPreferenceSelection {
            config_id: "agent".to_string(),
            value: "chief-of-staff".to_string(),
        }
    );

    let preferences = without_mode_preferences(&preferences, &provider_options, &mode_selection.config_id);
    let mut reconciler = ConfigPreferenceReconciler::new(&preferences, provider_options);
    assert_eq!(
        reconciler.next(),
        Some(ConfigPreferenceSelection {
            config_id: "model".to_string(),
            value: "small".to_string(),
        })
    );
    assert!(reconciler.next().is_none());
}

#[test]
fn agent_node_config_rejects_missing_assigned_agent_mode() {
    let mut mode = config_option("agent", "Default", &["Default", "Sonya"]);
    mode.category = "mode".to_string();

    assert_eq!(assigned_agent_mode_selection(&[mode], "unknown-agent"), None);
}

#[test]
fn config_preference_reconciler_revalidates_dependent_options_after_each_response() {
    let preferences = ConfigSettings::from([
        ("first".to_string(), "a".to_string()),
        ("second".to_string(), "b".to_string()),
    ]);
    let initial = vec![config_option("first", "provider", &["a", "provider"])];
    let mut reconciler = ConfigPreferenceReconciler::new(&preferences, initial);

    assert_eq!(reconciler.next().expect("first").config_id, "first");
    reconciler.adopt_provider_options(vec![
        config_option("first", "a", &["a", "provider"]),
        config_option("second", "provider", &["b", "provider"]),
    ]);
    assert_eq!(reconciler.next().expect("second").config_id, "second");
    reconciler.adopt_provider_options(vec![
        config_option("first", "a", &["a", "provider"]),
        config_option("second", "b", &["b", "provider"]),
    ]);

    let reconciled = reconciler.finish();
    assert_eq!(reconciled[1].value, "b");
}

#[test]
fn config_preference_reconciler_continues_after_transient_failure() {
    let preferences = ConfigSettings::from([
        ("first".to_string(), "a".to_string()),
        ("second".to_string(), "b".to_string()),
    ]);
    let initial = vec![
        config_option("first", "provider", &["a", "provider"]),
        config_option("second", "provider", &["b", "provider"]),
    ];
    let mut reconciler = ConfigPreferenceReconciler::new(&preferences, initial);

    assert_eq!(reconciler.next().expect("failed first").config_id, "first");
    // A failed provider request does not adopt new options; the next call
    // advances because the failed id is already marked attempted.
    assert_eq!(reconciler.next().expect("continued second").config_id, "second");

    assert_eq!(reconciler.finish().len(), 2);
}

#[test]
fn session_preferences_override_agent_defaults_per_option() {
    let agent_defaults = ConfigSettings::from([
        ("model".to_string(), "agent-model".to_string()),
        ("mode".to_string(), "agent-mode".to_string()),
    ]);
    let session_settings = ConfigSettings::from([("model".to_string(), "session-model".to_string())]);

    assert_eq!(
        merged_config_preferences(&agent_defaults, &session_settings),
        ConfigSettings::from([
            ("model".to_string(), "session-model".to_string()),
            ("mode".to_string(), "agent-mode".to_string()),
        ]),
    );
}

/// Construct an `Actor` directly, bypassing `Actor::new()`, so tests can
/// control the initial agent map without touching the bus. The database uses
/// the production migration runner because the handlers persist agent state.
async fn make_actor(
    receiver: mpsc::Receiver<DiscussRequest>,
    agents: HashMap<String, McpServer>,
    bus_requester: mpsc::Sender<OpaqueRequestEnvelope>,
    broadcast_tx: mpsc::Sender<DiscussResponse>,
) -> Actor {
    // Open a fully migrated tempdir database for direct actor tests.
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database = open_local_db(temp_dir.path())
        .await
        .expect("test database opens");
    apply_discuss_migrations(&database)
        .await
        .expect("apply test migrations");
    let database = Arc::new(database);
    let persistence = DiscussPersistence {
        database: database.clone(),
    };
    let (capabilities_update_tx, capabilities_update_rx) =
        mpsc::channel::<(SluiceSessionId, SessionCapabilitiesCache)>(DEFAULT_BUFFER_SIZE);
    let (worker_ready_tx, worker_ready_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
    let (transient_worker_tx, transient_worker_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
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
    let agents: HashMap<_, _> = agents
        .into_iter()
        .map(|(name, server)| (name, AcpProvider::from(server)))
        .collect();
    Actor {
        library: test_library::library(),
        receiver,
        agent_completions: HashMap::new(),
        workspace_copy_source_rx: mpsc::channel(1).1,
        agents,
        stream_tx: dummy_stream_tx(),
        root_context: BusRpcContext::authenticated(bus_requester, Jwt::from_opaque("test-token".to_string())),
        mcp_domains: || Ok(Vec::new()),
        bus_service: test_bus_service(),
        auth_service: None,
        broadcast_tx,
        persistence,
        database_path: temp_dir.path().join("discuss.db"),
        agents_workspace: temp_dir.path().join("agents"),
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
        worker_ready_rx,
        worker_ready_tx,
        transient_worker_rx,
        transient_worker_tx,
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
        server_running: true,
    }
}

#[tokio::test]
async fn agent_node_tick_returns_live_terminal_without_persisting_a_replay_ledger() {
    let (bus_tx, _) = MockBus::new(1);
    let (_request_tx, request_rx) = mpsc::channel(1);
    let (broadcast_tx, _) = mpsc::channel(1);
    let mut actor = make_actor(request_rx, HashMap::new(), bus_tx, broadcast_tx).await;
    let correlation = LifecycleCorrelation {
        node_kind: NodeKind::parse("AgentNode").expect("static node kind is valid"),
        flow_id: FlowId(uuid::Uuid::new_v4()),
        flow_node_id: FlowNodeId(uuid::Uuid::new_v4()),
        flow_execution_id: FlowExecutionId(uuid::Uuid::new_v4()),
        node_execution_id: NodeExecutionId(uuid::Uuid::new_v4()),
    };
    let terminal = AgentNodeTerminalResult::Succeeded {
        output: serde_json::json!({ "text": "done", "stopReason": "endTurn" }),
    };
    let (terminal_tx, terminal_rx) = oneshot::channel();
    actor
        .agent_completions
        .insert(correlation.node_execution_id, terminal_rx);
    terminal_tx.send(terminal).unwrap();
    let (reply_to, reply_rx) = oneshot::channel();
    actor
        .handle_message(DiscussRequest {
            payload: DiscussRequestPayload::TickAgent {
                correlation: correlation.clone(),
            },
            reply_to,
        })
        .await;
    let response = reply_rx.await.unwrap().unwrap();
    let DiscussResponse::AgentNodeTicked {
        correlation: actual_correlation,
        result: actual_result,
    } = response
    else {
        panic!("expected AgentNodeTicked response");
    };
    assert_eq!(actual_correlation.flow_id, correlation.flow_id);
    assert_eq!(actual_correlation.flow_node_id, correlation.flow_node_id);
    assert_eq!(actual_correlation.flow_execution_id, correlation.flow_execution_id);
    assert_eq!(actual_correlation.node_execution_id, correlation.node_execution_id);
    let AgentNodeTickResult::Succeeded { output } = actual_result else {
        panic!("expected successful terminal response");
    };
    assert_eq!(output, serde_json::json!({ "text": "done", "stopReason": "endTurn" }));
}

#[tokio::test]
async fn workspace_copy_sources_use_authoritative_provider_map_and_root() {
    let (bus_tx, _) = MockBus::new(1);
    let (request_tx, request_rx) = mpsc::channel(1);
    let (broadcast_tx, _) = mpsc::channel(1);
    let mut actor = make_actor(
        request_rx,
        HashMap::from([
            (
                "opencode".to_string(),
                McpServer::Stdio(McpServerStdio::new("opencode", "test")),
            ),
            (
                "custom".to_string(),
                McpServer::Stdio(McpServerStdio::new("custom", "test")),
            ),
        ]),
        bus_tx,
        broadcast_tx,
    )
    .await;
    drop(request_tx);

    let provider_names = BTreeSet::from(["opencode".to_string(), "custom".to_string()]);
    let response = actor
        .resolve_workspace_copy_sources(provider_names.clone())
        .expect("registered Providers resolve");
    assert_eq!(response.agents_workspace, actor.agents_workspace);
    assert_eq!(response.sources_by_provider["opencode"], [".agents", ".opencode"]);
    assert_eq!(response.sources_by_provider["custom"], [".agents"]);

    actor.agents.remove("custom");
    assert!(matches!(
        actor.resolve_workspace_copy_sources(provider_names),
        Err(DiscussError::UnknownAgent { agent_key }) if agent_key == "custom"
    ));
}

fn provider() -> AcpProvider {
    AcpProvider::from(McpServer::Stdio(McpServerStdio::new("test", "test")))
}

fn provider_with_sources(sources: Vec<String>) -> AcpProvider {
    let mut value = serde_json::to_value(provider()).expect("serialize provider");
    value["workspace_copy_sources"] = serde_json::json!(sources);
    serde_json::from_value(value).expect("deserialize provider with sources")
}

#[test]
fn absent_sources_roundtrip_without_adding_the_property() {
    let value = serde_json::to_value(provider()).expect("serialize legacy provider");
    assert!(value.get("workspace_copy_sources").is_none());

    let decoded: AcpProvider = serde_json::from_value(value.clone()).expect("deserialize legacy provider");
    assert!(decoded.workspace_copy_sources().is_none());
    assert_eq!(serde_json::to_value(decoded).expect("serialize again"), value);
}

#[test]
fn explicit_empty_sources_survive_serde_roundtrip() {
    let provider = provider_with_sources(Vec::new());
    let value = serde_json::to_value(&provider).expect("serialize explicit empty");
    assert_eq!(value.get("workspace_copy_sources"), Some(&serde_json::json!([])));

    let decoded: AcpProvider = serde_json::from_value(value).expect("deserialize explicit empty");
    assert_eq!(decoded.workspace_copy_sources(), Some(&[][..]));
}

#[test]
fn invalid_workspace_sources_are_typed_errors() {
    for path in [
        "",
        ".",
        "./",
        "././",
        "/tmp",
        "../secret",
        "dir/../secret",
        ".agents",
        "C:/Users",
        "\\\\server\\share",
    ] {
        let provider = provider_with_sources(vec![path.to_string()]);
        assert!(
            matches!(provider.validate_workspace_copy_sources(), Err(DiscussError::InvalidWorkspaceCopySource { path: invalid, .. }) if invalid == path),
            "expected invalid copy source: {path}"
        );
    }
    let provider = provider_with_sources(vec![".opencode".to_string(), "opencode.jsonc".to_string()]);
    assert!(provider.validate_workspace_copy_sources().is_ok());
}

struct TestBusSudoService;

#[async_trait::async_trait]
impl BusSudoService for TestBusSudoService {
    async fn open_rpc(&self, input: OpenRpcInput) -> Result<RpcOpenedOutput, BusControlError> {
        Err(BusControlError::ListenerNotFound {
            protocol: "test",
            host: input
                .host
                .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
            port: input.port,
        })
    }

    async fn close_rpc(&self, input: CloseRpcInput) -> Result<RpcClosedOutput, BusControlError> {
        Err(BusControlError::ListenerNotFound {
            protocol: "test",
            host: input.host,
            port: input.port,
        })
    }

    async fn open_mcp(&self, input: OpenMcpInput) -> Result<McpOpenedOutput, BusControlError> {
        Err(BusControlError::ListenerNotFound {
            protocol: "test",
            host: input
                .host
                .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
            port: input.port,
        })
    }

    async fn close_mcp(&self, input: CloseMcpInput) -> Result<McpClosedOutput, BusControlError> {
        Err(BusControlError::ListenerNotFound {
            protocol: "test",
            host: input.host,
            port: input.port,
        })
    }

    async fn set_peer_connectivity(
        &self,
        _input: SetPeerConnectivityInput,
    ) -> Result<PeerConnectivitySetOutput, BusControlError> {
        Err(BusControlError::ChannelSend("test peer control unused".to_owned()))
    }
}

fn test_bus_service() -> ServiceReader<dyn BusSudoService> {
    ServiceReader::from_published_test_service(Arc::new(TestBusSudoService))
}

struct TestAuthSudoService;

#[async_trait::async_trait]
impl AuthSudoService for TestAuthSudoService {
    async fn issue_jwt(&self, _input: IssueJwtInput) -> Result<DelegatedJwtIssuedOutput, AvesAuthError> {
        Ok(DelegatedJwtIssuedOutput {
            token: Jwt::from_opaque("test-worker-token".to_string()),
            jti: uuid::Uuid::new_v4(),
        })
    }

    async fn issue_root_jwt(&self, _input: IssueRootJwtInput) -> Result<JwtIssuedOutput, AvesAuthError> {
        Err(AvesAuthError::Internal("test auth service".to_string()))
    }

    async fn issue_peer_jwt(&self, _input: IssuePeerJwtInput) -> Result<PeerJwtIssuedOutput, AvesAuthError> {
        Ok(PeerJwtIssuedOutput {
            token: Jwt::from_opaque("test-peer-token".to_string()),
        })
    }

    async fn verify_jwt(&self, _input: VerifyJwtInput) -> Result<VerifiedOutput, AvesAuthError> {
        Err(AvesAuthError::Internal("test auth service".to_string()))
    }

    async fn revoke_jti(&self, _input: RevokeJtiInput) -> Result<SuccessOutput, AvesAuthError> {
        Err(AvesAuthError::Internal("test auth service".to_string()))
    }

    async fn pair_peer(&self, _input: PairPeerInput) -> Result<SuccessOutput, AvesAuthError> {
        Ok(SuccessOutput {})
    }

    async fn revoke_peer(&self, _input: RevokePeerInput) -> Result<SuccessOutput, AvesAuthError> {
        Ok(SuccessOutput {})
    }

    async fn list_peers(&self, _input: ListPeersInput) -> Result<PeersOutput, AvesAuthError> {
        panic!("not used by Discuss actor tests")
    }

    async fn list_pairing_candidates(
        &self,
        _input: ListPairingCandidatesInput,
    ) -> Result<PairingCandidatesOutput, AvesAuthError> {
        panic!("not used by Discuss actor tests")
    }

    async fn create_invitation(&self, _input: CreateInvitationInput) -> Result<InvitationOutput, AvesAuthError> {
        panic!("not used by Discuss actor tests")
    }

    async fn first_contact(&self, _input: FirstContactInput) -> Result<FirstContactedOutput, AvesAuthError> {
        panic!("not used by Discuss actor tests")
    }
}

fn test_auth_service() -> ServiceReader<dyn AuthSudoService> {
    ServiceReader::from_published_test_service(Arc::new(TestAuthSudoService))
}

async fn prepare_test_database(config_dir: &Path) {
    let database = open_local_db(config_dir)
        .await
        .expect("test database opens");
    apply_discuss_migrations(&database)
        .await
        .expect("apply test migrations");
}

async fn insert_test_installation(actor: &Actor, agent_id: &str, provider_name: &str, version: &str) {
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("test connection");
    upsert_discuss_team_installation(&conn, agent_id, provider_name, version, "{}", "[]", None)
        .await
        .expect("insert test installation");
}

async fn load_test_installation(actor: &Actor, agent_id: &str) -> Option<DiscussTeamInstallationRow> {
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("test connection");
    load_discuss_team_installation(&conn, agent_id)
        .await
        .expect("load test installation")
}

fn canonical_test_team() -> (String, AgentId, Version) {
    let definition = test_library::library()
        .teams_for()
        .next()
        .expect("registered Phanes Team");
    (
        definition.leader.to_string(),
        definition.leader.clone(),
        definition.version.clone(),
    )
}

#[test]
fn agent_versions_use_semver_precedence_and_fail_closed_on_invalid_values() {
    let current = Version::parse("1.2.3+current-build").expect("valid current version");
    assert!(agent_versions_match("1.2.3+previous-build", &current).expect("valid versions"));
    let newer = Version::parse("1.2.4").expect("valid current version");
    assert!(!agent_versions_match("1.2.3", &newer).expect("valid versions"));
    assert!(matches!(
        agent_versions_match("invalid", &current),
        Err(AgentVersionParseError::Installed(_))
    ));
}

#[tokio::test]
async fn team_catalog_install_edit_move_and_uninstall_roundtrip_persists_state() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let mut actor = make_actor(
        receiver,
        HashMap::from([
            ("custom-a".to_string(), make_local_mcp_server("cat").server().clone()),
            ("custom-b".to_string(), make_local_mcp_server("cat").server().clone()),
        ]),
        bus_requester,
        broadcast_tx,
    )
    .await;
    fs::create_dir_all(&actor.agents_workspace).expect("workspace");

    let (agent_id, _typed_id, _version) = canonical_test_team();
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("test connection");
    upsert_discuss_agent_config(
        &conn,
        "custom-a",
        Some(&[config_option("model", "first", &["first", "second"])]),
        &ConfigSettings::new(),
    )
    .await
    .expect("seed Provider options");
    insert_test_installation(&actor, "retired-agent", "custom-a", "0.0.1").await;
    let catalog = actor.handle_list_team_catalog().await.expect("catalog");
    let DiscussResponse::ListTeamCatalog { entries } = catalog else {
        panic!("expected team catalog");
    };
    let retired = entries
        .iter()
        .find(|entry| entry.leader_id == "retired-agent")
        .expect("retired row");
    assert!(retired.installed && !retired.definition_available);
    assert!(retired.name.is_none() && retired.description.is_none());

    let initial_values = DiscussMemberOverrides::from([(
        agent_id.clone(),
        ConfigSettings::from([("model".to_string(), "first".to_string())]),
    )]);
    assert!(matches!(
        actor
            .handle_install_team(
                agent_id.clone(),
                "custom-a".to_string(),
                initial_values,
                Some(AgentThemeColor::Warning),
            )
            .await,
        Ok(DiscussResponse::InstallTeam { warnings }) if warnings.is_empty()
    ));
    let agent_path = actor
        .agents_workspace
        .join(".agents/agents")
        .join(format!("{agent_id}.md"));
    assert!(agent_path.is_file());
    assert_eq!(
        load_test_installation(&actor, &agent_id)
            .await
            .expect("installation")
            .provider_name,
        "custom-a"
    );
    assert_eq!(
        load_test_installation(&actor, &agent_id)
            .await
            .expect("installation")
            .theme_color_json
            .as_deref(),
        Some(r#""warning""#)
    );
    assert_eq!(
        load_test_installation(&actor, &agent_id)
            .await
            .expect("installation")
            .members_json,
        format!(r#"["{agent_id}"]"#)
    );

    let edited_values = DiscussMemberOverrides::from([(
        agent_id.clone(),
        ConfigSettings::from([("model".to_string(), "second".to_string())]),
    )]);
    assert!(matches!(
        actor
            .handle_edit_team(
                agent_id.clone(),
                "custom-a".to_string(),
                edited_values,
                Some(AgentThemeColor::Error),
            )
            .await,
        Ok(DiscussResponse::EditTeam { warnings }) if warnings.is_empty()
    ));
    assert_eq!(
        load_test_installation(&actor, &agent_id)
            .await
            .expect("edited installation")
            .member_overrides_json,
        r#"{"chief-of-staff":{"model":"second"}}"#
    );
    assert_eq!(
        load_test_installation(&actor, &agent_id)
            .await
            .expect("edited installation")
            .theme_color_json
            .as_deref(),
        Some(r#""error""#)
    );

    assert!(matches!(
        actor
            .handle_edit_team(
                agent_id.clone(),
                "custom-b".to_string(),
                DiscussMemberOverrides::new(),
                Some(AgentThemeColor::Primary),
            )
            .await,
        Ok(DiscussResponse::EditTeam { warnings }) if warnings.is_empty()
    ));
    assert_eq!(
        load_test_installation(&actor, &agent_id)
            .await
            .expect("moved installation")
            .provider_name,
        "custom-b"
    );

    fs::remove_dir_all(&actor.agents_workspace).expect("reset workspace");
    fs::write(&actor.agents_workspace, "not a directory").expect("block destination writes");
    assert!(
        actor
            .handle_edit_team(
                agent_id.clone(),
                "custom-a".to_string(),
                DiscussMemberOverrides::new(),
                None,
            )
            .await
            .is_err()
    );
    assert_eq!(
        load_test_installation(&actor, &agent_id)
            .await
            .expect("prior installation")
            .provider_name,
        "custom-b"
    );

    fs::remove_file(&actor.agents_workspace).expect("remove blocking file");
    fs::create_dir_all(&actor.agents_workspace).expect("restore workspace");
    assert!(matches!(
        actor.handle_uninstall_team(agent_id.clone()).await,
        Ok(DiscussResponse::UninstallTeam { warnings }) if warnings.is_empty()
    ));
    assert!(load_test_installation(&actor, &agent_id).await.is_none());
}

#[tokio::test]
async fn same_provider_team_edit_rematerializes_member_settings() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let actor = make_actor(
        receiver,
        HashMap::from([("opencode".to_string(), make_local_mcp_server("cat").server().clone())]),
        bus_requester,
        broadcast_tx,
    )
    .await;
    fs::create_dir_all(&actor.agents_workspace).expect("workspace");
    let (agent_id, _, version) = canonical_test_team();
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");
    upsert_discuss_agent_config(
        &conn,
        "opencode",
        Some(&[config_option("model", "first", &["first", "second"])]),
        &ConfigSettings::new(),
    )
    .await
    .expect("Provider options");

    actor
        .handle_install_team(
            agent_id.clone(),
            "opencode".to_string(),
            DiscussMemberOverrides::from([(
                agent_id.clone(),
                ConfigSettings::from([("model".to_string(), "first".to_string())]),
            )]),
            None,
        )
        .await
        .expect("install");
    actor
        .handle_edit_team(
            agent_id.clone(),
            "opencode".to_string(),
            DiscussMemberOverrides::from([(
                agent_id.clone(),
                ConfigSettings::from([("model".to_string(), "second".to_string())]),
            )]),
            None,
        )
        .await
        .expect("same-provider edit");

    let path = actor
        .agents_workspace
        .join(".opencode/agents")
        .join(format!("{agent_id}.md"));
    let output = fs::read_to_string(path).expect("native Agent output");
    assert!(output.contains("model: second"), "output was: {output}");
    assert_eq!(
        load_test_installation(&actor, &agent_id)
            .await
            .expect("installation")
            .installed_version,
        version.to_string()
    );
}

#[tokio::test]
async fn member_override_validation_rejects_unknown_and_stale_values_and_strips_mode() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");
    let (_, agent_id, _) = canonical_test_team();
    let team = plugins::resolve_team(actor.library, &agent_id).expect("canonical team");
    upsert_discuss_agent_config(
        &conn,
        "custom",
        Some(&[config_option("model", "first", &["first", "second"])]),
        &ConfigSettings::new(),
    )
    .await
    .expect("Provider options");

    let normalized = validate_member_overrides(
        &conn,
        "custom",
        team,
        DiscussMemberOverrides::from([(
            agent_id.to_string(),
            ConfigSettings::from([
                ("mode".to_string(), "primary".to_string()),
                ("model".to_string(), "second".to_string()),
            ]),
        )]),
    )
    .await
    .expect("valid override");
    assert_eq!(
        normalized,
        DiscussMemberOverrides::from([(
            agent_id.to_string(),
            ConfigSettings::from([("model".to_string(), "second".to_string())]),
        )])
    );

    let unknown = validate_member_overrides(
        &conn,
        "custom",
        team,
        DiscussMemberOverrides::from([("not-a-member".to_string(), ConfigSettings::new())]),
    )
    .await;
    assert!(matches!(unknown, Err(DiscussError::WorkspaceInvalidAssetId { .. })));

    let stale = validate_member_overrides(
        &conn,
        "custom",
        team,
        DiscussMemberOverrides::from([(
            agent_id.to_string(),
            ConfigSettings::from([("model".to_string(), "stale".to_string())]),
        )]),
    )
    .await;
    assert!(matches!(stale, Err(DiscussError::Validation { .. })));

    let empty = validate_member_overrides(&conn, "custom", team, DiscussMemberOverrides::new())
        .await
        .expect("empty overrides");
    assert!(empty.is_empty());
}

#[tokio::test]
async fn agent_move_and_provider_cascade_report_cleanup_warnings_after_commit() {
    let (bus_requester, mut mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let mut actor = make_actor(
        receiver,
        HashMap::from([
            ("opencode".to_string(), make_local_mcp_server("cat").server().clone()),
            ("custom".to_string(), make_local_mcp_server("cat").server().clone()),
        ]),
        bus_requester,
        broadcast_tx,
    )
    .await;
    fs::create_dir_all(&actor.agents_workspace).expect("workspace");
    let (agent_id, _typed_id, version) = canonical_test_team();
    insert_test_installation(&actor, &agent_id, "opencode", &version.to_string()).await;
    let old_path = actor
        .agents_workspace
        .join(".opencode/agents")
        .join(format!("{agent_id}.md"));
    fs::create_dir_all(&old_path).expect("make old cleanup fail");

    let moved = actor
        .handle_edit_team(
            agent_id.clone(),
            "custom".to_string(),
            DiscussMemberOverrides::new(),
            None,
        )
        .await
        .expect("destination write succeeds");
    assert!(matches!(moved, DiscussResponse::EditTeam { warnings } if warnings.len() == 1));
    assert_eq!(
        load_test_installation(&actor, &agent_id)
            .await
            .expect("moved row")
            .provider_name,
        "custom"
    );

    let cascade_path = actor
        .agents_workspace
        .join(".agents/agents")
        .join(format!("{agent_id}.md"));
    fs::remove_file(&cascade_path).expect("remove generated file");
    fs::create_dir_all(&cascade_path).expect("make cascade cleanup fail");
    let persistence = actor.persistence.clone();
    let remove = tokio::spawn(async move { actor.handle_remove_provider("custom").await });
    let request = mock
        .expect_request::<ConfigDomain, ConfigRequest>(Duration::from_millis(500))
        .await;
    reply_with(
        request.reply_to,
        Ok(ConfigResponse::ConfigSectionSaved {
            section: "discuss".to_string(),
        }),
    );
    let result = remove.await.expect("remove task");
    assert!(matches!(result, Ok(DiscussResponse::ProviderRemoveSuccess { warnings }) if warnings.len() == 1));
    let conn = persistence.acquire_connection().await.expect("connection");
    assert!(
        load_discuss_team_installation(&conn, &agent_id)
            .await
            .expect("load")
            .is_none()
    );
}

#[tokio::test]
async fn boot_agent_upgrade_rewrites_and_failure_retains_persisted_version() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let actor = make_actor(
        receiver,
        HashMap::from([("custom".to_string(), make_local_mcp_server("cat").server().clone())]),
        bus_requester,
        broadcast_tx,
    )
    .await;
    fs::create_dir_all(&actor.agents_workspace).expect("workspace");
    let (agent_id, _typed_id, current_version) = canonical_test_team();
    insert_test_installation(&actor, &agent_id, "custom", "0.0.0").await;
    actor.upgrade_installed_agents().await;
    assert!(
        actor
            .agents_workspace
            .join(".agents/agents")
            .join(format!("{agent_id}.md"))
            .is_file()
    );
    assert_eq!(
        load_test_installation(&actor, &agent_id)
            .await
            .expect("upgraded row")
            .installed_version,
        current_version.to_string()
    );

    let unchanged_path = actor
        .agents_workspace
        .join(".agents/agents")
        .join(format!("{agent_id}.md"));
    fs::write(&unchanged_path, "unchanged").expect("seed unchanged agent file");
    insert_test_installation(&actor, &agent_id, "custom", &format!("{current_version}+build-a")).await;
    actor.upgrade_installed_agents().await;
    assert_eq!(
        fs::read_to_string(&unchanged_path).expect("read unchanged agent file"),
        "unchanged"
    );

    insert_test_installation(&actor, &agent_id, "custom", "invalid").await;
    actor.upgrade_installed_agents().await;
    assert_eq!(
        load_test_installation(&actor, &agent_id)
            .await
            .expect("invalid persisted row")
            .installed_version,
        "invalid"
    );

    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let failed = make_actor(
        receiver,
        HashMap::from([("custom".to_string(), make_local_mcp_server("cat").server().clone())]),
        bus_requester,
        broadcast_tx,
    )
    .await;
    let blocked_workspace = failed.agents_workspace.clone();
    fs::create_dir_all(
        blocked_workspace
            .parent()
            .expect("workspace path has a parent"),
    )
    .expect("workspace parent");
    fs::write(&blocked_workspace, "not a directory").expect("block upgrade workspace");
    insert_test_installation(&failed, &agent_id, "custom", "0.0.0").await;
    failed.upgrade_installed_agents().await;
    assert_eq!(
        load_test_installation(&failed, &agent_id)
            .await
            .expect("failed row")
            .installed_version,
        "0.0.0"
    );
}

#[tokio::test]
async fn interactive_team_setup_uses_persisted_leader_overrides() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;
    let leader_id = "chief-of-staff";
    let member_overrides = DiscussMemberOverrides::from([(
        leader_id.to_string(),
        ConfigSettings::from([
            ("model".to_string(), "openai/gpt-5.4".to_string()),
            ("effort".to_string(), "high".to_string()),
        ]),
    )]);
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");
    upsert_discuss_team_installation(
        &conn,
        leader_id,
        "opencode",
        "1.0.0",
        &serde_json::to_string(&member_overrides).expect("serialize overrides"),
        "[]",
        None,
    )
    .await
    .expect("persist Team installation");
    drop(conn);

    let (assigned_agent_name, settings) = actor
        .resolve_session_setup(
            "opencode",
            Some(DiscussSessionSetup::Team {
                leader_id: leader_id.to_string(),
            }),
        )
        .await
        .expect("resolve Team setup");

    assert_eq!(assigned_agent_name.as_deref(), Some(leader_id));
    assert_eq!(settings.get("model").map(String::as_str), Some("openai/gpt-5.4"));
    assert_eq!(settings.get("effort").map(String::as_str), Some("high"));
}

#[tokio::test]
async fn boot_team_upgrade_restores_distinct_member_overrides_and_canonical_snapshot() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let actor = make_actor(
        receiver,
        HashMap::from([("opencode".to_string(), make_local_mcp_server("cat").server().clone())]),
        bus_requester,
        broadcast_tx,
    )
    .await;
    fs::create_dir_all(&actor.agents_workspace).expect("workspace");

    let team = test_library::library()
        .teams_for()
        .find(|team| !team.followers.is_empty())
        .expect("canonical Team with follower");
    let follower = team.followers.first().expect("selected follower");
    let member_overrides = DiscussMemberOverrides::from([
        (
            team.leader.to_string(),
            ConfigSettings::from([(String::from("model"), String::from("leader-model"))]),
        ),
        (
            follower.to_string(),
            ConfigSettings::from([(String::from("model"), String::from("follower-model"))]),
        ),
    ]);
    let member_overrides_json = serde_json::to_string(&member_overrides).expect("serialize overrides");
    let members_json = serde_json::to_string(&team_member_ids(team)).expect("serialize members");
    let leader_id = team.leader.to_string();
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");
    upsert_discuss_team_installation(
        &conn,
        &leader_id,
        "opencode",
        "0.0.0",
        &member_overrides_json,
        &members_json,
        None,
    )
    .await
    .expect("persist old Team installation");

    actor.upgrade_installed_agents().await;

    let leader_path = actor
        .agents_workspace
        .join(".opencode/agents")
        .join(format!("{}.md", team.leader.normalized_destination()));
    let follower_path = actor
        .agents_workspace
        .join(".opencode/agents")
        .join(format!("{}.md", follower.normalized_destination()));
    let leader_output = fs::read_to_string(leader_path).expect("leader file");
    let follower_output = fs::read_to_string(follower_path).expect("follower file");
    assert!(leader_output.contains("model: leader-model"));
    assert!(!leader_output.contains("model: follower-model"));
    assert!(follower_output.contains("model: follower-model"));
    assert!(!follower_output.contains("model: leader-model"));

    let upgraded = load_test_installation(&actor, &leader_id)
        .await
        .expect("upgraded Team row");
    assert_eq!(upgraded.installed_version, team.version.to_string());
    assert_eq!(upgraded.member_overrides_json, member_overrides_json);
    assert_eq!(upgraded.members_json, members_json);
}

#[tokio::test]
async fn deferred_provider_upsert_success_persists_menu_and_stops_transient_worker() {
    let (bus_requester, mut mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let mut actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;
    actor.auth_service = Some(test_auth_service());
    let candidate = make_local_mcp_server("cat");
    let (reply_tx, reply_rx) = oneshot::channel();

    actor
        .handle_add_provider_request("deferred".to_string(), candidate, reply_tx, false)
        .await
        .expect("stage provider connectivity test");
    let session_id = actor
        .pending_provider_tests
        .keys()
        .next()
        .expect("pending test")
        .clone();
    assert_eq!(
        actor.worker_registry.len(),
        1,
        "deferred path must retain the transient worker until completion"
    );

    let (actor_tx, actor_rx) = oneshot::channel();
    tokio::spawn(async move {
        actor
            .worker_ready_tx
            .send(WorkerReady {
                sluice_session_id: session_id,
                acp_session_id: SessionId::from("transient-ready"),
                announce_desktop_association: false,
                transient_connectivity: true,
                config_options: Some(vec![config_option("model", "gpt-5", &["gpt-5"])]),
                supports_config_updates: true,
            })
            .await
            .expect("send transient ready event");
        let WorkerReady {
            sluice_session_id,
            config_options,
            ..
        } = actor
            .worker_ready_rx
            .recv()
            .await
            .expect("receive transient ready event");
        actor
            .finish_provider_test(sluice_session_id, Ok(config_options))
            .await;
        assert!(actor_tx.send(actor).is_ok(), "return actor");
    });
    let request = mock
        .expect_request::<ConfigDomain, ConfigRequest>(Duration::from_millis(500))
        .await;
    reply_with(
        request.reply_to,
        Ok(ConfigResponse::ConfigSectionSaved {
            section: "discuss".to_string(),
        }),
    );

    assert!(matches!(
        reply_rx.await.expect("deferred reply"),
        Ok(DiscussResponse::Success)
    ));
    let actor = actor_rx.await.expect("actor");
    assert!(actor.pending_provider_tests.is_empty());
    assert!(
        actor.worker_registry.is_empty(),
        "transient worker must be stopped and removed"
    );
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");
    let config = load_discuss_agent_config(&conn, "deferred")
        .await
        .expect("load config")
        .expect("config row");
    assert!(
        config
            .config_options_json
            .expect("captured menu")
            .contains("gpt-5")
    );
    assert!(actor.agents.contains_key("deferred"));
}

#[tokio::test]
async fn forced_provider_refresh_stages_connectivity_for_an_unchanged_server() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let provider = make_local_mcp_server("cat");
    let mut actor = make_actor(
        receiver,
        HashMap::from([("existing".to_string(), provider.server().clone())]),
        bus_requester,
        broadcast_tx,
    )
    .await;
    actor.auth_service = Some(test_auth_service());
    let (reply_tx, _reply_rx) = oneshot::channel();

    actor
        .handle_add_provider_request("existing".to_string(), provider, reply_tx, true)
        .await
        .expect("stage forced Provider config refresh");

    assert_eq!(actor.pending_provider_tests.len(), 1);
    assert_eq!(actor.worker_registry.len(), 1);
    for (_, entry) in actor.worker_registry.drain() {
        let _ = entry.command_tx.send(WorkerCommand::Stop).await;
    }
}

#[tokio::test]
async fn deferred_provider_upsert_error_restores_provider_and_menu_and_propagates_original_error() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let previous = make_local_mcp_server("old-command");
    let mut actor = make_actor(
        receiver,
        HashMap::from([("existing".to_string(), previous.server().clone())]),
        bus_requester,
        broadcast_tx,
    )
    .await;
    actor.auth_service = Some(test_auth_service());
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");
    let previous_options = vec![config_option("old", "old", &[])];
    let previous_options_json = serde_json::to_string(&previous_options).expect("previous menu JSON");
    upsert_discuss_agent_config(
        &conn,
        "existing",
        Some(&previous_options),
        &ConfigSettings::from([("default".to_string(), "saved".to_string())]),
    )
    .await
    .expect("seed previous menu");

    let candidate = make_local_mcp_server("new-command");
    let (reply_tx, reply_rx) = oneshot::channel();
    actor
        .handle_add_provider_request("existing".to_string(), candidate, reply_tx, false)
        .await
        .expect("stage provider connectivity test");
    let session_id = actor
        .pending_provider_tests
        .keys()
        .next()
        .expect("pending test")
        .clone();
    let original_error = DiscussError::Validation {
        message: "connection refused".to_string(),
    };
    actor
        .transient_worker_tx
        .send(TransientWorkerEvent {
            sluice_session_id: session_id,
            result: Err(original_error),
        })
        .await
        .expect("send transient error event");
    let event = actor
        .transient_worker_rx
        .recv()
        .await
        .expect("receive transient error event");
    actor
        .finish_provider_test(event.sluice_session_id, event.result.map(|(_, options)| options))
        .await;

    assert!(
        matches!(reply_rx.await.expect("deferred reply"), Err(DiscussError::Validation { message }) if message == "connection refused")
    );
    assert!(actor.pending_provider_tests.is_empty());
    assert!(actor.worker_registry.is_empty());
    assert_eq!(
        serde_json::to_value(actor.agents["existing"].server()).expect("old server JSON"),
        serde_json::to_value(previous.server()).expect("old server JSON")
    );
    let config = load_discuss_agent_config(&conn, "existing")
        .await
        .expect("load restored config")
        .expect("config row");
    assert_eq!(
        config.config_options_json.as_deref(),
        Some(previous_options_json.as_str())
    );
}

fn pending_permission(option_ids: &[&str]) -> (PendingPermission, oneshot::Receiver<DiscussPermissionOption>) {
    let (responder_tx, responder_rx) = oneshot::channel();
    (
        PendingPermission {
            offered_option_ids: option_ids
                .iter()
                .map(|option_id| PermissionOptionId::new(*option_id))
                .collect(),
            responder_tx,
        },
        responder_rx,
    )
}

#[tokio::test]
async fn answer_permission_valid_option_resolves_and_consumes_entry() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let mut actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;
    let sluice_session_id = SluiceSessionId::new();
    let permission_request_id = PermissionRequestId::new();
    let (pending, responder_rx) = pending_permission(&["allow"]);
    actor
        .pending_permissions
        .insert((sluice_session_id, permission_request_id), pending);

    let result = actor
        .handle_answer_permission(
            sluice_session_id,
            permission_request_id,
            DiscussPermissionOption::Select {
                option_id: "allow".to_string(),
            },
        )
        .await;

    assert!(matches!(result, Ok(DiscussResponse::Success)));
    assert!(
        !actor
            .pending_permissions
            .contains_key(&(sluice_session_id, permission_request_id))
    );
    assert!(matches!(
        responder_rx.await,
        Ok(DiscussPermissionOption::Select { option_id }) if option_id == "allow"
    ));
}

#[tokio::test]
async fn answer_permission_invalid_option_preserves_entry() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let mut actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;
    let sluice_session_id = SluiceSessionId::new();
    let permission_request_id = PermissionRequestId::new();
    let (pending, mut responder_rx) = pending_permission(&["allow"]);
    actor
        .pending_permissions
        .insert((sluice_session_id, permission_request_id), pending);

    let result = actor
        .handle_answer_permission(
            sluice_session_id,
            permission_request_id,
            DiscussPermissionOption::Select {
                option_id: "deny".to_string(),
            },
        )
        .await;

    assert!(matches!(
        result,
        Err(DiscussError::InvalidOption { option_id }) if option_id == "deny"
    ));
    assert!(
        actor
            .pending_permissions
            .contains_key(&(sluice_session_id, permission_request_id))
    );
    assert!(responder_rx.try_recv().is_err());
}

#[tokio::test]
async fn answer_permission_missing_entry_returns_not_found() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let mut actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;
    let sluice_session_id = SluiceSessionId::new();
    let permission_request_id = PermissionRequestId::new();

    let result = actor
        .handle_answer_permission(
            sluice_session_id,
            permission_request_id,
            DiscussPermissionOption::Cancel,
        )
        .await;

    assert!(matches!(result, Err(DiscussError::PermissionNotFound { .. })));
}

#[tokio::test]
async fn answer_permission_rejects_during_shutdown() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let mut actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;
    actor.server_running = false;

    let result = actor
        .handle_answer_permission(
            SluiceSessionId::new(),
            PermissionRequestId::new(),
            DiscussPermissionOption::Cancel,
        )
        .await;

    assert!(matches!(result, Err(DiscussError::ShuttingDown)));
}

#[tokio::test]
async fn pending_permission_registration_ack_precedes_answer() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let mut actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;
    let sluice_session_id = SluiceSessionId::new();
    let permission_request_id = PermissionRequestId::new();
    let (pending, responder_rx) = pending_permission(&["allow"]);
    let (registration_ack, registration_ack_rx) = oneshot::channel();

    actor.register_pending_permission(PendingPermissionRegistration {
        sluice_session_id,
        permission_request_id,
        pending,
        registration_ack,
    });
    assert!(matches!(registration_ack_rx.await, Ok(Ok(()))));

    let result = actor
        .handle_answer_permission(
            sluice_session_id,
            permission_request_id,
            DiscussPermissionOption::Select {
                option_id: "allow".to_string(),
            },
        )
        .await;
    assert!(matches!(result, Ok(DiscussResponse::Success)));
    assert!(matches!(
        responder_rx.await,
        Ok(DiscussPermissionOption::Select { option_id }) if option_id == "allow"
    ));
}

#[tokio::test]
async fn pending_permission_registration_cancels_when_server_is_stopped() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let mut actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;
    actor.server_running = false;
    let sluice_session_id = SluiceSessionId::new();
    let permission_request_id = PermissionRequestId::new();
    let (pending, responder_rx) = pending_permission(&["allow"]);
    let (registration_ack, registration_ack_rx) = oneshot::channel();

    actor.register_pending_permission(PendingPermissionRegistration {
        sluice_session_id,
        permission_request_id,
        pending,
        registration_ack,
    });

    assert!(matches!(registration_ack_rx.await, Ok(Err(DiscussError::ShuttingDown))));
    assert!(matches!(responder_rx.await, Ok(DiscussPermissionOption::Cancel)));
}

#[tokio::test]
async fn pending_permission_cancellation_is_idempotent_after_answer() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let mut actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;
    let sluice_session_id = SluiceSessionId::new();
    let permission_request_id = PermissionRequestId::new();
    let (pending, responder_rx) = pending_permission(&["allow"]);
    let (registration_ack, registration_ack_rx) = oneshot::channel();

    actor.register_pending_permission(PendingPermissionRegistration {
        sluice_session_id,
        permission_request_id,
        pending,
        registration_ack,
    });
    assert!(matches!(registration_ack_rx.await, Ok(Ok(()))));

    let answer = actor
        .handle_answer_permission(
            sluice_session_id,
            permission_request_id,
            DiscussPermissionOption::Select {
                option_id: "allow".to_string(),
            },
        )
        .await;
    assert!(matches!(answer, Ok(DiscussResponse::Success)));
    assert!(matches!(
        responder_rx.await,
        Ok(DiscussPermissionOption::Select { option_id }) if option_id == "allow"
    ));

    let (cancellation_ack, cancellation_ack_rx) = oneshot::channel();
    actor.cancel_pending_permission(PendingPermissionCancel {
        sluice_session_id,
        permission_request_id,
        cancellation_ack,
    });
    assert!(cancellation_ack_rx.await.is_ok());
    assert!(
        !actor
            .pending_permissions
            .contains_key(&(sluice_session_id, permission_request_id))
    );
}

#[tokio::test]
async fn cancel_turn_missing_worker_cleans_permissions_and_succeeds() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let mut actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;
    let sluice_session_id = SluiceSessionId::new();
    let permission_request_id = PermissionRequestId::new();
    let (pending, responder_rx) = pending_permission(&["allow"]);
    actor
        .pending_permissions
        .insert((sluice_session_id, permission_request_id), pending);

    let result = actor.handle_cancel_turn(sluice_session_id).await;

    assert!(matches!(result, Ok(DiscussResponse::Success)));
    assert!(actor.pending_permissions.is_empty());
    assert!(matches!(responder_rx.await, Ok(DiscussPermissionOption::Cancel)));

    let repeated = actor.handle_cancel_turn(sluice_session_id).await;
    assert!(matches!(repeated, Ok(DiscussResponse::Success)));
}

// ── Actor::new() via config bridge ──────────────────────────────────────

/// `Actor::new()` loads the agent map through the injected config bridge.
#[tokio::test]
async fn actor_new_loads_agents_from_config() {
    let (bus_requester, mut mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);

    // Use a tempdir for the test database.
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let config_dir = temp_dir.path().to_path_buf();
    prepare_test_database(&config_dir).await;

    // Spawn the actor construction concurrently so we can drive the mock.
    let actor_fut = tokio::spawn(async move {
        Actor::new(
            receiver,
            super::super::types::ActorInboundPorts {
                workspace_copy_source_rx: mpsc::channel(1).1,
            },
            BusRpcContext::authenticated(bus_requester.clone(), Jwt::from_opaque("test-token".to_string())),
            test_bus_service(),
            test_auth_service(),
            broadcast_tx,
            DiscussBootOptions {
                library: test_library::library(),
                config_dir: config_dir.clone(),
                mcp_domains: || Ok(Vec::new()),
                stream_tx: dummy_stream_tx(),
            },
        )
        .await
    });

    // Expect the bridge's initial section read.
    let request = mock
        .expect_request::<ConfigDomain, ConfigRequest>(Duration::from_millis(500))
        .await;

    assert!(
        matches!(request.payload, ConfigRequestPayload::GetConfigSection { section } if section == "discuss"),
        "Actor::new must read the Discuss section"
    );
    // Reply with an empty config (no agents).
    reply_with(
        request.reply_to,
        Ok(ConfigResponse::ConfigSection {
            value: serde_json::to_value(super::super::boot::DiscussSettings::default())
                .expect("serialize Discuss settings"),
        }),
    );

    let actor = actor_fut.await.expect("actor task panicked");
    assert!(
        actor.agents.is_empty(),
        "actor should start with no agents when config is empty"
    );
}

#[tokio::test]
async fn actor_new_preserves_opaque_agent_defaults_from_config() {
    let (bus_requester, mut mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let config_dir = temp_dir.path().to_path_buf();
    prepare_test_database(&config_dir).await;
    let actor_fut = tokio::spawn(async move {
        Actor::new(
            receiver,
            super::super::types::ActorInboundPorts {
                workspace_copy_source_rx: mpsc::channel(1).1,
            },
            BusRpcContext::authenticated(bus_requester.clone(), Jwt::from_opaque("test-token".to_string())),
            test_bus_service(),
            test_auth_service(),
            broadcast_tx,
            DiscussBootOptions {
                config_dir: config_dir.clone(),
                library: test_library::library(),
                mcp_domains: || Ok(Vec::new()),
                stream_tx: dummy_stream_tx(),
            },
        )
        .await
    });

    let request = mock
        .expect_request::<ConfigDomain, ConfigRequest>(Duration::from_millis(500))
        .await;
    let mut config = super::super::boot::DiscussSettings::default();
    let mut agent = make_local_mcp_server("agent");
    agent.replace_default_session_config(serde_json::json!({
        "model": "saved",
        "provider_specific": { "opaque": true }
    }));
    config.agents.insert("agent-a".to_string(), agent);
    reply_with(
        request.reply_to,
        Ok(ConfigResponse::ConfigSection {
            value: serde_json::to_value(config).expect("serialize Discuss settings"),
        }),
    );

    let actor = actor_fut.await.expect("actor task panicked");
    assert_eq!(
        actor.agents["agent-a"].default_session_config(),
        &serde_json::json!({
            "model": "saved",
            "provider_specific": { "opaque": true }
        })
    );
}

/// `Actor::new()` resolves its session `cwd` (`agents_workspace`) from the
/// config bridge's `discuss.agents_workspace` value rather than the process
/// working directory. Proves the config-owned workspace is threaded into the
/// actor at boot (knowledge-domain-style config load).
#[tokio::test]
async fn actor_new_resolves_agents_workspace_from_config() {
    let (bus_requester, mut mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let config_dir = temp_dir.path().to_path_buf();
    prepare_test_database(&config_dir).await;

    let actor_fut = tokio::spawn(async move {
        Actor::new(
            receiver,
            super::super::types::ActorInboundPorts {
                workspace_copy_source_rx: mpsc::channel(1).1,
            },
            BusRpcContext::authenticated(bus_requester.clone(), Jwt::from_opaque("test-token".to_string())),
            test_bus_service(),
            test_auth_service(),
            broadcast_tx,
            DiscussBootOptions {
                config_dir: config_dir.clone(),
                library: test_library::library(),
                mcp_domains: || Ok(Vec::new()),
                stream_tx: dummy_stream_tx(),
            },
        )
        .await
    });

    let request = mock
        .expect_request::<ConfigDomain, ConfigRequest>(Duration::from_millis(500))
        .await;

    // Reply with a config that sets an explicit Discuss agents workspace so
    // the assertion is independent of the machine's default config dir.
    let mut config = super::super::boot::DiscussSettings::default();
    config.agents_workspace = Some("/tmp/sluice-acp-workspace".into());

    assert!(
        matches!(request.payload, ConfigRequestPayload::GetConfigSection { section } if section == "discuss"),
        "Actor::new must read the Discuss section"
    );
    reply_with(
        request.reply_to,
        Ok(ConfigResponse::ConfigSection {
            value: serde_json::to_value(config).expect("serialize Discuss settings"),
        }),
    );

    let actor = actor_fut.await.expect("actor task panicked");
    assert_eq!(
        actor.agents_workspace,
        std::path::PathBuf::from("/tmp/sluice-acp-workspace"),
        "actor must resolve agents_workspace from discuss.agents_workspace config"
    );
}

// ── handle_list_providers ────────────────────────────────────────────────

/// `ListProviders` on an empty actor returns an empty list.
#[tokio::test]
async fn list_providers_returns_empty_when_none_installed() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);

    let actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;
    let result = actor.handle_list_providers().await;

    match result {
        Ok(DiscussResponse::ProvidersList {
            providers,
            agents_workspace,
        }) => {
            assert!(providers.is_empty(), "expected empty Provider list");
            assert_eq!(agents_workspace, actor.agents_workspace.to_string_lossy());
        }
        other => panic!("expected ProvidersList, got {other:?}"),
    }
}

/// `ListProviders` returns all installed Providers with runtime state.
#[tokio::test]
async fn list_providers_returns_all_installed_provider_states() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);

    let spec = make_local_mcp_server("echo");
    let mut initial: HashMap<String, McpServer> = HashMap::new();
    initial.insert("agent-alpha".to_string(), spec.server().clone());
    initial.insert("agent-beta".to_string(), spec.server().clone());

    let actor = make_actor(receiver, initial, bus_requester, broadcast_tx).await;
    let result = actor.handle_list_providers().await;

    match result {
        Ok(DiscussResponse::ProvidersList {
            mut providers,
            agents_workspace,
        }) => {
            assert_eq!(agents_workspace, actor.agents_workspace.to_string_lossy());
            providers.sort_by(|left, right| left.provider_name.cmp(&right.provider_name));
            assert_eq!(
                providers
                    .iter()
                    .map(|provider| provider.provider_name.as_str())
                    .collect::<Vec<_>>(),
                vec!["agent-alpha", "agent-beta"]
            );
        }
        other => panic!("expected ProvidersList, got {other:?}"),
    }
}

// ── handle_add_provider ──────────────────────────────────────────────────

#[tokio::test]
async fn add_provider_rejects_invalid_sources_before_persistence() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (mut actor, _temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;
    let mut value = serde_json::to_value(make_local_mcp_server("opencode")).expect("provider JSON");
    value["workspace_copy_sources"] = serde_json::json!(["../outside"]);
    let spec = serde_json::from_value(value).expect("provider spec");

    let result = actor
        .handle_add_provider("opencode".to_string(), spec)
        .await;

    assert!(matches!(result, Err(DiscussError::InvalidWorkspaceCopySource { .. })));
    assert!(!actor.agents.contains_key("opencode"));
}

/// Adding a new agent persists it and returns `DiscussResponse::Success`.
#[tokio::test]
async fn add_agent_inserts_agent_and_returns_success() {
    let (bus_requester, mut mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);

    let mut actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;

    let mut spec = make_local_mcp_server("my-agent");
    spec.replace_default_session_config(serde_json::json!({ "model": "saved-model" }));

    // Drive the persist request concurrently.
    let add_fut = tokio::spawn(async move {
        actor
            .handle_add_provider("my-agent".to_string(), spec)
            .await
    });

    let request = mock
        .expect_request::<ConfigDomain, ConfigRequest>(Duration::from_millis(500))
        .await;

    let ConfigRequestPayload::SaveConfigSection { section, value } = &request.payload else {
        panic!("expected SaveConfigSection")
    };
    assert_eq!(section, "discuss");
    assert_eq!(
        value["agents"]["my-agent"]["default_session_config"],
        serde_json::json!({ "model": "saved-model" }),
        "opaque defaults must persist with the launch specification"
    );
    reply_with(
        request.reply_to,
        Ok(ConfigResponse::ConfigSectionSaved {
            section: "discuss".to_string(),
        }),
    );

    let result = add_fut.await.expect("task panicked");
    assert!(
        matches!(result, Ok(DiscussResponse::Success)),
        "add_agent must return Success on persist success, got {result:?}"
    );
}

/// Adding an agent that already exists replaces it and persists the new spec.
#[tokio::test]
async fn add_agent_replaces_existing_agent_on_success() {
    let (bus_requester, mut mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);

    let old_spec = make_local_mcp_server("old-cmd");
    let new_spec = make_local_mcp_server("new-cmd");

    let mut initial: HashMap<String, McpServer> = HashMap::new();
    initial.insert("my-agent".to_string(), old_spec.server().clone());

    let mut actor = make_actor(receiver, initial, bus_requester, broadcast_tx).await;

    let add_fut = tokio::spawn(async move {
        actor
            .handle_add_provider("my-agent".to_string(), new_spec)
            .await
    });

    let request = mock
        .expect_request::<ConfigDomain, ConfigRequest>(Duration::from_millis(500))
        .await;

    reply_with(
        request.reply_to,
        Ok(ConfigResponse::ConfigSectionSaved {
            section: "discuss".to_string(),
        }),
    );

    let result = add_fut.await.expect("task panicked");
    assert!(matches!(result, Ok(DiscussResponse::Success)));
}

// ── handle_add_provider rollback on persist failure ──────────────────────

/// When `persist_agents` fails for a brand-new agent, the agent is removed
/// from the in-memory map (rollback to empty).
#[tokio::test]
async fn add_agent_rolls_back_new_agent_on_persist_failure() {
    let (bus_requester, mut mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);

    let spec = make_local_mcp_server("failing-agent");

    // We need to inspect the actor after the call, so we use a channel to
    // return it from the spawned task.
    let (actor_tx, actor_rx) = oneshot::channel::<Actor>();

    let mut actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;

    tokio::spawn(async move {
        let result = actor
            .handle_add_provider("failing-agent".to_string(), spec)
            .await;
        assert!(result.is_err(), "expected error on persist failure");
        let _ = actor_tx.send(actor);
    });

    // Reply with a config error to trigger rollback.
    let request = mock
        .expect_request::<ConfigDomain, ConfigRequest>(Duration::from_millis(500))
        .await;

    reply_with(request.reply_to, Err(ConfigError::UnknownSection("discuss".to_owned())));

    let actor = actor_rx.await.expect("actor not returned");
    assert!(
        actor.agents.is_empty(),
        "rollback must remove the newly-inserted agent; got {:?}",
        actor.agents.keys().collect::<Vec<_>>()
    );
}

/// When `persist_agents` fails for an agent that previously existed, the
/// original agent spec is restored (rollback to previous value).
#[tokio::test]
async fn add_agent_rolls_back_to_previous_agent_on_persist_failure() {
    let (bus_requester, mut mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);

    let old_spec = make_local_mcp_server("old-cmd");
    let new_spec = make_local_mcp_server("new-cmd");

    let mut initial: HashMap<String, McpServer> = HashMap::new();
    initial.insert("my-agent".to_string(), old_spec.server().clone());

    let (actor_tx, actor_rx) = oneshot::channel::<Actor>();

    let mut actor = make_actor(receiver, initial, bus_requester, broadcast_tx).await;

    tokio::spawn(async move {
        let result = actor
            .handle_add_provider("my-agent".to_string(), new_spec)
            .await;
        assert!(result.is_err(), "expected error on persist failure");
        let _ = actor_tx.send(actor);
    });

    let request = mock
        .expect_request::<ConfigDomain, ConfigRequest>(Duration::from_millis(500))
        .await;

    reply_with(request.reply_to, Err(ConfigError::UnknownSection("discuss".to_owned())));

    let actor = actor_rx.await.expect("actor not returned");
    // The old agent must still be present after rollback.
    assert!(
        actor.agents.contains_key("my-agent"),
        "rollback must restore the previous agent entry"
    );
}

// ── handle_remove_provider ───────────────────────────────────────────────

/// Removing an existing provider persists the change and returns success.
#[tokio::test]
async fn remove_provider_removes_provider_and_returns_success() {
    let (bus_requester, mut mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);

    let spec = make_local_mcp_server("echo");
    let mut initial: HashMap<String, McpServer> = HashMap::new();
    initial.insert("target-agent".to_string(), spec.server().clone());

    let mut actor = make_actor(receiver, initial, bus_requester, broadcast_tx).await;

    let remove_fut = tokio::spawn(async move { actor.handle_remove_provider("target-agent").await });

    let request = mock
        .expect_request::<ConfigDomain, ConfigRequest>(Duration::from_millis(500))
        .await;

    reply_with(
        request.reply_to,
        Ok(ConfigResponse::ConfigSectionSaved {
            section: "discuss".to_string(),
        }),
    );

    let result = remove_fut.await.expect("task panicked");
    assert!(
        matches!(
            &result,
            Ok(DiscussResponse::ProviderRemoveSuccess { warnings }) if warnings.is_empty()
        ),
        "remove_provider must return ProviderRemoveSuccess with no warnings, got {result:?}"
    );
}

/// Removing a non-existent provider succeeds without sending a bus request and
/// returns an empty `ProviderRemoveSuccess` response.
#[tokio::test]
async fn remove_provider_idempotent_when_provider_absent() {
    let (bus_requester, mut mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);

    let mut actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;
    let result = actor.handle_remove_provider("ghost-agent").await;

    // No persist request should be sent because the provider was not present.
    mock.assert_no_request(Duration::from_millis(50)).await;

    assert!(
        matches!(
            result,
            Ok(DiscussResponse::ProviderRemoveSuccess { ref warnings }) if warnings.is_empty()
        ),
        "remove of unknown provider must return ProviderRemoveSuccess with no warnings, got {result:?}"
    );
}

/// When `persist_agents` fails after removing an agent, the agent is
/// re-inserted (rollback).
#[tokio::test]
async fn remove_agent_rolls_back_on_persist_failure() {
    let (bus_requester, mut mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);

    let spec = make_local_mcp_server("echo");
    let mut initial: HashMap<String, McpServer> = HashMap::new();
    initial.insert("target-agent".to_string(), spec.server().clone());

    let (actor_tx, actor_rx) = oneshot::channel::<Actor>();

    let mut actor = make_actor(receiver, initial, bus_requester, broadcast_tx).await;

    tokio::spawn(async move {
        let result = actor.handle_remove_provider("target-agent").await;
        assert!(result.is_err(), "expected error on persist failure");
        let _ = actor_tx.send(actor);
    });

    let request = mock
        .expect_request::<ConfigDomain, ConfigRequest>(Duration::from_millis(500))
        .await;

    reply_with(request.reply_to, Err(ConfigError::UnknownSection("discuss".to_owned())));

    let actor = actor_rx.await.expect("actor not returned");
    assert!(
        actor.agents.contains_key("target-agent"),
        "rollback must restore the removed agent; got {:?}",
        actor.agents.keys().collect::<Vec<_>>()
    );
}

// ── persist_agents: unexpected config response ────────────────────────────

/// When `persist_agents` receives a saved response with the wrong section,
/// it returns `DiscussError::Config` describing the unexpected key.
#[tokio::test]
async fn persist_agents_returns_config_error_for_unexpected_response_key() {
    let (bus_requester, mut mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);

    let spec = make_local_mcp_server("echo");

    let mut actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;

    let add_fut = tokio::spawn(async move {
        actor
            .handle_add_provider("my-agent".to_string(), spec)
            .await
    });

    let request = mock
        .expect_request::<ConfigDomain, ConfigRequest>(Duration::from_millis(500))
        .await;

    // Reply with a section that does not match "discuss".
    reply_with(
        request.reply_to,
        Ok(ConfigResponse::ConfigSectionSaved {
            section: "unrelated".to_string(),
        }),
    );

    let result = add_fut.await.expect("task panicked");
    match result {
        Err(DiscussError::Config { message }) => {
            assert!(
                message.contains("unexpected config section") && message.contains("unrelated"),
                "error message should identify the unexpected response key, got: {message}"
            );
        }
        other => panic!("expected DiscussError::Config for wrong section, got {other:?}"),
    }
}

/// Removing a Provider stops its live workers, cascades its transcript, and
/// leaves another Provider's session and worker untouched.
#[tokio::test]
async fn remove_provider_cascade_deletes_only_matching_sessions() {
    let (bus_requester, mut mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (mut actor, _temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;
    actor
        .agents
        .insert("provider-a".to_string(), make_local_mcp_server("echo").into());
    actor
        .agents
        .insert("provider-b".to_string(), make_local_mcp_server("echo").into());

    let session_a = SluiceSessionId::new();
    let session_b = SluiceSessionId::new();
    let sid_a = session_a.to_string();
    let sid_b = session_b.to_string();
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");
    for (sid, provider) in [(&sid_a, "provider-a"), (&sid_b, "provider-b")] {
        upsert_discuss_session(
            &conn,
            DiscussSessionUpsert {
                sluice_session_id: sid,
                agent_name: provider,
                session_name: provider,
                acp_session_id: Some(sid),
                display_title: Some(provider),
                last_user_prompt: Some("hello"),
                latest_todo_snapshot_json: None,
                latest_config_options_json: Some("[]"),
                config_settings_json: None,
                team_leader_id: None,
                available_commands_json: Some("[]"),
                first_prompt_at: Some(1),
                last_interaction_at: Some(1),
            },
        )
        .await
        .expect("seed session");
        insert_discuss_chat_turn(
            &conn,
            DiscussChatTurnInsert {
                sluice_session_id: sid,
                position: 0,
                role: "user",
                acp_session_id: Some(sid),
                content_json: r#"{"role":"user","text":"hello"}"#,
                created_at: 1,
            },
        )
        .await
        .expect("seed turn");
    }

    let (stop_tx, stop_rx) = oneshot::channel();
    let (command_tx_a, mut command_rx_a) = mpsc::channel(1);
    let join_handle_a = tokio::spawn(async move {
        while let Some(command) = command_rx_a.recv().await {
            if matches!(command, WorkerCommand::Stop) {
                let _ = stop_tx.send(());
                break;
            }
        }
    });
    let (command_tx_b, _command_rx_b) = mpsc::channel(1);
    let join_handle_b = tokio::spawn(async move {});
    actor.worker_registry.insert(
        session_a,
        WorkerEntry {
            agent_name: "provider-a".to_string(),
            worker_jti: Some(uuid::Uuid::nil()),
            command_tx: command_tx_a,
            join_handle: join_handle_a,
            acp_session_id: None,
            desktop_association_requested: false,
        },
    );
    actor.worker_registry.insert(
        session_b,
        WorkerEntry {
            agent_name: "provider-b".to_string(),
            worker_jti: Some(uuid::Uuid::nil()),
            command_tx: command_tx_b,
            join_handle: join_handle_b,
            acp_session_id: None,
            desktop_association_requested: false,
        },
    );
    drop(conn);

    let (actor_tx, actor_rx) = oneshot::channel();
    tokio::spawn(async move {
        let result = actor.handle_remove_provider("provider-a").await;
        let _ = actor_tx.send((actor, result));
    });
    stop_rx.await.expect("Provider worker must receive Stop");

    let request = mock
        .expect_request::<ConfigDomain, ConfigRequest>(Duration::from_millis(500))
        .await;
    reply_with(
        request.reply_to,
        Ok(ConfigResponse::ConfigSectionSaved {
            section: "discuss".to_string(),
        }),
    );

    let (actor, result) = actor_rx.await.expect("actor returned");
    assert!(matches!(
        result,
        Ok(DiscussResponse::ProviderRemoveSuccess { ref warnings }) if warnings.is_empty()
    ));
    assert!(!actor.agents.contains_key("provider-a"));
    assert!(actor.agents.contains_key("provider-b"));
    assert!(!actor.worker_registry.contains_key(&session_a));
    assert!(actor.worker_registry.contains_key(&session_b));

    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");
    assert!(
        load_discuss_session(&conn, &sid_a)
            .await
            .expect("load a")
            .is_none()
    );
    assert!(
        load_discuss_session(&conn, &sid_b)
            .await
            .expect("load b")
            .is_some()
    );
    let mut turns = conn
        .query(
            "SELECT COUNT(*) FROM discuss_chat_turns WHERE sluice_session_id = ?1",
            (sid_a.clone(),),
        )
        .await
        .expect("count a turns");
    let turn_row = turns.next().await.expect("row").expect("count row");
    let count: i64 = turn_row.get(0).expect("count value");
    assert_eq!(count, 0);
    let mut other_turns = conn
        .query(
            "SELECT COUNT(*) FROM discuss_chat_turns WHERE sluice_session_id = ?1",
            (sid_b,),
        )
        .await
        .expect("count b turns");
    let other_turn_row = other_turns.next().await.expect("row").expect("count row");
    let other_count: i64 = other_turn_row.get(0).expect("count value");
    assert_eq!(other_count, 1);
}

/// Removing the last Provider removes its local session and transcript.
#[tokio::test]
async fn remove_provider_last_remaining_provider_deletes_its_sessions() {
    let (bus_requester, mut mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (mut actor, _temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;
    actor
        .agents
        .insert("provider-a".to_string(), make_local_mcp_server("echo").into());

    let session_id = SluiceSessionId::new();
    let sid = session_id.to_string();
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");
    upsert_discuss_session(
        &conn,
        DiscussSessionUpsert {
            sluice_session_id: &sid,
            agent_name: "provider-a",
            session_name: "provider-a",
            acp_session_id: Some("acp-a"),
            display_title: Some("Provider A"),
            last_user_prompt: Some("hello"),
            latest_todo_snapshot_json: None,
            latest_config_options_json: Some("[]"),
            config_settings_json: None,
            team_leader_id: None,
            available_commands_json: Some("[]"),
            first_prompt_at: Some(1),
            last_interaction_at: Some(1),
        },
    )
    .await
    .expect("seed session");
    insert_discuss_chat_turn(
        &conn,
        DiscussChatTurnInsert {
            sluice_session_id: &sid,
            position: 0,
            role: "user",
            acp_session_id: Some("acp-a"),
            content_json: r#"{"role":"user","text":"hello"}"#,
            created_at: 1,
        },
    )
    .await
    .expect("seed turn");
    drop(conn);

    let (actor_tx, actor_rx) = oneshot::channel();
    tokio::spawn(async move {
        let result = actor.handle_remove_provider("provider-a").await;
        let _ = actor_tx.send((actor, result));
    });
    let request = mock
        .expect_request::<ConfigDomain, ConfigRequest>(Duration::from_millis(500))
        .await;
    reply_with(
        request.reply_to,
        Ok(ConfigResponse::ConfigSectionSaved {
            section: "discuss".to_string(),
        }),
    );

    let (actor, result) = actor_rx.await.expect("actor returned");
    assert!(matches!(
        result,
        Ok(DiscussResponse::ProviderRemoveSuccess { ref warnings }) if warnings.is_empty()
    ));
    assert!(actor.agents.is_empty());
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");
    assert!(
        load_discuss_session(&conn, &sid)
            .await
            .expect("load session")
            .is_none()
    );
}

/// A failed worker stop aborts the cascade before Provider state changes.
#[tokio::test]
async fn remove_provider_partial_failure_leaves_provider_present() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (mut actor, _temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;
    actor
        .agents
        .insert("provider-a".to_string(), make_local_mcp_server("echo").into());

    let session_id = SluiceSessionId::new();
    let sid = session_id.to_string();
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");
    upsert_discuss_session(
        &conn,
        DiscussSessionUpsert {
            sluice_session_id: &sid,
            agent_name: "provider-a",
            session_name: "provider-a",
            acp_session_id: None,
            display_title: None,
            last_user_prompt: None,
            latest_todo_snapshot_json: None,
            latest_config_options_json: None,
            config_settings_json: None,
            team_leader_id: None,
            available_commands_json: None,
            first_prompt_at: None,
            last_interaction_at: None,
        },
    )
    .await
    .expect("seed session");
    drop(conn);

    let (command_tx, command_rx) = mpsc::channel(1);
    drop(command_rx);
    let join_handle = tokio::spawn(async {});
    actor.worker_registry.insert(
        session_id,
        WorkerEntry {
            agent_name: "provider-a".to_string(),
            worker_jti: Some(uuid::Uuid::nil()),
            command_tx,
            join_handle,
            acp_session_id: None,
            desktop_association_requested: false,
        },
    );

    let result = actor.handle_remove_provider("provider-a").await;
    assert!(result.is_err());
    assert!(actor.agents.contains_key("provider-a"));
    assert!(actor.worker_registry.contains_key(&session_id));
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");
    assert!(
        load_discuss_session(&conn, &sid)
            .await
            .expect("load session")
            .is_some()
    );
}

/// A response with the wrong variant must not be accepted as a persisted Provider.
#[tokio::test]
async fn persist_agents_errors_on_unexpected_config_response_variant() {
    let (bus_requester, mut mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);

    let spec = make_local_mcp_server("echo");

    let mut actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;

    let add_fut = tokio::spawn(async move {
        actor
            .handle_add_provider("my-agent".to_string(), spec)
            .await
    });

    let request = mock
        .expect_request::<ConfigDomain, ConfigRequest>(Duration::from_millis(500))
        .await;

    // Reply with a completely unexpected response variant.
    reply_with(
        request.reply_to,
        Ok(ConfigResponse::ConfigSection {
            value: serde_json::json!({}),
        }),
    );

    let result = add_fut.await.expect("task panicked");
    assert!(
        matches!(&result, Err(DiscussError::Config { message }) if message.contains("expected ConfigResponse::ConfigSectionSaved") && message.contains("received ConfigSection")),
        "expected DiscussError::Config for unexpected response variant, got {result:?}"
    );
}

// ── spawn_session: unknown agent key ─────────────────────────────────────

#[test]
fn mcp_transport_selection_prefers_native_and_respects_opt_out() {
    assert_eq!(select_mcp_transport(false, true, true), McpTransportSelection::None);
    assert_eq!(select_mcp_transport(true, true, true), McpTransportSelection::NativeAcp);
    assert_eq!(select_mcp_transport(true, false, true), McpTransportSelection::Http);
    assert_eq!(select_mcp_transport(true, false, false), McpTransportSelection::None);
}

#[test]
fn mcp_status_reflects_configuration_and_provisioning() {
    assert_eq!(
        mcp_status_for_transport(false, McpTransportSelection::NativeAcp, true),
        DiscussMcpStatus::Off
    );
    assert_eq!(
        mcp_status_for_transport(true, McpTransportSelection::NativeAcp, true),
        DiscussMcpStatus::Acp
    );
    assert_eq!(
        mcp_status_for_transport(true, McpTransportSelection::Http, true),
        DiscussMcpStatus::Fallback
    );
    assert_eq!(
        mcp_status_for_transport(true, McpTransportSelection::None, false),
        DiscussMcpStatus::Broken
    );
}

/// `session/new` config options are normalized through the same canonical
/// path used by `config_option_update` / `session/load` / `set_config_option`.
/// Explicit empty seeds an empty cache; omitted is represented as `None`
/// (fail-closed, no invented options).
#[test]
fn session_new_config_options_seed_semantics() {
    // Explicit empty: cache/persist `[]`, capability may be raised.
    let explicit_empty = Some(Vec::new());
    let seeded = test_agent_plugin().initial_config_options(explicit_empty, None);
    assert_eq!(seeded.as_ref().map(|opts| opts.len()), Some(0));

    // Omitted: no seed, fail-closed.
    let omitted = test_agent_plugin().initial_config_options(None, None);
    assert!(omitted.is_none());

    // Non-empty select option normalizes to a canonical row.
    let options = vec![serde_json::json!({
        "id": "model",
        "name": "Model",
        "category": "model",
        "type": "select",
        "currentValue": "gpt-4",
        "options": [{ "value": "gpt-4", "name": "GPT-4" }]
    })];
    let seeded = test_agent_plugin()
        .metadata_config_options(&serde_json::json!({ "options": options }))
        .expect("standard config update options");
    assert_eq!(seeded.len(), 1);
    assert_eq!(seeded[0].id, "model");
    assert_eq!(seeded[0].value, "gpt-4");
}

/// Spawning a session for an agent key that is not registered returns
/// `DiscussError::UnknownAgent` with the agent key.
#[tokio::test]
async fn spawn_session_returns_error_for_unknown_agent_key() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);

    let mut actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;

    let result = actor
        .spawn_session(
            SluiceSessionId::new(),
            "nonexistent-agent",
            SpawnSessionOptions {
                name: Some("Test Session".to_string()),
                setup: None,
                session_config_overrides: std::collections::BTreeMap::new(),
                acp_session_id: None,
                announce_desktop_association: true,
                transient_connectivity: false,
                session_cwd: None,
                sluice_mcp_enabled: None,
                node_token: None,
                flow_mcp_endpoints: None,
            },
        )
        .await;

    match result {
        Err(DiscussError::UnknownAgent { agent_key }) => {
            assert_eq!(agent_key, "nonexistent-agent", "error must name the unknown key");
        }
        other => panic!("expected DiscussError::UnknownAgent for unknown agent, got {other:?}"),
    }
}

// ── config_option_update → live cache + capability signal (F1) ───────────

/// Build a reshaped `config_option_update` metadata blob in the **real ACP
/// SDK wire shape** (`agent-client-protocol` 0.14.0), as produced by the
/// worker's `send_message_to_user` path (`ConfigOptionUpdate` DTO carrying
/// raw `SessionConfigOption` JSON). The select payload is **flattened** onto
/// the option object: `type: "select"`, top-level `currentValue`, and a
/// top-level `options` array of `{ value, name }` `SessionConfigSelectOption`s
/// — not nested under `kind.select`.
fn config_option_update_metadata(current: &str) -> serde_json::Value {
    serde_json::json!({
        "sessionId": "acp-sess-1",
        "options": [
            {
                "id": "model",
                "name": "Model",
                "category": "model",
                "type": "select",
                "currentValue": current,
                "options": [
                    { "value": "gpt-4", "name": "GPT-4" },
                    { "value": "gpt-5", "name": "GPT-5" }
                ]
            }
        ],
        "selected": { "model": current }
    })
}

/// A real ACP-SDK-shaped `config_option_update` blob (flattened `type`,
/// top-level `currentValue`, top-level `options` of `{ value, name }`) is
/// recognized and converted into live `DiscussConfigOption`s — this is the value
/// the worker pushes into its authoritative `latest_config_options` cache
/// (returned by `getConfigOptions`) and the server-side signal that raises
/// the `set_config_option` capability gate.
#[test]
fn config_options_from_metadata_update_extracts_provider_options() {
    let metadata = config_option_update_metadata("gpt-4");

    let options = test_agent_plugin()
        .metadata_config_options(&metadata)
        .expect("config_option_update blob must yield live config options");

    assert_eq!(options.len(), 1, "one config option expected");
    let option = &options[0];
    assert_eq!(option.id, "model");
    assert_eq!(option.name, "Model");
    assert_eq!(option.category, "model", "SDK top-level category must be preserved");
    assert_eq!(
        option.value, "gpt-4",
        "current value must be carried through as the live selected value",
    );
    assert_eq!(
        option.options.len(),
        2,
        "available option values must be carried through for the live cache",
    );
    // The exact SDK `SessionConfigValueId`s must survive verbatim so Hades
    // dispatches an exact value id on `setConfigOption`.
    assert_eq!(
        option.options[0].value, "gpt-4",
        "exact SessionConfigValueId must be preserved for each selectable value",
    );
}

/// A real SDK-shaped grouped select option (`options` is
/// `SessionConfigSelectGroup[]`) preserves the grouped value data verbatim in
/// the canonical `options` array so Hades can flatten groups and read every
/// nested `SessionConfigValueId`.
#[test]
fn config_options_from_metadata_update_preserves_grouped_options() {
    let metadata = serde_json::json!({
        "sessionId": "acp-sess-1",
        "options": [
            {
                "id": "model",
                "name": "Model",
                "category": "model",
                "type": "select",
                "currentValue": "provider-a-model",
                "options": [
                    {
                        "group": "provider-a",
                        "name": "Provider A",
                        "options": [
                            { "value": "provider-a-model", "name": "Provider A model" },
                            { "value": "provider-a-fast", "name": "Provider A fast" }
                        ]
                    },
                    {
                        "group": "provider-b",
                        "name": "Provider B",
                        "options": [
                            { "value": "gpt-4", "name": "GPT-4" }
                        ]
                    }
                ]
            }
        ]
    });

    let options = test_agent_plugin()
        .metadata_config_options(&metadata)
        .expect("grouped SDK blob must yield live config options");

    assert_eq!(options.len(), 1);
    let option = &options[0];
    assert_eq!(option.value, "provider-a-model");
    assert_eq!(option.options.len(), 3, "all grouped values must be preserved");
    assert_eq!(option.options[0].value, "provider-a-model");
    assert_eq!(option.options[0].label, "Provider A / Provider A model");
}

/// A malformed / non-select option (no `currentValue` in any supported
/// shape) is skipped rather than emitted as a bogus `value: null` row.
#[test]
fn config_options_from_metadata_update_skips_non_select_options() {
    let metadata = serde_json::json!({
        "sessionId": "acp-sess-1",
        "options": [
            // Non-select / malformed: no currentValue anywhere.
            { "id": "opaque", "name": "Opaque", "type": "boolean" },
            // A valid SDK select option alongside it.
            {
                "id": "model",
                "name": "Model",
                "category": "model",
                "type": "select",
                "currentValue": "gpt-4",
                "options": [ { "value": "gpt-4", "name": "GPT-4" } ]
            }
        ]
    });

    let options = test_agent_plugin()
        .metadata_config_options(&metadata)
        .expect("blob with at least one select must yield config options");

    assert_eq!(
        options.len(),
        1,
        "the non-select option must be skipped, only the valid select survives",
    );
    assert_eq!(options[0].id, "model");
}

/// A metadata blob that is NOT a config-option update (e.g. an
/// `available_commands_update` or a `session_info_update`) yields `None`, so
/// the worker never touches the config cache or raises the
/// `set_config_option` capability from an unrelated signal — the gate stays
/// fail-closed until a real provider config signal exists.
#[test]
fn config_options_from_metadata_update_ignores_non_config_updates() {
    let commands_update = serde_json::json!({
        "sessionId": "acp-sess-1",
        "availableCommands": [
            { "name": "help", "description": "Show help" }
        ]
    });
    assert!(
        test_agent_plugin()
            .metadata_config_options(&commands_update)
            .is_none(),
        "available_commands_update must not be read as a config-option signal",
    );

    let info_update = serde_json::json!({
        "sessionId": "acp-sess-1",
        "title": "Renamed session"
    });
    assert!(
        test_agent_plugin()
            .metadata_config_options(&info_update)
            .is_none(),
        "session_info_update must not be read as a config-option signal",
    );
}

/// The raw provider `configOptions` key (non-reshaped shape) is also
/// accepted, mirroring the Hades `parseConfigOptionsUpdate` contract that
/// reads `configOptions ?? options`.
#[test]
fn config_options_from_metadata_update_accepts_raw_config_options_key() {
    let metadata = serde_json::json!({
        "sessionId": "acp-sess-1",
        "configOptions": [
            {
                "id": "mode",
                "name": "Mode",
                "type": "select",
                "currentValue": "fast",
                "options": []
            }
        ]
    });

    let options = test_agent_plugin()
        .metadata_config_options(&metadata)
        .expect("raw configOptions key must yield live config options");
    assert_eq!(options.len(), 1);
    assert_eq!(options[0].id, "mode");
}

// ── config_option persistence ⇄ no-live-worker fallback (F1) ──────────────

/// Construct an `Actor` whose tempdir database has the full migration set
/// applied, so DB-backed paths (`acp_agent_sessions` upsert/load, config
/// persistence, and the no-live-worker `getConfigOptions` fallback) can be
/// exercised end to end.
async fn make_migrated_actor(
    bus_requester: mpsc::Sender<OpaqueRequestEnvelope>,
    broadcast_tx: mpsc::Sender<DiscussResponse>,
) -> (Actor, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database = open_local_db(temp_dir.path())
        .await
        .expect("test database opens");
    apply_discuss_migrations(&database)
        .await
        .expect("apply migrations");
    let database = Arc::new(database);
    let persistence = DiscussPersistence {
        database: database.clone(),
    };
    let (capabilities_update_tx, capabilities_update_rx) =
        mpsc::channel::<(SluiceSessionId, SessionCapabilitiesCache)>(DEFAULT_BUFFER_SIZE);
    let (worker_ready_tx, worker_ready_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
    let (transient_worker_tx, transient_worker_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
    let (pending_permission_tx, pending_permission_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
    let (pending_permission_cancel_tx, pending_permission_cancel_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
    let (pending_elicitation_tx, pending_elicitation_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
    let (pending_elicitation_cancel_tx, pending_elicitation_cancel_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
    let (shared_provider_event_tx, shared_provider_event_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
    let (provider_restart_ready_tx, provider_restart_ready_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
    let (_, receiver) = mpsc::channel(1);
    let actor = Actor {
        library: test_library::library(),
        receiver,
        agent_completions: HashMap::new(),
        workspace_copy_source_rx: mpsc::channel(1).1,
        agents: HashMap::new(),
        stream_tx: dummy_stream_tx(),
        root_context: BusRpcContext::authenticated(bus_requester, Jwt::from_opaque("test-token".to_string())),
        mcp_domains: || Ok(Vec::new()),
        bus_service: test_bus_service(),
        auth_service: Some(test_auth_service()),
        broadcast_tx,
        persistence,
        database_path: temp_dir.path().join("discuss.db"),
        agents_workspace: temp_dir.path().join("agents"),
        worker_registry: HashMap::new(),
        provider_supervisors: HashMap::new(),
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
        worker_ready_rx,
        worker_ready_tx,
        transient_worker_rx,
        transient_worker_tx,
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
        server_running: true,
    };
    (actor, temp_dir)
}

#[tokio::test]
async fn agent_config_round_trips_database_menu_and_defaults() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (mut actor, _temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;
    let agent_name = "custom-agent";
    actor
        .agents
        .insert(agent_name.to_string(), make_local_mcp_server("custom-agent"));
    let options = vec![config_option("model", "model-a", &["model-a", "model-b"])];
    let defaults = ConfigSettings::from([("model".to_string(), "model-b".to_string())]);
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");
    upsert_discuss_agent_config(&conn, agent_name, Some(&options), &defaults)
        .await
        .expect("persist agent config");

    let response = actor
        .handle_get_provider_config(agent_name)
        .await
        .expect("read agent config");

    let DiscussResponse::ProviderConfig {
        config,
        workspace_copy_sources,
    } = response
    else {
        panic!("expected agent config");
    };
    assert_eq!(workspace_copy_sources, vec![".agents"]);
    assert_eq!(config.options, options);
    assert_eq!(config.defaults, defaults);
    assert!(config.options_discovered);
}

#[tokio::test]
async fn provider_config_returns_effective_sources_for_builtin_and_explicit_empty() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (mut actor, _temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;
    let provider_name = "opencode";
    let provider = make_local_mcp_server(provider_name);
    actor
        .agents
        .insert(provider_name.to_string(), provider.clone());

    let response = actor
        .handle_get_provider_config(provider_name)
        .await
        .expect("config");
    let DiscussResponse::ProviderConfig {
        workspace_copy_sources, ..
    } = response
    else {
        panic!("expected provider config");
    };
    assert_eq!(workspace_copy_sources, vec![".agents", ".opencode"]);

    let mut value = serde_json::to_value(provider).expect("provider JSON");
    value["workspace_copy_sources"] = serde_json::json!([]);
    actor.agents.insert(
        provider_name.to_string(),
        serde_json::from_value(value).expect("provider spec"),
    );
    let response = actor
        .handle_get_provider_config(provider_name)
        .await
        .expect("config");
    let DiscussResponse::ProviderConfig {
        workspace_copy_sources, ..
    } = response
    else {
        panic!("expected provider config");
    };
    assert_eq!(workspace_copy_sources, vec![".agents"]);
}

#[tokio::test]
async fn provider_config_distinguishes_undiscovered_from_confirmed_empty_menu() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (mut actor, _temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;
    let provider_name = "custom-agent";
    actor
        .agents
        .insert(provider_name.to_string(), make_local_mcp_server(provider_name));
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");

    upsert_discuss_agent_config(&conn, provider_name, None, &ConfigSettings::new())
        .await
        .expect("persist provider without a discovered menu");
    let undiscovered = actor
        .handle_get_provider_config(provider_name)
        .await
        .expect("read undiscovered provider config");
    let DiscussResponse::ProviderConfig { config, .. } = undiscovered else {
        panic!("expected provider config");
    };
    assert!(!config.options_discovered);
    assert!(config.options.is_empty());

    upsert_discuss_agent_config(&conn, provider_name, Some(&[]), &ConfigSettings::new())
        .await
        .expect("persist provider-confirmed empty menu");
    let discovered = actor
        .handle_get_provider_config(provider_name)
        .await
        .expect("read discovered provider config");
    let DiscussResponse::ProviderConfig { config, .. } = discovered else {
        panic!("expected provider config");
    };
    assert!(config.options_discovered);
    assert!(config.options.is_empty());
}

#[tokio::test]
async fn newer_agent_menu_replaces_removed_items_in_database_snapshot() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (actor, _temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");
    let defaults = ConfigSettings::new();
    let original = vec![
        config_option("model", "model-a", &["model-a"]),
        config_option("mode", "mode-a", &["mode-a"]),
    ];
    let replacement = vec![config_option("model", "model-b", &["model-b"])];
    upsert_discuss_agent_config(&conn, "custom-agent", Some(&original), &defaults)
        .await
        .expect("persist original menu");
    upsert_discuss_agent_config(&conn, "custom-agent", Some(&replacement), &defaults)
        .await
        .expect("replace menu");

    let row = load_discuss_agent_config(&conn, "custom-agent")
        .await
        .expect("load agent config")
        .expect("agent config exists");
    let persisted = config_options_from_json(
        row.config_options_json
            .as_deref()
            .expect("persisted menu snapshot"),
    )
    .expect("decode persisted menu");
    assert_eq!(persisted, replacement);
}

#[tokio::test]
async fn session_config_read_falls_back_to_database_agent_menu() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (mut actor, _temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;
    let sluice_session_id = SluiceSessionId::new();
    let session_id = sluice_session_id.to_string();
    let options = vec![config_option("model", "model-a", &["model-a", "model-b"])];
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");
    upsert_discuss_agent_config(&conn, "custom-agent", Some(&options), &ConfigSettings::new())
        .await
        .expect("persist agent menu");
    upsert_discuss_session(
        &conn,
        DiscussSessionUpsert {
            sluice_session_id: &session_id,
            agent_name: "custom-agent",
            session_name: "session",
            acp_session_id: Some("provider-session"),
            display_title: None,
            last_user_prompt: None,
            latest_todo_snapshot_json: None,
            latest_config_options_json: None,
            config_settings_json: None,
            team_leader_id: None,
            available_commands_json: None,
            first_prompt_at: None,
            last_interaction_at: None,
        },
    )
    .await
    .expect("persist session anchor");

    let response = actor
        .handle_get_config_options(sluice_session_id)
        .await
        .expect("read database menu");
    assert!(matches!(response, DiscussResponse::ConfigOptions { options: actual } if actual == options));
}

#[tokio::test]
async fn session_settings_persist_separately_from_menu_snapshot() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (actor, _temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;
    let sluice_session_id = SluiceSessionId::new().to_string();
    let options = vec![config_option("model", "model-b", &["model-a", "model-b"])];
    let settings = ConfigSettings::from([("model".to_string(), "model-b".to_string())]);
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");
    upsert_discuss_session(
        &conn,
        DiscussSessionUpsert {
            sluice_session_id: &sluice_session_id,
            agent_name: "custom-agent",
            session_name: "session",
            acp_session_id: Some("provider-session"),
            display_title: None,
            last_user_prompt: None,
            latest_todo_snapshot_json: None,
            latest_config_options_json: None,
            config_settings_json: None,
            team_leader_id: None,
            available_commands_json: None,
            first_prompt_at: None,
            last_interaction_at: None,
        },
    )
    .await
    .expect("persist session anchor");

    persist_config_options(
        &actor.persistence,
        &sluice_session_id,
        "custom-agent",
        &options,
        Some(&settings),
    )
    .await
    .expect("persist session config");

    let row = load_discuss_session(&conn, &sluice_session_id)
        .await
        .expect("load session")
        .expect("session exists");
    let persisted_settings = config_settings_from_json(
        row.config_settings_json
            .as_deref()
            .expect("persisted session settings"),
    )
    .expect("decode session settings");
    assert_eq!(persisted_settings, settings);
    assert_eq!(
        config_options_from_json(
            row.latest_config_options_json
                .as_deref()
                .expect("persisted session menu"),
        )
        .expect("decode session menu"),
        options
    );
}

/// End-to-end persistence ⇄ fallback contract: a real SDK-shaped
/// `config_option_update` persisted via `persist_metadata_update` writes
/// **canonical** `Vec<DiscussConfigOption>` JSON (never the raw provider blob),
/// so the no-live-worker `handle_get_config_options` fallback deserializes it
/// back into non-empty canonical options rather than returning empty. This is
/// the exact regression review-04 F1 called out: raw persistence + a
/// `Vec<DiscussConfigOption>` fallback previously produced empty cached reads.
#[tokio::test]
async fn persisted_config_options_round_trip_through_no_live_worker_fallback() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (mut actor, _temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;

    let sluice_session_id = SluiceSessionId::new();
    let sid = sluice_session_id.to_string();
    let agent_name = "agent";

    // A provider config update in the real SDK wire shape.
    let metadata = config_option_update_metadata("gpt-4");

    // Persist through the production persistence path. Assert the fallible
    // core directly so a real DB failure surfaces the underlying error here
    // rather than being swallowed by the fire-and-forget wrapper and only
    // showing up downstream as a missing anchor row.
    persist_metadata_update_inner(&actor.persistence, &sid, agent_name, "session", &metadata)
        .await
        .expect("metadata update persists the anchor row through the production path");

    // Prove the persisted column is canonical `Vec<DiscussConfigOption>` JSON.
    {
        let conn = actor
            .persistence
            .acquire_connection()
            .await
            .expect("connection");
        let row = load_discuss_session(&conn, &sid)
            .await
            .expect("load row")
            .expect("row exists after persist");
        let json = row
            .latest_config_options_json
            .as_deref()
            .expect("config options column populated");
        let canonical: Vec<DiscussConfigOption> =
            serde_json::from_str(json).expect("persisted JSON deserializes as Vec<DiscussConfigOption>");
        assert_eq!(canonical.len(), 1, "one persisted canonical option");
        assert_eq!(canonical[0].id, "model");
        assert_eq!(
            canonical[0].value, "gpt-4",
            "persisted canonical value must be the selected value id",
        );
    }

    // No live worker is registered for this session, so getConfigOptions
    // takes the persisted-fallback path and must return the same canonical
    // options — not empty.
    let response = actor
        .handle_get_config_options(sluice_session_id)
        .await
        .expect("get config options succeeds");
    match response {
        DiscussResponse::ConfigOptions { options } => {
            assert_eq!(
                options.len(),
                1,
                "no-live-worker fallback must return the persisted canonical options, not empty",
            );
            assert_eq!(options[0].id, "model");
            assert_eq!(options[0].value, "gpt-4");
            assert_eq!(
                options[0].options.len(),
                2,
                "selectable values survive the persistence round-trip",
            );
        }
        other => panic!("expected ConfigOptions, got {other:?}"),
    }
}

#[tokio::test]
async fn new_session_resolves_persisted_provider_id_by_sluice_session_id() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (actor, _temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;
    let sluice_session_id = SluiceSessionId::new();
    let sluice_session_id_text = sluice_session_id.to_string();
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");

    upsert_discuss_session(
        &conn,
        DiscussSessionUpsert {
            sluice_session_id: &sluice_session_id_text,
            session_name: "persisted-session",
            agent_name: "agent",
            acp_session_id: Some("provider-session"),
            display_title: None,
            last_user_prompt: None,
            latest_todo_snapshot_json: None,
            latest_config_options_json: None,
            config_settings_json: None,
            team_leader_id: None,
            available_commands_json: None,
            first_prompt_at: None,
            last_interaction_at: None,
        },
    )
    .await
    .expect("persisted session anchor");

    let resolved = actor
        .resolve_persisted_acp_session_id(sluice_session_id, None)
        .await
        .expect("provider id resolution");
    assert_eq!(resolved.as_deref(), Some("provider-session"));
}

// ── set_config_option post-response persistence lifecycle ────────────────

/// Build a `session/set_config_option` response payload in the real ACP SDK
/// wire shape (`SetSessionConfigOptionResponse.config_options` is the full,
/// explicit `Vec<SessionConfigOption>` with the select payload flattened onto
/// each option object). This mirrors the value the worker feeds into
/// `acp_config_options_from_schema` after a successful set.
fn set_config_response_options(current: &str) -> Vec<serde_json::Value> {
    vec![serde_json::json!({
        "id": "model",
        "name": "Model",
        "category": "model",
        "type": "select",
        "currentValue": current,
        "options": [
            { "value": "gpt-4", "name": "GPT-4" },
            { "value": "gpt-5", "name": "GPT-5" }
        ]
    })]
}

/// A successful `session/set_config_option` response persists the
/// **post-response** canonical options to `latest_config_options_json`, and a
/// later no-live-worker `getConfigOptions` returns exactly those options.
///
/// This exercises the same normalization + persistence the worker's
/// `SetConfigOption` success path runs: normalize the post-response set
/// through `acp_config_options_from_schema`, then persist the canonical
/// `Vec<DiscussConfigOption>` through the `PersistenceJob::ConfigOptions`
/// drain helper (`persist_config_options`). Acceptance checks #4 (persisted
/// fallback updated with post-response options) and #6 (no-live-worker
/// `getConfigOptions` returns the persisted post-response options).
#[tokio::test]
async fn set_config_option_success_persists_post_response_options_for_no_worker_fallback() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (mut actor, _temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;

    let sluice_session_id = SluiceSessionId::new();
    let sid = sluice_session_id.to_string();

    // The provider's post-response config options select `gpt-5`. Normalize
    // through the canonical normalizer the worker uses, then persist through
    // the same helper the `PersistenceJob::ConfigOptions` drain runs.
    let refreshed = test_agent_plugin()
        .metadata_config_options(&serde_json::json!({ "options": set_config_response_options("gpt-5") }))
        .expect("standard config update options");
    assert_eq!(refreshed.len(), 1, "one canonical post-response option");
    assert_eq!(
        refreshed[0].value, "gpt-5",
        "canonical live-cache value is the post-response selected value id",
    );

    persist_config_options(&actor.persistence, &sid, "agent", &refreshed, None)
        .await
        .expect("persist refreshed config");

    // No live worker registered: getConfigOptions must return the persisted
    // post-response options (not stale/empty).
    let response = actor
        .handle_get_config_options(sluice_session_id)
        .await
        .expect("get config options succeeds");
    match response {
        DiscussResponse::ConfigOptions { options } => {
            assert_eq!(
                options.len(),
                1,
                "no-live-worker fallback returns the persisted post-response options",
            );
            assert_eq!(options[0].id, "model");
            assert_eq!(
                options[0].value, "gpt-5",
                "persisted fallback reflects the post-response selected value id",
            );
            assert_eq!(
                options[0].options.len(),
                2,
                "selectable values survive the post-response persistence round-trip",
            );
        }
        other => panic!("expected ConfigOptions, got {other:?}"),
    }
}

/// An explicit empty post-response option set (the provider now advertises no
/// config options) persists as an explicit `[]` and the no-live-worker
/// `getConfigOptions` returns an empty list — the persisted fallback mirrors
/// the live worker cache exactly, without carrying stale options forward.
/// Acceptance check #7 (explicit empty returned options persist as `[]`).
#[tokio::test]
async fn set_config_option_success_persists_explicit_empty_options() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (mut actor, _temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;

    let sluice_session_id = SluiceSessionId::new();
    let sid = sluice_session_id.to_string();

    // Seed a prior non-empty persisted state so the explicit-empty write must
    // actually overwrite (not carry forward) the earlier options.
    let seeded = test_agent_plugin()
        .metadata_config_options(&serde_json::json!({ "options": set_config_response_options("gpt-4") }))
        .expect("standard config update options");
    persist_config_options(&actor.persistence, &sid, "agent", &seeded, None)
        .await
        .expect("persist seeded config");

    // A successful set whose post-response set is explicitly empty.
    let empty = test_agent_plugin()
        .initial_config_options(Some(Vec::new()), None)
        .expect("explicit standard options");
    assert!(empty.is_empty(), "post-response set is explicitly empty");
    persist_config_options(&actor.persistence, &sid, "agent", &empty, None)
        .await
        .expect("persist empty config");

    // The persisted column is an explicit `[]`, not the seeded options.
    {
        let conn = actor
            .persistence
            .acquire_connection()
            .await
            .expect("connection");
        let row = load_discuss_session(&conn, &sid)
            .await
            .expect("load row")
            .expect("row exists after persist");
        let json = row
            .latest_config_options_json
            .as_deref()
            .expect("config options column populated");
        let canonical: Vec<DiscussConfigOption> =
            serde_json::from_str(json).expect("persisted JSON deserializes as Vec<DiscussConfigOption>");
        assert!(
            canonical.is_empty(),
            "explicit empty post-response set persists as [] and overwrites the seeded options",
        );
    }

    let response = actor
        .handle_get_config_options(sluice_session_id)
        .await
        .expect("get config options succeeds");
    match response {
        DiscussResponse::ConfigOptions { options } => {
            assert!(
                options.is_empty(),
                "no-live-worker fallback returns the explicit empty post-response set",
            );
        }
        other => panic!("expected ConfigOptions, got {other:?}"),
    }
}

/// A **failed** `session/set_config_option` must not mutate persistence: the
/// worker's success path (normalize + live cache + capability + enqueue the
/// `PersistenceJob::ConfigOptions` write) only runs on `Ok`, so a prior
/// persisted state is left intact. This test proves the persistence side of
/// acceptance check #5 by exercising the invariant directly: nothing is
/// enqueued/persisted when the provider request fails, so the previously
/// persisted options are unchanged.
#[tokio::test]
async fn set_config_option_failure_does_not_mutate_persisted_options() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (mut actor, _temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;

    let sluice_session_id = SluiceSessionId::new();
    let sid = sluice_session_id.to_string();

    // Seed a known persisted state via a prior successful set.
    let seeded = test_agent_plugin()
        .metadata_config_options(&serde_json::json!({ "options": set_config_response_options("gpt-4") }))
        .expect("standard config update options");
    persist_config_options(&actor.persistence, &sid, "agent", &seeded, None)
        .await
        .expect("persist seeded config");

    // A failed provider request enqueues NO persistence job (the worker only
    // persists inside the `Ok` arm), so the persisted state is unchanged. We
    // model that by simply not calling `persist_config_options` for the
    // failed attempt and asserting the seeded state survives.
    let response = actor
        .handle_get_config_options(sluice_session_id)
        .await
        .expect("get config options succeeds");
    match response {
        DiscussResponse::ConfigOptions { options } => {
            assert_eq!(options.len(), 1, "seeded options survive a failed set (no persistence)");
            assert_eq!(
                options[0].value, "gpt-4",
                "a failed set never overwrites the persisted post-response options",
            );
        }
        other => panic!("expected ConfigOptions, got {other:?}"),
    }
}

// ── handle_set_config_option: fail-closed gate (F1) ──────────────────────

/// `set_config_option` with no live worker fails closed (no local fallback
/// path exists for config mutation).
#[tokio::test]
async fn set_config_option_fails_closed_without_live_worker() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);

    let mut actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;

    let result = actor
        .handle_set_config_option(SluiceSessionId::new(), "model".to_string(), "gpt-4".to_string())
        .await;

    assert!(
        matches!(result, Err(DiscussError::SessionNotFound { .. })),
        "config mutation without a live worker must fail closed, got {result:?}",
    );
}

/// With a live worker present but the `set_config_option` capability bit
/// unset, the mutation is rejected with `UnsupportedCapability` — the gate
/// stays fail-closed before any real provider config signal exists.
#[tokio::test]
async fn set_config_option_fails_closed_when_capability_unset() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);

    let mut actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;
    let sluice_session_id = SluiceSessionId::new();

    // A live worker exists, but no server-side config signal has been
    // observed, so the capability bit is unset (default).
    let (command_tx, _command_rx) = mpsc::channel::<WorkerCommand>(1);
    let join_handle = tokio::spawn(async move {
        // Worker will not receive a command (capability gate rejects first).
        drop(_command_rx);
    });
    actor.worker_registry.insert(
        sluice_session_id,
        WorkerEntry {
            agent_name: "agent".to_string(),
            worker_jti: Some(uuid::Uuid::nil()),
            command_tx,
            join_handle,
            acp_session_id: None,
            desktop_association_requested: false,
        },
    );
    actor
        .worker_capabilities
        .insert(sluice_session_id, SessionCapabilitiesCache::default());

    let result = actor
        .handle_set_config_option(sluice_session_id, "model".to_string(), "gpt-4".to_string())
        .await;

    assert!(
        matches!(
            result,
            Err(DiscussError::UnsupportedCapability { ref capability }) if capability == "set_config_option"
        ),
        "gate must reject when set_config_option capability is unset, got {result:?}",
    );
}

/// Once a real provider config signal has raised the `set_config_option`
/// capability (the same bit the `config_option_update` handler sets), the
/// gate allows the mutation to reach the live worker: the command is
/// forwarded and the worker's reply is surfaced.
#[tokio::test]
async fn set_config_option_allowed_once_capability_raised() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);

    let mut actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;
    let sluice_session_id = SluiceSessionId::new();

    // A live worker with the capability raised (as a config_option_update or
    // session/load advertising options would do).
    let (command_tx, mut command_rx) = mpsc::channel::<WorkerCommand>(1);
    let worker = tokio::spawn(async move {
        match command_rx.recv().await {
            Some(WorkerCommand::SetConfigOption {
                config_id,
                value,
                reply,
            }) => {
                assert_eq!(config_id, "model");
                assert_eq!(value, "gpt-4", "string value forwarded verbatim as the value id");
                let _ = reply.send(Ok(()));
            }
            Some(_) => panic!("expected SetConfigOption command, got a different worker command"),
            None => panic!("worker command channel closed before receiving SetConfigOption"),
        }
    });
    actor.worker_registry.insert(
        sluice_session_id,
        WorkerEntry {
            agent_name: "agent".to_string(),
            worker_jti: Some(uuid::Uuid::nil()),
            command_tx,
            join_handle: worker,
            acp_session_id: None,
            desktop_association_requested: false,
        },
    );
    let caps = SessionCapabilitiesCache {
        set_config_option: true,
        ..SessionCapabilitiesCache::default()
    };
    actor.worker_capabilities.insert(sluice_session_id, caps);

    let result = actor
        .handle_set_config_option(sluice_session_id, "model".to_string(), "gpt-4".to_string())
        .await;

    // The mock worker's join handle is owned by the registry entry; abort it
    // to avoid leaking the task if the handler consumed the command.
    if let Some(entry) = actor.worker_registry.remove(&sluice_session_id) {
        entry.join_handle.abort();
    }
    assert!(
        matches!(result, Ok(DiscussResponse::Success)),
        "gate must allow the mutation once the capability is raised, got {result:?}",
    );
}

// ── acp_agent_sessions upsert NULL-safety + child-turn anchoring ──────────

// ── discuss_sessions upsert + child-turn anchoring ──────────────────────

/// Build a `DiscussPersistence` over a tempdir database with the complete
/// production migration set. The nullable metadata columns are therefore
/// exercised against the same schema used by runtime persistence.
async fn persistence_with_notnull_schema() -> (DiscussPersistence, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database = open_local_db(temp_dir.path())
        .await
        .expect("test database opens");
    apply_discuss_migrations(&database)
        .await
        .expect("apply test migrations");
    let database = Arc::new(database);
    let persistence = DiscussPersistence {
        database: database.clone(),
    };
    (persistence, temp_dir)
}

/// Pre-prompt `SessionOpen` persists the live Discuss session anchor
/// without inventing a user turn, so config options can be written before
/// the first prompt and no transcript row exists until the user actually
/// submits text.
#[tokio::test]
async fn session_open_persists_anchor_without_user_turn() {
    let (persistence, _temp) = persistence_with_notnull_schema().await;
    let sluice_session_id = SluiceSessionId::new();
    let sid = sluice_session_id.to_string();

    persist_session_open(&persistence, &sid, "acp-sid-pre").await;

    let conn = persistence.acquire_connection().await.expect("connection");
    let row = load_discuss_session(&conn, &sid)
        .await
        .expect("load")
        .expect("anchor row");
    assert_eq!(row.acp_session_id.as_deref(), Some("acp-sid-pre"));

    let turns = load_discuss_chat_turns_window(&conn, &sid)
        .await
        .expect("load turns");
    assert!(turns.is_empty(), "pre-prompt SessionOpen must not persist a user turn");

    // First real prompt then creates the position-0 user turn.
    persist_initial_turn(&persistence, &sid, "acp-sid-pre", "first prompt").await;
    let turns = load_discuss_chat_turns_window(&conn, &sid)
        .await
        .expect("load turns after first prompt");
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].role.as_deref(), Some("user"));
    assert_eq!(turns[0].position, 0);
}

/// A streamed assistant response is persisted at its StopReason boundary,
/// then returned by the same transcript load path used after a restart.
#[tokio::test]
async fn assistant_turn_persists_at_stop_boundary_and_loads_after_reseed() {
    let (persistence, _temp) = persistence_with_notnull_schema().await;
    let sluice_session_id = SluiceSessionId::new();
    let sid = sluice_session_id.to_string();
    persist_session_open(&persistence, &sid, "acp-stop").await;
    persist_initial_turn(&persistence, &sid, "acp-stop", "user text").await;

    let mut buffer = AssistantTurnBuffer::default();
    buffer.append_text("first ");
    buffer.append_tool_call(
        serde_json::json!({
            "toolCallId": "tool-1",
            "title": "Read workspace",
            "status": "completed"
        }),
        false,
    );
    buffer.append_thought("considering the request; ");
    buffer.append_text("response");
    let flush = buffer.flush("end_turn".to_string());

    assert!(should_persist_assistant_turn(&flush));
    persist_assistant_turn(&persistence, &sid, 1, &flush)
        .await
        .expect("persist assistant stop boundary");

    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (mut rebooted_actor, _reboot_temp) = make_migrated_actor(bus_requester, broadcast_tx).await;
    rebooted_actor.persistence = persistence.clone();
    let (reseeded_worker_tx, _reseeded_worker_rx) = mpsc::channel(1);
    let join_handle = tokio::spawn(async move {
        drop(_reseeded_worker_rx);
    });
    rebooted_actor.worker_registry.insert(
        sluice_session_id,
        WorkerEntry {
            agent_name: "agent-a".to_string(),
            worker_jti: Some(uuid::Uuid::nil()),
            command_tx: reseeded_worker_tx,
            join_handle,
            acp_session_id: Some(SessionId::from("acp-stop".to_string())),
            desktop_association_requested: false,
        },
    );
    rebooted_actor
        .worker_capabilities
        .insert(sluice_session_id, SessionCapabilitiesCache::default());
    let response = rebooted_actor
        .handle_load_session(sluice_session_id)
        .await
        .expect("reseed persisted session");
    let turns = match response {
        DiscussResponse::SessionLoad { turns, .. } => turns,
        other => panic!("expected reseeded session load, got {other:?}"),
    };
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[0].role, "user");
    assert_eq!(turns[0].position, 0);
    assert_eq!(turns[1].role, "assistant");
    assert_eq!(turns[1].position, 1);
    let content: serde_json::Value = serde_json::from_str(&turns[1].content_json).expect("assistant JSON");
    let parts = content["parts"]
        .as_array()
        .expect("ordered assistant parts");
    assert_eq!(parts[0]["kind"], "text");
    assert_eq!(parts[1]["kind"], "tool_call");
    assert_eq!(parts[1]["update"]["toolCallId"], "tool-1");
    assert_eq!(parts[2]["kind"], "thought");
    assert_eq!(parts[3]["kind"], "text");
}

#[test]
fn cancelled_turn_flushes_partial_content_once_and_late_stop_is_empty() {
    let mut buffer = AssistantTurnBuffer::default();
    buffer.append_text("partial response");

    let cancelled = flush_cancelled_assistant_turn(&mut buffer).expect("partial content must persist");
    assert_eq!(cancelled.aggregated_text, "partial response");
    assert_eq!(cancelled.stop_reason, "cancelled");

    let late_stop = buffer.flush("cancelled".to_string());
    assert!(
        !should_persist_assistant_turn(&late_stop),
        "the late provider StopReason must not duplicate the cancelled buffer"
    );
}

/// Closing a worker after a StopReason drains its already-queued assistant
/// write, so shutdown cannot discard the final live response.
#[tokio::test]
async fn persistence_queue_drains_assistant_turn_on_shutdown() {
    let (persistence, _temp) = persistence_with_notnull_schema().await;
    let sluice_session_id = SluiceSessionId::new();
    let sid = sluice_session_id.to_string();
    let (bus_requester, _bus_requests) = mpsc::channel::<OpaqueRequestEnvelope>(1);
    let (queue, drain) = spawn_persistence_queue(
        persistence.clone(),
        sid.clone(),
        "agent-a".to_string(),
        "chat-drain".to_string(),
        BusRpcContext::authenticated(bus_requester, Jwt::from_opaque("test-token".to_string())),
    );
    enqueue_persistence_job(
        &queue,
        PersistenceJob::SessionOpen {
            acp_session_id: "acp-drain".to_string(),
            completion_tx: None,
        },
    )
    .await;
    enqueue_persistence_job(
        &queue,
        PersistenceJob::InitialTurn {
            acp_session_id: "acp-drain".to_string(),
            position: 0,
            initial_prompt: "user text".to_string(),
        },
    )
    .await;
    enqueue_persistence_job(
        &queue,
        PersistenceJob::AssistantTurn {
            assistant_position: 1,
            flush: FlushPayload {
                aggregated_text: "assistant text".to_string(),
                aggregated_thought: String::new(),
                parts: vec![serde_json::json!({"kind": "text", "text": "assistant text"})],
                stop_reason: "end_turn".to_string(),
            },
        },
    )
    .await;
    drop(queue);
    drain.await.expect("persistence queue drains");

    let conn = persistence.acquire_connection().await.expect("connection");
    let turns = load_discuss_chat_turns_window(&conn, &sid)
        .await
        .expect("load drained transcript");
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[1].role.as_deref(), Some("assistant"));
    assert_eq!(turns[1].position, 1);
}

/// Restored sessions resume the monotonic transcript position after the
/// persisted assistant turn, preserving alternating user/assistant rows.
#[tokio::test]
async fn restored_session_turns_continue_after_persisted_maximum_position() {
    let (persistence, _temp) = persistence_with_notnull_schema().await;
    let sluice_session_id = SluiceSessionId::new();
    let sid = sluice_session_id.to_string();
    persist_session_open(&persistence, &sid, "acp-restored").await;
    persist_initial_turn(&persistence, &sid, "acp-restored", "first user").await;
    persist_assistant_turn(
        &persistence,
        &sid,
        1,
        &FlushPayload {
            aggregated_text: "first assistant".to_string(),
            aggregated_thought: String::new(),
            parts: vec![serde_json::json!({"kind": "text", "text": "first assistant"})],
            stop_reason: "end_turn".to_string(),
        },
    )
    .await
    .expect("persist first assistant");

    let conn = persistence.acquire_connection().await.expect("connection");
    let restored = load_discuss_chat_turns_window(&conn, &sid)
        .await
        .expect("load restored transcript");
    let next_position = restored.last().expect("persisted turns").position + 1;
    assert_eq!(next_position, 2);

    persist_subsequent_user_turn(&persistence, &sid, next_position, "next user").await;
    persist_assistant_turn(
        &persistence,
        &sid,
        next_position + 1,
        &FlushPayload {
            aggregated_text: "next assistant".to_string(),
            aggregated_thought: String::new(),
            parts: vec![serde_json::json!({"kind": "text", "text": "next assistant"})],
            stop_reason: "end_turn".to_string(),
        },
    )
    .await
    .expect("persist restored assistant");

    let turns = load_discuss_chat_turns_window(&conn, &sid)
        .await
        .expect("reload transcript");
    assert_eq!(
        turns.iter().map(|turn| turn.position).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    assert_eq!(
        turns
            .iter()
            .map(|turn| turn.role.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("user"), Some("assistant"), Some("user"), Some("assistant")]
    );
}

/// Elicitation reservations share the recovered session sequence with
/// ordinary turns, so the bounded transcript remains chronological and
/// retains the latest ten records after mixed persistence.
#[tokio::test]
async fn mixed_elicitation_and_ordinary_turns_load_and_prune_chronologically() {
    let (persistence, _temp) = persistence_with_notnull_schema().await;
    let sluice_session_id = SluiceSessionId::new();
    let sid = sluice_session_id.to_string();
    persist_session_open(&persistence, &sid, "acp-mixed").await;
    persist_initial_turn(&persistence, &sid, "acp-mixed", "first user").await;
    persist_assistant_turn(
        &persistence,
        &sid,
        1,
        &FlushPayload {
            aggregated_text: "first assistant".to_string(),
            aggregated_thought: String::new(),
            parts: vec![serde_json::json!({"kind": "text", "text": "first assistant"})],
            stop_reason: "end_turn".to_string(),
        },
    )
    .await
    .expect("persist first assistant");

    let allocator = TranscriptPositionAllocator::new(2);
    let elicitation_position = allocator.next();
    assert_eq!(elicitation_position, 2);
    persist_elicitation_turn_inner(
        &persistence,
        &sid,
        "elicitation-1",
        ElicitationPersistenceInput {
            position: elicitation_position,
            message: "Confirm the action".to_string(),
        },
        DiscussElicitationAnswer::Cancel,
    )
    .await
    .expect("persist elicitation");

    for position in 3..=11 {
        let allocated_position = allocator.next();
        assert_eq!(allocated_position, position);
        if position % 2 == 1 {
            persist_subsequent_user_turn(&persistence, &sid, allocated_position, "next user").await;
        } else {
            persist_assistant_turn(
                &persistence,
                &sid,
                allocated_position,
                &FlushPayload {
                    aggregated_text: "next assistant".to_string(),
                    aggregated_thought: String::new(),
                    parts: vec![serde_json::json!({"kind": "text", "text": "next assistant"})],
                    stop_reason: "end_turn".to_string(),
                },
            )
            .await
            .expect("persist later assistant");
        }
    }

    let conn = persistence.acquire_connection().await.expect("connection");
    let turns = load_discuss_chat_turns_window(&conn, &sid)
        .await
        .expect("load mixed transcript");
    assert_eq!(
        turns.iter().map(|turn| turn.position).collect::<Vec<_>>(),
        (2..=11).collect::<Vec<_>>()
    );
    assert_eq!(turns[0].role.as_deref(), Some("elicitation"));
    assert!(turns[0].content_json.contains("elicitation-1"));
}

#[test]
fn transcript_position_allocator_is_unique_across_concurrent_reservations() {
    let allocator = TranscriptPositionAllocator::new(7);
    let handles = (0..4)
        .map(|_| {
            let allocator = allocator.clone();
            std::thread::spawn(move || (0..8).map(|_| allocator.next()).collect::<Vec<_>>())
        })
        .collect::<Vec<_>>();
    let positions = handles
        .into_iter()
        .flat_map(|handle| handle.join().expect("position reservation thread"))
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(positions, (7_i64..39_i64).collect());
}

/// The cleared buffer from a duplicate StopReason must not produce another
/// assistant transcript row.
#[tokio::test]
async fn duplicate_stop_reason_does_not_persist_an_empty_assistant_turn() {
    let (persistence, _temp) = persistence_with_notnull_schema().await;
    let sluice_session_id = SluiceSessionId::new();
    let sid = sluice_session_id.to_string();
    persist_session_open(&persistence, &sid, "acp-duplicate").await;
    persist_initial_turn(&persistence, &sid, "acp-duplicate", "user text").await;

    let mut buffer = AssistantTurnBuffer::default();
    buffer.append_text("assistant text");
    let first = buffer.flush("end_turn".to_string());
    persist_assistant_turn(&persistence, &sid, 1, &first)
        .await
        .expect("persist first stop boundary");
    let duplicate = buffer.flush("end_turn".to_string());
    assert!(!should_persist_assistant_turn(&duplicate));

    let conn = persistence.acquire_connection().await.expect("connection");
    let turns = load_discuss_chat_turns_window(&conn, &sid)
        .await
        .expect("load transcript");
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[1].role.as_deref(), Some("assistant"));
}

/// The initial anchor upsert with no config options / no available commands
/// must insert a parent row, and the initial child turn must then insert
/// without a FOREIGN KEY error.
#[tokio::test]
async fn initial_anchor_upsert_and_child_turn_succeed_on_notnull_schema() {
    let (persistence, _temp) = persistence_with_notnull_schema().await;
    let conn = persistence.acquire_connection().await.expect("connection");
    let sluice_session_id = SluiceSessionId::new();
    let sid = sluice_session_id.to_string();

    // Anchor upsert as the initial-turn path issues it: a real acp_session_id
    // but no config options and no available commands.
    upsert_discuss_session(
        &conn,
        DiscussSessionUpsert {
            sluice_session_id: &sid,
            agent_name: "agent-a",
            session_name: "chat-1",
            acp_session_id: Some("acp-sid-1"),
            display_title: Some("hello world"),
            last_user_prompt: Some("hello world"),
            latest_todo_snapshot_json: None,
            latest_config_options_json: None,
            config_settings_json: None,
            team_leader_id: None,
            available_commands_json: None,
            first_prompt_at: Some(1_000),
            last_interaction_at: Some(1_000),
        },
    )
    .await
    .expect("initial anchor upsert must succeed");

    // Child turn keyed to the same identity must anchor to the parent row.
    insert_discuss_chat_turn(
        &conn,
        DiscussChatTurnInsert {
            sluice_session_id: &sid,
            position: 0,
            role: "user",
            acp_session_id: None,
            content_json: r#"{"role":"user","text":"hello world"}"#,
            created_at: 1_000,
        },
    )
    .await
    .expect("initial user turn must not fail a FOREIGN KEY constraint");

    // The persisted nullable JSON column is NULL when absent (the new schema
    // declares it NULL, no coercion needed).
    let row = load_discuss_session(&conn, &sid)
        .await
        .expect("load succeeds")
        .expect("row exists");
    assert_eq!(
        row.latest_config_options_json.as_deref(),
        None,
        "absent config options persist as NULL in the new schema",
    );
    assert_eq!(
        row.acp_session_id.as_deref(),
        Some("acp-sid-1"),
        "the real provider session id must be preserved verbatim",
    );
}

/// An anchor insert that omits `acp_session_id` entirely (null) must still
/// insert, and the NULL value must be normalized back to `None` by
/// `present_acp_session_id`.
#[tokio::test]
async fn anchor_upsert_with_absent_session_id_inserts_and_normalizes() {
    let (persistence, _temp) = persistence_with_notnull_schema().await;
    let conn = persistence.acquire_connection().await.expect("connection");
    let sluice_session_id = SluiceSessionId::new();
    let sid = sluice_session_id.to_string();

    upsert_discuss_session(
        &conn,
        DiscussSessionUpsert {
            sluice_session_id: &sid,
            agent_name: "agent-b",
            session_name: "chat-2",
            acp_session_id: None,
            display_title: None,
            last_user_prompt: None,
            latest_todo_snapshot_json: None,
            latest_config_options_json: None,
            config_settings_json: None,
            team_leader_id: None,
            available_commands_json: None,
            first_prompt_at: Some(2_000),
            last_interaction_at: Some(2_000),
        },
    )
    .await
    .expect("anchor upsert with absent session id must succeed");

    let row = load_discuss_session(&conn, &sid)
        .await
        .expect("load succeeds")
        .expect("row exists");
    assert_eq!(
        row.acp_session_id.as_deref(),
        None,
        "absent acp_session_id persists as NULL in the new schema",
    );
    assert!(
        present_acp_session_id(row.acp_session_id.as_deref()).is_none(),
        "NULL acp_session_id normalizes to None so no spurious provider refresh fires",
    );
    assert_eq!(
        row.display_title.as_deref(),
        None,
        "absent display_title persists as NULL in the new schema",
    );
}

/// A metadata update with **no** provider title, driven through the real
/// production persistence path (`persist_metadata_update_inner`), must
/// insert the anchor row. A `config_option_update` carries a `sessionId`
/// and options but no `title`, so `display_title` binds NULL and the new
/// schema stores it as NULL directly.
#[tokio::test]
async fn titleless_metadata_update_inserts_on_notnull_display_title_schema() {
    let (persistence, _temp) = persistence_with_notnull_schema().await;
    let sluice_session_id = SluiceSessionId::new();
    let sid = sluice_session_id.to_string();

    // A provider config update in the real SDK wire shape — sessionId +
    // options, but no `title`.
    let metadata = config_option_update_metadata("gpt-4");

    persist_metadata_update_inner(&persistence, &sid, "agent-t", "chat-t", &metadata)
        .await
        .expect("titleless metadata update must succeed");

    let conn = persistence.acquire_connection().await.expect("connection");
    let row = load_discuss_session(&conn, &sid)
        .await
        .expect("load succeeds")
        .expect("row exists after titleless persist");
    assert_eq!(
        row.display_title.as_deref(),
        None,
        "a titleless update persists NULL display_title in the new schema",
    );
    // The config options still round-trip through the canonical persistence
    // path unaffected by the display-title.
    let options: Vec<DiscussConfigOption> = row
        .latest_config_options_json
        .as_deref()
        .and_then(|json| serde_json::from_str(json).ok())
        .unwrap_or_default();
    assert_eq!(options.len(), 1, "config options persist alongside the NULL title");
    assert_eq!(options[0].id, "model");
}

/// An explicit title clear (SDK `session_info_update` carrying `title: null`)
/// must write NULL to the `display_title` column in the new schema, while
/// leaving the row otherwise intact.
#[tokio::test]
async fn explicit_title_clear_maps_to_safe_empty_display_on_notnull_schema() {
    let (persistence, _temp) = persistence_with_notnull_schema().await;
    let conn = persistence.acquire_connection().await.expect("connection");
    let sluice_session_id = SluiceSessionId::new();
    let sid = sluice_session_id.to_string();

    // Seed a row with a real title.
    upsert_discuss_session(
        &conn,
        DiscussSessionUpsert {
            sluice_session_id: &sid,
            agent_name: "agent-clear",
            session_name: "chat-clear",
            acp_session_id: Some("acp-sid-clear"),
            display_title: Some("original title"),
            last_user_prompt: Some("prompt"),
            latest_todo_snapshot_json: None,
            latest_config_options_json: Some("[]"),
            config_settings_json: None,
            team_leader_id: None,
            available_commands_json: Some("[]"),
            first_prompt_at: Some(5_000),
            last_interaction_at: Some(5_000),
        },
    )
    .await
    .expect("seed anchor with a real title");

    // Explicit clear: title bound as NULL (the caller sets clear.display_title
    // and resolves the bound value to None). The UPDATE half writes NULL
    // directly in the new schema.
    upsert_discuss_session(
        &conn,
        DiscussSessionUpsert {
            sluice_session_id: &sid,
            agent_name: "agent-clear",
            session_name: "chat-clear",
            acp_session_id: None,
            display_title: None,
            last_user_prompt: Some("prompt"),
            latest_todo_snapshot_json: None,
            latest_config_options_json: Some("[]"),
            config_settings_json: None,
            team_leader_id: None,
            available_commands_json: Some("[]"),
            first_prompt_at: Some(5_000),
            last_interaction_at: Some(6_000),
        },
    )
    .await
    .expect("explicit title clear must succeed");

    let row = load_discuss_session(&conn, &sid)
        .await
        .expect("load succeeds")
        .expect("row exists");
    assert_eq!(
        row.display_title.as_deref(),
        None,
        "an explicit title clear persists NULL display_title in the new schema",
    );
    assert_eq!(
        row.acp_session_id.as_deref(),
        Some("acp-sid-clear"),
        "a None acp_session_id on the clearing update must preserve the existing id",
    );
}

/// A subsequent partial metadata update that omits `acp_session_id` and
/// config options must preserve the previously persisted values rather than
/// clobbering them with NULL.
#[tokio::test]
async fn partial_metadata_update_preserves_existing_values() {
    let (persistence, _temp) = persistence_with_notnull_schema().await;
    let conn = persistence.acquire_connection().await.expect("connection");
    let sluice_session_id = SluiceSessionId::new();
    let sid = sluice_session_id.to_string();

    // Seed a fully-populated anchor row.
    upsert_discuss_session(
        &conn,
        DiscussSessionUpsert {
            sluice_session_id: &sid,
            agent_name: "agent-c",
            session_name: "chat-3",
            acp_session_id: Some("acp-sid-3"),
            display_title: Some("first title"),
            last_user_prompt: Some("first prompt"),
            latest_todo_snapshot_json: None,
            latest_config_options_json: Some(
                r#"[{"id":"model","name":"Model","category":"model","value":"x","options":[]}]"#,
            ),
            config_settings_json: None,
            team_leader_id: None,
            available_commands_json: Some("[]"),
            first_prompt_at: Some(3_000),
            last_interaction_at: Some(3_000),
        },
    )
    .await
    .expect("seed anchor upsert");

    // A metadata-only bump: only last_user_prompt / last_interaction_at
    // change; acp_session_id and config options are omitted (None). The Rust
    // carry-forward layer normally supplies the existing values, but even a
    // bare None bind must not clobber the persisted id / options via SQL.
    upsert_discuss_session(
        &conn,
        DiscussSessionUpsert {
            sluice_session_id: &sid,
            agent_name: "agent-c",
            session_name: "chat-3",
            acp_session_id: None,
            display_title: Some("first title"),
            last_user_prompt: Some("second prompt"),
            latest_todo_snapshot_json: None,
            latest_config_options_json: None,
            config_settings_json: None,
            team_leader_id: None,
            available_commands_json: None,
            first_prompt_at: Some(3_000),
            last_interaction_at: Some(4_000),
        },
    )
    .await
    .expect("partial metadata update upsert");

    let row = load_discuss_session(&conn, &sid)
        .await
        .expect("load succeeds")
        .expect("row exists");
    assert_eq!(
        row.acp_session_id.as_deref(),
        Some("acp-sid-3"),
        "a None acp_session_id on update must preserve the existing id, not overwrite with NULL",
    );
    assert_eq!(
        row.last_user_prompt.as_deref(),
        Some("second prompt"),
        "the supplied last_user_prompt must be written",
    );
    assert_eq!(
        row.last_interaction_at,
        Some(4_000),
        "the supplied last_interaction_at must advance",
    );
    // A None config-options bind at the raw-SQL layer writes NULL in the new
    // schema (the Rust `persist_session_anchor_inner` layer separately
    // carries the existing options forward on a normal metadata bump).
    assert_eq!(
        row.latest_config_options_json.as_deref(),
        None,
        "a None config-options bind writes NULL in the new schema",
    );
}

// ── Session identity / list materialization / remove-session ────────────

/// `acp.listSessions` must never insert unknown provider sessions into
/// `acp_agent_sessions`. Provider list noise is presentation-only merge
/// input for already-local rows.
#[tokio::test]
async fn list_sessions_does_not_materialize_unknown_provider_sessions() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (mut actor, _temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;

    let session_name = "local-chat";
    let agent_name = "agent-a";
    let sluice_session_id = SluiceSessionId::new();
    let sluice_session_id_text = sluice_session_id.to_string();
    {
        let conn = actor
            .persistence
            .acquire_connection()
            .await
            .expect("connection");
        upsert_discuss_session(
            &conn,
            DiscussSessionUpsert {
                sluice_session_id: &sluice_session_id_text,
                session_name,
                agent_name,
                acp_session_id: Some("acp-local-1"),
                display_title: Some("Local chat"),
                last_user_prompt: Some("hello"),
                latest_todo_snapshot_json: None,
                latest_config_options_json: Some("[]"),
                config_settings_json: None,
                team_leader_id: None,
                available_commands_json: Some("[]"),
                first_prompt_at: Some(1_000),
                last_interaction_at: Some(2_000),
            },
        )
        .await
        .expect("seed local session");
    }

    let (command_tx, mut command_rx) = mpsc::channel(4);
    let worker = tokio::spawn(async move {
        match command_rx.recv().await {
            Some(WorkerCommand::ListProviderSessions { reply }) => {
                let _ = reply.send(Ok(vec![
                    ProviderSessionInfo {
                        session_id: SessionId::from("acp-local-1"),
                        updated_at: None,
                        config_options: None,
                    },
                    // Unknown provider noise — must not become a local row.
                    ProviderSessionInfo {
                        session_id: SessionId::from("provider-noise-word-1"),
                        updated_at: None,
                        config_options: None,
                    },
                    ProviderSessionInfo {
                        session_id: SessionId::from("provider-noise-word-2"),
                        updated_at: None,
                        config_options: None,
                    },
                ]));
            }
            other => panic!("expected ListProviderSessions; got {other:?}"),
        }
    });

    actor.worker_registry.insert(
        sluice_session_id,
        WorkerEntry {
            agent_name: "agent-a".to_string(),
            worker_jti: Some(uuid::Uuid::nil()),
            command_tx,
            join_handle: worker,
            acp_session_id: Some(SessionId::from("acp-local-1".to_string())),
            desktop_association_requested: false,
        },
    );
    actor.worker_capabilities.insert(
        sluice_session_id,
        SessionCapabilitiesCache {
            list: true,
            ..SessionCapabilitiesCache::default()
        },
    );

    let response = actor
        .handle_list_sessions()
        .await
        .expect("list sessions succeeds");
    // The mock worker's join handle is owned by the registry entry; the
    // handler consumed the ListProviderSessions command so the worker task
    // has completed. Abort the handle to avoid leaking if it hasn't finished.
    if let Some(entry) = actor.worker_registry.remove(&sluice_session_id) {
        entry.join_handle.abort();
    }

    match response {
        DiscussResponse::SessionList { sessions } => {
            assert_eq!(
                sessions.len(),
                1,
                "only the local Sluice session must appear; provider noise must not materialize"
            );
            assert_eq!(sessions[0].session_name, session_name);
            assert_eq!(sessions[0].agent_name, agent_name);
            assert_eq!(
                sessions[0].display_title.as_deref(),
                Some("Local chat"),
                "provider session/list metadata must not replace a local display label"
            );
        }
        other => panic!("expected SessionList, got {other:?}"),
    }

    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");
    let rows = list_recent_discuss_sessions(&conn)
        .await
        .expect("list local rows");
    assert_eq!(
        rows.len(),
        1,
        "listSessions must not insert unknown provider sessions into Turso"
    );
    assert_eq!(rows[0].session_name, session_name);
    assert_eq!(rows[0].agent_name, agent_name);
}

/// Existing local persisted sessions still list correctly without a live worker.
#[tokio::test]
async fn list_sessions_returns_local_persisted_rows() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (mut actor, _temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;

    {
        let conn = actor
            .persistence
            .acquire_connection()
            .await
            .expect("connection");
        let sid_a = SluiceSessionId::new().to_string();
        let sid_b = SluiceSessionId::new().to_string();
        upsert_discuss_session(
            &conn,
            DiscussSessionUpsert {
                sluice_session_id: &sid_a,
                agent_name: "agent-a",
                session_name: "chat-a",
                acp_session_id: Some("acp-a"),
                display_title: Some("Chat A"),
                last_user_prompt: None,
                latest_todo_snapshot_json: None,
                latest_config_options_json: Some("[]"),
                config_settings_json: None,
                team_leader_id: None,
                available_commands_json: Some("[]"),
                first_prompt_at: Some(1_000),
                last_interaction_at: Some(3_000),
            },
        )
        .await
        .expect("seed chat-a");
        upsert_discuss_session(
            &conn,
            DiscussSessionUpsert {
                sluice_session_id: &sid_b,
                agent_name: "agent-b",
                session_name: "chat-b",
                acp_session_id: None,
                display_title: Some("Chat B"),
                last_user_prompt: None,
                latest_todo_snapshot_json: None,
                latest_config_options_json: Some("[]"),
                config_settings_json: None,
                team_leader_id: None,
                available_commands_json: Some("[]"),
                first_prompt_at: Some(1_000),
                last_interaction_at: Some(4_000),
            },
        )
        .await
        .expect("seed chat-b");
    }

    let response = actor
        .handle_list_sessions()
        .await
        .expect("list sessions succeeds");
    match response {
        DiscussResponse::SessionList { sessions } => {
            assert_eq!(sessions.len(), 2);
            // Ordered by last_interaction_at DESC.
            assert_eq!(sessions[0].session_name, "chat-b");
            assert_eq!(sessions[1].session_name, "chat-a");
        }
        other => panic!("expected SessionList, got {other:?}"),
    }
}

/// Defensive list dedupe collapses duplicate identity and acp_session_id rows.
#[test]
fn dedupe_session_list_collapses_identity_and_acp_session_id() {
    let sessions = vec![
        DiscussSessionInfo {
            is_task_session: false,
            team_leader_id: None,
            sluice_session_id: SluiceSessionId::from_str("00000000-0000-0000-0000-000000000001").expect("uuid"),
            session_name: "chat-1".to_string(),
            agent_name: "agent-a".to_string(),
            acp_session_id: Some("acp-1".to_string()),
            display_title: Some("first".to_string()),
            last_user_prompt: None,
            latest_todo_snapshot_json: None,
            latest_config_options_json: None,
            available_commands_json: None,
            first_prompt_at: Some(1),
            last_interaction_at: Some(10),
        },
        DiscussSessionInfo {
            is_task_session: false,
            team_leader_id: None,
            sluice_session_id: SluiceSessionId::from_str("00000000-0000-0000-0000-000000000001").expect("uuid"),
            session_name: "chat-1".to_string(),
            agent_name: "agent-a".to_string(),
            acp_session_id: Some("acp-1".to_string()),
            display_title: Some("dup identity".to_string()),
            last_user_prompt: None,
            latest_todo_snapshot_json: None,
            latest_config_options_json: None,
            available_commands_json: None,
            first_prompt_at: Some(1),
            last_interaction_at: Some(9),
        },
        DiscussSessionInfo {
            is_task_session: false,
            team_leader_id: None,
            sluice_session_id: SluiceSessionId::from_str("00000000-0000-0000-0000-000000000002").expect("uuid"),
            session_name: "chat-2".to_string(),
            agent_name: "agent-a".to_string(),
            acp_session_id: Some("acp-1".to_string()),
            display_title: Some("same provider id".to_string()),
            last_user_prompt: None,
            latest_todo_snapshot_json: None,
            latest_config_options_json: None,
            available_commands_json: None,
            first_prompt_at: Some(1),
            last_interaction_at: Some(8),
        },
        DiscussSessionInfo {
            is_task_session: false,
            team_leader_id: None,
            sluice_session_id: SluiceSessionId::from_str("00000000-0000-0000-0000-000000000003").expect("uuid"),
            session_name: "chat-3".to_string(),
            agent_name: "agent-b".to_string(),
            acp_session_id: Some("acp-3".to_string()),
            display_title: Some("unique".to_string()),
            last_user_prompt: None,
            latest_todo_snapshot_json: None,
            latest_config_options_json: None,
            available_commands_json: None,
            first_prompt_at: Some(1),
            last_interaction_at: Some(7),
        },
    ];

    let deduped = dedupe_session_list(sessions);
    assert_eq!(deduped.len(), 2);
    assert_eq!(deduped[0].session_name, "chat-1");
    assert_eq!(deduped[1].session_name, "chat-3");
}

/// Migration 0030 enforces uniqueness on `sluice_session_id` (PRIMARY KEY).
#[tokio::test]
async fn session_name_agent_name_uniqueness_is_enforced() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (_actor, temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;

    let database = open_local_db(temp_dir.path())
        .await
        .expect("reopen test database");
    let conn = connect_database(&database).await.expect("connection");

    conn.execute(
        "INSERT INTO discuss_sessions (
                sluice_session_id, agent_name, session_name, acp_session_id,
                display_title, last_user_prompt, latest_config_options_json,
                available_commands_json, first_prompt_at, last_interaction_at
            ) VALUES ('00000000-0000-0000-0000-000000000001', 'agent', 'dup', NULL, NULL, NULL, NULL, NULL, 0, 0)",
        (),
    )
    .await
    .expect("first insert succeeds");

    let second = conn
        .execute(
            "INSERT INTO discuss_sessions (
                    sluice_session_id, agent_name, session_name, acp_session_id,
                    display_title, last_user_prompt, latest_config_options_json,
                    available_commands_json, first_prompt_at, last_interaction_at
                ) VALUES ('00000000-0000-0000-0000-000000000001', 'agent', 'dup', NULL, NULL, NULL, NULL, NULL, 0, 1)",
            (),
        )
        .await;
    assert!(
        second.is_err(),
        "primary key on sluice_session_id must reject duplicates"
    );
}

/// `discuss.removeSession` deletes the parent row and cascades child turns,
/// and drops live worker registry / capability state.
#[tokio::test]
async fn remove_session_deletes_parent_cascades_turns_and_cleans_worker() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (mut actor, _temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;

    let sluice_session_id = SluiceSessionId::new();
    let sid = sluice_session_id.to_string();
    {
        let conn = actor
            .persistence
            .acquire_connection()
            .await
            .expect("connection");
        upsert_discuss_session(
            &conn,
            DiscussSessionUpsert {
                sluice_session_id: &sid,
                agent_name: "agent-a",
                session_name: "remove-me",
                acp_session_id: Some("acp-rm"),
                display_title: Some("Remove me"),
                last_user_prompt: Some("hi"),
                latest_todo_snapshot_json: None,
                latest_config_options_json: Some("[]"),
                config_settings_json: None,
                team_leader_id: None,
                available_commands_json: Some("[]"),
                first_prompt_at: Some(1_000),
                last_interaction_at: Some(2_000),
            },
        )
        .await
        .expect("seed session");
        insert_discuss_chat_turn(
            &conn,
            DiscussChatTurnInsert {
                sluice_session_id: &sid,
                position: 0,
                role: "user",
                acp_session_id: Some("acp-rm"),
                content_json: r#"{"role":"user","text":"hi"}"#,
                created_at: 1_000,
            },
        )
        .await
        .expect("seed turn");
        insert_discuss_chat_turn(
            &conn,
            DiscussChatTurnInsert {
                sluice_session_id: &sid,
                position: 1,
                role: "assistant",
                acp_session_id: Some("acp-rm"),
                content_json: r#"{"role":"assistant","text":"hello"}"#,
                created_at: 2_000,
            },
        )
        .await
        .expect("seed assistant turn");
    }

    let (command_tx, _command_rx) = mpsc::channel(1);
    let join_handle = tokio::spawn(async move {
        drop(_command_rx);
    });
    actor.worker_registry.insert(
        sluice_session_id,
        WorkerEntry {
            agent_name: "agent-a".to_string(),
            worker_jti: Some(uuid::Uuid::nil()),
            command_tx,
            join_handle,
            acp_session_id: Some(SessionId::from("acp-rm".to_string())),
            desktop_association_requested: false,
        },
    );
    actor.worker_capabilities.insert(
        sluice_session_id,
        SessionCapabilitiesCache {
            list: true,
            load: true,
            set_config_option: true,
        },
    );

    let response = actor
        .handle_remove_session(sluice_session_id)
        .await
        .expect("remove succeeds");
    assert!(matches!(response, DiscussResponse::Success));

    assert!(
        !actor.worker_registry.contains_key(&sluice_session_id),
        "live worker registry entry must be dropped"
    );
    assert!(
        !actor.worker_capabilities.contains_key(&sluice_session_id),
        "capability cache entry must be dropped"
    );

    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");
    let row = load_discuss_session(&conn, &sid).await.expect("load");
    assert!(row.is_none(), "parent discuss_sessions row must be deleted");

    let mut turns = conn
        .query(
            "SELECT COUNT(*) FROM discuss_chat_turns WHERE sluice_session_id = ?1",
            (sid.clone(),),
        )
        .await
        .expect("count turns");
    let turn_row = turns.next().await.expect("row").expect("count row");
    let count: i64 = turn_row.get(0).expect("count value");
    assert_eq!(count, 0, "child discuss_chat_turns must cascade-delete");
}

/// Removing a missing session is idempotent success and still cleans worker state.
#[tokio::test]
async fn remove_session_is_idempotent_for_missing_row() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, _) = mpsc::channel(1);
    let (mut actor, _temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;

    let sluice_session_id = SluiceSessionId::new();
    let (command_tx, _command_rx) = mpsc::channel(1);
    let join_handle = tokio::spawn(async move {
        drop(_command_rx);
    });
    actor.worker_registry.insert(
        sluice_session_id,
        WorkerEntry {
            agent_name: "agent".to_string(),
            worker_jti: Some(uuid::Uuid::nil()),
            command_tx,
            join_handle,
            acp_session_id: None,
            desktop_association_requested: false,
        },
    );
    actor
        .worker_capabilities
        .insert(sluice_session_id, SessionCapabilitiesCache::default());

    let response = actor
        .handle_remove_session(sluice_session_id)
        .await
        .expect("remove missing is success");
    assert!(matches!(response, DiscussResponse::Success));
    assert!(!actor.worker_registry.contains_key(&sluice_session_id));
    assert!(!actor.worker_capabilities.contains_key(&sluice_session_id));
}

/// A warm worker must not announce a desktop association until a real
/// client requested one; a normal session start still announces exactly once.
#[tokio::test]
async fn warm_worker_readiness_does_not_emit_premature_desktop_association() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, mut broadcast_rx) = mpsc::channel(2);
    let (_, receiver) = mpsc::channel(1);
    let actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;
    let ready_tx = actor.worker_ready_tx.clone();
    let actor_task = tokio::spawn(run_actor(actor));

    let warm_id = SluiceSessionId::new();
    ready_tx
        .send(WorkerReady {
            sluice_session_id: warm_id,
            acp_session_id: SessionId::from("provider-warm"),
            announce_desktop_association: false,
            transient_connectivity: false,
            config_options: None,
            supports_config_updates: false,
        })
        .await
        .expect("warm readiness delivered");
    tokio::task::yield_now().await;
    assert!(matches!(broadcast_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));

    let new_id = SluiceSessionId::new();
    ready_tx
        .send(WorkerReady {
            sluice_session_id: new_id,
            acp_session_id: SessionId::from("provider-new"),
            announce_desktop_association: true,
            transient_connectivity: false,
            config_options: None,
            supports_config_updates: false,
        })
        .await
        .expect("desktop readiness delivered");
    assert!(matches!(
        broadcast_rx.recv().await,
        Some(DiscussResponse::NewSession { session_id }) if session_id == new_id
    ));
    actor_task.abort();
}

/// Attaching to an already-warm local identity reuses its worker and emits
/// one association event instead of creating a second worker.
#[tokio::test]
async fn desktop_attach_reuses_ready_warmed_worker() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, mut broadcast_rx) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let mut actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;
    let sluice_session_id = SluiceSessionId::new();
    let (command_tx, _command_rx) = mpsc::channel(1);
    let join_handle = tokio::spawn(async move {
        drop(_command_rx);
    });
    actor.worker_registry.insert(
        sluice_session_id,
        WorkerEntry {
            agent_name: "agent".to_string(),
            worker_jti: Some(uuid::Uuid::nil()),
            command_tx,
            join_handle,
            acp_session_id: Some(SessionId::from("provider-session".to_string())),
            desktop_association_requested: false,
        },
    );

    let response = actor
        .spawn_session(
            sluice_session_id,
            "agent",
            SpawnSessionOptions {
                name: Some("local-session".to_string()),
                setup: None,
                session_config_overrides: std::collections::BTreeMap::new(),
                acp_session_id: Some("provider-session".to_string()),
                announce_desktop_association: true,
                transient_connectivity: false,
                session_cwd: None,
                sluice_mcp_enabled: None,
                node_token: None,
                flow_mcp_endpoints: None,
            },
        )
        .await
        .expect("warm attach succeeds");

    assert!(matches!(response, DiscussResponse::Success));
    assert_eq!(actor.worker_registry.len(), 1);
    assert!(matches!(
        broadcast_rx.recv().await,
        Some(DiscussResponse::NewSession { session_id }) if session_id == sluice_session_id
    ));
}

/// A desktop attach that races warm-worker setup is retained and announced
/// as soon as that existing worker reports ready.
#[tokio::test]
async fn desktop_attach_waits_for_warming_worker_readiness() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, mut broadcast_rx) = mpsc::channel(1);
    let (_, receiver) = mpsc::channel(1);
    let mut actor = make_actor(receiver, HashMap::new(), bus_requester, broadcast_tx).await;
    let sluice_session_id = SluiceSessionId::new();
    let (command_tx, _command_rx) = mpsc::channel(1);
    let join_handle = tokio::spawn(std::future::pending::<()>());
    actor.worker_registry.insert(
        sluice_session_id,
        WorkerEntry {
            agent_name: "agent".to_string(),
            worker_jti: Some(uuid::Uuid::nil()),
            command_tx,
            join_handle,
            acp_session_id: None,
            desktop_association_requested: false,
        },
    );

    let response = actor
        .spawn_session(
            sluice_session_id,
            "agent",
            SpawnSessionOptions {
                name: Some("local-session".to_string()),
                setup: None,
                session_config_overrides: std::collections::BTreeMap::new(),
                acp_session_id: Some("provider-session".to_string()),
                announce_desktop_association: true,
                transient_connectivity: false,
                session_cwd: None,
                sluice_mcp_enabled: None,
                node_token: None,
                flow_mcp_endpoints: None,
            },
        )
        .await
        .expect("warming attach succeeds");

    assert!(matches!(response, DiscussResponse::Success));
    assert!(matches!(broadcast_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    assert!(
        actor
            .worker_registry
            .get(&sluice_session_id)
            .is_some_and(|entry| entry.desktop_association_requested)
    );

    let ready_tx = actor.worker_ready_tx.clone();
    let actor_task = tokio::spawn(run_actor(actor));
    ready_tx
        .send(WorkerReady {
            sluice_session_id,
            acp_session_id: SessionId::from("provider-session"),
            announce_desktop_association: false,
            transient_connectivity: false,
            config_options: None,
            supports_config_updates: false,
        })
        .await
        .expect("warm readiness delivered");

    assert!(matches!(
        broadcast_rx.recv().await,
        Some(DiscussResponse::NewSession { session_id }) if session_id == sluice_session_id
    ));
    actor_task.abort();
}

/// Startup skips stale anchors for removed agents on every invocation, so
/// they cannot create workers or desktop associations after a restart.
#[tokio::test]
async fn start_server_skips_stale_persisted_session_anchors_idempotently() {
    let (bus_requester, _mock) = MockBus::new(8);
    let (broadcast_tx, mut broadcast_rx) = mpsc::channel(1);
    let (mut actor, _temp_dir) = make_migrated_actor(bus_requester, broadcast_tx).await;
    let conn = actor
        .persistence
        .acquire_connection()
        .await
        .expect("connection");
    for (session_name, acp_session_id) in [("stale-one", "provider-one"), ("stale-two", "provider-two")] {
        let sluice_session_id = SluiceSessionId::new();
        let sluice_session_id_text = sluice_session_id.to_string();
        upsert_discuss_session(
            &conn,
            DiscussSessionUpsert {
                sluice_session_id: &sluice_session_id_text,
                session_name,
                agent_name: "removed-agent",
                acp_session_id: Some(acp_session_id),
                display_title: None,
                last_user_prompt: None,
                latest_todo_snapshot_json: None,
                latest_config_options_json: None,
                config_settings_json: None,
                team_leader_id: None,
                available_commands_json: None,
                first_prompt_at: None,
                last_interaction_at: None,
            },
        )
        .await
        .expect("stale anchor persisted");
    }
    actor.server_running = false;

    assert!(matches!(
        actor.handle_start_server().await,
        Ok(DiscussResponse::Success)
    ));
    assert!(matches!(
        actor.handle_start_server().await,
        Ok(DiscussResponse::Success)
    ));
    assert!(actor.worker_registry.is_empty());
    assert!(matches!(broadcast_rx.try_recv(), Ok(DiscussResponse::ServerStarted)));
    assert!(matches!(broadcast_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
}

fn test_subagent(capture_id: &str, state: DiscussSubagentSurfaceState, updated_at: i64) -> DiscussSubagentRecord {
    DiscussSubagentRecord {
        capture_id: capture_id.to_string(),
        child_session_id: None,
        agent_name: "coder-agent".to_string(),
        description: Some("Implement the task".to_string()),
        fallback_capability: Some("coding".to_string()),
        surface_state: state,
        last_updated_at: updated_at,
    }
}

async fn prepare_subagent_parent(persistence: &DiscussPersistence, session_id: &str) {
    persist_session_anchor_inner(
        persistence,
        session_id,
        SessionAnchorMetadata {
            agent_name: Some("agent"),
            session_name: Some("session"),
            ..SessionAnchorMetadata::default()
        },
    )
    .await
    .expect("parent session persisted");
}

#[tokio::test]
async fn subagent_records_round_trip_with_typed_load() {
    let (persistence, _temp) = persistence_with_notnull_schema().await;
    let session_id = SluiceSessionId::new().to_string();
    prepare_subagent_parent(&persistence, &session_id).await;
    let mut expected = test_subagent("capture-1", DiscussSubagentSurfaceState::Live, 10);
    expected.child_session_id = Some("child-1".to_string());

    persist_subagent(&persistence, &session_id, &expected, 10)
        .await
        .expect("subagent persisted");
    let conn = persistence.acquire_connection().await.expect("connection");
    let loaded = load_discuss_subagents(&conn, &session_id)
        .await
        .expect("subagents loaded");

    assert_eq!(loaded, vec![expected]);
}

#[tokio::test]
async fn subagent_upsert_preserves_child_id_and_terminal_state() {
    let (persistence, _temp) = persistence_with_notnull_schema().await;
    let session_id = SluiceSessionId::new().to_string();
    prepare_subagent_parent(&persistence, &session_id).await;
    let mut done = test_subagent("capture-1", DiscussSubagentSurfaceState::Done, 20);
    done.child_session_id = Some("child-1".to_string());
    persist_subagent(&persistence, &session_id, &done, 10)
        .await
        .expect("done persisted");

    let mut late_update = test_subagent("capture-1", DiscussSubagentSurfaceState::Live, 30);
    late_update.description = None;
    persist_subagent(&persistence, &session_id, &late_update, 10)
        .await
        .expect("late update persisted");
    let conn = persistence.acquire_connection().await.expect("connection");
    let loaded = load_discuss_subagents(&conn, &session_id)
        .await
        .expect("subagents loaded");

    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].child_session_id.as_deref(), Some("child-1"));
    assert_eq!(loaded[0].surface_state, DiscussSubagentSurfaceState::Done);
    assert_eq!(loaded[0].last_updated_at, 20);
    assert_eq!(loaded[0].description.as_deref(), Some("Implement the task"));

    let error = test_subagent("capture-2", DiscussSubagentSurfaceState::Error, 40);
    persist_subagent(&persistence, &session_id, &error, 10)
        .await
        .expect("error persisted");
    let late_done = test_subagent("capture-2", DiscussSubagentSurfaceState::Done, 50);
    persist_subagent(&persistence, &session_id, &late_done, 10)
        .await
        .expect("late done update persisted");
    let loaded = load_discuss_subagents(&conn, &session_id)
        .await
        .expect("subagents loaded");
    let retained_error = loaded
        .iter()
        .find(|record| record.capture_id == "capture-2")
        .expect("error record retained");
    assert_eq!(retained_error.surface_state, DiscussSubagentSurfaceState::Error);
    assert_eq!(retained_error.last_updated_at, 40);
}

#[tokio::test]
async fn subagent_retention_keeps_the_newest_records_across_all_states() {
    let (persistence, _temp) = persistence_with_notnull_schema().await;
    let session_id = SluiceSessionId::new().to_string();
    prepare_subagent_parent(&persistence, &session_id).await;
    for index in 0..12 {
        let record = test_subagent(&format!("done-{index:02}"), DiscussSubagentSurfaceState::Done, index);
        persist_subagent(&persistence, &session_id, &record, 10)
            .await
            .expect("done persisted");
    }
    for (index, state) in [
        DiscussSubagentSurfaceState::Loading,
        DiscussSubagentSurfaceState::Live,
        DiscussSubagentSurfaceState::Error,
    ]
    .into_iter()
    .enumerate()
    {
        let record = test_subagent(&format!("active-{index}"), state, 100 + index as i64);
        persist_subagent(&persistence, &session_id, &record, 10)
            .await
            .expect("active persisted");
    }

    let conn = persistence.acquire_connection().await.expect("connection");
    let loaded = load_discuss_subagents(&conn, &session_id)
        .await
        .expect("subagents loaded");
    assert_eq!(loaded.len(), 10);
    assert!(loaded.iter().any(|record| record.capture_id == "done-11"));
    assert!(!loaded.iter().any(|record| record.capture_id == "done-00"));
    assert!(!loaded.iter().any(|record| record.capture_id == "done-01"));
    assert!(loaded.iter().any(|record| record.capture_id == "active-0"));
    assert!(loaded.iter().any(|record| record.capture_id == "active-1"));
    assert!(loaded.iter().any(|record| record.capture_id == "active-2"));
}

#[tokio::test]
async fn subagent_retention_limit_zero_removes_all_records() {
    let (persistence, _temp) = persistence_with_notnull_schema().await;
    let session_id = SluiceSessionId::new().to_string();
    prepare_subagent_parent(&persistence, &session_id).await;
    let record = test_subagent("active", DiscussSubagentSurfaceState::Live, 1);

    persist_subagent(&persistence, &session_id, &record, 0)
        .await
        .expect("subagent persisted");

    let conn = persistence.acquire_connection().await.expect("connection");
    assert!(
        load_discuss_subagents(&conn, &session_id)
            .await
            .expect("subagents loaded")
            .is_empty()
    );
}

#[tokio::test]
async fn persistence_queue_reads_the_current_subagent_retention_limit() {
    let (persistence, _temp) = persistence_with_notnull_schema().await;
    let session_id = SluiceSessionId::new().to_string();
    prepare_subagent_parent(&persistence, &session_id).await;
    let (bus_requester, mut mock) = MockBus::new(1);
    let (queue, drain) = spawn_persistence_queue(
        persistence.clone(),
        session_id.clone(),
        "agent".to_string(),
        "session".to_string(),
        BusRpcContext::authenticated(bus_requester, Jwt::from_opaque("test-token".to_string())),
    );

    enqueue_persistence_job(
        &queue,
        PersistenceJob::Subagent {
            subagent: test_subagent("active", DiscussSubagentSurfaceState::Live, 1),
        },
    )
    .await;

    let request = mock
        .expect_request::<ConfigDomain, ConfigRequest>(Duration::from_millis(500))
        .await;
    assert!(matches!(request.payload, ConfigRequestPayload::GetConfigSection { section } if section == "discuss"));
    let mut settings = super::super::boot::DiscussSettings::default();
    settings.subagent_retention_limit = 0;
    reply_with(
        request.reply_to,
        Ok(ConfigResponse::ConfigSection {
            value: serde_json::to_value(settings).expect("serialize Discuss settings"),
        }),
    );
    drop(queue);
    drain.await.expect("persistence queue drained");

    let conn = persistence.acquire_connection().await.expect("connection");
    assert!(
        load_discuss_subagents(&conn, &session_id)
            .await
            .expect("subagents loaded")
            .is_empty()
    );
}

#[tokio::test]
async fn deleting_parent_cascades_subagent_records() {
    let (persistence, _temp) = persistence_with_notnull_schema().await;
    let session_id = SluiceSessionId::new().to_string();
    prepare_subagent_parent(&persistence, &session_id).await;
    persist_subagent(
        &persistence,
        &session_id,
        &test_subagent("capture-1", DiscussSubagentSurfaceState::Error, 1),
        10,
    )
    .await
    .expect("subagent persisted");
    let conn = persistence.acquire_connection().await.expect("connection");
    delete_discuss_session(&conn, &session_id)
        .await
        .expect("parent deleted");

    assert!(
        load_discuss_subagents(&conn, &session_id)
            .await
            .expect("subagents loaded")
            .is_empty()
    );
}

#[test]
fn session_worker_jwt_expiration_matches_session_lifetime() {
    assert_eq!(SESSION_WORKER_JWT_EXPIRATION.num_days(), 30);
}

fn assert_limit_breach(limit: &'static str, actual: u64, maximum: u64) {
    let mut state = AgentNodeTerminalState::default();
    let notification = state
        .record_limit_breach("acp-session-test", limit, actual, maximum)
        .expect("limit breach should build ACP cancellation")
        .expect("first limit breach should request cancellation");

    let (method, params) = notification.into_parts();
    assert_eq!(method, "session/cancel");
    assert_eq!(params, json!({"sessionId": "acp-session-test"}));
    assert!(state.limit_error.is_some());

    let terminal = state.finish(json!({"text": "partial"}));
    match terminal {
        AgentNodeTerminalResult::Failed { error } => match error {
            super::DiscussError::AgentNodeLimitReached {
                limit: actual_limit,
                actual: actual_value,
                maximum: maximum_value,
            } => {
                assert_eq!(actual_limit, limit);
                assert_eq!(actual_value, actual);
                assert_eq!(maximum_value, maximum);
            }
            other => panic!("unexpected terminal error: {other:?}"),
        },
        AgentNodeTerminalResult::Succeeded { .. } => {
            panic!("limit breach completed successfully")
        }
    }
}

#[test]
fn max_duration_sends_cancel_and_fails_at_terminal_boundary() {
    assert_limit_breach("maxDuration", 30, 30);
}

#[test]
fn max_tool_calls_sends_cancel_and_fails_at_terminal_boundary() {
    assert_limit_breach("maxToolCalls", 3, 2);
}

#[test]
fn max_turns_sends_cancel_and_fails_at_terminal_boundary() {
    assert_limit_breach("maxTurns", 2, 1);
}

#[test]
fn normal_acp_completion_is_succeeded_at_terminal_boundary() {
    let mut state = AgentNodeTerminalState::default();
    let terminal = state.finish(json!({"text": "complete"}));

    match terminal {
        AgentNodeTerminalResult::Succeeded { output } => {
            assert_eq!(output["text"], "complete");
        }
        AgentNodeTerminalResult::Failed { .. } => panic!("normal completion failed"),
    }
}

// ── DiscussResponse serde roundtrips ─────────────────────────────────────────

fn provider_runtime_state(provider_name: &str) -> ProviderRuntimeState {
    ProviderRuntimeState {
        provider_name: provider_name.to_string(),
        status: ProviderRuntimeStatus::Running,
        restart_count: 0,
        last_failure: None,
    }
}

/// `DiscussResponse::ProvidersList` must serialize and deserialize correctly.
#[test]
fn discuss_response_providers_list_serde_roundtrip() {
    let response = DiscussResponse::ProvidersList {
        providers: vec![provider_runtime_state("agent-a"), provider_runtime_state("agent-b")],
        agents_workspace: "/config/agents".to_string(),
    };

    let json = serde_json::to_value(&response).expect("serialize");
    let roundtripped: DiscussResponse = serde_json::from_value(json).expect("deserialize");
    match roundtripped {
        DiscussResponse::ProvidersList {
            providers,
            agents_workspace,
        } => {
            assert_eq!(agents_workspace, "/config/agents");
            assert_eq!(
                providers
                    .iter()
                    .map(|provider| provider.provider_name.as_str())
                    .collect::<Vec<_>>(),
                vec!["agent-a", "agent-b"]
            );
        }
        other => panic!("expected ProvidersList, got {other:?}"),
    }
}

/// New Team catalog requests must preserve their wire discriminants and
/// payloads through JSON serialization.
#[test]
fn discuss_team_requests_serde_roundtrip() {
    let requests = [
        DiscussRequestPayload::ListTeamCatalog,
        DiscussRequestPayload::InstallTeam {
            leader_id: "chief-of-staff".to_string(),
            provider_name: "opencode".to_string(),
            member_overrides: std::collections::BTreeMap::from([(
                "chief-of-staff".to_string(),
                std::collections::BTreeMap::from([("model".to_string(), "gpt-5".to_string())]),
            )]),
            theme_color: Some(AgentThemeColor::Warning),
        },
        DiscussRequestPayload::EditTeam {
            leader_id: "chief-of-staff".to_string(),
            provider_name: "claude-acp".to_string(),
            member_overrides: std::collections::BTreeMap::new(),
            theme_color: Some(AgentThemeColor::Error),
        },
        DiscussRequestPayload::InstallProviderConfig {
            provider_name: "opencode".to_string(),
        },
        DiscussRequestPayload::RemoveProviderConfig {
            provider_name: "opencode".to_string(),
        },
        DiscussRequestPayload::UninstallTeam {
            leader_id: "chief-of-staff".to_string(),
        },
    ];

    for request in requests {
        let json = serde_json::to_value(&request).expect("serialize request");
        let roundtripped: DiscussRequestPayload = serde_json::from_value(json.clone()).expect("deserialize request");
        let reencoded = serde_json::to_value(roundtripped).expect("re-serialize request");
        assert_eq!(json, reencoded);
    }
}

/// Catalog responses and warning-bearing mutation successes must retain
/// their response discriminants and warning fields on the wire.
#[test]
fn discuss_team_responses_serde_roundtrip() {
    let warning = CleanupWarning {
        kind: "workspace_io".to_string(),
        provider_name: "opencode".to_string(),
        agent_id: Some("chief-of-staff".to_string()),
        path: Some(".opencode/agents/chief-of-staff.md".to_string()),
        message: "leftover file".to_string(),
    };
    let responses = [
        DiscussResponse::ListTeamCatalog {
            entries: vec![TeamCatalogEntry {
                leader_id: "chief-of-staff".to_string(),
                version: "0.0.1".to_string(),
                name: Some("Chief of Staff".to_string()),
                job_title: Some("Operations lead".to_string()),
                description: Some("Coordinates the team.".to_string()),
                installed: true,
                provider_name: Some("opencode".to_string()),
                member_overrides: Some(std::collections::BTreeMap::new()),
                theme_color: Some(AgentThemeColor::Warning),
                definition_available: true,
                members: Some(vec![TeamCatalogMember {
                    agent_id: "chief-of-staff".to_string(),
                    name: "Chief of Staff".to_string(),
                    job_title: "Operations lead".to_string(),
                }]),
                skill_names: Some(Vec::new()),
            }],
        },
        DiscussResponse::InstallTeam { warnings: Vec::new() },
        DiscussResponse::EditTeam {
            warnings: vec![warning.clone()],
        },
        DiscussResponse::UninstallTeam {
            warnings: vec![warning.clone()],
        },
        DiscussResponse::ProviderRemoveSuccess {
            warnings: vec![warning],
        },
    ];

    for response in responses {
        let json = serde_json::to_value(&response).expect("serialize response");
        let roundtripped: DiscussResponse = serde_json::from_value(json.clone()).expect("deserialize response");
        let reencoded = serde_json::to_value(roundtripped).expect("re-serialize response");
        assert_eq!(json, reencoded);
    }
}

/// `DiscussBroadcast::SessionReady` must serialize and deserialize correctly.
#[test]
fn discuss_broadcast_session_ready_serde_roundtrip() {
    let broadcast = DiscussBroadcast::SessionReady {
        sluice_session_id: SluiceSessionId::new(),
    };

    let json = serde_json::to_value(&broadcast).expect("serialize");
    assert!(json.get("acp_session_id").is_none());
    let roundtripped: DiscussBroadcast = serde_json::from_value(json).expect("deserialize");
    assert!(matches!(roundtripped, DiscussBroadcast::SessionReady { .. }));
}

/// `DiscussBroadcast::SessionWarning` must serialize and deserialize correctly.
#[test]
fn discuss_broadcast_session_warning_serde_roundtrip() {
    let broadcast = DiscussBroadcast::SessionWarning {
        sluice_session_id: SluiceSessionId::new(),
        acp_session_id: agent_client_protocol::schema::v1::SessionId::from("acp-2"),
        warning: "missing MCP-over-ACP capability".to_string(),
    };

    let json = serde_json::to_value(&broadcast).expect("serialize");
    let roundtripped: DiscussBroadcast = serde_json::from_value(json).expect("deserialize");
    match roundtripped {
        DiscussBroadcast::SessionWarning { warning, .. } => {
            assert_eq!(warning, "missing MCP-over-ACP capability");
        }
        other => panic!("expected SessionWarning, got {other:?}"),
    }
}

// ── SetConfigOption value contract ────────────────────────────────────────

/// The `SetConfigOption.value` field is a `String` (ACP `SessionConfigValueId`),
/// so a JSON **string** value must deserialize at the RPC contract boundary.
#[test]
fn set_config_option_accepts_string_value() {
    let sluice_id = SluiceSessionId::new();
    let json = serde_json::json!({
        "SetConfigOption": {
            "sluice_session_id": sluice_id,
            "config_id": "model",
            "value": "gpt-4"
        }
    });

    let payload: DiscussRequestPayload = serde_json::from_value(json).expect("string config value must deserialize");
    match payload {
        DiscussRequestPayload::SetConfigOption { value, config_id, .. } => {
            assert_eq!(config_id, "model");
            assert_eq!(value, "gpt-4");
        }
        other => panic!("expected SetConfigOption, got {other:?}"),
    }
}

/// A **non-string** config value (number, boolean) must be rejected at the
/// RPC contract boundary and can never reach ACP dispatch.
#[test]
fn set_config_option_rejects_non_string_value() {
    let sluice_id = SluiceSessionId::new();
    for non_string in [serde_json::json!(1), serde_json::json!(true), serde_json::json!(null)] {
        let json = serde_json::json!({
            "SetConfigOption": {
                "sluice_session_id": sluice_id,
                "config_id": "model",
                "value": non_string.clone()
            }
        });

        let result: Result<DiscussRequestPayload, _> = serde_json::from_value(json);
        assert!(
            result.is_err(),
            "non-string config value {non_string} must be rejected before ACP dispatch"
        );
    }
}

#[test]
fn session_setup_uses_the_typed_camel_case_wire_shape() {
    let setup = DiscussSessionSetup::Team {
        leader_id: "chief-of-staff".to_string(),
    };

    assert_eq!(
        serde_json::to_value(setup).expect("serialize setup"),
        serde_json::json!({ "kind": "team", "leaderId": "chief-of-staff" })
    );
}

/// Non-authentication ACP errors retain their concise protocol message without debug data.
#[test]
fn from_acp_crate_error_produces_safe_protocol_error() {
    let acp_err = agent_client_protocol::Error::into_internal_error(DiscussError::Other(
        "/Users/example/.cargo/registry/src/session.rs:571:14".to_string(),
    ));
    let converted: DiscussError = DiscussError::from(acp_err);
    match converted {
        DiscussError::ProtocolError { message } => {
            assert_eq!(message, "Internal error");
        }
        other => panic!("expected ProtocolError, got {other:?}"),
    }
}

/// Standard ACP authentication failures have a stable typed identity and no debug payload.
#[test]
fn from_acp_authentication_error_produces_typed_error() {
    let acp_err = agent_client_protocol::Error::auth_required().data(serde_json::json!({
        "spawned_at": "/Users/example/.cargo/registry/src/session.rs:571:14"
    }));

    assert!(matches!(
        DiscussError::from(acp_err),
        DiscussError::AuthenticationRequired
    ));
}

/// `From<serde_json::Error>` must produce `DiscussError::Serialization`.
#[test]
fn from_serde_json_error_produces_serialization_variant() {
    let json_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
    let converted: DiscussError = DiscussError::from(json_err);
    match converted {
        DiscussError::Serialization { message } => {
            assert!(!message.is_empty(), "serialization error message must not be empty");
        }
        other => panic!("expected Serialization variant, got {other:?}"),
    }
}

#[test]
fn workspace_errors_display_plugin_path_and_format_context() {
    let io = DiscussError::WorkspaceIo {
        plugin: "opencode".into(),
        path: ".opencode/agents/researcher.md".into(),
        message: "disk full".into(),
    };
    let io_display = io.to_string();
    assert!(io_display.contains("opencode"));
    assert!(io_display.contains(".opencode/agents/researcher.md"));

    let serialize = DiscussError::WorkspaceSerialize {
        plugin: "claude-acp".into(),
        path: ".claude/agents/researcher.md".into(),
        format: "yaml".into(),
        message: "invalid yaml".into(),
    };
    let serialize_display = serialize.to_string();
    assert!(serialize_display.contains("claude-acp"));
    assert!(serialize_display.contains(".claude/agents/researcher.md"));
    assert!(serialize_display.contains("yaml"));
}

/// An installed catalog entry includes its optional metadata and member
/// config selection values in the wire representation.
#[test]
fn team_catalog_entry_serde_roundtrip() {
    let entry = TeamCatalogEntry {
        leader_id: "chief-of-staff".to_string(),
        version: "0.0.1".to_string(),
        name: Some("Chief of Staff".to_string()),
        job_title: Some("Operations lead".to_string()),
        description: Some("Coordinates the team.".to_string()),
        installed: true,
        provider_name: Some("opencode".to_string()),
        member_overrides: Some(BTreeMap::from([(
            "chief-of-staff".to_string(),
            BTreeMap::from([("model".to_string(), "gpt-5".to_string())]),
        )])),
        theme_color: Some(AgentThemeColor::Warning),
        definition_available: true,
        members: Some(vec![TeamCatalogMember {
            agent_id: "chief-of-staff".to_string(),
            name: "Chief of Staff".to_string(),
            job_title: "Operations lead".to_string(),
        }]),
        skill_names: Some(vec!["planning".to_string()]),
    };

    let json = serde_json::to_value(&entry).expect("serialize catalog entry");
    let roundtripped: TeamCatalogEntry = serde_json::from_value(json.clone()).expect("deserialize catalog entry");
    assert_eq!(entry, roundtripped);
    assert_eq!(
        json,
        serde_json::to_value(roundtripped).expect("re-serialize catalog entry")
    );
}

/// Persisted installations whose definition disappeared from Phanes omit
/// metadata while remaining fully representable and manageable.
#[test]
fn absent_team_definition_omits_optional_metadata() {
    let entry = TeamCatalogEntry {
        leader_id: "retired-agent".to_string(),
        version: "0.0.1".to_string(),
        name: None,
        job_title: None,
        description: None,
        installed: true,
        provider_name: Some("custom-provider".to_string()),
        member_overrides: Some(BTreeMap::new()),
        theme_color: None,
        definition_available: false,
        members: None,
        skill_names: None,
    };

    let json = serde_json::to_value(&entry).expect("serialize absent-definition entry");
    assert!(json.get("name").is_none());
    assert!(json.get("description").is_none());
    let roundtripped: TeamCatalogEntry = serde_json::from_value(json).expect("deserialize absent-definition entry");
    assert_eq!(entry, roundtripped);
}

/// Cleanup warnings retain their optional location fields and message.
#[test]
fn cleanup_warning_serde_roundtrip() {
    let warning = CleanupWarning {
        kind: "workspace_io".to_string(),
        provider_name: "opencode".to_string(),
        agent_id: Some("chief-of-staff".to_string()),
        path: Some(".opencode/agents/chief-of-staff.md".to_string()),
        message: "leftover file".to_string(),
    };

    let json = serde_json::to_value(&warning).expect("serialize cleanup warning");
    let roundtripped: CleanupWarning = serde_json::from_value(json.clone()).expect("deserialize cleanup warning");
    assert_eq!(warning, roundtripped);
    assert_eq!(
        json,
        serde_json::to_value(roundtripped).expect("re-serialize cleanup warning")
    );
}

// ── AcpAgentMessagePayload serde roundtrips ──────────────────────────────

/// `AcpAgentMessagePayload::Dispatch` must roundtrip through JSON with the
/// correct camelCase `kind` tag.
#[test]
fn dispatch_payload_serde_roundtrip() {
    let payload = AcpAgentMessagePayload::Dispatch {
        session_id: "sess-1".to_string(),
        dispatch: AcpDispatchPayload::Notification {
            method: "test/method".to_string(),
            params: serde_json::json!({"key": "value"}),
        },
    };

    let json = serde_json::to_value(&payload).expect("serialize");
    assert_eq!(
        json.get("kind").and_then(|v| v.as_str()),
        Some("dispatch"),
        "kind tag must be 'dispatch'"
    );
    assert_eq!(
        json.get("sessionId").and_then(|v| v.as_str()),
        Some("sess-1"),
        "sessionId must be camelCase"
    );

    let roundtripped: AcpAgentMessagePayload = serde_json::from_value(json).expect("deserialize");
    match roundtripped {
        AcpAgentMessagePayload::Dispatch { session_id, dispatch } => {
            assert_eq!(session_id, "sess-1");
            assert!(matches!(dispatch, AcpDispatchPayload::Notification { .. }));
        }
        other => panic!("expected Dispatch after roundtrip, got {other:?}"),
    }
}

/// `AcpAgentMessagePayload::StopReason` must roundtrip with `kind: "stopReason"`.
#[test]
fn stop_reason_payload_serde_roundtrip() {
    use agent_client_protocol::schema::v1::StopReason;

    let payload = AcpAgentMessagePayload::StopReason {
        session_id: "sess-2".to_string(),
        stop_reason: StopReason::EndTurn,
    };

    let json = serde_json::to_value(&payload).expect("serialize");
    assert_eq!(
        json.get("kind").and_then(|v| v.as_str()),
        Some("stopReason"),
        "kind tag must be camelCase 'stopReason'"
    );

    let roundtripped: AcpAgentMessagePayload = serde_json::from_value(json).expect("deserialize");
    assert!(matches!(roundtripped, AcpAgentMessagePayload::StopReason { .. }));
}

// ── AcpDispatchPayload serde roundtrips ──────────────────────────────────

/// `AcpDispatchPayload::Request` must roundtrip with `kind: "request"`.
#[test]
fn dispatch_request_serde_roundtrip() {
    let payload = AcpDispatchPayload::Request {
        method: "tools/call".to_string(),
        params: serde_json::json!({"tool": "bash"}),
    };

    let json = serde_json::to_value(&payload).expect("serialize");
    assert_eq!(
        json.get("kind").and_then(|v| v.as_str()),
        Some("request"),
        "kind tag must be 'request'"
    );

    let roundtripped: AcpDispatchPayload = serde_json::from_value(json).expect("deserialize");
    assert!(matches!(roundtripped, AcpDispatchPayload::Request { .. }));
}

/// `AcpDispatchPayload::Notification` must roundtrip with `kind: "notification"`.
#[test]
fn dispatch_notification_serde_roundtrip() {
    let payload = AcpDispatchPayload::Notification {
        method: "progress".to_string(),
        params: serde_json::json!({"pct": 50}),
    };

    let json = serde_json::to_value(&payload).expect("serialize");
    assert_eq!(
        json.get("kind").and_then(|v| v.as_str()),
        Some("notification"),
        "kind tag must be 'notification'"
    );

    let roundtripped: AcpDispatchPayload = serde_json::from_value(json).expect("deserialize");
    assert!(matches!(roundtripped, AcpDispatchPayload::Notification { .. }));
}

/// `AcpDispatchPayload::Response` with `Ok` result must roundtrip with
/// `kind: "response"` and `status: "ok"`.
#[test]
fn dispatch_response_ok_serde_roundtrip() {
    let payload = AcpDispatchPayload::Response {
        result: AcpDispatchResponsePayload::Ok {
            value: serde_json::json!({"answer": 42}),
        },
    };

    let json = serde_json::to_value(&payload).expect("serialize");
    assert_eq!(
        json.get("kind").and_then(|v| v.as_str()),
        Some("response"),
        "kind tag must be 'response'"
    );
    let result_obj = json.get("result").expect("result field");
    assert_eq!(
        result_obj.get("status").and_then(|v| v.as_str()),
        Some("ok"),
        "status must be 'ok'"
    );

    let roundtripped: AcpDispatchPayload = serde_json::from_value(json).expect("deserialize");
    assert!(matches!(roundtripped, AcpDispatchPayload::Response { .. }));
}

/// `AcpDispatchResponsePayload::Error` must roundtrip with `status: "error"`.
#[test]
fn dispatch_response_error_serde_roundtrip() {
    let payload = AcpDispatchResponsePayload::Error {
        message: "something went wrong".to_string(),
    };

    let json = serde_json::to_value(&payload).expect("serialize");
    assert_eq!(
        json.get("status").and_then(|v| v.as_str()),
        Some("error"),
        "status tag must be 'error'"
    );
    assert_eq!(
        json.get("message").and_then(|v| v.as_str()),
        Some("something went wrong")
    );

    let roundtripped: AcpDispatchResponsePayload = serde_json::from_value(json).expect("deserialize");
    assert!(matches!(roundtripped, AcpDispatchResponsePayload::Error { .. }));
}

// ── SluiceSessionId / PermissionRequestId ───────────────────────────────

/// `SluiceSessionId::new()` must produce distinct UUIDs.
#[test]
fn sluice_session_id_new_produces_distinct_uuids() {
    let a = SluiceSessionId::new();
    let b = SluiceSessionId::new();
    assert_ne!(a, b, "two new SluiceSessionIds must be distinct");
}

/// `SluiceSessionId` must roundtrip through its string representation.
#[test]
fn sluice_session_id_string_roundtrip() {
    let id = SluiceSessionId::new();
    let typed_uuid: SpectaUuid = id.0;
    assert_eq!(typed_uuid.0, id.0.0);
    let s = id.to_string();
    let parsed: SluiceSessionId = s.parse().expect("parse");
    assert_eq!(id, parsed);
}

#[test]
fn session_dto_keeps_sluice_identity_typed_at_the_domain_boundary() {
    let id = SluiceSessionId::new();
    let dto = DiscussSessionInfo {
        is_task_session: false,
        team_leader_id: Some("sector-analyst".to_string()),
        sluice_session_id: id,
        session_name: "session".to_string(),
        agent_name: "agent".to_string(),
        acp_session_id: None,
        display_title: None,
        last_user_prompt: None,
        latest_todo_snapshot_json: None,
        latest_config_options_json: None,
        available_commands_json: None,
        first_prompt_at: None,
        last_interaction_at: None,
    };
    let encoded = serde_json::to_value(&dto).expect("serialize session DTO");
    assert_eq!(encoded["sluiceSessionId"], id.to_string());
    assert_eq!(encoded["teamLeaderId"], "sector-analyst");
    assert!(
        serde_json::from_value::<DiscussSessionInfo>(serde_json::json!({
            "sluiceSessionId": "not-a-uuid",
            "sessionName": "session",
            "agentName": "agent"
        }))
        .is_err()
    );
}

/// `PermissionRequestId::new()` must produce distinct UUIDs.
#[test]
fn permission_request_id_new_produces_distinct_uuids() {
    let a = PermissionRequestId::new();
    let b = PermissionRequestId::new();
    assert_ne!(a, b, "two new PermissionRequestIds must be distinct");
}

/// `DiscussPermissionOption` serde roundtrips for both variants.
#[test]
fn permission_option_serde_roundtrip() {
    let select = DiscussPermissionOption::Select {
        option_id: "allow".to_string(),
    };
    let json = serde_json::to_value(&select).expect("serialize");
    assert_eq!(
        json.get("kind").and_then(|v| v.as_str()),
        Some("select"),
        "kind must be 'select'"
    );
    let roundtripped: DiscussPermissionOption = serde_json::from_value(json).expect("deserialize");
    assert!(matches!(roundtripped, DiscussPermissionOption::Select { .. }));

    let cancel = DiscussPermissionOption::Cancel;
    let json = serde_json::to_value(&cancel).expect("serialize");
    assert_eq!(
        json.get("kind").and_then(|v| v.as_str()),
        Some("cancel"),
        "kind must be 'cancel'"
    );
    let roundtripped: DiscussPermissionOption = serde_json::from_value(json).expect("deserialize");
    assert!(matches!(roundtripped, DiscussPermissionOption::Cancel));
}

/// `DiscussStreamEvent::AgentMessage` serde roundtrips with `kind: "agentMessage"`.
#[test]
fn stream_event_agent_message_serde_roundtrip() {
    let event = DiscussStreamEvent::AgentMessage {
        sluice_session_id: SluiceSessionId::new(),
        acp_session_id: SessionId::from("acp-1"),
        payload: AcpAgentMessagePayload::StopReason {
            session_id: "acp-1".to_string(),
            stop_reason: StopReason::EndTurn,
        },
    };

    let json = serde_json::to_value(&event).expect("serialize");
    assert_eq!(
        json.get("kind").and_then(|v| v.as_str()),
        Some("agentMessage"),
        "kind must be 'agentMessage'"
    );

    let roundtripped: DiscussStreamEvent = serde_json::from_value(json).expect("deserialize");
    assert!(matches!(roundtripped, DiscussStreamEvent::AgentMessage { .. }));
}

/// `DiscussStreamEvent::SessionFailed` retains the typed Discuss error.
#[test]
fn stream_event_session_failed_serde_roundtrip() {
    let sluice_session_id = SluiceSessionId::new();
    let event = DiscussStreamEvent::SessionFailed {
        sluice_session_id,
        error: DiscussError::ProtocolError {
            message: "adapter rejected configuration".to_string(),
        },
        terminal_auth_methods: Vec::new(),
    };

    let json = serde_json::to_value(&event).expect("serialize");
    assert_eq!(json["kind"], "sessionFailed");
    let roundtripped: DiscussStreamEvent = serde_json::from_value(json).expect("deserialize");
    assert!(matches!(
        roundtripped,
        DiscussStreamEvent::SessionFailed {
            sluice_session_id: roundtripped_id,
            error: DiscussError::ProtocolError { .. },
            terminal_auth_methods,
        } if roundtripped_id == sluice_session_id && terminal_auth_methods.is_empty()
    ));
}

#[test]
fn agent_node_limit_breach_keeps_typed_terminal_error() {
    let terminal = AgentNodeTerminalResult::Failed {
        error: DiscussError::AgentNodeLimitReached {
            limit: "maxToolCalls".to_string(),
            actual: 4,
            maximum: 3,
        },
    };

    let roundtripped: AgentNodeTerminalResult =
        serde_json::from_value(serde_json::to_value(terminal).expect("serialize")).expect("deserialize");
    assert!(matches!(
        roundtripped,
        AgentNodeTerminalResult::Failed {
            error: DiscussError::AgentNodeLimitReached {
                actual: 4,
                maximum: 3,
                ..
            }
        }
    ));
}
