use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::{Value, from_value, json, to_string};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior, interval};
use turso::Database;
use uuid::Uuid;

use crate::auth::AuthSudoService;
use crate::bus::{BusRpcContext, BusSudoService};
use crate::machine::MachinePeerService;
use crate::runtime::ServiceReader;
use crate::utils::database::maintain_database;

use super::adapter::{HostLifecycleAdapter, OwnerLifecycleResult, dispatch_cancel};
use super::ast::{ContentAddressedRoot, FlowNode};
use super::blackboard::{Blackboard, BoundOutputs};
use super::controllers::validate_parallel_composition;
use super::cron::FlowTriggerRecord;
use super::database::FlowStore;
use super::graph::{FlowGraph, GraphNode, content_roots};
use super::hook::{
    FlowAppCheckpointCommittedContext, FlowAppCheckpointContext, FlowAppCheckpointResult, FlowAppHookContext,
    FlowAppLandingContext, FlowAppRestoreContext, FlowAppTakeoffContext, FlowAppWorkspaceHook,
};
use super::mailbox::{MailboxMessage, TerminalResult, TerminalStatus, TokenExpired};
use super::mcp::FlowMcpPorts;
use super::messages::{
    ErasedLifecycleMessage, FlowBroadcast, FlowRequest, FlowRequestPayload, FlowResponse, LifecycleCorrelation,
};
use super::mobility::{FlowMobilityService, MobilityCoordinator};
use super::registration::{FlowNodeRegistry, NodeKind};
use super::snapshot::{ExecutionSnapshot, ExecutionSnapshotStore, ScratchpadSnapshot, WorkspaceProvenance};
use super::tick::{FlowDispatch, FlowDispatchKind, FlowTickLoop, FlowTreeStatus, TickResult};
use super::triggers::TriggerRegistry;
use super::types::{
    ActiveExecution, ExecutionState, FlowAdapterError, FlowDefinition, FlowError, FlowExecutionId,
    FlowExecutionSnapshot, FlowExecutionStatus, FlowId, FlowInvocationKind, FlowTimestamp, NodeExecutionId,
    PendingTerminalPersistence, ScriptSourceLocation, WorkspaceError,
};

const TERMINAL_RETENTION_RETRY_INTERVAL: Duration = Duration::from_secs(60);
const NODE_TOKEN_EXPIRED_MESSAGE: &str = "Flow node capability token expired";

struct DispatchCompletion {
    correlation: LifecycleCorrelation,
    node_kind: NodeKind,
    result: Result<OwnerLifecycleResult, FlowAdapterError>,
    token_expired: bool,
    kind: FlowDispatchKind,
}

/// Dependencies supplied during Flow actor construction.
pub(super) struct ActorDependencies {
    /// The authentication service for the Flow actor.
    pub(super) auth_service: ServiceReader<dyn AuthSudoService>,
    /// The MCP bus service for the Flow actor.
    pub(super) mcp_bus_service: Option<ServiceReader<dyn BusSudoService>>,
    /// Late-bound peer services published by Machine.
    pub(super) sync_services: ServiceReader<dyn MachinePeerService>,
    /// Prepared mobility implementation.
    pub(super) mobility_service: Arc<dyn FlowMobilityService>,
    /// The bus context for the Flow actor.
    pub(super) bus_context: BusRpcContext,
    /// The broadcast channel for Flow events.
    pub(super) broadcast_tx: mpsc::Sender<FlowBroadcast>,
    /// The database for the Flow actor.
    pub(super) database: Database,
    pub(super) database_path: PathBuf,
    /// The trigger registry for the Flow actor.
    pub(super) trigger_registry: TriggerRegistry,
    /// The node registrations for the Flow actor.
    pub(super) node_registry: Arc<FlowNodeRegistry>,
    /// Complete workspace implementation selected before actor construction.
    pub(super) workspace_hook: Arc<dyn FlowAppWorkspaceHook>,
    /// Persisted ownership identity of newly prepared workspaces.
    pub(super) application_workspace: bool,
    /// The retention completed runs for the Flow actor.
    pub(super) retention_completed_runs: u32,
}

/// Actor-owned state for the Aves Flow domain.
pub(super) struct Actor {
    /// The receiver for Flow requests.
    receiver: mpsc::Receiver<FlowRequest>,
    /// The broadcast sender for Flow broadcasts.
    pub(super) broadcast_tx: mpsc::Sender<FlowBroadcast>,
    /// The completion sender for Flow dispatch completions.
    completion_tx: mpsc::Sender<DispatchCompletion>,
    /// The completion receiver for Flow dispatch completions.
    completion_rx: mpsc::Receiver<DispatchCompletion>,
    /// The MCP ports for the Flow actor.
    pub(super) mcp_ports: FlowMcpPorts,
    /// Complete workspace implementation supplied during construction.
    pub(super) workspace_hook: Arc<dyn FlowAppWorkspaceHook>,
    application_workspace: bool,
    /// The flow definitions for the Flow actor.
    pub(super) definitions: HashMap<FlowId, FlowDefinition>,
    /// The lifecycle adapter for the Flow actor.
    lifecycle_adapter: HostLifecycleAdapter,
    /// The flow store for the Flow actor.
    pub(super) store: FlowStore,
    database_path: PathBuf,
    /// The node registry for the Flow actor.
    pub(super) node_registry: Arc<FlowNodeRegistry>,
    /// The mobility service for the Flow actor.
    pub(super) mobility_service: Arc<dyn FlowMobilityService>,
    /// The mobility coordinator for the Flow actor.
    pub(super) mobility: MobilityCoordinator,
    /// The triggers for the Flow actor.
    pub(super) triggers: HashMap<Uuid, FlowTriggerRecord>,
    /// The trigger registry for the Flow actor.
    pub(super) trigger_registry: TriggerRegistry,
    /// The terminal executions for the Flow actor.
    pub(super) terminal_executions: Vec<FlowExecutionSnapshot>,
    /// The execution snapshot store for the Flow actor.
    pub(super) snapshot_store: ExecutionSnapshotStore,
    /// The execution state for each Flow execution.
    pub(super) execution: HashMap<FlowExecutionId, ExecutionState>,
    /// The node tokens for each Flow node execution.
    node_tokens: HashMap<NodeExecutionId, Uuid>,
    /// Peer services are resolved after every domain has been constructed.
    sync_services: ServiceReader<dyn MachinePeerService>,
    peer_sync_available: bool,
    /// The number of completed runs to retain.
    pub(super) retention_completed_runs: usize,
    /// The next terminal retention retry time.
    pub(super) next_terminal_retention_retry: Instant,
    /// The background tasks for the Flow actor.
    background_tasks: Vec<JoinHandle<()>>,
    /// Whether the Flow actor is running.
    running: bool,
}

impl Actor {
    // TODO: remove the result return. new shouldn't return a result. they should panic if there's an error
    pub(super) async fn new(
        receiver: mpsc::Receiver<FlowRequest>,
        dependencies: ActorDependencies,
    ) -> Result<Self, FlowError> {
        Self::new_with_recovery(receiver, dependencies).await
    }

    async fn new_with_recovery(
        receiver: mpsc::Receiver<FlowRequest>,
        dependencies: ActorDependencies,
    ) -> Result<Self, FlowError> {
        let ActorDependencies {
            trigger_registry,
            broadcast_tx,
            auth_service,
            mcp_bus_service,
            bus_context,
            database,
            database_path,
            node_registry,
            workspace_hook,
            application_workspace,
            sync_services,
            mobility_service,
            retention_completed_runs,
            ..
        } = dependencies;

        let store = FlowStore::new(database.clone());
        let definitions = store.load_definitions().await?;
        store
            .initialize_authority_states(definitions.keys().copied())
            .await?;

        let mobility = MobilityCoordinator::restore(store.load_mobility_states().await?)?;
        let triggers = store.load_triggers().await?;
        for trigger in triggers.values() {
            trigger_registry.validate_configuration(trigger)?;
        }
        let snapshot_store = ExecutionSnapshotStore::new(database);
        let (completion_tx, completion_rx) = mpsc::channel(64);
        let mcp_ports = FlowMcpPorts::new(mcp_bus_service);

        let mut actor = Self {
            node_registry,
            lifecycle_adapter: HostLifecycleAdapter {
                auth_service,
                bus_context,
                mcp_ports: mcp_ports.clone(),
            },
            workspace_hook,
            application_workspace,
            definitions,
            mobility,
            mobility_service,
            triggers,
            trigger_registry,
            terminal_executions: Vec::new(),
            receiver,
            broadcast_tx,
            snapshot_store,
            store,
            database_path,
            running: false,
            completion_tx,
            completion_rx,
            execution: HashMap::new(),
            node_tokens: HashMap::new(),
            mcp_ports,
            sync_services,
            peer_sync_available: false,
            retention_completed_runs: retention_completed_runs as usize,
            next_terminal_retention_retry: Instant::now() + TERMINAL_RETENTION_RETRY_INTERVAL,
            background_tasks: Vec::new(),
        };

        let mut durable_snapshots = Vec::new();
        for snapshot in actor.snapshot_store.load_all().await? {
            if snapshot.workspace.is_application() != actor.application_workspace {
                return Err(FlowError::AppWorkspaceUnavailable {
                    execution_id: snapshot.execution.execution_id,
                });
            }
            let context = FlowAppHookContext {
                execution_id: snapshot.execution.execution_id,
                flow_id: snapshot.execution.flow_id,
                definition_revision: snapshot.execution.revision,
                execution_root: snapshot.workspace.root(),
                invocation_kind: snapshot.execution.invocation_kind,
            };
            if actor
                .workspace_hook
                .recoverable(context, snapshot.execution.status.clone())
                .await?
            {
                durable_snapshots.push(snapshot);
            } else {
                actor
                    .store
                    .delete_execution(snapshot.execution.execution_id)
                    .await?;
            }
        }
        actor.terminal_executions = durable_snapshots
            .iter()
            .filter(|snapshot| snapshot.execution.status.is_terminal())
            .map(|snapshot| snapshot.execution.clone())
            .collect();
        for snapshot in durable_snapshots
            .iter()
            .filter(|snapshot| !snapshot.execution.status.is_terminal())
        {
            if !actor
                .mobility
                .permits_local_work(snapshot.execution.flow_id)
            {
                continue;
            }
            match actor.restore_execution(snapshot).await {
                Ok((id, state)) => {
                    actor.execution.insert(id, state);
                }
                Err(FlowError::UnregisteredNodeKind(kind)) => {
                    let mut failed = snapshot.clone();
                    failed.execution.status = FlowExecutionStatus::Failed;
                    failed.execution.completed_at = Some(FlowTimestamp::now());
                    failed.execution.error = Some(FlowError::UnregisteredNodeKind(kind).to_string());
                    actor.snapshot_store.save(&failed).await?;
                    actor.terminal_executions.push(failed.execution);
                }
                Err(error) => return Err(error),
            }
        }

        for execution_id in actor.execution.keys().copied().collect::<Vec<_>>() {
            actor.mcp_ports.start(execution_id).await;
        }
        if let Err(error) = actor.prune_terminal_executions().await {
            tracing::warn!(%error, "Flow terminal retention cleanup will retry after construction");
        }

        Ok(actor)
    }

    async fn handle_message(&mut self, request: FlowRequest) {
        let FlowRequest { payload, reply_to } = request;

        if let FlowRequestPayload::Start {
            flow_id,
            pulse,
            automatic,
            invocation_kind,
        } = payload
        {
            match self
                .begin_start(flow_id, None, pulse, automatic, invocation_kind)
                .await
            {
                Ok(execution_id) => {
                    if let Some(state) = self.execution.get_mut(&execution_id) {
                        state.pending_start_reply = Some(reply_to);
                    }
                }
                Err(error) => {
                    let _ = reply_to.send(Err(error));
                }
            }
            return;
        }

        let triggers_changed = matches!(
            &payload,
            FlowRequestPayload::StartRuntime
                | FlowRequestPayload::StopRuntime
                | FlowRequestPayload::CreateFlowTrigger { .. }
                | FlowRequestPayload::UpdateFlowTrigger { .. }
                | FlowRequestPayload::DeleteFlowTrigger { .. }
                | FlowRequestPayload::SetEnabled { .. }
                | FlowRequestPayload::UpdateFlow { .. }
                | FlowRequestPayload::DeleteFlow { .. }
                | FlowRequestPayload::ImportMobilitySnapshot { .. }
                | FlowRequestPayload::YieldFlowAuthority { .. }
                | FlowRequestPayload::PromoteFlowReplica { .. }
                | FlowRequestPayload::FinalizeFlowTransfer { .. }
                | FlowRequestPayload::SynchronizeFlow { .. }
        );

        let result = match payload {
            FlowRequestPayload::ListSourceTriggers { adapter_kind } => Ok(FlowResponse::SourceTriggersListed {
                triggers: self
                    .triggers
                    .values()
                    .filter(|trigger| {
                        self.running
                            && trigger.enabled
                            && trigger.adapter_kind == adapter_kind
                            && self.mobility.permits_local_work(trigger.flow_id)
                            && self
                                .definitions
                                .get(&trigger.flow_id)
                                .is_some_and(|flow| flow.automatic_enabled)
                    })
                    .cloned()
                    .collect(),
            }),
            FlowRequestPayload::FireSourceTrigger { trigger_id, pulse } => {
                match self.triggers.get(&trigger_id).cloned() {
                    Some(trigger) if trigger.enabled && trigger.adapter_kind != "cron" => self
                        .begin_start(
                            trigger.flow_id,
                            Some(trigger.revision),
                            pulse,
                            true,
                            FlowInvocationKind::Event,
                        )
                        .await
                        .map(|execution_id| FlowResponse::Started { execution_id }),
                    _ => Err(FlowError::TriggerNotFound),
                }
            }
            FlowRequestPayload::StartRuntime => self.start_runtime().await,
            FlowRequestPayload::StopRuntime => {
                let result = self.shutdown_runtime().await;
                if result.is_ok() {
                    maintain_database(&self.store.database(), &self.database_path).await;
                }
                result
            }
            FlowRequestPayload::CreateFlow { flow } => self.create_flow(flow).await,
            FlowRequestPayload::ImportWorkspace { source_path } => self.import_workspace(source_path).await,
            FlowRequestPayload::DownloadWorkspaceBlob { ticket } => self.download_workspace_blob(ticket).await,
            FlowRequestPayload::ReadScriptSource { location, path } => self.read_script_source(location, path).await,
            FlowRequestPayload::SaveScriptSource { location, path, source } => {
                self.save_script_source(location, path, source).await
            }
            FlowRequestPayload::CreateFlowTrigger { trigger } => self.create_trigger(trigger).await,
            FlowRequestPayload::CreateFlowNode { flow_id, node } => self.create_node(flow_id, node).await,
            FlowRequestPayload::UpdateFlow { flow } => self.update_flow(flow).await,
            FlowRequestPayload::SetEnabled { flow_id, enabled } => self.set_flow_enabled(flow_id, enabled).await,
            FlowRequestPayload::UpdateFlowTrigger { trigger } => self.update_trigger(trigger).await,
            FlowRequestPayload::UpdateFlowNode { flow_id, node } => self.update_node(flow_id, node).await,
            FlowRequestPayload::DeleteFlow { flow_id } => self.delete_flow(flow_id).await,
            FlowRequestPayload::DeleteFlowTrigger { trigger_id } => self.delete_trigger(trigger_id).await,
            FlowRequestPayload::DeleteFlowNode { flow_id, node_id } => self.delete_node(flow_id, node_id).await,
            FlowRequestPayload::GetFlow { flow_id } => self.get_flow(flow_id),
            FlowRequestPayload::ListFlows => Ok(FlowResponse::FlowsListed {
                flows: self.definitions.values().cloned().collect(),
            }),
            FlowRequestPayload::ListFlowTriggers { flow_id } => self.list_triggers(flow_id).await,
            FlowRequestPayload::ListActiveExecutions => Ok(FlowResponse::ActiveExecutionsListed {
                executions: self.active_snapshots(),
            }),
            FlowRequestPayload::ListTerminalExecutions => Ok(FlowResponse::TerminalExecutionsListed {
                executions: self.terminal_executions.clone(),
            }),
            FlowRequestPayload::GetExecutionDetail { execution_id } => {
                self.get_flow_execution_state(execution_id).await
            }
            FlowRequestPayload::Start { .. } => unreachable!("start handled before request dispatch"),
            FlowRequestPayload::Abort { execution_id } => self.abort(execution_id).await,
            FlowRequestPayload::SelectMobilityDestination { flow_id, destinations } => {
                self.select_mobility_destination(flow_id, destinations)
                    .await
            }
            FlowRequestPayload::SynchronizeFlow { flow_id } => self.synchronize_flow(flow_id).await,
            FlowRequestPayload::ImportMobilitySnapshot {
                snapshot,
                source_peer_key,
            } => {
                self.import_selected_flow_native_content(snapshot, source_peer_key)
                    .await
            }
            FlowRequestPayload::GetFlowMobilityReadiness { flow_id } => self.mobility_readiness_response(flow_id).await,
            FlowRequestPayload::YieldFlowAuthority { flow_id } => self.yield_flow_authority(flow_id).await,
            FlowRequestPayload::PromoteFlowReplica {
                flow_id,
                acknowledgement,
            } => self.promote_flow_replica(flow_id, acknowledgement).await,
            FlowRequestPayload::FinalizeFlowTransfer { flow_id } => self.finalize_flow_transfer(flow_id).await,
            FlowRequestPayload::UpdateRetention {
                retention_completed_runs,
            } => self.apply_retention_update(retention_completed_runs).await,
        };

        if triggers_changed {
            let _ = self.broadcast_tx.send(FlowBroadcast::TriggersChanged).await;
        }
        let _ = reply_to.send(result);
    }

    async fn start_runtime(&mut self) -> Result<FlowResponse, FlowError> {
        if self.running {
            return Ok(FlowResponse::RuntimeStarted);
        }

        self.peer_sync_available = self.sync_services.get().is_some();

        let execution_ids = self.execution.keys().copied().collect::<Vec<_>>();
        let mut recovered_dispatches = Vec::new();

        for execution_id in execution_ids {
            if self
                .execution
                .get(&execution_id)
                .is_some_and(|state| state.abort_pending)
            {
                self.finalize_abort(execution_id).await?;
                continue;
            }
            let tick = self
                .execution
                .get_mut(&execution_id)
                .ok_or(FlowError::ExecutionNotFound)?
                .tick_loop
                .tick()?;
            if let Some(terminal) = tick.terminal.clone() {
                self.persist_recovered_terminal(execution_id, tick, terminal)
                    .await?;
            } else {
                recovered_dispatches.extend(tick.dispatches);
            }
        }

        self.spawn_dispatches(recovered_dispatches);
        self.initialize_trigger_cursors().await?;
        self.running = true;

        Ok(FlowResponse::RuntimeStarted)
    }

    async fn initialize_trigger_cursors(&mut self) -> Result<(), FlowError> {
        let now = Utc::now();
        let trigger_ids = self.triggers.keys().copied().collect::<Vec<_>>();
        for trigger_id in trigger_ids {
            let Some(trigger) = self.triggers.get(&trigger_id).cloned() else {
                continue;
            };
            if !self.mobility.permits_local_work(trigger.flow_id) {
                continue;
            }
            let Some(configuration) = trigger.cron_config()? else {
                continue;
            };
            if trigger.enabled && trigger.next_fire_at.is_none() {
                let mut updated = trigger;
                updated.next_fire_at = Some(configuration.cron.next_after(&now)?);
                self.store.update_trigger(&updated).await?;
                self.triggers.insert(trigger_id, updated);
            }
        }
        Ok(())
    }

    async fn fire_due_triggers(&mut self) -> Vec<(Uuid, FlowError)> {
        if !self.running {
            return Vec::new();
        }
        let mut errors = Vec::new();
        let now = Utc::now();
        let due = self
            .triggers
            .values()
            .filter(|trigger| {
                trigger.adapter_kind == "cron"
                    && trigger.enabled
                    && self.mobility.permits_local_work(trigger.flow_id)
                    && trigger
                        .next_fire_at
                        .as_ref()
                        .is_some_and(|next| next.as_datetime() <= &now)
            })
            .cloned()
            .collect::<Vec<_>>();
        for mut trigger in due {
            trigger.last_fired_at = Some(FlowTimestamp::from_datetime(now));
            let configuration = match trigger.cron_config() {
                Ok(Some(configuration)) => configuration,
                Ok(None) => continue,
                Err(error) => {
                    errors.push((trigger.trigger_id, error));
                    continue;
                }
            };
            match configuration.cron.next_after(&now) {
                Ok(next) => trigger.next_fire_at = Some(next),
                Err(error) => {
                    tracing::error!(trigger_id = %trigger.trigger_id, error = %error, "Flow trigger advance failed");
                    errors.push((trigger.trigger_id, error.into()));
                    continue;
                }
            }
            if let Err(error) = self.store.update_trigger(&trigger).await {
                tracing::error!(trigger_id = %trigger.trigger_id, error = %error, "Flow trigger cursor persistence failed");
                errors.push((trigger.trigger_id, error));
                continue;
            }
            self.triggers.insert(trigger.trigger_id, trigger.clone());
            // Every persisted cursor advance is observable, including skipped and failed starts.
            let _ = self.broadcast_tx.send(FlowBroadcast::TriggersChanged).await;
            if self.has_active_execution(trigger.flow_id) {
                continue;
            }
            let pulse = json!({
                "triggerId": trigger.trigger_id,
                "adapterKind": trigger.adapter_kind,
                "firedAt": now.to_rfc3339(),
            });
            if let Err(error) = self
                .begin_start(
                    trigger.flow_id,
                    Some(trigger.revision),
                    pulse,
                    true,
                    FlowInvocationKind::Schedule,
                )
                .await
            {
                tracing::error!(trigger_id = %trigger.trigger_id, error = %error, "Flow trigger start failed");
                errors.push((trigger.trigger_id, error));
            }
        }
        errors
    }

    pub(super) fn has_active_execution(&self, flow_id: FlowId) -> bool {
        self.execution
            .values()
            .any(|state| state.active.flow_id() == flow_id)
    }

    async fn begin_start(
        &mut self,
        flow_id: FlowId,
        revision: Option<i64>,
        pulse: Value,
        automatic: bool,
        invocation_kind: FlowInvocationKind,
    ) -> Result<FlowExecutionId, FlowError> {
        self.admit_start(flow_id, automatic)?;
        let definition = self.start_definition(flow_id, revision).await?;
        let graph = FlowGraph::load_json(&to_string(&definition.graph)?)?;
        graph.ast().validate_with_registry(&self.node_registry)?;
        validate_parallel_composition(&graph, &self.node_registry)?;
        let mut blackboard = Blackboard::new(from_value(definition.blackboard_defaults.clone())?);
        if !pulse.is_null() {
            blackboard.set("pulse", pulse.clone());
        }
        let execution_id = FlowExecutionId(Uuid::new_v4());
        let mut context = FlowAppHookContext {
            execution_id,
            flow_id: definition.flow_id,
            definition_revision: definition.revision,
            execution_root: PathBuf::new(),
            invocation_kind,
        };
        context.execution_root = self.workspace_hook.execution_root(context.clone()).await?;
        let prepared = self
            .workspace_hook
            .takeoff(FlowAppTakeoffContext {
                execution: context,
                definition: definition.clone(),
                pulse,
            })
            .await
            .map_err(|source| FlowError::AppWorkspaceTakeoff { source })?;
        let execution_root = prepared.execution_root;
        let imported_workspace_root = prepared.imported_workspace_root;
        let workspace = if self.application_workspace {
            WorkspaceProvenance::Application {
                root: execution_root.clone(),
            }
        } else {
            WorkspaceProvenance::Native {
                root: execution_root.clone(),
            }
        };
        let mut tick_loop = FlowTickLoop::compile(
            &graph,
            &self.node_registry,
            flow_id,
            execution_id,
            blackboard,
            execution_root.clone(),
        )?;
        let first_tick = tick_loop.tick()?;
        for dispatch in &first_tick.dispatches {
            self.lifecycle_adapter
                .ensure_available(&self.node_registry, &dispatch.message.correlation.node_kind)?;
        }
        let (code_roots, state_roots) = content_roots(&graph, &self.node_registry)?;
        self.commit_start(
            execution_id,
            definition,
            tick_loop,
            first_tick,
            code_roots,
            state_roots,
            imported_workspace_root,
            execution_root,
            graph_nodes(&graph.ast().root),
            workspace,
            invocation_kind,
        )
        .await?;
        Ok(execution_id)
    }

    fn admit_start(&self, flow_id: FlowId, automatic: bool) -> Result<(), FlowError> {
        if !self.running {
            return Err(FlowError::RuntimeNotStarted);
        }
        if !self.definitions.contains_key(&flow_id) {
            return Err(FlowError::DefinitionNotFound);
        }
        self.ensure_local_work(flow_id)?;
        if automatic
            && !self
                .definitions
                .get(&flow_id)
                .ok_or(FlowError::DefinitionNotFound)?
                .automatic_enabled
        {
            return Err(FlowError::AutomaticStartDisabled { flow_id });
        }
        Ok(())
    }

    async fn start_definition(&self, flow_id: FlowId, revision: Option<i64>) -> Result<FlowDefinition, FlowError> {
        if let Some(revision) = revision {
            return self.store.load_definition_revision(flow_id, revision).await;
        }
        self.definitions
            .get(&flow_id)
            .cloned()
            .ok_or(FlowError::DefinitionNotFound)
    }

    async fn import_workspace(&mut self, source_path: String) -> Result<FlowResponse, FlowError> {
        let root = self
            .workspace_hook
            .import_path(PathBuf::from(source_path))
            .await?;
        Ok(FlowResponse::WorkspaceImported { root })
    }

    async fn download_workspace_blob(&mut self, ticket: String) -> Result<FlowResponse, FlowError> {
        self.require_peer_service()?;
        let root = self
            .workspace_hook
            .download_collection_ticket(ticket)
            .await?;
        Ok(FlowResponse::WorkspaceImported { root })
    }

    pub(super) fn require_peer_service(&self) -> Result<(), WorkspaceError> {
        if self.peer_sync_available {
            Ok(())
        } else {
            Err(WorkspaceError::ProviderUnavailable)
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_start(
        &mut self,
        execution_id: FlowExecutionId,
        definition: FlowDefinition,
        tick_loop: FlowTickLoop,
        first_tick: TickResult,
        code_roots: Vec<ContentAddressedRoot>,
        state_roots: Vec<ContentAddressedRoot>,
        imported_workspace_root: Option<ContentAddressedRoot>,
        execution_root: PathBuf,
        node_envelopes: HashMap<super::types::FlowNodeId, FlowNode>,
        workspace: WorkspaceProvenance,
        invocation_kind: FlowInvocationKind,
    ) -> Result<(), FlowError> {
        let pending_dispatches = first_tick.dispatches;
        let reconstruction = first_tick.reconstruction;
        let started_at = FlowTimestamp::now();
        let blackboard_snapshot = tick_loop.blackboard().values().clone();
        let scratchpad = tick_loop.scratchpad_snapshot();
        let nodes = tick_loop.node_snapshots();
        let context = FlowAppHookContext {
            execution_id,
            flow_id: definition.flow_id,
            definition_revision: definition.revision,
            execution_root,
            invocation_kind,
        };
        let checkpoint = self
            .workspace_hook
            .checkpoint(FlowAppCheckpointContext {
                execution: context.clone(),
                previous_entries: Default::default(),
                sequence: 0,
            })
            .await
            .map_err(|source| FlowError::AppWorkspaceCheckpoint { source })?;
        let FlowAppCheckpointResult {
            workspace_checkpoint,
            workspace_entries,
        } = checkpoint;
        let snapshot = FlowExecutionSnapshot {
            execution_id,
            flow_id: definition.flow_id,
            revision: definition.revision,
            invocation_kind,
            status: FlowExecutionStatus::Active,
            started_at: started_at.clone(),
            completed_at: None,
            error: None,
        };
        self.snapshot_store
            .save(&ExecutionSnapshot {
                execution: snapshot.clone(),
                blackboard: blackboard_snapshot,
                code_roots: code_roots.clone(),
                state_roots: state_roots.clone(),
                workspace_checkpoint: workspace_checkpoint.clone(),
                workspace_entries: workspace_entries.clone(),
                imported_workspace_root: imported_workspace_root.clone(),
                workspace: workspace.clone(),
                reconstruction,
                scratchpad,
                nodes,
            })
            .await?;
        if let Err(source) = self
            .workspace_hook
            .checkpoint_committed(FlowAppCheckpointCommittedContext {
                execution: context,
                sequence: 0,
                superseded_sequence: None,
            })
            .await
        {
            self.store.delete_execution(execution_id).await?;
            return Err(FlowError::AppWorkspaceCheckpoint { source });
        }
        self.execution.insert(
            execution_id,
            ExecutionState {
                active: ActiveExecution::new(definition.flow_id, definition.revision, started_at, invocation_kind),
                nodes: node_envelopes,
                tick_loop,
                pending_start_reply: None,
                pending_terminal_persistence: HashMap::new(),
                prepared_checkpoint_commit: None,
                writer_active: HashMap::new(),
                code_roots,
                state_roots,
                workspace_entries,
                workspace_checkpoint,
                imported_workspace_root: imported_workspace_root.clone(),
                workspace,
                workspace_mappings: definition.workspace_mappings,
                checkpoint_sequence: 0,
                abort_pending: false,
                dispatch_tasks: Vec::new(),
                dispatch_in_flight: HashSet::new(),
            },
        );
        self.mcp_ports.start(execution_id).await;
        let _ = self
            .broadcast_tx
            .send(FlowBroadcast::ExecutionStatusChanged {
                flow_name: self
                    .definitions
                    .get(&snapshot.flow_id)
                    .map_or_else(|| "Flow".to_owned(), |flow| flow.name.clone()),
                snapshot,
            })
            .await;
        for dispatch in &pending_dispatches {
            let _ = self
                .broadcast_tx
                .send(FlowBroadcast::Ready {
                    correlation: dispatch.correlation.clone(),
                })
                .await;
        }
        self.spawn_dispatches(pending_dispatches);
        Ok(())
    }

    async fn handle_start_completion(&mut self, mut completion: DispatchCompletion) -> Result<(), FlowError> {
        let execution_id = completion.correlation.flow_execution_id;
        if !self
            .execution
            .get(&execution_id)
            .is_some_and(|state| state.tick_loop.accepts(completion.correlation.clone()))
        {
            return Ok(());
        }
        let owner_kind = completion.node_kind.clone();
        match std::mem::replace(&mut completion.result, Err(FlowAdapterError::Cancelled)) {
            Ok(OwnerLifecycleResult::Running { correlation, token_jti }) => {
                if let Err(error) = self.advance_accepted_start(correlation, token_jti).await {
                    self.reply_start_error(execution_id, error);
                    return Ok(());
                }
                self.acknowledge_started(execution_id).await;
                Ok(())
            }
            Ok(result @ OwnerLifecycleResult::Succeeded { .. }) => {
                completion.result = Ok(result);
                completion.kind = FlowDispatchKind::Tick;
                completion.token_expired = false;
                self.acknowledge_started(execution_id).await;
                self.handle_node_result(completion).await
            }
            Ok(result @ OwnerLifecycleResult::Failed { .. }) => {
                let OwnerLifecycleResult::Failed { message, .. } = &result else {
                    unreachable!()
                };
                let message = message.clone();
                completion.result = Ok(result);
                completion.kind = FlowDispatchKind::Tick;
                completion.token_expired = false;
                self.handle_node_result(completion).await?;
                self.reply_start_error(
                    execution_id,
                    FlowError::OwnerStartRejected {
                        kind: owner_kind,
                        message,
                    },
                );
                Ok(())
            }
            Err(error) => {
                self.reply_start_error(execution_id, FlowError::Adapter(error));
                Ok(())
            }
        }
    }

    async fn acknowledge_started(&mut self, execution_id: FlowExecutionId) {
        let flow_name = self
            .execution
            .get(&execution_id)
            .and_then(|state| self.definitions.get(&state.active.flow_id))
            .map_or_else(|| "Flow".to_owned(), |flow| flow.name.clone());
        if let Some(reply) = self
            .execution
            .get_mut(&execution_id)
            .and_then(|state| state.pending_start_reply.take())
        {
            let _ = reply.send(Ok(FlowResponse::Started { execution_id }));
        }
        let _ = self
            .broadcast_tx
            .send(FlowBroadcast::Started {
                execution_id,
                flow_name,
            })
            .await;
    }

    fn reply_start_error(&mut self, execution_id: FlowExecutionId, error: FlowError) {
        if let Some(reply) = self
            .execution
            .get_mut(&execution_id)
            .and_then(|state| state.pending_start_reply.take())
        {
            let _ = reply.send(Err(error));
        }
    }

    async fn advance_accepted_start(
        &mut self,
        correlation: LifecycleCorrelation,
        token_jti: Option<Uuid>,
    ) -> Result<(), FlowError> {
        let execution_id = correlation.flow_execution_id;
        let state = self
            .execution
            .get_mut(&execution_id)
            .ok_or(FlowError::ExecutionNotFound)?;
        if !state.tick_loop.accepts(correlation.clone()) {
            return Ok(());
        }

        let node_execution_id = correlation.node_execution_id;
        let tick = state.tick_loop.tick_with_admission(!state.abort_pending)?;
        let dispatches = tick.dispatches;
        let durable = state.snapshot(execution_id, FlowExecutionStatus::Active, tick.reconstruction, None);

        let snapshot = durable.execution.clone();
        self.snapshot_store.save(&durable).await?;
        if let Some(token_jti) = token_jti {
            self.node_tokens.insert(node_execution_id, token_jti);
        }
        self.spawn_dispatches(dispatches);
        if self
            .execution
            .get(&execution_id)
            .is_some_and(|state| state.abort_pending)
        {
            Box::pin(self.request_owner_cancel(execution_id)).await?;
        }

        let _ = self
            .broadcast_tx
            .send(FlowBroadcast::ExecutionStatusChanged {
                flow_name: self
                    .definitions
                    .get(&snapshot.flow_id)
                    .map_or_else(|| "Flow".to_owned(), |flow| flow.name.clone()),
                snapshot,
            })
            .await;

        Ok(())
    }

    async fn get_flow_execution_state(&mut self, execution_id: FlowExecutionId) -> Result<FlowResponse, FlowError> {
        let mut snapshot = if let Some(state) = self.execution.get(&execution_id) {
            state.snapshot(
                execution_id,
                FlowExecutionStatus::Active,
                state.tick_loop.reconstruction_state(),
                None,
            )
        } else {
            self.snapshot_store
                .load_all()
                .await?
                .into_iter()
                .find(|snapshot| snapshot.execution.execution_id == execution_id)
                .ok_or(FlowError::ExecutionNotFound)?
        };
        self.workspace_hook
            .project_artifacts(execution_id, &mut snapshot.nodes)
            .await?;
        Ok(FlowResponse::ExecutionDetailRetrieved {
            detail: self.store.assemble_execution_detail(snapshot).await?,
        })
    }

    async fn restore_execution(
        &self,
        snapshot: &ExecutionSnapshot,
    ) -> Result<(FlowExecutionId, ExecutionState), FlowError> {
        let id = snapshot.execution.execution_id;
        if snapshot.workspace.is_application() != self.application_workspace {
            return Err(FlowError::AppWorkspaceUnavailable { execution_id: id });
        }
        let definition = self
            .store
            .load_definition_revision(snapshot.execution.flow_id, snapshot.execution.revision)
            .await?;
        let graph = FlowGraph::load_json(&to_string(&definition.graph)?)?;
        graph.ast().validate_with_registry(&self.node_registry)?;
        let root = self
            .workspace_hook
            .restore(FlowAppRestoreContext {
                execution: FlowAppHookContext {
                    execution_id: id,
                    flow_id: snapshot.execution.flow_id,
                    definition_revision: snapshot.execution.revision,
                    execution_root: snapshot.workspace.root(),
                    invocation_kind: snapshot.execution.invocation_kind,
                },
                definition: definition.clone(),
                workspace_checkpoint: snapshot.workspace_checkpoint.clone(),
                workspace_entries: snapshot.workspace_entries.clone(),
                imported_workspace_root: snapshot.imported_workspace_root.clone(),
            })
            .await
            .map_err(|source| FlowError::AppWorkspaceRestore { source })?;
        let workspace = if snapshot.workspace.is_application() {
            WorkspaceProvenance::Application { root: root.clone() }
        } else {
            WorkspaceProvenance::Native { root: root.clone() }
        };
        let mut restored = snapshot.clone();
        restored.workspace = workspace.clone();
        self.snapshot_store.save(&restored).await?;
        Ok((
            id,
            ExecutionState {
                active: ActiveExecution::new(
                    snapshot.execution.flow_id,
                    snapshot.execution.revision,
                    snapshot.execution.started_at.clone(),
                    snapshot.execution.invocation_kind,
                ),
                nodes: graph_nodes(&graph.ast().root),
                tick_loop: FlowTickLoop::restore(&graph, &self.node_registry, snapshot, root)?,
                pending_start_reply: None,
                pending_terminal_persistence: HashMap::new(),
                prepared_checkpoint_commit: None,
                writer_active: HashMap::new(),
                code_roots: snapshot.code_roots.clone(),
                state_roots: snapshot.state_roots.clone(),
                workspace_entries: snapshot.workspace_entries.clone(),
                workspace_checkpoint: snapshot.workspace_checkpoint.clone(),
                imported_workspace_root: snapshot.imported_workspace_root.clone(),
                workspace,
                workspace_mappings: definition.workspace_mappings,
                checkpoint_sequence: 0,
                abort_pending: snapshot.execution.status == FlowExecutionStatus::Aborting,
                dispatch_tasks: Vec::new(),
                dispatch_in_flight: HashSet::new(),
            },
        ))
    }

    async fn tick_active_executions(&mut self) {
        if !self.running {
            return;
        }
        let execution_ids = self.execution.keys().copied().collect::<Vec<_>>();
        for execution_id in execution_ids {
            let Some(state) = self.execution.get_mut(&execution_id) else {
                continue;
            };
            if state.prepared_checkpoint_commit.is_some() || !state.pending_terminal_persistence.is_empty() {
                continue;
            }
            let aborting = state.abort_pending;
            match state.tick_loop.tick_with_admission(!aborting) {
                Ok(tick) if !aborting && (tick.status != FlowTreeStatus::Running || tick.terminal.is_some()) => {
                    if let Err(error) = self.prepare_execution_transition(execution_id, tick).await {
                        tracing::error!(%execution_id, %error, "Flow scheduled tick persistence failed");
                    }
                }
                Ok(tick) => self.spawn_dispatches(tick.dispatches),
                Err(error) => tracing::error!(%execution_id, %error, "Flow scheduled tick failed"),
            }
        }
    }

    fn active_snapshots(&self) -> Vec<FlowExecutionSnapshot> {
        self.execution
            .iter()
            .map(|(execution_id, state)| FlowExecutionSnapshot {
                execution_id: *execution_id,
                flow_id: state.active.flow_id(),
                revision: state.active.revision(),
                invocation_kind: state.active.invocation_kind(),
                status: if state.abort_pending {
                    FlowExecutionStatus::Aborting
                } else {
                    FlowExecutionStatus::Active
                },
                started_at: state.active.started_at(),
                completed_at: None,
                error: None,
            })
            .collect()
    }

    async fn handle_node_result(&mut self, completion: DispatchCompletion) -> Result<(), FlowError> {
        let execution_id = completion.correlation.flow_execution_id;

        let (abort_pending, terminal_ready) = {
            let Some(state) = self.execution.get_mut(&execution_id) else {
                return Ok(());
            };
            if !state.tick_loop.accepts(completion.correlation.clone()) {
                return Ok(());
            }
            if completion.token_expired {
                state.tick_loop.submit_expired(
                    TokenExpired {
                        correlation: completion.correlation,
                    },
                    NODE_TOKEN_EXPIRED_MESSAGE.to_owned(),
                );
                return Ok(());
            }
            let mut failure_message = None;
            let terminal = match completion.result {
                Ok(OwnerLifecycleResult::Running { .. }) => {
                    return Ok(());
                }
                Ok(OwnerLifecycleResult::Succeeded {
                    correlation,
                    outputs,
                    state,
                }) => TerminalResult {
                    correlation,
                    status: TerminalStatus::Success,
                    outputs,
                    state,
                    error: None,
                },
                Ok(OwnerLifecycleResult::Failed {
                    correlation,
                    message,
                    state,
                }) => {
                    failure_message = Some(message);
                    TerminalResult {
                        correlation,
                        status: TerminalStatus::Failure,
                        outputs: BoundOutputs::new(Default::default()),
                        state,
                        error: failure_message.clone(),
                    }
                }
                Err(FlowAdapterError::Cancelled) => return Ok(()),
                Err(error) => {
                    let message = error.to_string();
                    failure_message = Some(message.clone());
                    TerminalResult {
                        correlation: completion.correlation.clone(),
                        status: TerminalStatus::Failure,
                        outputs: BoundOutputs::new(Default::default()),
                        state: Value::Null,
                        error: Some(message),
                    }
                }
            };
            state.tick_loop.submit(MailboxMessage::Terminal(terminal));
            state
                .writer_active
                .remove(&completion.correlation.node_execution_id);
            state.pending_terminal_persistence.insert(
                completion.correlation.node_execution_id,
                PendingTerminalPersistence {
                    correlation: completion.correlation.clone(),
                    failure_message,
                },
            );
            let abort_pending = state.abort_pending;
            if abort_pending {
                state.pending_terminal_persistence.clear();
            }
            (abort_pending, state.writer_active.is_empty())
        };

        if abort_pending {
            if terminal_ready {
                self.finalize_abort(execution_id).await?;
            }
            return Ok(());
        }

        if terminal_ready {
            self.advance_after_node_result(execution_id).await
        } else {
            self.persist_active_snapshot(execution_id).await
        }
    }

    async fn handle_dispatch_result(&mut self, completion: DispatchCompletion) -> Result<(), FlowError> {
        if !completion.token_expired
            && let Some(state) = self
                .execution
                .get_mut(&completion.correlation.flow_execution_id)
        {
            state
                .dispatch_in_flight
                .remove(&completion.correlation.node_execution_id);
        }
        if completion.kind == FlowDispatchKind::Start {
            return self.handle_start_completion(completion).await;
        }
        self.handle_node_result(completion).await
    }

    async fn abort(&mut self, execution_id: FlowExecutionId) -> Result<FlowResponse, FlowError> {
        let state = self
            .execution
            .get_mut(&execution_id)
            .ok_or(FlowError::ExecutionNotFound)?;
        if state.abort_pending {
            return Ok(FlowResponse::Aborted { execution_id });
        }
        state.abort_pending = true;
        state.tick_loop.discard_pending();
        self.request_owner_cancel(execution_id).await?;
        Ok(FlowResponse::Aborted { execution_id })
    }

    pub(super) async fn request_owner_cancel(&mut self, execution_id: FlowExecutionId) -> Result<(), FlowError> {
        let Some(state) = self.execution.get(&execution_id) else {
            return Ok(());
        };
        let aborting = state.abort_pending;

        let cancellations = state
            .writer_active
            .values()
            .cloned()
            .map(|correlation| {
                let node = state.nodes.get(&correlation.flow_node_id).cloned();
                (correlation, node)
            })
            .collect::<Vec<_>>();
        if cancellations.is_empty() {
            if aborting {
                self.finalize_abort(execution_id).await?;
            }
            return Ok(());
        }

        if aborting && let Err(error) = self.persist_active_snapshot(execution_id).await {
            tracing::warn!(%execution_id, %error, "Failed to persist active snapshot");
        }

        for (cancel_target, node) in cancellations {
            let Some(node) = node else {
                let _ = self
                    .completion_tx
                    .send(DispatchCompletion {
                        correlation: cancel_target.clone(),
                        node_kind: cancel_target.node_kind.clone(),
                        result: Err(FlowAdapterError::CancelTargetMissing {
                            flow_node_id: cancel_target.flow_node_id,
                        }),
                        token_expired: false,
                        kind: FlowDispatchKind::Tick,
                    })
                    .await;
                continue;
            };
            let registry = self.node_registry.clone();
            let adapter = self.lifecycle_adapter.clone();
            let completion_tx = self.completion_tx.clone();
            let execution_root = self
                .execution
                .get(&execution_id)
                .ok_or(FlowError::ExecutionNotFound)?
                .workspace
                .root();

            self.background_tasks.push(tokio::spawn(async move {
                let message = ErasedLifecycleMessage {
                    correlation: cancel_target.clone(),
                    node,
                    execution_root,
                    node_token: None,
                    payload: Box::new(()),
                };
                let result = dispatch_cancel(&registry, &adapter, message).await;
                let _ = completion_tx
                    .send(DispatchCompletion {
                        correlation: cancel_target.clone(),
                        node_kind: cancel_target.node_kind.clone(),
                        result,
                        token_expired: false,
                        kind: FlowDispatchKind::Tick,
                    })
                    .await;
            }));
        }

        Ok(())
    }

    async fn finalize_abort(&mut self, execution_id: FlowExecutionId) -> Result<(), FlowError> {
        let correlations = self
            .execution
            .get(&execution_id)
            .map(|state| {
                state
                    .pending_terminal_persistence
                    .values()
                    .map(|pending| pending.correlation.clone())
                    .collect()
            })
            .unwrap_or_default();
        let mut durable = {
            let state = self
                .execution
                .get_mut(&execution_id)
                .ok_or(FlowError::ExecutionNotFound)?;
            state.pending_terminal_persistence.clear();
            state.prepared_checkpoint_commit = None;
            for task in state.dispatch_tasks.drain(..) {
                task.abort();
            }
            state.writer_active.clear();
            if let Some(reply) = state.pending_start_reply.take() {
                let _ = reply.send(Err(FlowError::ExecutionAborted));
            }
            state.tick_loop.mark_pending_aborted();
            state.snapshot(
                execution_id,
                FlowExecutionStatus::Aborted,
                state.tick_loop.reconstruction_state(),
                None,
            )
        };
        durable.scratchpad = ScratchpadSnapshot::default();

        let snapshot = durable.execution.clone();
        self.snapshot_store.save(&durable).await?;

        self.land_execution_best_effort(&snapshot, correlations)
            .await;

        self.terminal_executions.push(snapshot.clone());
        self.revoke_execution_tokens_best_effort(execution_id).await;
        self.mcp_ports.finish(execution_id).await;
        self.execution.remove(&execution_id);
        self.prune_terminal_executions().await?;

        let _ = self
            .broadcast_tx
            .send(FlowBroadcast::ExecutionStatusChanged {
                flow_name: self
                    .definitions
                    .get(&snapshot.flow_id)
                    .map_or_else(|| "Flow".to_owned(), |flow| flow.name.clone()),
                snapshot,
            })
            .await;
        let _ = self
            .broadcast_tx
            .send(FlowBroadcast::Aborted { execution_id })
            .await;

        Ok(())
    }

    pub(super) async fn revoke_execution_tokens_best_effort(&mut self, execution_id: FlowExecutionId) {
        if let Err(error) = self.revoke_execution_tokens(execution_id).await {
            tracing::warn!(%execution_id, %error, "Flow terminal token cleanup failed after durable terminalization");
        }
    }

    pub(super) async fn land_execution_best_effort(
        &self,
        snapshot: &FlowExecutionSnapshot,
        correlations: Vec<LifecycleCorrelation>,
    ) {
        let Some(state) = self.execution.get(&snapshot.execution_id) else {
            return;
        };
        let execution_root = state.workspace.root();
        let context = FlowAppLandingContext {
            execution: FlowAppHookContext {
                execution_id: snapshot.execution_id,
                flow_id: snapshot.flow_id,
                definition_revision: snapshot.revision,
                execution_root,
                invocation_kind: snapshot.invocation_kind,
            },
            status: snapshot.status.clone(),
            correlations,
            workspace_mappings: state.workspace_mappings.clone(),
            imported_workspace_root: state.imported_workspace_root.clone(),
        };
        if let Err(error) = self.workspace_hook.landing(context).await {
            tracing::warn!(execution_id = %snapshot.execution_id, %error, "Flow application workspace landing failed after durable terminalization");
        }
    }

    async fn revoke_execution_tokens(&mut self, execution_id: FlowExecutionId) -> Result<(), FlowError> {
        let node_execution_ids = self
            .execution
            .get(&execution_id)
            .map(|state| state.tick_loop.node_snapshots())
            .unwrap_or_default()
            .into_iter()
            .map(|node| node.node_execution_id)
            .collect::<Vec<_>>();
        for node_execution_id in node_execution_ids {
            if let Some(jti) = self.node_tokens.get(&node_execution_id).copied() {
                self.lifecycle_adapter.revoke_token(jti).await?;
                self.node_tokens.remove(&node_execution_id);
            }
        }
        Ok(())
    }

    pub(super) fn spawn_dispatches(&mut self, dispatches: Vec<FlowDispatch>) {
        for dispatch in dispatches {
            let correlation = dispatch.correlation.clone();
            let Some(state) = self.execution.get_mut(&correlation.flow_execution_id) else {
                continue;
            };
            if (state.abort_pending && dispatch.kind == FlowDispatchKind::Start)
                || !state
                    .dispatch_in_flight
                    .insert(correlation.node_execution_id)
            {
                continue;
            }
            if dispatch.kind == FlowDispatchKind::Start {
                state
                    .writer_active
                    .insert(correlation.node_execution_id, correlation.clone());
            }
            let kind = dispatch.kind;
            let node_kind = dispatch.message.correlation.node_kind.clone();
            let registry = self.node_registry.clone();
            let adapter = self.lifecycle_adapter.clone();
            let completion_tx = self.completion_tx.clone();
            let task_correlation = correlation.clone();
            let task = tokio::spawn(async move {
                let result = FlowTickLoop::dispatch(dispatch, &registry, &adapter).await;
                let _ = completion_tx
                    .send(DispatchCompletion {
                        correlation: task_correlation,
                        node_kind,
                        result,
                        token_expired: false,
                        kind,
                    })
                    .await;
            });
            if let Some(state) = self.execution.get_mut(&correlation.flow_execution_id) {
                state.dispatch_tasks.push(task.abort_handle());
            }
            self.background_tasks.retain(|task| !task.is_finished());
            self.background_tasks.push(task);
        }
    }

    async fn apply_retention_update(&mut self, retention_completed_runs: u32) -> Result<FlowResponse, FlowError> {
        self.retention_completed_runs = retention_completed_runs as usize;
        self.prune_terminal_executions().await?;
        Ok(FlowResponse::RetentionUpdated)
    }

    async fn save_script_source(
        &mut self,
        location: ScriptSourceLocation,
        path: String,
        source: String,
    ) -> Result<FlowResponse, FlowError> {
        let stored = self
            .workspace_hook
            .save_script_source(location, path, source)
            .await?;
        Ok(FlowResponse::ScriptSourceSaved { stored })
    }

    async fn read_script_source(
        &mut self,
        location: ScriptSourceLocation,
        path: String,
    ) -> Result<FlowResponse, FlowError> {
        let source = self
            .workspace_hook
            .read_script_source(location, path)
            .await?;
        Ok(FlowResponse::ScriptSourceRead { source })
    }

    async fn shutdown_runtime(&mut self) -> Result<FlowResponse, FlowError> {
        for task in self.background_tasks.drain(..) {
            task.abort();
            let _ = task.await;
        }
        self.workspace_hook.shutdown().await?;
        Ok(FlowResponse::RuntimeStopped)
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        for task in &self.background_tasks {
            task.abort();
        }
    }
}

pub(super) fn terminal_retention_retry_due(now: Instant, next_retry: &mut Instant) -> bool {
    if now < *next_retry {
        return false;
    }
    *next_retry = now + TERMINAL_RETENTION_RETRY_INTERVAL;
    true
}

pub(super) fn graph_nodes(root: &GraphNode) -> HashMap<super::types::FlowNodeId, FlowNode> {
    let mut nodes = HashMap::new();
    root.for_each_leaf(&mut |node| {
        nodes.insert(node.id, node.clone());
    });
    nodes
}

pub(super) async fn run_actor(mut actor: Actor) {
    let mut scheduler = interval(Duration::from_secs(1));
    scheduler.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            request = actor.receiver.recv() => match request {
                Some(request) => actor.handle_message(request).await,
                None => break,
            },
            completion = actor.completion_rx.recv() => match completion {
                Some(completion) => {
                    if let Err(error) = actor.handle_dispatch_result(completion).await {
                        tracing::error!(error = %error, "Flow lifecycle completion failed");
                    }
                }
                None => break,
            },
            _ = scheduler.tick() => {
                actor.retry_pending_execution_work().await;
                actor.tick_active_executions().await;
                let _ = actor.fire_due_triggers().await;
            }
        }
    }
    if let Err(error) = actor.shutdown_runtime().await {
        tracing::error!(error = %error, "Flow workspace shutdown failed");
    }
}

#[cfg(test)]
mod tests;
