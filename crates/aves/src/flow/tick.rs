use std::any::Any;
use std::collections::HashMap;
use std::path::PathBuf;

use bonsai_bt::{BT, Behavior, Event, Status};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use specta::Type;
use uuid::Uuid;

use super::adapter::{FlowLifecycleAdapter, OwnerLifecycleResult, dispatch_start, dispatch_tick};
use super::ast::FlowNode;
use super::blackboard::{Blackboard, BoundInputs, BoundOutputs};
use super::graph::{FlowGraph, GraphNode, ParallelPolicy};
use super::mailbox::{MailboxMessage, MailboxResolution, NodeMailbox, TerminalResult, TerminalStatus, TokenExpired};
use super::messages::{ErasedLifecycleMessage, LifecycleCorrelation, LifecycleStart, LifecycleTick};
use super::registration::{FlowNodeRegistry, NodeKind};
use super::snapshot::{ExecutionSnapshot, NodeExecutionSnapshot, NodeExecutionStatus, ScratchpadSnapshot};
use super::types::{
    BlackboardError, FlowAdapterError, FlowExecutionId, FlowId, FlowNodeId, FlowTimestamp, NodeExecutionId, TickError,
};

/// Static action passed to Bonsai's callback.
#[derive(Clone, Debug)]
pub(super) struct FlowAction {
    /// Persisted node identity.
    node_id: FlowNodeId,
    /// Owner node kind.
    kind: NodeKind,
    /// Complete leaf configuration required by its owner adapter.
    node: FlowNode,
}

/// Owner work emitted by a non-blocking tick.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FlowDispatchKind {
    /// Begin an owner node execution.
    Start,
    /// Advance an owner node execution without waiting in Flow.
    Tick,
}

/// Owner work emitted by a non-blocking tick.
pub(super) struct FlowDispatch {
    /// Node correlation.
    pub(super) correlation: LifecycleCorrelation,
    /// Lifecycle operation selected by Flow.
    pub(super) kind: FlowDispatchKind,
    /// Erased payload for the host adapter to translate to its typed bus message.
    pub(super) message: ErasedLifecycleMessage,
}

/// Flow-owned serializable controller position. This is not a Bonsai cursor.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize, Type, JsonSchema)]
pub(super) struct ReconstructionState {
    /// Active controller path in the persisted graph.
    pub(super) controller_path: Vec<usize>,
    /// Stable node currently awaiting an owner result.
    pub(super) active_node: Option<FlowNodeId>,
}

/// Result of one synchronous Bonsai tick.
pub(super) struct TickResult {
    /// Work that the host may dispatch asynchronously after the tick returns.
    pub(super) dispatches: Vec<FlowDispatch>,
    /// Flow-owned reconstruction state.
    pub(super) reconstruction: ReconstructionState,
    /// Current whole-tree outcome after this non-blocking tick.
    pub(super) status: FlowTreeStatus,
    /// Owner result consumed while advancing the tree, if any.
    pub(super) terminal: Option<TerminalResult>,
}

/// Flow-owned projection of Bonsai's whole-tree status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FlowTreeStatus {
    Running,
    Succeeded,
    Failed,
}

/// Application-owned data carried by Bonsai between action callbacks.
#[derive(Default)]
struct NodeProgress {
    execution_id: Option<NodeExecutionId>,
    status: Option<NodeExecutionStatus>,
    state: Option<Value>,
    inputs: Option<BoundInputs>,
    outputs: Option<BoundOutputs>,
    error: Option<String>,
    started_at: Option<FlowTimestamp>,
    updated_at: Option<FlowTimestamp>,
    completed_at: Option<FlowTimestamp>,
    started: bool,
    completed: bool,
}

struct TickState {
    blackboard: Blackboard,
    flow_id: FlowId,
    flow_execution_id: FlowExecutionId,
    mailbox: NodeMailbox,
    pending: HashMap<FlowNodeId, NodeExecutionId>,
    progress: HashMap<FlowNodeId, NodeProgress>,
    execution_root: PathBuf,
}

impl TickState {
    /// Advances one admitted Bonsai leaf using its saved owner progress.
    fn tick_action(
        &mut self,
        action: &FlowAction,
        admit_new_writers: bool,
        dispatches: &mut Vec<FlowDispatch>,
        tick_error: &mut Option<BlackboardError>,
        terminal: &mut Option<TerminalResult>,
    ) -> Status {
        let existing_execution = self.pending.get(&action.node_id).copied();
        if existing_execution.is_none() && !admit_new_writers {
            return Status::Running;
        }
        let node_execution_id = existing_execution.unwrap_or_else(|| {
            let execution_id = self
                .progress
                .get(&action.node_id)
                .and_then(|progress| progress.execution_id)
                .unwrap_or_else(|| NodeExecutionId(Uuid::new_v4()));
            self.progress
                .entry(action.node_id)
                .or_default()
                .execution_id = Some(execution_id);
            self.pending.insert(action.node_id, execution_id);
            execution_id
        });
        if self
            .progress
            .get(&action.node_id)
            .is_some_and(|progress| progress.completed)
        {
            return match self
                .progress
                .get(&action.node_id)
                .and_then(|progress| progress.status)
            {
                Some(NodeExecutionStatus::Failed | NodeExecutionStatus::Aborted) => Status::Failure,
                _ => Status::Success,
            };
        }
        let correlation = LifecycleCorrelation {
            node_kind: action.kind.clone(),
            flow_id: self.flow_id,
            flow_node_id: action.node_id,
            flow_execution_id: self.flow_execution_id,
            node_execution_id,
        };
        match self.mailbox.resolve(correlation.clone()) {
            MailboxResolution::Pending => self.dispatch_pending(action, correlation, dispatches, tick_error),
            MailboxResolution::Expired => self.finish_expired(correlation),
            MailboxResolution::Terminal(result) => {
                self.finish_terminal(action, correlation, *result, tick_error, terminal)
            }
        }
    }

    /// Binds configured inputs and emits one asynchronous owner lifecycle message.
    fn dispatch_pending(
        &mut self,
        action: &FlowAction,
        correlation: LifecycleCorrelation,
        dispatches: &mut Vec<FlowDispatch>,
        tick_error: &mut Option<BlackboardError>,
    ) -> Status {
        let (inputs_binding, _) = action.node.bindings();
        let inputs = match self.blackboard.bind_inputs(inputs_binding) {
            Ok(inputs) => inputs,
            Err(error) => {
                *tick_error = Some(error);
                return Status::Running;
            }
        };
        self.progress
            .entry(action.node_id)
            .or_default()
            .inputs
            .get_or_insert_with(|| inputs.clone());
        let kind = if !self
            .progress
            .get(&action.node_id)
            .is_some_and(|progress| progress.started)
        {
            let now = FlowTimestamp::now();
            let progress = self.progress.entry(action.node_id).or_default();
            progress.started = true;
            progress.started_at.get_or_insert_with(|| now.clone());
            progress.updated_at = Some(now);
            FlowDispatchKind::Start
        } else {
            FlowDispatchKind::Tick
        };
        self.progress.entry(action.node_id).or_default().status = Some(NodeExecutionStatus::Running);
        let payload: Box<dyn Any + Send + Sync> = match kind {
            FlowDispatchKind::Start => Box::new(LifecycleStart {
                correlation: correlation.clone(),
                payload: inputs,
            }),
            FlowDispatchKind::Tick => Box::new(LifecycleTick {
                correlation: correlation.clone(),
                payload: inputs,
            }),
        };
        dispatches.push(FlowDispatch {
            correlation: correlation.clone(),
            kind,
            message: ErasedLifecycleMessage {
                correlation,
                node: action.node.clone(),
                execution_root: self.execution_root.clone(),
                node_token: None,
                payload,
            },
        });
        Status::Running
    }

    /// Applies terminal output before clearing pending state, preserving patch-failure ordering.
    fn finish_terminal(
        &mut self,
        action: &FlowAction,
        _correlation: LifecycleCorrelation,
        result: TerminalResult,
        tick_error: &mut Option<BlackboardError>,
        terminal: &mut Option<TerminalResult>,
    ) -> Status {
        *terminal = Some(result.clone());
        if result.status == TerminalStatus::Success {
            let (_, output_bindings) = action.node.bindings();
            if let Err(error) = self
                .blackboard
                .patch_outputs(output_bindings, &result.outputs)
            {
                *tick_error = Some(error);
                return Status::Failure;
            }
            self.progress.entry(action.node_id).or_default().outputs = Some(result.outputs.clone());
        }

        self.pending.remove(&action.node_id);
        let progress = self.progress.entry(action.node_id).or_default();

        progress.started = false;
        progress.completed = true;

        progress.state = Some(result.state);
        if let Some(error) = result.error {
            progress.error = Some(error);
        }
        let (execution_status, bt_status) = match result.status {
            TerminalStatus::Success => (NodeExecutionStatus::Succeeded, Status::Success),
            TerminalStatus::Failure => (NodeExecutionStatus::Failed, Status::Failure),
        };
        progress.status = Some(execution_status);

        bt_status
    }

    /// Records an expired token without marking its execution completed.
    fn finish_expired(&mut self, correlation: LifecycleCorrelation) -> Status {
        self.pending.remove(&correlation.flow_node_id);
        let progress = self.progress.entry(correlation.flow_node_id).or_default();
        progress.started = false;
        progress.status = Some(NodeExecutionStatus::Failed);
        let now = FlowTimestamp::now();
        progress.updated_at = Some(now.clone());
        progress.completed_at.get_or_insert(now);
        Status::Failure
    }
}

/// A compiled Bonsai tree and its Flow-owned asynchronous state.
pub(super) struct FlowTickLoop {
    tree: BT<FlowAction, TickState>,
}

impl FlowTickLoop {
    /// Compiles the persisted AST to Bonsai's static action enum.
    pub(super) fn compile(
        graph: &FlowGraph,
        registry: &FlowNodeRegistry,
        flow_id: FlowId,
        flow_execution_id: FlowExecutionId,
        blackboard: Blackboard,
        execution_root: PathBuf,
    ) -> Result<Self, TickError> {
        let behavior = compile_node(&graph.ast().root, registry)?;
        Ok(Self {
            tree: BT::new(
                behavior,
                TickState {
                    blackboard,
                    flow_id,
                    flow_execution_id,
                    mailbox: NodeMailbox::default(),
                    pending: HashMap::new(),
                    progress: HashMap::new(),
                    execution_root,
                },
            ),
        })
    }

    /// Rebuilds an active loop with the stable node identities in a durable snapshot.
    pub(super) fn restore(
        graph: &FlowGraph,
        registry: &FlowNodeRegistry,
        snapshot: &ExecutionSnapshot,
        execution_root: PathBuf,
    ) -> Result<Self, TickError> {
        let mut loop_state = Self::compile(
            graph,
            registry,
            snapshot.execution.flow_id,
            snapshot.execution.execution_id,
            Blackboard::new(snapshot.blackboard.clone()),
            execution_root,
        )?;
        for node in &snapshot.nodes {
            let state = loop_state.tree.blackboard_mut();
            let progress = state.progress.entry(node.flow_node_id).or_default();
            progress.execution_id = Some(node.node_execution_id);
            progress.status = Some(node.status);
            progress.state = Some(node.state.clone());
            progress.inputs = Some(node.inputs.clone());
            if node.status == NodeExecutionStatus::Succeeded {
                progress.outputs = Some(node.outputs.clone());
            }
            progress.completed = matches!(
                node.status,
                NodeExecutionStatus::Succeeded | NodeExecutionStatus::Failed | NodeExecutionStatus::Aborted
            );
            if let Some(error) = &node.error {
                progress.error = Some(error.clone());
            }
            if let Some(started_at) = &node.started_at {
                progress.started_at = Some(started_at.clone());
            }
            progress.updated_at = Some(node.updated_at.clone());
            if let Some(completed_at) = &node.completed_at {
                progress.completed_at = Some(completed_at.clone());
            }
            match node.status {
                NodeExecutionStatus::Running => {
                    state
                        .pending
                        .insert(node.flow_node_id, node.node_execution_id);
                }
                NodeExecutionStatus::Succeeded | NodeExecutionStatus::Failed | NodeExecutionStatus::Aborted => {}
                NodeExecutionStatus::Ready => {}
            }
        }
        for message in &snapshot.scratchpad.mailbox {
            loop_state
                .tree
                .blackboard_mut()
                .mailbox
                .push(message.clone());
        }
        Ok(loop_state)
    }

    /// Queues an owner result; the actor remains free to process other work.
    pub(super) fn submit(&mut self, message: MailboxMessage) {
        let state = self.tree.blackboard_mut();
        if let MailboxMessage::Terminal(result) = &message {
            let node_id = result.correlation.flow_node_id;
            let progress = state.progress.entry(node_id).or_default();
            progress.state = Some(result.state.clone());
            if let Some(error) = &result.error {
                progress.error = Some(error.clone());
            }
            progress.status = Some(match result.status {
                TerminalStatus::Success => NodeExecutionStatus::Succeeded,
                TerminalStatus::Failure => NodeExecutionStatus::Failed,
            });
            let now = FlowTimestamp::now();
            progress.started_at.get_or_insert_with(|| now.clone());
            progress.updated_at = Some(now.clone());
            progress.completed_at.get_or_insert(now);
        }
        state.mailbox.push(message);
    }

    /// Records the expiry error before queuing its terminal signal.
    pub(super) fn submit_expired(&mut self, expired: TokenExpired, error: String) {
        self.tree
            .blackboard_mut()
            .progress
            .entry(expired.correlation.flow_node_id)
            .or_default()
            .error = Some(error);
        self.submit(MailboxMessage::TokenExpired(expired));
    }

    /// Checks whether a completion belongs to this loop's currently pending node.
    pub(super) fn accepts(&self, correlation: LifecycleCorrelation) -> bool {
        let state = self.tree.blackboard();
        correlation.flow_id == state.flow_id
            && correlation.flow_execution_id == state.flow_execution_id
            && state.pending.get(&correlation.flow_node_id).copied() == Some(correlation.node_execution_id)
    }

    /// Dispatches one emitted action through its registered owner descriptor.
    pub(super) async fn dispatch<A: FlowLifecycleAdapter + ?Sized>(
        dispatch: FlowDispatch,
        registry: &FlowNodeRegistry,
        adapter: &A,
    ) -> Result<OwnerLifecycleResult, FlowAdapterError> {
        match dispatch.kind {
            FlowDispatchKind::Start => dispatch_start(registry, adapter, dispatch.message).await,
            FlowDispatchKind::Tick => dispatch_tick(registry, adapter, dispatch.message).await,
        }
    }

    /// Performs one non-blocking tree tick.
    pub(super) fn tick(&mut self) -> Result<TickResult, TickError> {
        self.tick_with_admission(true)
    }

    /// Performs one tick while optionally refusing to admit a new owner node.
    ///
    /// Existing pending owners still receive their normal lifecycle tick. This
    /// is the abort boundary: stopping admission must not be mistaken for a
    /// cancellation acknowledgement from work that is already running.
    pub(super) fn tick_with_admission(&mut self, admit_new_writers: bool) -> Result<TickResult, TickError> {
        let mut dispatches = Vec::new();
        let mut tick_error = None;
        let mut terminal = None;
        let event = Event::zero_dt_args();
        let status = self.tree.tick(&event, &mut |args, state| {
            (
                state.tick_action(
                    args.action,
                    admit_new_writers,
                    &mut dispatches,
                    &mut tick_error,
                    &mut terminal,
                ),
                args.dt,
            )
        });
        if let Some(error) = tick_error {
            return Err(TickError::Blackboard(error));
        }
        Ok(TickResult {
            dispatches,
            reconstruction: self.reconstruction_state(),
            status: match status.map(|(status, _)| status).unwrap_or(Status::Running) {
                Status::Running => FlowTreeStatus::Running,
                Status::Success => FlowTreeStatus::Succeeded,
                Status::Failure => FlowTreeStatus::Failed,
            },
            terminal,
        })
    }

    /// Returns Flow-owned blackboard state; no Bonsai cursor is exposed.
    pub(super) fn blackboard(&self) -> &Blackboard {
        &self.tree.blackboard().blackboard
    }

    /// Captures pending owner work and queued completions for restart recovery.
    pub(super) fn scratchpad_snapshot(&self) -> ScratchpadSnapshot {
        ScratchpadSnapshot {
            pending: self.tree.blackboard().pending.values().copied().collect(),
            mailbox: self.tree.blackboard().mailbox.snapshot(),
        }
    }

    /// Captures the Flow-owned reconstruction position without advancing the tree.
    pub(super) fn reconstruction_state(&self) -> ReconstructionState {
        ReconstructionState {
            controller_path: Vec::new(),
            active_node: self.tree.blackboard().pending.keys().next().copied(),
        }
    }

    /// Captures stable node execution identities and their current statuses.
    pub(super) fn node_snapshots(&self) -> Vec<NodeExecutionSnapshot> {
        let state = self.tree.blackboard();
        state
            .progress
            .iter()
            .filter_map(|(flow_node_id, progress)| {
                let node_execution_id = progress.execution_id?;
                let status = progress.status.unwrap_or(NodeExecutionStatus::Ready);
                let terminal = matches!(
                    status,
                    NodeExecutionStatus::Succeeded | NodeExecutionStatus::Failed | NodeExecutionStatus::Aborted
                );
                Some(NodeExecutionSnapshot {
                    flow_node_id: *flow_node_id,
                    node_execution_id,
                    status,
                    state: progress.state.clone().unwrap_or(Value::Null),
                    inputs: progress.inputs.clone().unwrap_or_default(),
                    outputs: if status == NodeExecutionStatus::Succeeded {
                        progress.outputs.clone().unwrap_or_default()
                    } else {
                        Default::default()
                    },
                    artifacts: Vec::new(),
                    started_at: progress.started_at.clone(),
                    updated_at: progress
                        .updated_at
                        .clone()
                        .unwrap_or_else(FlowTimestamp::now),
                    completed_at: terminal.then(|| progress.completed_at.clone()).flatten(),
                    error: progress.error.clone(),
                })
            })
            .collect()
    }

    /// Discards queued mail while aborting this execution.
    ///
    /// The owner remains active until the actor receives its real terminal
    /// acknowledgement. In particular, this method must not mark a writer
    /// aborted or make its files eligible for cleanup.
    pub(super) fn discard_pending(&mut self) {
        let state = self.tree.blackboard_mut();
        for node_execution_id in state.pending.values() {
            state.mailbox.discard(*node_execution_id);
        }
    }

    /// Terminalizes unfinished nodes after acknowledgement or the abort deadline.
    pub(super) fn mark_pending_aborted(&mut self) {
        self.terminalize_pending(NodeExecutionStatus::Aborted);
    }

    /// Records unfinished work as failed when the behavior tree has terminated.
    pub(super) fn mark_pending_failed(&mut self) {
        self.terminalize_pending(NodeExecutionStatus::Failed);
    }

    /// Applies one terminal status to admitted unfinished work and clears transient membership.
    fn terminalize_pending(&mut self, status: NodeExecutionStatus) {
        let state = self.tree.blackboard_mut();
        let now = FlowTimestamp::now();
        for progress in state.progress.values_mut() {
            progress.started = false;
            if progress.execution_id.is_none() {
                continue;
            }
            if !matches!(
                progress.status,
                Some(NodeExecutionStatus::Succeeded | NodeExecutionStatus::Failed | NodeExecutionStatus::Aborted)
            ) {
                progress.status = Some(status);
                progress.updated_at = Some(now.clone());
                progress.completed_at.get_or_insert_with(|| now.clone());
                progress.completed = true;
            }
        }
        state.pending.clear();
    }
}

fn compile_node(node: &GraphNode, registry: &FlowNodeRegistry) -> Result<Behavior<FlowAction>, TickError> {
    match node {
        GraphNode::AlwaysSucceed { child } => Ok(Behavior::AlwaysSucceed(Box::new(compile_node(child, registry)?))),
        GraphNode::Sequence { children } => compile_children(children, registry, Behavior::Sequence),
        GraphNode::Selector { children } | GraphNode::Fallback { children } => {
            compile_children(children, registry, Behavior::Select)
        }
        GraphNode::Parallel { policy, children } => {
            let children = children
                .iter()
                .map(|child| compile_node(child, registry))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(match policy {
                ParallelPolicy::WhileAll => Behavior::WhileAll(Box::new(Behavior::WaitForever), children),
                ParallelPolicy::WhenAll => Behavior::WhenAll(children),
                ParallelPolicy::WhenAny => Behavior::WhenAny(children),
                ParallelPolicy::Race => Behavior::Race(children),
            })
        }
        GraphNode::Leaf { node } => compile_leaf(node, registry),
    }
}

fn compile_leaf(node: &FlowNode, registry: &FlowNodeRegistry) -> Result<Behavior<FlowAction>, TickError> {
    let kind = node.node_kind.clone();
    registry
        .get(&kind)
        .map_err(|_| TickError::Unregistered(kind.clone()))?;
    Ok(Behavior::Action(FlowAction {
        node_id: node.id,
        kind,
        node: node.clone(),
    }))
}

fn compile_children(
    children: &[GraphNode],
    registry: &FlowNodeRegistry,
    constructor: fn(Vec<Behavior<FlowAction>>) -> Behavior<FlowAction>,
) -> Result<Behavior<FlowAction>, TickError> {
    children
        .iter()
        .map(|child| compile_node(child, registry))
        .collect::<Result<Vec<_>, _>>()
        .map(constructor)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::super::mailbox::{TerminalResult, TerminalStatus};
    use super::super::registration::{
        CancellationDisposition, NodeKindDescriptor, NodeKindRegistration, NodeLifecycleResult, SideEffectDisposition,
        WorkspacePreparation,
    };
    use super::super::snapshot::{NodeExecutionSnapshot, ScratchpadSnapshot};
    use super::super::{FlowExecutionSnapshot, FlowExecutionStatus, FlowTimestamp};
    use super::*;

    fn registry() -> FlowNodeRegistry {
        let mut registry = FlowNodeRegistry::new();
        for kind in ["ScriptNode", "AgentNode", "OrderNode"] {
            let kind = NodeKind::parse(kind).unwrap();
            registry
                .register(NodeKindRegistration::typed::<(), _, _, _, _, _, _, _>(
                    NodeKindDescriptor {
                        kind,
                        cancellation: CancellationDisposition::Cancellable,
                        side_effects: SideEffectDisposition::Pure,
                    },
                    |_| Ok(()),
                    |_, ()| async { Ok(NodeLifecycleResult::Running) },
                    |_, ()| async { Ok(NodeLifecycleResult::Running) },
                    |_, ()| async { Ok(NodeLifecycleResult::Running) },
                    None,
                    WorkspacePreparation::default(),
                ))
                .unwrap();
        }
        registry
    }

    fn leaf(kind: NodeKind, id: FlowNodeId) -> serde_json::Value {
        let configuration = match kind.as_str() {
            "ScriptNode" => {
                serde_json::json!({"source_root": null, "entry_point": "script.ts", "fuel_limit": 1, "timeout_secs": 1})
            }
            "OrderNode" => serde_json::json!({"command": "place"}),
            "AgentNode" => {
                serde_json::json!({"providerName": "provider", "agentName": "agent", "prompt": "run", "maxTurns": null, "maxToolCalls": null, "maxDuration": null, "configOverrides": {}, "sluiceMcpEnabled": false, "workspace": null})
            }
            _ => unreachable!(),
        };
        serde_json::json!({
            "id": id,
            "label": "node",
            "constraints": { "runtime": {}, "capabilities": [] },
            "data_space": { "code_paths": [], "state_paths": [] },
            "inputs": [],
            "outputs": [],
            "node_kind": kind,
            "configuration": configuration,
        })
    }

    fn graph(root: serde_json::Value) -> FlowGraph {
        FlowGraph::load_json(&serde_json::json!({ "version": 1, "root": root }).to_string()).unwrap()
    }

    fn snapshot(
        flow_id: FlowId,
        execution_id: FlowExecutionId,
        node_id: FlowNodeId,
        node_execution_id: NodeExecutionId,
        mailbox: Vec<MailboxMessage>,
    ) -> ExecutionSnapshot {
        ExecutionSnapshot {
            execution: FlowExecutionSnapshot {
                execution_id,
                flow_id,
                revision: 1,
                status: FlowExecutionStatus::Active,
                started_at: FlowTimestamp::now(),
                completed_at: None,
                error: None,
            },
            blackboard: BTreeMap::new(),
            code_roots: Vec::new(),
            state_roots: Vec::new(),
            workspace_checkpoint: None,
            workspace_entries: BTreeMap::new(),
            imported_workspace_root: None,
            reconstruction: ReconstructionState::default(),
            scratchpad: ScratchpadSnapshot {
                pending: vec![node_execution_id],
                mailbox,
            },
            nodes: vec![NodeExecutionSnapshot {
                flow_node_id: node_id,
                node_execution_id,
                status: NodeExecutionStatus::Running,
                state: serde_json::Value::Null,
                inputs: BoundInputs::default(),
                outputs: BoundOutputs::default(),
                artifacts: Vec::new(),
                started_at: Some(FlowTimestamp::now()),
                updated_at: FlowTimestamp::now(),
                completed_at: None,
                error: None,
            }],
        }
    }

    #[test]
    fn restored_resultless_work_restarts_all_owner_kinds_with_start() {
        let registry = registry();
        let flow_id = FlowId(Uuid::new_v4());
        for kind in [
            NodeKind::parse("ScriptNode").unwrap(),
            NodeKind::parse("AgentNode").unwrap(),
            NodeKind::parse("OrderNode").unwrap(),
        ] {
            let execution_id = FlowExecutionId(Uuid::new_v4());
            let node_id = FlowNodeId(Uuid::new_v4());
            let node_execution_id = NodeExecutionId(Uuid::new_v4());
            let graph = graph(serde_json::json!({ "type": "Leaf", "node": leaf(kind.clone(), node_id) }));
            let mut tick_loop = FlowTickLoop::restore(
                &graph,
                &registry,
                &snapshot(flow_id, execution_id, node_id, node_execution_id, Vec::new()),
                std::path::PathBuf::new(),
            )
            .unwrap();

            let tick = tick_loop.tick().unwrap();

            assert_eq!(tick.dispatches.len(), 1);
            assert_eq!(tick.dispatches[0].kind, FlowDispatchKind::Start);
            assert_eq!(tick.dispatches[0].message.correlation.node_kind, kind);
        }
    }

    #[test]
    fn always_succeed_waits_then_runs_the_next_task_once_and_preserves_outcomes() {
        let registry = registry();
        for first_status in [TerminalStatus::Success, TerminalStatus::Failure] {
            for second_status in [TerminalStatus::Success, TerminalStatus::Failure] {
                let first = FlowNodeId(Uuid::new_v4());
                let second = FlowNodeId(Uuid::new_v4());
                let script_kind = NodeKind::parse("ScriptNode").unwrap();
                let graph = graph(serde_json::json!({
                    "type": "Sequence",
                    "children": [
                        { "type": "AlwaysSucceed", "child": { "type": "Leaf", "node": leaf(script_kind.clone(), first) } },
                        { "type": "Leaf", "node": leaf(script_kind, second) }
                    ]
                }));
                let mut runner = FlowTickLoop::compile(
                    &graph,
                    &registry,
                    FlowId(Uuid::new_v4()),
                    FlowExecutionId(Uuid::new_v4()),
                    Blackboard::default(),
                    std::path::PathBuf::new(),
                )
                .unwrap();
                let started = runner.tick().unwrap();
                assert_eq!(started.dispatches.len(), 1);
                assert_eq!(started.dispatches[0].correlation.flow_node_id, first);
                assert_eq!(started.dispatches[0].kind, FlowDispatchKind::Start);
                let first_correlation = started.dispatches[0].correlation.clone();
                let waiting = runner.tick().unwrap();
                assert!(
                    waiting
                        .dispatches
                        .iter()
                        .all(|dispatch| dispatch.correlation.flow_node_id == first)
                );
                runner.submit(MailboxMessage::Terminal(TerminalResult {
                    correlation: first_correlation,
                    status: first_status.clone(),
                    outputs: super::super::blackboard::BoundOutputs::default(),
                    state: serde_json::Value::Null,
                    error: None,
                }));
                let continued = runner.tick().unwrap();
                assert_eq!(continued.dispatches.len(), 1);
                assert_eq!(continued.dispatches[0].correlation.flow_node_id, second);
                assert_eq!(continued.dispatches[0].kind, FlowDispatchKind::Start);
                assert_eq!(
                    runner
                        .node_snapshots()
                        .iter()
                        .find(|node| node.flow_node_id == first)
                        .unwrap()
                        .status,
                    match first_status {
                        TerminalStatus::Success => NodeExecutionStatus::Succeeded,
                        TerminalStatus::Failure => NodeExecutionStatus::Failed,
                    },
                );
                runner.submit(MailboxMessage::Terminal(TerminalResult {
                    correlation: continued.dispatches[0].correlation.clone(),
                    status: second_status.clone(),
                    outputs: super::super::blackboard::BoundOutputs::default(),
                    state: serde_json::Value::Null,
                    error: None,
                }));
                let finished = runner.tick().unwrap();
                assert!(finished.dispatches.is_empty());
                assert_eq!(
                    finished.status,
                    match second_status {
                        TerminalStatus::Success => FlowTreeStatus::Succeeded,
                        TerminalStatus::Failure => FlowTreeStatus::Failed,
                    }
                );
                assert!(runner.tick().unwrap().dispatches.is_empty());
                assert_eq!(runner.node_snapshots().len(), 2);
            }
        }
    }

    #[test]
    fn restored_terminal_results_do_not_restart_any_owner_kind() {
        let registry = registry();
        let flow_id = FlowId(Uuid::new_v4());
        for kind in [
            NodeKind::parse("ScriptNode").unwrap(),
            NodeKind::parse("AgentNode").unwrap(),
            NodeKind::parse("OrderNode").unwrap(),
        ] {
            for (terminal_status, expected_status) in [
                (TerminalStatus::Success, FlowTreeStatus::Succeeded),
                (TerminalStatus::Failure, FlowTreeStatus::Failed),
            ] {
                let execution_id = FlowExecutionId(Uuid::new_v4());
                let node_id = FlowNodeId(Uuid::new_v4());
                let node_execution_id = NodeExecutionId(Uuid::new_v4());
                let correlation = LifecycleCorrelation {
                    node_kind: kind.clone(),
                    flow_id,
                    flow_node_id: node_id,
                    flow_execution_id: execution_id,
                    node_execution_id,
                };
                let graph = graph(serde_json::json!({ "type": "Leaf", "node": leaf(kind.clone(), node_id) }));
                let mut tick_loop = FlowTickLoop::restore(
                    &graph,
                    &registry,
                    &snapshot(
                        flow_id,
                        execution_id,
                        node_id,
                        node_execution_id,
                        vec![MailboxMessage::Terminal(TerminalResult {
                            correlation: correlation.clone(),
                            status: terminal_status,
                            outputs: super::super::blackboard::BoundOutputs::new(BTreeMap::new()),
                            state: serde_json::Value::Null,
                            error: None,
                        })],
                    ),
                    std::path::PathBuf::new(),
                )
                .unwrap();

                let tick = tick_loop.tick().unwrap();

                assert!(tick.dispatches.is_empty());
                assert_eq!(tick.status, expected_status);
                assert_eq!(tick.terminal.unwrap().correlation, correlation);
            }
        }
    }

    #[test]
    fn restored_sequence_and_selector_do_not_prestart_unreached_nodes() {
        let registry = registry();
        let flow_id = FlowId(Uuid::new_v4());
        for controller in ["Sequence", "Selector"] {
            let execution_id = FlowExecutionId(Uuid::new_v4());
            let first_node_id = FlowNodeId(Uuid::new_v4());
            let second_node_id = FlowNodeId(Uuid::new_v4());
            let first_execution_id = NodeExecutionId(Uuid::new_v4());
            let script_kind = NodeKind::parse("ScriptNode").unwrap();
            let graph = graph(serde_json::json!({
                "type": controller,
                "children": [
                    { "type": "Leaf", "node": leaf(script_kind.clone(), first_node_id) },
                    { "type": "Leaf", "node": leaf(script_kind, second_node_id) },
                ],
            }));
            let mut tick_loop = FlowTickLoop::restore(
                &graph,
                &registry,
                &snapshot(flow_id, execution_id, first_node_id, first_execution_id, Vec::new()),
                std::path::PathBuf::new(),
            )
            .unwrap();

            let tick = tick_loop.tick().unwrap();

            assert_eq!(tick.dispatches.len(), 1);
            assert_eq!(tick.dispatches[0].correlation.flow_node_id, first_node_id);
            assert_eq!(tick.dispatches[0].kind, FlowDispatchKind::Start);
        }
    }

    #[test]
    fn abort_mail_cleanup_does_not_mark_running_owner_terminal() {
        let registry = registry();
        let flow_id = FlowId(Uuid::new_v4());
        let execution_id = FlowExecutionId(Uuid::new_v4());
        let node_id = FlowNodeId(Uuid::new_v4());
        let node_execution_id = NodeExecutionId(Uuid::new_v4());
        let graph =
            graph(serde_json::json!({ "type": "Leaf", "node": leaf(NodeKind::parse("ScriptNode").unwrap(), node_id) }));
        let mut tick_loop = FlowTickLoop::restore(
            &graph,
            &registry,
            &snapshot(flow_id, execution_id, node_id, node_execution_id, Vec::new()),
            std::path::PathBuf::new(),
        )
        .unwrap();

        tick_loop.discard_pending();

        assert_eq!(tick_loop.node_snapshots()[0].status, NodeExecutionStatus::Running);
        assert!(tick_loop.accepts(LifecycleCorrelation {
            node_kind: NodeKind::parse("ScriptNode").unwrap(),
            flow_id,
            flow_node_id: node_id,
            flow_execution_id: execution_id,
            node_execution_id,
        }));
    }
}
