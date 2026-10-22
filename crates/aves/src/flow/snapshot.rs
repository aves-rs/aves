use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use specta::Type;
use turso::{Connection, Database, IntoParams};
use uuid::Uuid;

use super::ast::{ContentAddressedRoot, ContentHash};
use super::blackboard::{BoundInputs, BoundOutputs};
use super::mailbox::MailboxMessage;
use super::tick::ReconstructionState;
use super::types::{
    FlowExecutionId, FlowExecutionSnapshot, FlowExecutionStatus, FlowId, FlowNodeId, FlowTimestamp, NodeExecutionId,
    SnapshotError,
};

pub(super) const UPSERT_EXECUTION_SQL: &str = include_str!("queries/upsert_execution.sql");
pub(super) const DELETE_NODE_EXECUTIONS_SQL: &str = include_str!("queries/delete_node_executions.sql");
pub(super) const INSERT_NODE_EXECUTION_SQL: &str = include_str!("queries/insert_node_execution.sql");

/// The complete Flow-owned state written as one recovery unit.
///
/// This type intentionally has no token, cursor, or owner-runtime field. A
/// snapshot can therefore be safely persisted and used to reconstruct a new
/// in-memory execution after a process restart.
#[derive(Clone, Debug, Deserialize, Serialize, Type, schemars::JsonSchema)]
pub(super) struct ExecutionSnapshot {
    pub(super) execution: FlowExecutionSnapshot,
    pub(super) blackboard: BTreeMap<String, Value>,
    pub(super) code_roots: Vec<ContentAddressedRoot>,
    pub(super) state_roots: Vec<ContentAddressedRoot>,
    /// Immutable workspace content bound to this graph position.
    #[serde(default)]
    pub(super) workspace_checkpoint: Option<ContentAddressedRoot>,
    /// The document projection used to diff the live workspace at the next capture.
    #[serde(default)]
    pub(super) workspace_entries: BTreeMap<String, ContentHash>,
    /// The source collection imported for this execution, when mapped input was used.
    #[serde(default)]
    pub(super) imported_workspace_root: Option<ContentAddressedRoot>,
    pub(super) reconstruction: ReconstructionState,
    pub(super) scratchpad: ScratchpadSnapshot,
    pub(super) nodes: Vec<NodeExecutionSnapshot>,
}

/// Serializable local-context state needed to resume owner work.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize, Type, schemars::JsonSchema)]
pub(super) struct ScratchpadSnapshot {
    pub(super) pending: Vec<NodeExecutionId>,
    pub(super) mailbox: Vec<MailboxMessage>,
}

/// Durable state for one node execution, including its stable replay identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, Type, schemars::JsonSchema)]
pub struct NodeExecutionSnapshot {
    pub flow_node_id: FlowNodeId,
    pub node_execution_id: NodeExecutionId,
    pub status: NodeExecutionStatus,
    pub state: Value,
    #[serde(default)]
    pub inputs: BoundInputs,
    #[serde(default)]
    pub outputs: BoundOutputs,
    #[serde(default)]
    pub artifacts: Vec<String>,
    pub started_at: Option<FlowTimestamp>,
    pub updated_at: FlowTimestamp,
    pub completed_at: Option<FlowTimestamp>,
    pub error: Option<String>,
}

/// Observer-visible node status persisted with an execution snapshot.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, Type, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum NodeExecutionStatus {
    Ready,
    Running,
    Succeeded,
    Failed,
    Aborted,
}

/// Persists execution and node state in one database transaction.
#[derive(Clone)]
pub(super) struct ExecutionSnapshotStore {
    database: Database,
    #[cfg(test)]
    terminal_saves_before_injected_failure: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    checkpoint_saves_before_injected_failure: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl ExecutionSnapshotStore {
    /// Creates a store using the already-migrated Aves Flow database.
    pub(super) fn new(database: Database) -> Self {
        Self {
            database,
            #[cfg(test)]
            terminal_saves_before_injected_failure: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(
                usize::MAX,
            )),
            #[cfg(test)]
            checkpoint_saves_before_injected_failure: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(
                usize::MAX,
            )),
        }
    }

    /// Loads every durable recovery unit and verifies its pinned observer identity.
    pub(super) async fn load_all(&self) -> Result<Vec<ExecutionSnapshot>, SnapshotError> {
        let connection = self.database.connect()?;
        let mut rows = connection
            .query(include_str!("queries/load_executions.sql"), ())
            .await?;
        let mut snapshots = Vec::new();
        while let Some(row) = rows.next().await? {
            let execution_id = FlowExecutionId(Uuid::parse_str(&row.get::<String>(0)?)?);
            let flow_id = FlowId(Uuid::parse_str(&row.get::<String>(1)?)?);
            let revision = row.get::<i64>(2)?;
            let started_at = row.get::<String>(8)?;
            let execution: FlowExecutionSnapshot = serde_json::from_str(&row.get::<String>(7)?)?;
            if execution.execution_id != execution_id
                || execution.flow_id != flow_id
                || execution.revision != revision
                || execution.started_at.as_rfc3339() != started_at
            {
                return Err(SnapshotError::AttributionMismatch { execution_id });
            }
            let (code_roots, state_roots) = serde_json::from_str(&row.get::<String>(4)?)?;
            let workspace_checkpoint = row
                .get::<Option<String>>(9)?
                .map(|value| serde_json::from_str(&value))
                .transpose()?;
            let workspace_entries = serde_json::from_str(&row.get::<String>(10)?)?;
            let imported_workspace_root = row
                .get::<Option<String>>(11)?
                .map(|value| serde_json::from_str(&value))
                .transpose()?;
            snapshots.push(ExecutionSnapshot {
                execution,
                blackboard: serde_json::from_str(&row.get::<String>(3)?)?,
                code_roots,
                state_roots,
                workspace_checkpoint,
                workspace_entries,
                imported_workspace_root,
                reconstruction: serde_json::from_str(&row.get::<String>(5)?)?,
                scratchpad: serde_json::from_str(&row.get::<String>(6)?)?,
                nodes: load_nodes(&connection, execution_id).await?,
            });
        }
        Ok(snapshots)
    }

    /// Loads the retained execution history for one selected Flow.
    pub(super) async fn load_for_flow(&self, flow_id: FlowId) -> Result<Vec<ExecutionSnapshot>, SnapshotError> {
        Ok(self
            .load_all()
            .await?
            .into_iter()
            .filter(|snapshot| snapshot.execution.flow_id == flow_id)
            .collect())
    }

    /// Atomically replaces the execution projection and all node projections.
    pub(super) async fn save(&self, snapshot: &ExecutionSnapshot) -> Result<(), SnapshotError> {
        #[cfg(test)]
        tests::maybe_fail_save(self, snapshot)?;
        let execution = &snapshot.execution;
        let values = execution_values(snapshot)?;

        let mut connection = self.database.connect()?;
        let transaction = connection.transaction().await?;
        let changed = transaction.execute(UPSERT_EXECUTION_SQL, values).await?;
        if changed != 1 {
            return Err(SnapshotError::AttributionMismatch {
                execution_id: execution.execution_id,
            });
        }
        transaction
            .execute(DELETE_NODE_EXECUTIONS_SQL, (execution.execution_id.0.to_string(),))
            .await?;
        for node in &snapshot.nodes {
            transaction
                .execute(
                    INSERT_NODE_EXECUTION_SQL,
                    node_values(execution.execution_id, execution.flow_id, execution.revision, node)?,
                )
                .await?;
        }
        transaction.commit().await?;
        Ok(())
    }
}

/// Builds the parent parameter projection shared by checkpoint and import persistence.
pub(super) fn execution_values(snapshot: &ExecutionSnapshot) -> Result<impl IntoParams, serde_json::Error> {
    let execution = &snapshot.execution;
    Ok((
        execution.execution_id.0.to_string(),
        execution.flow_id.0.to_string(),
        execution.revision,
        execution_status(&execution.status),
        serde_json::to_string(&snapshot.blackboard)?,
        serde_json::to_string(&(&snapshot.code_roots, &snapshot.state_roots))?,
        serde_json::to_string(&snapshot.reconstruction)?,
        serde_json::to_string(&snapshot.scratchpad)?,
        serde_json::to_string(execution)?,
        execution.started_at.as_rfc3339(),
        execution
            .completed_at
            .as_ref()
            .map(FlowTimestamp::as_rfc3339),
        execution.error.clone(),
        snapshot
            .workspace_checkpoint
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?,
        serde_json::to_string(&snapshot.workspace_entries)?,
        snapshot
            .imported_workspace_root
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?,
    ))
}

/// Builds the node parameter projection shared by checkpoint and import persistence.
pub(super) fn node_values(
    execution_id: FlowExecutionId,
    flow_id: FlowId,
    revision: i64,
    node: &NodeExecutionSnapshot,
) -> Result<impl IntoParams, serde_json::Error> {
    Ok((
        execution_id.0.to_string(),
        node.node_execution_id.0.to_string(),
        flow_id.0.to_string(),
        revision,
        node.flow_node_id.0.to_string(),
        node_status(node.status),
        serde_json::to_string(&node.state)?,
        serde_json::to_string(&node.inputs)?,
        serde_json::to_string(&node.outputs)?,
        node.started_at.as_ref().map(FlowTimestamp::as_rfc3339),
        node.updated_at.as_rfc3339(),
        node.completed_at.as_ref().map(FlowTimestamp::as_rfc3339),
        node.error.clone(),
    ))
}

async fn load_nodes(
    connection: &Connection,
    execution_id: FlowExecutionId,
) -> Result<Vec<NodeExecutionSnapshot>, SnapshotError> {
    let mut rows = connection
        .query(
            include_str!("queries/load_node_executions.sql"),
            (execution_id.0.to_string(),),
        )
        .await?;
    let mut nodes = Vec::new();
    while let Some(row) = rows.next().await? {
        let status = match row.get::<String>(2)?.as_str() {
            "ready" => NodeExecutionStatus::Ready,
            "running" => NodeExecutionStatus::Running,
            "succeeded" => NodeExecutionStatus::Succeeded,
            "failed" => NodeExecutionStatus::Failed,
            "aborted" => NodeExecutionStatus::Aborted,
            status => {
                return Err(SnapshotError::UnknownNodeStatus {
                    status: status.to_owned(),
                });
            }
        };
        nodes.push(NodeExecutionSnapshot {
            flow_node_id: FlowNodeId(Uuid::parse_str(&row.get::<String>(0)?)?),
            node_execution_id: NodeExecutionId(Uuid::parse_str(&row.get::<String>(1)?)?),
            status,
            state: serde_json::from_str(&row.get::<String>(3)?)?,
            inputs: row
                .get::<Option<String>>(4)?
                .map(|value| serde_json::from_str(&value))
                .transpose()?
                .unwrap_or_default(),
            outputs: row
                .get::<Option<String>>(5)?
                .map(|value| serde_json::from_str(&value))
                .transpose()?
                .unwrap_or_default(),
            artifacts: Vec::new(),
            started_at: row
                .get::<Option<String>>(6)?
                .map(|value| FlowTimestamp::parse(&value))
                .transpose()?,
            updated_at: FlowTimestamp::parse(&row.get::<String>(7)?)?,
            completed_at: row
                .get::<Option<String>>(8)?
                .map(|value| FlowTimestamp::parse(&value))
                .transpose()?,
            error: row.get(9)?,
        });
    }
    Ok(nodes)
}

/// Maps lifecycle status to SQL storage status; the observer JSON retains `Aborting`.
pub(super) fn execution_status(status: &FlowExecutionStatus) -> &'static str {
    match status {
        FlowExecutionStatus::Active | FlowExecutionStatus::Aborting => "active",
        FlowExecutionStatus::Succeeded => "succeeded",
        FlowExecutionStatus::Failed => "failed",
        FlowExecutionStatus::Aborted => "aborted",
    }
}

/// Maps the observer-visible node status to its stable persisted spelling.
pub(super) fn node_status(status: NodeExecutionStatus) -> &'static str {
    match status {
        NodeExecutionStatus::Ready => "ready",
        NodeExecutionStatus::Running => "running",
        NodeExecutionStatus::Succeeded => "succeeded",
        NodeExecutionStatus::Failed => "failed",
        NodeExecutionStatus::Aborted => "aborted",
    }
}

#[cfg(test)]
impl ExecutionSnapshotStore {
    pub(super) fn fail_save_after_successes(&self, successes: usize) {
        self.terminal_saves_before_injected_failure
            .store(successes, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn fail_checkpoint_save_after_successes(&self, successes: usize) {
        self.checkpoint_saves_before_injected_failure
            .store(successes, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use uuid::Uuid;

    use crate::migrations::flows::MIGRATIONS;
    use crate::utils::database::{MigrationRunner, open_database_at_path};

    use super::super::{FlowExecutionId, FlowId, FlowNodeId, FlowTimestamp, NodeExecutionId};
    use super::*;

    pub(super) fn maybe_fail_save(
        store: &super::ExecutionSnapshotStore,
        snapshot: &ExecutionSnapshot,
    ) -> Result<(), SnapshotError> {
        if snapshot.execution.status == FlowExecutionStatus::Active {
            if store
                .checkpoint_saves_before_injected_failure
                .load(Ordering::SeqCst)
                == 0
            {
                store
                    .checkpoint_saves_before_injected_failure
                    .store(usize::MAX, Ordering::SeqCst);
                return Err(SnapshotError::AttributionMismatch {
                    execution_id: snapshot.execution.execution_id,
                });
            }
            if store
                .checkpoint_saves_before_injected_failure
                .load(Ordering::SeqCst)
                != usize::MAX
            {
                store
                    .checkpoint_saves_before_injected_failure
                    .fetch_sub(1, Ordering::SeqCst);
            }
        } else {
            if store
                .terminal_saves_before_injected_failure
                .load(Ordering::SeqCst)
                == 0
            {
                store
                    .terminal_saves_before_injected_failure
                    .store(usize::MAX, Ordering::SeqCst);
                return Err(SnapshotError::AttributionMismatch {
                    execution_id: snapshot.execution.execution_id,
                });
            }
            if store
                .terminal_saves_before_injected_failure
                .load(Ordering::SeqCst)
                != usize::MAX
            {
                store
                    .terminal_saves_before_injected_failure
                    .fetch_sub(1, Ordering::SeqCst);
            }
        }
        Ok(())
    }

    #[test]
    fn snapshot_roundtrip_preserves_recovery_inputs_without_tokens() {
        let execution_id = FlowExecutionId(Uuid::new_v4());
        let flow_id = FlowId(Uuid::new_v4());
        let node_execution_id = NodeExecutionId(Uuid::new_v4());
        let snapshot = ExecutionSnapshot {
            execution: FlowExecutionSnapshot {
                execution_id,
                flow_id,
                revision: 3,
                status: FlowExecutionStatus::Active,
                started_at: FlowTimestamp::now(),
                completed_at: None,
                error: None,
            },
            blackboard: BTreeMap::from([(String::from("input"), serde_json::json!(true))]),
            code_roots: Vec::new(),
            state_roots: Vec::new(),
            workspace_checkpoint: Some(ContentAddressedRoot {
                digest: super::super::ast::ContentHash::try_from(
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                )
                .unwrap(),
            }),
            workspace_entries: BTreeMap::from([(
                "input.txt".to_owned(),
                super::super::ast::ContentHash::try_from(
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                )
                .unwrap(),
            )]),
            imported_workspace_root: None,
            reconstruction: super::super::tick::ReconstructionState {
                controller_path: vec![0, 1],
                active_node: Some(FlowNodeId(Uuid::new_v4())),
            },
            scratchpad: ScratchpadSnapshot {
                pending: vec![node_execution_id],
                mailbox: Vec::new(),
            },
            nodes: vec![NodeExecutionSnapshot {
                flow_node_id: FlowNodeId(Uuid::new_v4()),
                node_execution_id,
                status: NodeExecutionStatus::Running,
                state: serde_json::json!({"step":1}),
                inputs: BoundInputs::default(),
                outputs: BoundOutputs::default(),
                artifacts: Vec::new(),
                started_at: Some(FlowTimestamp::now()),
                updated_at: FlowTimestamp::now(),
                completed_at: None,
                error: None,
            }],
        };
        let encoded = serde_json::to_value(&snapshot).unwrap();
        assert!(encoded.get("token").is_none());
        assert_eq!(
            serde_json::from_value::<ExecutionSnapshot>(encoded)
                .unwrap()
                .reconstruction
                .controller_path,
            vec![0, 1]
        );
        let restored = serde_json::from_value::<ExecutionSnapshot>(serde_json::to_value(&snapshot).unwrap()).unwrap();
        assert_eq!(restored.workspace_checkpoint, snapshot.workspace_checkpoint);
        assert_eq!(restored.workspace_entries, snapshot.workspace_entries);
    }

    #[tokio::test]
    async fn pre_dataflow_migration_node_rows_load_with_empty_bound_values() {
        let directory = tempfile::tempdir().unwrap();
        let database = open_database_at_path(directory.path().join("flows.db"))
            .await
            .unwrap();
        MigrationRunner::new("__flow_migrations", &MIGRATIONS[..6])
            .apply(&database)
            .await
            .unwrap();

        let flow_id = FlowId(Uuid::new_v4());
        let execution_id = FlowExecutionId(Uuid::new_v4());
        let flow_node_id = FlowNodeId(Uuid::new_v4());
        let node_execution_id = NodeExecutionId(Uuid::new_v4());
        let started_at = FlowTimestamp::now();
        let execution = FlowExecutionSnapshot {
            execution_id,
            flow_id,
            revision: 1,
            status: FlowExecutionStatus::Active,
            started_at: started_at.clone(),
            completed_at: None,
            error: None,
        };
        let connection = database.connect().unwrap();
        connection
            .execute(
                "INSERT INTO flows (flow_id, active_revision, deleted_at) VALUES (?1, ?2, ?3)",
                (flow_id.0.to_string(), 1_i64, Option::<String>::None),
            )
            .await
            .unwrap();
        connection
            .execute(
                "INSERT INTO flow_definition_revisions (flow_id, revision, name, graph_json, blackboard_defaults_json, blackboard_bindings_json, editor_metadata_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                (
                    flow_id.0.to_string(),
                    1_i64,
                    "legacy",
                    "{}",
                    "{}",
                    "[]",
                    "{}",
                ),
            )
            .await
            .unwrap();
        connection
            .execute(
                "INSERT INTO flow_nodes (flow_id, revision, node_id, parent_node_id, node_kind, label, constraints_json, data_space_json, configuration_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                (
                    flow_id.0.to_string(),
                    1_i64,
                    flow_node_id.0.to_string(),
                    Option::<String>::None,
                    "ScriptNode",
                    "legacy",
                    "{}",
                    "{}",
                    "{}",
                ),
            )
            .await
            .unwrap();
        connection
            .execute(
                "INSERT INTO flow_executions (execution_id, flow_id, revision, status, blackboard_json, workspace_roots_json, reconstruction_state_json, scratchpad_json, observer_state_json, started_at, updated_at, completed_at, error_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                (
                    execution_id.0.to_string(),
                    flow_id.0.to_string(),
                    1_i64,
                    "active",
                    "{}",
                    "[[],[]]",
                    r#"{"controller_path":[],"active_node":null}"#,
                    r#"{"pending":[],"mailbox":[]}"#,
                    serde_json::to_string(&execution).unwrap(),
                    started_at.as_rfc3339(),
                    started_at.as_rfc3339(),
                    Option::<String>::None,
                    Option::<String>::None,
                ),
            )
            .await
            .unwrap();
        connection
            .execute(
                "INSERT INTO flow_node_executions (execution_id, node_execution_id, flow_id, revision, node_id, status, state_json, started_at, updated_at, completed_at, error_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                (
                    execution_id.0.to_string(),
                    node_execution_id.0.to_string(),
                    flow_id.0.to_string(),
                    1_i64,
                    flow_node_id.0.to_string(),
                    "ready",
                    "{}",
                    Option::<String>::None,
                    started_at.as_rfc3339(),
                    Option::<String>::None,
                    Option::<String>::None,
                ),
            )
            .await
            .unwrap();

        MigrationRunner::new("__flow_migrations", MIGRATIONS)
            .apply(&database)
            .await
            .unwrap();
        let store = ExecutionSnapshotStore::new(database.clone());
        let snapshots = store.load_all().await.unwrap();

        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].nodes.len(), 1);
        assert_eq!(snapshots[0].nodes[0].inputs, BoundInputs::default());
        assert_eq!(snapshots[0].nodes[0].outputs, BoundOutputs::default());

        database
            .connect()
            .unwrap()
            .execute(
                "UPDATE flow_node_executions SET inputs_json = ?1, outputs_json = ?2 WHERE execution_id = ?3 AND node_execution_id = ?4",
                (
                    "{}",
                    "{}",
                    execution_id.0.to_string(),
                    node_execution_id.0.to_string(),
                ),
            )
            .await
            .unwrap();
        let snapshots = store.load_all().await.unwrap();

        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].nodes.len(), 1);
        assert_eq!(snapshots[0].nodes[0].inputs, BoundInputs::default());
        assert_eq!(snapshots[0].nodes[0].outputs, BoundOutputs::default());
    }
}
