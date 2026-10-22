use std::collections::HashMap;

use serde::Deserialize;
use turso::{Connection, Database, IntoParams, Row};
use uuid::Uuid;

use crate::utils::database::connect_database;

use super::ast::{BlackboardBinding, FlowNode, NodeConstraints, NodeDataSpace};
use super::cron::FlowTriggerRecord;
use super::files::validate_mappings;
use super::graph::GraphNode;
use super::mobility::{
    FlowActivationAcknowledgement, FlowAuthorityState, FlowMobilityCommitAcknowledgement, FlowMobilitySnapshot,
};
use super::snapshot::{
    DELETE_NODE_EXECUTIONS_SQL, ExecutionSnapshot, INSERT_NODE_EXECUTION_SQL, UPSERT_EXECUTION_SQL, execution_values,
    node_values,
};
use super::types::{
    FlowDefinition, FlowError, FlowExecutionDetail, FlowExecutionId, FlowId, FlowMobilityDestination, FlowTimestamp,
    FlowWorkspaceMapping,
};

const CREATE_EXECUTION_DOCUMENT_SQL: &str = include_str!("queries/create_execution_document.sql");
const DELETE_EXECUTION_SQL: &str = include_str!("queries/delete_execution.sql");
const DELETE_FLOW_DEFINITION_REVISIONS_SQL: &str = include_str!("queries/delete-flow-definition-revisions.sql");
const DELETE_FLOW_EXECUTIONS_SQL: &str = include_str!("queries/delete-flow-executions.sql");
const DELETE_FLOW_NODES_SQL: &str = include_str!("queries/delete-flow-nodes.sql");
const DELETE_FLOW_SQL: &str = include_str!("queries/delete-flow.sql");
const DELETE_FLOW_TRIGGERS_SQL: &str = include_str!("queries/delete-flow-triggers.sql");
const DELETE_TRIGGER_SQL: &str = include_str!("queries/delete_trigger.sql");
const DISABLE_FLOW_TRIGGERS_SQL: &str = include_str!("queries/disable_flow_triggers.sql");
const DOCUMENT_IS_EXCLUSIVE_SQL: &str = include_str!("queries/document-is-exclusive.sql");
const INSERT_DEFINITION_REVISION_SQL: &str = include_str!("queries/insert_definition_revision.sql");
const INSERT_FLOW_AUTHORITY_SQL: &str = include_str!("queries/insert_flow_authority.sql");
const INSERT_FLOW_NODE_SQL: &str = include_str!("queries/insert_flow_node.sql");
const INSERT_FLOW_SQL: &str = include_str!("queries/insert_flow.sql");
const INSERT_MISSING_FLOW_AUTHORITY_SQL: &str = include_str!("queries/insert_missing_flow_authority.sql");
const INSERT_TRIGGER_SQL: &str = include_str!("queries/insert_trigger.sql");
const LIST_TRIGGERS_SQL: &str = include_str!("queries/list_triggers.sql");
const LOAD_DEFINITION_REVISION_SQL: &str = include_str!("queries/load_definition_revision.sql");
const LOAD_DEFINITIONS_SQL: &str = include_str!("queries/load_definitions.sql");
const LOAD_EXECUTION_DOCUMENT_SQL: &str = include_str!("queries/load_execution_document.sql");
const LOAD_FLOW_AUTHORITY_SQL: &str = include_str!("queries/load_flow_authority.sql");
const LOAD_MOBILITY_SNAPSHOT_SQL: &str = include_str!("queries/load_mobility_snapshot.sql");
const LOAD_MOBILITY_STATES_SQL: &str = include_str!("queries/load_mobility_states.sql");
const LOAD_TRIGGERS_SQL: &str = include_str!("queries/load_triggers.sql");
const RETARGET_FLOW_TRIGGERS_SQL: &str = include_str!("queries/retarget_flow_triggers.sql");
const SAVE_MOBILITY_ACTIVATION_SQL: &str = include_str!("queries/save_mobility_activation.sql");
const SAVE_MOBILITY_COMMIT_SQL: &str = include_str!("queries/save_mobility_commit.sql");
const SAVE_MOBILITY_DESTINATION_SQL: &str = include_str!("queries/save_mobility_destination.sql");
const SET_FLOW_AUTHORITY_SQL: &str = include_str!("queries/set_flow_authority.sql");
const SET_FLOW_ENABLED_SQL: &str = include_str!("queries/set_flow_enabled.sql");
const SOFT_DELETE_FLOW_SQL: &str = include_str!("queries/soft_delete_flow.sql");
const UPDATE_FLOW_ACTIVE_REVISION_SQL: &str = include_str!("queries/update_flow_active_revision.sql");
const UPDATE_TRIGGER_SQL: &str = include_str!("queries/update_trigger.sql");
const UPSERT_MOBILITY_SNAPSHOT_SQL: &str = include_str!("queries/upsert_mobility_snapshot.sql");

/// The persisted authority and its optional handoff acknowledgements.
pub(super) struct PersistedMobilityState {
    pub(super) flow_id: FlowId,
    pub(super) authority_state: FlowAuthorityState,
    pub(super) destination: Option<FlowMobilityDestination>,
    pub(super) commit: Option<FlowMobilityCommitAcknowledgement>,
    pub(super) activation: Option<FlowActivationAcknowledgement>,
}

/// Durable identity pair for one execution's local document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ExecutionDocumentLink {
    pub(super) namespace_id: String,
    pub(super) author_id: String,
}

/// Durable Flow catalog and execution projection store.
pub(super) struct FlowStore {
    database: Database,
}

impl FlowStore {
    /// Creates the Flow-owned persistence facade for the shared database.
    pub(super) fn new(database: Database) -> Self {
        Self { database }
    }

    pub(super) async fn save_mobility_destination(
        &self,
        flow_id: FlowId,
        destination: &FlowMobilityDestination,
    ) -> Result<(), FlowError> {
        let connection = connect_database(&self.database).await?;
        connection
            .execute(
                SAVE_MOBILITY_DESTINATION_SQL,
                (flow_id.0.to_string(), serde_json::to_string(destination)?),
            )
            .await?;
        Ok(())
    }

    pub(super) async fn save_mobility_commit(
        &self,
        flow_id: FlowId,
        destination: &FlowMobilityDestination,
        acknowledgement: &FlowMobilityCommitAcknowledgement,
    ) -> Result<(), FlowError> {
        let connection = connect_database(&self.database).await?;
        connection
            .execute(
                SAVE_MOBILITY_COMMIT_SQL,
                (
                    flow_id.0.to_string(),
                    serde_json::to_string(destination)?,
                    serde_json::to_string(acknowledgement)?,
                ),
            )
            .await?;
        Ok(())
    }

    pub(super) async fn save_mobility_activation(
        &self,
        flow_id: FlowId,
        acknowledgement: &FlowActivationAcknowledgement,
    ) -> Result<(), FlowError> {
        let connection = connect_database(&self.database).await?;
        connection
            .execute(
                SAVE_MOBILITY_ACTIVATION_SQL,
                (flow_id.0.to_string(), serde_json::to_string(acknowledgement)?),
            )
            .await?;
        Ok(())
    }

    pub(super) async fn load_mobility_states(&self) -> Result<Vec<PersistedMobilityState>, FlowError> {
        let connection = connect_database(&self.database).await?;
        let mut rows = connection.query(LOAD_MOBILITY_STATES_SQL, ()).await?;
        let mut states = Vec::new();
        while let Some(row) = rows.next().await? {
            let flow_id = FlowId(
                Uuid::parse_str(&row.get::<String>(0)?).map_err(|error| FlowError::MobilityPeer {
                    message: error.to_string(),
                })?,
            );
            let destination = row
                .get::<Option<String>>(2)?
                .map(|value| serde_json::from_str(&value))
                .transpose()?;
            let commit = row
                .get::<Option<String>>(3)?
                .map(|value| serde_json::from_str(&value))
                .transpose()?;
            let activation = row
                .get::<Option<String>>(4)?
                .map(|value| serde_json::from_str(&value))
                .transpose()?;
            for (name, valid) in [
                (
                    "destination",
                    destination.is_some() && commit.is_some() || destination.is_none(),
                ),
                ("commit", commit.is_some() && destination.is_some() || commit.is_none()),
                (
                    "activation",
                    activation.is_some() && destination.is_some() || activation.is_none(),
                ),
            ] {
                if !valid {
                    return Err(FlowError::MobilityPeer {
                        message: format!("malformed mobility {name} for flow {flow_id}"),
                    });
                }
            }
            states.push(PersistedMobilityState {
                flow_id,
                authority_state: decode_authority_state(&row.get::<String>(1)?)?,
                destination,
                commit,
                activation,
            });
        }
        Ok(states)
    }

    pub(super) async fn create_execution_document(
        &self,
        execution_id: FlowExecutionId,
        link: &ExecutionDocumentLink,
    ) -> Result<(), FlowError> {
        let connection = connect_database(&self.database).await?;
        connection
            .execute(
                CREATE_EXECUTION_DOCUMENT_SQL,
                (
                    execution_id.0.to_string(),
                    link.namespace_id.clone(),
                    link.author_id.clone(),
                ),
            )
            .await?;
        Ok(())
    }

    pub(super) async fn load_execution_document(
        &self,
        execution_id: FlowExecutionId,
    ) -> Result<ExecutionDocumentLink, FlowError> {
        self.load_execution_document_optional(execution_id)
            .await?
            .ok_or(FlowError::ExecutionNotFound)
    }

    pub(super) async fn load_execution_document_optional(
        &self,
        execution_id: FlowExecutionId,
    ) -> Result<Option<ExecutionDocumentLink>, FlowError> {
        let connection = connect_database(&self.database).await?;
        let mut rows = connection
            .query(LOAD_EXECUTION_DOCUMENT_SQL, (execution_id.0.to_string(),))
            .await?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        Ok(Some(ExecutionDocumentLink {
            namespace_id: row.get(0)?,
            author_id: row.get(1)?,
        }))
    }

    pub(super) async fn delete_execution(&self, execution_id: FlowExecutionId) -> Result<(), FlowError> {
        let connection = connect_database(&self.database).await?;
        connection
            .execute(DELETE_EXECUTION_SQL, (execution_id.0.to_string(),))
            .await?;
        Ok(())
    }

    /// Makes a source permanently unable to schedule or mutate its selected Flow
    /// before returning a promotion acknowledgement.
    pub(super) async fn fence_and_yield_flow(&self, flow_id: FlowId, revision: u64) -> Result<(), FlowError> {
        let mut connection = connect_database(&self.database).await?;
        let transaction = connection.transaction().await?;
        transaction
            .execute(SET_FLOW_ENABLED_SQL, (flow_id.0.to_string(), revision as i64, 0_i64))
            .await?;
        transaction
            .execute(DISABLE_FLOW_TRIGGERS_SQL, (flow_id.0.to_string(),))
            .await?;
        transaction
            .execute(
                SET_FLOW_AUTHORITY_SQL,
                (flow_id.0.to_string(), FlowAuthorityState::Yielded.as_sql()),
            )
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// A document is removable only when no other execution still links it.
    pub(super) async fn document_is_exclusive(
        &self,
        namespace_id: &str,
        author_id: &str,
        execution_id: FlowExecutionId,
    ) -> Result<bool, FlowError> {
        let connection = connect_database(&self.database).await?;
        let mut rows = connection
            .query(
                DOCUMENT_IS_EXCLUSIVE_SQL,
                (
                    namespace_id.to_owned(),
                    author_id.to_owned(),
                    execution_id.0.to_string(),
                ),
            )
            .await?;
        Ok(rows.next().await?.is_none())
    }

    /// Deletes only rows owned by the transferred Flow, children before their parents.
    /// Blob content is released through per-execution tags, so shared content remains GC-reachable.
    pub(super) async fn delete_transferred_flow(&self, flow_id: FlowId) -> Result<(), FlowError> {
        let mut connection = connect_database(&self.database).await?;
        let transaction = connection.transaction().await?;
        for statement in [
            DELETE_FLOW_EXECUTIONS_SQL,
            DELETE_FLOW_TRIGGERS_SQL,
            DELETE_FLOW_NODES_SQL,
            DELETE_FLOW_DEFINITION_REVISIONS_SQL,
            DELETE_FLOW_SQL,
        ] {
            transaction
                .execute(statement, (flow_id.0.to_string(),))
                .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub(super) fn database(&self) -> Database {
        self.database.clone()
    }

    pub(super) async fn load_definitions(&self) -> Result<HashMap<FlowId, FlowDefinition>, FlowError> {
        let connection = connect_database(&self.database).await?;
        let mut rows = connection.query(LOAD_DEFINITIONS_SQL, ()).await?;
        let mut definitions = HashMap::new();
        while let Some(row) = rows.next().await? {
            let flow_id = FlowId(Uuid::parse_str(&row.get::<String>(3)?)?);
            let definition = FlowDefinition {
                flow_id,
                name: row.get(5)?,
                revision: row.get(4)?,
                graph: serde_json::from_str(&row.get::<String>(0)?)?,
                blackboard_defaults: serde_json::from_str(&row.get::<String>(1)?)?,
                editor_metadata: serde_json::from_str(&row.get::<String>(2)?)?,
                automatic_enabled: row.get::<i64>(6)? != 0,
                workspace_mappings: decode_workspace_mappings(row.get::<Option<String>>(7)?)?,
            };
            definitions.insert(flow_id, definition);
        }
        Ok(definitions)
    }

    pub(super) async fn load_definition_revision(
        &self,
        flow_id: FlowId,
        revision: i64,
    ) -> Result<FlowDefinition, FlowError> {
        let connection = connect_database(&self.database).await?;
        let mut rows = connection
            .query(LOAD_DEFINITION_REVISION_SQL, (flow_id.0.to_string(), revision))
            .await?;
        let row = rows.next().await?.ok_or(FlowError::DefinitionNotFound)?;
        Ok(FlowDefinition {
            flow_id,
            name: row.get(0)?,
            revision,
            graph: serde_json::from_str(&row.get::<String>(1)?)?,
            blackboard_defaults: serde_json::from_str(&row.get::<String>(2)?)?,
            editor_metadata: serde_json::from_str(&row.get::<String>(3)?)?,
            automatic_enabled: row.get::<i64>(4)? != 0,
            workspace_mappings: decode_workspace_mappings(row.get::<Option<String>>(5)?)?,
        })
    }

    pub(super) async fn assemble_execution_detail(
        &self,
        snapshot: ExecutionSnapshot,
    ) -> Result<FlowExecutionDetail, FlowError> {
        let flow = self
            .load_definition_revision(snapshot.execution.flow_id, snapshot.execution.revision)
            .await?;
        Ok(FlowExecutionDetail {
            flow,
            execution: snapshot.execution,
            nodes: snapshot.nodes,
            blackboard: snapshot.blackboard,
        })
    }

    pub(super) async fn load_triggers(&self) -> Result<HashMap<Uuid, FlowTriggerRecord>, FlowError> {
        let connection = connect_database(&self.database).await?;
        let mut rows = connection.query(LOAD_TRIGGERS_SQL, ()).await?;
        let mut triggers = HashMap::new();
        while let Some(row) = rows.next().await? {
            let trigger = decode_trigger(&row)?;
            let trigger_id = trigger.trigger_id;
            triggers.insert(trigger_id, trigger);
        }
        Ok(triggers)
    }

    pub(super) async fn list_triggers(&self, flow_id: FlowId) -> Result<Vec<FlowTriggerRecord>, FlowError> {
        let connection = connect_database(&self.database).await?;
        let mut rows = connection
            .query(LIST_TRIGGERS_SQL, (flow_id.0.to_string(),))
            .await?;
        let mut triggers = Vec::new();
        while let Some(row) = rows.next().await? {
            triggers.push(decode_trigger(&row)?);
        }
        Ok(triggers)
    }

    pub(super) async fn create_definition(&self, definition: &FlowDefinition) -> Result<(), FlowError> {
        validate_mappings(&definition.workspace_mappings)?;
        if definition.revision <= 0 {
            return Err(FlowError::RevisionConflict {
                expected: 1,
                actual: definition.revision,
            });
        }
        let mut connection = connect_database(&self.database).await?;
        let transaction = connection.transaction().await?;
        transaction
            .execute(INSERT_FLOW_SQL, (definition.flow_id.0.to_string(), definition.revision))
            .await?;
        transaction
            .execute(INSERT_FLOW_AUTHORITY_SQL, (definition.flow_id.0.to_string(),))
            .await?;
        insert_revision(&transaction, definition).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn initialize_authority_states(
        &self,
        flow_ids: impl IntoIterator<Item = FlowId>,
    ) -> Result<(), FlowError> {
        let mut connection = connect_database(&self.database).await?;
        let transaction = connection.transaction().await?;
        for flow_id in flow_ids {
            transaction
                .execute(INSERT_MISSING_FLOW_AUTHORITY_SQL, (flow_id.0.to_string(),))
                .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn authority_state(&self, flow_id: FlowId) -> Result<FlowAuthorityState, FlowError> {
        let connection = connect_database(&self.database).await?;
        let mut rows = connection
            .query(LOAD_FLOW_AUTHORITY_SQL, (flow_id.0.to_string(),))
            .await?;
        let row = rows.next().await?.ok_or(FlowError::DefinitionNotFound)?;
        decode_authority_state(&row.get::<String>(0)?)
    }

    pub(super) async fn set_authority_state(
        &self,
        flow_id: FlowId,
        state: FlowAuthorityState,
    ) -> Result<(), FlowError> {
        let connection = connect_database(&self.database).await?;
        let changed = connection
            .execute(SET_FLOW_AUTHORITY_SQL, (flow_id.0.to_string(), state.as_sql()))
            .await?;
        if changed != 1 {
            return Err(FlowError::DefinitionNotFound);
        }
        Ok(())
    }

    pub(super) async fn set_enabled(&self, flow_id: FlowId, revision: i64, enabled: bool) -> Result<(), FlowError> {
        let mut connection = connect_database(&self.database).await?;
        let transaction = connection.transaction().await?;
        let changed = transaction
            .execute(
                SET_FLOW_ENABLED_SQL,
                (flow_id.0.to_string(), revision, i64::from(enabled)),
            )
            .await?;
        if changed != 1 {
            return Err(FlowError::DefinitionNotFound);
        }
        if enabled {
            transaction
                .execute(RETARGET_FLOW_TRIGGERS_SQL, (flow_id.0.to_string(), revision))
                .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn update_definition(
        &self,
        definition: &FlowDefinition,
        expected_revision: i64,
    ) -> Result<(), FlowError> {
        validate_mappings(&definition.workspace_mappings)?;
        if definition.revision != expected_revision + 1 {
            return Err(FlowError::RevisionConflict {
                expected: expected_revision + 1,
                actual: definition.revision,
            });
        }
        let mut connection = connect_database(&self.database).await?;
        let transaction = connection.transaction().await?;
        insert_revision(&transaction, definition).await?;
        let changed = transaction
            .execute(
                UPDATE_FLOW_ACTIVE_REVISION_SQL,
                (definition.flow_id.0.to_string(), definition.revision, expected_revision),
            )
            .await?;
        if changed != 1 {
            return Err(FlowError::RevisionConflict {
                expected: expected_revision,
                actual: definition.revision,
            });
        }
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn delete_definition(&self, flow_id: FlowId, revision: i64) -> Result<(), FlowError> {
        let mut connection = connect_database(&self.database).await?;
        let transaction = connection.transaction().await?;
        let changed = transaction
            .execute(SOFT_DELETE_FLOW_SQL, (flow_id.0.to_string(), revision))
            .await?;
        if changed != 1 {
            return Err(FlowError::DefinitionNotFound);
        }
        transaction
            .execute(DISABLE_FLOW_TRIGGERS_SQL, (flow_id.0.to_string(),))
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn create_trigger(&self, trigger: &FlowTriggerRecord) -> Result<(), FlowError> {
        let connection = connect_database(&self.database).await?;
        connection
            .execute(INSERT_TRIGGER_SQL, trigger_values(trigger)?)
            .await?;
        Ok(())
    }

    pub(super) async fn update_trigger(&self, trigger: &FlowTriggerRecord) -> Result<(), FlowError> {
        let connection = connect_database(&self.database).await?;
        let values = trigger_values(trigger)?;
        let changed = connection.execute(UPDATE_TRIGGER_SQL, values).await?;
        if changed != 1 {
            return Err(FlowError::TriggerNotFound);
        }
        Ok(())
    }

    pub(super) async fn delete_trigger(&self, trigger_id: Uuid) -> Result<(), FlowError> {
        let connection = connect_database(&self.database).await?;
        let changed = connection
            .execute(DELETE_TRIGGER_SQL, (trigger_id.to_string(),))
            .await?;
        if changed != 1 {
            return Err(FlowError::TriggerNotFound);
        }
        Ok(())
    }

    pub(super) async fn save_mobility_snapshot(&self, snapshot: &FlowMobilitySnapshot) -> Result<(), FlowError> {
        if snapshot.flow_id != snapshot.definition.flow_id {
            return Err(FlowError::MobilitySnapshotMismatch {
                flow_id: snapshot.flow_id,
            });
        }

        let payload = serde_json::to_string(snapshot)?;
        let mut connection = connect_database(&self.database).await?;
        let transaction = connection.transaction().await?;
        transaction
            .execute(
                UPSERT_MOBILITY_SNAPSHOT_SQL,
                (snapshot.flow_id.0.to_string(), payload, snapshot.definition.revision),
            )
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Commits a staged Flow replica in foreign-key order as one inactive unit.
    pub(super) async fn commit_imported_flow(&self, snapshot: &FlowMobilitySnapshot) -> Result<(), FlowError> {
        snapshot
            .validate_selected_flow()
            .map_err(|flow_id| FlowError::MobilitySnapshotMismatch { flow_id })?;
        validate_mappings(&snapshot.definition.workspace_mappings)?;
        if snapshot.definition.revision <= 0
            || snapshot
                .executions
                .iter()
                .any(|execution| execution.execution.revision != snapshot.definition.revision)
            || snapshot
                .triggers
                .iter()
                .any(|trigger| trigger.flow_id != snapshot.flow_id || trigger.revision != snapshot.definition.revision)
        {
            return Err(FlowError::MobilitySnapshotMismatch {
                flow_id: snapshot.flow_id,
            });
        }

        let payload = serde_json::to_string(snapshot)?;
        let mut connection = connect_database(&self.database).await?;
        let transaction = connection.transaction().await?;
        let flow_id = snapshot.flow_id.0.to_string();
        let revision = snapshot.definition.revision;

        transaction
            .execute(INSERT_FLOW_SQL, (flow_id.clone(), revision))
            .await?;
        transaction
            .execute(INSERT_FLOW_AUTHORITY_SQL, (flow_id.clone(),))
            .await?;
        insert_revision(&transaction, &snapshot.definition).await?;
        for trigger in &snapshot.triggers {
            transaction
                .execute(INSERT_TRIGGER_SQL, trigger_values(trigger)?)
                .await?;
        }
        for execution in &snapshot.executions {
            insert_imported_execution(&transaction, execution).await?;
        }
        for reference in &snapshot.native_references {
            transaction
                .execute(
                    CREATE_EXECUTION_DOCUMENT_SQL,
                    (
                        reference.execution_id.0.to_string(),
                        reference.document_namespace_id.clone(),
                        reference.document_author_id.clone(),
                    ),
                )
                .await?;
        }
        transaction
            .execute(UPSERT_MOBILITY_SNAPSHOT_SQL, (flow_id.clone(), payload, revision))
            .await?;
        transaction
            .execute(
                SET_FLOW_AUTHORITY_SQL,
                (flow_id, FlowAuthorityState::InactiveCommitted.as_sql()),
            )
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn load_mobility_snapshot(
        &self,
        flow_id: FlowId,
    ) -> Result<Option<FlowMobilitySnapshot>, FlowError> {
        let connection = connect_database(&self.database).await?;
        let mut rows = connection
            .query(LOAD_MOBILITY_SNAPSHOT_SQL, (flow_id.0.to_string(),))
            .await?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        let snapshot: FlowMobilitySnapshot = serde_json::from_str(&row.get::<String>(0)?)?;
        if snapshot.flow_id != flow_id || snapshot.definition.flow_id != flow_id {
            return Err(FlowError::MobilitySnapshotMismatch { flow_id });
        }
        Ok(Some(snapshot))
    }
}

fn decode_workspace_mappings(value: Option<String>) -> Result<Vec<FlowWorkspaceMapping>, FlowError> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StoredMappings {
        Multiple(Vec<FlowWorkspaceMapping>),
        Legacy(FlowWorkspaceMapping),
    }
    Ok(match value {
        None => Vec::new(),
        Some(value) => match serde_json::from_str(&value)? {
            StoredMappings::Multiple(mappings) => mappings,
            StoredMappings::Legacy(mapping) => vec![mapping],
        },
    })
}

fn decode_authority_state(value: &str) -> Result<FlowAuthorityState, FlowError> {
    FlowAuthorityState::from_sql(value).ok_or_else(|| FlowError::UnknownAuthorityState {
        state: value.to_owned(),
    })
}

async fn insert_imported_execution(transaction: &Connection, snapshot: &ExecutionSnapshot) -> Result<(), FlowError> {
    let execution = &snapshot.execution;
    let changed = transaction
        .execute(UPSERT_EXECUTION_SQL, execution_values(snapshot)?)
        .await?;
    if changed != 1 {
        return Err(FlowError::MobilitySnapshotMismatch {
            flow_id: execution.flow_id,
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
    Ok(())
}

fn trigger_values(trigger: &FlowTriggerRecord) -> Result<impl IntoParams, FlowError> {
    Ok((
        trigger.trigger_id.to_string(),
        trigger.flow_id.0.to_string(),
        trigger.revision,
        trigger.adapter_kind.clone(),
        serde_json::to_string(&trigger.adapter_config)?,
        serde_json::to_string(&trigger.state)?,
        i64::from(trigger.enabled),
        trigger.next_fire_at.as_ref().map(FlowTimestamp::as_rfc3339),
        trigger
            .last_fired_at
            .as_ref()
            .map(FlowTimestamp::as_rfc3339),
    ))
}

/// Persists one paused definition revision, then its leaves in traversal order.
async fn insert_revision(transaction: &Connection, definition: &FlowDefinition) -> Result<(), FlowError> {
    let graph_json = serde_json::to_string(&definition.graph)?;
    let blackboard_defaults_json = serde_json::to_string(&definition.blackboard_defaults)?;
    let editor_metadata_json = serde_json::to_string(&definition.editor_metadata)?;
    let workspace_mapping_json = serde_json::to_string(&definition.workspace_mappings)?;
    let bindings_json = serde_json::to_string(&collect_bindings(&definition.graph.root))?;
    transaction
        .execute(
            INSERT_DEFINITION_REVISION_SQL,
            (
                definition.flow_id.0.to_string(),
                definition.revision,
                definition.name.clone(),
                graph_json,
                blackboard_defaults_json,
                bindings_json,
                editor_metadata_json,
                0_i64,
                workspace_mapping_json,
            ),
        )
        .await?;
    let mut leaves = Vec::new();
    definition
        .graph
        .root
        .for_each_leaf(&mut |node| leaves.push(node));
    for node in leaves {
        let (kind, label, constraints, data_space) = node_parts(node);
        transaction
            .execute(
                INSERT_FLOW_NODE_SQL,
                (
                    definition.flow_id.0.to_string(),
                    definition.revision,
                    node.id().0.to_string(),
                    Option::<String>::None,
                    kind,
                    label.to_owned(),
                    serde_json::to_string(constraints)?,
                    serde_json::to_string(data_space)?,
                    serde_json::to_string(node)?,
                ),
            )
            .await?;
    }
    Ok(())
}

fn node_parts(node: &FlowNode) -> (&str, &str, &NodeConstraints, &NodeDataSpace) {
    (
        node.node_kind.as_str(),
        node.label(),
        node.constraints(),
        node.data_space(),
    )
}

fn collect_bindings(root: &GraphNode) -> Vec<&BlackboardBinding> {
    let mut bindings = Vec::new();
    root.for_each_leaf(&mut |node| {
        let (inputs, outputs) = node.bindings();
        bindings.extend(inputs);
        bindings.extend(outputs);
    });
    bindings
}

fn decode_trigger(row: &Row) -> Result<FlowTriggerRecord, FlowError> {
    Ok(FlowTriggerRecord {
        trigger_id: Uuid::parse_str(&row.get::<String>(0)?)?,
        flow_id: FlowId(Uuid::parse_str(&row.get::<String>(1)?)?),
        revision: row.get(2)?,
        adapter_kind: row.get(3)?,
        adapter_config: serde_json::from_str(&row.get::<String>(4)?)?,
        state: serde_json::from_str(&row.get::<String>(5)?)?,
        enabled: row.get::<i64>(6)? != 0,
        next_fire_at: row
            .get::<Option<String>>(7)?
            .as_deref()
            .map(FlowTimestamp::parse)
            .transpose()?,
        last_fired_at: row
            .get::<Option<String>>(8)?
            .as_deref()
            .map(FlowTimestamp::parse)
            .transpose()?,
    })
}

#[cfg(test)]
mod tests {
    use super::super::graph::{FlowGraphAst, GraphNode};
    use super::super::mobility::FlowMobilitySnapshot;
    use super::super::types::FlowBootOptions;

    use super::*;

    #[tokio::test]
    async fn authority_state_persists_across_store_reads() {
        let directory = tempfile::tempdir().unwrap();
        let options = FlowBootOptions::open(directory.path().to_path_buf())
            .await
            .unwrap();
        let flow_id = FlowId(uuid::Uuid::new_v4());
        let connection = connect_database(&options.database).await.unwrap();
        connection
            .execute(
                "INSERT INTO flows (flow_id, active_revision, deleted_at) VALUES (?1, 1, NULL)",
                (flow_id.0.to_string(),),
            )
            .await
            .unwrap();
        let store = FlowStore::new(options.database);

        store.initialize_authority_states([flow_id]).await.unwrap();
        assert_eq!(
            store.authority_state(flow_id).await.unwrap(),
            FlowAuthorityState::Authoritative
        );

        store
            .set_authority_state(flow_id, FlowAuthorityState::InactiveCommitted)
            .await
            .unwrap();
        assert_eq!(
            store
                .load_mobility_states()
                .await
                .unwrap()
                .into_iter()
                .find(|state| state.flow_id == flow_id)
                .map(|state| state.authority_state),
            Some(FlowAuthorityState::InactiveCommitted)
        );
    }

    #[tokio::test]
    async fn mobility_snapshot_round_trips_through_flow_store() {
        let directory = tempfile::tempdir().unwrap();
        let options = FlowBootOptions::open(directory.path().to_path_buf())
            .await
            .unwrap();
        let flow_id = FlowId(uuid::Uuid::new_v4());
        let connection = connect_database(&options.database).await.unwrap();
        connection
            .execute(
                "INSERT INTO flows (flow_id, active_revision, deleted_at) VALUES (?1, 1, NULL)",
                (flow_id.0.to_string(),),
            )
            .await
            .unwrap();
        let store = FlowStore::new(options.database);
        let snapshot = FlowMobilitySnapshot {
            flow_id,
            definition: FlowDefinition {
                flow_id,
                name: "Mobility persistence test".to_owned(),
                revision: 1,
                graph: FlowGraphAst {
                    version: 1,
                    root: GraphNode::Sequence { children: Vec::new() },
                },
                blackboard_defaults: serde_json::json!({"exchange": "paper"}),
                editor_metadata: serde_json::json!({"zoom": 1}),
                automatic_enabled: true,
                workspace_mappings: Vec::new(),
            },
            triggers: Vec::new(),
            executions: Vec::new(),
            native_references: Vec::new(),
        };

        store.save_mobility_snapshot(&snapshot).await.unwrap();
        let restored = store
            .load_mobility_snapshot(flow_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(restored.flow_id, flow_id);
        assert_eq!(restored.definition.revision, 1);
        assert_eq!(
            restored.definition.blackboard_defaults,
            serde_json::json!({"exchange": "paper"})
        );
    }

    #[tokio::test]
    async fn empty_drafts_save_and_reload_paused_without_runnable_validation() {
        use super::super::graph::{FlowGraphAst, GraphNode};
        use super::super::types::{FlowBootOptions, FlowDefinition, FlowId};

        let directory = tempfile::tempdir().unwrap();
        let options = FlowBootOptions::open(directory.path().to_path_buf())
            .await
            .unwrap();
        let store = super::FlowStore::new(options.database);
        let mut draft = FlowDefinition {
            flow_id: FlowId(uuid::Uuid::new_v4()),
            name: "Unfinished job".to_owned(),
            revision: 1,
            graph: FlowGraphAst {
                version: 1,
                root: GraphNode::Sequence { children: Vec::new() },
            },
            blackboard_defaults: serde_json::json!({}),
            editor_metadata: serde_json::json!({}),
            automatic_enabled: true,
            workspace_mappings: Vec::new(),
        };
        store.create_definition(&draft).await.unwrap();
        let saved = store
            .load_definition_revision(draft.flow_id, 1)
            .await
            .unwrap();
        assert!(!saved.automatic_enabled);
        assert!(saved.graph.validate().is_err());
        assert!(
            store
                .load_definitions()
                .await
                .unwrap()
                .contains_key(&draft.flow_id)
        );

        draft.revision = 2;
        store.update_definition(&draft, 1).await.unwrap();
        let saved = store
            .load_definition_revision(draft.flow_id, 2)
            .await
            .unwrap();
        assert!(!saved.automatic_enabled);
        assert_eq!(saved.graph, draft.graph);
    }

    #[test]
    fn old_root_mapping_and_multiple_destinations_decode() {
        let old = decode_workspace_mappings(Some(r#"{"directory":"notes","mode":"ReadOnly"}"#.to_owned())).unwrap();
        assert_eq!(old.len(), 1);
        assert_eq!(old[0].destination, "");
        let mappings = decode_workspace_mappings(Some(r#"[{"directory":"notes","destination":"input","mode":"ReadOnly"},{"directory":"reports","destination":"output","mode":"ReadWrite"}]"#.to_owned())).unwrap();
        assert_eq!(mappings.len(), 2);
        assert_eq!(mappings[0].destination, "input");
        assert_eq!(mappings[1].destination, "output");
        assert!(decode_workspace_mappings(None).unwrap().is_empty());
        assert!(decode_workspace_mappings(Some("invalid".to_owned())).is_err());
    }
}
