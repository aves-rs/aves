use chrono::Utc;
use uuid::Uuid;

use super::actor::Actor;
use super::ast::FlowNode;
use super::controllers::validate_parallel_composition;
use super::cron::FlowTriggerRecord;
use super::graph::{FlowGraph, GraphNode, contains_node, remove_node, replace_node};
use super::messages::{FlowBroadcast, FlowResponse};
use super::mobility::FlowAuthorityState;
use super::types::{FlowDefinition, FlowError, FlowId, FlowNodeId};

impl Actor {
    fn definition(&self, flow_id: FlowId) -> Result<&FlowDefinition, FlowError> {
        self.definitions
            .get(&flow_id)
            .ok_or(FlowError::DefinitionNotFound)
    }

    pub(super) async fn create_flow(&mut self, mut flow: FlowDefinition) -> Result<FlowResponse, FlowError> {
        if self.definitions.contains_key(&flow.flow_id) {
            return Err(FlowError::DefinitionAlreadyExists);
        }
        flow.automatic_enabled = false;
        self.store.create_definition(&flow).await?;
        self.mobility
            .set_authority_state(flow.flow_id, FlowAuthorityState::Authoritative);
        self.definitions.insert(flow.flow_id, flow.clone());
        self.broadcast_definition(flow.clone()).await;
        Ok(FlowResponse::FlowCreated { flow })
    }

    pub(super) async fn create_trigger(&mut self, mut trigger: FlowTriggerRecord) -> Result<FlowResponse, FlowError> {
        self.trigger_registry.validate_configuration(&trigger)?;
        self.ensure_local_work(trigger.flow_id)?;
        let definition = self.definition(trigger.flow_id)?;
        if definition.revision != trigger.revision {
            return Err(FlowError::RevisionConflict {
                expected: definition.revision,
                actual: trigger.revision,
            });
        }
        if let Some(configuration) = trigger.cron_config()? {
            if trigger.enabled && trigger.next_fire_at.is_none() {
                trigger.next_fire_at = Some(configuration.cron.next_after(&Utc::now())?);
            }
        } else {
            trigger.next_fire_at = None;
            trigger.last_fired_at = None;
        }
        self.store.create_trigger(&trigger).await?;
        if trigger.enabled {
            self.triggers.insert(trigger.trigger_id, trigger.clone());
        }
        self.resynchronize_if_needed(trigger.flow_id).await?;
        Ok(FlowResponse::FlowTriggerCreated { trigger })
    }

    pub(super) async fn create_node(&mut self, flow_id: FlowId, node: FlowNode) -> Result<FlowResponse, FlowError> {
        self.ensure_local_work(flow_id)?;
        let mut definition = self.definition(flow_id)?.clone();
        if contains_node(&definition.graph.root, node.id()) {
            return Err(FlowError::RevisionConflict {
                expected: definition.revision + 1,
                actual: definition.revision,
            });
        }
        let old_revision = definition.revision;
        definition.revision += 1;
        definition.automatic_enabled = false;
        definition.graph.root = match definition.graph.root {
            GraphNode::Sequence { mut children } => {
                children.push(GraphNode::Leaf {
                    node: Box::new(node.clone()),
                });
                GraphNode::Sequence { children }
            }
            root => GraphNode::Sequence {
                children: vec![
                    root,
                    GraphNode::Leaf {
                        node: Box::new(node.clone()),
                    },
                ],
            },
        };
        self.commit_node_revision(definition, old_revision).await?;
        Ok(FlowResponse::FlowNodeCreated { node })
    }

    pub(super) fn get_flow(&self, flow_id: FlowId) -> Result<FlowResponse, FlowError> {
        Ok(FlowResponse::FlowRetrieved {
            flow: self.definition(flow_id)?.clone(),
        })
    }

    pub(super) async fn list_triggers(&mut self, flow_id: FlowId) -> Result<FlowResponse, FlowError> {
        if !self.definitions.contains_key(&flow_id) {
            return Err(FlowError::DefinitionNotFound);
        }
        Ok(FlowResponse::FlowTriggersListed {
            triggers: self.store.list_triggers(flow_id).await?,
        })
    }

    async fn trigger_flow_id(&self, trigger_id: Uuid) -> Result<FlowId, FlowError> {
        if let Some(trigger) = self.triggers.get(&trigger_id) {
            return Ok(trigger.flow_id);
        }
        for flow_id in self.definitions.keys().copied() {
            if self
                .store
                .list_triggers(flow_id)
                .await?
                .into_iter()
                .any(|trigger| trigger.trigger_id == trigger_id)
            {
                return Ok(flow_id);
            }
        }
        Err(FlowError::TriggerNotFound)
    }

    async fn broadcast_definition(&mut self, definition: FlowDefinition) {
        let _ = self
            .broadcast_tx
            .send(FlowBroadcast::DefinitionChanged {
                flow: Box::new(definition),
            })
            .await;
    }

    pub(super) async fn update_flow(&mut self, mut flow: FlowDefinition) -> Result<FlowResponse, FlowError> {
        self.ensure_local_work(flow.flow_id)?;
        let revision = self.definition(flow.flow_id)?.revision;
        flow.automatic_enabled = false;
        self.store.update_definition(&flow, revision).await?;
        self.definitions.insert(flow.flow_id, flow.clone());
        self.broadcast_definition(flow.clone()).await;
        self.resynchronize_if_needed(flow.flow_id).await?;
        Ok(FlowResponse::FlowUpdated { flow })
    }

    pub(super) async fn update_trigger(&mut self, mut trigger: FlowTriggerRecord) -> Result<FlowResponse, FlowError> {
        self.trigger_registry.validate_configuration(&trigger)?;
        self.ensure_local_work(trigger.flow_id)?;
        if !self.definitions.contains_key(&trigger.flow_id) {
            return Err(FlowError::DefinitionNotFound);
        }

        let existing = match self.triggers.get(&trigger.trigger_id).cloned() {
            Some(trigger) => Some(trigger),
            None => self
                .store
                .list_triggers(trigger.flow_id)
                .await?
                .into_iter()
                .find(|stored| stored.trigger_id == trigger.trigger_id),
        };
        let cron_changed = existing
            .as_ref()
            .map(|stored| {
                stored.adapter_kind != trigger.adapter_kind || stored.adapter_config != trigger.adapter_config
            })
            .unwrap_or(true);
        trigger.last_fired_at = existing
            .as_ref()
            .and_then(|stored| stored.last_fired_at.clone());
        if let Some(configuration) = trigger.cron_config()? {
            if trigger.enabled {
                let now = Utc::now();
                trigger.next_fire_at = if cron_changed {
                    Some(configuration.cron.next_after(&now)?)
                } else {
                    match existing
                        .as_ref()
                        .and_then(|stored| stored.next_fire_at.clone())
                    {
                        Some(next) => Some(next),
                        None => Some(configuration.cron.next_after(&now)?),
                    }
                };
            } else {
                trigger.next_fire_at = existing
                    .as_ref()
                    .and_then(|stored| stored.next_fire_at.clone());
            }
        } else {
            trigger.next_fire_at = None;
            trigger.last_fired_at = None;
        }
        self.store.update_trigger(&trigger).await?;
        if trigger.enabled {
            self.triggers.insert(trigger.trigger_id, trigger.clone());
        } else {
            self.triggers.remove(&trigger.trigger_id);
        }
        self.resynchronize_if_needed(trigger.flow_id).await?;
        Ok(FlowResponse::FlowTriggerUpdated { trigger })
    }

    pub(super) async fn update_node(&mut self, flow_id: FlowId, node: FlowNode) -> Result<FlowResponse, FlowError> {
        self.ensure_local_work(flow_id)?;
        let mut definition = self.definition(flow_id)?.clone();
        if !replace_node(&mut definition.graph.root, &node) {
            return Err(FlowError::NodeNotFound);
        }
        let old_revision = definition.revision;
        definition.revision += 1;
        definition.automatic_enabled = false;
        self.commit_node_revision(definition, old_revision).await?;
        Ok(FlowResponse::FlowNodeUpdated { node })
    }

    pub(super) async fn set_flow_enabled(&mut self, flow_id: FlowId, enabled: bool) -> Result<FlowResponse, FlowError> {
        self.ensure_local_work(flow_id)?;
        let mut flow = self.definition(flow_id)?.clone();
        if enabled {
            let graph = FlowGraph::load_json(&serde_json::to_string(&flow.graph)?)?;
            graph.ast().validate_with_registry(&self.node_registry)?;
            validate_parallel_composition(&graph, &self.node_registry)?;
        }
        self.store
            .set_enabled(flow_id, flow.revision, enabled)
            .await?;
        flow.automatic_enabled = enabled;
        if enabled {
            for trigger in self
                .triggers
                .values_mut()
                .filter(|trigger| trigger.flow_id == flow_id)
            {
                trigger.revision = flow.revision;
            }
        }
        self.definitions.insert(flow_id, flow.clone());
        self.broadcast_definition(flow.clone()).await;
        Ok(FlowResponse::FlowUpdated { flow })
    }

    pub(super) async fn resynchronize_if_needed(&mut self, flow_id: FlowId) -> Result<(), FlowError> {
        if self.mobility.authority_state(flow_id) == Some(FlowAuthorityState::Synchronizing) {
            self.synchronize_flow(flow_id).await?;
        }
        Ok(())
    }

    async fn commit_node_revision(&mut self, definition: FlowDefinition, old_revision: i64) -> Result<(), FlowError> {
        let flow_id = definition.flow_id;
        self.store
            .update_definition(&definition, old_revision)
            .await?;
        self.definitions.insert(flow_id, definition.clone());
        self.broadcast_definition(definition).await;
        self.resynchronize_if_needed(flow_id).await
    }

    pub(super) async fn delete_flow(&mut self, flow_id: FlowId) -> Result<FlowResponse, FlowError> {
        self.ensure_local_work(flow_id)?;
        let revision = self.definition(flow_id)?.revision;
        self.store.delete_definition(flow_id, revision).await?;
        self.definitions.remove(&flow_id);
        self.triggers
            .retain(|_, trigger| trigger.flow_id != flow_id);
        Ok(FlowResponse::FlowDeleted { flow_id })
    }

    pub(super) async fn delete_trigger(&mut self, trigger_id: Uuid) -> Result<FlowResponse, FlowError> {
        let flow_id = self.trigger_flow_id(trigger_id).await?;
        self.ensure_local_work(flow_id)?;
        self.store.delete_trigger(trigger_id).await?;
        self.triggers.remove(&trigger_id);
        self.resynchronize_if_needed(flow_id).await?;
        Ok(FlowResponse::FlowTriggerDeleted { trigger_id })
    }

    pub(super) async fn delete_node(
        &mut self,
        flow_id: FlowId,
        node_id: FlowNodeId,
    ) -> Result<FlowResponse, FlowError> {
        self.ensure_local_work(flow_id)?;
        let mut definition = self.definition(flow_id)?.clone();
        if matches!(&definition.graph.root, GraphNode::Leaf { node } if node.id() == node_id) {
            definition.graph.root = GraphNode::Sequence { children: Vec::new() };
        } else if !remove_node(&mut definition.graph.root, node_id) {
            return Err(FlowError::NodeNotFound);
        }
        let old_revision = definition.revision;
        definition.revision += 1;
        definition.automatic_enabled = false;
        self.commit_node_revision(definition, old_revision).await?;
        Ok(FlowResponse::FlowNodeDeleted { flow_id, node_id })
    }
}
