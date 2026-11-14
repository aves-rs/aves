use std::path::PathBuf;
use std::sync::Arc;

use arc_malachitebft_app_channel::app::{
    config::NodeConfig as _,
    metrics::{Metrics, SharedRegistry},
    spawn::{spawn_consensus_actor, spawn_node_actor, spawn_sync_actor, spawn_wal_actor},
};
use arc_malachitebft_app_channel::{AppMsg, ConsensusContext, EngineHandle, NetworkMsg};
use arc_malachitebft_core_types::ValidatorSet as _;
use arc_malachitebft_engine::network::NetworkRef;
use arc_malachitebft_engine::util::output_port::OutputPortSubscriberTrait as _;
use arc_malachitebft_engine::{
    host::HostMsg,
    util::{events::TxEvent, output_port::OutputPort},
};
use ractor::{Actor as RactorActor, RpcReplyPort};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::{JoinError, JoinSet},
};

use crate::auth::AuthAdmissionRegistry;
use crate::machine::MachinePeerService;
use crate::runtime::ServiceReader;

use super::abci::CoordinationApplication;
use super::messages::{CoordinationBroadcast, CoordinationRequest, CoordinationRequestPayload, CoordinationResponse};
use super::network::{ApplicationEvent, NetworkAttachment, encode_transaction};
use super::persistence::CoordinationStore;
use super::serialization::CoordinationJsonCodec;
use super::signing::{CoordinationSigner, CoordinationVerifier};
use super::transaction::{ConsensusMessageAdapter, PendingTransaction};
use super::types::{CoordinationContext, CoordinationError, CoordinationNodeConfig, PublicKey, Regenesis, SignedBody};

struct RunningEngine {
    callbacks: mpsc::Receiver<HostMsg<CoordinationContext>>,
    engine: EngineHandle,
    transport: NetworkAttachment,
}

/// Owns coordination requests and the coordination ABCI application.
pub(super) struct Actor {
    requests: mpsc::Receiver<CoordinationRequest>,
    broadcasts: mpsc::Sender<CoordinationBroadcast>,
    abci: CoordinationApplication,
    sync_services: Option<Arc<dyn MachinePeerService>>,
    sync_services_reader: Option<ServiceReader<dyn MachinePeerService>>,
    admission: Option<AuthAdmissionRegistry>,
    config_dir: PathBuf,
    running: Option<RunningEngine>,
    native_changes: Option<watch::Receiver<()>>,
    server_requested: bool,
    restart_after_recovery: bool,
    replies: JoinSet<()>,
}

impl Actor {
    /// Opens coordination persistence once and loads its recovery unit.
    pub(super) async fn new(
        requests: mpsc::Receiver<CoordinationRequest>,
        config_dir: PathBuf,
        sync_services: Option<Arc<dyn MachinePeerService>>,
        admission: Option<AuthAdmissionRegistry>,
        broadcasts: mpsc::Sender<CoordinationBroadcast>,
    ) -> Self {
        let store = CoordinationStore::open(&config_dir)
            .await
            .expect("coordination database must open and migrate");

        let recovery = store
            .load()
            .await
            .expect("coordination recovery state must load");

        let abci = CoordinationApplication::with_store(store, recovery).expect("coordination ABCI state must restore");

        Self {
            requests,
            broadcasts,
            abci,
            sync_services,
            sync_services_reader: None,
            admission,
            config_dir,
            running: None,
            native_changes: None,
            server_requested: false,
            restart_after_recovery: false,
            replies: JoinSet::new(),
        }
    }

    pub(super) fn set_peer_services_reader(&mut self, reader: ServiceReader<dyn MachinePeerService>) {
        self.sync_services_reader = Some(reader);
    }

    fn peer_services(&self) -> Option<Arc<dyn MachinePeerService>> {
        self.sync_services_reader
            .as_ref()
            .and_then(ServiceReader::get)
            .or_else(|| self.sync_services.clone())
    }

    /// Routes typed coordination operations to the configured chain-host interface.
    async fn handle_message(
        &mut self,
        payload: CoordinationRequestPayload,
    ) -> Result<CoordinationResponse, CoordinationError> {
        let abci = &mut self.abci;

        match payload {
            CoordinationRequestPayload::StartServer => {
                self.start_server().await?;
                self.server_requested = true;
                Ok(CoordinationResponse::ServerStarted)
            }
            CoordinationRequestPayload::StopServer => {
                self.server_requested = false;
                self.restart_after_recovery = false;
                self.stop_server()
                    .await
                    .map(|()| CoordinationResponse::ServerStopped)
            }
            CoordinationRequestPayload::Genesis { genesis } => abci
                .create_genesis(genesis.validators)
                .await
                .map(|record| CoordinationResponse::Chain { record }),
            CoordinationRequestPayload::ExportSnapshot { domain } => abci
                .export_chain_snapshot(domain)
                .await
                .map(|snapshot| CoordinationResponse::Snapshot { snapshot }),
            CoordinationRequestPayload::AdoptSnapshot {
                snapshot,
                expected_hash,
            } => abci
                .adopt_snapshot(snapshot, expected_hash)
                .await
                .map(|record| CoordinationResponse::Chain { record }),
            CoordinationRequestPayload::RegisterUser { record } => {
                self.submit(PendingTransaction::UserRegistered(record))
                    .await
            }
            CoordinationRequestPayload::RotateUserEncryptionKey { record } => {
                self.submit(PendingTransaction::UserEncRotated(record))
                    .await
            }
            CoordinationRequestPayload::RotateUserRootKey { record } => {
                self.submit(PendingTransaction::UserRootRotated(record))
                    .await
            }
            CoordinationRequestPayload::LinkUserSuccessor { record } => {
                self.submit(PendingTransaction::UserSuccessorLinked(record))
                    .await
            }
            CoordinationRequestPayload::RevokeUser { record } => {
                self.submit(PendingTransaction::UserRevoked(record)).await
            }
            CoordinationRequestPayload::RegisterMachine { record } => {
                self.submit(PendingTransaction::MachineRegistered(record))
                    .await
            }
            CoordinationRequestPayload::BindMachine { record } => {
                self.submit(PendingTransaction::MachineBound(record)).await
            }
            CoordinationRequestPayload::UnbindMachine { record } => {
                self.submit(PendingTransaction::MachineUnbound(record))
                    .await
            }
            CoordinationRequestPayload::CreateExecution { record } => {
                self.submit(PendingTransaction::ExecutionCreated(record))
                    .await
            }
            CoordinationRequestPayload::LandExecution { record } => {
                self.submit(PendingTransaction::ExecutionLanded(record))
                    .await
            }
            CoordinationRequestPayload::CancelExecution { record } => {
                self.submit(PendingTransaction::ExecutionCancelled(record))
                    .await
            }
            CoordinationRequestPayload::ActivateSchema { record } => {
                self.submit(PendingTransaction::SchemaActivated(record))
                    .await
            }
            CoordinationRequestPayload::Regenesis { record } => self.regenesis(record).await,
            CoordinationRequestPayload::GetUser { user } => abci
                .get_user(user)
                .map(|record| CoordinationResponse::User { record }),
            CoordinationRequestPayload::GetMachine { machine } => abci
                .get_machine(machine)
                .map(|record| CoordinationResponse::Machine { record }),
            CoordinationRequestPayload::ListMachines => abci
                .list_machines()
                .map(|records| CoordinationResponse::MachinesList { records }),
            CoordinationRequestPayload::GetExecution { execution } => abci
                .get_execution(execution)
                .map(|record| CoordinationResponse::Execution { record }),
            CoordinationRequestPayload::ListExecutions => abci
                .list_executions()
                .map(|records| CoordinationResponse::ExecutionsList { records }),
            CoordinationRequestPayload::GetSchema { at } => abci
                .get_schema(at)
                .map(|record| CoordinationResponse::Schema { record }),
            CoordinationRequestPayload::GetSchemaHistory => abci
                .get_schema_history()
                .map(|records| CoordinationResponse::SchemaHistory { records }),
            CoordinationRequestPayload::GetChain => abci
                .get_chain()
                .map(|record| CoordinationResponse::Chain { record }),
            CoordinationRequestPayload::GetRegenesis => abci
                .get_regenesis()
                .map(|record| CoordinationResponse::Regenesis { record }),
        }
    }

    /// Routes an engine callback or closes the stopped engine's resources.
    async fn handle_consensus_message(&mut self, message: Option<HostMsg<CoordinationContext>>) {
        let result = match message {
            Some(message) => {
                let Some(running) = self.running.as_ref() else { return };
                let network = running.transport.sender.clone();
                self.dispatch_consensus_message(message, &network).await
            }
            None => self.stop_server().await,
        };
        if let Err(error) = result {
            tracing::error!(%error, "coordination consensus handling failed");
            if let Err(cleanup) = self.stop_server().await {
                tracing::error!(%cleanup, "coordination consensus cleanup failed");
            }
        }
    }

    /// Dispatches the application request without waiting for deferred replies.
    async fn dispatch_consensus_message(
        &mut self,
        message: HostMsg<CoordinationContext>,
        network: &mpsc::Sender<NetworkMsg<CoordinationContext>>,
    ) -> Result<(), CoordinationError> {
        macro_rules! callback {
            ($port:ident, $variant:ident { $($field:ident),* $(,)? }) => {{
                let (reply, receiver) = oneshot::channel();
                self.abci.handle_app_msg(AppMsg::$variant { $($field,)* reply }, network).await?;
                self.forward_reply(receiver, $port, None);
            }};
        }
        match message {
            HostMsg::ConsensusReady { reply_to } => callback!(reply_to, ConsensusReady {}),
            HostMsg::StartedRound {
                height,
                round,
                proposer,
                role,
                reply_to,
            } => {
                let (reply_value, receiver) = oneshot::channel();
                self.abci
                    .handle_app_msg(
                        AppMsg::StartedRound {
                            height,
                            round,
                            proposer,
                            role,
                            reply_value,
                        },
                        network,
                    )
                    .await?;
                self.forward_reply(receiver, reply_to, None);
            }
            HostMsg::GetValue {
                height,
                round,
                timeout,
                reply_to,
            } => {
                let (reply, receiver) = oneshot::channel();
                self.abci
                    .handle_app_msg(
                        AppMsg::GetValue {
                            height,
                            round,
                            timeout,
                            reply,
                        },
                        network,
                    )
                    .await?;
                self.forward_reply(receiver, reply_to, Some(timeout));
            }
            HostMsg::ExtendVote {
                height,
                round,
                value_id,
                vote_extension_policy,
                reply_to,
            } => {
                callback!(
                    reply_to,
                    ExtendVote {
                        height,
                        round,
                        value_id,
                        vote_extension_policy
                    }
                );
            }
            HostMsg::VerifyVoteExtensions {
                height,
                round,
                value_id,
                extensions,
                reply_to,
            } => {
                callback!(
                    reply_to,
                    VerifyVoteExtensions {
                        height,
                        round,
                        value_id,
                        extensions
                    }
                );
            }
            HostMsg::RestreamValue {
                height,
                round,
                valid_round,
                address,
                value_id,
            } => {
                self.abci
                    .handle_app_msg(
                        AppMsg::RestreamProposal {
                            height,
                            round,
                            valid_round,
                            address,
                            value_id,
                        },
                        network,
                    )
                    .await?;
            }
            HostMsg::GetHistoryMinHeight { reply_to } => callback!(reply_to, GetHistoryMinHeight {}),
            HostMsg::ReceivedProposalPart {
                from,
                published_by,
                part,
                reply_to,
            } => {
                let (reply, receiver) = oneshot::channel();
                self.abci
                    .handle_app_msg(
                        AppMsg::ReceivedProposalPart {
                            from,
                            published_by,
                            part,
                            reply,
                        },
                        network,
                    )
                    .await?;
                self.replies.spawn(async move {
                    if let Ok(Some(value)) = receiver.await {
                        let _ = reply_to.send(value);
                    }
                });
            }
            HostMsg::Decided {
                certificate,
                extensions,
                reply_to,
            } => {
                callback!(
                    reply_to,
                    Decided {
                        certificate,
                        extensions
                    }
                );
            }
            HostMsg::Finalized {
                certificate,
                extensions,
                evidence,
                reply_to,
            } => {
                callback!(
                    reply_to,
                    Finalized {
                        certificate,
                        extensions,
                        evidence
                    }
                );
            }
            HostMsg::GetDecidedValues { range, reply_to } => callback!(reply_to, GetDecidedValues { range }),
            HostMsg::ProcessSyncedValue {
                height,
                round,
                proposer,
                value_bytes,
                reply_to,
            } => {
                callback!(
                    reply_to,
                    ProcessSyncedValue {
                        height,
                        round,
                        proposer,
                        value_bytes
                    }
                );
            }
        }
        Ok(())
    }

    /// Attaches the shared native transport before starting Malachite's own actors.
    async fn start_server(&mut self) -> Result<(), CoordinationError> {
        if self.running.is_some() {
            return Ok(());
        }
        let services = self
            .peer_services()
            .expect("Machine domain must publish MachinePeerService before Coordination starts");
        if self.native_changes.is_none() {
            self.native_changes = Some(services.subscribe_native_changes());
        }
        let admission = self
            .admission
            .clone()
            .ok_or(CoordinationError::NotImplemented)?;
        let (domain, _) = self.abci.active_chain_consensus_params()?;
        let transport = NetworkAttachment::start(services.as_ref(), admission, domain).await?;
        match self.build_engine(transport.actor.clone()).await {
            Ok((callbacks, engine)) => {
                self.running = Some(RunningEngine {
                    callbacks,
                    engine,
                    transport,
                });
                let _ = self
                    .broadcasts
                    .send(CoordinationBroadcast::ServerStarted)
                    .await;
                Ok(())
            }
            Err(error) => {
                if let Err(cleanup) = transport.stop().await {
                    tracing::error!(%cleanup, "coordination failed-start transport cleanup failed");
                }
                Err(error)
            }
        }
    }

    /// Builds an engine against the caller's attached coordination network.
    async fn build_engine(
        &self,
        network: NetworkRef<CoordinationContext>,
    ) -> Result<(mpsc::Receiver<HostMsg<CoordinationContext>>, EngineHandle), CoordinationError> {
        let services = self
            .peer_services()
            .ok_or(CoordinationError::NotImplemented)?;
        let (domain, validators) = self.abci.active_chain_consensus_params()?;
        let key = services.transport().await.0.secret_key().clone();
        let address = PublicKey(*key.public().as_bytes());
        let verifier = Box::new(CoordinationVerifier::new(domain));
        let consensus = if validators.get_by_address(&address).is_some() {
            ConsensusContext::new_validator(address, verifier, Box::new(CoordinationSigner::new(key, domain)))
        } else {
            ConsensusContext::new_full_node(address, verifier)
        };
        let wal_path = self
            .config_dir
            .join(format!("coordination-{}.wal", PublicKey(*domain.as_bytes())));
        let config = CoordinationNodeConfig::new();
        let codec = CoordinationJsonCodec::new();
        let registry = SharedRegistry::global().with_moniker(config.moniker());
        let metrics = Metrics::register(&registry);
        let (node, node_task) = spawn_node_actor(metrics.clone())
            .await
            .map_err(|error| CoordinationError::Engine(error.into()))?;
        let result = async {
            network.link(node.get_cell());
            let wal = spawn_wal_actor(&CoordinationContext, codec, &wal_path, &registry, node.clone())
                .await
                .map_err(|error| CoordinationError::Engine(error.into()))?;
            wal.link(node.get_cell());
            let (sender, callbacks) = mpsc::channel(128);
            let (host, _) = RactorActor::spawn(None, ConsensusMessageAdapter, sender)
                .await
                .map_err(|error| CoordinationError::Engine(error.into()))?;
            host.link(node.get_cell());
            let sync_port = Arc::new(OutputPort::new());
            let consensus = spawn_consensus_actor(
                CoordinationContext,
                consensus.address,
                config.consensus().clone(),
                consensus.verifier,
                consensus.signer,
                network.clone(),
                host.clone(),
                wal,
                sync_port.clone(),
                metrics,
                TxEvent::new(),
                node.clone(),
            )
            .await
            .map_err(|error| CoordinationError::Engine(error.into()))?;
            consensus.link(node.get_cell());
            if let Some(sync) = spawn_sync_actor(
                CoordinationContext,
                network,
                host,
                consensus,
                codec,
                config.value_sync(),
                &registry,
            )
            .await
            .map_err(|error| CoordinationError::Engine(error.into()))?
            {
                sync.link(node.get_cell());
                sync.subscribe_to_port(&sync_port);
            }
            Ok(callbacks)
        }
        .await;
        let engine = EngineHandle::new(node, node_task);
        match result {
            Ok(callbacks) => Ok((callbacks, engine)),
            Err(error) => {
                if let Err(cleanup) = Self::stop_engine(engine).await {
                    tracing::error!(%cleanup, "coordination failed-start engine cleanup failed");
                }
                Err(error)
            }
        }
    }

    /// Owns deferred replies and lets Malachite's proposal deadline cancel idle work.
    fn forward_reply<T: Send + 'static>(
        &mut self,
        receiver: oneshot::Receiver<T>,
        reply: RpcReplyPort<T>,
        timeout: Option<std::time::Duration>,
    ) {
        self.replies.spawn(async move {
            let value = match timeout {
                Some(timeout) => tokio::time::timeout(timeout, receiver)
                    .await
                    .ok()
                    .and_then(Result::ok),
                None => receiver.await.ok(),
            };
            if let Some(value) = value {
                let _ = reply.send(value);
            }
        });
    }

    /// Acknowledges an admitted transaction only after the ABCI durably records it.
    async fn submit(&mut self, transaction: PendingTransaction) -> Result<CoordinationResponse, CoordinationError> {
        let encoded = self.encode_for_network(&transaction).await?;
        let event_id = self.abci.submit(transaction.clone()).await?;
        if let (Some(running), Some(encoded)) = (&self.running, encoded)
            && running.transport.transactions.send(encoded).await.is_err()
        {
            tracing::warn!("coordination transport closed before transaction publication");
        }
        Ok(CoordinationResponse::TransactionSubmitted { event_id })
    }

    async fn encode_for_network(&self, transaction: &PendingTransaction) -> Result<Option<Vec<u8>>, CoordinationError> {
        let max_message_size = if let Some(running) = &self.running {
            running.transport.max_message_size
        } else if let Some(services) = self.peer_services() {
            services
                .transport()
                .await
                .1
                .ok_or(super::network::ChainNetworkError::GossipUnavailable)?
                .max_message_size()
        } else {
            return Ok(None);
        };
        Ok(Some(encode_transaction(transaction, max_message_size)?))
    }

    /// Accepts only currently admitted validator neighbors and uses normal durable admission.
    async fn handle_application_event(&mut self, event: Option<ApplicationEvent>) {
        let Some(event) = event else {
            tracing::error!("coordination application event channel closed");
            if let Err(error) = self.stop_server().await {
                tracing::error!(%error, "coordination transport cleanup failed");
            }
            return;
        };
        let peer = match &event {
            ApplicationEvent::NeighborUp(peer) | ApplicationEvent::Transaction(peer, _) => *peer,
        };
        let admitted = self.admission.as_ref().is_some_and(|registry| {
            registry
                .current_issuers()
                .iter()
                .any(|issuer| issuer.issuer.public_key() == peer)
        });
        let validator = self
            .abci
            .active_chain_consensus_params()
            .ok()
            .is_some_and(|(_, validators)| {
                validators
                    .get_by_address(&PublicKey(*peer.as_bytes()))
                    .is_some()
            });
        if !admitted || !validator {
            return;
        }
        match event {
            ApplicationEvent::NeighborUp(_) => {
                if let Some(running) = &self.running {
                    for (_, transaction) in &self.abci.state.pending {
                        match encode_transaction(transaction, running.transport.max_message_size) {
                            Ok(encoded) => {
                                if running.transport.transactions.send(encoded).await.is_err() {
                                    tracing::warn!("coordination transport closed during pending replay");
                                    break;
                                }
                            }
                            Err(error) => {
                                tracing::warn!(%error, "coordination pending transaction cannot be published")
                            }
                        }
                    }
                }
            }
            ApplicationEvent::Transaction(_, transaction) => {
                let encoded = match self.encode_for_network(&transaction).await {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        tracing::warn!(%error, "coordination transaction refused");
                        return;
                    }
                };
                let already_known = transaction.event_id().ok().is_some_and(|id| {
                    self.abci.state.committed.applied_events.contains(&id)
                        || self
                            .abci
                            .state
                            .pending
                            .iter()
                            .any(|(pending, _)| *pending == id)
                });
                match self.abci.submit(*transaction).await {
                    Ok(_) if !already_known => {
                        if let (Some(running), Some(encoded)) = (&self.running, encoded)
                            && running.transport.transactions.send(encoded).await.is_err()
                        {
                            tracing::warn!("coordination transport closed before transaction relay");
                        }
                    }
                    Ok(_) => {}
                    Err(error) => tracing::warn!(%error, "coordination transaction refused"),
                }
            }
        }
    }

    fn handle_reply(reply: Option<Result<(), JoinError>>) {
        if let Some(Err(error)) = reply {
            tracing::error!(%error, "coordination reply task failed");
        }
    }

    /// Rebuilds an active engine against Machine's latest native handles.
    async fn reconnect(&mut self) {
        if !self.server_requested {
            return;
        }
        if let Err(error) = self.stop_server().await {
            tracing::error!(%error, "coordination reconnect cleanup failed");
        }
        if let Err(error) = self.start_server().await {
            tracing::warn!(%error, "coordination reconnect failed; retrying");
        }
    }

    /// Stops the predecessor before atomically preparing and starting its successor.
    async fn regenesis(&mut self, record: SignedBody<Regenesis>) -> Result<CoordinationResponse, CoordinationError> {
        self.restart_after_recovery |= self.running.is_some();
        self.stop_server().await?;
        let result = self.abci.begin_successor(record).await;
        if self.restart_after_recovery {
            match self.start_server().await {
                Ok(()) => self.restart_after_recovery = false,
                Err(error) => {
                    tracing::error!(%error, "coordination recovery engine start failed");
                    return Err(error);
                }
            }
        }
        result.map(|event_id| CoordinationResponse::TransactionSubmitted { event_id })
    }

    /// Stops the owned engine while keeping its database and native endpoint available.
    async fn stop_server(&mut self) -> Result<(), CoordinationError> {
        let Some(running) = self.running.take() else {
            return Ok(());
        };

        let RunningEngine {
            callbacks,
            engine,
            transport,
        } = running;
        drop(callbacks);
        self.abci.cancel_value_requests();
        self.replies.abort_all();
        while self.replies.join_next().await.is_some() {}
        let (engine_result, transport_result) = tokio::join!(biased;
            Self::stop_engine(engine), transport.stop(),
        );
        engine_result?;
        transport_result?;
        let _ = self
            .broadcasts
            .send(CoordinationBroadcast::ServerStopped)
            .await;
        Ok(())
    }

    /// Lets every Malachite child finish its stop hook, including the WAL worker.
    async fn stop_engine(engine: EngineHandle) -> Result<(), tokio::task::JoinError> {
        let children = engine.actor.get_cell().get_children();
        for child in &children {
            child.unlink(engine.actor.get_cell());
        }
        engine.actor.stop(None);
        for child in &children {
            child.stop(None);
        }
        let engine_result = engine.handle.await;
        for child in children {
            let _ = child.wait(None).await;
        }
        engine_result?;
        Ok(())
    }
}

/// Serves bus requests and callbacks from the owned consensus engine.
pub(super) async fn run_actor(mut actor: Actor) {
    loop {
        let retry = actor.server_requested && actor.running.is_none();
        let (callbacks, events) = match actor.running.as_mut() {
            Some(running) => (Some(&mut running.callbacks), Some(&mut running.transport.events)),
            None => (None, None),
        };
        let native_changes = &mut actor.native_changes;
        tokio::select! {
            biased;
            request = actor.requests.recv() => match request {
                Some(request) => {
                    let response = actor.handle_message(request.payload).await;
                    let _ = request.reply_to.send(response);
                }
                None => break,
            },
            changed = async {
                match native_changes {
                    Some(changes) => changes.changed().await,
                    None => std::future::pending().await,
                }
            } => {
                if changed.is_ok() {
                    actor.reconnect().await;
                }
            },
            callback = async {
                match callbacks {
                    Some(callbacks) => callbacks.recv().await,
                    None => std::future::pending().await,
                }
            } => actor.handle_consensus_message(callback).await,
            event = async {
                match events {
                    Some(events) => events.recv().await,
                    None => std::future::pending().await,
                }
            } => actor.handle_application_event(event).await,
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)), if retry => actor.reconnect().await,
            reply = actor.replies.join_next(), if !actor.replies.is_empty() => Actor::handle_reply(reply),
        }
    }
    if let Err(error) = actor.stop_server().await {
        tracing::error!(%error, "coordination actor shutdown failed");
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod sharing_tests;
