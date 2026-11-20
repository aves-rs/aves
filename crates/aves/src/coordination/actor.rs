use std::collections::HashMap;
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
use futures_util::stream::{self, BoxStream, SelectAll, StreamExt};
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
use super::types::{
    CoordinationContext, CoordinationError, CoordinationNodeConfig, DomainId, PublicKey, Regenesis,
    SharingNoticeRecord, SignedBody,
};

struct HostEvent {
    domain: DomainId,
    generation: u64,
    kind: HostEventKind,
}

enum HostEventKind {
    Consensus(Box<HostMsg<CoordinationContext>>),
    Application(ApplicationEvent),
    Retry,
    Closed,
}

struct RunningEngine {
    engine: EngineHandle,
    transport: NetworkAttachment,
}

struct ChainHost {
    abci: CoordinationApplication,
    running: Option<RunningEngine>,
    server_requested: bool,
    restart_after_recovery: bool,
    generation: u64,
    pending_start: bool,
}

impl ChainHost {
    fn new(abci: CoordinationApplication) -> Self {
        Self {
            abci,
            running: None,
            server_requested: false,
            restart_after_recovery: false,
            generation: 0,
            pending_start: false,
        }
    }
}

/// Owns coordination requests and the coordination ABCI application.
pub(super) struct Actor {
    requests: mpsc::Receiver<CoordinationRequest>,
    broadcasts: mpsc::Sender<CoordinationBroadcast>,
    store: Arc<CoordinationStore>,
    hosts: HashMap<DomainId, ChainHost>,
    sync_services: Option<Arc<dyn MachinePeerService>>,
    sync_services_reader: Option<ServiceReader<dyn MachinePeerService>>,
    admission: Option<AuthAdmissionRegistry>,
    config_dir: PathBuf,
    native_changes: Option<watch::Receiver<()>>,
    mux: SelectAll<BoxStream<'static, HostEvent>>,
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
        let store = Arc::new(
            CoordinationStore::open(&config_dir)
                .await
                .expect("coordination database must open and migrate"),
        );

        let domains = store
            .list_domains()
            .await
            .expect("coordination domains must load");
        let mut hosts = HashMap::new();
        for domain in domains {
            let recovery = store
                .load(domain)
                .await
                .expect("coordination recovery state must load");
            let abci = CoordinationApplication::with_store(store.clone(), recovery)
                .expect("coordination ABCI state must restore");
            hosts.insert(domain, ChainHost::new(abci));
        }

        Self {
            requests,
            broadcasts,
            store,
            hosts,
            sync_services,
            sync_services_reader: None,
            admission,
            config_dir,
            native_changes: None,
            mux: SelectAll::new(),
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
        if matches!(
            payload,
            CoordinationRequestPayload::StartAllServers | CoordinationRequestPayload::StopAllServers
        ) {
            let start = matches!(payload, CoordinationRequestPayload::StartAllServers);
            let domains: Vec<_> = self.hosts.keys().copied().collect();
            for domain in domains {
                if let Some(mut host) = self.hosts.remove(&domain) {
                    if start {
                        host.server_requested = true;
                        if let Err(error) = self.start_server(&mut host).await {
                            if !matches!(error, CoordinationError::PeerServicesPending) {
                                tracing::warn!(%error, "coordination startup failed; retrying");
                            }
                            self.schedule_retry(&mut host);
                        }
                    } else {
                        host.server_requested = false;
                        host.restart_after_recovery = false;
                        if let Err(error) = self.stop_server(&mut host).await {
                            tracing::error!(%error, "coordination shutdown failed");
                        }
                    }
                    self.hosts.insert(domain, host);
                }
            }
            return Ok(if start {
                CoordinationResponse::ServerStarted
            } else {
                CoordinationResponse::ServerStopped
            });
        }
        let domain = match &payload {
            CoordinationRequestPayload::GetEffectiveRoster { domain, .. }
            | CoordinationRequestPayload::SignMachineRecord { domain, .. }
            | CoordinationRequestPayload::SubmitShareGrantChanged { domain, .. }
            | CoordinationRequestPayload::SubmitStoryPublished { domain, .. }
            | CoordinationRequestPayload::SubmitEnvelopeAddition { domain, .. }
            | CoordinationRequestPayload::SubmitVerificationReport { domain, .. }
            | CoordinationRequestPayload::ListShareGrants { domain, .. }
            | CoordinationRequestPayload::ListPublications { domain }
            | CoordinationRequestPayload::GetPublication { domain, .. }
            | CoordinationRequestPayload::ListEnvelopeAdditions { domain, .. }
            | CoordinationRequestPayload::ListVerificationReports { domain }
            | CoordinationRequestPayload::StartServer { domain }
            | CoordinationRequestPayload::StopServer { domain }
            | CoordinationRequestPayload::Genesis { domain, .. }
            | CoordinationRequestPayload::ExportSnapshot { domain }
            | CoordinationRequestPayload::AdoptSnapshot { domain, .. }
            | CoordinationRequestPayload::RegisterCollectiveOwner { domain, .. }
            | CoordinationRequestPayload::SubmitInvitation { domain, .. }
            | CoordinationRequestPayload::SubmitMembershipDecision { domain, .. }
            | CoordinationRequestPayload::SubmitSharingNotice { domain, .. }
            | CoordinationRequestPayload::ListSharingNotices { domain, .. }
            | CoordinationRequestPayload::GetSharingNotice { domain, .. }
            | CoordinationRequestPayload::SubmitMemberRemoved { domain, .. }
            | CoordinationRequestPayload::SubmitMemberPowerChanged { domain, .. }
            | CoordinationRequestPayload::SubmitMemberValidatorSplitChanged { domain, .. }
            | CoordinationRequestPayload::SubmitChainPolicyChanged { domain, .. }
            | CoordinationRequestPayload::SubmitAcceptance { domain, .. }
            | CoordinationRequestPayload::SubmitMemberMachineCertUpdate { domain, .. }
            | CoordinationRequestPayload::SubmitMemberAuthorityUpdated { domain, .. }
            | CoordinationRequestPayload::GetMemberAuthority { domain, .. }
            | CoordinationRequestPayload::ListMembers { domain }
            | CoordinationRequestPayload::ListInvitationsForUser { domain, .. }
            | CoordinationRequestPayload::GetMemberPower { domain, .. }
            | CoordinationRequestPayload::GetChainPolicy { domain }
            | CoordinationRequestPayload::RegisterUser { domain, .. }
            | CoordinationRequestPayload::RotateUserEncryptionKey { domain, .. }
            | CoordinationRequestPayload::RotateUserRootKey { domain, .. }
            | CoordinationRequestPayload::LinkUserSuccessor { domain, .. }
            | CoordinationRequestPayload::RevokeUser { domain, .. }
            | CoordinationRequestPayload::RegisterMachine { domain, .. }
            | CoordinationRequestPayload::BindMachine { domain, .. }
            | CoordinationRequestPayload::UnbindMachine { domain, .. }
            | CoordinationRequestPayload::CreateExecution { domain, .. }
            | CoordinationRequestPayload::LandExecution { domain, .. }
            | CoordinationRequestPayload::CancelExecution { domain, .. }
            | CoordinationRequestPayload::ActivateSchema { domain, .. }
            | CoordinationRequestPayload::Regenesis { domain, .. }
            | CoordinationRequestPayload::GetUser { domain, .. }
            | CoordinationRequestPayload::GetMachine { domain, .. }
            | CoordinationRequestPayload::ListMachines { domain }
            | CoordinationRequestPayload::GetExecution { domain, .. }
            | CoordinationRequestPayload::ListExecutions { domain }
            | CoordinationRequestPayload::GetSchema { domain, .. }
            | CoordinationRequestPayload::GetSchemaHistory { domain }
            | CoordinationRequestPayload::GetChain { domain }
            | CoordinationRequestPayload::GetSubmissionStatus { domain, .. }
            | CoordinationRequestPayload::GetRegenesis { domain } => *domain,
            CoordinationRequestPayload::JoinChain { descriptor, .. } => descriptor.body.domain,
            CoordinationRequestPayload::StartAllServers | CoordinationRequestPayload::StopAllServers => unreachable!(),
        };
        match &payload {
            CoordinationRequestPayload::Genesis { genesis, .. } if genesis.body.domain != domain => {
                return Err(CoordinationError::InvalidSnapshot);
            }
            CoordinationRequestPayload::Genesis { .. } if self.store.load_retired(domain).await?.is_some() => {
                return Err(CoordinationError::DomainRetired { domain });
            }
            CoordinationRequestPayload::JoinChain { .. } if self.store.load_retired(domain).await?.is_some() => {
                return Err(CoordinationError::DomainRetired { domain });
            }
            CoordinationRequestPayload::AdoptSnapshot { snapshot, .. } if snapshot.domain != domain => {
                return Err(CoordinationError::InvalidSnapshot);
            }
            CoordinationRequestPayload::ExportSnapshot { .. } if !self.hosts.contains_key(&domain) => {
                if self.store.load_retired(domain).await?.is_none() {
                    return Err(CoordinationError::ChainNotHosted { domain });
                }
                let abci = CoordinationApplication::with_store(self.store.clone(), None)?;
                return abci
                    .export_chain_snapshot(Some(domain))
                    .await
                    .map(|snapshot| CoordinationResponse::Snapshot { snapshot });
            }
            _ => {}
        }
        if let CoordinationRequestPayload::SignMachineRecord {
            record: record @ super::signing::MachineRecord::Acceptance { .. },
            ..
        } = &payload
            && !self.hosts.contains_key(&domain)
        {
            let services = self.peer_services();
            let (signed_bytes, event_id) =
                super::signing::sign_machine_record(services.as_deref(), domain, record.clone(), None, &[]).await?;
            return Ok(CoordinationResponse::SignedMachineRecord { signed_bytes, event_id });
        }
        let new_host = !self.hosts.contains_key(&domain)
            && matches!(
                &payload,
                CoordinationRequestPayload::Genesis { .. }
                    | CoordinationRequestPayload::AdoptSnapshot { .. }
                    | CoordinationRequestPayload::JoinChain { .. }
            );
        if new_host {
            let abci = CoordinationApplication::with_store(self.store.clone(), None)?;
            self.hosts.insert(domain, ChainHost::new(abci));
        }
        let mut host = self
            .hosts
            .remove(&domain)
            .ok_or(CoordinationError::ChainNotHosted { domain })?;
        let result = self.handle_host_message(&mut host, payload).await;
        if matches!(result, Err(CoordinationError::GenesisConflict { .. })) {
            host.server_requested = false;
            host.restart_after_recovery = false;
            let _ = self.stop_server(&mut host).await;
        }
        let final_domain = host.abci.state.domain.unwrap_or(domain);
        if !new_host || result.is_ok() {
            self.hosts.insert(final_domain, host);
        }
        result
    }

    async fn handle_host_message(
        &mut self,
        host: &mut ChainHost,
        payload: CoordinationRequestPayload,
    ) -> Result<CoordinationResponse, CoordinationError> {
        match payload {
            CoordinationRequestPayload::GetEffectiveRoster { height, .. } => {
                let (height, validators, halted) = super::roster::effective_roster(&host.abci.state.committed, height)?;
                Ok(CoordinationResponse::EffectiveRoster {
                    height,
                    validators,
                    halted,
                })
            }
            CoordinationRequestPayload::SignMachineRecord { domain, record } => {
                let services = self.peer_services();
                let (signed_bytes, event_id) = super::signing::sign_machine_record(
                    services.as_deref(),
                    domain,
                    record,
                    Some(&host.abci.state.committed),
                    &host.abci.state.pending,
                )
                .await?;
                Ok(CoordinationResponse::SignedMachineRecord { signed_bytes, event_id })
            }
            CoordinationRequestPayload::SubmitShareGrantChanged { record, .. } => {
                self.submit(host, PendingTransaction::ShareGrantChanged(record))
                    .await
            }
            CoordinationRequestPayload::SubmitStoryPublished { record, .. } => {
                self.submit(host, PendingTransaction::StoryPublished(record))
                    .await
            }
            CoordinationRequestPayload::SubmitEnvelopeAddition { record, .. } => {
                self.submit(host, PendingTransaction::EnvelopeAddition(record))
                    .await
            }
            CoordinationRequestPayload::SubmitVerificationReport { record, .. } => {
                self.submit(host, PendingTransaction::VerificationReport(record))
                    .await
            }
            CoordinationRequestPayload::ListShareGrants { scope, recipient, .. } => {
                let grants = &host.abci.state.committed.share_grants;
                Ok(CoordinationResponse::ShareGrants {
                    records: grants
                        .iter()
                        .filter(|grant| {
                            scope.is_none_or(|scope| grant.scope == scope)
                                && recipient.is_none_or(|recipient| grant.recipient == recipient)
                                && !grants.iter().any(|later| {
                                    later.domain == grant.domain
                                        && later.grantor == grant.grantor
                                        && later.recipient == grant.recipient
                                        && later.scope == grant.scope
                                        && later.revision > grant.revision
                                })
                        })
                        .cloned()
                        .collect(),
                })
            }
            CoordinationRequestPayload::ListPublications { .. } => Ok(CoordinationResponse::Publications {
                records: host.abci.state.committed.publications.clone(),
            }),
            CoordinationRequestPayload::GetPublication { key, .. } => Ok(CoordinationResponse::Publication {
                record: host
                    .abci
                    .state
                    .committed
                    .publications
                    .iter()
                    .find(|record| record.publication.key == key)
                    .cloned(),
            }),
            CoordinationRequestPayload::ListEnvelopeAdditions { key, .. } => {
                Ok(CoordinationResponse::EnvelopeAdditions {
                    records: host
                        .abci
                        .state
                        .committed
                        .envelope_additions
                        .iter()
                        .filter(|record| record.key == key)
                        .cloned()
                        .collect(),
                })
            }
            CoordinationRequestPayload::ListVerificationReports { .. } => {
                Ok(CoordinationResponse::VerificationReports {
                    records: host.abci.state.committed.verification_reports.clone(),
                })
            }
            CoordinationRequestPayload::StartAllServers | CoordinationRequestPayload::StopAllServers => unreachable!(),
            CoordinationRequestPayload::StartServer { .. } => {
                let result = self.start_server(host).await;
                if result.is_ok() || matches!(&result, Err(CoordinationError::PeerServicesPending)) {
                    host.server_requested = true;
                }
                if result.is_err() && host.server_requested {
                    self.schedule_retry(host);
                }
                result.map(|()| CoordinationResponse::ServerStarted)
            }
            CoordinationRequestPayload::StopServer { .. } => {
                host.server_requested = false;
                host.restart_after_recovery = false;
                self.stop_server(host)
                    .await
                    .map(|()| CoordinationResponse::ServerStopped)
            }
            CoordinationRequestPayload::Genesis { genesis, .. } => host
                .abci
                .create_genesis(genesis)
                .await
                .map(|record| CoordinationResponse::Chain { record }),
            CoordinationRequestPayload::JoinChain {
                descriptor,
                expected_snapshot_hash,
                participants,
            } => {
                let record = host
                    .abci
                    .join_chain(descriptor, expected_snapshot_hash, participants)
                    .await?;
                host.server_requested = true;
                if let Err(error) = self.start_server(host).await {
                    if !matches!(error, CoordinationError::PeerServicesPending) {
                        tracing::warn!(%error, "coordination join attachment failed; retrying");
                    }
                    self.schedule_retry(host);
                }
                Ok(CoordinationResponse::Chain { record })
            }
            CoordinationRequestPayload::ExportSnapshot { domain } => host
                .abci
                .export_chain_snapshot(Some(domain))
                .await
                .map(|snapshot| CoordinationResponse::Snapshot { snapshot }),
            CoordinationRequestPayload::AdoptSnapshot {
                snapshot,
                expected_hash,
                ..
            } => {
                self.stop_server(host).await?;
                let result = host.abci.adopt_snapshot(snapshot, expected_hash).await;
                if host.server_requested
                    && let Err(error) = self.start_server(host).await
                {
                    tracing::warn!(%error, "coordination restart after snapshot failed");
                    self.schedule_retry(host);
                }
                result.map(|record| CoordinationResponse::Chain { record })
            }
            CoordinationRequestPayload::RegisterCollectiveOwner { record, .. } => {
                self.submit(host, PendingTransaction::OwnerRegistration(record))
                    .await
            }
            CoordinationRequestPayload::SubmitInvitation { record, .. } => {
                self.submit(host, PendingTransaction::Invitation(Box::new(record)))
                    .await
            }
            CoordinationRequestPayload::SubmitMembershipDecision { record, .. } => {
                self.submit(host, PendingTransaction::MembershipDecision(Box::new(record)))
                    .await
            }
            CoordinationRequestPayload::SubmitSharingNotice { record, .. } => {
                self.submit(host, PendingTransaction::SharingNotice(Box::new(record)))
                    .await
            }
            CoordinationRequestPayload::ListSharingNotices { target_user, .. } => {
                let committed = &host.abci.state.committed;
                Ok(CoordinationResponse::SharingNotices {
                    records: committed
                        .sharing_notices
                        .iter()
                        .filter(|(key, _)| key.target_user == target_user)
                        .filter_map(|(key, history)| {
                            let notice = history.last()?.clone();
                            let signer = committed
                                .sharing_notice_signers
                                .get(key)
                                .and_then(|signers| signers.get(history.len().saturating_sub(1)))
                                .cloned()
                                .flatten();
                            Some(SharingNoticeRecord { notice, signer })
                        })
                        .collect(),
                })
            }
            CoordinationRequestPayload::GetSharingNotice { key, .. } => {
                let committed = &host.abci.state.committed;
                let record = committed.sharing_notices.get(&key).and_then(|history| {
                    let notice = history.last()?.clone();
                    let signer = committed
                        .sharing_notice_signers
                        .get(&key)
                        .and_then(|signers| signers.get(history.len().saturating_sub(1)))
                        .cloned()
                        .flatten();
                    Some(SharingNoticeRecord { notice, signer })
                });
                Ok(CoordinationResponse::SharingNotice { record })
            }
            CoordinationRequestPayload::SubmitMemberRemoved { record, .. } => {
                self.submit(host, PendingTransaction::MemberRemoved(record))
                    .await
            }
            CoordinationRequestPayload::SubmitMemberPowerChanged { record, .. } => {
                self.submit(host, PendingTransaction::MemberPowerChanged(record))
                    .await
            }
            CoordinationRequestPayload::SubmitMemberValidatorSplitChanged { record, .. } => {
                self.submit(host, PendingTransaction::MemberValidatorSplitChanged(record))
                    .await
            }
            CoordinationRequestPayload::SubmitChainPolicyChanged { record, .. } => {
                self.submit(host, PendingTransaction::ChainPolicyChanged(record))
                    .await
            }
            CoordinationRequestPayload::SubmitAcceptance { record, .. } => {
                self.submit(host, PendingTransaction::Acceptance(Box::new(record)))
                    .await
            }
            CoordinationRequestPayload::SubmitMemberMachineCertUpdate { record, .. } => {
                self.submit(host, PendingTransaction::MemberMachineCertUpdate(Box::new(record)))
                    .await
            }
            CoordinationRequestPayload::SubmitMemberAuthorityUpdated { record, .. } => {
                self.submit(host, PendingTransaction::MemberAuthorityUpdated(record))
                    .await
            }
            CoordinationRequestPayload::GetMemberAuthority { member, .. } => {
                Ok(CoordinationResponse::MemberAuthority {
                    authority: super::authority::current_member_authority(&host.abci.state.committed, member),
                })
            }
            CoordinationRequestPayload::ListMembers { .. } => Ok(CoordinationResponse::Members {
                records: host
                    .abci
                    .state
                    .committed
                    .members
                    .values()
                    .cloned()
                    .collect(),
            }),
            CoordinationRequestPayload::ListInvitationsForUser { target_user, .. } => {
                Ok(CoordinationResponse::Invitations {
                    records: host
                        .abci
                        .state
                        .committed
                        .invitations
                        .values()
                        .filter(|record| record.invitation.target_user == target_user)
                        .cloned()
                        .collect(),
                })
            }
            CoordinationRequestPayload::GetMemberPower { member, .. } => Ok(CoordinationResponse::MemberPower {
                power: host.abci.state.committed.member_power.get(&member).cloned(),
                split: host
                    .abci
                    .state
                    .committed
                    .member_splits
                    .get(&member)
                    .cloned(),
            }),
            CoordinationRequestPayload::GetChainPolicy { .. } => Ok(CoordinationResponse::ChainPolicy {
                policy: host.abci.state.committed.policy.clone(),
            }),
            CoordinationRequestPayload::RegisterUser { record, .. } => {
                self.submit(host, PendingTransaction::UserRegistered(record))
                    .await
            }
            CoordinationRequestPayload::RotateUserEncryptionKey { record, .. } => {
                self.submit(host, PendingTransaction::UserEncRotated(record))
                    .await
            }
            CoordinationRequestPayload::RotateUserRootKey { record, .. } => {
                self.submit(host, PendingTransaction::UserRootRotated(record))
                    .await
            }
            CoordinationRequestPayload::LinkUserSuccessor { record, .. } => {
                self.submit(host, PendingTransaction::UserSuccessorLinked(record))
                    .await
            }
            CoordinationRequestPayload::RevokeUser { record, .. } => {
                self.submit(host, PendingTransaction::UserRevoked(record))
                    .await
            }
            CoordinationRequestPayload::RegisterMachine { record, .. } => {
                self.submit(host, PendingTransaction::MachineRegistered(record))
                    .await
            }
            CoordinationRequestPayload::BindMachine { record, .. } => {
                self.submit(host, PendingTransaction::MachineBound(record))
                    .await
            }
            CoordinationRequestPayload::UnbindMachine { record, .. } => {
                self.submit(host, PendingTransaction::MachineUnbound(record))
                    .await
            }
            CoordinationRequestPayload::CreateExecution { record, .. } => {
                self.submit(host, PendingTransaction::ExecutionCreated(record))
                    .await
            }
            CoordinationRequestPayload::LandExecution { record, .. } => {
                self.submit(host, PendingTransaction::ExecutionLanded(record))
                    .await
            }
            CoordinationRequestPayload::CancelExecution { record, .. } => {
                self.submit(host, PendingTransaction::ExecutionCancelled(record))
                    .await
            }
            CoordinationRequestPayload::ActivateSchema { record, .. } => {
                self.submit(host, PendingTransaction::SchemaActivated(record))
                    .await
            }
            CoordinationRequestPayload::Regenesis { record, .. } => self.regenesis(host, record).await,
            CoordinationRequestPayload::GetUser { user, .. } => host
                .abci
                .get_user(user)
                .map(|record| CoordinationResponse::User { record }),
            CoordinationRequestPayload::GetMachine { machine, .. } => host
                .abci
                .get_machine(machine)
                .map(|record| CoordinationResponse::Machine { record }),
            CoordinationRequestPayload::ListMachines { .. } => host
                .abci
                .list_machines()
                .map(|records| CoordinationResponse::MachinesList { records }),
            CoordinationRequestPayload::GetExecution { execution, .. } => host
                .abci
                .get_execution(execution)
                .map(|record| CoordinationResponse::Execution { record }),
            CoordinationRequestPayload::ListExecutions { .. } => host
                .abci
                .list_executions()
                .map(|records| CoordinationResponse::ExecutionsList { records }),
            CoordinationRequestPayload::GetSchema { at, .. } => host
                .abci
                .get_schema(at)
                .map(|record| CoordinationResponse::Schema { record }),
            CoordinationRequestPayload::GetSchemaHistory { .. } => host
                .abci
                .get_schema_history()
                .map(|records| CoordinationResponse::SchemaHistory { records }),
            CoordinationRequestPayload::GetChain { .. } => host
                .abci
                .get_chain()
                .map(|record| CoordinationResponse::Chain { record }),
            CoordinationRequestPayload::GetRegenesis { .. } => host
                .abci
                .get_regenesis()
                .map(|record| CoordinationResponse::Regenesis { record }),
            CoordinationRequestPayload::GetSubmissionStatus { event_id, .. } => host
                .abci
                .submission_status(event_id)
                .map(|status| CoordinationResponse::SubmissionStatus { status }),
        }
    }

    fn schedule_retry(&mut self, host: &mut ChainHost) {
        if host.pending_start || !host.server_requested {
            return;
        }
        let Ok((domain, _)) = host.abci.active_chain_consensus_params() else {
            return;
        };
        host.pending_start = true;
        let generation = host.generation;
        self.mux.push(
            stream::once(async move {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                HostEvent {
                    domain,
                    generation,
                    kind: HostEventKind::Retry,
                }
            })
            .boxed(),
        );
    }

    async fn handle_host_event(&mut self, event: Option<HostEvent>) {
        let Some(event) = event else { return };
        let Some(mut host) = self.hosts.remove(&event.domain) else {
            return;
        };
        if event.generation != host.generation {
            self.hosts.insert(event.domain, host);
            return;
        }
        match event.kind {
            HostEventKind::Consensus(message) => {
                self.handle_consensus_message(&mut host, Some(*message))
                    .await
            }
            HostEventKind::Application(message) => {
                self.handle_application_event(&mut host, Some(message))
                    .await
            }
            HostEventKind::Retry => {
                host.pending_start = false;
                self.reconnect(&mut host).await;
            }
            HostEventKind::Closed => {
                if let Err(error) = self.stop_server(&mut host).await {
                    tracing::error!(%error, "coordination closed engine cleanup failed");
                }
                self.schedule_retry(&mut host);
            }
        }
        self.hosts.insert(event.domain, host);
    }

    /// Routes an engine callback or closes the stopped engine's resources.
    async fn handle_consensus_message(&mut self, host: &mut ChainHost, message: Option<HostMsg<CoordinationContext>>) {
        let previous_height = host.abci.state.committed.height;
        let result = match message {
            Some(message) => {
                let Some(running) = host.running.as_ref() else { return };
                let network = running.transport.sender.clone();
                self.dispatch_consensus_message(host, message, &network)
                    .await
            }
            None => self.stop_server(host).await,
        };
        let height = host.abci.state.committed.height;
        if height != previous_height
            && let Some(domain) = host.abci.state.domain
        {
            let _ = self
                .broadcasts
                .send(CoordinationBroadcast::ChainAdvanced { domain, height })
                .await;
        }
        if let Err(error) = result {
            tracing::error!(%error, "coordination consensus handling failed");
            if let Err(cleanup) = self.stop_server(host).await {
                tracing::error!(%cleanup, "coordination consensus cleanup failed");
            }
            self.schedule_retry(host);
        }
    }

    /// Dispatches the application request without waiting for deferred replies.
    async fn dispatch_consensus_message(
        &mut self,
        host: &mut ChainHost,
        message: HostMsg<CoordinationContext>,
        network: &mpsc::Sender<NetworkMsg<CoordinationContext>>,
    ) -> Result<(), CoordinationError> {
        macro_rules! callback {
            ($port:ident, $variant:ident { $($field:ident),* $(,)? }) => {{
                let (reply, receiver) = oneshot::channel();
                host.abci.handle_app_msg(AppMsg::$variant { $($field,)* reply }, network).await?;
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
                host.abci
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
                host.abci
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
                host.abci
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
                host.abci
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
    async fn start_server(&mut self, host: &mut ChainHost) -> Result<(), CoordinationError> {
        if host.running.is_some() {
            return Ok(());
        }
        let (domain, _) = host.abci.active_chain_consensus_params()?;
        let Some(services) = self.peer_services() else {
            return Err(CoordinationError::PeerServicesPending);
        };
        if self.native_changes.is_none() {
            self.native_changes = Some(services.subscribe_native_changes());
        }
        let admission = self
            .admission
            .clone()
            .ok_or(CoordinationError::NotImplemented)?;
        let participants = {
            let state = &host.abci.state.committed;
            let mut participants: Vec<PublicKey> = state
                .members
                .keys()
                .filter_map(|user| super::authority::current_member_authority(state, *user))
                .flat_map(|authority| {
                    authority
                        .certified_machines
                        .into_iter()
                        .map(|machine| machine.machine_key)
                })
                .collect();
            if state.height.0 == 0
                && let Some(genesis) = &host.abci.state.genesis
            {
                participants.extend(genesis.participants.iter().copied());
            }
            if let Ok(super::roster::RosterOutcome::Validators(roster)) = super::roster::derive_roster(state) {
                participants.extend(roster.into_iter().map(|(key, _)| key));
            }
            if let Some(owner) = &state.owner {
                participants.extend(state.machines.values().flat_map(|machine| {
                    machine
                        .bindings
                        .iter()
                        .filter(|binding| {
                            binding.user == owner.user
                                && !machine
                                    .unbindings
                                    .iter()
                                    .any(|unbinding| unbinding.user == owner.user)
                        })
                        .map(|binding| binding.machine_key)
                }));
            }
            if participants.is_empty()
                && state.initial_roster.is_empty()
                && let Some(genesis_roster) = host.abci.state.genesis_validators.as_ref()
            {
                participants.extend(genesis_roster.iter().map(|(key, _)| *key));
            }
            participants
        };
        let mut transport = NetworkAttachment::start(services.as_ref(), admission, domain, &participants).await?;
        match self.build_engine(host, transport.actor.clone()).await {
            Ok((callbacks, engine)) => {
                host.generation = host.generation.wrapping_add(1);
                host.pending_start = false;
                let (_, replacement) = mpsc::channel(1);
                let events = std::mem::replace(&mut transport.events, replacement);
                let generation = host.generation;
                let callbacks = stream::unfold(callbacks, |mut receiver| async move {
                    receiver.recv().await.map(|message| (message, receiver))
                })
                .map(move |message| HostEvent {
                    domain,
                    generation,
                    kind: HostEventKind::Consensus(Box::new(message)),
                })
                .chain(stream::once(async move {
                    HostEvent {
                        domain,
                        generation,
                        kind: HostEventKind::Closed,
                    }
                }));
                let events = stream::unfold(events, |mut receiver| async move {
                    receiver.recv().await.map(|message| (message, receiver))
                })
                .map(move |message| HostEvent {
                    domain,
                    generation,
                    kind: HostEventKind::Application(message),
                })
                .chain(stream::once(async move {
                    HostEvent {
                        domain,
                        generation,
                        kind: HostEventKind::Closed,
                    }
                }));
                self.mux.push(callbacks.boxed());
                self.mux.push(events.boxed());
                host.running = Some(RunningEngine { engine, transport });
                let _ = self
                    .broadcasts
                    .send(CoordinationBroadcast::ServerStarted { domain })
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
        &mut self,
        host: &ChainHost,
        network: NetworkRef<CoordinationContext>,
    ) -> Result<(mpsc::Receiver<HostMsg<CoordinationContext>>, EngineHandle), CoordinationError> {
        let services = self
            .peer_services()
            .ok_or(CoordinationError::NotImplemented)?;
        let (domain, _) = host.abci.active_chain_consensus_params()?;
        let key = services.transport().await.0.secret_key().clone();
        let address = PublicKey(*key.public().as_bytes());
        let verifier = Box::new(CoordinationVerifier::new(domain));
        let consensus =
            ConsensusContext::new_validator(address, verifier, Box::new(CoordinationSigner::new(key, domain)));
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
    async fn submit(
        &mut self,
        host: &mut ChainHost,
        transaction: PendingTransaction,
    ) -> Result<CoordinationResponse, CoordinationError> {
        let encoded = self.encode_for_network(host, &transaction).await?;
        let event_id = host.abci.submit(transaction.clone()).await?;
        if let (Some(running), Some(encoded)) = (&host.running, encoded)
            && running.transport.transactions.send(encoded).await.is_err()
        {
            tracing::warn!("coordination transport closed before transaction publication");
        }
        Ok(CoordinationResponse::TransactionSubmitted { event_id })
    }

    async fn encode_for_network(
        &mut self,
        host: &ChainHost,
        transaction: &PendingTransaction,
    ) -> Result<Option<Vec<u8>>, CoordinationError> {
        let max_message_size = if let Some(running) = &host.running {
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

    /// Relays member transactions while replaying pending records only to validators.
    async fn handle_application_event(&mut self, host: &mut ChainHost, event: Option<ApplicationEvent>) {
        let Some(event) = event else {
            tracing::error!("coordination application event channel closed");
            if let Err(error) = self.stop_server(host).await {
                tracing::error!(%error, "coordination transport cleanup failed");
            }
            self.schedule_retry(host);
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
        let validator = host
            .abci
            .active_chain_consensus_params()
            .ok()
            .is_some_and(|(_, validators)| {
                validators
                    .get_by_address(&PublicKey(*peer.as_bytes()))
                    .is_some()
            });
        if !admitted {
            return;
        }
        match event {
            ApplicationEvent::NeighborUp(_) => {
                if !validator {
                    return;
                }
                if let Some(running) = &host.running {
                    for (_, transaction) in &host.abci.state.pending {
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
                let state = &host.abci.state.committed;
                let key = PublicKey(*peer.as_bytes());
                let member = state.members.keys().any(|user| {
                    super::authority::current_member_authority(state, *user).is_some_and(|authority| {
                        authority
                            .certified_machines
                            .iter()
                            .any(|machine| machine.machine_key == key)
                    })
                });
                let owner_machine = state.owner.as_ref().is_some_and(|owner| {
                    state
                        .users
                        .get(&owner.user)
                        .is_none_or(|user| user.revoked.is_none())
                        && state.machines.values().any(|machine| {
                            machine
                                .bindings
                                .iter()
                                .any(|binding| binding.user == owner.user && binding.machine_key == key)
                                && !machine
                                    .unbindings
                                    .iter()
                                    .any(|unbinding| unbinding.user == owner.user)
                        })
                });
                if !member && !owner_machine {
                    return;
                }
                let encoded = match self.encode_for_network(host, &transaction).await {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        tracing::warn!(%error, "coordination transaction refused");
                        return;
                    }
                };
                let already_known = transaction.event_id().ok().is_some_and(|id| {
                    host.abci.state.committed.applied_events.contains(&id)
                        || host.abci.state.committed.rejected_events.contains_key(&id)
                        || host
                            .abci
                            .state
                            .pending
                            .iter()
                            .any(|(pending, _)| *pending == id)
                });
                match host.abci.submit(*transaction).await {
                    Ok(_) if !already_known => {
                        if let (Some(running), Some(encoded)) = (&host.running, encoded)
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
    async fn reconnect(&mut self, host: &mut ChainHost) {
        if !host.server_requested {
            return;
        }
        if let Err(error) = self.stop_server(host).await {
            tracing::error!(%error, "coordination reconnect cleanup failed");
        }
        if let Err(error) = self.start_server(host).await {
            if !matches!(error, CoordinationError::PeerServicesPending) {
                tracing::warn!(%error, "coordination reconnect failed; retrying");
            }
            self.schedule_retry(host);
        }
    }

    /// Stops the predecessor before atomically preparing and starting its successor.
    async fn regenesis(
        &mut self,
        host: &mut ChainHost,
        record: SignedBody<Regenesis>,
    ) -> Result<CoordinationResponse, CoordinationError> {
        if self.hosts.contains_key(&record.body.successor) {
            return Err(CoordinationError::InvalidSnapshot);
        }
        host.restart_after_recovery |= host.running.is_some();
        self.stop_server(host).await?;
        let result = host.abci.begin_successor(record).await;
        if host.restart_after_recovery {
            match self.start_server(host).await {
                Ok(()) => host.restart_after_recovery = false,
                Err(error) => {
                    tracing::error!(%error, "coordination recovery engine start failed");
                    self.schedule_retry(host);
                    return Err(error);
                }
            }
        }
        if host.running.is_none() {
            self.schedule_retry(host);
        }
        result.map(|event_id| CoordinationResponse::TransactionSubmitted { event_id })
    }

    /// Stops the owned engine while keeping its database and native endpoint available.
    async fn stop_server(&mut self, host: &mut ChainHost) -> Result<(), CoordinationError> {
        host.generation = host.generation.wrapping_add(1);
        host.pending_start = false;
        let Some(running) = host.running.take() else {
            return Ok(());
        };

        let RunningEngine { engine, transport } = running;
        host.abci.cancel_value_requests();
        let (engine_result, transport_result) = tokio::join!(biased;
            Self::stop_engine(engine), transport.stop(),
        );
        engine_result?;
        transport_result?;
        if let Some(domain) = host.abci.state.domain {
            let _ = self
                .broadcasts
                .send(CoordinationBroadcast::ServerStopped { domain })
                .await;
        }
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
                    let domains: Vec<_> = actor.hosts.keys().copied().collect();
                    for domain in domains {
                        if let Some(mut host) = actor.hosts.remove(&domain) {
                            actor.reconnect(&mut host).await;
                            actor.hosts.insert(domain, host);
                        }
                    }
                }
            },
            event = actor.mux.next(), if !actor.mux.is_empty() => actor.handle_host_event(event).await,
            reply = actor.replies.join_next(), if !actor.replies.is_empty() => Actor::handle_reply(reply),
        }
    }
    let domains: Vec<_> = actor.hosts.keys().copied().collect();
    for domain in domains {
        if let Some(mut host) = actor.hosts.remove(&domain)
            && let Err(error) = actor.stop_server(&mut host).await
        {
            tracing::error!(%error, "coordination actor shutdown failed");
        }
    }
    actor.replies.abort_all();
    while actor.replies.join_next().await.is_some() {}
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod sharing_tests;
