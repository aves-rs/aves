//! Malachite callbacks, proposal handling, decisions, and chain synchronization.

use std::collections::HashSet;

use arc_malachitebft_app_channel::AppMsg;
use arc_malachitebft_app_channel::NetworkMsg;
use arc_malachitebft_app_channel::Reply;
use arc_malachitebft_app_channel::app::types::core::{
    CommitCertificate, ExtendedCommitCertificate, Height as MalachiteHeight, HeightParams, LinearTimeouts, Round,
    Validity, Value, VoteExtensions,
};
use arc_malachitebft_app_channel::app::types::streaming::{StreamId, StreamMessage};
use arc_malachitebft_app_channel::app::types::sync::RawDecidedValue;
use arc_malachitebft_app_channel::app::types::{LocallyProposedValue, PeerId, ProposedValue};
use arc_malachitebft_core_consensus::VoteExtensionError;
use arc_malachitebft_engine::host::{Next, SyncedValueOutcome};
use arc_malachitebft_engine::network::NetworkEvent;
use arc_malachitebft_engine::util::streaming::StreamContent;
use arc_malachitebft_sync::{InboundRequestId, Response as SyncResponse, ValueResponse};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

use super::abci::{ApplicationState, CommittedState, CoordinationApplication, unique_map};
use super::persistence::{DecidedRow, PendingRow, ProposalRow, RecoveryState};
use super::serialization::{decode_chain_network_event, serialize_sync_response};
use super::signing::{ChainPublicKey, ChainSigningScheme};
use super::transaction::{
    ApplicationRecovery, CommittedRecovery, IdleValueReply, PendingTransaction, PersistedCertificate, ProposalMetadata,
    ProposalPartAssembly, ProposalRecovery,
};
use super::types::{
    ChainRecord, ConsensusProposal, ConsensusProposalPart, ConsensusValidator, ConsensusValidatorSet, ConsensusValue,
    ConsensusValueId, CoordinationContext, CoordinationEventId, PublicKey,
};
use super::{CoordinationError, DomainId, Height};

impl CoordinationApplication {
    /// Handles application callbacks; the actor supplies the engine's network sender.
    pub(super) async fn handle_app_msg(
        &mut self,
        message: AppMsg<CoordinationContext>,
        network: &mpsc::Sender<NetworkMsg<CoordinationContext>>,
    ) -> Result<(), CoordinationError> {
        match message {
            AppMsg::ConsensusReady { reply } => {
                let height = Self::next_height(self.state.committed.height)?;
                reply
                    .send((height, Self::height_params(&self.state)?))
                    .map_err(|_| CoordinationError::NotImplemented)
            }
            AppMsg::StartedRound {
                height,
                round,
                reply_value,
                ..
            } => {
                self.idle_value_replies
                    .retain(|pending| pending.height == height && pending.round == round && !pending.reply.is_closed());
                let mut values = Vec::new();
                for ((at, _), proposal) in &mut self.state.proposals {
                    if *at < height.0 {
                        proposal.metadata = None;
                    } else if *at == height.0
                        && let Some(metadata) = &proposal.metadata
                    {
                        values.push(ProposedValue {
                            height: metadata.height,
                            round: metadata.round.into(),
                            valid_round: metadata.valid_round.into(),
                            proposer: metadata.proposer,
                            value: serde_json::from_slice(&proposal.application_bytes)
                                .map_err(|_| CoordinationError::Encoding)?,
                            validity: Validity::Valid,
                        });
                    }
                }
                reply_value
                    .send(values)
                    .map_err(|_| CoordinationError::NotImplemented)
            }
            AppMsg::GetValue {
                height, round, reply, ..
            } => self.get_value(height, round, reply).await,
            AppMsg::ExtendVote { reply, .. } => reply
                .send(None)
                .map_err(|_| CoordinationError::NotImplemented),
            AppMsg::VerifyVoteExtensions { extensions, reply, .. } => reply
                .send(
                    extensions
                        .into_iter()
                        .map(|(address, _)| (address, Err(VoteExtensionError::InvalidVoteExtension)))
                        .collect(),
                )
                .map_err(|_| CoordinationError::NotImplemented),
            AppMsg::RestreamProposal {
                height,
                round,
                valid_round,
                address,
                value_id,
            } => {
                self.restream_proposal(height, round, valid_round, address, value_id, network)
                    .await
            }
            AppMsg::GetHistoryMinHeight { reply } => {
                let state = &self.state;
                state.domain.ok_or(CoordinationError::NotImplemented)?;
                let earliest = state
                    .committed_values
                    .keys()
                    .next()
                    .copied()
                    .map(Height)
                    .map_or_else(|| Self::next_height(state.committed.height), Ok)?;
                reply
                    .send(earliest)
                    .map_err(|_| CoordinationError::NotImplemented)
            }
            AppMsg::ReceivedProposalPart {
                published_by,
                part,
                reply,
                ..
            } => self.received_proposal_part(published_by, part, reply).await,
            AppMsg::Decided {
                certificate,
                extensions,
                reply,
            } => self.decided(certificate, extensions, reply).await,
            AppMsg::Finalized {
                certificate,
                extensions,
                reply,
                ..
            } => self.finalized(certificate, extensions, reply).await,
            AppMsg::GetDecidedValues { range, reply } => {
                let state = &self.state;
                state.domain.ok_or(CoordinationError::NotImplemented)?;
                let (start, end) = (range.start().0, range.end().0);
                let expected_count = end
                    .checked_sub(start)
                    .and_then(|count| count.checked_add(1));
                let mut values: Vec<_> = if start <= end && end <= state.committed.height.0 {
                    state
                        .committed_values
                        .range(start..=end)
                        .map(|(_, value)| value.clone())
                        .collect()
                } else {
                    Vec::new()
                };
                if expected_count != Some(values.len() as u64) {
                    values.clear();
                }
                reply
                    .send(values)
                    .map_err(|_| CoordinationError::NotImplemented)
            }
            AppMsg::ProcessSyncedValue {
                height,
                round,
                proposer,
                value_bytes,
                reply,
            } => {
                self.process_synced_value(height, round, proposer, value_bytes, reply)
                    .await
            }
        }
    }

    /// Creates and durably records a fresh chain from the caller-selected validator powers.
    pub(super) async fn create_genesis(
        &mut self,
        roster: Vec<(PublicKey, u64)>,
    ) -> Result<ChainRecord, CoordinationError> {
        let mut candidate = self.state.clone();
        if candidate.domain.is_some()
            || candidate.genesis_validators.is_some()
            || candidate.committed.height != Height(0)
            || candidate.committed.regenesis.is_some()
            || !candidate.committed.applied_events.is_empty()
            || !candidate.pending.is_empty()
            || !candidate.committed_values.is_empty()
        {
            return Err(CoordinationError::NotImplemented);
        }
        Self::validator_set(&roster)?;
        let mut domain_bytes = [0; 32];
        getrandom::fill(&mut domain_bytes).map_err(|_| CoordinationError::NotImplemented)?;
        let domain = DomainId::from_bytes(domain_bytes);
        candidate.domain = Some(domain);
        candidate.genesis_validators = Some(roster);
        let chain = ChainRecord {
            domain,
            height: candidate.committed.height,
        };
        self.save_state(&candidate).await?;
        self.state = candidate;
        Ok(chain)
    }

    /// Admits one signed transaction durably before acknowledging its identity.
    pub(super) async fn submit(
        &mut self,
        transaction: PendingTransaction,
    ) -> Result<CoordinationEventId, CoordinationError> {
        if matches!(transaction, PendingTransaction::Regenesis(_)) {
            return Err(CoordinationError::NotImplemented);
        }
        let domain = self.state.domain.ok_or(CoordinationError::NotImplemented)?;
        transaction.verify_signature(domain)?;
        transaction.validate_payload()?;
        let event_id = transaction.event_id()?;
        if !self.state.committed.applied_events.contains(&event_id)
            && !self.state.pending.iter().any(|(id, _)| *id == event_id)
        {
            let mut candidate = self.state.clone();
            candidate.pending.push((event_id, transaction));
            self.save_state(&candidate).await?;
            self.state = candidate;
        }
        self.fulfill_idle_value_replies().await?;
        Ok(event_id)
    }

    pub(super) fn decode_transactions(value: &ConsensusValue) -> Result<Vec<PendingTransaction>, CoordinationError> {
        serde_json::from_str(value.0.as_str()).map_err(|_| CoordinationError::Encoding)
    }

    pub(super) fn active_chain_consensus_params(&self) -> Result<(DomainId, ConsensusValidatorSet), CoordinationError> {
        let domain = self.state.domain.ok_or(CoordinationError::NotImplemented)?;
        Ok((domain, Self::height_params(&self.state)?.validator_set))
    }

    pub(super) fn validator_set(roster: &[(PublicKey, u64)]) -> Result<ConsensusValidatorSet, CoordinationError> {
        let validators = roster
            .iter()
            .map(|(address, voting_power)| {
                Ok(ConsensusValidator {
                    address: *address,
                    public_key: ChainPublicKey::try_from(*address)
                        .map_err(|_| CoordinationError::InvalidValidatorSet)?,
                    voting_power: *voting_power,
                })
            })
            .collect::<Result<Vec<_>, CoordinationError>>()?;
        ConsensusValidatorSet::try_new(validators)
    }

    pub(super) async fn fulfill_idle_value_replies(&mut self) -> Result<(), CoordinationError> {
        self.idle_value_replies
            .retain(|pending| !pending.reply.is_closed());
        if self.idle_value_replies.is_empty() {
            return Ok(());
        }
        let Some(value) = self.build_application_value()? else {
            return Ok(());
        };
        let application_bytes = serde_json::to_vec(&value).map_err(|_| CoordinationError::Encoding)?;
        let value_id = serde_json::to_vec(&value.id()).map_err(|_| CoordinationError::Encoding)?;
        let mut candidate = self.state.clone();
        let expected_height = Self::next_height(candidate.committed.height)?;
        if candidate.domain.is_none()
            || self
                .idle_value_replies
                .iter()
                .any(|pending| pending.height != expected_height)
        {
            return Err(CoordinationError::NotImplemented);
        }
        for pending in &self.idle_value_replies {
            candidate
                .proposals
                .entry((pending.height.0, value_id.clone()))
                .or_insert(ProposalRecovery {
                    application_bytes: application_bytes.clone(),
                    metadata: None,
                    parts: Vec::new(),
                })
                .application_bytes = application_bytes.clone();
        }
        self.save_state(&candidate).await?;
        self.state = candidate;
        let pending_replies = std::mem::take(&mut self.idle_value_replies);
        for pending in pending_replies {
            let _ = pending
                .reply
                .send(LocallyProposedValue::new(pending.height, pending.round, value.clone()));
        }
        Ok(())
    }

    /// Answers `GetValue` only when there is admitted work; an idle node proposes no empty value.
    pub(super) async fn get_value(
        &mut self,
        height: Height,
        round: Round,
        reply: Reply<LocallyProposedValue<CoordinationContext>>,
    ) -> Result<(), CoordinationError> {
        let state = &self.state;
        if state.domain.is_none() {
            return Err(CoordinationError::NotImplemented);
        }
        let expected_height = Self::next_height(state.committed.height)?;
        if height != expected_height {
            return Err(CoordinationError::NotImplemented);
        }
        let Some(value) = self.build_application_value()? else {
            self.idle_value_replies
                .push(IdleValueReply { height, round, reply });
            return Ok(());
        };
        let application_bytes = serde_json::to_vec(&value).map_err(|_| CoordinationError::Encoding)?;
        let value_key = serde_json::to_vec(&value.id()).map_err(|_| CoordinationError::Encoding)?;
        let mut candidate = self.state.clone();
        candidate
            .proposals
            .entry((height.0, value_key))
            .and_modify(|proposal| proposal.application_bytes.clone_from(&application_bytes))
            .or_insert(ProposalRecovery {
                application_bytes,
                metadata: None,
                parts: Vec::new(),
            });
        self.save_state(&candidate).await?;
        self.state = candidate;
        reply
            .send(LocallyProposedValue::new(height, round, value))
            .map_err(|_| CoordinationError::NotImplemented)
    }

    /// Releases outstanding local proposal requests when the owning engine stops.
    pub(super) fn cancel_value_requests(&mut self) {
        self.idle_value_replies.clear();
    }

    /// Applies the retained application value and acknowledges Malachite's `Decided` callback.
    pub(super) async fn decided(
        &mut self,
        certificate: CommitCertificate<CoordinationContext>,
        extensions: VoteExtensions<CoordinationContext>,
        reply: Reply<()>,
    ) -> Result<(), CoordinationError> {
        let height = certificate.height.as_u64();
        let value_key = serde_json::to_vec(&certificate.value_id).map_err(|_| CoordinationError::Encoding)?;
        let proposal_key = (height, value_key);
        let mut candidate = self.state.clone();
        if candidate.committed.height.0 == height {
            if candidate
                .committed_values
                .get(&height)
                .is_some_and(|value| value.certificate.value_id == certificate.value_id)
            {
                return reply
                    .send(())
                    .map_err(|_| CoordinationError::NotImplemented);
            }
            return Err(CoordinationError::NotImplemented);
        }
        if Self::next_height(candidate.committed.height)?.0 != height {
            return Err(CoordinationError::NotImplemented);
        }
        let application_bytes = candidate
            .proposals
            .get(&proposal_key)
            .map(|proposal| proposal.application_bytes.clone())
            .ok_or(CoordinationError::NotImplemented)?;
        let value: ConsensusValue =
            serde_json::from_slice(&application_bytes).map_err(|_| CoordinationError::Encoding)?;
        let transactions = Self::decode_transactions(&value)?;
        let domain = candidate.domain.ok_or(CoordinationError::NotImplemented)?;
        let applied = candidate
            .committed
            .apply_transactions(domain, transactions, Height(height))?;
        candidate.committed.height = Height(height);
        candidate
            .pending
            .retain(|(event_id, _)| !applied.contains(event_id));
        let extended_certificate =
            ExtendedCommitCertificate::from_commit_certificate_and_extensions(certificate, extensions);
        let raw_value = RawDecidedValue::new(application_bytes.into(), extended_certificate);
        candidate
            .proposals
            .retain(|(cached_height, _), _| *cached_height > height);
        candidate.committed_values.insert(height, raw_value);
        self.save_state(&candidate).await?;
        self.state = candidate;
        reply
            .send(())
            .map_err(|_| CoordinationError::NotImplemented)
    }

    /// Starts the next height only after this height is committed and retained in memory.
    pub(super) async fn finalized(
        &mut self,
        certificate: CommitCertificate<CoordinationContext>,
        extensions: VoteExtensions<CoordinationContext>,
        reply: Reply<Next<CoordinationContext>>,
    ) -> Result<(), CoordinationError> {
        let height = certificate.height.as_u64();
        let mut candidate = self.state.clone();
        if candidate.committed.height.0 != height {
            return Err(CoordinationError::NotImplemented);
        }
        let next_height = Self::next_height(Height(height))?;
        let active_domain = candidate.domain.ok_or(CoordinationError::NotImplemented)?;
        if candidate
            .committed
            .regenesis
            .as_ref()
            .is_some_and(|regenesis| regenesis.successor != active_domain)
        {
            return Err(CoordinationError::NotImplemented);
        }
        let params = Self::height_params(&candidate)?;
        let raw_value = candidate
            .committed_values
            .get_mut(&height)
            .ok_or(CoordinationError::NotImplemented)?;
        if raw_value.certificate.value_id != certificate.value_id {
            return Err(CoordinationError::NotImplemented);
        }
        raw_value.certificate =
            ExtendedCommitCertificate::from_commit_certificate_and_extensions(certificate, extensions);
        self.save_state(&candidate).await?;
        self.state = candidate;
        reply
            .send(Next::Start(next_height, params))
            .map_err(|_| CoordinationError::NotImplemented)
    }

    fn next_height(height: Height) -> Result<Height, CoordinationError> {
        height
            .0
            .checked_add(1)
            .map(Height)
            .ok_or(CoordinationError::NotImplemented)
    }

    pub(super) fn recovery_state(state: &ApplicationState) -> Result<RecoveryState, CoordinationError> {
        let committed = &state.committed;
        let state_json = serde_json::to_string(&ApplicationRecovery {
            domain: state.domain,
            genesis_validators: state.genesis_validators.clone(),
            restream_nonce: state.restream_nonce,
            committed: CommittedRecovery {
                users: committed
                    .users
                    .iter()
                    .map(|(key, value)| (*key, value.clone()))
                    .collect(),
                machines: committed
                    .machines
                    .iter()
                    .map(|(key, value)| (*key, value.clone()))
                    .collect(),
                executions: committed
                    .executions
                    .iter()
                    .map(|(key, value)| (*key, value.clone()))
                    .collect(),
                schemas: committed.schemas.clone(),
                regenesis: committed.regenesis.clone(),
                height: committed.height,
                applied_events: committed.applied_events.iter().copied().collect(),
            },
        })
        .map_err(|_| CoordinationError::Encoding)?;

        let pending = state
            .pending
            .iter()
            .map(|(_, transaction)| {
                Ok(PendingRow {
                    event_json: serde_json::to_string(transaction).map_err(|_| CoordinationError::Encoding)?,
                    body_json: transaction.body_json()?,
                })
            })
            .collect::<Result<_, CoordinationError>>()?;

        let decided = state
            .committed_values
            .iter()
            .map(|(height, value)| {
                Ok(DecidedRow {
                    height: *height,
                    value_bytes: value.value_bytes.to_vec(),
                    certificate_json: Self::encode_decided_certificate(value)?,
                })
            })
            .collect::<Result<_, CoordinationError>>()?;

        let proposals = state
            .proposals
            .iter()
            .map(|((height, value_id), proposal)| {
                let value: ConsensusValue =
                    serde_json::from_slice(&proposal.application_bytes).map_err(|_| CoordinationError::Encoding)?;
                let payload_json = serde_json::to_string(proposal).map_err(|_| CoordinationError::Encoding)?;
                let canonical_id = serde_json::to_vec(&value.id()).map_err(|_| CoordinationError::Encoding)?;
                if canonical_id != *value_id {
                    return Err(CoordinationError::Encoding);
                }
                Ok(ProposalRow {
                    height: *height,
                    value_id: value_id.clone(),
                    payload_json,
                })
            })
            .collect::<Result<_, CoordinationError>>()?;

        Ok(RecoveryState {
            state_json,
            pending,
            decided,
            proposals,
        })
    }

    pub(super) fn restore_recovery(recovery: RecoveryState) -> Result<ApplicationState, CoordinationError> {
        let persisted: ApplicationRecovery =
            serde_json::from_str(&recovery.state_json).map_err(|_| CoordinationError::Encoding)?;
        if persisted.domain.is_some() != persisted.genesis_validators.is_some() {
            return Err(CoordinationError::Encoding);
        }
        if let Some(roster) = persisted.genesis_validators.as_deref() {
            Self::validator_set(roster)?;
        }
        let committed = Self::restore_committed(persisted.committed)?;
        let mut state = ApplicationState {
            domain: persisted.domain,
            genesis_validators: persisted.genesis_validators,
            committed,
            pending: Vec::with_capacity(recovery.pending.len()),
            restream_nonce: persisted.restream_nonce,
            ..ApplicationState::default()
        };

        let recovery_domain = state.domain;
        let mut recovered_events = HashSet::with_capacity(recovery.pending.len());
        for row in recovery.pending {
            let transaction: PendingTransaction =
                serde_json::from_str(&row.event_json).map_err(|_| CoordinationError::Encoding)?;
            if transaction.body_json()? != row.body_json {
                return Err(CoordinationError::Encoding);
            }
            transaction.validate_payload()?;
            let event_id = transaction.event_id()?;
            transaction.verify_signature(recovery_domain.ok_or(CoordinationError::Encoding)?)?;
            if !recovered_events.insert(event_id) || state.committed.applied_events.contains(&event_id) {
                return Err(CoordinationError::Encoding);
            }
            state.pending.push((event_id, transaction));
        }

        for row in recovery.decided {
            let value = Self::decode_decided_row(row.height, &row.value_bytes, &row.certificate_json)?;
            let application_value: ConsensusValue =
                serde_json::from_slice(&row.value_bytes).map_err(|_| CoordinationError::Encoding)?;
            if value.height().as_u64() > state.committed.height.0
                || application_value.id() != value.certificate.value_id
            {
                return Err(CoordinationError::Encoding);
            }
            if state.committed_values.insert(row.height, value).is_some() {
                return Err(CoordinationError::Encoding);
            }
        }
        let mut expected_height = 1_u64;
        for height in state.committed_values.keys() {
            if *height != expected_height {
                return Err(CoordinationError::Encoding);
            }
            expected_height = expected_height
                .checked_add(1)
                .ok_or(CoordinationError::Encoding)?;
        }
        if expected_height - 1 != state.committed.height.0 {
            return Err(CoordinationError::Encoding);
        }

        for row in recovery.proposals {
            let value_id: ConsensusValueId =
                serde_json::from_slice(&row.value_id).map_err(|_| CoordinationError::Encoding)?;
            if serde_json::to_vec(&value_id).map_err(|_| CoordinationError::Encoding)? != row.value_id {
                return Err(CoordinationError::Encoding);
            }
            let key = (row.height, row.value_id.clone());
            let payload: ProposalRecovery =
                serde_json::from_str(&row.payload_json).map_err(|_| CoordinationError::Encoding)?;
            let value: ConsensusValue =
                serde_json::from_slice(&payload.application_bytes).map_err(|_| CoordinationError::Encoding)?;
            if value.id() != value_id || state.proposals.contains_key(&key) {
                return Err(CoordinationError::Encoding);
            }
            if let Some(metadata) = &payload.metadata {
                if metadata.height.0 != row.height || state.domain.is_none() {
                    return Err(CoordinationError::Encoding);
                }
                Self::validate_application_value_in_state(&state, &value, metadata.height)?;
            }
            state.proposals.insert(key, payload);
        }
        Ok(state)
    }

    fn restore_committed(value: CommittedRecovery) -> Result<CommittedState, CoordinationError> {
        for schema in &value.schemas {
            serde_json::from_str::<serde_json::Value>(&schema.schema).map_err(|_| CoordinationError::InvalidJson)?;
        }
        if let Some(regenesis) = &value.regenesis {
            Self::validator_set(&regenesis.validators.0)?;
        }
        let (users, machines, executions) = (
            unique_map(value.users)?,
            unique_map(value.machines)?,
            unique_map(value.executions)?,
        );
        let applied_events: HashSet<_> = value.applied_events.iter().copied().collect();
        if applied_events.len() != value.applied_events.len() {
            return Err(CoordinationError::Encoding);
        }
        Ok(CommittedState {
            users,
            machines,
            executions,
            schemas: value.schemas,
            regenesis: value.regenesis,
            height: value.height,
            applied_events,
        })
    }

    fn encode_decided_certificate(value: &RawDecidedValue<CoordinationContext>) -> Result<String, CoordinationError> {
        let peer = value
            .certificate
            .commit_signatures
            .first()
            .map(|signature| signature.address.0)
            .ok_or(CoordinationError::NotImplemented)?;
        let response = SyncResponse::ValueResponse(ValueResponse::new(value.height(), vec![value.clone()]));
        let network_response = serialize_sync_response(&InboundRequestId::new("coordination-recovery"), &response)
            .map_err(|_| CoordinationError::Encoding)?;
        serde_json::to_string(&PersistedCertificate { peer, network_response }).map_err(|_| CoordinationError::Encoding)
    }

    fn decode_decided_row(
        height: u64,
        value_bytes: &[u8],
        certificate_json: &str,
    ) -> Result<RawDecidedValue<CoordinationContext>, CoordinationError> {
        let persisted: PersistedCertificate =
            serde_json::from_str(certificate_json).map_err(|_| CoordinationError::Encoding)?;
        ChainSigningScheme::decode_public_key(&persisted.peer).map_err(|_| CoordinationError::Encoding)?;
        let peer = persisted.peer;
        let event =
            decode_chain_network_event(peer, &persisted.network_response).map_err(|_| CoordinationError::Encoding)?;
        let value = match event {
            NetworkEvent::SyncResponse(_, _, Some(SyncResponse::ValueResponse(response))) => response
                .values
                .into_iter()
                .next()
                .ok_or(CoordinationError::Encoding)?,
            _ => return Err(CoordinationError::Encoding),
        };
        if value.height().as_u64() != height || value.value_bytes.as_ref() != value_bytes {
            return Err(CoordinationError::Encoding);
        }
        Ok(value)
    }

    fn height_params(state: &ApplicationState) -> Result<HeightParams<CoordinationContext>, CoordinationError> {
        let domain = state.domain.ok_or(CoordinationError::NotImplemented)?;
        let roster = if let Some(regenesis) = &state.committed.regenesis {
            if regenesis.successor != domain {
                return Err(CoordinationError::NotImplemented);
            }
            regenesis.validators.0.as_slice()
        } else {
            state
                .genesis_validators
                .as_deref()
                .ok_or(CoordinationError::NotImplemented)?
        };
        Ok(HeightParams::new(
            Self::validator_set(roster)?,
            LinearTimeouts::default(),
            None,
        ))
    }

    async fn received_proposal_part(
        &mut self,
        published_by: Option<PeerId>,
        part: StreamMessage<ConsensusProposalPart>,
        reply: Reply<Option<ProposedValue<CoordinationContext>>>,
    ) -> Result<(), CoordinationError> {
        let Some(publisher) = published_by.map(|peer| peer.to_string()) else {
            return reply
                .send(None)
                .map_err(|_| CoordinationError::NotImplemented);
        };
        let stream_key = (publisher, part.stream_id.to_bytes().to_vec());
        let sequence = part.sequence;
        let proposal_part = part.content.into_data();
        let mut candidate = self.state.clone();
        let proposal_bytes = if let Some(part) = proposal_part {
            match Self::push_proposal_part(
                candidate
                    .proposal_parts
                    .entry(stream_key.clone())
                    .or_default(),
                sequence,
                part,
            ) {
                Ok(Some(bytes)) => candidate
                    .proposal_parts
                    .remove(&stream_key)
                    .map(|assembly| (bytes, assembly.ordered_parts)),
                Ok(None) => None,
                Err(()) => {
                    candidate.proposal_parts.remove(&stream_key);
                    None
                }
            }
        } else {
            candidate.proposal_parts.remove(&stream_key);
            None
        };

        let Some((proposal_bytes, received_parts)) = proposal_bytes else {
            self.state = candidate;
            return reply
                .send(None)
                .map_err(|_| CoordinationError::NotImplemented);
        };
        let proposal = match serde_json::from_slice::<ConsensusProposal>(&proposal_bytes) {
            Ok(proposal) => proposal,
            Err(_) => {
                self.state = candidate;
                return reply
                    .send(None)
                    .map_err(|_| CoordinationError::NotImplemented);
            }
        };
        let value_bytes = serde_json::to_vec(&proposal.value).map_err(|_| CoordinationError::Encoding)?;
        let validity = match Self::validate_application_value_in_state(&candidate, &proposal.value, proposal.height) {
            Ok(()) => Validity::Valid,
            Err(CoordinationError::SignatureInvalid | CoordinationError::Encoding | CoordinationError::InvalidJson) => {
                Validity::Invalid
            }
            Err(error) => return Err(error),
        };
        let value_key = serde_json::to_vec(&proposal.value.id()).map_err(|_| CoordinationError::Encoding)?;
        let is_valid = validity == Validity::Valid;
        let proposed_value: ProposedValue<CoordinationContext> = ProposedValue {
            height: proposal.height,
            round: proposal.round.into(),
            valid_round: proposal.pol_round.into(),
            proposer: proposal.proposer,
            value: proposal.value,
            validity,
        };
        if is_valid {
            let key = (proposed_value.height.0, value_key);
            candidate.proposals.insert(
                key,
                ProposalRecovery {
                    application_bytes: value_bytes,
                    metadata: Some(ProposalMetadata {
                        height: proposed_value.height,
                        round: proposed_value.round.as_i64(),
                        valid_round: proposed_value.valid_round.as_i64(),
                        proposer: proposed_value.proposer,
                    }),
                    parts: received_parts,
                },
            );
            self.save_state(&candidate).await?;
        }
        self.state = candidate;
        reply
            .send(Some(proposed_value))
            .map_err(|_| CoordinationError::NotImplemented)
    }

    fn push_proposal_part(
        assembly: &mut ProposalPartAssembly,
        sequence: u64,
        part: ConsensusProposalPart,
    ) -> Result<Option<Vec<u8>>, ()> {
        if sequence < assembly.next_sequence {
            return Ok(None);
        }
        if assembly
            .queued_parts
            .get(&sequence)
            .is_some_and(|queued| queued != &part)
        {
            return Err(());
        }
        assembly.queued_parts.entry(sequence).or_insert(part);

        while let Some(part) = assembly.queued_parts.remove(&assembly.next_sequence) {
            let current_sequence = assembly.next_sequence;
            assembly.next_sequence = assembly.next_sequence.checked_add(1).ok_or(())?;
            assembly.ordered_parts.push(part.clone());
            match part {
                ConsensusProposalPart::First if current_sequence == 0 && !assembly.started => {
                    assembly.started = true;
                }
                ConsensusProposalPart::Data(bytes) if assembly.started => assembly.proposal_bytes.extend(bytes),
                ConsensusProposalPart::Last if assembly.started => {
                    return Ok(Some(std::mem::take(&mut assembly.proposal_bytes)));
                }
                _ => return Err(()),
            }
        }
        Ok(None)
    }

    async fn process_synced_value(
        &mut self,
        height: Height,
        round: Round,
        proposer: super::types::PublicKey,
        value_bytes: impl AsRef<[u8]>,
        reply: Reply<SyncedValueOutcome<CoordinationContext>>,
    ) -> Result<(), CoordinationError> {
        let value: ConsensusValue = match serde_json::from_slice(value_bytes.as_ref()) {
            Ok(value) => value,
            Err(_) => {
                return reply
                    .send(SyncedValueOutcome::PeerFault)
                    .map_err(|_| CoordinationError::NotImplemented);
            }
        };
        let mut candidate = self.state.clone();
        let validity = match Self::validate_application_value_in_state(&candidate, &value, height) {
            Ok(()) => Validity::Valid,
            Err(CoordinationError::SignatureInvalid | CoordinationError::Encoding | CoordinationError::InvalidJson) => {
                Validity::Invalid
            }
            Err(_) => {
                return reply
                    .send(SyncedValueOutcome::LocalTransientError)
                    .map_err(|_| CoordinationError::NotImplemented);
            }
        };
        let value_key = serde_json::to_vec(&value.id()).map_err(|_| CoordinationError::Encoding)?;
        let is_valid = validity == Validity::Valid;
        let proposed_value = ProposedValue {
            height,
            round,
            valid_round: Round::Nil,
            proposer,
            value,
            validity,
        };
        if is_valid {
            let key = (height.0, value_key);
            let proposal = candidate.proposals.entry(key).or_insert(ProposalRecovery {
                application_bytes: value_bytes.as_ref().to_vec(),
                metadata: None,
                parts: Vec::new(),
            });
            proposal.application_bytes = value_bytes.as_ref().to_vec();
            proposal.metadata = Some(ProposalMetadata {
                height,
                round: round.as_i64(),
                valid_round: -1,
                proposer,
            });
            self.save_state(&candidate).await?;
            self.state = candidate;
        }
        reply
            .send(SyncedValueOutcome::Verdict(proposed_value))
            .map_err(|_| CoordinationError::NotImplemented)
    }

    /// Re-publishes cached proposal parts through the app-channel network sender.
    async fn restream_proposal(
        &mut self,
        height: Height,
        round: Round,
        valid_round: Round,
        address: PublicKey,
        value_id: ConsensusValueId,
        network: &mpsc::Sender<NetworkMsg<CoordinationContext>>,
    ) -> Result<(), CoordinationError> {
        let value_key = serde_json::to_vec(&value_id).map_err(|_| CoordinationError::Encoding)?;
        let mut candidate = self.state.clone();
        let key = (height.0, value_key);
        let retained = candidate
            .proposals
            .get_mut(&key)
            .ok_or(CoordinationError::NotImplemented)?;
        let parts = if retained.parts.is_empty() {
            let application_bytes = &retained.application_bytes;
            let value: ConsensusValue =
                serde_json::from_slice(application_bytes).map_err(|_| CoordinationError::Encoding)?;
            if value.id() != value_id {
                return Err(CoordinationError::Encoding);
            }
            let proposal = ConsensusProposal {
                height,
                round: round.as_i64(),
                value,
                pol_round: valid_round.as_i64(),
                proposer: address,
            };
            vec![
                ConsensusProposalPart::First,
                ConsensusProposalPart::Data(serde_json::to_vec(&proposal).map_err(|_| CoordinationError::Encoding)?),
                ConsensusProposalPart::Last,
            ]
        } else {
            retained.parts.clone()
        };
        if parts.is_empty() {
            return Err(CoordinationError::NotImplemented);
        }

        retained.parts = parts.clone();
        let nonce = candidate.restream_nonce;
        candidate.restream_nonce = nonce
            .checked_add(1)
            .ok_or(CoordinationError::NotImplemented)?;
        self.save_state(&candidate).await?;
        self.state = candidate;
        let stream_id_bytes =
            serde_json::to_vec(&(height.0, round.as_i64(), valid_round.as_i64(), address, value_id, nonce))
                .map_err(|_| CoordinationError::Encoding)?;
        let stream_id = StreamId::new(Sha256::digest(stream_id_bytes).to_vec().into());
        for (sequence, content) in parts
            .into_iter()
            .map(StreamContent::Data)
            .chain(std::iter::once(StreamContent::Fin))
            .enumerate()
        {
            let sequence = u64::try_from(sequence).map_err(|_| CoordinationError::NotImplemented)?;
            network
                .send(NetworkMsg::PublishProposalPart(StreamMessage::new(
                    stream_id.clone(),
                    sequence,
                    content,
                )))
                .await
                .map_err(|_| CoordinationError::NotImplemented)?;
        }
        Ok(())
    }

    /// Applies a local pending transaction in the isolated unit-test projection.
    #[cfg(test)]
    pub(super) fn apply_committed(
        &mut self,
        event_id: CoordinationEventId,
        height: Height,
    ) -> Result<(), CoordinationError> {
        let transaction = self
            .state
            .pending
            .iter()
            .find(|(candidate, _)| *candidate == event_id)
            .map(|(_, transaction)| transaction.clone())
            .ok_or(CoordinationError::NotImplemented)?;
        self.apply_committed_transaction(transaction, height)?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn pending_transactions(&self) -> Vec<(CoordinationEventId, PendingTransaction)> {
        self.state.pending.clone()
    }

    /// Activates a chain identity in isolated unit tests.
    #[cfg(test)]
    pub(super) fn activate_chain(&mut self, domain: DomainId) -> Result<(), CoordinationError> {
        let state = &mut self.state;
        match state.domain {
            Some(active) if active != domain => Err(CoordinationError::NotImplemented),
            Some(_) => Ok(()),
            None => {
                state.domain = Some(domain);
                Ok(())
            }
        }
    }

    /// Applies a decoded transaction in the isolated unit-test projection.
    #[cfg(test)]
    pub(super) fn apply_committed_transaction(
        &mut self,
        transaction: PendingTransaction,
        height: Height,
    ) -> Result<CoordinationEventId, CoordinationError> {
        let state = &mut self.state;
        let domain = state.domain.ok_or(CoordinationError::NotImplemented)?;
        transaction.verify_signature(domain)?;
        let event_id = transaction.event_id()?;
        if state.committed.applied_events.contains(&event_id) {
            state
                .pending
                .retain(|(candidate, _)| *candidate != event_id);
            return Ok(event_id);
        }
        let mut committed = state.committed.clone();
        transaction.apply(&mut committed, height)?;
        committed.applied_events.insert(event_id);
        state.committed = committed;
        state
            .pending
            .retain(|(candidate, _)| *candidate != event_id);
        Ok(event_id)
    }
}
