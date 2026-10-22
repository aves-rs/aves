use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use base64::Engine;
use chrono::{DateTime, Duration, Utc};
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh::{Endpoint, EndpointAddr, PublicKey, endpoint::Connection};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use specta::datatype::reference::Reference;
use specta::{DataType, Generics, Type, TypeCollection};
use zeroize::Zeroize;

use super::peer::{PeerConfig, PeerEndpoint, PeerRecord, PeerTrustState};
use super::types::{ApprovedIssuer, AuthAdmissionRegistry, AvesAuthError, MachineIssuerId};

pub(crate) const AUTH_ALPN: &[u8] = b"sluice/auth";

const FIRST_CONTACT_PAYLOAD_LIMIT: usize = 4096;
const INVITATION_SECRET_BYTES: usize = 32;

/// A short-lived invitation used by the local pairing UI.
#[derive(Clone, Eq, PartialEq)]
pub struct PairingInvitation {
    machine_id: MachineIssuerId,
    secret: [u8; INVITATION_SECRET_BYTES],
}

impl PairingInvitation {
    pub(crate) fn secret(&self) -> &[u8; INVITATION_SECRET_BYTES] {
        &self.secret
    }
}

impl Type for PairingInvitation {
    fn inline(type_map: &mut TypeCollection, generics: Generics<'_>) -> DataType {
        String::inline(type_map, generics)
    }

    fn reference(type_map: &mut TypeCollection, generics: &[DataType]) -> Reference {
        String::reference(type_map, generics)
    }
}

impl schemars::JsonSchema for PairingInvitation {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "PairingInvitation".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::PairingInvitation").into()
    }

    fn inline_schema() -> bool {
        true
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        <String as schemars::JsonSchema>::json_schema(generator)
    }
}

impl fmt::Debug for PairingInvitation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PairingInvitation")
            .field("machine_id", &self.machine_id)
            .field("secret", &"[redacted]")
            .finish()
    }
}

impl Drop for PairingInvitation {
    fn drop(&mut self) {
        self.secret.zeroize();
    }
}

impl fmt::Display for PairingInvitation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let secret = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(self.secret);
        write!(formatter, "pair::{}::{}", self.machine_id, secret)
    }
}

impl std::str::FromStr for PairingInvitation {
    type Err = AvesAuthError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let rest = value
            .strip_prefix("pair::")
            .ok_or(AvesAuthError::MalformedInvitation)?;
        let mut fields = rest.split("::");
        let machine_id = fields
            .next()
            .ok_or(AvesAuthError::MalformedInvitation)?
            .parse()
            .map_err(|_| AvesAuthError::MalformedInvitation)?;
        let encoded = fields.next().ok_or(AvesAuthError::MalformedInvitation)?;
        if fields.next().is_some() {
            return Err(AvesAuthError::MalformedInvitation);
        }
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| AvesAuthError::MalformedInvitation)?;
        let secret = bytes
            .try_into()
            .map_err(|_| AvesAuthError::MalformedInvitation)?;
        Ok(Self { machine_id, secret })
    }
}

impl Serialize for PairingInvitation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for PairingInvitation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FirstContactMetadata {
    machine_id: MachineIssuerId,
    secret: Option<[u8; INVITATION_SECRET_BYTES]>,
    endpoint: EndpointAddr,
    audience: uuid::Uuid,
    refresh: bool,
}

/// The authenticated identity and endpoint observed during first contact.
#[derive(Clone, Debug)]
struct PendingPeer {
    endpoint: PeerEndpoint,
    _audience: uuid::Uuid,
    _observed_at: DateTime<Utc>,
}

/// Owns pairing policy, durable peer state, and live Auth admission changes.
pub(crate) struct PairingCoordinator {
    peers: Vec<PeerRecord>,
    pending: Vec<(PublicKey, PendingPeer)>,
    config_path: Option<PathBuf>,
    admission_registry: AuthAdmissionRegistry,
    local_issuer: MachineIssuerId,
    audience: uuid::Uuid,
    endpoint: Option<Endpoint>,
    endpoint_addr: Option<EndpointAddr>,
    invitations: Vec<InvitationRecord>,
}

#[derive(Clone)]
struct InvitationRecord {
    secret_hash: [u8; 32],
    expires_at: DateTime<Utc>,
    consumed: bool,
}

impl PairingCoordinator {
    pub(crate) fn load(
        path: &Path,
        admission_registry: AuthAdmissionRegistry,
        local_issuer: MachineIssuerId,
        audience: uuid::Uuid,
    ) -> Result<Self, AvesAuthError> {
        let config = PeerConfig::load(path)?;
        for record in &config.peers {
            if record.endpoint.public_key() != record.peer {
                return Err(AvesAuthError::PairingRejected {
                    cause: "stored peer endpoint identity does not match its key".to_owned(),
                });
            }
        }
        Ok(Self {
            peers: config.peers,
            pending: Vec::new(),
            config_path: Some(path.to_owned()),
            admission_registry,
            local_issuer,
            audience,
            endpoint: None,
            endpoint_addr: None,
            invitations: Vec::new(),
        })
    }

    pub(crate) fn new_invitation(&mut self, ttl_seconds: u64) -> Result<PairingInvitation, AvesAuthError> {
        if !(1..=3600).contains(&ttl_seconds) {
            return Err(AvesAuthError::MalformedInvitation);
        }
        let mut secret = [0u8; INVITATION_SECRET_BYTES];
        rand::fill(&mut secret);
        let invitation = PairingInvitation {
            machine_id: self.local_issuer.clone(),
            secret,
        };
        self.invitations.push(InvitationRecord {
            secret_hash: Sha256::digest(secret).into(),
            expires_at: Utc::now() + Duration::seconds(ttl_seconds as i64),
            consumed: false,
        });
        Ok(invitation)
    }

    fn consume_invitation(&mut self, secret: &[u8; 32]) -> Result<(), AvesAuthError> {
        let hash: [u8; 32] = Sha256::digest(secret).into();
        let record = self
            .invitations
            .iter_mut()
            .find(|record| record.secret_hash == hash)
            .ok_or(AvesAuthError::UnknownInvitationSecret)?;
        if record.consumed {
            return Err(AvesAuthError::ConsumedInvitationSecret);
        }
        if record.expires_at <= Utc::now() {
            return Err(AvesAuthError::ExpiredInvitationSecret);
        }
        record.consumed = true;
        Ok(())
    }

    async fn outbound_exchange(
        connection: Connection,
        local: &FirstContactMetadata,
        open_bi_error: fn(String) -> AvesAuthError,
    ) -> Result<FirstContactMetadata, AvesAuthError> {
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .map_err(|error| open_bi_error(format!("{error}")))?;
        let payload = serde_json::to_vec(local).map_err(|error| AvesAuthError::PairingRejected {
            cause: format!("{error}"),
        })?;
        send.write_all(&payload)
            .await
            .map_err(|error| AvesAuthError::PairingRejected {
                cause: format!("{error}"),
            })?;
        send.finish()
            .map_err(|error| AvesAuthError::PairingRejected {
                cause: format!("{error}"),
            })?;
        let response = receive
            .read_to_end(FIRST_CONTACT_PAYLOAD_LIMIT)
            .await
            .map_err(|error| AvesAuthError::PairingRejected {
                cause: format!("{error}"),
            })?;
        serde_json::from_slice(&response).map_err(|error| AvesAuthError::PairingRejected {
            cause: format!("{error}"),
        })
    }

    fn receive_first_contact(
        &mut self,
        remote_id: PublicKey,
        metadata: FirstContactMetadata,
    ) -> Result<FirstContactMetadata, AvesAuthError> {
        if metadata.machine_id.public_key() != remote_id || metadata.endpoint.id != remote_id {
            return Err(AvesAuthError::PairingRejected {
                cause: "first-contact endpoint identity does not match the authenticated connection".to_owned(),
            });
        }
        if metadata.refresh {
            if !self
                .peers
                .iter()
                .any(|record| record.peer == remote_id && record.state == PeerTrustState::Confirmed)
            {
                return Err(AvesAuthError::PeerNotTrusted);
            }
        } else {
            let secret = metadata
                .secret
                .as_ref()
                .ok_or(AvesAuthError::UnknownInvitationSecret)?;
            self.consume_invitation(secret)?;
            self.observe(remote_id, metadata.endpoint, metadata.audience)?;
        }
        Ok(FirstContactMetadata {
            machine_id: self.local_issuer.clone(),
            secret: None,
            endpoint: self.local_endpoint()?,
            audience: self.local_audience(),
            refresh: true,
        })
    }

    pub(crate) fn confirm(&mut self, peer: MachineIssuerId) -> Result<(), AvesAuthError> {
        if peer == self.local_issuer {
            return Err(AvesAuthError::LocalIssuerImmutable);
        }
        let public_key = peer.public_key();
        let pending_index = self
            .pending
            .iter()
            .position(|(key, _)| *key == public_key)
            .ok_or(AvesAuthError::FingerprintNotConfirmed)?;
        let pending = self.pending[pending_index].1.clone();

        let record = PeerRecord {
            peer: public_key,
            endpoint: pending.endpoint,
            state: PeerTrustState::Confirmed,
            updated_at: Utc::now(),
        };
        let mut peers = self.peers.clone();
        if let Some(existing) = peers
            .iter_mut()
            .find(|existing| existing.peer == public_key)
        {
            *existing = record;
        } else {
            peers.push(record);
        }
        self.persist(&peers)?;
        self.admission_registry
            .upsert_remote(ApprovedIssuer { issuer: peer })?;
        self.peers = peers;
        self.pending.remove(pending_index);
        Ok(())
    }

    pub(super) fn is_confirmed(&self, peer: &MachineIssuerId) -> bool {
        self.peers
            .iter()
            .find(|record| record.peer == peer.public_key())
            .is_some_and(|record| record.state == PeerTrustState::Confirmed)
    }

    pub(crate) fn attach_endpoint(&mut self, endpoint: Endpoint, endpoint_addr: EndpointAddr) {
        self.endpoint = Some(endpoint);
        self.endpoint_addr = Some(endpoint_addr);
    }

    pub(crate) fn detach_endpoint(&mut self) {
        self.endpoint = None;
        self.endpoint_addr = None;
    }

    fn observe(
        &mut self,
        remote_id: PublicKey,
        endpoint: EndpointAddr,
        audience: uuid::Uuid,
    ) -> Result<(), AvesAuthError> {
        if endpoint.id != remote_id {
            return Err(AvesAuthError::PairingRejected {
                cause: "first-contact endpoint identity does not match the authenticated connection".to_owned(),
            });
        }
        let pending = PendingPeer {
            endpoint: PeerEndpoint::new(endpoint),
            _audience: audience,
            _observed_at: Utc::now(),
        };
        if let Some((_, existing)) = self.pending.iter_mut().find(|(key, _)| *key == remote_id) {
            *existing = pending;
        } else {
            self.pending.push((remote_id, pending));
        }
        Ok(())
    }

    #[cfg(not(test))]
    fn first_contact_metadata(
        &self,
        invitation: &PairingInvitation,
    ) -> Result<(Endpoint, FirstContactMetadata), AvesAuthError> {
        Ok((
            self.endpoint
                .clone()
                .ok_or_else(|| AvesAuthError::PairingRejected {
                    cause: "Auth Iroh endpoint is not attached".to_owned(),
                })?,
            FirstContactMetadata {
                machine_id: self.local_issuer.clone(),
                secret: Some(*invitation.secret()),
                endpoint: self.local_endpoint()?,
                audience: self.local_audience(),
                refresh: false,
            },
        ))
    }

    #[cfg(not(test))]
    fn record_outbound_observation(
        &mut self,
        remote_id: PublicKey,
        metadata: FirstContactMetadata,
    ) -> Result<(), AvesAuthError> {
        if metadata.machine_id.public_key() != remote_id || metadata.endpoint.id != remote_id {
            return Err(AvesAuthError::PairingRejected {
                cause: "first-contact response endpoint identity does not match the authenticated connection"
                    .to_owned(),
            });
        }
        self.observe(remote_id, metadata.endpoint, metadata.audience)
    }

    #[cfg(not(test))]
    pub(crate) async fn outbound_first_contact(
        owner: &PairingOwner,
        invitation: &PairingInvitation,
    ) -> Result<MachineIssuerId, AvesAuthError> {
        let (endpoint, local) = owner
            .lock()
            .expect("Auth pairing coordinator lock poisoned")
            .first_contact_metadata(invitation)?;
        let connection = endpoint
            .connect(invitation.machine_id.public_key(), AUTH_ALPN)
            .await
            .map_err(|error| AvesAuthError::PairingRejected {
                cause: format!("{error}"),
            })?;
        let remote_id = connection.remote_id();
        if remote_id != invitation.machine_id.public_key() {
            return Err(AvesAuthError::PairingRejected {
                cause: "pairing invitation target does not match authenticated remote identity".to_owned(),
            });
        }
        let remote = Self::outbound_exchange(connection, &local, AvesAuthError::Internal).await?;
        owner
            .lock()
            .expect("Auth pairing coordinator lock poisoned")
            .record_outbound_observation(remote_id, remote)
            .map(|()| MachineIssuerId::from(remote_id))
    }

    pub(crate) fn peers(&self) -> Vec<PeerRecord> {
        self.peers.clone()
    }

    pub(crate) fn pending_candidates(&self) -> Vec<MachineIssuerId> {
        let mut candidates: Vec<_> = self
            .pending
            .iter()
            .map(|(key, _)| MachineIssuerId::from(*key))
            .collect();
        candidates.sort_by_key(ToString::to_string);
        candidates.dedup();
        candidates
    }

    pub(crate) async fn refresh_audience(
        owner: &PairingOwner,
        target: &MachineIssuerId,
    ) -> Result<(EndpointAddr, uuid::Uuid), AvesAuthError> {
        let (endpoint, peer_endpoint, local) = {
            let coordinator = owner
                .lock()
                .expect("Auth pairing coordinator lock poisoned");
            let record = coordinator
                .peers
                .iter()
                .find(|record| record.peer == target.public_key() && record.state == PeerTrustState::Confirmed)
                .ok_or(AvesAuthError::PeerNotTrusted)?;
            (
                coordinator
                    .endpoint
                    .clone()
                    .ok_or_else(|| AvesAuthError::PairingRejected {
                        cause: "Auth Iroh endpoint is not attached".to_owned(),
                    })?,
                record.endpoint.endpoint().clone(),
                FirstContactMetadata {
                    machine_id: coordinator.local_issuer.clone(),
                    secret: None,
                    endpoint: coordinator.local_endpoint()?,
                    audience: coordinator.local_audience(),
                    refresh: true,
                },
            )
        };
        let connection = endpoint
            .connect(peer_endpoint.clone(), AUTH_ALPN)
            .await
            .map_err(|error| AvesAuthError::PairingRejected {
                cause: format!("{error}"),
            })?;
        let remote_id = connection.remote_id();
        if remote_id != target.public_key() {
            return Err(AvesAuthError::PairingRejected {
                cause: "confirmed peer identity changed".to_owned(),
            });
        }
        let response = Self::outbound_exchange(connection, &local, |error| AvesAuthError::PairingRejected {
            cause: error,
        })
        .await?;
        Ok((peer_endpoint, response.audience))
    }

    fn persist(&self, peers: &[PeerRecord]) -> Result<(), AvesAuthError> {
        let path = self
            .config_path
            .as_deref()
            .ok_or_else(|| AvesAuthError::PairingRejected {
                cause: "pairing persistence is unavailable".to_owned(),
            })?;
        PeerConfig { peers: peers.to_vec() }
            .persist(path)
            .map_err(AvesAuthError::from)
    }

    pub(crate) fn revoke(&mut self, peer: MachineIssuerId) -> Result<(), AvesAuthError> {
        if peer == self.local_issuer {
            return Err(AvesAuthError::LocalIssuerImmutable);
        }
        let mut peers = self.peers.clone();
        if let Some(record) = peers
            .iter_mut()
            .find(|record| record.peer == peer.public_key())
        {
            record.state = PeerTrustState::Revoked;
            record.updated_at = Utc::now();
        }
        self.persist(&peers)?;
        self.admission_registry.remove_remote(&peer)?;
        self.peers = peers;
        Ok(())
    }

    fn local_endpoint(&self) -> Result<EndpointAddr, AvesAuthError> {
        self.endpoint_addr
            .clone()
            .ok_or_else(|| AvesAuthError::PairingRejected {
                cause: "Auth Iroh endpoint is not attached".to_owned(),
            })
    }

    fn local_audience(&self) -> uuid::Uuid {
        self.audience
    }

    #[cfg(test)]
    pub(crate) fn ephemeral(
        admission_registry: AuthAdmissionRegistry,
        local_issuer: MachineIssuerId,
        audience: uuid::Uuid,
    ) -> Self {
        Self {
            peers: Vec::new(),
            pending: Vec::new(),
            config_path: None,
            admission_registry,
            local_issuer,
            audience,
            endpoint: None,
            endpoint_addr: None,
            invitations: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn seed_confirmed_peer_for_test(&mut self, peer: &MachineIssuerId) {
        self.peers.push(PeerRecord {
            peer: peer.public_key(),
            endpoint: PeerEndpoint::new(EndpointAddr::new(peer.public_key())),
            state: PeerTrustState::Confirmed,
            updated_at: Utc::now(),
        });
    }
}

/// Shared coordinator handle used by Auth and the native Iroh protocol handler.
pub(crate) type PairingOwner = Arc<Mutex<PairingCoordinator>>;

/// Auth's minimal native-Iroh first-contact protocol.
#[derive(Clone)]
pub(crate) struct AuthFirstContactHandler {
    pairing: PairingOwner,
}

impl AuthFirstContactHandler {
    pub(crate) fn new(pairing: PairingOwner) -> Self {
        Self { pairing }
    }
}

impl fmt::Debug for AuthFirstContactHandler {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("AuthFirstContactHandler").finish()
    }
}

impl ProtocolHandler for AuthFirstContactHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let remote_id = connection.remote_id();
        let (mut send, mut receive) = connection.accept_bi().await?;
        let payload = receive
            .read_to_end(FIRST_CONTACT_PAYLOAD_LIMIT)
            .await
            .map_err(AcceptError::from_err)?;
        let metadata: FirstContactMetadata = serde_json::from_slice(&payload).map_err(AcceptError::from_err)?;
        let response = self
            .pairing
            .lock()
            .expect("Auth pairing coordinator lock poisoned")
            .receive_first_contact(remote_id, metadata)
            .map_err(AcceptError::from_err)?;
        let response = serde_json::to_vec(&response).map_err(AcceptError::from_err)?;
        send.write_all(&response)
            .await
            .map_err(AcceptError::from_err)?;
        send.finish().map_err(AcceptError::from_err)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn machine_id() -> MachineIssuerId {
        iroh::SecretKey::generate().public().into()
    }

    #[test]
    fn invitation_round_trips_as_a_string() {
        let invitation = PairingInvitation {
            machine_id: machine_id(),
            secret: [7; INVITATION_SECRET_BYTES],
        };
        let encoded = invitation.to_string();
        assert!(encoded.starts_with("pair::"));
        assert_eq!(encoded.parse::<PairingInvitation>().unwrap(), invitation);
        assert_eq!(serde_json::to_string(&invitation).unwrap(), format!("\"{encoded}\""));
        assert!(
            !format!("{invitation:?}")
                .contains(&base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([7; INVITATION_SECRET_BYTES]))
        );
    }

    #[test]
    fn malformed_invitation_formats_are_rejected() {
        for value in [
            "",
            "pair::",
            "pair::bad::secret",
            "pair::bad::Zm9v",
            "pair::bad::not-padding=",
        ] {
            assert_eq!(
                value.parse::<PairingInvitation>(),
                Err(AvesAuthError::MalformedInvitation)
            );
        }
    }

    #[test]
    fn invitation_secret_is_consumed_once() {
        let local = machine_id();
        let registry = AuthAdmissionRegistry::new(ApprovedIssuer { issuer: local.clone() });
        let mut coordinator = PairingCoordinator::ephemeral(registry, local, uuid::Uuid::new_v4());
        let invitation = coordinator.new_invitation(60).unwrap();
        coordinator.consume_invitation(invitation.secret()).unwrap();
        assert_eq!(
            coordinator.consume_invitation(invitation.secret()),
            Err(AvesAuthError::ConsumedInvitationSecret)
        );
    }

    #[test]
    fn expired_invitation_secret_is_rejected() {
        let local = machine_id();
        let registry = AuthAdmissionRegistry::new(ApprovedIssuer { issuer: local.clone() });
        let mut coordinator = PairingCoordinator::ephemeral(registry, local, uuid::Uuid::new_v4());
        let invitation = coordinator.new_invitation(60).unwrap();
        coordinator.invitations[0].expires_at = Utc::now() - Duration::seconds(1);
        assert_eq!(
            coordinator.consume_invitation(invitation.secret()),
            Err(AvesAuthError::ExpiredInvitationSecret)
        );
    }

    #[test]
    fn authenticated_identity_mismatch_is_rejected_before_invitation_consumption() {
        let local = machine_id();
        let remote = machine_id();
        let registry = AuthAdmissionRegistry::new(ApprovedIssuer { issuer: local.clone() });
        let mut coordinator = PairingCoordinator::ephemeral(registry, local, uuid::Uuid::new_v4());
        let invitation = coordinator.new_invitation(60).unwrap();
        let error = coordinator
            .receive_first_contact(
                remote.public_key(),
                FirstContactMetadata {
                    machine_id: machine_id(),
                    secret: Some(*invitation.secret()),
                    endpoint: EndpointAddr::new(remote.public_key()),
                    audience: uuid::Uuid::new_v4(),
                    refresh: false,
                },
            )
            .expect_err("an authenticated identity mismatch must be rejected");
        assert!(matches!(error, AvesAuthError::PairingRejected { .. }));
        coordinator
            .consume_invitation(invitation.secret())
            .expect("mismatch must not consume the invitation");
    }

    #[test]
    fn pending_candidates_are_sorted_and_deduplicated() {
        let local = machine_id();
        let first = machine_id();
        let second = machine_id();
        let registry = AuthAdmissionRegistry::new(ApprovedIssuer { issuer: local.clone() });
        let mut coordinator = PairingCoordinator::ephemeral(registry, local, uuid::Uuid::new_v4());
        for peer in [&second, &first, &second] {
            coordinator.pending.push((
                peer.public_key(),
                PendingPeer {
                    endpoint: PeerEndpoint::new(EndpointAddr::new(peer.public_key())),
                    _audience: uuid::Uuid::new_v4(),
                    _observed_at: Utc::now(),
                },
            ));
        }
        let candidates = coordinator.pending_candidates();
        assert_eq!(candidates, {
            let mut expected = vec![first, second];
            expected.sort_by_key(ToString::to_string);
            expected
        });
    }

    #[test]
    fn confirm_rejects_an_unobserved_fingerprint() {
        let local = machine_id();
        let registry = AuthAdmissionRegistry::new(ApprovedIssuer { issuer: local.clone() });
        let mut coordinator = PairingCoordinator::ephemeral(registry, local, uuid::Uuid::new_v4());
        assert_eq!(
            coordinator.confirm(machine_id()),
            Err(AvesAuthError::FingerprintNotConfirmed)
        );
    }
}
