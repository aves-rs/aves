use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};
use iroh::PublicKey;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use specta::datatype::reference::Reference;
use specta::{DataType, Generics, Type, TypeCollection};
use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

use crate::bus::DispatchContextError;
use crate::runtime::GeneratedCallError;

use super::peer::PeerConfigError;

/// The stable identity of the Aves machine issuer.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct MachineIssuerId(PublicKey);

impl MachineIssuerId {
    pub(crate) fn verification_key(&self) -> [u8; 32] {
        *self.0.as_bytes()
    }

    /// Returns the issuer's public key.
    pub fn public_key(&self) -> PublicKey {
        self.0
    }
}

impl From<PublicKey> for MachineIssuerId {
    fn from(value: PublicKey) -> Self {
        Self(value)
    }
}

impl fmt::Display for MachineIssuerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::str::FromStr for MachineIssuerId {
    type Err = iroh::KeyParsingError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

impl Serialize for MachineIssuerId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for MachineIssuerId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

impl Type for MachineIssuerId {
    fn inline(type_map: &mut TypeCollection, generics: Generics<'_>) -> DataType {
        String::inline(type_map, generics)
    }

    fn reference(type_map: &mut TypeCollection, generics: &[DataType]) -> Reference {
        String::reference(type_map, generics)
    }
}

impl schemars::JsonSchema for MachineIssuerId {
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("MachineIssuerId")
    }

    fn schema_id() -> Cow<'static, str> {
        Cow::Borrowed(concat!(module_path!(), "::MachineIssuerId"))
    }

    fn inline_schema() -> bool {
        true
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        <String as schemars::JsonSchema>::json_schema(generator)
    }
}

/// Capabilities carried by an authenticated bus request.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, specta::Type, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuthCapability {
    AllBus,
    PullCloudBlobs,
}

/// The typed issuer identity accepted by bus admission.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ApprovedIssuer {
    pub issuer: MachineIssuerId,
    pub relationships: BTreeMap<RelationshipKey, PeerGrants>,
}

/// Native protocols independently authorized for a peer.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub enum NativeProtocol {
    AuthFirstContact,
    Bus,
    Gossip,
    Blobs,
}

/// A generated qualified bus operation name.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, Type, schemars::JsonSchema)]
#[serde(transparent)]
pub struct QualifiedOperation(pub String);

/// Bus operations granted by one relationship.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub enum OperationGrants {
    All,
    Only(BTreeSet<QualifiedOperation>),
}

/// Independent protocol and bus operation permissions.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Type, schemars::JsonSchema)]
pub struct PeerGrants {
    pub protocols: BTreeSet<NativeProtocol>,
    pub operations: OperationGrants,
}

impl PeerGrants {
    /// Grants every native protocol and operation.
    pub fn full() -> Self {
        Self {
            protocols: [
                NativeProtocol::AuthFirstContact,
                NativeProtocol::Bus,
                NativeProtocol::Gossip,
                NativeProtocol::Blobs,
            ]
            .into_iter()
            .collect(),
            operations: OperationGrants::All,
        }
    }

    /// Combines the authority of two relationships.
    pub fn union(&self, other: &Self) -> Self {
        let operations = match (&self.operations, &other.operations) {
            (OperationGrants::All, _) | (_, OperationGrants::All) => OperationGrants::All,
            (OperationGrants::Only(a), OperationGrants::Only(b)) => {
                OperationGrants::Only(a.union(b).cloned().collect())
            }
        };
        Self {
            protocols: self.protocols.union(&other.protocols).copied().collect(),
            operations,
        }
    }
}

impl Default for PeerGrants {
    fn default() -> Self {
        Self {
            protocols: [NativeProtocol::AuthFirstContact, NativeProtocol::Bus]
                .into_iter()
                .collect(),
            operations: OperationGrants::Only(BTreeSet::new()),
        }
    }
}

/// Application-owned stable relationship identity.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, Type, schemars::JsonSchema)]
#[serde(transparent)]
pub struct RelationshipKey(pub String);

#[derive(Debug)]
struct RegisteredConnection {
    peer: MachineIssuerId,
    protocol: NativeProtocol,
    connection: iroh::endpoint::Connection,
}

/// Removes a native connection from admission tracking on drop.
#[derive(Debug)]
pub struct ConnectionRegistration {
    connections: Arc<RwLock<HashMap<Uuid, RegisteredConnection>>>,
    id: Uuid,
}

impl Drop for ConnectionRegistration {
    fn drop(&mut self) {
        self.connections
            .write()
            .expect("auth connection registry lock poisoned")
            .remove(&self.id);
    }
}

/// Shared admission state with one immutable local issuer and dynamic remote issuers.
#[derive(Clone, Debug)]
pub struct AuthAdmissionRegistry {
    local_issuer: ApprovedIssuer,
    remote_issuers: Arc<RwLock<HashMap<MachineIssuerId, ApprovedIssuer>>>,
    generation: Arc<watch::Sender<u64>>,
    connections: Arc<RwLock<HashMap<Uuid, RegisteredConnection>>>,
}

impl AuthAdmissionRegistry {
    pub fn new(local_issuer: ApprovedIssuer) -> Self {
        Self {
            local_issuer,
            remote_issuers: Arc::new(RwLock::new(HashMap::new())),
            generation: Arc::new(watch::channel(0).0),
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Subscribe to admission changes before reading the current projection.
    pub fn subscribe_generation(&self) -> watch::Receiver<u64> {
        self.generation.subscribe()
    }

    /// Returns the union of all grants for an admitted issuer.
    pub fn effective_grants(&self, issuer: &MachineIssuerId) -> Option<PeerGrants> {
        if issuer == &self.local_issuer.issuer {
            return Some(PeerGrants::full());
        }
        let remotes = self
            .remote_issuers
            .read()
            .expect("auth admission registry lock poisoned");
        remotes.get(issuer).map(Self::issuer_grants)
    }

    fn issuer_grants(issuer: &ApprovedIssuer) -> PeerGrants {
        issuer.relationships.values().fold(
            PeerGrants {
                protocols: BTreeSet::new(),
                operations: OperationGrants::Only(BTreeSet::new()),
            },
            |grants, next| grants.union(next),
        )
    }

    /// Tests a native protocol grant.
    pub fn allows_protocol(&self, issuer: &MachineIssuerId, protocol: NativeProtocol) -> bool {
        self.effective_grants(issuer)
            .is_some_and(|grants| grants.protocols.contains(&protocol))
    }

    /// Tests a generated qualified operation grant.
    pub fn allows_operation(&self, issuer: &MachineIssuerId, qualified: &str) -> bool {
        self.effective_grants(issuer)
            .is_some_and(|grants| match grants.operations {
                OperationGrants::All => true,
                OperationGrants::Only(operations) => operations.contains(&QualifiedOperation(qualified.to_owned())),
            })
    }

    /// Registers a connection, closing it immediately if its protocol is no longer allowed.
    pub fn register_connection(
        &self,
        peer: MachineIssuerId,
        protocol: NativeProtocol,
        connection: iroh::endpoint::Connection,
    ) -> ConnectionRegistration {
        let remotes = self
            .remote_issuers
            .read()
            .expect("auth admission registry lock poisoned");
        let allowed = peer == self.local_issuer.issuer
            || remotes
                .get(&peer)
                .is_some_and(|issuer| Self::issuer_grants(issuer).protocols.contains(&protocol));
        let mut connections = self
            .connections
            .write()
            .expect("auth connection registry lock poisoned");
        let id = Uuid::new_v4();
        if allowed {
            connections.insert(
                id,
                RegisteredConnection {
                    peer,
                    protocol,
                    connection,
                },
            );
        } else {
            connection.close(1u32.into(), b"peer protocol grant revoked");
        }
        ConnectionRegistration {
            connections: self.connections.clone(),
            id,
        }
    }

    fn close_disallowed(&self, remotes: &HashMap<MachineIssuerId, ApprovedIssuer>) {
        let mut connections = self
            .connections
            .write()
            .expect("auth connection registry lock poisoned");
        connections.retain(|_, entry| {
            let allowed = entry.peer == self.local_issuer.issuer
                || remotes.get(&entry.peer).is_some_and(|issuer| {
                    Self::issuer_grants(issuer)
                        .protocols
                        .contains(&entry.protocol)
                });
            if !allowed {
                entry
                    .connection
                    .close(1u32.into(), b"peer protocol grant revoked");
            }
            allowed
        });
    }

    pub fn upsert_remote(&self, issuer: ApprovedIssuer) -> Result<(), AvesAuthError> {
        if issuer.issuer == self.local_issuer.issuer {
            return Err(AvesAuthError::LocalIssuerImmutable);
        }
        let mut remotes = self
            .remote_issuers
            .write()
            .expect("auth admission registry lock poisoned");
        remotes.insert(issuer.issuer.clone(), issuer);
        self.close_disallowed(&remotes);
        drop(remotes);
        self.generation
            .send_modify(|generation| *generation = generation.wrapping_add(1));
        Ok(())
    }

    pub fn remove_remote(&self, issuer: &MachineIssuerId) -> Result<(), AvesAuthError> {
        if issuer == &self.local_issuer.issuer {
            return Err(AvesAuthError::LocalIssuerImmutable);
        }
        let mut remotes = self
            .remote_issuers
            .write()
            .expect("auth admission registry lock poisoned");
        remotes.remove(issuer);
        self.close_disallowed(&remotes);
        drop(remotes);
        self.generation
            .send_modify(|generation| *generation = generation.wrapping_add(1));
        Ok(())
    }

    /// Replaces the remote admission projection after durable pairing reconciliation.
    pub(crate) fn replace_remotes(&self, issuers: Vec<ApprovedIssuer>) -> Result<(), AvesAuthError> {
        let mut remotes = HashMap::with_capacity(issuers.len());
        for issuer in issuers {
            if issuer.issuer == self.local_issuer.issuer {
                return Err(AvesAuthError::LocalIssuerImmutable);
            }
            remotes.insert(issuer.issuer.clone(), issuer);
        }
        let mut current = self
            .remote_issuers
            .write()
            .expect("auth admission registry lock poisoned");
        *current = remotes;
        self.close_disallowed(&current);
        drop(current);
        self.generation
            .send_modify(|generation| *generation = generation.wrapping_add(1));
        Ok(())
    }

    pub(crate) fn current_issuers(&self) -> Vec<ApprovedIssuer> {
        let mut issuers = vec![self.local_issuer.clone()];
        issuers.extend(
            self.remote_issuers
                .read()
                .expect("auth admission registry lock poisoned")
                .values()
                .cloned(),
        );
        issuers
    }
}

/// The claims projection forwarded after successful JWT admission.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, specta::Type, schemars::JsonSchema)]
pub struct AuthProjection {
    pub issuer: MachineIssuerId,
    pub sub: String,
    pub cap: Vec<AuthCapability>,
}

impl AuthProjection {
    pub fn new(issuer: MachineIssuerId, sub: String, cap: Vec<AuthCapability>) -> Self {
        Self { issuer, sub, cap }
    }
}

/// Shared issuance input used by Auth's actor boundary.
#[derive(Clone, Debug)]
pub(super) struct AuthIssueRequest {
    pub(super) sub: String,
    pub(super) cap: Vec<AuthCapability>,
    pub(super) exp: DateTime<Utc>,
}

/// Private peer-token input resolved from a confirmed pairing record.
#[derive(Clone, Debug)]
pub(super) struct PeerJwtIssueRequest {
    pub(super) target: MachineIssuerId,
    pub(super) audience: Uuid,
    pub(super) sub: String,
    pub(super) cap: Vec<AuthCapability>,
}

/// Typed failures produced while issuing or verifying Aves authentication.
#[derive(Debug, Error, PartialEq, Eq, serde::Serialize, specta::Type)]
pub enum AvesAuthError {
    #[error("machine identity keyring namespace cannot be empty")]
    EmptyKeyringNamespace,
    #[error("machine identity secret has invalid length: expected {expected} bytes, got {actual}")]
    InvalidMachineIdentitySecretLength { expected: usize, actual: usize },
    #[error("machine identity keyring operation failed: {cause}")]
    MachineIdentityKeyring { cause: String },
    #[error("malformed JWT")]
    MalformedJwt,
    #[error("JWT has expired")]
    ExpiredToken,
    #[error("JWT issuer is not accepted")]
    UnknownIssuer,
    #[error("JWT audience is not accepted")]
    WrongAudience,
    #[error("JWT signature is invalid")]
    SignatureInvalid,
    #[error("JWT subject is invalid")]
    InvalidSubject,
    #[error("JWT is not active")]
    InactiveToken,
    #[error("the local issuer is immutable")]
    LocalIssuerImmutable,
    #[error("a peer JWT cannot target the local machine")]
    LocalPeerTarget,
    #[error("the peer is not currently trusted")]
    PeerNotTrusted,
    #[error("peer permission coordinator is pending")]
    PeerGrantsPending,
    #[error("the peer fingerprint was not confirmed by local policy")]
    FingerprintNotConfirmed,
    #[error("peer pairing rejected: {cause}")]
    PairingRejected { cause: String },
    #[error("pairing invitation is malformed")]
    MalformedInvitation,
    #[error("pairing invitation secret has expired")]
    ExpiredInvitationSecret,
    #[error("pairing invitation secret has already been consumed")]
    ConsumedInvitationSecret,
    #[error("pairing invitation secret is unknown")]
    UnknownInvitationSecret,
    #[error("Auth peer configuration failed: {cause}")]
    PeerConfig { cause: String },
    #[error("Auth request channel send failed for {target_domain}.{operation}: {cause}")]
    ChannelSend {
        target_domain: String,
        operation: String,
        cause: String,
    },
    #[error("Auth response channel receive failed for {target_domain}.{operation}: {cause}")]
    ChannelRecv {
        target_domain: String,
        operation: String,
        cause: String,
    },
    #[error("internal Aves authentication error: {0}")]
    Internal(String),
}

impl From<PeerConfigError> for AvesAuthError {
    fn from(error: PeerConfigError) -> Self {
        Self::PeerConfig {
            cause: error.to_string(),
        }
    }
}

impl From<keyring::Error> for AvesAuthError {
    fn from(error: keyring::Error) -> Self {
        Self::MachineIdentityKeyring {
            cause: error.to_string(),
        }
    }
}

impl rspc::Error for AvesAuthError {
    fn into_procedure_error(self) -> rspc::ProcedureError {
        rspc::ProcedureError::Resolver(rspc::ResolverError::new(self, None::<AvesAuthError>))
    }
}

impl From<DispatchContextError> for AvesAuthError {
    fn from(error: DispatchContextError) -> Self {
        Self::Internal(error.to_string())
    }
}

impl From<serde_json::Error> for AvesAuthError {
    fn from(error: serde_json::Error) -> Self {
        Self::Internal(error.to_string())
    }
}

impl From<GeneratedCallError<AvesAuthError>> for AvesAuthError {
    fn from(error: GeneratedCallError<AvesAuthError>) -> Self {
        match error {
            GeneratedCallError::DomainError(error) => error,
            GeneratedCallError::Authentication(error) => error,
            GeneratedCallError::InputClosed => Self::Internal("Auth request input channel is closed".to_owned()),
            GeneratedCallError::ReplyDropped => Self::Internal("Auth request reply channel was dropped".to_owned()),
            GeneratedCallError::UnexpectedResponse { expected, actual } => {
                Self::Internal(format!("Auth response expected {expected}, got {actual}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ApprovedIssuer, AuthAdmissionRegistry, MachineIssuerId};

    #[test]
    fn admission_updates_publish_a_new_generation() {
        let local: MachineIssuerId = iroh::SecretKey::generate().public().into();
        let remote: MachineIssuerId = iroh::SecretKey::generate().public().into();
        let registry = AuthAdmissionRegistry::new(ApprovedIssuer {
            issuer: local,
            relationships: [(
                crate::auth::RelationshipKey("test".to_owned()),
                crate::auth::PeerGrants::full(),
            )]
            .into_iter()
            .collect(),
        });
        let mut changes = registry.subscribe_generation();
        assert_eq!(*changes.borrow_and_update(), 0);
        registry
            .upsert_remote(ApprovedIssuer {
                issuer: remote.clone(),
                relationships: [(
                    crate::auth::RelationshipKey("test".to_owned()),
                    crate::auth::PeerGrants::full(),
                )]
                .into_iter()
                .collect(),
            })
            .unwrap();
        assert_eq!(*changes.borrow_and_update(), 1);
        registry.remove_remote(&remote).unwrap();
        assert_eq!(*changes.borrow_and_update(), 2);
    }

    #[test]
    fn relationship_union_preserves_overlapping_protocols() {
        use super::{NativeProtocol, OperationGrants, PeerGrants, QualifiedOperation, RelationshipKey};
        let local: MachineIssuerId = iroh::SecretKey::generate().public().into();
        let remote: MachineIssuerId = iroh::SecretKey::generate().public().into();
        let registry = AuthAdmissionRegistry::new(ApprovedIssuer {
            issuer: local,
            relationships: Default::default(),
        });
        let a = PeerGrants {
            protocols: [NativeProtocol::Bus, NativeProtocol::Gossip]
                .into_iter()
                .collect(),
            operations: OperationGrants::Only([QualifiedOperation("a".into())].into_iter().collect()),
        };
        let b = PeerGrants {
            protocols: [NativeProtocol::Bus, NativeProtocol::Blobs]
                .into_iter()
                .collect(),
            operations: OperationGrants::Only([QualifiedOperation("b".into())].into_iter().collect()),
        };
        let mut relationships = [(RelationshipKey("a".into()), a), (RelationshipKey("b".into()), b)]
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>();
        registry
            .upsert_remote(ApprovedIssuer {
                issuer: remote.clone(),
                relationships: relationships.clone(),
            })
            .unwrap();
        assert!(registry.allows_protocol(&remote, NativeProtocol::Gossip));
        assert!(registry.allows_operation(&remote, "a"));
        assert!(registry.allows_operation(&remote, "b"));
        relationships.remove(&RelationshipKey("a".into()));
        registry
            .upsert_remote(ApprovedIssuer {
                issuer: remote.clone(),
                relationships,
            })
            .unwrap();
        assert!(registry.allows_protocol(&remote, NativeProtocol::Bus));
        assert!(!registry.allows_protocol(&remote, NativeProtocol::Gossip));
        assert!(!registry.allows_operation(&remote, "a"));
    }
}
