use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};
use iroh::PublicKey;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use specta::datatype::reference::Reference;
use specta::{DataType, Generics, Type, TypeCollection};
use thiserror::Error;
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

    pub(crate) fn public_key(&self) -> PublicKey {
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
}

/// Shared admission state with one immutable local issuer and dynamic remote issuers.
#[derive(Clone, Debug)]
pub struct AuthAdmissionRegistry {
    local_issuer: ApprovedIssuer,
    remote_issuers: Arc<RwLock<HashMap<MachineIssuerId, ApprovedIssuer>>>,
}

impl AuthAdmissionRegistry {
    pub fn new(local_issuer: ApprovedIssuer) -> Self {
        Self {
            local_issuer,
            remote_issuers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn upsert_remote(&self, issuer: ApprovedIssuer) -> Result<(), AvesAuthError> {
        if issuer.issuer == self.local_issuer.issuer {
            return Err(AvesAuthError::LocalIssuerImmutable);
        }
        self.remote_issuers
            .write()
            .expect("auth admission registry lock poisoned")
            .insert(issuer.issuer.clone(), issuer);
        Ok(())
    }

    pub fn remove_remote(&self, issuer: &MachineIssuerId) -> Result<(), AvesAuthError> {
        if issuer == &self.local_issuer.issuer {
            return Err(AvesAuthError::LocalIssuerImmutable);
        }
        self.remote_issuers
            .write()
            .expect("auth admission registry lock poisoned")
            .remove(issuer);
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
        *self
            .remote_issuers
            .write()
            .expect("auth admission registry lock poisoned") = remotes;
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
