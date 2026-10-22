use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::bus::BusRpcContext;

use super::jwt::Jwt;
use super::pairing::PairingInvitation;
use super::peer::PeerRecord;
use super::types::{ApprovedIssuer, AuthCapability, AuthProjection, AvesAuthError, MachineIssuerId};

/// Requests handled by the Aves Auth control-plane actor.
pub struct AuthRequest {
    pub payload: AuthRequestPayload,
    pub reply_to: oneshot::Sender<Result<AuthResponse, AvesAuthError>>,
}

/// Public and private Auth operations.
#[aves::bus_requests(
    marker = AuthDomain,
    context = BusRpcContext,
    root_error = AvesAuthError,
    domain_error = AvesAuthError
)]
#[derive(Clone, Debug)]
pub enum AuthRequestPayload {
    /// Issues a non-root token for a caller-selected subject.
    #[privileged(reason = "delegated JWT issuance is internal runtime authority")]
    IssueJwt {
        sub: String,
        cap: Vec<AuthCapability>,
        /// Expiration as an RFC3339 timestamp at the public RPC boundary.
        exp: String,
    },

    /// Issues the runtime root token through the private Auth service.
    #[privileged(reason = "root JWT issuance is internal runtime authority")]
    IssueRootJwt { exp: DateTime<Utc> },

    /// Issues a fixed-short-lifetime token for one already-confirmed peer.
    #[privileged(reason = "peer JWT issuance is internal pairing authority")]
    IssuePeerJwt {
        target: MachineIssuerId,
        audience: uuid::Uuid,
        sub: String,
        cap: Vec<AuthCapability>,
    },

    /// Verifies a token before the generic bus routes its opaque request body.
    #[privileged(reason = "JWT verification is internal bus admission")]
    VerifyJwt {
        jwt: Jwt,
        accepted_issuers: Vec<ApprovedIssuer>,
        audience: uuid::Uuid,
    },

    /// Revokes a delegated token identifier. Unknown or expired identifiers are
    /// intentionally treated as an idempotent success by Auth.
    #[privileged(reason = "delegated JWT revocation is internal runtime authority")]
    RevokeJti { jti: uuid::Uuid },

    #[privileged(reason = "peer pairing changes Auth admission policy")]
    PairPeer { peer: MachineIssuerId },

    #[privileged(reason = "peer revocation changes Auth admission policy")]
    RevokePeer { peer: MachineIssuerId },

    #[privileged(reason = "peer admission state is Auth-owned")]
    ListPeers {},

    #[privileged(reason = "pending pairing candidates are Auth-owned")]
    ListPairingCandidates {},

    #[privileged(reason = "pairing invitation creation is internal Auth authority")]
    CreateInvitation { ttl_seconds: u64 },

    #[privileged(reason = "pairing first contact is internal Auth authority")]
    FirstContact { invitation: PairingInvitation },
}

/// Responses produced by Auth operations.
#[aves::bus_responses]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AuthResponse {
    #[serde(skip)]
    JwtIssued { token: Jwt },
    #[serde(skip)]
    PeerJwtIssued { token: Jwt },
    #[serde(skip)]
    DelegatedJwtIssued { token: Jwt, jti: uuid::Uuid },
    #[serde(skip)]
    Verified { projection: AuthProjection },
    #[serde(skip)]
    Success,
    #[serde(skip)]
    Peers { peers: Vec<PeerRecord> },
    #[serde(skip)]
    PairingCandidates { candidates: Vec<MachineIssuerId> },
    #[serde(skip)]
    Invitation { invitation: PairingInvitation },
    #[serde(skip)]
    FirstContacted { peer: MachineIssuerId },
}
