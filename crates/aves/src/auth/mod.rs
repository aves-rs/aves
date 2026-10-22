mod actor;
mod domain;
mod handle;
mod jwt;
mod keystore;
mod pairing;
mod peer;

pub(crate) mod types;

pub mod messages;

pub(crate) use domain::AuthDomain;
pub(crate) use jwt::issuer_from_jwt;
pub(crate) use pairing::AUTH_ALPN;
pub(crate) use pairing::{AuthFirstContactHandler, PairingCoordinator, PairingOwner};

pub use domain::{AuthBootOptions, AuthSudoService};
pub use handle::ConstructionResult;
pub use jwt::Jwt;
pub use pairing::PairingInvitation;
pub use peer::{PeerEndpoint, PeerRecord, PeerTrustState};
pub use types::{
    ApprovedIssuer, AuthAdmissionRegistry, AuthCapability, AuthProjection, AvesAuthError, MachineIssuerId,
};
