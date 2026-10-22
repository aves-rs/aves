use std::path::Path;
use std::sync::Arc;

use chrono::{Duration, Utc};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::actor::{Actor, ActorRequest, ActorResponse};
use super::domain::{AuthSudoService, AuthSudoServiceImpl};
use super::jwt::Jwt;
use super::keystore::MachineIdentityKeyring;
use super::messages::{AuthRequest, AuthResponse};
use super::pairing::{AuthFirstContactHandler, PairingOwner};
use super::types::{ApprovedIssuer, AuthAdmissionRegistry, AvesAuthError};

pub struct ConstructionResult {
    pub root_jwt: Jwt,
    pub service: Arc<dyn AuthSudoService>,
    pub actor_task: JoinHandle<()>,
    pub actor_sender: mpsc::Sender<AuthRequest>,
    pub(crate) machine_secret_key: iroh::SecretKey,
    pub(crate) admission_registry: AuthAdmissionRegistry,
    pub(crate) first_contact_handler: AuthFirstContactHandler,
    pub(crate) pairing_owner: PairingOwner,
}

pub fn construct(
    namespace: &str,
    audience: Uuid,
    peer_config_path: &Path,
) -> Result<(ConstructionResult, mpsc::Receiver<AuthRequest>), AvesAuthError> {
    let keyring = MachineIdentityKeyring::new(namespace)?;
    let machine_secret_key = keyring.secret_key().clone();

    let (actor_sender, receiver) = mpsc::channel(64);
    let (auth_requests, mut auth_receiver) = mpsc::channel::<AuthRequest>(64);
    let (service, sudo_requests) = AuthSudoServiceImpl::channel(64);

    let local_issuer = ApprovedIssuer {
        issuer: machine_secret_key.public().into(),
    };
    let admission_registry = AuthAdmissionRegistry::new(local_issuer);
    let mut actor = Actor::with_registry(
        receiver,
        keyring,
        audience,
        admission_registry.clone(),
        peer_config_path,
    )?;
    let root_jwt = actor.issue_root(Utc::now() + Duration::days(365))?;
    let pairing_owner = actor.pairing_owner();
    let first_contact_handler = AuthFirstContactHandler::new(pairing_owner.clone());
    let actor_task = tokio::spawn(actor.run());

    tokio::spawn(async move {
        while let Some(request) = auth_receiver.recv().await {
            let (actor_reply, actor_response) = oneshot::channel();

            if actor_sender
                .send(ActorRequest::Issue {
                    payload: request.payload,
                    reply_to: actor_reply,
                })
                .await
                .is_err()
            {
                let _ = request.reply_to.send(Err(AvesAuthError::Internal(
                    "Auth actor request channel is closed".to_owned(),
                )));
                break;
            }

            let response = actor_response
                .await
                .map_err(|_| AvesAuthError::Internal("Auth actor response channel is closed".to_owned()))
                .and_then(|result| result.map(AuthResponse::from));

            let _ = request.reply_to.send(response);
        }
    });

    Ok((
        ConstructionResult {
            machine_secret_key,
            admission_registry,
            root_jwt,
            service: service as Arc<dyn AuthSudoService>,
            actor_task,
            actor_sender: auth_requests,
            first_contact_handler,
            pairing_owner,
        },
        sudo_requests,
    ))
}

impl From<ActorResponse> for AuthResponse {
    fn from(response: ActorResponse) -> Self {
        match response {
            ActorResponse::Jwt(token) => Self::JwtIssued { token },
            ActorResponse::PeerJwt(token) => Self::PeerJwtIssued { token },
            ActorResponse::DelegatedJwt { token, jti } => Self::DelegatedJwtIssued { token, jti },
            ActorResponse::Projection(projection) => Self::Verified { projection },
            ActorResponse::Success => Self::Success,
            ActorResponse::Peers(peers) => Self::Peers { peers },
            ActorResponse::PairingCandidates(candidates) => Self::PairingCandidates { candidates },
            ActorResponse::Invitation(invitation) => Self::Invitation { invitation },
            ActorResponse::FirstContacted { peer } => Self::FirstContacted { peer },
        }
    }
}
