use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use super::jwt::{Jwt, issue_jwt, issue_root_jwt, issuer_from_jwt, verify_jwt};
use super::keystore::MachineIdentityKeyring;
use super::messages::AuthRequestPayload;
use super::pairing::{PairingCoordinator, PairingInvitation, PairingOwner};
use super::peer::{PeerRecord, PeerTrustState};
use super::types::{
    ApprovedIssuer, AuthAdmissionRegistry, AuthIssueRequest, AuthProjection, AvesAuthError, MachineIssuerId,
    PeerJwtIssueRequest,
};

const PEER_JWT_LIFETIME: chrono::Duration = chrono::Duration::minutes(5);

pub(crate) enum ActorRequest {
    Issue {
        payload: AuthRequestPayload,
        reply_to: oneshot::Sender<Result<ActorResponse, AvesAuthError>>,
    },
}

#[derive(Debug)]
pub(crate) enum ActorResponse {
    Jwt(Jwt),
    PeerJwt(Jwt),
    DelegatedJwt {
        token: Jwt,
        jti: Uuid,
    },
    Projection(AuthProjection),
    Success,
    Peers(Vec<PeerRecord>),
    PairingCandidates(Vec<MachineIssuerId>),
    Invitation(PairingInvitation),
    #[cfg_attr(test, allow(dead_code))]
    FirstContacted {
        peer: MachineIssuerId,
    },
}

/// Owns the machine identity and all live JTI state.
pub(crate) struct Actor {
    receiver: mpsc::Receiver<ActorRequest>,
    keyring: MachineIdentityKeyring,
    audience: Uuid,
    active_jtis: HashMap<Uuid, DateTime<Utc>>,
    root_jtis: HashMap<Uuid, DateTime<Utc>>,
    pairing: PairingOwner,
}

impl Actor {
    pub(crate) fn with_registry(
        receiver: mpsc::Receiver<ActorRequest>,
        keyring: MachineIdentityKeyring,
        audience: Uuid,
        admission_registry: AuthAdmissionRegistry,
        peer_config_path: &Path,
    ) -> Result<Self, AvesAuthError> {
        let local_issuer = keyring.secret_key().public().into();
        let pairing = PairingCoordinator::load(peer_config_path, admission_registry.clone(), local_issuer, audience)?;
        admission_registry.replace_remotes(
            pairing
                .peers()
                .into_iter()
                .filter(|record| record.state == PeerTrustState::Confirmed)
                .map(|record| ApprovedIssuer {
                    issuer: record.peer.into(),
                })
                .collect(),
        )?;
        Ok(Self {
            receiver,
            keyring,
            audience,
            active_jtis: HashMap::new(),
            root_jtis: HashMap::new(),
            pairing: Arc::new(Mutex::new(pairing)),
        })
    }

    #[cfg(test)]
    pub(crate) fn new(receiver: mpsc::Receiver<ActorRequest>, keyring: MachineIdentityKeyring, audience: Uuid) -> Self {
        let issuer: MachineIssuerId = keyring.secret_key().public().into();
        let admission_registry = AuthAdmissionRegistry::new(ApprovedIssuer { issuer: issuer.clone() });
        Self {
            receiver,
            keyring,
            audience,
            active_jtis: HashMap::new(),
            root_jtis: HashMap::new(),
            pairing: Arc::new(Mutex::new(PairingCoordinator::ephemeral(
                admission_registry.clone(),
                issuer,
                audience,
            ))),
        }
    }

    pub(crate) fn pairing_owner(&self) -> PairingOwner {
        self.pairing.clone()
    }

    pub(crate) async fn run(mut self) {
        while let Some(request) = self.receiver.recv().await {
            let ActorRequest::Issue { payload, reply_to } = request;
            #[cfg(test)]
            let result = self.issue_or_verify(payload);
            #[cfg(not(test))]
            let result = self.issue_or_verify(payload).await;
            let _ = reply_to.send(result);
        }
    }

    pub(super) fn issue_root(&mut self, expires_at: DateTime<Utc>) -> Result<Jwt, AvesAuthError> {
        self.prune_expired();
        let token = issue_root_jwt(&self.keyring, self.audience, expires_at)?;
        let claims = verify_jwt(&token, &self.local_issuer(), self.audience)?;
        self.root_jtis.insert(claims.jti(), claims.expiration());
        Ok(token)
    }

    #[cfg(test)]
    pub(super) fn issue_or_verify(&mut self, payload: AuthRequestPayload) -> Result<ActorResponse, AvesAuthError> {
        self.issue_or_verify_inner(payload)
    }

    #[cfg(not(test))]
    async fn issue_or_verify(&mut self, payload: AuthRequestPayload) -> Result<ActorResponse, AvesAuthError> {
        self.issue_or_verify_inner(payload).await
    }

    fn issue_or_verify_sync(&mut self, payload: AuthRequestPayload) -> Result<ActorResponse, AvesAuthError> {
        match payload {
            AuthRequestPayload::IssueJwt { sub, cap, exp } => {
                let exp = DateTime::parse_from_rfc3339(&exp)
                    .map(|value| value.with_timezone(&Utc))
                    .map_err(|_| AvesAuthError::MalformedJwt)?;
                self.issue(AuthIssueRequest { sub, cap, exp })
            }
            AuthRequestPayload::IssueRootJwt { exp } => self.issue_root(exp).map(ActorResponse::Jwt),
            AuthRequestPayload::IssuePeerJwt {
                target,
                audience,
                sub,
                cap,
            } => self.issue_peer(PeerJwtIssueRequest {
                target,
                audience,
                sub,
                cap,
            }),
            AuthRequestPayload::VerifyJwt {
                jwt,
                accepted_issuers,
                audience,
            } => self.verify(jwt, accepted_issuers, audience),
            AuthRequestPayload::RevokeJti { jti } => {
                self.prune_expired();
                self.active_jtis.remove(&jti);
                Ok(ActorResponse::Success)
            }
            AuthRequestPayload::PairPeer { peer } => {
                self.pairing
                    .lock()
                    .expect("Auth pairing coordinator lock poisoned")
                    .confirm(peer)?;
                Ok(ActorResponse::Success)
            }
            AuthRequestPayload::RevokePeer { peer } => {
                self.pairing
                    .lock()
                    .expect("Auth pairing coordinator lock poisoned")
                    .revoke(peer)?;
                Ok(ActorResponse::Success)
            }
            AuthRequestPayload::ListPeers {} => Ok(ActorResponse::Peers(
                self.pairing
                    .lock()
                    .expect("Auth pairing coordinator lock poisoned")
                    .peers(),
            )),
            AuthRequestPayload::ListPairingCandidates {} => Ok(ActorResponse::PairingCandidates(
                self.pairing
                    .lock()
                    .expect("Auth pairing coordinator lock poisoned")
                    .pending_candidates(),
            )),
            AuthRequestPayload::CreateInvitation { ttl_seconds } => Ok(ActorResponse::Invitation(
                self.pairing
                    .lock()
                    .expect("Auth pairing coordinator lock poisoned")
                    .new_invitation(ttl_seconds)?,
            )),
            AuthRequestPayload::FirstContact { .. } => unreachable!("handled by async actor path"),
        }
    }

    #[cfg(test)]
    fn issue_or_verify_inner(&mut self, payload: AuthRequestPayload) -> Result<ActorResponse, AvesAuthError> {
        match payload {
            AuthRequestPayload::FirstContact { .. } => Err(AvesAuthError::Internal(
                "first contact requires an async actor boundary".to_owned(),
            )),
            payload => self.issue_or_verify_sync(payload),
        }
    }

    #[cfg(not(test))]
    async fn issue_or_verify_inner(&mut self, payload: AuthRequestPayload) -> Result<ActorResponse, AvesAuthError> {
        if let AuthRequestPayload::FirstContact { invitation } = payload {
            return Ok(ActorResponse::FirstContacted {
                peer: PairingCoordinator::outbound_first_contact(&self.pairing, &invitation).await?,
            });
        }
        self.issue_or_verify_sync(payload)
    }

    fn issue(&mut self, request: AuthIssueRequest) -> Result<ActorResponse, AvesAuthError> {
        self.prune_expired();
        if request.sub.is_empty() || request.sub == "root" {
            return Err(AvesAuthError::InvalidSubject);
        }
        let token = issue_jwt(&self.keyring, self.audience, request.sub, request.cap, request.exp)?;
        let claims = verify_jwt(&token, &self.local_issuer(), self.audience)?;
        self.active_jtis.insert(claims.jti(), claims.expiration());
        Ok(ActorResponse::DelegatedJwt {
            token,
            jti: claims.jti(),
        })
    }

    fn issue_peer(&mut self, request: PeerJwtIssueRequest) -> Result<ActorResponse, AvesAuthError> {
        self.prune_expired();
        if request.target == self.local_issuer() {
            return Err(AvesAuthError::LocalPeerTarget);
        }
        let is_confirmed = self
            .pairing
            .lock()
            .expect("Auth pairing coordinator lock poisoned")
            .is_confirmed(&request.target);
        if !is_confirmed {
            return Err(AvesAuthError::PeerNotTrusted);
        }
        if request.sub.is_empty() || request.sub == "root" {
            return Err(AvesAuthError::InvalidSubject);
        }
        let token = issue_jwt(
            &self.keyring,
            request.audience,
            request.sub,
            request.cap,
            Utc::now() + PEER_JWT_LIFETIME,
        )?;
        Ok(ActorResponse::PeerJwt(token))
    }

    fn verify(
        &mut self,
        jwt: Jwt,
        accepted_issuers: Vec<ApprovedIssuer>,
        audience: Uuid,
    ) -> Result<ActorResponse, AvesAuthError> {
        self.prune_expired();
        let issuer = issuer_from_jwt(&jwt)?;
        let approved = accepted_issuers
            .iter()
            .find(|approved| approved.issuer == issuer)
            .ok_or(AvesAuthError::UnknownIssuer)?;
        let claims = verify_jwt(&jwt, &approved.issuer, audience)?;
        let local_issuer = self.local_issuer();
        if issuer == local_issuer {
            let now = SystemTime::now();
            let Some(expires_at) = self
                .active_jtis
                .get(&claims.jti())
                .or_else(|| self.root_jtis.get(&claims.jti()))
                .copied()
            else {
                return Err(AvesAuthError::InactiveToken);
            };
            if expires_at <= DateTime::<Utc>::from(now) {
                self.active_jtis.remove(&claims.jti());
                self.root_jtis.remove(&claims.jti());
                return Err(AvesAuthError::ExpiredToken);
            }
        }
        Ok(ActorResponse::Projection(AuthProjection::new(
            issuer,
            claims.subject(),
            claims.capabilities(),
        )))
    }

    fn prune_expired(&mut self) {
        let now = Utc::now();
        self.active_jtis.retain(|_, expires_at| *expires_at > now);
        self.root_jtis.retain(|_, expires_at| *expires_at > now);
    }

    fn local_issuer(&self) -> MachineIssuerId {
        self.keyring.secret_key().public().into()
    }
}

#[cfg(test)]
mod tests;
