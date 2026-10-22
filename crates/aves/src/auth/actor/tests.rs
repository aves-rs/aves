use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use chrono::{Duration, Utc};
use keyring_core::mock::Store;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::bus::{BusControlError, EventBus, TransportBindings};
use crate::runtime::{DomainDescriptor, Runtime, RuntimeOptions};

use super::super::actor::{Actor, ActorRequest, ActorResponse};
use super::super::domain::{AuthSudoService, AuthSudoServiceImpl};
use super::super::jwt::verify_jwt;
use super::super::keystore::MachineIdentityKeyring;
use super::super::messages::{
    AuthRequestPayload, AuthResponse, IssueJwtInput, IssueRootJwtInput, RevokeJtiInput, VerifyJwtInput,
};
use super::super::types::{ApprovedIssuer, AuthCapability, AvesAuthError, MachineIssuerId};
use super::*;

fn shared_keyring(namespace: &str, store: Arc<Store>) -> MachineIdentityKeyring {
    MachineIdentityKeyring::from_store(namespace, Some(store)).expect("test keyring should open")
}

#[test]
fn approved_issuer_contains_the_local_machine_issuer() {
    let keyring = MachineIdentityKeyring::new_mock("auth-lifecycle").expect("mock keyring should open");
    let local_issuer = MachineIssuerId::from(keyring.secret_key().public());
    let accepted_issuers = [ApprovedIssuer {
        issuer: local_issuer.clone(),
    }];

    assert_eq!(accepted_issuers.len(), 1);
    assert_eq!(accepted_issuers[0].issuer, local_issuer);
}

#[test]
fn local_issuer_survives_auth_restart_and_keyring_reload() {
    let store = Store::new().expect("mock keyring store should open");
    let first = shared_keyring("auth-reload", store.clone());
    let first_secret = first.secret_key().to_bytes();
    let first_issuer = first.secret_key().public();

    let second = shared_keyring("auth-reload", store);
    let second_secret = second.secret_key().to_bytes();
    let second_issuer = second.secret_key().public();

    assert_eq!(first_secret, second_secret);
    assert_eq!(first_issuer, second_issuer);
}

#[test]
fn each_boot_gets_a_new_audience_and_rejects_prior_audience_tokens() {
    let store = Store::new().expect("mock keyring store should open");
    let first_keyring = shared_keyring("auth-audience", store.clone());
    let first_audience = Uuid::new_v4();
    let second_audience = Uuid::new_v4();
    assert_ne!(first_audience, second_audience);

    let (_first_tx, first_rx) = mpsc::channel(1);
    let mut first_actor = Actor::new(first_rx, first_keyring, first_audience);
    let token = match first_actor.issue_or_verify(AuthRequestPayload::IssueJwt {
        sub: "desktop".to_owned(),
        cap: vec![AuthCapability::AllBus],
        exp: (Utc::now() + Duration::minutes(5)).to_rfc3339(),
    }) {
        Ok(ActorResponse::DelegatedJwt { token, .. }) => token,
        _ => panic!("first boot should issue a token"),
    };

    let second_keyring = shared_keyring("auth-audience", store);
    let second_issuer = MachineIssuerId::from(second_keyring.secret_key().public());
    let error = verify_jwt(&token, &second_issuer, second_audience)
        .expect_err("a token from the prior boot audience must be rejected");
    assert_eq!(error, AvesAuthError::WrongAudience);
}

#[tokio::test]
async fn runtime_rejects_an_empty_keyring_namespace_before_auth_construction() {
    let result = Runtime::<()>::new(
        RuntimeOptions {
            keyring_service_namespace: String::new(),
            construction_inputs: Box::new(()),
            transport_bindings: TransportBindings::new(
                |_, _, _| Box::pin(async { Ok(()) }),
                |_, _, _| Box::pin(async { Ok(()) }),
                Vec::new(),
            ),
            data_root: std::env::temp_dir().join(format!("aves-auth-lifecycle-{}", Uuid::new_v4())),
            remote_operation_installer: None,
        },
        Vec::<DomainDescriptor<EventBus<()>>>::new(),
    )
    .await;

    assert!(matches!(result, Err(BusControlError::EmptyKeyringServiceNamespace)));
}

#[tokio::test]
async fn normal_issue_rejects_reserved_subjects_and_root_issue_fixes_root_subject() {
    let store = Store::new().expect("mock keyring store should open");
    let keyring = shared_keyring("auth-subjects", store.clone());
    let audience = Uuid::new_v4();
    let (actor_sender, actor_receiver) = mpsc::channel(1);
    let actor = Actor::new(actor_receiver, shared_keyring("auth-subjects", store.clone()), audience);
    let actor_task = tokio::spawn(actor.run());
    let (service, mut sudo_requests) = AuthSudoServiceImpl::channel(1);
    let sudo_task = tokio::spawn(async move {
        while let Some(request) = sudo_requests.recv().await {
            let (reply_to, reply) = oneshot::channel();
            actor_sender
                .send(ActorRequest::Issue {
                    payload: request.payload,
                    reply_to,
                })
                .await
                .expect("live auth actor should accept sudo request");
            let response = match reply.await.expect("live auth actor should reply") {
                Ok(ActorResponse::Jwt(token)) => Ok(AuthResponse::JwtIssued { token }),
                Ok(ActorResponse::DelegatedJwt { token, jti }) => Ok(AuthResponse::DelegatedJwtIssued { token, jti }),
                Ok(ActorResponse::Projection(projection)) => Ok(AuthResponse::Verified { projection }),
                Ok(ActorResponse::PeerJwt(token)) => Ok(AuthResponse::PeerJwtIssued { token }),
                Ok(ActorResponse::Success) => Ok(AuthResponse::Success),
                Ok(ActorResponse::Peers(_)) => panic!("unexpected peers response"),
                Ok(ActorResponse::PairingCandidates(_)) => panic!("unexpected pairing candidates response"),
                Ok(ActorResponse::Invitation(_)) => panic!("unexpected invitation response"),
                Ok(ActorResponse::FirstContacted { .. }) => panic!("unexpected first-contact response"),
                Err(error) => Err(error),
            };
            request
                .reply_to
                .send(response)
                .expect("sudo caller should await response");
        }
    });

    let mut actor_for_normal_checks = Actor::new(mpsc::channel(1).1, shared_keyring("auth-subjects", store), audience);

    for subject in [String::new(), "root".to_owned()] {
        let error = actor_for_normal_checks
            .issue_or_verify(AuthRequestPayload::IssueJwt {
                sub: subject,
                cap: vec![AuthCapability::AllBus],
                exp: (Utc::now() + Duration::minutes(5)).to_rfc3339(),
            })
            .expect_err("normal issuance must reject reserved subjects");
        assert_eq!(error, AvesAuthError::InvalidSubject);
    }

    let response = service
        .issue_root_jwt(IssueRootJwtInput {
            exp: Utc::now() + Duration::minutes(5),
        })
        .await
        .expect("sudo root issuance should succeed");
    let token = response.token;
    let verified = service
        .verify_jwt(VerifyJwtInput {
            jwt: token.clone(),
            accepted_issuers: vec![ApprovedIssuer {
                issuer: keyring.secret_key().public().into(),
            }],
            audience,
        })
        .await
        .expect("sudo verification should reach the Auth actor");
    assert_eq!(verified.projection.sub, "root");
    assert_eq!(verified.projection.cap, vec![AuthCapability::AllBus]);
    let local_issuer = MachineIssuerId::from(keyring.secret_key().public());
    let claims = verify_jwt(&token, &local_issuer, audience).expect("root token should verify");
    assert_eq!(claims.subject(), "root");
    drop(service);
    actor_task.abort();
    sudo_task.abort();
}

#[tokio::test]
async fn delegated_issue_returns_jti_and_sudo_revocation_is_idempotent() {
    let store = Store::new().expect("mock keyring store should open");
    let keyring = shared_keyring("auth-delegated", store.clone());
    let local_issuer = MachineIssuerId::from(keyring.secret_key().public());
    let audience = Uuid::new_v4();
    let (actor_sender, actor_receiver) = mpsc::channel(1);
    let actor = Actor::new(actor_receiver, keyring, audience);
    let actor_task = tokio::spawn(actor.run());
    let (service, mut sudo_requests) = AuthSudoServiceImpl::channel(1);
    let sudo_task = tokio::spawn(async move {
        while let Some(request) = sudo_requests.recv().await {
            let (reply_to, reply) = oneshot::channel();
            actor_sender
                .send(ActorRequest::Issue {
                    payload: request.payload,
                    reply_to,
                })
                .await
                .expect("live auth actor should accept sudo request");
            let response = match reply.await.expect("live auth actor should reply") {
                Ok(ActorResponse::Jwt(token)) => Ok(AuthResponse::JwtIssued { token }),
                Ok(ActorResponse::DelegatedJwt { token, jti }) => Ok(AuthResponse::DelegatedJwtIssued { token, jti }),
                Ok(ActorResponse::Projection(projection)) => Ok(AuthResponse::Verified { projection }),
                Ok(ActorResponse::PeerJwt(token)) => Ok(AuthResponse::PeerJwtIssued { token }),
                Ok(ActorResponse::Success) => Ok(AuthResponse::Success),
                Ok(ActorResponse::Peers(_)) => panic!("unexpected peers response"),
                Ok(ActorResponse::PairingCandidates(_)) => panic!("unexpected pairing candidates response"),
                Ok(ActorResponse::Invitation(_)) => panic!("unexpected invitation response"),
                Ok(ActorResponse::FirstContacted { .. }) => panic!("unexpected first-contact response"),
                Err(error) => Err(error),
            };
            let _ = request.reply_to.send(response);
        }
    });

    let issued = service
        .issue_jwt(IssueJwtInput {
            sub: "worker".to_owned(),
            cap: vec![AuthCapability::AllBus],
            exp: (Utc::now() + Duration::minutes(5)).to_rfc3339(),
        })
        .await
        .expect("sudo delegated issuance should succeed");
    let verified = service
        .verify_jwt(VerifyJwtInput {
            jwt: issued.token.clone(),
            accepted_issuers: vec![ApprovedIssuer {
                issuer: local_issuer.clone(),
            }],
            audience,
        })
        .await
        .expect("active delegated token should verify");
    assert_eq!(verified.projection.sub, "worker");

    service
        .revoke_jti(RevokeJtiInput { jti: issued.jti })
        .await
        .expect("delegated revocation should succeed");
    service
        .revoke_jti(RevokeJtiInput { jti: issued.jti })
        .await
        .expect("repeated delegated revocation should be idempotent");
    let error = match service
        .verify_jwt(VerifyJwtInput {
            jwt: issued.token,
            accepted_issuers: vec![ApprovedIssuer { issuer: local_issuer }],
            audience,
        })
        .await
    {
        Ok(_) => panic!("revoked delegated token must fail verification"),
        Err(error) => error,
    };
    assert_eq!(error, AvesAuthError::InactiveToken);

    drop(service);
    actor_task.abort();
    sudo_task.abort();
}

#[tokio::test]
async fn closed_auth_sudo_surface_returns_a_typed_error_without_hanging() {
    let (service, requests) = AuthSudoServiceImpl::channel(1);
    drop(requests);

    let result = match tokio::time::timeout(
        Duration::seconds(1).to_std().expect("positive timeout"),
        service.issue_root_jwt(IssueRootJwtInput {
            exp: Utc::now() + Duration::minutes(5),
        }),
    )
    .await
    .expect("closed Auth must fail promptly")
    {
        Ok(_) => panic!("closed Auth must return a typed transport error"),
        Err(error) => error,
    };

    assert!(matches!(result, AvesAuthError::ChannelSend { .. }));
}

#[test]
fn aves_source_has_no_hard_coded_sluice_namespace_literal() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let needle = ['"', 's', 'l', 'u', 'i', 'c', 'e', '"']
        .iter()
        .collect::<String>();
    let mut rust_files = Vec::new();
    for scope in [root.join("src"), root.join("tests")] {
        collect_rust_files(&scope, &mut rust_files).expect("Aves source should be readable");
    }
    rust_files.sort();

    let offenders = rust_files
        .into_iter()
        .filter_map(|path| {
            let contents = fs::read_to_string(&path).expect("Rust source should be readable");
            contents.contains(&needle).then_some(path)
        })
        .collect::<Vec<_>>();

    assert!(
        offenders.is_empty(),
        "Aves source hard-codes the Sluice namespace: {offenders:?}"
    );
}

fn collect_rust_files(path: &Path, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if path.is_file() {
        if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path.to_owned());
        }
        return Ok(());
    }

    let mut entries = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        collect_rust_files(&entry.path(), files)?;
    }
    Ok(())
}

fn test_actor() -> (Actor, MachineIssuerId, uuid::Uuid) {
    let keyring = MachineIdentityKeyring::new_mock("auth-actor-tests").expect("test keyring should open");
    let issuer = keyring.secret_key().public().into();
    let audience = uuid::Uuid::new_v4();
    let actor = Actor::new(mpsc::channel(1).1, keyring, audience);
    (actor, issuer, audience)
}

fn issuer(issuer: &MachineIssuerId) -> ApprovedIssuer {
    ApprovedIssuer { issuer: issuer.clone() }
}

#[test]
fn delegated_jti_revocation_is_idempotent_and_immediately_blocks_verification() {
    let (mut actor, local_issuer, audience) = test_actor();
    let response = actor
        .issue_or_verify(AuthRequestPayload::IssueJwt {
            sub: "worker".to_owned(),
            cap: vec![AuthCapability::AllBus],
            exp: (Utc::now() + Duration::minutes(5)).to_rfc3339(),
        })
        .expect("delegated issuance should succeed");
    let ActorResponse::DelegatedJwt { token, jti } = response else {
        panic!("delegated issuance should return a token and JTI");
    };

    let verified = actor
        .issue_or_verify(AuthRequestPayload::VerifyJwt {
            jwt: token.clone(),
            accepted_issuers: vec![issuer(&local_issuer)],
            audience,
        })
        .expect("active delegated token should verify");
    assert!(matches!(verified, ActorResponse::Projection(_)));

    assert!(matches!(
        actor.issue_or_verify(AuthRequestPayload::RevokeJti { jti }),
        Ok(ActorResponse::Success)
    ));
    assert_eq!(actor.active_jtis.get(&jti), None);
    assert_eq!(
        actor
            .issue_or_verify(AuthRequestPayload::VerifyJwt {
                jwt: token,
                accepted_issuers: vec![issuer(&local_issuer)],
                audience,
            })
            .expect_err("revoked delegated token must fail closed"),
        AvesAuthError::InactiveToken
    );
    assert!(matches!(
        actor.issue_or_verify(AuthRequestPayload::RevokeJti { jti }),
        Ok(ActorResponse::Success)
    ));
    assert!(matches!(
        actor.issue_or_verify(AuthRequestPayload::RevokeJti {
            jti: uuid::Uuid::new_v4()
        }),
        Ok(ActorResponse::Success)
    ));
}

#[test]
fn pruning_runs_on_delegated_issuance_verification_and_revocation() {
    let (mut actor, local_issuer, audience) = test_actor();
    let expired_on_issue = uuid::Uuid::new_v4();
    actor
        .active_jtis
        .insert(expired_on_issue, Utc::now() - Duration::seconds(1));
    let response = actor
        .issue_or_verify(AuthRequestPayload::IssueJwt {
            sub: "worker".to_owned(),
            cap: vec![AuthCapability::AllBus],
            exp: (Utc::now() + Duration::minutes(5)).to_rfc3339(),
        })
        .expect("delegated issuance should succeed");
    assert!(!actor.active_jtis.contains_key(&expired_on_issue));
    let ActorResponse::DelegatedJwt { token, .. } = response else {
        panic!("delegated issuance should return a token and JTI");
    };

    let expired_on_verify = uuid::Uuid::new_v4();
    actor
        .active_jtis
        .insert(expired_on_verify, Utc::now() - Duration::seconds(1));
    actor
        .issue_or_verify(AuthRequestPayload::VerifyJwt {
            jwt: token,
            accepted_issuers: vec![issuer(&local_issuer)],
            audience,
        })
        .expect("active delegated token should verify");
    assert!(!actor.active_jtis.contains_key(&expired_on_verify));

    let expired_on_revoke = uuid::Uuid::new_v4();
    actor
        .active_jtis
        .insert(expired_on_revoke, Utc::now() - Duration::seconds(1));
    actor
        .issue_or_verify(AuthRequestPayload::RevokeJti {
            jti: uuid::Uuid::new_v4(),
        })
        .expect("unknown revocation should be idempotent success");
    assert!(!actor.active_jtis.contains_key(&expired_on_revoke));
}

#[test]
fn root_jtis_are_live_until_expiry_and_are_not_revoked_by_delegated_cleanup() {
    let (mut actor, local_issuer, audience) = test_actor();
    let response = actor
        .issue_or_verify(AuthRequestPayload::IssueRootJwt {
            exp: Utc::now() + Duration::minutes(5),
        })
        .expect("root issuance should succeed");
    let ActorResponse::Jwt(token) = response else {
        panic!("root issuance should return a token");
    };
    let claims = verify_jwt(&token, &local_issuer, audience).expect("root token should have valid claims");

    actor
        .issue_or_verify(AuthRequestPayload::RevokeJti { jti: claims.jti() })
        .expect("delegated revocation cleanup should be idempotent");
    assert!(matches!(
        actor.issue_or_verify(AuthRequestPayload::VerifyJwt {
            jwt: token,
            accepted_issuers: vec![issuer(&local_issuer)],
            audience,
        }),
        Ok(ActorResponse::Projection(_))
    ));
}

#[test]
fn remote_issuer_verifies_without_a_local_jti_and_unknown_issuer_is_rejected() {
    let (mut actor, _local_issuer, audience) = test_actor();
    let remote_keyring =
        MachineIdentityKeyring::new_mock("remote-auth-actor-tests").expect("remote test keyring should open");
    let remote_issuer = remote_keyring.secret_key().public().into();
    let remote_token = issue_jwt(
        &remote_keyring,
        audience,
        "remote-worker".to_owned(),
        vec![AuthCapability::PullCloudBlobs],
        Utc::now() + Duration::minutes(5),
    )
    .expect("remote token should issue");
    let remote_issuer = issuer(&remote_issuer);

    assert!(matches!(
        actor.issue_or_verify(AuthRequestPayload::VerifyJwt {
            jwt: remote_token.clone(),
            accepted_issuers: vec![remote_issuer.clone()],
            audience,
        }),
        Ok(ActorResponse::Projection(_))
    ));
    assert_eq!(
        actor
            .issue_or_verify(AuthRequestPayload::VerifyJwt {
                jwt: remote_token,
                accepted_issuers: Vec::new(),
                audience,
            })
            .expect_err("unapproved remote issuer must fail"),
        AvesAuthError::UnknownIssuer
    );
}

#[test]
fn peer_tokens_are_short_lived_and_do_not_create_local_jtis() {
    let (mut actor, local_issuer, _audience) = test_actor();
    let target: MachineIssuerId = MachineIdentityKeyring::new_mock("peer-auth-target")
        .expect("target keyring should open")
        .secret_key()
        .public()
        .into();
    actor
        .pairing
        .lock()
        .expect("Auth pairing coordinator lock should not be poisoned")
        .seed_confirmed_peer_for_test(&target);
    let audience = Uuid::new_v4();

    let token = actor
        .issue_or_verify(AuthRequestPayload::IssuePeerJwt {
            target,
            audience,
            sub: "peer-bridge".to_owned(),
            cap: vec![AuthCapability::AllBus],
        })
        .expect("peer token should issue");
    let ActorResponse::PeerJwt(token) = token else {
        panic!("peer issuance should return a peer token");
    };
    let claims = verify_jwt(&token, &local_issuer, audience).expect("peer token should verify for its target audience");

    assert!(claims.expiration() <= Utc::now() + PEER_JWT_LIFETIME + Duration::seconds(1));
    assert!(!actor.active_jtis.contains_key(&claims.jti()));
}

#[test]
fn remote_issuers_can_be_added_and_removed_while_local_remains_immutable() {
    let local = ApprovedIssuer {
        issuer: machine_issuer(),
    };
    let remote = ApprovedIssuer {
        issuer: machine_issuer(),
    };
    let registry = AuthAdmissionRegistry::new(local.clone());

    assert_eq!(registry.current_issuers(), vec![local.clone()]);
    registry
        .upsert_remote(remote.clone())
        .expect("remote issuer should be admitted");
    assert!(registry.current_issuers().contains(&remote));
    registry
        .remove_remote(&remote.issuer)
        .expect("remote issuer should be removed");
    assert_eq!(registry.current_issuers(), vec![local.clone()]);
    assert_eq!(
        registry.remove_remote(&local.issuer),
        Err(AvesAuthError::LocalIssuerImmutable)
    );
}

#[test]
fn reconciliation_replaces_only_remote_issuers() {
    let local = ApprovedIssuer {
        issuer: machine_issuer(),
    };
    let first_remote = ApprovedIssuer {
        issuer: machine_issuer(),
    };
    let second_remote = ApprovedIssuer {
        issuer: machine_issuer(),
    };
    let registry = AuthAdmissionRegistry::new(local.clone());

    registry
        .upsert_remote(first_remote)
        .expect("first remote should be admitted");
    registry
        .replace_remotes(vec![second_remote.clone()])
        .expect("reconciliation should replace the remote projection");

    assert_eq!(registry.current_issuers().len(), 2);
    assert!(registry.current_issuers().contains(&local));
    assert!(registry.current_issuers().contains(&second_remote));
}

fn machine_issuer() -> MachineIssuerId {
    iroh::SecretKey::generate().public().into()
}
