#![allow(unused_crate_dependencies)]
#![cfg(feature = "keyring")]

use aves::{Jwt, OpaqueRequestEnvelope, TransportBindings};
use chrono::{Duration, Utc};

struct TestMarker;

#[derive(Debug, PartialEq, Eq)]
struct TestRequest(u8);

fn test_bindings() -> TransportBindings {
    TransportBindings::new(
        |_, _, shutdown| {
            Box::pin(async move {
                let _ = shutdown.await;
                Ok(())
            })
        },
        |_, _, shutdown| {
            Box::pin(async move {
                let _ = shutdown.await;
                Ok(())
            })
        },
        Vec::new(),
    )
}

#[tokio::test]
async fn malformed_non_empty_auth_is_rejected_before_public_routing() {
    let data_root = tempfile::tempdir().expect("test data root should be created");
    let runtime = aves::Runtime::<()>::new(
        aves::RuntimeOptions {
            keyring_service_namespace: "aves-malformed-auth-test".to_owned(),
            construction_inputs: Box::new(()),
            transport_bindings: test_bindings(),
            data_root: data_root.path().to_path_buf(),
            remote_operation_installer: None,
        },
        Vec::new(),
    )
    .await
    .expect("Auth construction should succeed");

    let (rejection_tx, rejection_rx) = tokio::sync::oneshot::channel();
    runtime
        .request_sender()
        .send(OpaqueRequestEnvelope::new_with_rejection::<TestMarker, _>(
            Jwt::from_opaque("not-a-jwt".to_owned()),
            TestRequest(7),
            move |error| {
                let error = error
                    .downcast::<aves::AvesAuthError>()
                    .expect("admission rejection should preserve the Auth error");
                rejection_tx.send(*error).expect("test receiver is alive");
            },
        ))
        .await
        .expect("bus ingress is alive");

    let error = rejection_rx
        .await
        .expect("rejection should arrive before routing");
    assert_eq!(error, aves::AvesAuthError::MalformedJwt);
    runtime.shutdown().await.expect("runtime should shut down");
}

#[tokio::test]
async fn authenticated_auth_sudo_dispatch_succeeds() {
    let data_root = tempfile::tempdir().expect("test data root should be created");
    let runtime = aves::Runtime::<()>::new(
        aves::RuntimeOptions {
            keyring_service_namespace: "aves-authenticated-context-test".to_owned(),
            construction_inputs: Box::new(()),
            transport_bindings: test_bindings(),
            data_root: data_root.path().to_path_buf(),
            remote_operation_installer: None,
        },
        Vec::new(),
    )
    .await
    .expect("Auth construction should succeed");

    let jwt = runtime
        .rpc_context()
        .jwt()
        .expect("runtime context is authenticated");
    assert!(!jwt.as_str().is_empty());
    let service = runtime
        .service_reader::<dyn aves::AuthSudoService>()
        .get()
        .expect("Auth sudo service should be published");
    let issued = service
        .issue_jwt(aves::messages::IssueJwtInput {
            sub: "authenticated-worker".to_owned(),
            cap: vec![aves::AuthCapability::AllBus],
            exp: (Utc::now() + Duration::minutes(5)).to_rfc3339(),
        })
        .await
        .expect("authenticated Auth sudo dispatch should succeed");
    assert!(!issued.token.as_str().is_empty());
    runtime.shutdown().await.expect("runtime should shut down");
}
