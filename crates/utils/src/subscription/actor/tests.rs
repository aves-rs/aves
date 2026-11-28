use std::sync::Arc;

use aves::bus::BusRpcContext;
use quota_core::Registry;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::{Actor, Observation, SubscriptionError, SubscriptionRequest, SubscriptionRequestPayload, catalog_provider};

#[test]
fn grok_and_cursor_observations_use_xai_branding() {
    assert_eq!(catalog_provider("grok", None), Some("xai"));
    assert_eq!(catalog_provider("cursor", Some("cursor")), Some("xai"));
    assert_eq!(catalog_provider("ollama", Some("ollama-cloud")), Some("ollama-cloud"));
    assert_eq!(catalog_provider("antigravity", None), None);
}

#[tokio::test]
async fn reset_returns_the_live_registry_errors_without_broadcasting() {
    let (_requests, receiver) = mpsc::channel(1);
    let (broadcasts, mut events) = mpsc::channel(1);
    let mut actor = Actor::new(receiver, broadcasts, BusRpcContext::unavailable());
    actor.running = Some(Observation {
        registry: Arc::new(Registry::new(Vec::new())),
        cancel: CancellationToken::new(),
    });

    for provider in ["claude", "codex", "unsupported"] {
        let (reply_to, reply) = oneshot::channel();
        actor
            .handle_message(SubscriptionRequest {
                payload: SubscriptionRequestPayload::UseSavedReset {
                    provider: provider.to_owned(),
                    account: "no-observed-account".to_owned(),
                    request_id: "test-request".to_owned(),
                    kind: None,
                },
                reply_to,
            })
            .await;

        let error = reply.await.unwrap().unwrap_err();
        if provider == "unsupported" {
            assert!(matches!(error, SubscriptionError::ResetUnsupportedProvider));
        } else {
            assert!(matches!(error, SubscriptionError::ResetUnknownAccount));
        }
    }
    assert!(matches!(events.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
}
