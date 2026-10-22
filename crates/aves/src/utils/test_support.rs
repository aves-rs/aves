//! Opaque bus responders for this crate's direct actor tests.

use std::any::TypeId;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

use crate::bus::OpaqueRequestEnvelope;

/// A minimal opaque bus responder for direct actor tests.
pub struct MockBus {
    receiver: mpsc::Receiver<OpaqueRequestEnvelope>,
}

impl MockBus {
    /// Creates a throwaway opaque request channel and its receiver wrapper.
    pub fn new(buffer: usize) -> (mpsc::Sender<OpaqueRequestEnvelope>, Self) {
        let (sender, receiver) = mpsc::channel(buffer);
        (sender, Self { receiver })
    }

    /// Receives the next opaque request, or `None` after the sender closes.
    pub async fn recv_request(&mut self) -> Option<OpaqueRequestEnvelope> {
        self.receiver.recv().await
    }

    /// Receives, validates, and downcasts the next typed request.
    pub async fn expect_request<Marker, Request>(&mut self, wait: Duration) -> Request
    where
        Marker: 'static,
        Request: Send + 'static,
    {
        let request = timeout(wait, self.recv_request())
            .await
            .expect("timed out waiting for bus request")
            .expect("bus requester closed before sending");
        assert_eq!(
            request.test_target(),
            TypeId::of::<Marker>(),
            "request envelope target did not match expected marker"
        );
        request
            .test_downcast::<Request>()
            .unwrap_or_else(|_| panic!("request envelope body did not match expected request type"))
    }

    /// Asserts that no request arrives within `wait`.
    pub async fn assert_no_request(&mut self, wait: Duration) {
        match timeout(wait, self.recv_request()).await {
            Ok(Some(_)) => panic!("expected no bus request, but received an opaque request"),
            Ok(None) | Err(_) => {}
        }
    }
}

/// Sends a typed reply through a request's oneshot channel.
pub fn reply_with<T, E>(reply_to: oneshot::Sender<Result<T, E>>, result: Result<T, E>) {
    let _ = reply_to.send(result);
}

#[cfg(test)]
mod tests {
    use crate::auth::Jwt;
    use crate::config::{ConfigDomain, ConfigError, ConfigRequest, ConfigRequestPayload, ConfigResponse};

    use super::*;

    struct ActionDomain;

    #[tokio::test]
    async fn recv_request_yields_typed_request() {
        let (sender, mut mock) = MockBus::new(1);
        let (reply_to, _receiver) = oneshot::channel();
        sender
            .send(OpaqueRequestEnvelope::new::<ConfigDomain, _>(
                Jwt::from_opaque("test-jwt".to_owned()),
                ConfigRequest {
                    payload: ConfigRequestPayload::GetConfigSection {
                        section: "mock".to_owned(),
                    },
                    reply_to,
                },
            ))
            .await
            .expect("send");

        let request = mock
            .expect_request::<ConfigDomain, ConfigRequest>(Duration::from_millis(100))
            .await;
        assert!(matches!(request.payload, ConfigRequestPayload::GetConfigSection { section } if section == "mock"));
    }

    #[tokio::test]
    async fn recv_request_returns_none_when_sender_dropped() {
        let (sender, mut mock) = MockBus::new(1);
        drop(sender);
        assert!(mock.recv_request().await.is_none());
    }

    #[tokio::test]
    #[should_panic(expected = "request envelope target did not match expected marker")]
    async fn expect_request_panics_on_mismatch() {
        let (sender, mut mock) = MockBus::new(1);
        let (reply_to, _receiver) = oneshot::channel();
        sender
            .send(OpaqueRequestEnvelope::new::<ConfigDomain, _>(
                Jwt::from_opaque("test-jwt".to_owned()),
                ConfigRequest {
                    payload: ConfigRequestPayload::GetConfigSection {
                        section: "mock".to_owned(),
                    },
                    reply_to,
                },
            ))
            .await
            .expect("send");
        mock.expect_request::<ActionDomain, ConfigRequest>(Duration::from_millis(100))
            .await;
    }

    #[tokio::test]
    #[should_panic(expected = "timed out waiting for bus request")]
    async fn expect_request_panics_on_timeout() {
        let (_sender, mut mock) = MockBus::new(1);
        mock.expect_request::<ConfigDomain, ConfigRequest>(Duration::from_millis(50))
            .await;
    }

    #[tokio::test]
    #[should_panic(expected = "bus requester closed before sending")]
    async fn expect_request_panics_when_sender_closes() {
        let (sender, mut mock) = MockBus::new(1);
        drop(sender);
        mock.expect_request::<ConfigDomain, ConfigRequest>(Duration::from_millis(100))
            .await;
    }

    #[tokio::test]
    #[should_panic(expected = "request envelope body did not match expected request type")]
    async fn expect_request_panics_on_body_mismatch() {
        let (sender, mut mock) = MockBus::new(1);
        let (reply_to, _receiver) = oneshot::channel();
        sender
            .send(OpaqueRequestEnvelope::new::<ConfigDomain, _>(
                Jwt::from_opaque("test-jwt".to_owned()),
                ConfigRequest {
                    payload: ConfigRequestPayload::GetConfigSection {
                        section: "mock".to_owned(),
                    },
                    reply_to,
                },
            ))
            .await
            .expect("send");
        mock.expect_request::<ConfigDomain, ConfigError>(Duration::from_millis(100))
            .await;
    }

    #[tokio::test]
    async fn assert_no_request_passes_when_idle() {
        let (_sender, mut mock) = MockBus::new(1);
        mock.assert_no_request(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn assert_no_request_passes_when_sender_dropped() {
        let (sender, mut mock) = MockBus::new(1);
        drop(sender);
        mock.assert_no_request(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    #[should_panic(expected = "expected no bus request")]
    async fn assert_no_request_panics_when_request_arrives() {
        let (sender, mut mock) = MockBus::new(1);
        let (reply_to, _receiver) = oneshot::channel();
        sender
            .send(OpaqueRequestEnvelope::new::<ConfigDomain, _>(
                Jwt::from_opaque("test-jwt".to_owned()),
                ConfigRequest {
                    payload: ConfigRequestPayload::GetConfigSection {
                        section: "mock".to_owned(),
                    },
                    reply_to,
                },
            ))
            .await
            .expect("send");
        mock.assert_no_request(Duration::from_millis(200)).await;
    }

    #[tokio::test]
    async fn reply_with_delivers_result_to_receiver() {
        let (reply_to, receiver) = oneshot::channel::<Result<ConfigResponse, ConfigError>>();
        reply_with(
            reply_to,
            Ok(ConfigResponse::ConfigSection {
                value: serde_json::json!({"enabled": true}),
            }),
        );
        assert!(matches!(
            receiver.await.expect("reply received"),
            Ok(ConfigResponse::ConfigSection { value }) if value == serde_json::json!({"enabled": true})
        ));
    }

    #[tokio::test]
    async fn reply_with_swallows_send_when_receiver_dropped() {
        let (reply_to, receiver) = oneshot::channel::<Result<ConfigResponse, ConfigError>>();
        drop(receiver);
        reply_with(reply_to, Ok(ConfigResponse::Registered));
    }

    #[tokio::test]
    async fn reply_with_delivers_error_result() {
        let (reply_to, receiver) = oneshot::channel::<Result<ConfigResponse, ConfigError>>();
        reply_with(reply_to, Err(ConfigError::InvalidSection("mock".to_owned())));
        assert!(matches!(
            receiver.await.expect("reply received"),
            Err(ConfigError::InvalidSection(section)) if section == "mock"
        ));
    }

    #[tokio::test]
    async fn full_request_response_roundtrip() {
        let (sender, mut mock) = MockBus::new(1);
        let (reply_to, receiver) = oneshot::channel();
        sender
            .send(OpaqueRequestEnvelope::new::<ConfigDomain, _>(
                Jwt::from_opaque("test-jwt".to_owned()),
                ConfigRequest {
                    payload: ConfigRequestPayload::GetConfigSection {
                        section: "mock".to_owned(),
                    },
                    reply_to,
                },
            ))
            .await
            .expect("send");

        let request = mock
            .expect_request::<ConfigDomain, ConfigRequest>(Duration::from_millis(100))
            .await;
        assert!(matches!(request.payload, ConfigRequestPayload::GetConfigSection { section } if section == "mock"));
        reply_with(
            request.reply_to,
            Ok(ConfigResponse::ConfigSection {
                value: serde_json::json!({"enabled": true}),
            }),
        );

        assert!(matches!(
            receiver.await.expect("reply received"),
            Ok(ConfigResponse::ConfigSection { value }) if value == serde_json::json!({"enabled": true})
        ));
    }
}
