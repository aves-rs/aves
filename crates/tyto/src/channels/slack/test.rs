use slack_morphism::prelude::{SlackEventId, SlackTeamId};

use crate::channels::InboundEvent;
use crate::messages::ListenerType;
use crate::types::{MessagingError, SlackConfig};

use super::connection::test_prepare as prepare;
use super::subscribe::{run_filters, test_handle_message_event as handle_message_event};

mod connection {
    use super::*;

    #[test]
    fn configure_rejects_missing_settings_and_wrong_credential_kind() {
        use crate::types::{MessagingConfig, MessagingCredentials};
        use zeroize::Zeroizing;

        let slack = MessagingCredentials::Slack {
            app_token: Zeroizing::new("xapp-test".to_owned()),
            bot_token: Zeroizing::new("xoxb-test".to_owned()),
        };
        assert!(matches!(
            super::super::connection::configure(&MessagingConfig::default(), &slack),
            Err(MessagingError::ChannelConnectionFailed {
                listener_type: ListenerType::Slack,
                ..
            })
        ));
        let settings = MessagingConfig {
            slack: Some(SlackConfig::default()),
            ..Default::default()
        };
        let telegram = MessagingCredentials::Telegram {
            token: Zeroizing::new("123:test".to_owned()),
        };
        assert!(matches!(
            super::super::connection::configure(&settings, &telegram),
            Err(MessagingError::ChannelConnectionFailed {
                listener_type: ListenerType::Slack,
                ..
            })
        ));
    }

    #[test]
    fn preparation_rejects_missing_app_token() {
        let config = SlackConfig {
            app_token: None,
            bot_token: Some("xoxb-test".to_owned().into()),
            ..Default::default()
        };
        assert!(
            matches!(prepare(&config), Err(MessagingError::ChannelConnectionFailed { listener_type: ListenerType::Slack, message }) if message.contains("app token is missing"))
        );
    }

    #[test]
    fn preparation_rejects_missing_bot_token() {
        let config = SlackConfig {
            app_token: Some("xapp-test".to_owned().into()),
            bot_token: None,
            ..Default::default()
        };
        assert!(
            matches!(prepare(&config), Err(MessagingError::ChannelConnectionFailed { listener_type: ListenerType::Slack, message }) if message.contains("bot token is missing"))
        );
    }

    #[test]
    fn preparation_checks_app_token_first_when_both_are_missing() {
        assert!(
            matches!(prepare(&SlackConfig::default()), Err(MessagingError::ChannelConnectionFailed { listener_type: ListenerType::Slack, message }) if message.contains("app token is missing"))
        );
    }
}

mod filter {
    use serde_json::json;
    use tokio::sync::mpsc;

    use crate::channels::MessagingSubscription;
    use crate::types::NormalizedMessage;

    use super::super::filter::SlackFilter;
    use super::run_filters;

    fn message(channel: &str, sender: Option<&str>) -> NormalizedMessage {
        NormalizedMessage {
            body: "hello".to_owned(),
            metadata: json!({ "channel_id": channel, "user_id": sender }),
        }
    }

    #[test]
    fn sender_allowlist_requires_matching_identity_on_the_selected_channel() {
        let policy = SlackFilter::new(Some(vec!["C123".to_owned()])).with_senders(vec!["U1".to_owned()]);
        assert!(policy.accepts_message(&message("C123", Some("U1"))));
        assert!(!policy.accepts_message(&message("C123", Some("U2"))));
        assert!(!policy.accepts_message(&message("C123", None)));
        assert!(!policy.accepts_message(&message("C999", Some("U1"))));
    }

    #[tokio::test]
    async fn filter_task_delivers_only_matching_messages_without_slack() {
        let (input_tx, input_rx) = mpsc::channel(4);
        let (output_tx, mut output_rx) = mpsc::channel(4);
        let task = run_filters(
            MessagingSubscription {
                subscribed_channels: Some(vec!["C123".to_owned()]),
                allowed_senders: vec!["U1".to_owned()],
            },
            input_rx,
            output_tx,
        )
        .unwrap();

        input_tx.send(message("C123", Some("U1"))).await.unwrap();
        input_tx.send(message("C123", None)).await.unwrap();
        input_tx.send(message("C999", Some("U1"))).await.unwrap();
        drop(input_tx);
        task.await.unwrap().unwrap();

        assert_eq!(output_rx.try_recv().unwrap().body, "hello");
        assert!(output_rx.try_recv().is_err());
    }
}

mod helpers {
    use std::time::Duration;

    use serde_json::json;
    use slack_morphism::blocks::SlackBlock;
    use slack_morphism::errors::{
        SlackClientApiError, SlackClientEndOfStreamError, SlackClientError, SlackRateLimitError,
    };
    use slack_morphism::prelude::{
        SlackChannelId, SlackEventId, SlackMessageContent, SlackMessageEvent, SlackMessageOrigin, SlackMessageSender,
        SlackTeamId, SlackTs,
    };
    use uuid::Uuid;

    use aves::utils::uuid::SpectaUuid;

    use crate::types::{InteractionAction, MessagingSendError, MessagingSendErrorCode, NormalizedMessage};

    use super::super::helpers::{
        classify_send_error, interaction_to_message, interactive_blocks, normalize_update, post_message_request,
        test_classify_http_error,
    };

    fn api_error(code: &str) -> SlackClientError {
        SlackClientError::ApiError(SlackClientApiError {
            code: code.to_owned(),
            errors: None,
            warnings: None,
            http_response_body: None,
        })
    }

    fn callback_event(value: &str) -> slack_morphism::events::SlackInteractionEvent {
        serde_json::from_value(json!({
            "type": "block_actions",
            "team": { "id": "T1" },
            "api_app_id": "A1",
            "container": { "type": "message", "message_ts": "1234567890.000001" },
            "trigger_id": "trigger",
            "actions": [{ "type": "button", "action_id": "permission_approve", "value": value }]
        }))
        .unwrap()
    }

    #[test]
    fn block_action_payload_maps_to_typed_permission_callback() {
        let request_id = SpectaUuid(Uuid::nil());
        let payload = crate::types::PermissionButtonPayload::encode(request_id, InteractionAction::Approve);
        let interaction = interaction_to_message(&callback_event(&payload)).unwrap();
        assert_eq!(interaction.request_id, request_id);
        assert_eq!(interaction.action, InteractionAction::Approve);
        assert!(interaction.raw_payload.contains("permission_approve"));
        assert!(interaction_to_message(&callback_event("unknown|not-an-id")).is_none());
    }

    #[tokio::test]
    async fn callback_is_delivered_to_the_actor_queue_without_slack() {
        let request_id = SpectaUuid(Uuid::nil());
        let payload = crate::types::PermissionButtonPayload::encode(request_id, InteractionAction::Approve);
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        super::super::subscribe::test_deliver_interaction(&callback_event(&payload), tx.clone()).await;
        let delivered = rx.try_recv().unwrap();
        assert_eq!(delivered.request_id, request_id);
        assert_eq!(delivered.action, InteractionAction::Approve);

        super::super::subscribe::test_deliver_interaction(&callback_event("invalid"), tx).await;
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn api_errors_have_terminal_or_retryable_categories() {
        let cases = [
            ("invalid_auth", MessagingSendErrorCode::InvalidCredentials),
            ("channel_not_found", MessagingSendErrorCode::InvalidDestination),
            ("missing_scope", MessagingSendErrorCode::PermissionDenied),
            ("other", MessagingSendErrorCode::Validation),
        ];
        for (api_code, category) in cases {
            assert!(matches!(
                classify_send_error(api_error(api_code)),
                MessagingSendError::Terminal { code, .. } if code == category
            ));
        }
        assert!(matches!(
            classify_send_error(api_error("ratelimited")),
            MessagingSendError::Retryable {
                code: MessagingSendErrorCode::RateLimited,
                ..
            }
        ));
    }

    #[test]
    fn http_statuses_preserve_retryable_and_terminal_error_categories() {
        assert!(matches!(
            test_classify_http_error(503),
            MessagingSendError::Retryable {
                code: MessagingSendErrorCode::Server,
                ..
            }
        ));
        assert!(matches!(
            test_classify_http_error(429),
            MessagingSendError::Retryable {
                code: MessagingSendErrorCode::RateLimited,
                ..
            }
        ));
        assert!(matches!(
            test_classify_http_error(400),
            MessagingSendError::Terminal {
                code: MessagingSendErrorCode::Validation,
                ..
            }
        ));
    }

    #[test]
    fn rate_limit_retry_after_rounds_up_to_whole_seconds() {
        let error = SlackClientError::RateLimitError(SlackRateLimitError {
            retry_after: Some(Duration::from_millis(1_500)),
            code: None,
            warnings: None,
            http_response_body: None,
        });
        assert!(matches!(
            classify_send_error(error),
            MessagingSendError::Retryable {
                code: MessagingSendErrorCode::RateLimited,
                retry_after_seconds: Some(2),
                ..
            }
        ));
    }

    #[test]
    fn closed_slack_stream_is_a_retryable_transport_error() {
        assert!(matches!(
            classify_send_error(SlackClientError::EndOfStream(SlackClientEndOfStreamError {})),
            MessagingSendError::Retryable {
                code: MessagingSendErrorCode::Transport,
                ..
            }
        ));
    }

    #[test]
    fn normalized_message_preserves_text_and_slack_routing_metadata() {
        let channel = SlackChannelId::new("C123".to_owned());
        let origin =
            SlackMessageOrigin::new(SlackTs::new("1234567890.000001".to_owned())).with_channel(channel.clone());
        let event = SlackMessageEvent::new(origin, SlackMessageSender::new())
            .with_content(SlackMessageContent::new().with_text("hello".to_owned()));

        let message = normalize_update(
            &event,
            channel,
            &SlackTeamId::new("T1".to_owned()),
            &SlackEventId::new("Ev1".to_owned()),
        )
        .unwrap();

        assert_eq!(message.body, "hello");
        assert_eq!(message.metadata["channel_id"], "C123");
        assert_eq!(message.metadata["team_id"], "T1");
        assert_eq!(message.metadata["event_id"], "Ev1");
        assert_eq!(message.metadata["ts"], "1234567890.000001");
        assert!(message.metadata["user_id"].is_null());
    }

    #[test]
    fn plain_text_request_uses_channel_and_rejects_invalid_destination_or_metadata() {
        let message = NormalizedMessage {
            body: "hello".to_owned(),
            metadata: json!({}),
        };
        let request = post_message_request("C123", &message).unwrap();
        let serialized = serde_json::to_string(&request).unwrap();
        assert!(serialized.contains("C123"));
        assert!(serialized.contains("hello"));

        let Err(error) = post_message_request(" ", &message) else {
            panic!("empty channel must fail");
        };
        assert!(matches!(
            error,
            MessagingSendError::Terminal {
                code: MessagingSendErrorCode::InvalidDestination,
                ..
            }
        ));
        let invalid_metadata = NormalizedMessage {
            body: "hello".to_owned(),
            metadata: json!({ "unexpected": true }),
        };
        let Err(error) = post_message_request("C123", &invalid_metadata) else {
            panic!("invalid metadata must fail");
        };
        assert!(matches!(
            error,
            MessagingSendError::Terminal {
                code: MessagingSendErrorCode::Validation,
                ..
            }
        ));
    }

    #[test]
    fn interactive_prompt_has_section_and_approval_buttons() {
        let request_id = SpectaUuid(Uuid::nil());
        let [section, actions] = interactive_blocks(request_id, "Approve this?");
        assert!(matches!(section, SlackBlock::Section(_)));
        assert!(matches!(&actions, SlackBlock::Actions(_)));
        let encoded = serde_json::to_string(&actions).unwrap();
        assert!(encoded.contains("permission_approve"));
        assert!(encoded.contains("permission_deny"));
        let approval_value = crate::types::PermissionButtonPayload::encode(request_id, InteractionAction::Approve);
        let denial_value = crate::types::PermissionButtonPayload::encode(request_id, InteractionAction::Deny);
        assert!(encoded.contains(&approval_value));
        assert!(encoded.contains(&denial_value));
    }
}

mod send {
    use serde_json::json;
    use uuid::Uuid;

    use aves::utils::uuid::SpectaUuid;

    use crate::types::{
        InteractionAction, MessagingSendError, MessagingSendErrorCode, NormalizedMessage, PermissionButtonPayload,
        SlackSenderConfig,
    };

    use super::super::helpers::post_message_request;
    use super::super::send::{test_prepare_interactive_edit, test_prepare_interactive_send};

    fn sender(channel_id: &str) -> SlackSenderConfig {
        SlackSenderConfig {
            connection_id: SpectaUuid(Uuid::nil()),
            channel_id: channel_id.to_owned(),
            persistence: Default::default(),
        }
    }

    #[test]
    fn final_edit_preserves_request_and_removes_permission_buttons() {
        let request = test_prepare_interactive_edit(&sender("C123"), "123.0001", "Approve?", "Denied").unwrap();
        let request = serde_json::to_value(request).unwrap();
        assert_eq!(request["channel"], "C123");
        assert_eq!(request["ts"], "123.0001");
        assert_eq!(request["text"], "Approve?\n\nDenied");
        assert_eq!(request["blocks"], json!([]));
    }

    #[test]
    fn send_preparation_rejects_invalid_metadata() {
        let message = NormalizedMessage {
            body: "hello".to_owned(),
            metadata: json!({ "unexpected": true }),
        };
        assert!(matches!(
            post_message_request("C123", &message),
            Err(MessagingSendError::Terminal {
                code: MessagingSendErrorCode::Validation,
                ..
            })
        ));
    }

    #[test]
    fn interactive_send_and_edit_preparation_reject_empty_channel() {
        let sender = sender(" ");
        assert!(matches!(
            test_prepare_interactive_send(&sender, SpectaUuid(Uuid::nil()), "Approve?"),
            Err(MessagingSendError::Terminal {
                code: MessagingSendErrorCode::InvalidDestination,
                ..
            })
        ));
        assert!(matches!(
            test_prepare_interactive_edit(&sender, "123.0001", "Approve?", "Denied"),
            Err(MessagingSendError::Terminal {
                code: MessagingSendErrorCode::InvalidDestination,
                ..
            })
        ));
    }

    #[test]
    fn interactive_send_preparation_preserves_destination_text_and_buttons() {
        let request_id = SpectaUuid(Uuid::nil());
        let request = test_prepare_interactive_send(&sender("C123"), request_id, "Approve?").unwrap();
        let request = serde_json::to_value(request).unwrap();
        assert_eq!(request["channel"], "C123");
        assert_eq!(request["text"], "Approve?");
        let buttons = request["blocks"][1]["elements"].as_array().unwrap();
        assert_eq!(buttons.len(), 2);
        assert_eq!(
            buttons[0]["value"],
            PermissionButtonPayload::encode(request_id, InteractionAction::Approve)
        );
        assert_eq!(
            buttons[1]["value"],
            PermissionButtonPayload::encode(request_id, InteractionAction::Deny)
        );
    }
}

mod plugin {
    use serde_json::json;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use aves::utils::uuid::SpectaUuid;

    use crate::channels::MessagingPlugin;
    use crate::messages::ListenerType;
    use crate::types::{
        MessagingError, MessagingSendError, MessagingSendErrorCode, MessagingSenderConfig, MessagingSubscriptionError,
        NormalizedMessage, SlackSenderConfig, TelegramSenderConfig,
    };

    use super::super::SlackPlugin;

    fn sender() -> MessagingSenderConfig {
        MessagingSenderConfig::Slack(SlackSenderConfig {
            connection_id: SpectaUuid(Uuid::nil()),
            channel_id: "C123".to_owned(),
            persistence: Default::default(),
        })
    }

    #[tokio::test]
    async fn send_requires_a_connected_slack_plugin() {
        let mut plugin = SlackPlugin::default();
        let message = NormalizedMessage {
            body: "hello".to_owned(),
            metadata: json!({}),
        };
        assert!(matches!(
            plugin.send(&sender(), &message).await,
            Err(MessagingError::Send(MessagingSendError::Terminal {
                code: MessagingSendErrorCode::InvalidCredentials,
                ..
            }))
        ));
    }

    #[tokio::test]
    async fn listen_requires_a_connected_slack_plugin() {
        let mut plugin = SlackPlugin::default();
        let (tx, _) = mpsc::channel(1);
        let (interaction_tx, _) = mpsc::channel(1);
        assert!(matches!(
            plugin.listen(tx, interaction_tx).await,
            Err(MessagingSubscriptionError::Configuration {
                listener_type: ListenerType::Slack,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn slack_rejects_another_provider_sender_for_all_send_operations() {
        let mut plugin = SlackPlugin::default();
        let sender = MessagingSenderConfig::Telegram(TelegramSenderConfig {
            connection_id: SpectaUuid(Uuid::nil()),
            chat_id: "123".to_owned(),
            persistence: Default::default(),
        });
        let message = NormalizedMessage {
            body: "hello".to_owned(),
            metadata: json!({}),
        };
        assert!(matches!(
            plugin.send(&sender, &message).await,
            Err(MessagingError::Send(MessagingSendError::Terminal {
                code: MessagingSendErrorCode::Validation,
                ..
            }))
        ));
        assert!(matches!(
            plugin
                .send_interactive(&sender, SpectaUuid(Uuid::nil()), "Approve?")
                .await,
            Err(MessagingError::Send(MessagingSendError::Terminal {
                code: MessagingSendErrorCode::Validation,
                ..
            }))
        ));
        assert!(matches!(
            plugin
                .edit_interactive(&sender, "1", "Approve?", "Denied")
                .await,
            Err(MessagingError::Send(MessagingSendError::Terminal {
                code: MessagingSendErrorCode::Validation,
                ..
            }))
        ));
    }
}

mod subscribe {
    use slack_morphism::prelude::{
        SlackChannelId, SlackMessageContent, SlackMessageEvent, SlackMessageEventType, SlackMessageOrigin,
        SlackMessageSender, SlackTs,
    };
    use tokio::sync::mpsc;

    use crate::channels::MessagingSubscription;

    use super::super::filter::SlackFilter;
    use super::*;

    const TARGET_CHANNEL: &str = "C12345678";
    const OTHER_CHANNEL: &str = "C99999999";
    const TEAM_ID: &str = "T0001";
    const EVENT_ID: &str = "Ev0001";

    fn make_team_id() -> SlackTeamId {
        SlackTeamId::new(TEAM_ID.into())
    }

    fn make_event_id() -> SlackEventId {
        SlackEventId::new(EVENT_ID.into())
    }

    fn make_ts() -> SlackTs {
        SlackTs::new("1234567890.000001".into())
    }

    async fn handle_message_event(
        message: SlackMessageEvent,
        team: &SlackTeamId,
        event: &SlackEventId,
        policy: &SlackFilter,
        tx: &mpsc::Sender<InboundEvent>,
    ) {
        let (raw_tx, mut raw_rx) = mpsc::channel(8);
        let (normalized_tx, normalized_rx) = mpsc::channel(8);
        let (filtered_tx, mut filtered_rx) = mpsc::channel(8);
        let task = run_filters(
            MessagingSubscription {
                subscribed_channels: policy.subscribed_channels.clone(),
                allowed_senders: policy.allowed_senders.clone(),
            },
            normalized_rx,
            filtered_tx,
        )
        .unwrap();
        let serialized = serde_json::to_string(&message).unwrap();
        super::handle_message_event(message, team, event, &serialized, &raw_tx).await;
        if let Ok(event) = raw_rx.try_recv()
            && let Some(normalized) = event.normalized
        {
            normalized_tx.send(normalized).await.unwrap();
        }
        drop(raw_tx);
        drop(normalized_tx);
        task.await.unwrap().unwrap();
        if let Ok(normalized) = filtered_rx.try_recv() {
            tx.send(InboundEvent {
                raw_payload: Vec::new(),
                normalized: Some(normalized),
            })
            .await
            .unwrap();
        }
    }

    /// Build a minimal `SlackMessageEvent` for the target channel with text content.
    fn make_text_event(channel: &str, text: &str) -> SlackMessageEvent {
        let origin = SlackMessageOrigin::new(make_ts()).with_channel(SlackChannelId::new(channel.into()));
        let sender = SlackMessageSender::new();
        let content = SlackMessageContent::new().with_text(text.into());
        SlackMessageEvent::new(origin, sender).with_content(content)
    }

    /// Build a minimal `SlackMessageEvent` with no channel ID in origin.
    fn make_event_no_channel(text: &str) -> SlackMessageEvent {
        let origin = SlackMessageOrigin::new(make_ts());
        let sender = SlackMessageSender::new();
        let content = SlackMessageContent::new().with_text(text.into());
        SlackMessageEvent::new(origin, sender).with_content(content)
    }

    /// Build a `SlackMessageEvent` with a subtype (e.g. bot_message).
    fn make_event_with_subtype(channel: &str, text: &str) -> SlackMessageEvent {
        let origin = SlackMessageOrigin::new(make_ts()).with_channel(SlackChannelId::new(channel.into()));
        let sender = SlackMessageSender::new();
        let content = SlackMessageContent::new().with_text(text.into());
        SlackMessageEvent::new(origin, sender)
            .with_content(content)
            .with_subtype(SlackMessageEventType::BotMessage)
    }

    /// Build a `SlackMessageEvent` with no text content (attachment-only).
    fn make_event_no_text(channel: &str) -> SlackMessageEvent {
        let origin = SlackMessageOrigin::new(make_ts()).with_channel(SlackChannelId::new(channel.into()));
        let sender = SlackMessageSender::new();
        // No content set — simulates attachment-only message.
        SlackMessageEvent::new(origin, sender)
    }

    #[tokio::test]
    async fn message_matching_channel_forwarded() {
        let (tx, mut rx) = mpsc::channel(10);
        let event = make_text_event(TARGET_CHANNEL, "hello slack");

        handle_message_event(
            event,
            &make_team_id(),
            &make_event_id(),
            &SlackFilter::new(Some(vec![TARGET_CHANNEL.to_owned()])),
            &tx,
        )
        .await;

        let msg = rx.try_recv().expect("expected a forwarded message");
        let msg = msg.normalized.expect("normalized");
        assert_eq!(msg.body, "hello slack");
        assert_eq!(msg.metadata["channel_type"], "slack");
    }

    #[tokio::test]
    async fn message_wrong_channel_dropped() {
        let (tx, mut rx) = mpsc::channel(10);
        let event = make_text_event(OTHER_CHANNEL, "wrong channel");

        handle_message_event(
            event,
            &make_team_id(),
            &make_event_id(),
            &SlackFilter::new(Some(vec![TARGET_CHANNEL.to_owned()])),
            &tx,
        )
        .await;

        assert!(rx.try_recv().is_err(), "expected no message for wrong channel");
    }

    #[tokio::test]
    async fn message_no_channel_id_dropped() {
        let (tx, mut rx) = mpsc::channel(10);
        let event = make_event_no_channel("no channel");

        handle_message_event(
            event,
            &make_team_id(),
            &make_event_id(),
            &SlackFilter::new(Some(vec![TARGET_CHANNEL.to_owned()])),
            &tx,
        )
        .await;

        assert!(rx.try_recv().is_err(), "expected no message when channel ID absent");
    }

    #[tokio::test]
    async fn message_with_subtype_dropped() {
        // Non-human subtypes (bot_message, channel_join, etc.) must be filtered.
        let (tx, mut rx) = mpsc::channel(10);
        let event = make_event_with_subtype(TARGET_CHANNEL, "bot says hi");

        handle_message_event(
            event,
            &make_team_id(),
            &make_event_id(),
            &SlackFilter::new(Some(vec![TARGET_CHANNEL.to_owned()])),
            &tx,
        )
        .await;

        assert!(rx.try_recv().is_err(), "expected no message for bot_message subtype");
    }

    #[tokio::test]
    async fn message_no_text_dropped() {
        // Attachment-only messages (no text content) must be filtered.
        let (tx, mut rx) = mpsc::channel(10);
        let event = make_event_no_text(TARGET_CHANNEL);

        handle_message_event(
            event,
            &make_team_id(),
            &make_event_id(),
            &SlackFilter::new(Some(vec![TARGET_CHANNEL.to_owned()])),
            &tx,
        )
        .await;

        assert!(rx.try_recv().is_err(), "expected no message for attachment-only event");
    }

    #[tokio::test]
    async fn message_empty_text_dropped() {
        // Empty text string must be treated as no body.
        let (tx, mut rx) = mpsc::channel(10);
        let event = make_text_event(TARGET_CHANNEL, "");

        handle_message_event(
            event,
            &make_team_id(),
            &make_event_id(),
            &SlackFilter::new(Some(vec![TARGET_CHANNEL.to_owned()])),
            &tx,
        )
        .await;

        assert!(rx.try_recv().is_err(), "expected no message for empty text");
    }

    #[tokio::test]
    async fn message_metadata_contains_channel_type() {
        let (tx, mut rx) = mpsc::channel(10);
        let event = make_text_event(TARGET_CHANNEL, "metadata check");

        handle_message_event(
            event,
            &make_team_id(),
            &make_event_id(),
            &SlackFilter::new(Some(vec![TARGET_CHANNEL.to_owned()])),
            &tx,
        )
        .await;

        let msg = rx
            .try_recv()
            .expect("expected a forwarded message")
            .normalized
            .expect("normalized");
        assert_eq!(msg.metadata["channel_type"], "slack");
        let channel_id_meta = msg.metadata["channel_id"].as_str().unwrap_or("");
        assert_eq!(channel_id_meta, TARGET_CHANNEL);
    }

    #[test]
    fn channel_policy_is_bound_to_each_subscription() {
        let restricted = SlackFilter::new(Some(vec!["C123".to_owned()]));
        assert!(restricted.accepts("C123"));
        assert!(!restricted.accepts("C999"));
        let unrestricted = SlackFilter::new(None);
        assert!(unrestricted.accepts("C123"));
        assert!(unrestricted.accepts("C999"));
        assert!(
            MessagingSubscription {
                subscribed_channels: Some(Vec::new()),
                allowed_senders: Vec::new()
            }
            .validate(ListenerType::Slack)
            .is_err()
        );
    }
}
