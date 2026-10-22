use crate::channels::MessagingSubscription;
use crate::messages::ListenerType;
use crate::types::{EmailConfig, MessagingError, NormalizedMessage};

use super::connection::test_prepare as prepare;
use super::filter::EmailFilter;
use super::helpers::sender_in_list;
use super::subscribe::run_filters;

mod connection {
    use super::*;

    #[test]
    fn configure_rejects_missing_settings_and_wrong_credential_kind() {
        use crate::types::{MessagingConfig, MessagingCredentials};
        use zeroize::Zeroizing;

        let email = MessagingCredentials::Email {
            username: Zeroizing::new("user".to_owned()),
            password: Zeroizing::new("password".to_owned()),
        };
        assert!(matches!(
            super::super::connection::configure(&MessagingConfig::default(), &email),
            Err(MessagingError::ChannelConnectionFailed {
                listener_type: ListenerType::Email,
                ..
            })
        ));
        let settings = MessagingConfig {
            email: Some(authenticated_config()),
            ..Default::default()
        };
        let telegram = MessagingCredentials::Telegram {
            token: Zeroizing::new("123:test".to_owned()),
        };
        assert!(matches!(
            super::super::connection::configure(&settings, &telegram),
            Err(MessagingError::ChannelConnectionFailed {
                listener_type: ListenerType::Email,
                ..
            })
        ));
    }

    fn authenticated_config() -> EmailConfig {
        EmailConfig {
            imap_server: Some("imap.example.com".to_owned()),
            username: Some("user@example.com".to_owned().into()),
            password: Some("password".to_owned().into()),
            ..Default::default()
        }
    }

    #[test]
    fn transport_preparation_requires_no_mailbox_or_trigger() {
        assert!(prepare(&authenticated_config()).is_ok());
    }

    #[test]
    fn transport_preparation_preserves_required_field_validation_order() {
        assert!(
            matches!(prepare(&EmailConfig::default()), Err(MessagingError::ChannelConnectionFailed { listener_type: ListenerType::Email, message }) if message.contains("IMAP server is missing"))
        );
        let mut config = authenticated_config();
        config.username = None;
        config.password = None;
        assert!(
            matches!(prepare(&config), Err(MessagingError::ChannelConnectionFailed { listener_type: ListenerType::Email, message }) if message.contains("username is missing"))
        );
        config.username = Some("user".to_owned().into());
        assert!(
            matches!(prepare(&config), Err(MessagingError::ChannelConnectionFailed { listener_type: ListenerType::Email, message }) if message.contains("password is missing"))
        );
    }
}

mod filter {
    use serde_json::{Value, json};
    use tokio::sync::mpsc;

    use super::super::helpers::normalize_update;
    use super::*;

    fn make_raw_email(from: &str, subject: &str, body: &str) -> Vec<u8> {
        format!("From: {from}\r\nSubject: {subject}\r\nContent-Type: text/plain\r\n\r\n{body}").into_bytes()
    }

    fn normalized_in(folder: Option<&str>) -> NormalizedMessage {
        NormalizedMessage {
            body: "message".to_owned(),
            metadata: json!({ "from_address": "sender@example.com", "channel_id": folder }),
        }
    }

    #[test]
    fn trigger_sender_policy_filters_before_emitting_normalized_updates() {
        let raw = make_raw_email("Sender <Sender@Example.com>", "subject", "body");
        assert!(
            EmailFilter::new(Vec::new())
                .apply(normalize_update(&raw).unwrap())
                .is_some()
        );
        assert!(
            EmailFilter::new(vec!["sender@example.com".to_owned()])
                .apply(normalize_update(&raw).unwrap())
                .is_some()
        );
        assert!(
            EmailFilter::new(vec!["other@example.com".to_owned()])
                .apply(normalize_update(&raw).unwrap())
                .is_none()
        );
        let unidentified = b"Subject: subject\r\nContent-Type: text/plain\r\n\r\nbody";
        assert!(
            EmailFilter::new(vec!["sender@example.com".to_owned()])
                .apply(normalize_update(unidentified).unwrap())
                .is_none()
        );
    }

    #[test]
    fn folder_policy_folds_inbox_only_and_requires_a_folder_for_restricted_subscriptions() {
        let mut inbox = EmailFilter::new(Vec::new());
        inbox.subscribed_channels = Some(vec!["INBOX".to_owned()]);
        assert!(inbox.apply(normalized_in(Some("inbox"))).is_some());
        assert!(inbox.apply(normalized_in(Some("Other"))).is_none());
        assert!(inbox.apply(normalized_in(None)).is_none());

        let mut custom = EmailFilter::new(Vec::new());
        custom.subscribed_channels = Some(vec!["Alerts".to_owned()]);
        assert!(custom.apply(normalized_in(Some("Alerts"))).is_some());
        assert!(custom.apply(normalized_in(Some("alerts"))).is_none());
    }

    #[tokio::test]
    async fn filter_task_delivers_only_allowed_sender_and_folder_without_imap() {
        let (input_tx, input_rx) = mpsc::channel(4);
        let (output_tx, mut output_rx) = mpsc::channel(4);
        let task = run_filters(
            MessagingSubscription {
                subscribed_channels: Some(vec!["INBOX".to_owned()]),
                allowed_senders: vec!["sender@example.com".to_owned()],
            },
            input_rx,
            output_tx,
        )
        .unwrap();

        input_tx.send(normalized_in(Some("INBOX"))).await.unwrap();
        input_tx.send(normalized_in(Some("Other"))).await.unwrap();
        input_tx
            .send(NormalizedMessage {
                body: "wrong sender".to_owned(),
                metadata: json!({ "from_address": "other@example.com", "channel_id": "INBOX" }),
            })
            .await
            .unwrap();
        drop(input_tx);
        task.await.unwrap().unwrap();

        assert_eq!(output_rx.try_recv().unwrap().body, "message");
        assert!(output_rx.try_recv().is_err());
    }

    #[test]
    fn empty_folder_selection_is_rejected_before_starting_a_filter() {
        let (_, input_rx) = mpsc::channel(1);
        let (output_tx, _) = mpsc::channel(1);
        assert!(
            run_filters(
                MessagingSubscription {
                    subscribed_channels: Some(Vec::new()),
                    allowed_senders: Vec::new(),
                },
                input_rx,
                output_tx,
            )
            .is_err()
        );
    }

    #[test]
    fn normalize_message_extracts_body_and_subject() {
        let raw = make_raw_email("Alice <alice@example.com>", "Hello world", "This is the body.");
        let msg = normalize_update(&raw).expect("should parse");
        assert_eq!(msg.body, "This is the body.");
        assert_eq!(msg.metadata.get("subject").and_then(Value::as_str), Some("Hello world"));
    }

    #[test]
    fn normalize_message_extracts_from_name_and_address() {
        let raw = make_raw_email("Alice <alice@example.com>", "Hi", "Body text.");
        let msg = normalize_update(&raw).expect("should parse");
        assert_eq!(msg.metadata.get("from_name").and_then(Value::as_str), Some("Alice"));
        assert_eq!(
            msg.metadata.get("from_address").and_then(Value::as_str),
            Some("alice@example.com")
        );
    }

    #[test]
    fn normalize_message_returns_none_when_body_and_subject_empty() {
        let raw = b"From: sender@example.com\r\nContent-Type: text/plain\r\n\r\n";
        let result = normalize_update(raw);
        assert!(result.is_none(), "expected None for empty body+subject");
    }

    #[test]
    fn normalize_message_accepts_subject_only() {
        let raw = b"From: sender@example.com\r\nSubject: Alert!\r\nContent-Type: text/plain\r\n\r\n";
        let msg = normalize_update(raw).expect("should parse when subject present");
        assert_eq!(msg.metadata.get("subject").and_then(Value::as_str), Some("Alert!"));
        assert_eq!(msg.body, "");
    }

    #[test]
    fn normalize_message_accepts_body_only() {
        let raw = b"From: sender@example.com\r\nContent-Type: text/plain\r\n\r\nJust a body.";
        let msg = normalize_update(raw).expect("should parse when body present");
        assert_eq!(msg.body, "Just a body.");
        assert_eq!(msg.metadata.get("subject").and_then(Value::as_str), Some(""));
    }

    #[test]
    fn normalize_message_includes_channel_type_in_metadata() {
        let raw = make_raw_email("x@example.com", "Test", "Body.");
        let msg = normalize_update(&raw).expect("should parse");
        assert!(
            msg.metadata.get("channel_type").is_some(),
            "channel_type must be present in metadata"
        );
    }

    #[test]
    fn normalize_message_missing_from_address_is_null_in_metadata() {
        let raw = b"Subject: No From\r\nContent-Type: text/plain\r\n\r\nBody here.";
        let msg = normalize_update(raw).expect("should parse");
        assert_eq!(msg.metadata.get("from_address"), Some(&Value::Null));
    }

    // ── allowed_senders filtering tests ──────────────────────────────────────
    fn passes_sender_filter(from_address: &str, allowed_senders: &Option<Vec<String>>) -> bool {
        if let Some(allowed) = allowed_senders {
            if !allowed.is_empty() {
                return allowed.iter().any(|a| a.eq_ignore_ascii_case(from_address));
            }
        }
        true
    }

    #[test]
    fn allowed_senders_none_passes_all() {
        assert!(passes_sender_filter("anyone@example.com", &None));
    }

    #[test]
    fn allowed_senders_empty_vec_passes_all() {
        assert!(passes_sender_filter("anyone@example.com", &Some(vec![])));
    }

    #[test]
    fn allowed_senders_exact_match_passes() {
        let allowed = Some(vec!["alice@example.com".to_string()]);
        assert!(passes_sender_filter("alice@example.com", &allowed));
    }

    #[test]
    fn allowed_senders_case_insensitive_match_passes() {
        let allowed = Some(vec!["Alice@Example.COM".to_string()]);
        assert!(passes_sender_filter("alice@example.com", &allowed));
    }

    #[test]
    fn allowed_senders_non_matching_sender_blocked() {
        let allowed = Some(vec!["alice@example.com".to_string()]);
        assert!(!passes_sender_filter("eve@evil.com", &allowed));
    }

    #[test]
    fn allowed_senders_multiple_entries_any_match_passes() {
        let allowed = Some(vec!["alice@example.com".to_string(), "bob@example.com".to_string()]);
        assert!(passes_sender_filter("bob@example.com", &allowed));
        assert!(!passes_sender_filter("eve@evil.com", &allowed));
    }

    fn uid_should_process(uid: Option<u32>, last_uid: u32) -> bool {
        super::super::subscribe::test_new_uid(uid, last_uid)
    }

    #[test]
    fn uid_guard_skips_equal_uid() {
        assert!(!uid_should_process(Some(42), 42));
    }

    #[test]
    fn uid_guard_skips_lower_uid() {
        assert!(!uid_should_process(Some(41), 42));
    }

    #[test]
    fn uid_guard_passes_strictly_greater_uid() {
        assert!(uid_should_process(Some(43), 42));
    }

    #[test]
    fn uid_guard_skips_none_uid() {
        assert!(!uid_should_process(None, 42));
    }

    #[test]
    fn uid_guard_passes_when_last_uid_is_zero() {
        assert!(uid_should_process(Some(1), 0));
    }
}

mod helpers {
    use serde_json::json;

    use super::*;

    fn message(from_address: serde_json::Value) -> NormalizedMessage {
        NormalizedMessage {
            body: String::new(),
            metadata: json!({ "from_address": from_address }),
        }
    }

    #[test]
    fn empty_sender_list_is_unrestricted() {
        assert!(sender_in_list(&message(json!(null)), &Vec::new()));
    }

    #[test]
    fn restricted_sender_list_requires_nonempty_string_identity() {
        let allowed = vec!["sender@example.com".to_owned()];

        assert!(!sender_in_list(&message(json!(null)), &allowed));
        assert!(!sender_in_list(&message(json!(42)), &allowed));
        assert!(!sender_in_list(&message(json!("")), &allowed));
        assert!(!sender_in_list(&message(json!("other@example.com")), &allowed));
    }

    #[test]
    fn sender_matching_is_case_insensitive() {
        let allowed = vec!["Sender@Example.com".to_owned()];

        assert!(sender_in_list(&message(json!("sender@example.COM")), &allowed));
    }
}

mod plugin {
    use serde_json::json;
    use tokio::sync::mpsc;
    use uuid::Uuid;
    use zeroize::Zeroizing;

    use aves::utils::uuid::SpectaUuid;

    use crate::channels::MessagingPlugin;
    use crate::messages::ListenerType;
    use crate::types::{
        EmailConfig, EmailSenderConfig, MessagingConfig, MessagingCredentials, MessagingError, MessagingSendError,
        MessagingSendErrorCode, MessagingSenderConfig, MessagingSubscriptionError, NormalizedMessage,
        TelegramSenderConfig,
    };

    use super::super::EmailPlugin;

    fn sender() -> MessagingSenderConfig {
        MessagingSenderConfig::Email(EmailSenderConfig {
            connection_id: SpectaUuid(Uuid::nil()),
            from: "sender@example.com".to_owned(),
            persistence: Default::default(),
        })
    }

    #[tokio::test]
    async fn send_requires_connected_email_credentials() {
        let mut plugin = EmailPlugin::default();
        let message = NormalizedMessage {
            body: "hello".to_owned(),
            metadata: json!({ "to": ["recipient@example.com"] }),
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
    async fn smtp_only_connection_rejects_inbound_listening_without_imap() {
        let mut plugin = EmailPlugin::default();
        let settings = MessagingConfig {
            email: Some(EmailConfig {
                smtp_server: Some("smtp.example.com".to_owned()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let credentials = MessagingCredentials::Email {
            username: Zeroizing::new("user".to_owned()),
            password: Zeroizing::new("password".to_owned()),
        };
        plugin.connect(&settings, &credentials).await.unwrap();
        let (tx, _) = mpsc::channel(1);
        let (interaction_tx, _) = mpsc::channel(1);
        assert!(matches!(
            plugin.listen(tx, interaction_tx).await,
            Err(MessagingSubscriptionError::Configuration {
                listener_type: ListenerType::Email,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn email_does_not_support_interactive_send() {
        let mut plugin = EmailPlugin::default();
        assert!(matches!(
            plugin
                .send_interactive(&sender(), SpectaUuid(Uuid::nil()), "Approve?")
                .await,
            Err(MessagingError::InteractiveUnsupported {
                provider: ListenerType::Email
            })
        ));
    }

    #[tokio::test]
    async fn email_rejects_another_provider_sender_before_connection_lookup() {
        let mut plugin = EmailPlugin::default();
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
    }
}

mod send {
    use serde_json::json;
    use uuid::Uuid;
    use zeroize::Zeroizing;

    use aves::utils::uuid::SpectaUuid;

    use crate::types::{
        EmailConfig, EmailSendMetadata, EmailSenderConfig, MessagingSendError, MessagingSendErrorCode,
        NormalizedMessage,
    };

    use super::super::helpers::{deserialize_metadata, parse_mailbox};
    use super::super::send::{send, test_build_email, test_permanent_code, test_transient_code};

    fn sender() -> EmailSenderConfig {
        EmailSenderConfig {
            connection_id: SpectaUuid(Uuid::nil()),
            from: "sender@example.com".to_owned(),
            persistence: Default::default(),
        }
    }

    fn message(metadata: serde_json::Value) -> NormalizedMessage {
        NormalizedMessage {
            body: "hello".to_owned(),
            metadata,
        }
    }

    #[test]
    fn composed_email_includes_all_recipients_subject_and_plain_text_body() {
        let email = test_build_email(
            &sender(),
            &message(json!({
                "to": ["to@example.com"],
                "cc": ["cc@example.com"],
                "bcc": ["bcc@example.com"],
                "subject": "Report"
            })),
        )
        .unwrap();
        assert_eq!(email.envelope().to().len(), 3);
        let formatted = String::from_utf8(email.formatted()).unwrap();
        assert!(formatted.contains("Subject: Report"));
        assert!(formatted.contains("text/plain"));
        assert!(formatted.contains("hello"));
    }

    #[test]
    fn transient_smtp_responses_retry_as_rate_limit_or_server() {
        for code in [421, 450, 451, 452] {
            assert_eq!(test_transient_code(Some(code)), MessagingSendErrorCode::RateLimited);
        }
        assert_eq!(test_transient_code(Some(454)), MessagingSendErrorCode::Server);
    }

    #[test]
    fn permanent_smtp_responses_have_terminal_categories() {
        for code in [530, 535] {
            assert_eq!(
                test_permanent_code(Some(code)),
                MessagingSendErrorCode::InvalidCredentials
            );
        }
        for code in [550, 551, 553] {
            assert_eq!(
                test_permanent_code(Some(code)),
                MessagingSendErrorCode::InvalidDestination
            );
        }
        assert_eq!(test_permanent_code(Some(554)), MessagingSendErrorCode::PermissionDenied);
        assert_eq!(test_permanent_code(Some(552)), MessagingSendErrorCode::Server);
    }

    #[tokio::test]
    async fn send_rejects_missing_recipients_before_smtp() {
        let username = Zeroizing::new("user".to_owned());
        let password = Zeroizing::new("password".to_owned());
        assert!(matches!(
            send(
                &sender(),
                &EmailConfig::default(),
                (&username, &password),
                &message(json!({}))
            )
            .await,
            Err(MessagingSendError::Terminal {
                code: MessagingSendErrorCode::Validation,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn send_rejects_bad_sender_address_before_smtp() {
        let username = Zeroizing::new("user".to_owned());
        let password = Zeroizing::new("password".to_owned());
        let mut sender = sender();
        sender.from = "invalid@@example.com".to_owned();
        assert!(matches!(
            send(
                &sender,
                &EmailConfig::default(),
                (&username, &password),
                &message(json!({ "to": ["recipient@example.com"] }))
            )
            .await,
            Err(MessagingSendError::Terminal {
                code: MessagingSendErrorCode::InvalidDestination,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn send_rejects_missing_smtp_server_before_transport() {
        let username = Zeroizing::new("user".to_owned());
        let password = Zeroizing::new("password".to_owned());
        assert!(matches!(
            send(
                &sender(),
                &EmailConfig::default(),
                (&username, &password),
                &message(json!({ "to": ["recipient@example.com"] }))
            )
            .await,
            Err(MessagingSendError::Terminal {
                code: MessagingSendErrorCode::Validation,
                ..
            })
        ));
    }

    #[test]
    fn outbound_metadata_rejects_unknown_fields_before_smtp() {
        let error = deserialize_metadata::<EmailSendMetadata>(&json!({ "unexpected": true })).unwrap_err();
        assert!(matches!(
            error,
            MessagingSendError::Terminal {
                code: MessagingSendErrorCode::Validation,
                ..
            }
        ));
    }

    #[test]
    fn invalid_mailbox_is_a_terminal_destination_error() {
        let error = parse_mailbox("invalid@@example.com").unwrap_err();
        assert!(matches!(
            error,
            MessagingSendError::Terminal {
                code: MessagingSendErrorCode::InvalidDestination,
                ..
            }
        ));
    }
}

mod subscribe {
    use serde_json::json;
    use tokio::sync::mpsc;

    use crate::channels::MessagingSubscription;
    use crate::types::NormalizedMessage;

    use super::super::subscribe::run_filters;

    #[test]
    fn cursor_range_uses_only_new_uids_and_resets_after_uidvalidity_change() {
        let (range, uid) = super::super::subscribe::test_cursor_range(Some(1), 42, Some(1), Some(45));
        assert_eq!(range, Some((43, 45)));
        assert_eq!(uid, 42);

        let (range, uid) = super::super::subscribe::test_cursor_range(Some(1), 42, Some(2), Some(45));
        assert_eq!(range, None);
        assert_eq!(uid, 44);

        let (range, uid) = super::super::subscribe::test_cursor_range(Some(1), u32::MAX, Some(1), Some(45));
        assert_eq!(range, None);
        assert_eq!(uid, u32::MAX);
    }

    #[tokio::test]
    async fn unrestricted_subscription_forwards_messages_without_a_folder_or_sender() {
        let (input_tx, input_rx) = mpsc::channel(2);
        let (output_tx, mut output_rx) = mpsc::channel(2);
        let task = run_filters(
            MessagingSubscription {
                subscribed_channels: None,
                allowed_senders: Vec::new(),
            },
            input_rx,
            output_tx,
        )
        .unwrap();

        input_tx
            .send(NormalizedMessage {
                body: "folderless".to_owned(),
                metadata: json!({}),
            })
            .await
            .unwrap();
        input_tx
            .send(NormalizedMessage {
                body: "another mailbox".to_owned(),
                metadata: json!({ "channel_id": "Alerts" }),
            })
            .await
            .unwrap();
        drop(input_tx);
        task.await.unwrap().unwrap();

        assert_eq!(output_rx.try_recv().unwrap().body, "folderless");
        assert_eq!(output_rx.try_recv().unwrap().body, "another mailbox");
        assert!(output_rx.try_recv().is_err());
    }
}
