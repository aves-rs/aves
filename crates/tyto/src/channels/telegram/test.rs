use super::connection::test_prepare as prepare;
use super::filter::TelegramFilter;
use super::subscribe::run_filters;
use crate::channels::MessagingSubscription;
use crate::messages::ListenerType;
use crate::types::{MessagingError, NormalizedMessage, TelegramConfig};
use tokio::sync::mpsc;

mod connection {
    use super::*;

    #[test]
    fn configure_rejects_missing_settings_and_wrong_credential_kind() {
        use crate::types::{MessagingConfig, MessagingCredentials};
        use zeroize::Zeroizing;

        let telegram = MessagingCredentials::Telegram {
            token: Zeroizing::new("123:test".to_owned()),
        };
        assert!(matches!(
            super::super::connection::configure(&MessagingConfig::default(), &telegram),
            Err(MessagingError::ChannelConnectionFailed {
                listener_type: ListenerType::Telegram,
                ..
            })
        ));
        let settings = MessagingConfig {
            telegram: Some(TelegramConfig::default()),
            ..Default::default()
        };
        let email = MessagingCredentials::Email {
            username: Zeroizing::new("user".to_owned()),
            password: Zeroizing::new("password".to_owned()),
        };
        assert!(matches!(
            super::super::connection::configure(&settings, &email),
            Err(MessagingError::ChannelConnectionFailed {
                listener_type: ListenerType::Telegram,
                ..
            })
        ));
    }

    #[test]
    fn transport_preparation_rejects_missing_token_without_needing_a_target() {
        assert!(
            matches!(prepare(&TelegramConfig::default()), Err(MessagingError::ChannelConnectionFailed { listener_type: ListenerType::Telegram, ref message }) if message.contains("token is missing")),
            "expected ChannelConnectionFailed for missing token"
        );
    }

    #[test]
    fn invalid_api_url_fails_before_opening_a_telegram_connection() {
        let config = TelegramConfig {
            token: Some("123:test".to_owned().into()),
            api_url: Some("not a url".to_owned()),
        };
        assert!(matches!(
            prepare(&config),
            Err(MessagingError::ChannelConnectionFailed {
                listener_type: ListenerType::Telegram,
                ..
            })
        ));
    }
}

mod filter {
    use teloxide_core::types::Update;

    use super::super::helpers::extract_body;
    use super::*;

    fn normalize_update(update: &Update, chat_id: i64, _bot: &str, _poll: u64) -> Option<NormalizedMessage> {
        let message = super::super::helpers::normalize_update(update)?;
        TelegramFilter::new(Some(vec![chat_id.to_string()]), Vec::new()).apply(message)
    }

    fn make_text_update(update_id: i32, chat_id: i64, text: &str) -> teloxide_core::types::Update {
        let json = serde_json::json!({
            "update_id": update_id,
            "message": {
                "message_id": 1,
                "date": 1_234_567_890_i64,
                "chat": { "id": chat_id, "type": "private" },
                "from": { "id": 100, "is_bot": false, "first_name": "Test" },
                "text": text
            }
        });
        serde_json::from_str(&json.to_string()).expect("valid update JSON")
    }

    /// Construct an update whose message belongs to a different chat.
    fn make_text_update_wrong_chat(update_id: i32, text: &str) -> teloxide_core::types::Update {
        make_text_update(update_id, 99999, text)
    }

    #[test]
    fn trigger_policy_enforces_sender_acl_before_the_workstream() {
        let update = make_text_update(1, 12345, "message");
        assert!(
            TelegramFilter::new(Some(vec!["12345".to_owned()]), Vec::new())
                .apply(super::super::helpers::normalize_update(&update).unwrap())
                .is_some()
        );
        assert!(
            TelegramFilter::new(Some(vec!["12345".to_owned()]), vec!["100".to_owned()])
                .apply(super::super::helpers::normalize_update(&update).unwrap())
                .is_some()
        );
        assert!(
            TelegramFilter::new(Some(vec!["12345".to_owned()]), vec!["999".to_owned()])
                .apply(super::super::helpers::normalize_update(&update).unwrap())
                .is_none()
        );
        assert!(
            TelegramFilter::new(Some(vec!["999".to_owned()]), vec!["100".to_owned()])
                .apply(super::super::helpers::normalize_update(&update).unwrap())
                .is_none()
        );
    }

    #[test]
    fn sender_acl_rejects_an_update_without_sender_identity() {
        let mut update = make_text_update(1, 12345, "message");
        if let teloxide_core::types::UpdateKind::Message(message) = &mut update.kind {
            message.from = None;
        }
        assert!(
            TelegramFilter::new(Some(vec!["12345".to_owned()]), vec!["100".to_owned()])
                .apply(super::super::helpers::normalize_update(&update).unwrap())
                .is_none()
        );
    }

    /// Construct an update with a photo that carries a caption.
    fn make_photo_update_with_caption(update_id: i32, chat_id: i64, caption: &str) -> teloxide_core::types::Update {
        let json = serde_json::json!({
            "update_id": update_id,
            "message": {
                "message_id": 2,
                "date": 1_234_567_890_i64,
                "chat": { "id": chat_id, "type": "private" },
                "from": { "id": 100, "is_bot": false, "first_name": "Test" },
                "photo": [
                    { "file_id": "abc", "file_unique_id": "u1", "width": 100, "height": 100, "file_size": 1024 }
                ],
                "caption": caption
            }
        });
        serde_json::from_str(&json.to_string()).expect("valid photo update JSON")
    }

    /// Construct an update with a sticker (no text, no caption — attachment-only).
    fn make_sticker_update(update_id: i32, chat_id: i64) -> teloxide_core::types::Update {
        let json = serde_json::json!({
            "update_id": update_id,
            "message": {
                "message_id": 3,
                "date": 1_234_567_890_i64,
                "chat": { "id": chat_id, "type": "private" },
                "from": { "id": 100, "is_bot": false, "first_name": "Test" },
                "sticker": {
                    "file_id": "stk1",
                    "file_unique_id": "u2",
                    "width": 512,
                    "height": 512,
                    "is_animated": false,
                    "is_video": false,
                    "type": "regular"
                }
            }
        });
        serde_json::from_str(&json.to_string()).expect("valid sticker update JSON")
    }

    /// Construct a channel_post update (not a direct message).
    fn make_channel_post_update(update_id: i32, chat_id: i64, text: &str) -> teloxide_core::types::Update {
        let json = serde_json::json!({
            "update_id": update_id,
            "channel_post": {
                "message_id": 4,
                "date": 1_234_567_890_i64,
                "chat": { "id": chat_id, "type": "channel" },
                "text": text
            }
        });
        serde_json::from_str(&json.to_string()).expect("valid channel_post update JSON")
    }

    /// Construct an update with a video that carries a caption.
    fn make_video_update_with_caption(update_id: i32, chat_id: i64, caption: &str) -> teloxide_core::types::Update {
        let json = serde_json::json!({
            "update_id": update_id,
            "message": {
                "message_id": 20,
                "date": 1_234_567_890_i64,
                "chat": { "id": chat_id, "type": "private" },
                "from": { "id": 100, "is_bot": false, "first_name": "Test" },
                "video": {
                    "file_id": "vid1",
                    "file_unique_id": "uv1",
                    "width": 1280,
                    "height": 720,
                    "duration": 10,
                    "mime_type": "video/mp4"
                },
                "caption": caption
            }
        });
        serde_json::from_str(&json.to_string()).expect("valid video update JSON")
    }

    /// Construct an update with a video that has no caption.
    fn make_video_update_no_caption(update_id: i32, chat_id: i64) -> teloxide_core::types::Update {
        let json = serde_json::json!({
            "update_id": update_id,
            "message": {
                "message_id": 21,
                "date": 1_234_567_890_i64,
                "chat": { "id": chat_id, "type": "private" },
                "from": { "id": 100, "is_bot": false, "first_name": "Test" },
                "video": {
                    "file_id": "vid2",
                    "file_unique_id": "uv2",
                    "width": 1280,
                    "height": 720,
                    "duration": 5,
                    "mime_type": "video/mp4"
                }
            }
        });
        serde_json::from_str(&json.to_string()).expect("valid video-no-caption update JSON")
    }

    /// Construct an update with a voice message that carries a caption.
    fn make_voice_update_with_caption(update_id: i32, chat_id: i64, caption: &str) -> teloxide_core::types::Update {
        let json = serde_json::json!({
            "update_id": update_id,
            "message": {
                "message_id": 30,
                "date": 1_234_567_890_i64,
                "chat": { "id": chat_id, "type": "private" },
                "from": { "id": 100, "is_bot": false, "first_name": "Test" },
                "voice": {
                    "file_id": "voc1",
                    "file_unique_id": "uvc1",
                    "duration": 3,
                    "mime_type": "audio/ogg"
                },
                "caption": caption
            }
        });
        serde_json::from_str(&json.to_string()).expect("valid voice update JSON")
    }

    /// Construct an update with a voice message that has no caption.
    fn make_voice_update_no_caption(update_id: i32, chat_id: i64) -> teloxide_core::types::Update {
        let json = serde_json::json!({
            "update_id": update_id,
            "message": {
                "message_id": 31,
                "date": 1_234_567_890_i64,
                "chat": { "id": chat_id, "type": "private" },
                "from": { "id": 100, "is_bot": false, "first_name": "Test" },
                "voice": {
                    "file_id": "voc2",
                    "file_unique_id": "uvc2",
                    "duration": 2,
                    "mime_type": "audio/ogg"
                }
            }
        });
        serde_json::from_str(&json.to_string()).expect("valid voice-no-caption update JSON")
    }

    /// Construct an update with an audio file that carries a caption.
    fn make_audio_update_with_caption(update_id: i32, chat_id: i64, caption: &str) -> teloxide_core::types::Update {
        let json = serde_json::json!({
            "update_id": update_id,
            "message": {
                "message_id": 40,
                "date": 1_234_567_890_i64,
                "chat": { "id": chat_id, "type": "private" },
                "from": { "id": 100, "is_bot": false, "first_name": "Test" },
                "audio": {
                    "file_id": "aud1",
                    "file_unique_id": "ua1",
                    "duration": 120,
                    "mime_type": "audio/mpeg"
                },
                "caption": caption
            }
        });
        serde_json::from_str(&json.to_string()).expect("valid audio update JSON")
    }

    /// Construct an update with an audio file that has no caption.
    fn make_audio_update_no_caption(update_id: i32, chat_id: i64) -> teloxide_core::types::Update {
        let json = serde_json::json!({
            "update_id": update_id,
            "message": {
                "message_id": 41,
                "date": 1_234_567_890_i64,
                "chat": { "id": chat_id, "type": "private" },
                "from": { "id": 100, "is_bot": false, "first_name": "Test" },
                "audio": {
                    "file_id": "aud2",
                    "file_unique_id": "ua2",
                    "duration": 60,
                    "mime_type": "audio/mpeg"
                }
            }
        });
        serde_json::from_str(&json.to_string()).expect("valid audio-no-caption update JSON")
    }

    /// Construct an update with an animation (GIF) that carries a caption.
    fn make_animation_update_with_caption(update_id: i32, chat_id: i64, caption: &str) -> teloxide_core::types::Update {
        let json = serde_json::json!({
            "update_id": update_id,
            "message": {
                "message_id": 50,
                "date": 1_234_567_890_i64,
                "chat": { "id": chat_id, "type": "private" },
                "from": { "id": 100, "is_bot": false, "first_name": "Test" },
                "animation": {
                    "file_id": "anim1",
                    "file_unique_id": "uan1",
                    "width": 320,
                    "height": 240,
                    "duration": 4,
                    "mime_type": "video/mp4"
                },
                "document": {
                    "file_id": "anim1",
                    "file_unique_id": "uan1",
                    "mime_type": "video/mp4"
                },
                "caption": caption
            }
        });
        serde_json::from_str(&json.to_string()).expect("valid animation update JSON")
    }

    /// Construct an update with an animation (GIF) that has no caption.
    fn make_animation_update_no_caption(update_id: i32, chat_id: i64) -> teloxide_core::types::Update {
        let json = serde_json::json!({
            "update_id": update_id,
            "message": {
                "message_id": 51,
                "date": 1_234_567_890_i64,
                "chat": { "id": chat_id, "type": "private" },
                "from": { "id": 100, "is_bot": false, "first_name": "Test" },
                "animation": {
                    "file_id": "anim2",
                    "file_unique_id": "uan2",
                    "width": 320,
                    "height": 240,
                    "duration": 2,
                    "mime_type": "video/mp4"
                },
                "document": {
                    "file_id": "anim2",
                    "file_unique_id": "uan2",
                    "mime_type": "video/mp4"
                }
            }
        });
        serde_json::from_str(&json.to_string()).expect("valid animation-no-caption update JSON")
    }

    const EXPECTED_CHAT_ID: i64 = 12345;
    const BOT_ID: &str = "my_bot";
    const POLL_INTERVAL: u64 = 30;

    // ── normalize_update tests ────────────────────────────────────────────────

    #[test]
    fn normalize_text_message_returns_body() {
        let update = make_text_update(1, EXPECTED_CHAT_ID, "hello world");
        let result = normalize_update(&update, EXPECTED_CHAT_ID, BOT_ID, POLL_INTERVAL);
        assert!(result.is_some(), "expected Some for matching chat text message");
        let msg = result.unwrap();
        assert_eq!(msg.body, "hello world");
        assert_eq!(msg.metadata["channel"], "telegram");
    }

    #[test]
    fn normalize_wrong_chat_drops_message() {
        // Update belongs to chat 99999, but we filter for EXPECTED_CHAT_ID.
        let update = make_text_update_wrong_chat(2, "should be dropped");
        let result = normalize_update(&update, EXPECTED_CHAT_ID, BOT_ID, POLL_INTERVAL);
        assert!(result.is_none(), "expected None for wrong chat ID");
    }

    #[test]
    fn normalize_photo_with_caption_returns_caption() {
        let update = make_photo_update_with_caption(3, EXPECTED_CHAT_ID, "photo caption");
        let result = normalize_update(&update, EXPECTED_CHAT_ID, BOT_ID, POLL_INTERVAL);
        assert!(result.is_some(), "expected Some for photo with caption");
        assert_eq!(result.unwrap().body, "photo caption");
    }

    #[test]
    fn normalize_sticker_drops_attachment_only() {
        // Sticker has no text or caption — should be filtered out.
        let update = make_sticker_update(4, EXPECTED_CHAT_ID);
        let result = normalize_update(&update, EXPECTED_CHAT_ID, BOT_ID, POLL_INTERVAL);
        assert!(result.is_none(), "expected None for attachment-only sticker");
    }

    #[test]
    fn normalize_channel_post_returns_body() {
        // channel_post updates are also accepted (UpdateKind::ChannelPost).
        let update = make_channel_post_update(5, EXPECTED_CHAT_ID, "channel text");
        let result = normalize_update(&update, EXPECTED_CHAT_ID, BOT_ID, POLL_INTERVAL);
        assert!(result.is_some(), "expected Some for channel_post");
        assert_eq!(result.unwrap().body, "channel text");
    }

    #[test]
    fn normalize_empty_text_drops_message() {
        // Empty text string should be treated as no body.
        let update = make_text_update(6, EXPECTED_CHAT_ID, "");
        let result = normalize_update(&update, EXPECTED_CHAT_ID, BOT_ID, POLL_INTERVAL);
        assert!(result.is_none(), "expected None for empty text body");
    }

    // ── extract_body tests ────────────────────────────────────────────────────

    #[test]
    fn extract_body_text_message() {
        let update = make_text_update(10, EXPECTED_CHAT_ID, "test body");
        if let teloxide_core::types::UpdateKind::Message(msg) = &update.kind {
            let body = extract_body(msg);
            assert_eq!(body, Some("test body".to_string()));
        } else {
            panic!("expected Message update kind");
        }
    }

    #[test]
    fn extract_body_photo_with_caption() {
        let update = make_photo_update_with_caption(11, EXPECTED_CHAT_ID, "my caption");
        if let teloxide_core::types::UpdateKind::Message(msg) = &update.kind {
            let body = extract_body(msg);
            assert_eq!(body, Some("my caption".to_string()));
        } else {
            panic!("expected Message update kind");
        }
    }

    #[test]
    fn extract_body_sticker_returns_none() {
        let update = make_sticker_update(12, EXPECTED_CHAT_ID);
        if let teloxide_core::types::UpdateKind::Message(msg) = &update.kind {
            let body = extract_body(msg);
            assert!(body.is_none(), "sticker should yield no body");
        } else {
            panic!("expected Message update kind");
        }
    }

    // ── extract_body: video/voice/audio/animation caption coverage ────────────

    #[test]
    fn extract_body_video_with_caption_returns_caption() {
        let update = make_video_update_with_caption(20, EXPECTED_CHAT_ID, "video caption");
        if let teloxide_core::types::UpdateKind::Message(msg) = &update.kind {
            let body = extract_body(msg);
            assert_eq!(
                body,
                Some("video caption".to_string()),
                "video with caption should return caption"
            );
        } else {
            panic!("expected Message update kind");
        }
    }

    #[test]
    fn extract_body_video_without_caption_returns_none() {
        let update = make_video_update_no_caption(21, EXPECTED_CHAT_ID);
        if let teloxide_core::types::UpdateKind::Message(msg) = &update.kind {
            let body = extract_body(msg);
            assert!(body.is_none(), "video without caption should yield no body");
        } else {
            panic!("expected Message update kind");
        }
    }

    #[test]
    fn extract_body_voice_with_caption_returns_caption() {
        let update = make_voice_update_with_caption(30, EXPECTED_CHAT_ID, "voice caption");
        if let teloxide_core::types::UpdateKind::Message(msg) = &update.kind {
            let body = extract_body(msg);
            assert_eq!(
                body,
                Some("voice caption".to_string()),
                "voice with caption should return caption"
            );
        } else {
            panic!("expected Message update kind");
        }
    }

    #[test]
    fn extract_body_voice_without_caption_returns_none() {
        let update = make_voice_update_no_caption(31, EXPECTED_CHAT_ID);
        if let teloxide_core::types::UpdateKind::Message(msg) = &update.kind {
            let body = extract_body(msg);
            assert!(body.is_none(), "voice without caption should yield no body");
        } else {
            panic!("expected Message update kind");
        }
    }

    #[test]
    fn extract_body_audio_with_caption_returns_caption() {
        let update = make_audio_update_with_caption(40, EXPECTED_CHAT_ID, "audio caption");
        if let teloxide_core::types::UpdateKind::Message(msg) = &update.kind {
            let body = extract_body(msg);
            assert_eq!(
                body,
                Some("audio caption".to_string()),
                "audio with caption should return caption"
            );
        } else {
            panic!("expected Message update kind");
        }
    }

    #[test]
    fn extract_body_audio_without_caption_returns_none() {
        let update = make_audio_update_no_caption(41, EXPECTED_CHAT_ID);
        if let teloxide_core::types::UpdateKind::Message(msg) = &update.kind {
            let body = extract_body(msg);
            assert!(body.is_none(), "audio without caption should yield no body");
        } else {
            panic!("expected Message update kind");
        }
    }

    #[test]
    fn extract_body_animation_with_caption_returns_caption() {
        let update = make_animation_update_with_caption(50, EXPECTED_CHAT_ID, "animation caption");
        if let teloxide_core::types::UpdateKind::Message(msg) = &update.kind {
            let body = extract_body(msg);
            assert_eq!(
                body,
                Some("animation caption".to_string()),
                "animation with caption should return caption"
            );
        } else {
            panic!("expected Message update kind");
        }
    }

    #[test]
    fn extract_body_animation_without_caption_returns_none() {
        let update = make_animation_update_no_caption(51, EXPECTED_CHAT_ID);
        if let teloxide_core::types::UpdateKind::Message(msg) = &update.kind {
            let body = extract_body(msg);
            assert!(body.is_none(), "animation without caption should yield no body");
        } else {
            panic!("expected Message update kind");
        }
    }
}

mod helpers {
    use std::sync::Arc;

    use serde_json::json;
    use teloxide_core::errors::{ApiError, RequestError};
    use teloxide_core::types::{CallbackQuery, ChatId, Seconds};
    use uuid::Uuid;

    use aves::utils::uuid::SpectaUuid;

    use crate::types::{InteractionAction, MessagingSendError, MessagingSendErrorCode, PermissionButtonPayload};

    use super::super::helpers::{
        callback_interaction, classify_send_error, parse_chat_id, permission_keyboard, validate_send_metadata,
    };

    #[test]
    fn permission_keyboard_carries_decodable_approve_and_deny_payloads() {
        let request_id = SpectaUuid(Uuid::nil());
        let keyboard = serde_json::to_value(permission_keyboard(request_id)).unwrap();
        let buttons = &keyboard["inline_keyboard"][0];
        assert_eq!(buttons[0]["text"], "Approve");
        assert_eq!(buttons[1]["text"], "Deny");
        for (button, expected) in [(0, InteractionAction::Approve), (1, InteractionAction::Deny)] {
            let encoded = buttons[button]["callback_data"].as_str().unwrap();
            assert_eq!(PermissionButtonPayload::decode(encoded), Some((request_id, expected)));
        }
    }

    #[test]
    fn callback_only_admits_valid_permission_payloads() {
        let request_id = SpectaUuid(Uuid::nil());
        let payload = PermissionButtonPayload::encode(request_id, InteractionAction::Deny);
        let callback = |data: Option<&str>| -> CallbackQuery {
            serde_json::from_value(json!({
                "id": "callback",
                "from": { "id": 100, "is_bot": false, "first_name": "Test" },
                "chat_instance": "chat",
                "data": data,
            }))
            .unwrap()
        };
        let result = callback_interaction(&callback(Some(&payload)), "raw".to_owned()).unwrap();
        assert_eq!(result.request_id, request_id);
        assert_eq!(result.action, InteractionAction::Deny);
        assert_eq!(result.raw_payload, "raw");
        assert!(callback_interaction(&callback(None), String::new()).is_none());
        assert!(callback_interaction(&callback(Some("bad")), String::new()).is_none());
    }

    #[test]
    fn outbound_metadata_and_chat_id_are_checked_before_sending() {
        assert!(validate_send_metadata(&json!({})).is_ok());
        assert!(matches!(
            validate_send_metadata(&json!({ "unexpected": true })),
            Err(MessagingSendError::Terminal {
                code: MessagingSendErrorCode::Validation,
                ..
            })
        ));
        assert_eq!(parse_chat_id("-100123").unwrap().0, -100123);
        assert!(matches!(
            parse_chat_id("not-a-chat"),
            Err(MessagingSendError::Terminal {
                code: MessagingSendErrorCode::InvalidDestination,
                ..
            })
        ));
    }

    #[test]
    fn telegram_api_error_categories_preserve_retryability() {
        let terminal = [
            (ApiError::InvalidToken, MessagingSendErrorCode::InvalidCredentials),
            (ApiError::ChatNotFound, MessagingSendErrorCode::PermissionDenied),
            (ApiError::MessageIsTooLong, MessagingSendErrorCode::Validation),
        ];
        for (api_error, expected) in terminal {
            assert!(matches!(
                classify_send_error(RequestError::Api(api_error)),
                MessagingSendError::Terminal { code, .. } if code == expected
            ));
        }
        assert!(matches!(
            classify_send_error(RequestError::Api(ApiError::UnknownHost)),
            MessagingSendError::Retryable {
                code: MessagingSendErrorCode::Server,
                ..
            }
        ));
    }

    #[test]
    fn rate_limits_and_migrated_chat_ids_keep_their_typed_outcomes() {
        assert!(matches!(
            classify_send_error(RequestError::RetryAfter(Seconds::from_seconds(7))),
            MessagingSendError::Retryable {
                code: MessagingSendErrorCode::RateLimited,
                retry_after_seconds: Some(7),
                ..
            }
        ));
        assert!(matches!(
            classify_send_error(RequestError::MigrateToChatId(ChatId(-100123))),
            MessagingSendError::Terminal {
                code: MessagingSendErrorCode::InvalidDestination,
                ..
            }
        ));
    }

    #[test]
    fn transport_and_unclassified_api_failures_preserve_their_categories() {
        let transport = RequestError::Io(Arc::new(std::io::Error::other("connection closed")));
        assert!(matches!(
            classify_send_error(transport),
            MessagingSendError::Retryable {
                code: MessagingSendErrorCode::Transport,
                ..
            }
        ));
        assert!(matches!(
            classify_send_error(RequestError::Api(ApiError::MessageCantBeEdited)),
            MessagingSendError::Terminal {
                code: MessagingSendErrorCode::InvalidDestination,
                ..
            }
        ));
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
        EmailSenderConfig, MessagingError, MessagingSendError, MessagingSendErrorCode, MessagingSenderConfig,
        MessagingSubscriptionError, NormalizedMessage, TelegramSenderConfig,
    };

    use super::super::TelegramPlugin;

    fn sender() -> MessagingSenderConfig {
        MessagingSenderConfig::Telegram(TelegramSenderConfig {
            connection_id: SpectaUuid(Uuid::nil()),
            chat_id: "123".to_owned(),
            persistence: Default::default(),
        })
    }

    #[tokio::test]
    async fn send_requires_a_connected_telegram_plugin() {
        let mut plugin = TelegramPlugin::default();
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
    async fn listen_requires_a_connected_telegram_plugin() {
        let mut plugin = TelegramPlugin::default();
        let (tx, _) = mpsc::channel(1);
        let (interaction_tx, _) = mpsc::channel(1);
        assert!(matches!(
            plugin.listen(tx, interaction_tx).await,
            Err(MessagingSubscriptionError::Configuration {
                listener_type: ListenerType::Telegram,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn telegram_rejects_another_provider_sender_for_all_send_operations() {
        let mut plugin = TelegramPlugin::default();
        let sender = MessagingSenderConfig::Email(EmailSenderConfig {
            connection_id: SpectaUuid(Uuid::nil()),
            from: "sender@example.com".to_owned(),
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

mod send {
    use serde_json::json;
    use teloxide_core::Bot;
    use uuid::Uuid;

    use aves::utils::uuid::SpectaUuid;

    use crate::types::{MessagingSendError, MessagingSendErrorCode, NormalizedMessage, TelegramSenderConfig};

    use super::super::send::{edit_interactive, send, send_interactive, test_finalized_text};

    #[test]
    fn final_edit_preserves_original_request_and_outcome() {
        assert_eq!(test_finalized_text("Approve?", "Denied"), "Approve?\n\nDenied");
    }

    #[tokio::test]
    async fn send_rejects_invalid_inputs_without_a_telegram_request() {
        let bot = Bot::new("123:test");
        let mut sender = TelegramSenderConfig {
            connection_id: SpectaUuid(Uuid::nil()),
            chat_id: "123".to_owned(),
            persistence: Default::default(),
        };
        let mut message = NormalizedMessage {
            body: "hello".to_owned(),
            metadata: json!({ "unexpected": true }),
        };
        assert!(matches!(
            send(&bot, &sender, &message).await,
            Err(MessagingSendError::Terminal {
                code: MessagingSendErrorCode::Validation,
                ..
            })
        ));
        message.metadata = json!({});
        sender.chat_id = "invalid".to_owned();
        assert!(matches!(
            send(&bot, &sender, &message).await,
            Err(MessagingSendError::Terminal {
                code: MessagingSendErrorCode::InvalidDestination,
                ..
            })
        ));
        assert!(matches!(
            send_interactive(&bot, &sender, SpectaUuid(Uuid::nil()), "Approve?").await,
            Err(MessagingSendError::Terminal {
                code: MessagingSendErrorCode::InvalidDestination,
                ..
            })
        ));
        sender.chat_id = "123".to_owned();
        assert!(matches!(
            edit_interactive(&bot, &sender, "invalid", "Approve?", "Denied").await,
            Err(MessagingSendError::Terminal {
                code: MessagingSendErrorCode::Validation,
                ..
            })
        ));
    }
}

mod subscribe {
    use super::*;

    #[test]
    fn polling_projection_advances_offset_and_preserves_raw_and_normalized_message() {
        let payload = serde_json::json!({
            "update_id": 41,
            "message": {
                "message_id": 1,
                "date": 1_234_567_890_i64,
                "chat": { "id": -100123, "type": "channel" },
                "text": "hello"
            }
        });
        let update: teloxide_core::types::Update = serde_json::from_str(&payload.to_string()).unwrap();
        let (offset, event) = super::super::subscribe::test_project_inbound_update(&update).unwrap();
        assert_eq!(offset, 42);
        assert!(
            String::from_utf8(event.raw_payload)
                .unwrap()
                .contains("update_id")
        );
        let message = event.normalized.unwrap();
        assert_eq!(message.body, "hello");
        assert_eq!(message.metadata["chat_id"], "-100123");
    }

    #[test]
    fn callback_projection_preserves_serialized_payload_and_decision() {
        let request_id = aves::utils::uuid::SpectaUuid(uuid::Uuid::nil());
        let callback: teloxide_core::types::CallbackQuery = serde_json::from_value(serde_json::json!({
            "id": "callback",
            "from": { "id": 100, "is_bot": false, "first_name": "Test" },
            "chat_instance": "chat",
            "data": crate::types::PermissionButtonPayload::encode(request_id, crate::types::InteractionAction::Approve),
        }))
        .unwrap();
        let interaction = super::super::subscribe::test_project_callback(&callback)
            .unwrap()
            .unwrap();
        assert_eq!(interaction.request_id, request_id);
        assert_eq!(interaction.action, crate::types::InteractionAction::Approve);
        assert!(interaction.raw_payload.contains("callback"));
    }

    #[tokio::test]
    async fn subscription_checks_explicit_chat_ids_without_opening_a_connection() {
        for channels in [Some(Vec::new()), Some(vec!["not-a-chat-id".to_owned()])] {
            let (_, rx) = mpsc::channel::<NormalizedMessage>(1);
            let (tx, _) = mpsc::channel(1);
            assert!(
                run_filters(
                    MessagingSubscription {
                        subscribed_channels: channels,
                        allowed_senders: Vec::new()
                    },
                    rx,
                    tx
                )
                .is_err()
            );
        }
        for channels in [None, Some(vec!["-100123456789".to_owned()])] {
            let (_, rx) = mpsc::channel::<NormalizedMessage>(1);
            let (tx, _) = mpsc::channel(1);
            let task = run_filters(
                MessagingSubscription {
                    subscribed_channels: channels,
                    allowed_senders: Vec::new(),
                },
                rx,
                tx,
            )
            .unwrap();
            task.abort();
        }
    }

    #[tokio::test]
    async fn filter_task_routes_matching_chat_and_username_without_telegram() {
        use serde_json::json;

        let (input_tx, input_rx) = mpsc::channel(4);
        let (output_tx, mut output_rx) = mpsc::channel(4);
        let task = run_filters(
            MessagingSubscription {
                subscribed_channels: Some(vec!["-100123456789".to_owned()]),
                allowed_senders: vec!["@Alice".to_owned()],
            },
            input_rx,
            output_tx,
        )
        .unwrap();

        for (chat_id, username) in [
            ("-100123456789", Some("alice")),
            ("-100123456789", None),
            ("123", Some("alice")),
        ] {
            input_tx
                .send(NormalizedMessage {
                    body: "message".to_owned(),
                    metadata: json!({ "chat_id": chat_id, "sender_username": username }),
                })
                .await
                .unwrap();
        }
        drop(input_tx);
        task.await.unwrap().unwrap();

        assert_eq!(output_rx.try_recv().unwrap().body, "message");
        assert!(output_rx.try_recv().is_err());
    }
}
