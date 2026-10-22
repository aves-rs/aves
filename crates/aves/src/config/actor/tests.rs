use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::json;
use tempfile::tempdir;
use tokio::sync::mpsc;

use super::super::messages::{ConfigBroadcast, ConfigRequestPayload, ConfigResponse};
use super::super::types::{ConfigError, SectionRegistration};
use super::Actor;

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct MockSettings {
    enabled: bool,
    nested: MockNested,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct MockNested {
    limit: u32,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct MockMapSettings {
    entries: HashMap<String, String>,
}

async fn actor(dir: &std::path::Path) -> (Actor, mpsc::Receiver<ConfigBroadcast>) {
    let (_requests_tx, requests_rx) = mpsc::channel(8);
    let (broadcast_tx, broadcast_rx) = mpsc::channel(8);
    let mut actor = Actor::new(dir.to_path_buf(), requests_rx, broadcast_tx).expect("open config");
    actor
        .handle_message(ConfigRequestPayload::RegisterSection {
            registration: SectionRegistration::new::<MockSettings>("mock").expect("registration"),
        })
        .await
        .expect("unique section");
    (actor, broadcast_rx)
}

#[tokio::test]
async fn load_does_not_rewrite_and_missing_sections_return_defaults() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "[unknown]\nvalue = 1\n").expect("fixture");
    let original = std::fs::read(&path).expect("fixture bytes");
    let (mut actor, _events) = actor(dir.path()).await;
    assert!(matches!(
        actor.handle_message(ConfigRequestPayload::GetConfigSection { section: "mock".into() }).await.expect("read"),
        ConfigResponse::ConfigSection { value } if value == json!({"enabled": false, "nested": {"limit": 0}})
    ));
    assert_eq!(std::fs::read(&path).expect("after boot"), original);
    actor
        .handle_message(ConfigRequestPayload::SealRegistration)
        .await
        .expect("seal");
    actor
        .handle_message(ConfigRequestPayload::SaveConfigSection {
            section: "mock".into(),
            value: json!({"nested": {"limit": 9}}),
        })
        .await
        .expect("save");
    assert!(
        !std::fs::read_to_string(path)
            .expect("saved")
            .contains("unknown")
    );
}

#[tokio::test]
async fn patch_merges_validates_broadcasts_and_survives_reload() {
    let dir = tempdir().expect("tempdir");
    let (mut config_actor, mut events) = actor(dir.path()).await;
    config_actor
        .handle_message(ConfigRequestPayload::SealRegistration)
        .await
        .expect("seal");
    config_actor
        .handle_message(ConfigRequestPayload::SaveConfigSection {
            section: "mock".into(),
            value: json!({"nested": {"limit": 7}}),
        })
        .await
        .expect("patch");
    assert!(
        matches!(events.try_recv(), Ok(ConfigBroadcast::ConfigChanged { section, value }) if section == "mock" && value == json!({"enabled": false, "nested": {"limit": 7}}))
    );
    config_actor
        .handle_message(ConfigRequestPayload::SaveConfigSection {
            section: "mock".into(),
            value: json!({"enabled": true}),
        })
        .await
        .expect("second patch");
    let (mut restored, _) = actor(dir.path()).await;
    assert!(matches!(
        restored.handle_message(ConfigRequestPayload::GetConfigSection { section: "mock".into() }).await.expect("reload"),
        ConfigResponse::ConfigSection { value } if value == json!({"enabled": true, "nested": {"limit": 7}})
    ));
}

#[tokio::test]
async fn saving_a_map_field_replaces_it_so_deleted_entries_stay_deleted() {
    let dir = tempdir().expect("tempdir");
    let (mut actor, _) = actor(dir.path()).await;
    actor
        .handle_message(ConfigRequestPayload::RegisterSection {
            registration: SectionRegistration::new::<MockMapSettings>("maps").expect("registration"),
        })
        .await
        .expect("register map owner");
    actor
        .handle_message(ConfigRequestPayload::SealRegistration)
        .await
        .expect("seal");
    for entries in [json!({"one": "1", "two": "2"}), json!({"two": "2"})] {
        actor
            .handle_message(ConfigRequestPayload::SaveConfigSection {
                section: "maps".into(),
                value: json!({"entries": entries}),
            })
            .await
            .expect("save map");
    }
    assert!(matches!(
        actor.handle_message(ConfigRequestPayload::GetConfigSection { section: "maps".into() }).await.expect("read"),
        ConfigResponse::ConfigSection { value } if value == json!({"entries": {"two": "2"}})
    ));
}

#[tokio::test]
async fn rejects_unknown_sections_and_invalid_types_without_writing() {
    let dir = tempdir().expect("tempdir");
    let (mut actor, _events) = actor(dir.path()).await;
    let path = dir.path().join("config.toml");
    assert!(
        actor
            .handle_message(ConfigRequestPayload::SaveConfigSection {
                section: "mock".into(),
                value: json!({"enabled": true}),
            })
            .await
            .is_err()
    );
    assert!(!path.exists());
    actor
        .handle_message(ConfigRequestPayload::SealRegistration)
        .await
        .expect("seal");
    for (section, value) in [
        ("other", json!({"enabled": true})),
        ("mock", json!({"nested": {"limit": "bad"}})),
    ] {
        assert!(
            actor
                .handle_message(ConfigRequestPayload::SaveConfigSection {
                    section: section.into(),
                    value,
                })
                .await
                .is_err()
        );
        assert!(!path.exists());
    }
}

#[tokio::test]
async fn owner_controls_unknown_fields_within_its_section() {
    let dir = tempdir().expect("tempdir");
    let (mut actor, _events) = actor(dir.path()).await;
    actor
        .handle_message(ConfigRequestPayload::SealRegistration)
        .await
        .expect("seal");
    actor
        .handle_message(ConfigRequestPayload::SaveConfigSection {
            section: "mock".into(),
            value: json!({"nested": {"extra": 2}}),
        })
        .await
        .expect("owner accepts unknown field");
    assert!(matches!(
        actor.handle_message(ConfigRequestPayload::GetConfigSection { section: "mock".into() }).await.expect("read"),
        ConfigResponse::ConfigSection { value } if value == json!({"enabled": false, "nested": {"limit": 0}})
    ));
    assert!(!dir.path().join("config.toml").exists());
}

#[tokio::test]
async fn registered_owner_hook_runs_only_for_changed_values() {
    let dir = tempdir().expect("tempdir");
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = calls.clone();
    let (_tx, rx) = mpsc::channel(4);
    let (broadcast_tx, mut broadcasts) = mpsc::channel(4);
    let mut actor = Actor::new(dir.path().to_path_buf(), rx, broadcast_tx).expect("actor");
    actor
        .handle_message(ConfigRequestPayload::RegisterSection {
            registration: SectionRegistration::new::<MockSettings>("mock")
                .expect("registration")
                .on_change::<MockSettings, _, _>(move |_before, _after| {
                    let observed = observed.clone();
                    async move {
                        observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(())
                    }
                }),
        })
        .await
        .expect("unique section");
    actor
        .handle_message(ConfigRequestPayload::SealRegistration)
        .await
        .expect("seal");
    for _ in 0..2 {
        actor
            .handle_message(ConfigRequestPayload::SaveConfigSection {
                section: "mock".into(),
                value: json!({"enabled": true}),
            })
            .await
            .expect("patch");
    }
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(broadcasts.try_recv().is_ok());
    assert!(broadcasts.try_recv().is_err());
}

#[tokio::test]
async fn owner_hook_failures_keep_their_write_boundary() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let dir = tempdir().expect("tempdir");
    let reject = Arc::new(AtomicBool::new(true));
    let guard = Arc::clone(&reject);
    let (_tx, requests) = mpsc::channel(4);
    let (broadcasts, mut received) = mpsc::channel(4);
    let mut actor = Actor::new(dir.path().to_path_buf(), requests, broadcasts).expect("actor");
    let registration = SectionRegistration::new::<MockSettings>("mock")
        .expect("registration")
        .before_write::<MockSettings, _, _>(move |_, _| {
            let reject = Arc::clone(&guard);
            async move {
                if reject.load(Ordering::SeqCst) {
                    return Err(ConfigError::OwnerUpdate {
                        section: "mock".to_owned(),
                        reason: "rejected".to_owned(),
                        code: None,
                    });
                }
                Ok(())
            }
        })
        .on_change::<MockSettings, _, _>(|_, _| async {
            Err(ConfigError::OwnerUpdate {
                section: "mock".to_owned(),
                reason: "update failed".to_owned(),
                code: None,
            })
        });
    actor
        .handle_message(ConfigRequestPayload::RegisterSection { registration })
        .await
        .expect("register");
    actor
        .handle_message(ConfigRequestPayload::SealRegistration)
        .await
        .expect("seal");
    let write = || ConfigRequestPayload::SaveConfigSection {
        section: "mock".to_owned(),
        value: json!({"enabled": true}),
    };

    assert!(actor.handle_message(write()).await.is_err());
    assert!(!dir.path().join("config.toml").exists());
    assert!(received.try_recv().is_err());

    reject.store(false, Ordering::SeqCst);
    assert!(actor.handle_message(write()).await.is_err());
    assert!(dir.path().join("config.toml").exists());
    assert!(matches!(received.try_recv(), Ok(ConfigBroadcast::ConfigChanged { section, .. }) if section == "mock"));
}

#[tokio::test]
async fn registration_closes_before_first_write() {
    let dir = tempdir().expect("tempdir");
    let (mut actor, _) = actor(dir.path()).await;
    assert!(
        actor
            .handle_message(ConfigRequestPayload::SaveConfigSection {
                section: "mock".into(),
                value: json!({"enabled": true})
            })
            .await
            .is_err()
    );
    actor
        .handle_message(ConfigRequestPayload::SealRegistration)
        .await
        .expect("seal");
    assert!(
        actor
            .handle_message(ConfigRequestPayload::RegisterSection {
                registration: SectionRegistration::new::<MockSettings>("late").expect("registration")
            })
            .await
            .is_err()
    );
    assert!(!dir.path().join("config.toml").exists());
}
