use std::fs;

use tempfile::{TempDir, tempdir};

use super::claude::CLAUDE_CREDENTIAL;
use super::grok::GROK_CREDENTIAL;
use super::*;

const FUTURE_MS: i64 = 4_102_444_800_000;

fn home_with(claude: Option<&str>, grok: Option<&str>) -> (TempDir, CliCredentials) {
    let home = tempdir().unwrap();
    if let Some(body) = claude {
        fs::create_dir_all(home.path().join(".claude")).unwrap();
        fs::write(home.path().join(".claude/.credentials.json"), body).unwrap();
    }
    if let Some(body) = grok {
        fs::create_dir_all(home.path().join(".grok")).unwrap();
        fs::write(home.path().join(".grok/auth.json"), body).unwrap();
    }
    let credentials = CliCredentials {
        home: home.path().to_path_buf(),
    };
    (home, credentials)
}

fn claude_file(expires_at: i64) -> String {
    serde_json::json!({ "claudeAiOauth": { "accessToken": "claude-token", "expiresAt": expires_at } }).to_string()
}

fn grok_file(expires_at: &str) -> String {
    serde_json::json!({
        "https://auth.x.ai::client": {
            "key": "grok-token",
            "expires_at": expires_at,
            "user_id": "grok-user",
            "email": "user@example.com"
        }
    })
    .to_string()
}

#[tokio::test]
async fn serves_valid_claude_token() {
    let (_home, credentials) = home_with(Some(&claude_file(FUTURE_MS)), None);

    let credential = credentials
        .get_scoped(CLAUDE_CREDENTIAL, 120_000)
        .await
        .unwrap();

    assert_eq!(credential.payload, b"claude-token");
    assert_eq!(credential.expires_at_ms, Some(FUTURE_MS));
}

#[tokio::test]
async fn serves_labelled_grok_token() {
    let (_home, credentials) = home_with(None, Some(&grok_file("2100-01-01T00:00:00.5Z")));

    let credential = credentials
        .get_scoped(GROK_CREDENTIAL, 120_000)
        .await
        .unwrap();

    assert_eq!(credential.payload, b"grok-token");
    assert_eq!(credential.account_id.as_deref(), Some("grok-user"));
    assert_eq!(credential.email.as_deref(), Some("user@example.com"));
}

#[tokio::test]
async fn expired_login_requires_authentication_and_lists_as_needs_reauth() {
    let (_home, credentials) = home_with(Some(&claude_file(1)), Some(&grok_file("2020-01-01T00:00:00Z")));

    let claude = credentials.get_scoped(CLAUDE_CREDENTIAL, 120_000).await;
    let grok = credentials.get_scoped(GROK_CREDENTIAL, 120_000).await;
    let snapshot = credentials.list_scoped().await.unwrap();

    assert!(matches!(claude, Err(VaultGetError::AuthRequired)));
    assert!(matches!(grok, Err(VaultGetError::AuthRequired)));
    assert!(snapshot.rows.iter().all(|row| row.state == "needs_reauth"));
}

#[tokio::test]
async fn token_inside_requested_lifetime_floor_is_refused() {
    let (_home, credentials) = home_with(Some(&claude_file(now_ms() + 60_000)), None);

    let result = credentials.get_scoped(CLAUDE_CREDENTIAL, 120_000).await;

    assert!(matches!(result, Err(VaultGetError::AuthRequired)));
}

#[tokio::test]
async fn missing_and_malformed_files_are_not_listed() {
    let (_home, credentials) = home_with(Some("{not json"), None);

    let snapshot = credentials.list_scoped().await.unwrap();
    let result = credentials.get_scoped(GROK_CREDENTIAL, 0).await;

    assert_eq!(snapshot.grants, 1);
    assert!(snapshot.rows.is_empty());
    assert!(matches!(result, Err(VaultGetError::NotFound)));
}

#[tokio::test]
async fn lists_both_valid_logins_under_insula_families() {
    let (_home, credentials) = home_with(Some(&claude_file(FUTURE_MS)), Some(&grok_file("2100-01-01T00:00:00Z")));

    let snapshot = credentials.list_scoped().await.unwrap();
    let ids: Vec<_> = snapshot
        .rows
        .iter()
        .map(|row| row.credential_id.as_str())
        .collect();

    assert_eq!(ids, [CLAUDE_CREDENTIAL, GROK_CREDENTIAL]);
    assert!(snapshot.rows.iter().all(|row| row.state == "ready"));
}
