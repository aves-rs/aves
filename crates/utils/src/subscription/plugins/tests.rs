use std::fs;

use aes::Aes128;
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use cbc::cipher::block_padding::Pkcs7;
use cbc::cipher::{BlockModeEncrypt, KeyIvInit};
use tempfile::{TempDir, tempdir};

use super::claude::CLAUDE_CREDENTIAL;
use super::cursor::{CURSOR_CREDENTIAL, V10_IV, V10_KEY, V10_PREFIX};
use super::grok::GROK_CREDENTIAL;
use super::ollama::OLLAMA_CREDENTIAL;
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
        ollama: OllamaPlugin::new(|| None),
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
async fn expired_login_requires_authentication_and_still_lists_as_ready() {
    let (_home, credentials) = home_with(Some(&claude_file(1)), Some(&grok_file("2020-01-01T00:00:00Z")));

    let claude = credentials.get_scoped(CLAUDE_CREDENTIAL, 120_000).await;
    let grok = credentials.get_scoped(GROK_CREDENTIAL, 120_000).await;
    let snapshot = credentials.list_scoped().await.unwrap();

    assert!(matches!(claude, Err(VaultGetError::AuthRequired)));
    assert!(matches!(grok, Err(VaultGetError::AuthRequired)));
    assert_eq!(snapshot.rows.len(), 2);
    assert!(snapshot.rows.iter().all(|row| row.state == "ready"));
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
async fn exposes_ollama_key_as_a_scoped_api_credential() {
    let (_home, mut credentials) = home_with(None, None);
    credentials.ollama = OllamaPlugin::new(|| {
        Some(CliLogin {
            token: "fixture-key".to_owned(),
            expires_at_ms: None,
            record_version: 7,
            account_id: None,
            email: None,
        })
    });

    let snapshot = credentials.list_scoped().await.unwrap();
    let [row] = snapshot.rows.as_slice() else {
        panic!("expected only the Ollama credential");
    };
    assert_eq!(row.credential_id, OLLAMA_CREDENTIAL);
    assert_eq!(row.credential_type, "apikey");
    assert_eq!(row.state, "ready");
    let credential = credentials.get_scoped(OLLAMA_CREDENTIAL, 0).await.unwrap();
    assert_eq!(credential.payload.as_slice(), b"fixture-key");
    assert_eq!(credential.record_version, row.record_version);
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

/// Write Grok Bot's secrets file under a fresh home.
fn home_with_grok_bot(body: &str) -> (TempDir, CliCredentials) {
    let home = tempdir().unwrap();
    fs::create_dir_all(home.path().join(".config/Grok Bot")).unwrap();
    fs::write(home.path().join(".config/Grok Bot/sand-secrets.json"), body).unwrap();
    let credentials = CliCredentials {
        home: home.path().to_path_buf(),
        ollama: OllamaPlugin::new(|| None),
    };
    (home, credentials)
}

/// A JWT whose claims carry `sub` and `exp` (seconds); the signature is never checked.
fn cursor_token(expires_at_secs: i64) -> String {
    let claims = serde_json::json!({ "sub": "grok|cursor-user", "exp": expires_at_secs }).to_string();
    format!("header.{}.signature", URL_SAFE_NO_PAD.encode(claims))
}

/// Seal `plain` the way Electron `safeStorage` does on Linux without a keyring.
fn seal_v10(plain: &str) -> String {
    let mut buffer = plain.as_bytes().to_vec();
    let length = buffer.len();
    buffer.resize(length + 16, 0);
    let sealed = cbc::Encryptor::<Aes128>::new(&V10_KEY.into(), &V10_IV.into())
        .encrypt_padded::<Pkcs7>(&mut buffer, length)
        .unwrap();
    STANDARD.encode([V10_PREFIX, sealed].concat())
}

fn cursor_file(sealed_token: &str) -> String {
    let accounts = serde_json::json!({
        "active": "account-a",
        "accounts": { "account-a": { "cursor-access-token": sealed_token } }
    });
    serde_json::json!({ "version": 1, "cursor-accounts": accounts.to_string() }).to_string()
}

#[tokio::test]
async fn serves_decrypted_cursor_token_from_grok_bot() {
    let token = cursor_token(FUTURE_MS / 1000);
    let (_home, credentials) = home_with_grok_bot(&cursor_file(&seal_v10(&token)));

    let credential = credentials
        .get_scoped(CURSOR_CREDENTIAL, 120_000)
        .await
        .unwrap();
    let snapshot = credentials.list_scoped().await.unwrap();

    assert_eq!(credential.payload, token.as_bytes());
    assert_eq!(credential.expires_at_ms, Some(FUTURE_MS));
    assert_eq!(credential.account_id.as_deref(), Some("cursor-user"));
    assert_eq!(snapshot.rows.len(), 1);
    assert_eq!(snapshot.rows[0].credential_id, CURSOR_CREDENTIAL);
    assert_eq!(snapshot.rows[0].state, "ready");
}

#[tokio::test]
async fn expired_cursor_token_requires_authentication() {
    let (_home, credentials) = home_with_grok_bot(&cursor_file(&seal_v10(&cursor_token(1))));

    let result = credentials.get_scoped(CURSOR_CREDENTIAL, 120_000).await;

    assert!(matches!(result, Err(VaultGetError::AuthRequired)));
}

#[tokio::test]
async fn unsealed_cursor_token_is_not_listed() {
    let (_home, credentials) = home_with_grok_bot(&cursor_file("not-sealed"));

    let snapshot = credentials.list_scoped().await.unwrap();
    let result = credentials.get_scoped(CURSOR_CREDENTIAL, 0).await;

    assert!(snapshot.rows.is_empty());
    assert!(matches!(result, Err(VaultGetError::NotFound)));
}

#[tokio::test]
async fn cursor_fallback_is_hidden_while_grok_build_login_is_valid() {
    let (home, credentials) = home_with(None, Some(&grok_file("2100-01-01T00:00:00Z")));
    fs::create_dir_all(home.path().join(".config/Grok Bot")).unwrap();
    fs::write(
        home.path().join(".config/Grok Bot/sand-secrets.json"),
        cursor_file(&seal_v10(&cursor_token(FUTURE_MS / 1000))),
    )
    .unwrap();

    let snapshot = credentials.list_scoped().await.unwrap();
    let cursor = credentials.get_scoped(CURSOR_CREDENTIAL, 0).await;

    let ids: Vec<_> = snapshot
        .rows
        .iter()
        .map(|row| row.credential_id.as_str())
        .collect();
    assert_eq!(ids, [GROK_CREDENTIAL]);
    assert!(matches!(cursor, Err(VaultGetError::NotFound)));
}

#[tokio::test]
async fn cursor_fallback_serves_once_grok_build_login_expires() {
    let (home, credentials) = home_with(None, Some(&grok_file("2020-01-01T00:00:00Z")));
    fs::create_dir_all(home.path().join(".config/Grok Bot")).unwrap();
    fs::write(
        home.path().join(".config/Grok Bot/sand-secrets.json"),
        cursor_file(&seal_v10(&cursor_token(FUTURE_MS / 1000))),
    )
    .unwrap();

    let cursor = credentials.get_scoped(CURSOR_CREDENTIAL, 0).await;

    assert!(cursor.is_ok());
}
