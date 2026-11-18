use std::path::Path;

use aes::Aes128;
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use cbc::cipher::block_padding::Pkcs7;
use cbc::cipher::{BlockModeDecrypt, KeyIvInit};
use serde::Deserialize;
use serde_json::Value;

use aves::utils::time::now_ms;

use super::grok::PLUGIN as GROK_PLUGIN;
use super::{CliLogin, CredentialPlugin, record_version};

pub(super) const CURSOR_CREDENTIAL: &str = "oauth:cursor";

const SECRETS_FILE: &str = ".config/Grok Bot/sand-secrets.json";
/// Chromium's Linux `v10` key: PBKDF2-HMAC-SHA1("peanuts", "saltysalt", 1 round, 16 bytes).
pub(super) const V10_KEY: [u8; 16] = [
    0xfd, 0x62, 0x1f, 0xe5, 0xa2, 0xb4, 0x02, 0x53, 0x9d, 0xfa, 0x14, 0x7c, 0xa9, 0x27, 0x27, 0x78,
];
pub(super) const V10_IV: [u8; 16] = [b' '; 16];
pub(super) const V10_PREFIX: &[u8] = b"v10";

/// Grok Bot's Cursor sign-in, which also carries The X Subscription's shared Cursor and Grok pool.
/// A fallback: served only while the Grok Build login is missing or expired.
pub(super) struct CursorPlugin;

pub(super) const PLUGIN: CursorPlugin = CursorPlugin;

#[derive(Deserialize)]
struct SecretsFile {
    #[serde(rename = "cursor-accounts")]
    cursor_accounts: String,
}

#[derive(Deserialize)]
struct Accounts {
    active: String,
    accounts: serde_json::Map<String, Value>,
}

#[derive(Deserialize)]
struct Account {
    #[serde(rename = "cursor-access-token")]
    access_token: String,
}

#[derive(Deserialize)]
struct Claims {
    exp: Option<i64>,
    sub: Option<String>,
}

impl CredentialPlugin for CursorPlugin {
    fn credential_id(&self) -> &'static str {
        CURSOR_CREDENTIAL
    }

    fn login(&self, home: &Path) -> Option<CliLogin> {
        if GROK_PLUGIN
            .login(home)
            .is_some_and(|login| login.usable(now_ms(), 0))
        {
            return None;
        }
        let path = home.join(SECRETS_FILE);
        let file: SecretsFile = serde_json::from_slice(&std::fs::read(&path).ok()?).ok()?;
        let accounts: Accounts = serde_json::from_str(&file.cursor_accounts).ok()?;
        let account: Account = serde_json::from_value(accounts.accounts.get(&accounts.active)?.clone()).ok()?;
        let token = decrypt_v10(&account.access_token)?;
        let claims = jwt_claims(&token);
        Some(CliLogin {
            expires_at_ms: claims
                .as_ref()
                .and_then(|claims| claims.exp)
                .map(|exp| exp * 1000),
            account_id: claims
                .and_then(|claims| claims.sub)
                .and_then(|sub| sub.rsplit('|').next().map(str::to_owned)),
            token,
            record_version: record_version(&path),
            email: None,
        })
    }
}

/// Decrypt one base64 Electron `safeStorage` value sealed with Chromium's fixed Linux `v10` key.
fn decrypt_v10(value: &str) -> Option<String> {
    let sealed = STANDARD.decode(value).ok()?;
    let mut buffer = sealed.strip_prefix(V10_PREFIX)?.to_vec();
    let plain = cbc::Decryptor::<Aes128>::new(&V10_KEY.into(), &V10_IV.into())
        .decrypt_padded::<Pkcs7>(&mut buffer)
        .ok()?;
    String::from_utf8(plain.to_vec()).ok()
}

/// Unverified JWT claims; only read for expiry and account labelling.
fn jwt_claims(token: &str) -> Option<Claims> {
    let payload = token.split('.').nth(1)?;
    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).ok()?).ok()
}
