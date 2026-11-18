use std::path::Path;

use serde::Deserialize;
use serde_json::{Map, Value};

use aves::utils::time::iso_to_ms;

use super::{CliLogin, CredentialPlugin, record_version};

pub(super) const GROK_CREDENTIAL: &str = "oauth:xai";

const AUTH_FILE: &str = ".grok/auth.json";
const ISSUER: &str = "https://auth.x.ai";

/// The Grok CLI's `auth.x.ai` login.
pub(super) struct GrokPlugin;

pub(super) const PLUGIN: GrokPlugin = GrokPlugin;

#[derive(Deserialize)]
struct Login {
    key: String,
    expires_at: Option<String>,
    user_id: Option<String>,
    email: Option<String>,
}

impl CredentialPlugin for GrokPlugin {
    fn credential_id(&self) -> &'static str {
        GROK_CREDENTIAL
    }

    fn login(&self, home: &Path) -> Option<CliLogin> {
        let path = home.join(AUTH_FILE);
        let file: Map<String, Value> = serde_json::from_slice(&std::fs::read(&path).ok()?).ok()?;
        let (_, entry) = file.into_iter().find(|(key, _)| key.starts_with(ISSUER))?;
        let login: Login = serde_json::from_value(entry).ok()?;
        Some(CliLogin {
            token: login.key,
            expires_at_ms: login
                .expires_at
                .as_deref()
                .and_then(|value| iso_to_ms(value).ok()),
            record_version: record_version(&path),
            account_id: login.user_id,
            email: login.email,
        })
    }
}
