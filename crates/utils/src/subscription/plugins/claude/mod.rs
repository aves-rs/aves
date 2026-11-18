use std::path::Path;

use serde::Deserialize;

use super::{CliLogin, CredentialPlugin, record_version};

pub(super) const CLAUDE_CREDENTIAL: &str = "oauth:anthropic";

const CREDENTIALS_FILE: &str = ".claude/.credentials.json";

/// Claude Code's `claudeAiOauth` login.
pub(super) struct ClaudePlugin;

pub(super) const PLUGIN: ClaudePlugin = ClaudePlugin;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CredentialsFile {
    claude_ai_oauth: Oauth,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Oauth {
    access_token: String,
    expires_at: Option<i64>,
}

impl CredentialPlugin for ClaudePlugin {
    fn credential_id(&self) -> &'static str {
        CLAUDE_CREDENTIAL
    }

    fn login(&self, home: &Path) -> Option<CliLogin> {
        let path = home.join(CREDENTIALS_FILE);
        let file: CredentialsFile = serde_json::from_slice(&std::fs::read(&path).ok()?).ok()?;
        Some(CliLogin {
            token: file.claude_ai_oauth.access_token,
            expires_at_ms: file.claude_ai_oauth.expires_at,
            record_version: record_version(&path),
            account_id: None,
            email: None,
        })
    }
}
