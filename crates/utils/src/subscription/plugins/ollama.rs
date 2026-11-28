use std::path::Path;

use quota_core::opencode_auth::{OpencodeAuth, auth_path, read_provider};

use super::{CliLogin, CredentialPlugin, record_version};

pub(super) const OLLAMA_CREDENTIAL: &str = "apikey:ollama-cloud";
const OPENCODE_PROVIDER: &str = "ollama-cloud";

/// Exposes OpenCode's Ollama Cloud API key to Insula's allowance collector.
pub(super) struct OllamaPlugin(fn() -> Option<CliLogin>);

impl OllamaPlugin {
    pub(super) fn new(read: fn() -> Option<CliLogin>) -> Self {
        Self(read)
    }
}

impl CredentialPlugin for OllamaPlugin {
    fn credential_id(&self) -> &'static str {
        OLLAMA_CREDENTIAL
    }

    fn credential_type(&self) -> &'static str {
        "apikey"
    }

    fn login(&self, _: &Path) -> Option<CliLogin> {
        (self.0)()
    }
}

pub(super) fn read_login() -> Option<CliLogin> {
    let OpencodeAuth::Api { key } = read_provider(OPENCODE_PROVIDER).ok()?? else {
        return None;
    };
    if key.trim().is_empty() {
        return None;
    }
    let path = auth_path()?;
    Some(CliLogin {
        token: key,
        expires_at_ms: None,
        record_version: record_version(&path).max(record_version(&path.with_file_name("opencode.db"))),
        account_id: None,
        email: None,
    })
}
