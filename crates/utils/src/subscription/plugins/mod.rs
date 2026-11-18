//! Credential plugin boundary for allowance observation.
//!
//! Each plugin reads one CLI's own login file and serves it to insula under that
//! provider's vault credential id. Tokens are never refreshed or written back; an
//! expired login stays unusable until its CLI refreshes it.

mod claude;
mod grok;

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use quota_core::credential_source::{CredentialSource, ScopedRowState, ScopedSnapshot, VaultCredential, VaultGetError};

use aves::utils::time::now_ms;

use claude::PLUGIN as CLAUDE_PLUGIN;
use grok::PLUGIN as GROK_PLUGIN;

const PLUGINS: [&dyn CredentialPlugin; 2] = [&CLAUDE_PLUGIN, &GROK_PLUGIN];

/// One provider's CLI login, served to insula as a scoped vault credential.
trait CredentialPlugin: Sync {
    /// insula vault credential id this login is served under.
    fn credential_id(&self) -> &'static str;

    /// Read the login under `home`; absent or malformed files yield `None`.
    fn login(&self, home: &Path) -> Option<CliLogin>;
}

/// One login read from a CLI credentials file.
struct CliLogin {
    token: String,
    expires_at_ms: Option<i64>,
    record_version: u64,
    account_id: Option<String>,
    email: Option<String>,
}

impl CliLogin {
    /// Whether the token has at least `min_ttl_ms` left at `now_ms`.
    fn usable(&self, now_ms: i64, min_ttl_ms: u64) -> bool {
        let min_ttl_ms = i64::try_from(min_ttl_ms).unwrap_or(i64::MAX);
        self.expires_at_ms
            .is_none_or(|expires| expires.saturating_sub(now_ms) >= min_ttl_ms)
    }
}

/// Read-only credential source over every plugin's CLI login.
pub(super) struct CliCredentials {
    home: PathBuf,
}

impl CliCredentials {
    pub(super) fn new(home: PathBuf) -> Self {
        Self { home }
    }

    fn login(&self, credential_id: &str) -> Option<CliLogin> {
        PLUGINS
            .iter()
            .find(|plugin| plugin.credential_id() == credential_id)?
            .login(&self.home)
    }
}

#[async_trait]
impl CredentialSource for CliCredentials {
    async fn get_scoped(&self, credential_id: &str, min_ttl_ms: u64) -> Result<VaultCredential, VaultGetError> {
        let login = self.login(credential_id).ok_or(VaultGetError::NotFound)?;
        if !login.usable(now_ms(), min_ttl_ms) {
            return Err(VaultGetError::AuthRequired);
        }
        Ok(VaultCredential {
            payload: login.token.into_bytes(),
            expires_at_ms: login.expires_at_ms,
            record_version: login.record_version,
            account_id: login.account_id,
            project_id: None,
            email: login.email,
            org_name: None,
        })
    }

    async fn list_scoped(&self) -> Result<ScopedSnapshot, VaultGetError> {
        let now = now_ms();
        let rows = PLUGINS
            .iter()
            .filter_map(|plugin| {
                let login = plugin.login(&self.home)?;
                let state = if login.usable(now, 0) { "ready" } else { "needs_reauth" };
                Some(ScopedRowState {
                    credential_id: plugin.credential_id().to_owned(),
                    credential_type: "oauth".to_owned(),
                    record_version: login.record_version,
                    state: state.to_owned(),
                    account_id: login.account_id,
                })
            })
            .collect();
        Ok(ScopedSnapshot { grants: 1, rows })
    }
}

/// File modification time in milliseconds, which advances whenever the CLI rewrites its login.
fn record_version(path: &Path) -> u64 {
    std::fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok())
        .unwrap_or(0)
}
