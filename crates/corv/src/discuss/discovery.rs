use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

const REGISTRY_URL: &str = "https://cdn.agentclientprotocol.com/registry/v1/latest/registry.json";
const SNAPSHOT: &str = include_str!("registry-snapshot.json");
const MAX_CATALOG_BYTES: usize = 4 * 1024 * 1024;

/// How the provider catalog was obtained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoverySource {
    /// The current registry was fetched successfully.
    Live,
    /// Fetch, HTTP status, size, or catalog decoding failed.
    Bundled,
}

/// An ACP stdio command; environment entries are named pairs for the ACP schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderLaunch {
    /// Provider display name.
    pub name: String,
    /// Executable name resolved from PATH when discovery ran.
    pub command: String,
    /// Arguments passed to the executable.
    pub args: Vec<String>,
    /// Environment variables passed to the executable.
    pub env: Vec<ProviderEnvironment>,
}

/// One ACP stdio environment variable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderEnvironment {
    /// Variable name.
    pub name: String,
    /// Variable value.
    pub value: String,
}

/// Registry identity and the corresponding launch command.
#[derive(Debug, Clone)]
pub struct DiscoveredProvider {
    /// Registry identity, independent of the display name.
    pub id: String,
    /// Catalog version, when available.
    pub version: Option<String>,
    /// Catalog description, when available.
    pub description: Option<String>,
    /// Launch specification suitable for ACP stdio provider configuration.
    pub launch: ProviderLaunch,
}

/// Discovery results and whether the offline snapshot was used.
#[derive(Debug, Clone)]
pub struct DiscoveryOutcome {
    /// Origin of the catalog.
    pub source: DiscoverySource,
    /// Launchable catalog entries; this does not prove package installation or authentication.
    pub providers: Vec<DiscoveredProvider>,
    /// False if the PATH lookup worker could not complete; the list is then not authoritative.
    pub path_lookup_complete: bool,
}

#[derive(Deserialize)]
struct Catalog {
    agents: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct Entry {
    id: String,
    name: String,
    version: Option<String>,
    description: Option<String>,
    distribution: Option<Distribution>,
}

#[derive(Deserialize)]
struct Distribution {
    npx: Option<Package>,
    uvx: Option<Package>,
}

#[derive(Deserialize)]
struct Package {
    package: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
}

/// Fetch the public ACP registry with a bounded request and bundled fallback.
/// Only PATH candidates for npx and uvx are inspected; no commands are executed.
pub async fn discover_providers() -> DiscoveryOutcome {
    let live = fetch_catalog().await;
    let (source, catalog) = match live {
        Some(catalog) => (DiscoverySource::Live, catalog),
        None => (
            DiscoverySource::Bundled,
            serde_json::from_str(SNAPSHOT).expect("bundled registry is valid"),
        ),
    };
    let (providers, path_lookup_complete) = match tokio::task::spawn_blocking(move || resolve_catalog(catalog)).await {
        Ok(providers) => (providers, true),
        Err(_) => (Vec::new(), false),
    };
    DiscoveryOutcome {
        source,
        providers,
        path_lookup_complete,
    }
}

async fn fetch_catalog() -> Option<Catalog> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .ok()?;
    let mut response = client
        .get(REGISTRY_URL)
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?;
    if response
        .content_length()
        .is_some_and(|size| size > MAX_CATALOG_BYTES as u64)
    {
        return None;
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.ok()? {
        if chunk.len() > MAX_CATALOG_BYTES.saturating_sub(bytes.len()) {
            return None;
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).ok()
}

fn resolve_catalog(catalog: Catalog) -> Vec<DiscoveredProvider> {
    resolve_entries(catalog, on_path("npx"), on_path("uvx"))
}

fn resolve_entries(catalog: Catalog, npx: bool, uvx: bool) -> Vec<DiscoveredProvider> {
    let mut providers: Vec<_> = catalog
        .agents
        .into_iter()
        .filter_map(|value| {
            let entry: Entry = serde_json::from_value(value).ok()?;
            if entry.id.is_empty() || entry.name.is_empty() {
                return None;
            }
            let distribution = entry.distribution?;
            // Binary distributions require installation/probing and are intentionally skipped.
            let (command, package, available) = match distribution.npx.filter(|p| !p.package.is_empty()) {
                Some(package) => ("npx", package, npx),
                None => ("uvx", distribution.uvx.filter(|p| !p.package.is_empty())?, uvx),
            };
            if !available {
                return None;
            }
            let mut args = Vec::new();
            if command == "npx" {
                args.push("-y".to_owned());
            }
            args.push(package.package);
            args.extend(package.args);
            let env = package
                .env
                .into_iter()
                .map(|(name, value)| ProviderEnvironment { name, value })
                .collect();
            Some(DiscoveredProvider {
                id: entry.id,
                version: entry.version,
                description: entry.description,
                launch: ProviderLaunch {
                    name: entry.name,
                    command: command.to_owned(),
                    args,
                    env,
                },
            })
        })
        .collect();
    providers.sort_by(|left, right| left.launch.name.cmp(&right.launch.name));
    providers
}

fn on_path(command: &str) -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path)
        .filter(|dir| dir.is_absolute())
        .any(|dir| {
            #[cfg(windows)]
            let candidates: Vec<_> = std::env::var_os("PATHEXT")
                .unwrap_or_else(|| OsStr::new(".COM;.EXE;.BAT;.CMD").to_owned())
                .to_string_lossy()
                .split(';')
                .filter(|ext| !ext.is_empty())
                .map(|ext| dir.join([command, ext].concat()))
                .collect();
            #[cfg(not(windows))]
            let candidates = [dir.join(OsStr::new(command))];
            candidates.iter().any(|candidate| executable(candidate))
        })
}

fn executable(path: &Path) -> bool {
    let Ok(metadata) = path.metadata() else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::{Catalog, SNAPSHOT, resolve_entries};

    #[test]
    fn bundled_entries_require_npx_and_preserve_launch_arguments() {
        let catalog: Catalog = serde_json::from_str(SNAPSHOT).unwrap();
        assert!(resolve_entries(catalog, false, true).is_empty());
        let catalog: Catalog = serde_json::from_str(SNAPSHOT).unwrap();
        let providers = resolve_entries(catalog, true, false);
        assert_eq!(providers.len(), 6);
        let gemini = providers
            .iter()
            .find(|provider| provider.id == "gemini")
            .unwrap();
        assert_eq!(gemini.launch.args, ["-y", "@google/gemini-cli@0.47.0", "--acp"]);
    }

    #[test]
    fn uvx_launch_and_binary_only_entries_follow_app() {
        let catalog: Catalog = serde_json::from_str(
            r#"{"agents":[
            {"id":"uv","name":"UV","distribution":{"uvx":{"package":"example","args":["--acp"],"env":{"A":"B"}}}},
            {"id":"binary","name":"Binary","distribution":{"binary":{"linux-x86_64":{"cmd":"example"}}}}
        ]}"#,
        )
        .unwrap();
        let providers = resolve_entries(catalog, false, true);
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].launch.command, "uvx");
        assert_eq!(providers[0].launch.args, ["example", "--acp"]);
        assert_eq!(providers[0].launch.env[0].name, "A");
    }
}
