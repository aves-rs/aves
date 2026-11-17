use std::collections::VecDeque;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use reqwest::{StatusCode, Url};
use serde::Deserialize;

use super::library::{CustomSkillFile, validate_custom_relative_path};
use super::types::DiscussError;

const MAX_FILE_BYTES: usize = 1024 * 1024;
const MAX_FILES: usize = 64;
const MAX_TOTAL_BYTES: usize = 8 * 1024 * 1024;
const MAX_DEPTH: usize = 4;
const MAX_API_CALLS: usize = 32;
const MAX_API_BODY_BYTES: usize = 2 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Fetched source content, before asset metadata validation or persistence.
pub(super) enum AssetSource {
    /// One Markdown document.
    Document(String),
    /// Files relative to a GitHub directory.
    Directory(Vec<CustomSkillFile>),
}

#[derive(Deserialize)]
struct GithubEntry {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    size: Option<usize>,
    encoding: Option<String>,
    content: Option<String>,
}

struct GithubSource {
    owner: String,
    repo: String,
    parts: Vec<String>,
    directory: bool,
}

/// Fetches a Markdown URL or a GitHub file or directory without writing locally.
pub(super) async fn fetch_asset_source(input: &str) -> Result<AssetSource, DiscussError> {
    let url = Url::parse(input).map_err(|error| import_error("parse_url", error))?;
    if url.scheme() != "https" || url.host_str().is_none() || !url.username().is_empty() || url.password().is_some() {
        return Err(import_error(
            "parse_url",
            "an HTTPS URL without credentials is required",
        ));
    }
    if url.host_str() == Some("github.com") {
        return fetch_github(parse_github(&url)?).await;
    }
    let (status, bytes) = fetch(&url, MAX_FILE_BYTES, None).await?;
    if !status.is_success() {
        return Err(import_error("fetch", format!("server returned {status}")));
    }
    Ok(AssetSource::Document(decode_text(bytes)?))
}

fn parse_github(url: &Url) -> Result<GithubSource, DiscussError> {
    if url.port().is_some() || url.query().is_some() || url.fragment().is_some() {
        return Err(import_error("parse_url", "invalid GitHub URL"));
    }
    let mut parts = url
        .path_segments()
        .ok_or_else(|| import_error("parse_url", "missing GitHub path"))?
        .map(decode_segment)
        .collect::<Result<Vec<_>, _>>()?;
    if parts.last().is_some_and(String::is_empty) {
        parts.pop();
    }
    if parts.len() < 4 || parts.iter().any(String::is_empty) || !matches!(parts[2].as_str(), "blob" | "tree") {
        return Err(import_error(
            "parse_url",
            "GitHub URL must identify a file or directory below the repository root",
        ));
    }
    if parts[..3].iter().any(|part| part.contains('/')) {
        return Err(import_error("parse_url", "invalid GitHub repository"));
    }
    let path = parts[3..]
        .iter()
        .flat_map(|part| part.split('/'))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if path.len() < 2
        || path
            .iter()
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(import_error("parse_url", "invalid GitHub path component"));
    }
    Ok(GithubSource {
        owner: parts[0].clone(),
        repo: parts[1].clone(),
        directory: parts[2] == "tree",
        parts: path,
    })
}

/// GitHub URL path components are percent encoded, but API path construction takes decoded components.
fn decode_segment(segment: &str) -> Result<String, DiscussError> {
    let mut bytes = Vec::with_capacity(segment.len());
    let mut input = segment.as_bytes().iter().copied();
    while let Some(byte) = input.next() {
        if byte == b'%' {
            let high = input.next().and_then(|v| (v as char).to_digit(16));
            let low = input.next().and_then(|v| (v as char).to_digit(16));
            let value = high
                .zip(low)
                .ok_or_else(|| import_error("parse_url", "invalid URL escape"))?;
            bytes.push((value.0 * 16 + value.1) as u8);
        } else {
            bytes.push(byte);
        }
    }
    let result = String::from_utf8(bytes).map_err(|error| import_error("parse_url", error))?;
    if result.contains('\\') || result.chars().any(char::is_control) || result == "." || result == ".." {
        return Err(import_error("parse_url", "invalid GitHub path component"));
    }
    Ok(result)
}

async fn fetch_github(source: GithubSource) -> Result<AssetSource, DiscussError> {
    let mut calls = 0;
    for split in (1..source.parts.len()).rev() {
        let reference = source.parts[..split].join("/");
        let path = source.parts[split..].join("/");
        let (status, body) = github_request(&source, &reference, &path, &mut calls).await?;
        if status == StatusCode::NOT_FOUND {
            continue;
        }
        if !status.is_success() {
            return Err(import_error("github_api", format!("GitHub returned {status}")));
        }
        if source.directory {
            if let Ok(entries) = serde_json::from_slice::<Vec<GithubEntry>>(&body) {
                return fetch_directory(&source, &reference, &path, entries, &mut calls).await;
            }
        } else if let Ok(entry) = serde_json::from_slice::<GithubEntry>(&body)
            && entry.kind == "file"
        {
            return Ok(AssetSource::Document(decode_github_file(entry, MAX_FILE_BYTES)?));
        }
    }
    Err(import_error("github_api", "GitHub file or directory not found"))
}

/// Traverse only API-provided child names; never follow API-provided URLs or symlinks.
async fn fetch_directory(
    source: &GithubSource,
    reference: &str,
    root: &str,
    entries: Vec<GithubEntry>,
    calls: &mut usize,
) -> Result<AssetSource, DiscussError> {
    let mut queue = VecDeque::from([(String::new(), 0usize, entries)]);
    let mut files = Vec::new();
    let mut total = 0usize;
    while let Some((parent, depth, entries)) = queue.pop_front() {
        for entry in entries {
            if entry.name.contains('/') {
                return Err(import_error("github_api", "invalid GitHub entry name"));
            }
            let relative = if parent.is_empty() {
                entry.name.clone()
            } else {
                format!("{parent}/{}", entry.name)
            };
            validate_custom_relative_path(&relative).map_err(|error| import_error("github_api", error))?;
            let path = format!("{root}/{relative}");
            match entry.kind.as_str() {
                "dir" => {
                    if depth >= MAX_DEPTH {
                        return Err(import_error("limit", "directory depth exceeded"));
                    }
                    let (status, body) = github_request(source, reference, &path, calls).await?;
                    if !status.is_success() {
                        return Err(import_error("github_api", format!("GitHub returned {status}")));
                    }
                    let children = serde_json::from_slice(&body).map_err(|error| import_error("github_api", error))?;
                    queue.push_back((relative, depth + 1, children));
                }
                "file" => {
                    if files.len() >= MAX_FILES {
                        return Err(import_error("limit", "file count exceeded"));
                    }
                    if entry
                        .size
                        .is_some_and(|size| size > MAX_FILE_BYTES || size > MAX_TOTAL_BYTES - total)
                    {
                        return Err(import_error("limit", "file or bundle size exceeded"));
                    }
                    let (status, body) = github_request(source, reference, &path, calls).await?;
                    if !status.is_success() {
                        return Err(import_error("github_api", format!("GitHub returned {status}")));
                    }
                    let file = serde_json::from_slice(&body).map_err(|error| import_error("github_api", error))?;
                    let contents = decode_github_file(file, MAX_FILE_BYTES.min(MAX_TOTAL_BYTES - total))?;
                    total += contents.len();
                    files.push(CustomSkillFile {
                        relative_path: relative,
                        contents,
                    });
                }
                _ => return Err(import_error("github_api", "unsupported GitHub entry type")),
            }
        }
    }
    files.sort_by_key(|file| file.relative_path != "SKILL.md");
    Ok(AssetSource::Directory(files))
}

async fn github_request(
    source: &GithubSource,
    reference: &str,
    path: &str,
    calls: &mut usize,
) -> Result<(StatusCode, Vec<u8>), DiscussError> {
    if *calls >= MAX_API_CALLS {
        return Err(import_error("limit", "GitHub API call limit exceeded"));
    }
    *calls += 1;
    let mut url = Url::parse("https://api.github.com/").map_err(|error| import_error("parse_url", error))?;
    url.path_segments_mut()
        .map_err(|_| import_error("parse_url", "invalid GitHub API URL"))?
        .extend(["repos", &source.owner, &source.repo, "contents"])
        .extend(path.split('/'));
    url.query_pairs_mut().append_pair("ref", reference);
    fetch(&url, MAX_API_BODY_BYTES, Some("application/vnd.github+json")).await
}

fn decode_github_file(entry: GithubEntry, limit: usize) -> Result<String, DiscussError> {
    if entry.kind != "file" || entry.encoding.as_deref() != Some("base64") {
        return Err(import_error(
            "github_api",
            "GitHub did not return a regular file with base64 content",
        ));
    }
    if entry.size.is_some_and(|size| size > limit) {
        return Err(import_error("limit", "file size exceeded"));
    }
    let encoded = entry
        .content
        .ok_or_else(|| import_error("github_api", "GitHub file content is missing"))?;
    let encoded: String = encoded
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect();
    if encoded.len() > limit.saturating_add(2) / 3 * 4 {
        return Err(import_error("limit", "file size exceeded"));
    }
    let bytes = STANDARD
        .decode(encoded)
        .map_err(|error| import_error("decode", error))?;
    if bytes.len() > limit {
        return Err(import_error("limit", "file size exceeded"));
    }
    decode_text(bytes)
}

fn decode_text(bytes: Vec<u8>) -> Result<String, DiscussError> {
    String::from_utf8(bytes).map_err(|error| import_error("decode", error))
}

/// Resolve once and disable proxies so the TLS connection uses only checked addresses.
async fn fetch(url: &Url, limit: usize, accept: Option<&str>) -> Result<(StatusCode, Vec<u8>), DiscussError> {
    let host = url
        .host_str()
        .ok_or_else(|| import_error("parse_url", "missing host"))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| import_error("parse_url", "missing port"))?;
    let lookup_host = host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host);
    let addresses: Vec<SocketAddr> =
        tokio::time::timeout(REQUEST_TIMEOUT, tokio::net::lookup_host((lookup_host, port)))
            .await
            .map_err(|error| import_error("resolve", error))?
            .map_err(|error| import_error("resolve", error))?
            .collect();
    if addresses.is_empty() {
        return Err(import_error("resolve", "host has no addresses"));
    }
    if addresses
        .iter()
        .any(|address| !global_address(address.ip()))
    {
        return Err(DiscussError::ImportAddressBlocked);
    }
    let client = reqwest::Client::builder()
        .https_only(true)
        .no_proxy()
        .timeout(REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .resolve_to_addrs(host, &addresses)
        .build()
        .map_err(|error| import_error("fetch", error))?;
    let mut request = client
        .get(url.clone())
        .header(reqwest::header::USER_AGENT, "aves-corv-asset-import");
    if let Some(accept) = accept {
        request = request.header(reqwest::header::ACCEPT, accept);
    }
    let mut response = request
        .send()
        .await
        .map_err(|error| import_error("fetch", error))?;
    let status = response.status();
    if status.is_redirection() {
        return Err(import_error("fetch", "redirects are not permitted"));
    }
    if !status.is_success() {
        return Ok((status, Vec::new()));
    }
    if response
        .content_length()
        .is_some_and(|size| size > limit as u64)
    {
        return Err(import_error("limit", "response size exceeded"));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| import_error("fetch", error))?
    {
        if chunk.len() > limit.saturating_sub(body.len()) {
            return Err(import_error("limit", "response size exceeded"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok((status, body))
}

/// Fail closed for special-purpose ranges, including mapped and transition IPv6 addresses.
fn global_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(ip) => global_v4(ip),
        IpAddr::V6(ip) => global_v6(ip),
    }
}

fn global_v4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !matches!(a, 0 | 10 | 127 | 224..=255)
        && !(a == 100 && (64..=127).contains(&b))
        && !(a == 169 && b == 254)
        && !(a == 172 && (16..=31).contains(&b))
        && !(a == 192 && (b == 168 || b == 0 || (b == 88 && c == 99)))
        && !(a == 198 && (b == 18 || b == 19 || (b == 51 && c == 100)))
        && !(a == 203 && b == 0 && c == 113)
}

fn global_v6(ip: Ipv6Addr) -> bool {
    if let Some(mapped) = ip.to_ipv4_mapped() {
        return global_v4(mapped);
    }
    let segments = ip.segments();
    (segments[0] & 0xe000) == 0x2000
        && !(segments[0] == 0x2001 && (segments[1] < 0x0200 || segments[1] == 0x0db8))
        && segments[0] != 0x2002
        && !(segments[0] == 0x3fff && segments[1] < 0x1000)
}

fn import_error(stage: &str, cause: impl std::fmt::Display) -> DiscussError {
    DiscussError::AssetImport {
        stage: stage.to_owned(),
        cause: cause.to_string(),
    }
}
