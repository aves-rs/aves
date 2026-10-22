use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use specta::Type;
use thiserror::Error;

use aves::bus::DispatchContextError;
use aves::runtime::GeneratedCallError;
use aves::utils::errors::SharedInfraError;
use aves::utils::uuid::SpectaUuid;

/// Knowledge creation always writes a file beneath the configured root.
pub type CreateKnowledgeRequest = VaultCreateKnowledgeRequest;

/// Direct vault request and result shapes; no backend discriminator is needed.
pub type GetKnowledgeRequest = String;

/// Update request for knowledge.
pub type UpdateKnowledgeRequest = VaultUpdateKnowledgeRequest;

/// Delete request for knowledge.
pub type DeleteKnowledgeRequest = String;

/// List request for knowledge.
pub type ListKnowledgeRequest = VaultListKnowledgeRequest;

/// Search request for knowledge.
pub type SearchKnowledgeRequest = VaultSearchKnowledgeRequest;

/// Knowledge record shape.
pub type KnowledgeRecord = VaultKnowledge;

/// Lookup result for a knowledge record.
pub type KnowledgeRecordLookup = Option<VaultKnowledge>;

/// List of knowledge records.
pub type KnowledgeRecordList = Vec<VaultKnowledge>;

/// Search outcome for knowledge records.
pub type KnowledgeSearchOutcome = VaultKnowledgeSearchResponse;

/// Complete lightweight Obsidian vault tree entry list.
pub type VaultKnowledgeTree = Vec<VaultKnowledgeTreeEntry>;

/// Knowledge document operation result.
pub(super) type KnowledgeResult<T> = Result<T, KnowledgeError>;

/// Knowledge body content type supported by the first slice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub enum KnowledgeContentType {
    /// Markdown content.
    Markdown,
    /// Plain text content.
    Plaintext,
}

/// Canvas dashboard create request body.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct CreateCanvasDashboardRequest {
    /// User-facing dashboard title.
    pub title: String,
    /// Complete HTML document or fragment used to render the dashboard.
    pub html: String,
}

/// Canvas dashboard lookup request body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct GetCanvasDashboardRequest {
    /// Dashboard identifier.
    pub id: SpectaUuid,
}

/// Canvas dashboard update request body.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct UpdateCanvasDashboardRequest {
    /// Dashboard identifier.
    pub id: SpectaUuid,
    /// Replacement user-facing dashboard title.
    pub title: String,
    /// Replacement complete HTML document or fragment.
    pub html: String,
}

/// Canvas dashboard delete request body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct DeleteCanvasDashboardRequest {
    /// Dashboard identifier.
    pub id: SpectaUuid,
}

/// Canvas dashboard list request body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct ListCanvasDashboardsRequest {
    /// Maximum number of dashboard summaries to return.
    pub limit: u32,
}

/// Persisted Canvas dashboard record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct CanvasDashboard {
    /// Dashboard identifier.
    pub id: SpectaUuid,
    /// User-facing dashboard title.
    pub title: String,
    /// Complete stored dashboard HTML.
    pub html: String,
    /// RFC3339 creation timestamp.
    pub created_at: String,
    /// RFC3339 last-update timestamp.
    pub updated_at: String,
}

/// Canvas dashboard summary without the stored HTML body.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct CanvasDashboardSummary {
    /// Dashboard identifier.
    pub id: SpectaUuid,
    /// User-facing dashboard title.
    pub title: String,
    /// RFC3339 creation timestamp.
    pub created_at: String,
    /// RFC3339 last-update timestamp.
    pub updated_at: String,
}

/// Optional Canvas dashboard lookup result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct CanvasDashboardLookup(pub Option<CanvasDashboard>);

/// Canvas dashboard summary list result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct CanvasDashboardSummaryList(pub Vec<CanvasDashboardSummary>);

/// Canvas dashboard delete result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct CanvasDashboardDeleteResult {
    /// Whether a dashboard file was deleted.
    pub deleted: bool,
}

/// Create a note from inline content or a host source path.
/// Agents SHOULD supply absolute `input_file` paths; relative paths are looked up
/// only beneath `config_dir/discuss` and fail if the source does not exist.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct VaultCreateKnowledgeRequest {
    /// Optional desired vault-relative path or backend id seed.
    pub backend_id: Option<String>,
    /// User-facing note title.
    pub title: String,
    /// Optional inline source content; mutually complementary with `input_file`.
    pub content: Option<String>,
    /// Optional source file path on the host, not the destination note path or `backend_id`.
    /// Agents SHOULD use an absolute path. Relative paths resolve only under
    /// `config_dir/discuss`, regardless of Discuss settings; a missing path returns an error.
    pub input_file: Option<PathBuf>,
    /// Content format.
    pub content_type: KnowledgeContentType,
    /// Frontmatter values to write with the note.
    pub frontmatter: Map<String, Value>,
    /// Vault-native tags.
    pub tags: Vec<String>,
}

/// Obsidian vault update request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct VaultUpdateKnowledgeRequest {
    /// Vault backend id of the note to update.
    pub backend_id: String,
    /// Replacement or newly resolved vault-relative path.
    pub path: Option<String>,
    /// Replacement title.
    pub title: Option<String>,
    /// Replacement full note content.
    pub content: Option<String>,
    /// Replacement content format.
    pub content_type: Option<KnowledgeContentType>,
    /// Replacement frontmatter values.
    pub frontmatter: Option<Map<String, Value>>,
    /// Replacement vault-native tags.
    pub tags: Option<Vec<String>>,
}

/// Filters for Knowledge notes, applied after semantic ranking when querying.
///
/// All fields are optional. Date lower bounds
/// (`*_after`) are inclusive RFC3339 timestamps; upper bounds (`*_before`) are
/// exclusive RFC3339 timestamps when the backend can apply them.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct VaultKnowledgeFilters {
    /// Match notes under a vault-relative path prefix.
    pub path_prefix: Option<String>,
    /// Match notes containing any of these vault-native tags.
    pub tags: Option<Vec<String>>,
    /// Match notes whose frontmatter contains these key/value pairs.
    pub frontmatter: Option<Map<String, Value>>,
    /// Match this content format.
    pub content_type: Option<KnowledgeContentType>,
    /// Include notes created at or after this RFC3339 timestamp.
    pub created_after: Option<String>,
    /// Include notes created before this RFC3339 timestamp.
    pub created_before: Option<String>,
    /// Include notes updated at or after this RFC3339 timestamp.
    pub updated_after: Option<String>,
    /// Include notes updated before this RFC3339 timestamp.
    pub updated_before: Option<String>,
}

/// Directory-backed note list request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct VaultListKnowledgeRequest {
    /// Optional text query for relevance ordering.
    pub query: Option<String>,
    /// Vault-native filters.
    pub filters: VaultKnowledgeFilters,
    /// Maximum number of records to return.
    pub limit: u32,
}

/// Directory-backed note search request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct VaultSearchKnowledgeRequest {
    /// Search query text.
    pub query: String,
    /// Vault-native filters.
    pub filters: VaultKnowledgeFilters,
    /// Maximum number of results to return.
    pub limit: u32,
}

/// Obsidian vault record shape exposed through the public RPC surface.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct VaultKnowledge {
    /// Backend-specific path-based vault id.
    pub backend_id: String,
    /// Vault-relative note path.
    pub path: String,
    /// Resolved title, usually frontmatter title or filename fallback.
    pub title: String,
    /// Full note content.
    pub content: String,
    /// Content format.
    pub content_type: KnowledgeContentType,
    /// Parsed frontmatter JSON object.
    pub frontmatter: Map<String, Value>,
    /// Vault-native tags.
    pub tags: Vec<String>,
    /// RFC3339 creation timestamp when available.
    pub created_at: Option<String>,
    /// RFC3339 update timestamp when available.
    pub updated_at: Option<String>,
}

/// Kind of entry exposed by the lightweight Obsidian vault tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub enum VaultKnowledgeTreeEntryKind {
    /// A vault directory, including empty directories.
    Directory,
    /// A surfaceable vault file.
    File,
}

/// Lightweight Obsidian vault entry used to build the complete navigation tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct VaultKnowledgeTreeEntry {
    /// Stable vault-relative path used as the backend identifier.
    pub backend_id: String,
    /// Vault-relative display path.
    pub path: String,
    /// Whether this entry is a directory or file.
    pub kind: VaultKnowledgeTreeEntryKind,
}

/// One vault search result with its backend-native score.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct VaultKnowledgeSearchResult {
    /// Matching vault record.
    pub knowledge: VaultKnowledge,
    /// Backend-native search score.
    pub score: f64,
}

/// Obsidian vault search response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct VaultKnowledgeSearchResponse {
    /// Search query text.
    pub query: String,
    /// Ranked vault results.
    pub results: Vec<VaultKnowledgeSearchResult>,
}

/// Exact key and JSON object for the Knowledge data store. Writes replace the entire object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct UpsertDataRequest {
    pub key: String,
    pub value: Map<String, Value>,
}

/// A stored JSON object with its most recent write time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct DataObject {
    pub key: String,
    pub value: Map<String, Value>,
    pub updated_at: String,
}

/// Exact key and last write time for list results.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct DataSummary {
    pub key: String,
    pub updated_at: String,
}

/// Paginated date-filtered data listing; values are fetched by exact key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct DataList {
    pub entries: Vec<DataSummary>,
}

/// Inclusive RFC3339 write-time bounds and offset pagination.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct ListDataRequest {
    pub since: Option<String>,
    pub until: Option<String>,
    pub limit: u32,
    pub offset: u32,
}

/// Knowledge-private create request with source content resolved before writing documents.
///
/// The actor clears `content` and `input_file` on the embedded public request body;
/// the resolved text lives only in this envelope's separate `content` field.
#[derive(Debug, Clone)]
pub(super) enum ResolvedCreateKnowledgeRequest {
    /// One note with resolved source content.
    Single {
        request: VaultCreateKnowledgeRequest,
        content: String,
    },
    /// One note resolved from a directory source file.
    DirectoryFile {
        request: VaultCreateKnowledgeRequest,
        content: String,
        source_relative_path: PathBuf,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub struct DeleteKnowledgeResult {
    pub backend_id: String,
    pub deleted: bool,
}

/// Knowledge-owned configuration section. An absent root uses `config_dir/knowledge`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(default)]
pub struct KnowledgeConfig {
    pub root: Option<PathBuf>,
    /// Root-level directory used for trashed notes.
    pub trash_path: PathBuf,
    /// File or directory paths excluded from Knowledge discovery.
    pub excluded_paths: Vec<String>,
    /// Allowed note file extensions, without a leading dot by default.
    pub allowed_extensions: Vec<String>,
}

impl Default for KnowledgeConfig {
    fn default() -> Self {
        Self {
            root: None,
            trash_path: PathBuf::from(".trash"),
            excluded_paths: Vec::new(),
            allowed_extensions: vec![
                "md".to_owned(),
                "markdown".to_owned(),
                "txt".to_owned(),
                "canvas".to_owned(),
            ],
        }
    }
}

impl KnowledgeConfig {
    pub(super) fn excludes_path(&self, root: &Path, relative: &Path) -> bool {
        self.excluded_paths.iter().any(|excluded| {
            let excluded_path = Path::new(excluded);
            if excluded_path.is_absolute() {
                root.join(relative).starts_with(excluded_path)
            } else if excluded.starts_with("./") || excluded.contains('/') || excluded.contains('\\') {
                relative.starts_with(Path::new(excluded.strip_prefix("./").unwrap_or(excluded)))
            } else {
                relative
                    .components()
                    .any(|component| component.as_os_str() == excluded_path.as_os_str())
            }
        })
    }
}

/// Errors that can occur in the knowledge domain.
///
/// All variants store only serializable data so knowledge errors can cross RPC
/// boundaries without carrying raw external error types.
#[derive(Error, Debug, Clone, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
pub enum KnowledgeError {
    /// A data key was not found by exact lookup or deletion.
    #[error("knowledge data not found: {key}")]
    DataNotFound { key: String },

    /// A date filter must be an RFC3339 timestamp.
    #[error("invalid knowledge data date: {value}")]
    InvalidDataDate { value: String },

    /// The Knowledge JSON data store could not complete an operation.
    #[error("knowledge data {operation} failed: {message}")]
    DataStorage { operation: String, message: String },
    /// I/O error when reading or writing files.
    #[error("i/o error at path: {message}")]
    Io {
        /// The path that caused the error.
        path: String,
        /// The error message.
        message: String,
    },

    /// Requested Canvas dashboard does not exist.
    #[error("canvas dashboard not found: {id}")]
    CanvasDashboardNotFound {
        /// Dashboard identifier that was not found.
        id: SpectaUuid,
    },

    /// Requested knowledge record does not exist at the exact backend identifier.
    #[error(
        "{backend} knowledge not found at `{identifier}`; use listKnowledge or searchKnowledge to rediscover its current identifier"
    )]
    KnowledgeNotFound {
        /// Active backend that performed the exact lookup.
        backend: String,
        /// Exact UUID or backend path supplied by the caller.
        identifier: String,
    },

    /// A surfaced text knowledge file contains bytes that are not valid UTF-8.
    #[error("knowledge file is not valid UTF-8: {path}")]
    InvalidTextEncoding {
        /// Caller-supplied knowledge text file path.
        path: String,
    },

    /// I/O error while reading or writing a Canvas dashboard artifact.
    #[error("canvas dashboard i/o error at path {path}: {message}")]
    CanvasDashboardIo {
        /// The dashboard path that caused the error.
        path: String,
        /// The error message.
        message: String,
    },

    /// Actor channel send error.
    #[error("channel send error")]
    ChannelSend,

    /// Actor channel receive error.
    #[error("channel receive error")]
    ChannelRecv,

    /// Oneshot reply channel closed before a response was sent.
    #[error("oneshot reply channel closed")]
    OneshotReply,

    /// Neither inline content nor an input file was supplied for knowledge creation.
    #[error("neither content nor input_file supplied for createKnowledge")]
    MissingCreateInput,

    /// A directory input contained no importable text files.
    #[error("no directory files could be imported: {path}")]
    NoDirectoryFilesImported {
        /// The effective directory input path.
        path: String,
    },

    /// Active Knowledge backend has invalid backend-specific configuration.
    #[error("invalid {backend} knowledge config: {message}")]
    InvalidBackendConfig {
        /// Backend whose configuration was invalid.
        backend: String,
        /// Safe validation message.
        message: String,
    },

    /// Knowledge store found an invalid persisted value or invalid bounded numeric value.
    #[error("invalid knowledge value {field}: {message}")]
    InvalidStoreValue {
        /// Field or input label that failed validation.
        field: String,
        /// Invalid value when it is safe and useful to expose.
        value: Option<String>,
        /// Safe validation or parse error message.
        message: String,
    },

    /// Knowledge store metadata JSON serialization or deserialization failed.
    #[error("knowledge metadata failed {operation}: {message}")]
    MetadataSerialization {
        /// Serialization operation that failed.
        operation: String,
        /// Safe serde error message.
        message: String,
    },

    /// Knowledge store write completed but a required follow-up invariant was not satisfied.
    #[error("knowledge store failure {operation}: {message}")]
    StoreConsistency {
        /// Store operation whose invariant failed.
        operation: String,
        /// Safe invariant failure message.
        message: String,
    },

    /// Bus context was unavailable or unauthenticated.
    #[error("knowledge bus dispatch failed: {message}")]
    BusDispatch { message: String },

    /// Bus authentication rejected the knowledge request.
    #[error("knowledge bus authentication failed: {message}")]
    BusAuthentication { message: String },

    /// Bus reply did not match the operation's declared response.
    #[error("expected knowledge response {expected}, got {actual}")]
    UnexpectedBusResponse { expected: String, actual: String },

    /// Explicit boundary fallback for unclassified legacy IPC errors.
    ///
    /// New routine failures should use a dedicated typed variant instead of
    /// entering this escape hatch.
    #[error("{0}")]
    Other(
        /// Legacy error message.
        String,
    ),
}

impl KnowledgeError {
    /// Stable client-facing error code and safe structured details.
    pub fn rpc_identity(&self) -> (&'static str, Map<String, Value>) {
        match self {
            KnowledgeError::DataNotFound { key } => {
                let mut details = Map::new();
                details.insert("key".to_owned(), Value::String(key.clone()));
                ("DataNotFound", details)
            }
            KnowledgeError::InvalidDataDate { value } => {
                let mut details = Map::new();
                details.insert("value".to_owned(), Value::String(value.clone()));
                ("InvalidDataDate", details)
            }
            KnowledgeError::DataStorage { operation, .. } => {
                let mut details = Map::new();
                details.insert("operation".to_owned(), Value::String(operation.clone()));
                ("DataStorage", details)
            }
            KnowledgeError::Io { path, .. } => {
                let mut details = Map::new();
                details.insert("path".to_string(), Value::String(path.clone()));
                ("Io", details)
            }
            KnowledgeError::CanvasDashboardNotFound { id } => {
                let mut details = Map::new();
                details.insert("id".to_string(), Value::String(id.to_string()));
                ("CanvasDashboardNotFound", details)
            }
            KnowledgeError::KnowledgeNotFound { backend, identifier } => {
                let mut details = Map::new();
                details.insert("backend".to_string(), Value::String(backend.clone()));
                details.insert("identifier".to_string(), Value::String(identifier.clone()));
                ("KnowledgeNotFound", details)
            }
            KnowledgeError::InvalidTextEncoding { path } => {
                let mut details = Map::new();
                details.insert("path".to_string(), Value::String(path.clone()));
                ("InvalidTextEncoding", details)
            }
            KnowledgeError::CanvasDashboardIo { path, .. } => {
                let mut details = Map::new();
                details.insert("path".to_string(), Value::String(path.clone()));
                ("CanvasDashboardIo", details)
            }
            KnowledgeError::ChannelSend => ("ChannelSend", Map::new()),
            KnowledgeError::ChannelRecv => ("ChannelRecv", Map::new()),
            KnowledgeError::OneshotReply => ("OneshotReply", Map::new()),
            KnowledgeError::MissingCreateInput => ("MissingCreateInput", Map::new()),
            KnowledgeError::NoDirectoryFilesImported { path } => {
                let mut details = Map::new();
                details.insert("path".to_string(), Value::String(path.clone()));
                ("NoDirectoryFilesImported", details)
            }
            KnowledgeError::InvalidBackendConfig { .. } => ("InvalidBackendConfig", Map::new()),
            KnowledgeError::InvalidStoreValue { field, value, .. } => {
                let mut details = Map::new();
                details.insert("field".to_string(), Value::String(field.clone()));
                if let Some(value) = value {
                    details.insert("value".to_string(), Value::String(value.clone()));
                }
                ("InvalidStoreValue", details)
            }
            KnowledgeError::MetadataSerialization { operation, .. } => {
                let mut details = Map::new();
                details.insert("operation".to_string(), Value::String(operation.clone()));
                ("MetadataSerialization", details)
            }
            KnowledgeError::StoreConsistency { operation, .. } => {
                let mut details = Map::new();
                details.insert("operation".to_string(), Value::String(operation.clone()));
                ("StoreConsistency", details)
            }
            KnowledgeError::BusDispatch { .. } => ("BusDispatch", Map::new()),
            KnowledgeError::BusAuthentication { .. } => ("BusAuthentication", Map::new()),
            KnowledgeError::UnexpectedBusResponse { expected, actual } => {
                let mut details = Map::new();
                details.insert("expected".to_string(), Value::String(expected.clone()));
                details.insert("actual".to_string(), Value::String(actual.clone()));
                ("UnexpectedBusResponse", details)
            }
            KnowledgeError::Other(_) => ("Other", Map::new()),
        }
    }
}

impl From<SharedInfraError> for KnowledgeError {
    fn from(error: SharedInfraError) -> Self {
        use SharedInfraError;

        match error {
            SharedInfraError::ChannelSend { .. } => Self::ChannelSend,
            SharedInfraError::ChannelRecv { .. } => Self::ChannelRecv,
            SharedInfraError::OneshotReplyTimeout { .. } => Self::ChannelRecv,
            SharedInfraError::OneshotReplyMissing { .. } => Self::OneshotReply,
        }
    }
}

impl From<GeneratedCallError<KnowledgeError>> for KnowledgeError {
    fn from(error: GeneratedCallError<KnowledgeError>) -> Self {
        match error {
            GeneratedCallError::DomainError(error) => error,
            GeneratedCallError::Authentication(error) => Self::BusAuthentication {
                message: error.to_string(),
            },
            GeneratedCallError::InputClosed => Self::ChannelSend,
            GeneratedCallError::ReplyDropped => Self::OneshotReply,
            GeneratedCallError::UnexpectedResponse { expected, actual } => {
                Self::UnexpectedBusResponse { expected, actual }
            }
        }
    }
}

impl From<DispatchContextError> for KnowledgeError {
    fn from(error: DispatchContextError) -> Self {
        Self::BusDispatch {
            message: error.to_string(),
        }
    }
}

impl From<serde_json::Error> for KnowledgeError {
    fn from(error: serde_json::Error) -> Self {
        Self::MetadataSerialization {
            operation: "serialize knowledge RPC".to_string(),
            message: error.to_string(),
        }
    }
}

impl rspc::Error for KnowledgeError {
    fn into_procedure_error(self) -> rspc::ProcedureError {
        let (code, details) = self.rpc_identity();
        let safe_error = serde_json::json!({
            "DomainError": {
                "domain": "knowledge",
                "code": code,
                "message": self.to_string(),
                "details": details,
            }
        });
        rspc::ProcedureError::Resolver(rspc::ResolverError::new(safe_error, None::<Self>))
    }
}

impl From<io::Error> for KnowledgeError {
    fn from(err: io::Error) -> Self {
        KnowledgeError::Io {
            path: String::new(),
            message: err.to_string(),
        }
    }
}
