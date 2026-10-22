use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde_json::{Map, Value};
use thiserror::Error;
use tokio::sync::{Mutex, mpsc};
use turbovault_core::{PathValidator, Precondition, ServerConfig, VaultConfig};
use turbovault_parser::ParsedContent;
use turbovault_tools::SimilarityEngine;
use turbovault_vault::VaultManager;

use aves::flow::{FlowWorkspaceSourceError, FlowWorkspaceSourceService};
use aves::utils::time::{system_time_to_rfc3339, timestamp_in_bounds};

use super::types::{
    DeleteKnowledgeRequest, DeleteKnowledgeResult, GetKnowledgeRequest, KnowledgeConfig, KnowledgeContentType,
    KnowledgeError, KnowledgeRecord, KnowledgeRecordList, KnowledgeRecordLookup, KnowledgeResult,
    KnowledgeSearchOutcome, ListKnowledgeRequest, ResolvedCreateKnowledgeRequest, SearchKnowledgeRequest,
    UpdateKnowledgeRequest, VaultCreateKnowledgeRequest, VaultKnowledge, VaultKnowledgeFilters,
    VaultKnowledgeSearchResponse, VaultKnowledgeSearchResult, VaultKnowledgeTree, VaultKnowledgeTreeEntry,
    VaultKnowledgeTreeEntryKind, VaultListKnowledgeRequest, VaultSearchKnowledgeRequest,
};

const BACKEND: &str = "obsidian";
const DEFAULT_NOTE_EXTENSION: &str = "md";

pub(super) struct DocumentRuntime {
    pub(super) notes: Documents,
    pub(super) change_rx: Option<mpsc::UnboundedReceiver<()>>,
}

pub(super) fn create_workspace_source(
    config: &KnowledgeConfig,
    config_dir: &Path,
) -> (Arc<dyn FlowWorkspaceSourceService>, Arc<RwLock<PathBuf>>) {
    let root = config
        .root
        .clone()
        .unwrap_or_else(|| config_dir.join("knowledge"));
    let shared_root = Arc::new(RwLock::new(root));
    (
        Arc::new(KnowledgeWorkspaceSource::new(shared_root.clone())),
        shared_root,
    )
}

pub(super) async fn open_documents(config_dir: &Path, config: &KnowledgeConfig) -> KnowledgeResult<DocumentRuntime> {
    let root = config
        .root
        .clone()
        .unwrap_or_else(|| config_dir.join("knowledge"));
    if config.root.is_none() {
        tokio::fs::create_dir_all(&root)
            .await
            .map_err(|error| KnowledgeError::Io {
                path: root.display().to_string(),
                message: error.to_string(),
            })?;
    }
    let mut notes = Documents::new_with_config(&root, config)?;
    let change_rx = Some(notes.start_change_monitor());
    Ok(DocumentRuntime { notes, change_rx })
}

#[derive(Default)]
struct SemanticIndexCache {
    generation: AtomicU64,
    index: RwLock<Option<Arc<SimilarityEngine>>>,
}

impl SemanticIndexCache {
    fn current(&self) -> Option<Arc<SimilarityEngine>> {
        self.index
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn invalidate(&self) {
        let mut index = self
            .index
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *index = None;
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    fn publish(&self, generation: u64, built: Arc<SimilarityEngine>) -> bool {
        let mut index = self
            .index
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.generation.load(Ordering::Acquire) != generation {
            return false;
        }
        *index = Some(built);
        true
    }
}

/// Flow's narrow, read/write boundary for a known Obsidian directory.
struct KnowledgeWorkspaceSource {
    vault_path: Arc<RwLock<PathBuf>>,
}

impl KnowledgeWorkspaceSource {
    fn new(vault_path: Arc<RwLock<PathBuf>>) -> Self {
        Self { vault_path }
    }
}

impl FlowWorkspaceSourceService for KnowledgeWorkspaceSource {
    fn resolve_known_directory(&self, relative_identity: &str) -> Result<PathBuf, FlowWorkspaceSourceError> {
        let relative = Path::new(relative_identity);
        if relative_identity.contains('\\')
            || relative.is_absolute()
            || relative
                .components()
                .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
        {
            return Err(FlowWorkspaceSourceError::InvalidIdentity {
                identity: relative_identity.to_owned(),
            });
        }
        let root = self
            .vault_path
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let candidate = root.join(relative);
        let metadata = std::fs::symlink_metadata(&candidate).map_err(|source| FlowWorkspaceSourceError::Io {
            path: candidate.clone(),
            source,
        })?;
        if metadata.file_type().is_symlink() {
            return Err(FlowWorkspaceSourceError::SymbolicLink { path: candidate });
        }
        if !metadata.is_dir() {
            return Err(FlowWorkspaceSourceError::NotDirectory { path: candidate });
        }
        let canonical_root = std::fs::canonicalize(&root).map_err(|source| FlowWorkspaceSourceError::Io {
            path: root.clone(),
            source,
        })?;
        let canonical_candidate = std::fs::canonicalize(&candidate).map_err(|source| FlowWorkspaceSourceError::Io {
            path: candidate.clone(),
            source,
        })?;
        if !canonical_candidate.starts_with(&canonical_root) {
            return Err(FlowWorkspaceSourceError::PathEscapesRoot {
                path: canonical_candidate,
            });
        }
        Ok(canonical_candidate)
    }
}

/// Knowledge documents stored in a TurboVault-managed directory.
pub(super) struct Documents {
    manager: Arc<VaultManager>,
    semantic_index: Arc<SemanticIndexCache>,
    semantic_build: Mutex<()>,
    vault_path: PathBuf,
    trash_path: PathBuf,
    config: KnowledgeConfig,
    change_monitor: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for Documents {
    fn drop(&mut self) {
        if let Some(monitor) = self.change_monitor.take() {
            monitor.abort();
        }
    }
}

impl Documents {
    #[cfg(test)]
    fn new(root: &Path) -> KnowledgeResult<Self> {
        Self::new_with_config(root, &KnowledgeConfig::default())
    }

    /// Open an existing Markdown directory without requiring Obsidian metadata.
    fn new_with_config(root: &Path, config: &KnowledgeConfig) -> KnowledgeResult<Self> {
        let vault_path = validate_vault_path(&root.to_string_lossy())?;
        let trash_path = validate_root_component(&config.trash_path)?;
        let mut server_config = ServerConfig::new();
        server_config.reconcile_external_changes = true;
        server_config.max_file_size = u64::MAX;
        server_config.allowed_extensions = config
            .allowed_extensions
            .iter()
            .map(|extension| {
                format!(
                    ".{}",
                    extension
                        .trim()
                        .trim_start_matches('.')
                        .to_ascii_lowercase()
                )
            })
            .collect();
        server_config.excluded_paths.extend(
            config
                .excluded_paths
                .iter()
                .filter(|path| !path.contains('/') && !path.contains('\\'))
                .cloned(),
        );
        server_config
            .excluded_paths
            .insert(path_to_backend_id(&trash_path));
        let vault_config = VaultConfig::builder("kea-knowledge", &vault_path)
            .as_default()
            .reconcile_external_changes(true)
            .build()?;
        server_config.vaults.push(vault_config);

        let manager = Arc::new(VaultManager::new(server_config)?);
        let semantic_index = Arc::new(SemanticIndexCache::default());
        let weak_index = Arc::downgrade(&semantic_index);
        manager.set_change_listener(Arc::new(move |_changes| {
            let index = weak_index.upgrade();
            Box::pin(async move {
                if let Some(index) = index {
                    index.invalidate();
                }
            })
        }));
        Ok(Self {
            manager,
            semantic_index,
            semantic_build: Mutex::new(()),
            config: config.clone(),
            vault_path,
            trash_path,
            change_monitor: None,
        })
    }

    fn note_path_for_create(&self, request: &VaultCreateKnowledgeRequest) -> KnowledgeResult<PathBuf> {
        let seed = request
            .backend_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| title_to_filename(&request.title));
        let mut relative = normalize_relative_path(&seed)?;
        if relative.extension().is_none() {
            relative.set_extension(DEFAULT_NOTE_EXTENSION);
        }
        self.unique_path(relative)
    }

    fn note_path_for_directory_create(
        &self,
        request: &VaultCreateKnowledgeRequest,
        source_relative_path: &Path,
    ) -> KnowledgeResult<PathBuf> {
        let source = normalize_relative_path(&path_to_backend_id(source_relative_path))?;
        let prefix = request
            .backend_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(normalize_relative_path)
            .transpose()?
            .unwrap_or_default();
        self.unique_path(prefix.join(source))
    }

    fn unique_path(&self, relative: PathBuf) -> KnowledgeResult<PathBuf> {
        let mut candidate = relative;
        let extension = candidate
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or(DEFAULT_NOTE_EXTENSION)
            .to_string();
        let stem = candidate
            .file_stem()
            .and_then(|value| value.to_str())
            .filter(|value| !value.is_empty())
            .unwrap_or("note")
            .to_string();
        let parent = candidate
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_default();
        let mut suffix = 1_u32;

        while self.absolute_path(&candidate).exists() {
            candidate = parent.join(format!("{stem}-{suffix}.{extension}"));
            suffix += 1;
        }

        Ok(candidate)
    }

    async fn read_vault_record(&self, relative: &Path) -> KnowledgeResult<Option<VaultKnowledge>> {
        let absolute = self.absolute_path(relative);
        if !absolute.exists() {
            return Ok(None);
        }
        let raw_content = self
            .manager
            .read_file(relative)
            .await
            .map_err(|error| vault_read_error(relative, error))?;
        Ok(Some(
            self.vault_knowledge_from_content(relative, raw_content)
                .await?,
        ))
    }

    async fn vault_knowledge_from_content(
        &self,
        relative: &Path,
        raw_content: String,
    ) -> KnowledgeResult<VaultKnowledge> {
        let parsed = ParsedContent::parse(&raw_content);
        // The TurboVault parser retains the `tags` key in frontmatter data
        // even though it also extracts tags into a first-class field. Strip
        // `tags` from the frontmatter map so callers see only user metadata;
        // the canonical tags live in the separate `tags` field below.
        let mut frontmatter: Map<String, Value> = parsed
            .frontmatter
            .as_ref()
            .map(|frontmatter| frontmatter.data.clone().into_iter().collect())
            .unwrap_or_default();
        frontmatter.remove("tags");
        let content = if relative
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("md") || extension.eq_ignore_ascii_case("markdown"))
        {
            let (_, body) = turbovault_tools::file_tools::split_frontmatter(&raw_content);
            body.trim_start_matches('\n').to_owned()
        } else {
            raw_content.clone()
        };
        let title = resolve_title(relative, &frontmatter);
        let mut tags = parsed
            .tags
            .into_iter()
            .map(|tag| tag.name)
            .collect::<Vec<_>>();
        if let Some(frontmatter_tags) = parsed
            .frontmatter
            .as_ref()
            .map(|frontmatter| frontmatter.tags())
        {
            tags.extend(frontmatter_tags);
        }
        tags.sort();
        tags.dedup();

        let metadata = self.absolute_path(relative).metadata().ok();
        let created_at = metadata
            .as_ref()
            .and_then(|metadata| metadata.created().ok())
            .map(system_time_to_rfc3339);
        let updated_at = metadata
            .as_ref()
            .and_then(|metadata| metadata.modified().ok())
            .map(system_time_to_rfc3339);
        let path = path_to_backend_id(relative);

        Ok(VaultKnowledge {
            backend_id: path.clone(),
            path,
            title,
            content,
            content_type: KnowledgeContentType::Markdown,
            frontmatter,
            tags,
            created_at,
            updated_at,
        })
    }

    fn discover_tree(&self) -> KnowledgeResult<VaultKnowledgeTree> {
        let mut entries = Vec::new();
        let mut directories = std::collections::BTreeSet::new();
        for note in self.manager.scan_vault_with_metadata()? {
            let relative = self.relative_path(&note.path)?;
            if self.config.excludes_path(&self.vault_path, &relative) {
                continue;
            }
            let mut parent = relative.parent();
            while let Some(directory) = parent {
                if directory.as_os_str().is_empty() {
                    break;
                }
                directories.insert(directory.to_path_buf());
                parent = directory.parent();
            }
            let path = path_to_backend_id(&relative);
            entries.push(VaultKnowledgeTreeEntry {
                backend_id: path.clone(),
                path,
                kind: VaultKnowledgeTreeEntryKind::File,
            });
        }
        for directory in directories {
            let path = path_to_backend_id(&directory);
            entries.push(VaultKnowledgeTreeEntry {
                backend_id: path.clone(),
                path,
                kind: VaultKnowledgeTreeEntryKind::Directory,
            });
        }

        entries.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(entries)
    }

    async fn list_records(&self, request: &VaultListKnowledgeRequest) -> KnowledgeResult<Vec<VaultKnowledge>> {
        let mut records = Vec::new();
        let limit = normalize_limit(request.limit);
        let mut notes = self
            .manager
            .scan_vault_with_metadata()?
            .into_iter()
            .map(|note| self.relative_path(&note.path))
            .collect::<KnowledgeResult<Vec<_>>>()?;
        notes.sort();
        for path in notes {
            if self.config.excludes_path(&self.vault_path, &path) {
                continue;
            }
            // Listing skips files that fail safety filters (size limits,
            // extension checks) rather than failing the entire operation.
            // Direct get/write paths still reject with a typed error.
            let record = match self.read_vault_record(&path).await {
                Ok(Some(record)) => record,
                Ok(None) => continue,
                Err(KnowledgeError::InvalidBackendConfig { .. }) => continue,
                Err(KnowledgeError::InvalidTextEncoding { path }) => {
                    tracing::debug!(path, "skipping non-UTF-8 Obsidian knowledge file");
                    continue;
                }
                Err(error) => return Err(error),
            };
            if !matches_filters(&record, &request.filters) {
                continue;
            }
            records.push(record);
            if records.len() >= limit {
                break;
            }
        }
        Ok(records)
    }

    async fn search_records(
        &self,
        request: &VaultSearchKnowledgeRequest,
    ) -> KnowledgeResult<VaultKnowledgeSearchResponse> {
        let query = request.query.trim();
        let results = if query.is_empty() {
            self.list_records(&VaultListKnowledgeRequest {
                query: None,
                filters: request.filters.clone(),
                limit: request.limit,
            })
            .await?
            .into_iter()
            .map(|knowledge| VaultKnowledgeSearchResult { knowledge, score: 1.0 })
            .collect()
        } else {
            let index = self.semantic_engine().await?;
            let mut results = Vec::new();
            for candidate in index.semantic_search(query, index.document_count()) {
                let path = Path::new(&candidate.path);
                if self.config.excludes_path(&self.vault_path, path) {
                    continue;
                }
                let record = match self.read_vault_record(path).await {
                    Ok(Some(record)) => record,
                    Ok(None) | Err(KnowledgeError::InvalidBackendConfig { .. }) => continue,
                    Err(KnowledgeError::InvalidTextEncoding { path }) => {
                        tracing::debug!(path, "skipping non-UTF-8 knowledge file in semantic search");
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                if matches_filters(&record, &request.filters) {
                    results.push(VaultKnowledgeSearchResult {
                        knowledge: record,
                        score: candidate.score,
                    });
                    if results.len() >= normalize_limit(request.limit) {
                        break;
                    }
                }
            }
            results
        };

        Ok(VaultKnowledgeSearchResponse {
            query: request.query.clone(),
            results,
        })
    }

    async fn semantic_engine(&self) -> KnowledgeResult<Arc<SimilarityEngine>> {
        self.manager.ensure_fresh().await;
        if let Some(index) = self.semantic_index.current() {
            return Ok(index);
        }

        let _build = self.semantic_build.lock().await;
        if let Some(index) = self.semantic_index.current() {
            return Ok(index);
        }

        for _ in 0..2 {
            let generation = self.semantic_index.generation.load(Ordering::Acquire);
            let index = Arc::new(SimilarityEngine::new(Arc::clone(&self.manager)).await?);
            if self.semantic_index.publish(generation, Arc::clone(&index)) {
                return Ok(index);
            }
        }

        Err(KnowledgeError::StoreConsistency {
            operation: "semantic_search".to_owned(),
            message: "vault changed while building the semantic index".to_owned(),
        })
    }

    pub(super) async fn reconcile_external_changes(&self) {
        self.manager.reconcile_now().await;
    }

    fn absolute_path(&self, relative: &Path) -> PathBuf {
        self.vault_path.join(relative)
    }

    fn relative_path(&self, absolute: &Path) -> KnowledgeResult<PathBuf> {
        absolute
            .strip_prefix(&self.vault_path)
            .map(Path::to_path_buf)
            .map_err(|err| {
                DocumentError::InvalidPath {
                    message: err.to_string(),
                }
                .into()
            })
    }
}

impl Documents {
    fn start_change_monitor(&mut self) -> mpsc::UnboundedReceiver<()> {
        let (change_tx, change_rx) = mpsc::unbounded_channel();
        let weak_index = Arc::downgrade(&self.semantic_index);
        self.manager.set_change_listener(Arc::new(move |_changes| {
            let index = weak_index.upgrade();
            let change_tx = change_tx.clone();
            Box::pin(async move {
                if let Some(index) = index {
                    index.invalidate();
                }
                let _ = change_tx.send(());
            })
        }));
        let manager = Arc::downgrade(&self.manager);
        self.change_monitor = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let Some(manager) = manager.upgrade() else {
                    break;
                };
                manager.ensure_fresh().await;
            }
        }));
        change_rx
    }

    pub(super) async fn create(&self, request: ResolvedCreateKnowledgeRequest) -> KnowledgeResult<KnowledgeRecord> {
        let (request, content, path) = match request {
            ResolvedCreateKnowledgeRequest::Single { request, content } => {
                let path = self.note_path_for_create(&request)?;
                (request, content, path)
            }
            ResolvedCreateKnowledgeRequest::DirectoryFile {
                request,
                content,
                source_relative_path,
            } => {
                let path = self.note_path_for_directory_create(&request, &source_relative_path)?;
                (request, content, path)
            }
        };
        let content = compose_note_content(&request.frontmatter, &request.tags, &request.title, &content);
        self.manager
            .write_file(
                &path,
                &content,
                Precondition::for_replace(None, false),
                "kea.knowledge.obsidian.create",
            )
            .await?;
        let record = self
            .read_vault_record(&path)
            .await?
            .ok_or_else(|| KnowledgeError::StoreConsistency {
                operation: "obsidian.create".to_string(),
                message: "created note could not be read back from the vault".to_string(),
            })?;
        Ok(record)
    }

    pub(super) async fn get(&self, backend_id: GetKnowledgeRequest) -> KnowledgeResult<KnowledgeRecordLookup> {
        let path = normalize_relative_path(&backend_id)?;
        let record = self.read_vault_record(&path).await?;
        Ok(record)
    }

    pub(super) async fn update(&self, request: UpdateKnowledgeRequest) -> KnowledgeResult<KnowledgeRecord> {
        let current_path = normalize_relative_path(&request.backend_id)?;
        let Some(current) = self.read_vault_record(&current_path).await? else {
            return Err(DocumentError::NotFound {
                path: request.backend_id,
            }
            .into());
        };
        let destination = match &request.path {
            Some(path) => normalize_relative_path(path)?,
            None => current_path.clone(),
        };
        let title = request.title.clone().unwrap_or(current.title);
        let mut frontmatter = request.frontmatter.clone().unwrap_or(current.frontmatter);
        frontmatter.insert("title".to_string(), Value::String(title.clone()));
        let tags = request.tags.clone().unwrap_or(current.tags);
        let content_body = request.content.clone().unwrap_or(current.content);
        let content = compose_note_content(&frontmatter, &tags, &title, &content_body);

        if destination != current_path {
            if self.absolute_path(&destination).exists() {
                return Err(DocumentError::PathAlreadyExists {
                    path: path_to_backend_id(&destination),
                }
                .into());
            }
            self.manager
                .move_file(
                    &current_path,
                    &destination,
                    Precondition::for_in_place(None),
                    Precondition::for_replace(None, false),
                    "kea.knowledge.obsidian.update.move",
                )
                .await?;
        }

        self.manager
            .write_file(
                &destination,
                &content,
                Precondition::for_in_place(None),
                "kea.knowledge.obsidian.update.write",
            )
            .await?;
        let record = self
            .read_vault_record(&destination)
            .await?
            .ok_or_else(|| KnowledgeError::StoreConsistency {
                operation: "obsidian.update".to_string(),
                message: "updated note could not be read back from the vault".to_string(),
            })?;
        Ok(record)
    }

    pub(super) async fn delete(&self, backend_id: DeleteKnowledgeRequest) -> KnowledgeResult<DeleteKnowledgeResult> {
        let path = normalize_relative_path(&backend_id)?;
        if !self.absolute_path(&path).exists() {
            return Ok(DeleteKnowledgeResult {
                backend_id,
                deleted: false,
            });
        }

        let Some(file_name) = path.file_name() else {
            return Err(DocumentError::InvalidPath {
                message: format!("vault note path has no file name: {backend_id}"),
            }
            .into());
        };
        let trash_target = self.unique_path(self.trash_path.join(file_name))?;
        let source = self.manager.resolve_path(&path)?;
        let target = PathValidator::resolve_in_vault(&self.vault_path, &trash_target)?.path;
        tokio::fs::create_dir_all(target.parent().ok_or_else(|| DocumentError::InvalidPath {
            message: "trash destination has no parent directory".to_owned(),
        })?)
        .await
        .map_err(|err| DocumentError::Io {
            path: path_to_backend_id(&self.trash_path),
            message: err.to_string(),
        })?;
        // Hard-link creation is atomic and fails if the target already exists,
        // unlike rename on Unix. If removing the source fails, both names remain
        // as recovery copies and the error is surfaced without losing the note.
        tokio::fs::hard_link(&source, &target)
            .await
            .map_err(|err| DocumentError::Io {
                path: path_to_backend_id(&path),
                message: err.to_string(),
            })?;
        tokio::fs::remove_file(&source)
            .await
            .map_err(|err| DocumentError::Io {
                path: path_to_backend_id(&path),
                message: format!(
                    "trash copy created at {}; original retained: {err}",
                    path_to_backend_id(&trash_target)
                ),
            })?;
        self.manager.reconcile_now().await;
        Ok(DeleteKnowledgeResult {
            backend_id,
            deleted: true,
        })
    }

    pub(super) async fn list(&self, request: ListKnowledgeRequest) -> KnowledgeResult<KnowledgeRecordList> {
        if let Some(query) = request
            .query
            .as_deref()
            .map(str::trim)
            .filter(|query| !query.is_empty())
        {
            let results = self
                .search_records(&VaultSearchKnowledgeRequest {
                    query: query.to_owned(),
                    filters: request.filters,
                    limit: request.limit,
                })
                .await?;
            return Ok(results
                .results
                .into_iter()
                .map(|result| result.knowledge)
                .collect());
        }
        self.list_records(&request).await
    }

    pub(super) async fn list_tree(&self) -> KnowledgeResult<VaultKnowledgeTree> {
        self.discover_tree()
    }

    pub(super) async fn search(&self, request: SearchKnowledgeRequest) -> KnowledgeResult<KnowledgeSearchOutcome> {
        self.search_records(&request).await
    }
}

fn validate_vault_path(path: &str) -> KnowledgeResult<PathBuf> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(DocumentError::MissingVaultPath.into());
    }
    let path = PathBuf::from(trimmed);
    if !path.is_dir() {
        return Err(DocumentError::InvalidVaultPath {
            path: path.display().to_string(),
        }
        .into());
    }
    Ok(path)
}

fn normalize_relative_path(value: &str) -> KnowledgeResult<PathBuf> {
    let trimmed = value.trim().trim_matches('/');
    if trimmed.is_empty() {
        return Ok(PathBuf::new());
    }
    let path = PathBuf::from(trimmed);
    if path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
    {
        return Err(DocumentError::InvalidPath {
            message: format!("vault paths must be relative and stay inside the vault: {value}"),
        }
        .into());
    }
    Ok(path)
}

fn compose_note_content(frontmatter: &Map<String, Value>, tags: &[String], _title: &str, content: &str) -> String {
    let mut frontmatter = frontmatter.clone();
    // Tags are written into frontmatter so the note is self-describing on disk;
    // the TurboVault parser extracts them into a separate first-class field.
    if !tags.is_empty() && !frontmatter.contains_key("tags") {
        frontmatter.insert(
            "tags".to_string(),
            Value::Array(tags.iter().map(|tag| Value::String(tag.clone())).collect()),
        );
    }

    let reconstructed = turbovault_tools::file_tools::reconstruct_content(Some(&frontmatter), content);
    if frontmatter.is_empty() {
        content.to_owned()
    } else {
        reconstructed
    }
}

fn validate_root_component(path: &Path) -> KnowledgeResult<PathBuf> {
    let mut components = path.components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => Ok(path.to_path_buf()),
        _ => Err(DocumentError::InvalidPath {
            message: "trash_path must be one normal root-level path component".to_owned(),
        }
        .into()),
    }
}

fn resolve_title(path: &Path, frontmatter: &Map<String, Value>) -> String {
    frontmatter
        .get("title")
        .and_then(Value::as_str)
        .filter(|title| !title.trim().is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            path.file_stem()
                .and_then(|stem| stem.to_str())
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| path_to_backend_id(path))
}

fn title_to_filename(title: &str) -> String {
    let mut filename = title
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    while filename.contains("--") {
        filename = filename.replace("--", "-");
    }
    let filename = filename.trim_matches('-');
    if filename.is_empty() {
        format!("note.{DEFAULT_NOTE_EXTENSION}")
    } else {
        format!("{filename}.{DEFAULT_NOTE_EXTENSION}")
    }
}

fn matches_filters(record: &VaultKnowledge, filters: &VaultKnowledgeFilters) -> bool {
    if let Some(path_prefix) = filters
        .path_prefix
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        && !record
            .path
            .starts_with(path_prefix.trim().trim_matches('/'))
    {
        return false;
    }
    if let Some(tags) = &filters.tags
        && !tags.is_empty()
        && !tags
            .iter()
            .any(|tag| record.tags.iter().any(|record_tag| record_tag == tag))
    {
        return false;
    }
    if let Some(frontmatter) = &filters.frontmatter
        && !frontmatter
            .iter()
            .all(|(key, value)| record.frontmatter.get(key) == Some(value))
    {
        return false;
    }
    if let Some(content_type) = &filters.content_type
        && &record.content_type != content_type
    {
        return false;
    }
    if !timestamp_in_bounds(
        record.created_at.as_deref(),
        filters.created_after.as_deref(),
        filters.created_before.as_deref(),
    ) {
        return false;
    }
    timestamp_in_bounds(
        record.updated_at.as_deref(),
        filters.updated_after.as_deref(),
        filters.updated_before.as_deref(),
    )
}

fn path_to_backend_id(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(value) => value.to_str().map(ToOwned::to_owned),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn normalize_limit(limit: u32) -> usize {
    if limit == 0 { 100 } else { limit.min(100) as usize }
}

fn vault_read_error(path: &Path, error: turbovault_core::Error) -> KnowledgeError {
    match error {
        turbovault_core::Error::Io(error) if error.kind() == std::io::ErrorKind::InvalidData => {
            KnowledgeError::InvalidTextEncoding {
                path: path_to_backend_id(path),
            }
        }
        error => error.into(),
    }
}

#[derive(Debug, Error)]
enum DocumentError {
    #[error("invalid path: {message}")]
    InvalidPath { message: String },
    #[error("vault path is required")]
    MissingVaultPath,
    #[error("vault path is not an existing directory: {path}")]
    InvalidVaultPath { path: String },
    #[error("vault note was not found: {path}")]
    NotFound { path: String },
    #[error("vault path already exists: {path}")]
    PathAlreadyExists { path: String },
    #[error("I/O error at {path}: {message}")]
    Io { path: String, message: String },
    #[error("TurboVault operation failed: {message}")]
    TurboVault { message: String },
}

impl From<DocumentError> for KnowledgeError {
    fn from(error: DocumentError) -> Self {
        match error {
            DocumentError::MissingVaultPath => KnowledgeError::InvalidBackendConfig {
                backend: BACKEND.to_string(),
                message: error.to_string(),
            },
            DocumentError::InvalidVaultPath { .. }
            | DocumentError::InvalidPath { .. }
            | DocumentError::PathAlreadyExists { .. } => KnowledgeError::InvalidBackendConfig {
                backend: BACKEND.to_string(),
                message: error.to_string(),
            },
            DocumentError::NotFound { path } => KnowledgeError::InvalidStoreValue {
                field: "backend_id".to_string(),
                value: Some(path),
                message: "vault note was not found".to_string(),
            },
            DocumentError::Io { path, message } => KnowledgeError::Io { path, message },
            DocumentError::TurboVault { message } => KnowledgeError::StoreConsistency {
                operation: "obsidian.turbovault".to_string(),
                message,
            },
        }
    }
}

impl From<turbovault_core::Error> for KnowledgeError {
    fn from(error: turbovault_core::Error) -> Self {
        DocumentError::TurboVault {
            message: error.to_string(),
        }
        .into()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    fn documents(root: &Path) -> Documents {
        Documents::new(root).expect("open directory")
    }

    fn create_request(title: &str, path: Option<&str>) -> ResolvedCreateKnowledgeRequest {
        ResolvedCreateKnowledgeRequest::Single {
            request: VaultCreateKnowledgeRequest {
                backend_id: path.map(str::to_owned),
                title: title.to_owned(),
                content: None,
                input_file: None,
                content_type: KnowledgeContentType::Markdown,
                frontmatter: Map::new(),
                tags: Vec::new(),
            },
            content: "# saved\nbody".to_owned(),
        }
    }

    #[tokio::test]
    async fn directory_crud_search_and_tree_have_no_backend_discriminator() {
        let root = tempdir().expect("tempdir");
        let plugin = documents(root.path());
        let first = plugin
            .create(create_request("Alpha", Some("folder/alpha.md")))
            .await
            .expect("create note");
        let second = plugin
            .create(create_request("Alpha", Some("folder/alpha.md")))
            .await
            .expect("collision suffix");
        assert_ne!(first.backend_id, second.backend_id);
        assert_eq!(
            plugin
                .get(first.backend_id.clone())
                .await
                .expect("get note")
                .expect("note")
                .content,
            "# saved\nbody"
        );
        let listed = plugin
            .list(VaultListKnowledgeRequest {
                query: None,
                filters: VaultKnowledgeFilters::default(),
                limit: 10,
            })
            .await
            .expect("list notes");
        assert_eq!(listed.len(), 2);
        tokio::fs::write(root.path().join("other.md"), "# Other\nunrelated document")
            .await
            .expect("write an unrelated note");
        let searched = plugin
            .search(VaultSearchKnowledgeRequest {
                query: "saved".to_owned(),
                filters: VaultKnowledgeFilters::default(),
                limit: 10,
            })
            .await
            .expect("search notes");
        assert_eq!(searched.results.len(), 2);
        assert!(
            plugin
                .list_tree()
                .await
                .expect("tree")
                .iter()
                .any(|entry| entry.path == "folder")
        );
        assert!(
            plugin
                .delete(first.backend_id.clone())
                .await
                .expect("delete note")
                .deleted
        );
        assert!(
            plugin
                .get(first.backend_id)
                .await
                .expect("get deleted note")
                .is_none()
        );
    }

    #[tokio::test]
    async fn existing_directories_work_without_obsidian_metadata_or_configuration() {
        let root = tempdir().expect("tempdir");
        tokio::fs::write(root.path().join("plain.md"), "plain markdown")
            .await
            .expect("write ordinary file");
        let plugin = documents(root.path());
        assert_eq!(
            plugin
                .get("plain.md".to_owned())
                .await
                .expect("read file")
                .expect("note")
                .content,
            "plain markdown"
        );
    }

    #[test]
    fn flow_workspace_source_uses_the_current_shared_root() {
        let first = tempdir().expect("first root");
        let second = tempdir().expect("second root");
        std::fs::create_dir_all(first.path().join("research")).expect("first directory");
        std::fs::create_dir_all(second.path().join("research")).expect("second directory");
        let shared = Arc::new(RwLock::new(first.path().to_path_buf()));
        let source = KnowledgeWorkspaceSource::new(shared.clone());
        assert_eq!(
            source
                .resolve_known_directory("research")
                .expect("first root"),
            first
                .path()
                .join("research")
                .canonicalize()
                .expect("canonical first directory")
        );
        *shared.write().expect("root lock") = second.path().to_path_buf();
        assert_eq!(
            source
                .resolve_known_directory("research")
                .expect("new root"),
            second
                .path()
                .join("research")
                .canonicalize()
                .expect("canonical second directory")
        );
        assert!(source.resolve_known_directory("../research").is_err());
    }

    #[test]
    fn configured_root_must_exist_and_unsafe_relative_paths_are_rejected() {
        let root = tempdir().expect("tempdir");
        assert!(Documents::new(&root.path().join("missing")).is_err());
        assert!(normalize_relative_path("../escape.md").is_err());
    }

    #[tokio::test]
    async fn configured_exclusions_extensions_and_trash_are_absent_from_notes_and_tree() {
        let root = tempdir().expect("vault root");
        for directory in ["large-cache/nested", "private/nested", ".trash", "empty"] {
            tokio::fs::create_dir_all(root.path().join(directory))
                .await
                .expect("create directory");
        }
        for (path, content) in [
            ("large-cache/nested/hidden.md", "excluded subtree"),
            ("private/nested/hidden.txt", "excluded component"),
            (".trash/removed.md", "trashed"),
            ("research/keep.md", "markdown"),
            ("research/keep.txt", "text"),
            ("research/skip.pdf", "unsupported"),
        ] {
            let destination = root.path().join(path);
            tokio::fs::create_dir_all(destination.parent().expect("parent"))
                .await
                .expect("create parent");
            tokio::fs::write(destination, content)
                .await
                .expect("write file");
        }
        let config = KnowledgeConfig {
            root: Some(root.path().to_path_buf()),
            trash_path: PathBuf::from(".trash"),
            excluded_paths: vec!["large-cache".to_owned(), "private".to_owned()],
            allowed_extensions: vec!["md".to_owned(), "txt".to_owned()],
        };
        let plugin = Documents::new_with_config(root.path(), &config).expect("open configured vault");

        let listed = plugin
            .list(VaultListKnowledgeRequest {
                query: None,
                filters: VaultKnowledgeFilters::default(),
                limit: 20,
            })
            .await
            .expect("list configured notes");
        let paths = listed
            .iter()
            .map(|note| note.path.as_str())
            .collect::<Vec<_>>();
        assert_eq!(paths, vec!["research/keep.md", "research/keep.txt"]);

        let tree = plugin.list_tree().await.expect("configured tree");
        let paths = tree
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>();
        assert!(paths.contains(&"research"));
        assert!(paths.contains(&"research/keep.md"));
        assert!(paths.contains(&"research/keep.txt"));
        assert!(!paths.contains(&"empty"));
        assert!(
            !paths.iter().any(|path| path.starts_with("large-cache")
                || path.starts_with("private")
                || path.starts_with(".trash"))
        );
        assert!(!paths.contains(&"research/skip.pdf"));
    }

    #[tokio::test]
    async fn picked_paths_exclude_exact_files_and_subtrees_without_hiding_same_names_elsewhere() {
        let root = tempdir().expect("vault root");
        for file in [
            "research/private/hidden.md",
            "research/secret.md",
            "other/private/keep.md",
            "other/secret.md",
            "absolute/hidden.md",
        ] {
            let destination = root.path().join(file);
            tokio::fs::create_dir_all(destination.parent().expect("parent"))
                .await
                .expect("create directory");
            let content = if file == "absolute/hidden.md" {
                "unrelated control content"
            } else {
                "research evidence"
            };
            tokio::fs::write(destination, content)
                .await
                .expect("write note");
        }
        let config = KnowledgeConfig {
            excluded_paths: vec![
                "./research/private".to_owned(),
                "./research/secret.md".to_owned(),
                root.path().join("absolute").display().to_string(),
            ],
            ..KnowledgeConfig::default()
        };
        let plugin = Documents::new_with_config(root.path(), &config).expect("open vault");
        let listed = plugin
            .list(VaultListKnowledgeRequest {
                query: None,
                filters: VaultKnowledgeFilters::default(),
                limit: 20,
            })
            .await
            .expect("list notes");
        assert_eq!(
            listed
                .iter()
                .map(|note| note.path.as_str())
                .collect::<Vec<_>>(),
            vec!["other/private/keep.md", "other/secret.md"]
        );
        let tree = plugin.list_tree().await.expect("tree");
        assert!(
            !tree
                .iter()
                .any(|entry| entry.path.starts_with("research") || entry.path.starts_with("absolute"))
        );
        let searched = plugin
            .search_records(&VaultSearchKnowledgeRequest {
                query: "research evidence".to_owned(),
                filters: VaultKnowledgeFilters::default(),
                limit: 20,
            })
            .await
            .expect("search notes");
        assert_eq!(searched.results.len(), 2);
        assert!(
            searched
                .results
                .iter()
                .all(|result| result.knowledge.path.starts_with("other/"))
        );
    }

    #[tokio::test]
    async fn delete_preserves_existing_trash_file_and_moves_note_to_a_distinct_target() {
        let root = tempdir().expect("vault root");
        tokio::fs::create_dir_all(root.path().join(".trash"))
            .await
            .expect("create trash");
        tokio::fs::write(root.path().join("note.md"), "source note")
            .await
            .expect("write source");
        tokio::fs::write(root.path().join(".trash/note.md"), "pre-existing trash")
            .await
            .expect("write existing trash");
        let plugin = documents(root.path());

        let result = plugin
            .delete("note.md".to_owned())
            .await
            .expect("soft delete");

        assert!(result.deleted);
        assert_eq!(
            tokio::fs::read_to_string(root.path().join(".trash/note.md"))
                .await
                .expect("existing target"),
            "pre-existing trash"
        );
        assert_eq!(
            tokio::fs::read_to_string(root.path().join(".trash/note-1.md"))
                .await
                .expect("new trash target"),
            "source note"
        );
        assert!(!root.path().join("note.md").exists());
    }
}
