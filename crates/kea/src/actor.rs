use std::{
    io,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::sync::mpsc;
use uuid::Uuid;

use turbovault_core::{ServerConfig, VaultConfig};
use turbovault_vault::VaultManager;

use aves::bus::BusRpcContext;
use aves::config::{GetConfigSectionInput, dispatch_get_config_section};
use aves::utils::time::system_time_to_rfc3339;
use aves::utils::uuid::SpectaUuid;

use super::data::DataStore;
use super::documents::{Documents, open_documents};
use super::messages::{KnowledgeRequest, KnowledgeRequestPayload, KnowledgeResponse};
use super::types::{
    CanvasDashboard, CanvasDashboardDeleteResult, CanvasDashboardLookup, CanvasDashboardSummary,
    CanvasDashboardSummaryList, CreateCanvasDashboardRequest, CreateKnowledgeRequest, DeleteCanvasDashboardRequest,
    DeleteKnowledgeRequest, GetCanvasDashboardRequest, GetKnowledgeRequest, KnowledgeConfig, KnowledgeError,
    ListCanvasDashboardsRequest, ResolvedCreateKnowledgeRequest, UpdateCanvasDashboardRequest,
    VaultCreateKnowledgeRequest,
};

enum ResolvedCreateWork {
    Single(ResolvedCreateKnowledgeRequest),
    Directory {
        request: VaultCreateKnowledgeRequest,
        root: PathBuf,
        sources: Vec<(PathBuf, PathBuf)>,
    },
}

#[derive(Debug, Deserialize, Serialize)]
struct CanvasDashboardRecord {
    title: String,
    html: String,
}

/// Knowledge actor that owns note storage and the separate JSON object store.
pub(super) struct Actor {
    /// Root directory for user-owned configuration artifacts.
    config_dir: PathBuf,

    /// Active TurboVault directory.
    notes: Documents,

    /// Receiver for knowledge requests.
    receiver: mpsc::Receiver<KnowledgeRequest>,

    /// Active directory configuration, reloaded after a saved config change.
    knowledge_config: KnowledgeConfig,

    /// Root shared with Flow's workspace source so saved changes take effect without restart.
    shared_root: Arc<RwLock<PathBuf>>,

    /// Directory change stream.
    change_rx: Option<mpsc::UnboundedReceiver<()>>,

    /// Centralized runtime context for cross-domain dispatch.
    root_context: BusRpcContext,

    /// Separate JSON object store; notes never enter this database.
    data: DataStore,

    /// Sender for actor responses that should become broadcasts.
    broadcast_tx: mpsc::Sender<KnowledgeResponse>,
}

impl Actor {
    pub(super) async fn new(
        receiver: mpsc::Receiver<KnowledgeRequest>,
        root_context: BusRpcContext,
        config_dir: PathBuf,
        knowledge_config: KnowledgeConfig,
        shared_root: Arc<RwLock<PathBuf>>,
        broadcast_tx: mpsc::Sender<KnowledgeResponse>,
    ) -> Self {
        let runtime = match open_documents(&config_dir, &knowledge_config).await {
            Ok(runtime) => runtime,
            Err(error) => panic!("knowledge actor boot failed to open note directory: {error}",),
        };
        let data = DataStore::open(&config_dir)
            .await
            .unwrap_or_else(|error| panic!("knowledge data store boot failed: {error}"));

        Self {
            config_dir,
            notes: runtime.notes,
            receiver,
            knowledge_config,
            shared_root,
            change_rx: runtime.change_rx,
            root_context,
            data,
            broadcast_tx,
        }
    }

    /// Handle incoming knowledge messages.
    async fn handle_message(&mut self, request: KnowledgeRequest) {
        let KnowledgeRequest { payload, reply_to, .. } = request;

        let result: Result<KnowledgeResponse, KnowledgeError> = match payload {
            KnowledgeRequestPayload::KnowledgeConfigChanged => {
                self.reload_knowledge_config().await;
                return;
            }
            KnowledgeRequestPayload::StopRuntime => {
                self.data.maintain_db().await;
                Ok(KnowledgeResponse::RuntimeStopped)
            }
            KnowledgeRequestPayload::CreateKnowledge { request } => self.handle_create_knowledge(request).await,
            KnowledgeRequestPayload::GetKnowledge { request } => self.handle_get_knowledge(request).await,
            KnowledgeRequestPayload::UpdateKnowledge { request } => self
                .notes
                .update(request)
                .await
                .map(|knowledge| KnowledgeResponse::KnowledgeUpdated { knowledge }),
            KnowledgeRequestPayload::DeleteKnowledge { request } => self.handle_delete_knowledge(request).await,
            KnowledgeRequestPayload::ListKnowledge { request } => self
                .notes
                .list(request)
                .await
                .map(KnowledgeResponse::KnowledgeListed),
            KnowledgeRequestPayload::ListKnowledgeTree => self
                .notes
                .list_tree()
                .await
                .map(|entries| KnowledgeResponse::KnowledgeTreeListed { entries }),
            KnowledgeRequestPayload::SearchKnowledge { request } => self
                .notes
                .search(request)
                .await
                .map(KnowledgeResponse::KnowledgeSearched),
            KnowledgeRequestPayload::UpsertData { request } => self
                .data
                .upsert(request)
                .await
                .map(|data| KnowledgeResponse::DataUpserted { data }),
            KnowledgeRequestPayload::GetData { key } => self
                .data
                .get(&key)
                .await
                .map(|data| KnowledgeResponse::DataFound { data }),
            KnowledgeRequestPayload::ListData { request } => self
                .data
                .list(request)
                .await
                .map(|data| KnowledgeResponse::DataListed { data }),
            KnowledgeRequestPayload::DeleteData { key } => self
                .data
                .delete(&key)
                .await
                .map(|()| KnowledgeResponse::DataDeleted { key }),
            KnowledgeRequestPayload::CreateCanvasDashboard { request } => {
                self.handle_create_canvas_dashboard(request).await
            }
            KnowledgeRequestPayload::GetCanvasDashboard { request } => self.handle_get_canvas_dashboard(request).await,
            KnowledgeRequestPayload::UpdateCanvasDashboard { request } => {
                self.handle_update_canvas_dashboard(request).await
            }
            KnowledgeRequestPayload::DeleteCanvasDashboard { request } => {
                self.handle_delete_canvas_dashboard(request).await
            }
            KnowledgeRequestPayload::ListCanvasDashboards { request } => {
                self.handle_list_canvas_dashboards(request).await
            }
        };

        let _ = reply_to.send(result);
    }

    async fn discover_directory_sources(&self, root: &Path) -> Vec<(PathBuf, PathBuf)> {
        let mut server = ServerConfig::new();

        server.max_file_size = u64::MAX;
        server.allowed_extensions = self
            .knowledge_config
            .allowed_extensions
            .iter()
            .map(|extension| format!(".{}", extension.trim().trim_start_matches('.').to_lowercase()))
            .collect();

        server.excluded_paths.extend(
            self.knowledge_config
                .excluded_paths
                .iter()
                .filter(|path| !path.contains('/') && !path.contains('\\'))
                .cloned(),
        );

        if let Some(trash_path) = self
            .knowledge_config
            .trash_path
            .file_name()
            .and_then(|name| name.to_str())
        {
            server.excluded_paths.insert(trash_path.to_owned());
        }

        let vault = match VaultConfig::builder("kea-import", root)
            .as_default()
            .build()
        {
            Ok(vault) => vault,
            Err(error) => {
                tracing::trace!(path = %root.display(), error = %error, "skipping directory input after TurboVault configuration failure");
                return Vec::new();
            }
        };
        server.vaults.push(vault);

        let manager = match VaultManager::new(server) {
            Ok(manager) => manager,
            Err(error) => {
                tracing::trace!(path = %root.display(), error = %error, "skipping directory input after TurboVault manager failure");
                return Vec::new();
            }
        };

        let notes = match manager.scan_vault_with_metadata() {
            Ok(notes) => notes,
            Err(error) => {
                tracing::trace!(path = %root.display(), error = %error, "skipping directory input after TurboVault scan failure");
                return Vec::new();
            }
        };

        let mut sources = notes
            .into_iter()
            .map(|note| {
                let path = note.path;
                let relative = PathBuf::from(manager.relative_path(&path));
                (path, relative)
            })
            .filter(|(_, relative)| !self.knowledge_config.excludes_path(root, relative))
            .collect::<Vec<_>>();

        sources.sort_by(|left, right| left.1.cmp(&right.1));
        sources
    }

    async fn fetch_knowledge_config(root_context: &BusRpcContext) -> KnowledgeConfig {
        let knowledge = dispatch_get_config_section(
            root_context,
            GetConfigSectionInput {
                section: "knowledge".to_owned(),
            },
        )
        .await
        .expect("knowledge actor boot failed to read Knowledge section");
        serde_json::from_value(knowledge.value).expect("Knowledge section validated by Config")
    }

    async fn reload_knowledge_config(&mut self) {
        let next = Self::fetch_knowledge_config(&self.root_context).await;
        if next == self.knowledge_config {
            return;
        }
        match open_documents(&self.config_dir, &next).await {
            Ok(runtime) => {
                self.notes = runtime.notes;
                self.change_rx = runtime.change_rx;
                *self
                    .shared_root
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = next
                    .root
                    .clone()
                    .unwrap_or_else(|| self.config_dir.join("knowledge"));
                self.knowledge_config = next;
                let _ = self
                    .broadcast_tx
                    .send(KnowledgeResponse::KnowledgeRecordsChanged)
                    .await;
            }
            Err(error) => {
                tracing::error!(%error, "knowledge config reload was rejected; retaining active documents directory")
            }
        }
    }

    async fn handle_create_knowledge(
        &mut self,
        request: CreateKnowledgeRequest,
    ) -> Result<KnowledgeResponse, KnowledgeError> {
        match self.resolve_create_inputs(request).await? {
            ResolvedCreateWork::Single(request) => self
                .notes
                .create(request)
                .await
                .map(|knowledge| KnowledgeResponse::KnowledgeCreated { knowledge }),
            ResolvedCreateWork::Directory { request, root, sources } => {
                let mut first = None;
                for (source_path, source_relative_path) in sources {
                    let bytes = match fs::read(&source_path).await {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            tracing::trace!(
                                path = %source_path.display(),
                                error = %error,
                                "skipping unreadable directory input file"
                            );
                            continue;
                        }
                    };
                    let content = match String::from_utf8(bytes) {
                        Ok(content) => content,
                        Err(error) => {
                            tracing::trace!(
                                path = %source_path.display(),
                                error = %error,
                                "skipping non-UTF-8 directory input file"
                            );
                            continue;
                        }
                    };
                    let mut request = request.clone();
                    request.content = None;
                    request.input_file = None;
                    let resolved = ResolvedCreateKnowledgeRequest::DirectoryFile {
                        request,
                        content,
                        source_relative_path: source_relative_path.clone(),
                    };
                    match self.notes.create(resolved).await {
                        Ok(knowledge) => {
                            if first.is_none() {
                                first = Some(knowledge);
                            }
                        }
                        Err(error) => {
                            tracing::trace!(
                                directory = %root.display(),
                                source = %source_relative_path.display(),
                                error = %error,
                                "skipping directory input file after plugin create failure"
                            );
                        }
                    }
                }
                first
                    .map(|knowledge| KnowledgeResponse::KnowledgeCreated { knowledge })
                    .ok_or_else(|| KnowledgeError::NoDirectoryFilesImported {
                        path: root.display().to_string(),
                    })
            }
        }
    }

    async fn resolve_create_inputs(
        &self,
        request: CreateKnowledgeRequest,
    ) -> Result<ResolvedCreateWork, KnowledgeError> {
        let Some(input_file) = request.input_file.as_ref() else {
            return Ok(ResolvedCreateWork::Single(self.resolve_create_input(request).await?));
        };
        let effective_path = self.effective_create_path(input_file);
        let metadata = Self::create_file_metadata(&effective_path).await?;
        if !metadata.is_dir() {
            if !metadata.is_file() {
                return Err(KnowledgeError::Io {
                    path: effective_path.display().to_string(),
                    message: "input path is not a regular file".to_string(),
                });
            }
            let mut request = request;
            let content = Self::read_create_file(&effective_path).await?;
            request.content = None;
            request.input_file = None;
            return Ok(ResolvedCreateWork::Single(ResolvedCreateKnowledgeRequest::Single {
                request,
                content,
            }));
        }

        let sources = self.discover_directory_sources(&effective_path).await;
        if sources.is_empty() {
            return Err(KnowledgeError::NoDirectoryFilesImported {
                path: effective_path.display().to_string(),
            });
        }
        Ok(ResolvedCreateWork::Directory {
            request,
            root: effective_path,
            sources,
        })
    }

    async fn resolve_create_input(
        &self,
        request: CreateKnowledgeRequest,
    ) -> Result<ResolvedCreateKnowledgeRequest, KnowledgeError> {
        let mut request = request;
        let content = self
            .resolve_create_content(request.content.take(), request.input_file.take())
            .await?;
        Ok(ResolvedCreateKnowledgeRequest::Single { request, content })
    }

    async fn resolve_create_content(
        &self,
        content: Option<String>,
        input_file: Option<PathBuf>,
    ) -> Result<String, KnowledgeError> {
        let Some(input_file) = input_file else {
            return content.ok_or(KnowledgeError::MissingCreateInput);
        };

        let effective_path = self.effective_create_path(&input_file);
        let metadata = Self::create_file_metadata(&effective_path).await?;
        if !metadata.is_file() {
            return Err(KnowledgeError::Io {
                path: effective_path.display().to_string(),
                message: "input path is not a regular file".to_string(),
            });
        }
        let bytes = fs::read(&effective_path).await;
        Self::read_create_file_result(&effective_path, bytes)
    }

    async fn read_create_file(path: &Path) -> Result<String, KnowledgeError> {
        let bytes = fs::read(path).await;
        Self::read_create_file_result(path, bytes)
    }

    fn effective_create_path(&self, input_file: &Path) -> PathBuf {
        if input_file.is_absolute() {
            input_file.to_path_buf()
        } else {
            self.config_dir.join("discuss").join(input_file)
        }
    }

    async fn create_file_metadata(path: &Path) -> Result<std::fs::Metadata, KnowledgeError> {
        fs::metadata(path)
            .await
            .map_err(|error| KnowledgeError::Io {
                path: path.display().to_string(),
                message: format!(
                    "cannot read input_file `{}`: {error}. Agents should use absolute paths; relative paths are resolved only under config_dir/discuss",
                    path.display(),
                ),
            })
    }

    fn read_create_file_result(path: &Path, bytes: io::Result<Vec<u8>>) -> Result<String, KnowledgeError> {
        let bytes = bytes.map_err(|error| KnowledgeError::Io {
            path: path.display().to_string(),
            message: error.to_string(),
        })?;
        String::from_utf8(bytes).map_err(|_| KnowledgeError::InvalidTextEncoding {
            path: path.display().to_string(),
        })
    }

    async fn handle_get_knowledge(
        &mut self,
        request: GetKnowledgeRequest,
    ) -> Result<KnowledgeResponse, KnowledgeError> {
        let knowledge = self
            .notes
            .get(request.clone())
            .await?
            .ok_or(KnowledgeError::KnowledgeNotFound {
                backend: "obsidian".to_owned(),
                identifier: request,
            })?;
        Ok(KnowledgeResponse::KnowledgeFound { knowledge })
    }

    async fn handle_delete_knowledge(
        &mut self,
        request: DeleteKnowledgeRequest,
    ) -> Result<KnowledgeResponse, KnowledgeError> {
        let result = self.notes.delete(request).await?;
        Ok(KnowledgeResponse::KnowledgeDeleted { result })
    }

    async fn handle_create_canvas_dashboard(
        &mut self,
        request: CreateCanvasDashboardRequest,
    ) -> Result<KnowledgeResponse, KnowledgeError> {
        let dashboards_dir = self.config_dir.join("dashboards");
        fs::create_dir_all(&dashboards_dir)
            .await
            .map_err(|error| canvas_dashboard_io(&dashboards_dir, error))?;

        let id = SpectaUuid::from(Uuid::new_v4());
        let path = canvas_dashboard_path(&dashboards_dir, id);
        let record = CanvasDashboardRecord {
            title: request.title,
            html: request.html,
        };
        let (created_at, updated_at) = write_canvas_dashboard(&path, &record).await?;
        Ok(KnowledgeResponse::CanvasDashboardCreated {
            dashboard: CanvasDashboard {
                id,
                title: record.title,
                html: record.html,
                created_at,
                updated_at,
            },
        })
    }

    async fn handle_list_canvas_dashboards(
        &mut self,
        request: ListCanvasDashboardsRequest,
    ) -> Result<KnowledgeResponse, KnowledgeError> {
        let dashboards_dir = self.config_dir.join("dashboards");
        let mut entries = match fs::read_dir(&dashboards_dir).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(KnowledgeResponse::CanvasDashboardListed {
                    dashboards: CanvasDashboardSummaryList(Vec::new()),
                });
            }
            Err(error) => return Err(canvas_dashboard_io(&dashboards_dir, error)),
        };

        let mut dashboards = Vec::new();
        while dashboards.len() < request.limit as usize {
            let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|error| canvas_dashboard_io(&dashboards_dir, error))?
            else {
                break;
            };
            let path = entry.path();
            if entry
                .file_type()
                .await
                .map_err(|error| canvas_dashboard_io(&path, error))?
                .is_dir()
                || path.extension().and_then(|extension| extension.to_str()) != Some("json")
            {
                continue;
            }
            let Some(file_stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            let Ok(uuid) = Uuid::parse_str(file_stem) else {
                continue;
            };
            let id = SpectaUuid::from(uuid);
            let record = read_canvas_dashboard(&path)
                .await
                .map_err(|error| canvas_dashboard_io(&path, error))?;
            let (created_at, updated_at) = canvas_dashboard_timestamps(&path).await?;
            dashboards.push(CanvasDashboardSummary {
                id,
                title: record.title,
                created_at,
                updated_at,
            });
        }

        Ok(KnowledgeResponse::CanvasDashboardListed {
            dashboards: CanvasDashboardSummaryList(dashboards),
        })
    }

    async fn handle_get_canvas_dashboard(
        &mut self,
        request: GetCanvasDashboardRequest,
    ) -> Result<KnowledgeResponse, KnowledgeError> {
        let dashboards_dir = self.config_dir.join("dashboards");
        let path = canvas_dashboard_path(&dashboards_dir, request.id);
        let record = match read_canvas_dashboard(&path).await {
            Ok(record) => record,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(KnowledgeResponse::CanvasDashboardFound {
                    dashboard: CanvasDashboardLookup(None),
                });
            }
            Err(error) => return Err(canvas_dashboard_io(&path, error)),
        };
        let (created_at, updated_at) = canvas_dashboard_timestamps(&path).await?;
        Ok(KnowledgeResponse::CanvasDashboardFound {
            dashboard: CanvasDashboardLookup(Some(CanvasDashboard {
                id: request.id,
                title: record.title,
                html: record.html,
                created_at,
                updated_at,
            })),
        })
    }

    async fn handle_update_canvas_dashboard(
        &mut self,
        request: UpdateCanvasDashboardRequest,
    ) -> Result<KnowledgeResponse, KnowledgeError> {
        let dashboards_dir = self.config_dir.join("dashboards");
        let path = canvas_dashboard_path(&dashboards_dir, request.id);
        match fs::metadata(&path).await {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(KnowledgeError::CanvasDashboardNotFound { id: request.id });
            }
            Err(error) => return Err(canvas_dashboard_io(&path, error)),
        }

        let record = CanvasDashboardRecord {
            title: request.title,
            html: request.html,
        };
        let (created_at, updated_at) = write_canvas_dashboard(&path, &record).await?;
        Ok(KnowledgeResponse::CanvasDashboardUpdated {
            dashboard: CanvasDashboard {
                id: request.id,
                title: record.title,
                html: record.html,
                created_at,
                updated_at,
            },
        })
    }

    async fn handle_delete_canvas_dashboard(
        &mut self,
        request: DeleteCanvasDashboardRequest,
    ) -> Result<KnowledgeResponse, KnowledgeError> {
        let dashboards_dir = self.config_dir.join("dashboards");
        let path = canvas_dashboard_path(&dashboards_dir, request.id);
        let deleted = match fs::remove_file(&path).await {
            Ok(()) => true,
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(error) => return Err(canvas_dashboard_io(&path, error)),
        };
        Ok(KnowledgeResponse::CanvasDashboardDeleted {
            id: request.id,
            result: CanvasDashboardDeleteResult { deleted },
        })
    }
}

fn canvas_dashboard_path(dashboards_dir: &Path, id: SpectaUuid) -> PathBuf {
    dashboards_dir.join(format!("{id}.json"))
}

fn canvas_dashboard_io(path: &Path, error: io::Error) -> KnowledgeError {
    KnowledgeError::CanvasDashboardIo {
        path: path.display().to_string(),
        message: error.to_string(),
    }
}

async fn read_canvas_dashboard(path: &Path) -> io::Result<CanvasDashboardRecord> {
    let contents = fs::read_to_string(path).await?;
    serde_json::from_str(&contents).map_err(io::Error::other)
}

async fn write_canvas_dashboard(
    path: &Path,
    record: &CanvasDashboardRecord,
) -> Result<(String, String), KnowledgeError> {
    let contents = serde_json::to_vec(record).map_err(|error| KnowledgeError::MetadataSerialization {
        operation: "serialize canvas dashboard".to_string(),
        message: error.to_string(),
    })?;
    fs::write(path, contents)
        .await
        .map_err(|error| canvas_dashboard_io(path, error))?;
    canvas_dashboard_timestamps(path).await
}

async fn canvas_dashboard_timestamps(path: &Path) -> Result<(String, String), KnowledgeError> {
    let metadata = fs::metadata(path)
        .await
        .map_err(|error| canvas_dashboard_io(path, error))?;
    let modified = metadata
        .modified()
        .map_err(|error| canvas_dashboard_io(path, error))?;
    let created = metadata.created().unwrap_or(modified);
    Ok((system_time_to_rfc3339(created), system_time_to_rfc3339(modified)))
}

async fn receive_document_change(receiver: &mut Option<mpsc::UnboundedReceiver<()>>) -> Option<()> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => std::future::pending().await,
    }
}

pub(super) async fn run_actor(mut actor: Actor) {
    loop {
        tokio::select! {
            Some(message) = actor.receiver.recv() => match message.payload {
                KnowledgeRequestPayload::KnowledgeConfigChanged => actor.reload_knowledge_config().await,
                payload => actor.handle_message(KnowledgeRequest { payload, reply_to: message.reply_to }).await,
            },
            change = receive_document_change(&mut actor.change_rx) => {
                if change.is_some() {
                    actor.notes.reconcile_external_changes().await;
                    let _ = actor.broadcast_tx.send(KnowledgeResponse::KnowledgeRecordsChanged).await;
                } else {
                    actor.change_rx = None;
                }
            }
            else => break,
        }
    }
}

#[cfg(test)]
mod tests;
