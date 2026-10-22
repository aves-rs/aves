use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use futures_util::StreamExt;
use iroh::{EndpointAddr, endpoint::Connection};
use iroh_blobs::{
    BlobFormat, Hash,
    api::blobs::{AddPathOptions, ImportMode},
    format::collection::Collection,
    ticket::BlobTicket,
};
use iroh_docs::{AuthorId, NamespaceId, api::Doc, sync::Capability};
use uuid::Uuid;

use super::ast::{ContentAddressedPath, ContentAddressedRoot, ContentHash};
use super::database::ExecutionDocumentLink;
use super::files::{checked_relative, collect_files, mapped_input_path, path_name};
use super::mobility::FlowExecutionNativeReferences;
use super::sync::FlowNativeServices;
use super::types::{FlowExecutionId, WorkspaceError};
use super::workspace::{WorkspaceInput, WorkspaceStore};

pub(super) const EXECUTION_PATH: &str = "flow/execution";
pub(super) const WORKSPACE_PATH: &str = "flow/workspace";

struct MaterializedEntry {
    hash: Hash,
    root: ContentAddressedRoot,
}

impl WorkspaceStore {
    /// Creates a native Docs namespace and its independent author identity.
    pub(super) async fn create_execution_document(&self) -> Result<ExecutionDocumentLink, WorkspaceError> {
        let native = self.native.read().await;
        let document = native
            .docs_api
            .create()
            .await
            .map_err(anyhow_document_api_error)?;
        let author_id = native
            .docs_api
            .author_create()
            .await
            .map_err(anyhow_document_api_error)?;
        let namespace_id = document.id().to_string();
        document.close().await.map_err(anyhow_document_api_error)?;
        Ok(ExecutionDocumentLink {
            namespace_id,
            author_id: author_id.to_string(),
        })
    }

    /// Imports one execution document namespace and starts native Docs reconciliation.
    ///
    /// The persisted link intentionally remains the source namespace and author identity.
    /// An inactive replica can read that document but cannot mutate it with the source author.
    pub(super) async fn import_execution_document(
        &self,
        link: &ExecutionDocumentLink,
        source: EndpointAddr,
    ) -> Result<(), WorkspaceError> {
        let native = self.native.read().await;
        if !self.connectivity.is_enabled() {
            return Err(WorkspaceError::ProviderUnavailable);
        }
        let namespace = parse_namespace(&link.namespace_id)?;
        parse_author(&link.author_id)?;
        let (document, newly_opened) = Self::open_or_import_document(&native, namespace).await?;
        if newly_opened
            && let Err(error) = document
                .start_sync(vec![source.clone()])
                .await
                .map_err(anyhow_document_api_error)
        {
            native.open_documents.lock().await.remove(&namespace);
            let _ = document.close().await;
            return Err(error);
        }
        native.sync_peers.lock().await.insert(namespace, source);
        Ok(())
    }

    /// Opens a cached document or returns a temporary document owned by the caller.
    async fn open_document(
        native: &FlowNativeServices,
        namespace: NamespaceId,
        identity: &str,
    ) -> Result<(Doc, bool), WorkspaceError> {
        let open_documents = native.open_documents.lock().await;
        if let Some(document) = open_documents.get(&namespace) {
            return Ok((document.clone(), false));
        }
        let document = native
            .docs_api
            .open(namespace)
            .await
            .map_err(anyhow_document_api_error)?
            .ok_or_else(|| WorkspaceError::InvalidDocumentIdentity {
                value: identity.to_owned(),
            })?;
        Ok((document, true))
    }

    /// Opens or imports a namespace while holding the cache lock through insertion.
    async fn open_or_import_document(
        native: &FlowNativeServices,
        namespace: NamespaceId,
    ) -> Result<(Doc, bool), WorkspaceError> {
        let mut open_documents = native.open_documents.lock().await;
        if let Some(document) = open_documents.get(&namespace) {
            return Ok((document.clone(), false));
        }
        let document = match native
            .docs_api
            .open(namespace)
            .await
            .map_err(anyhow_document_api_error)?
        {
            Some(document) => document,
            None => native
                .docs_api
                .import_namespace(Capability::Read(namespace))
                .await
                .map_err(anyhow_document_api_error)?,
        };
        open_documents.insert(namespace, document.clone());
        Ok((document, true))
    }

    /// Reopens a document using only its persisted public identities; secret material stays in native Docs.
    pub(super) async fn reopen_execution_document(&self, link: &ExecutionDocumentLink) -> Result<(), WorkspaceError> {
        let native = self.native.read().await;
        let namespace = parse_namespace(&link.namespace_id)?;
        parse_author(&link.author_id)?;
        let (document, temporary) = Self::open_document(&native, namespace, &link.namespace_id).await?;
        if temporary {
            document.close().await.map_err(anyhow_document_api_error)?;
        }
        Ok(())
    }

    /// Fetches the exact immutable roots retained by an execution through BlobsProtocol.
    pub(super) async fn import_native_content(
        &self,
        references: &FlowExecutionNativeReferences,
        source: EndpointAddr,
    ) -> Result<(), WorkspaceError> {
        let native = self.native.read().await;
        if !self.connectivity.is_enabled() {
            return Err(WorkspaceError::ProviderUnavailable);
        }
        let connection = native
            .endpoint
            .connect(source, iroh_blobs::ALPN)
            .await
            .map_err(|source| WorkspaceError::ProviderUnreachable {
                source: Box::new(source),
            })?;
        for root in references
            .code_roots
            .iter()
            .chain(&references.state_roots)
            .chain(references.workspace_checkpoint.iter())
            .chain(references.imported_workspace_root.iter())
        {
            self.fetch_collection(&connection, root).await?;
        }
        for hash in references.workspace_entries.values() {
            self.fetch_hash(&connection, parse_hash_value(hash)?)
                .await?;
        }
        Ok(())
    }

    /// Replaces the local execution directory with a projection derived only from imported content.
    pub(super) async fn materialize_imported_execution(
        &self,
        references: &FlowExecutionNativeReferences,
    ) -> Result<(), WorkspaceError> {
        let target = self.execution_root(references.execution_id);
        match &references.workspace_checkpoint {
            Some(root) => {
                self.materialize_clean(
                    root,
                    &target,
                    &format!("{EXECUTION_PATH}/{}/checkpoint/imported", references.execution_id),
                )
                .await
            }
            None => self.materialize_empty_clean(&target),
        }
    }

    /// Writes a native Docs hash entry, closing temporary handles before returning write errors.
    pub(super) async fn insert_document_entry(
        &self,
        link: &ExecutionDocumentLink,
        key: &str,
        hash: Hash,
        len: u64,
    ) -> Result<(), WorkspaceError> {
        let native = self.native.read().await;
        let namespace = parse_namespace(&link.namespace_id)?;
        let author_id = parse_author(&link.author_id)?;
        let (document, temporary) = Self::open_document(&native, namespace, &link.namespace_id).await?;
        let result = document
            .set_hash(author_id, document_key(key), hash, len)
            .await
            .map_err(anyhow_document_api_error);
        if temporary {
            document.close().await.map_err(anyhow_document_api_error)?;
        }
        result?;
        Ok(())
    }

    /// Tombstones the exact NUL-delimited key prefix without matching sibling names.
    pub(super) async fn delete_document_prefix(
        &self,
        link: &ExecutionDocumentLink,
        prefix: &str,
    ) -> Result<(), WorkspaceError> {
        let native = self.native.read().await;
        let namespace = parse_namespace(&link.namespace_id)?;
        let author_id = parse_author(&link.author_id)?;
        let (document, temporary) = Self::open_document(&native, namespace, &link.namespace_id).await?;
        let result = document
            .del(author_id, document_key(prefix))
            .await
            .map_err(anyhow_document_api_error);
        if temporary {
            document.close().await.map_err(anyhow_document_api_error)?;
        }
        result.map(|_| ())
    }

    /// Removes local sync/cache state, then drops the Doc and author when present.
    pub(super) async fn remove_execution_document(&self, link: &ExecutionDocumentLink) -> Result<(), WorkspaceError> {
        let native = self.native.read().await;
        let namespace = parse_namespace(&link.namespace_id)?;
        let author = parse_author(&link.author_id)?;
        native.sync_peers.lock().await.remove(&namespace);
        if let Some(document) = native.open_documents.lock().await.remove(&namespace) {
            document.close().await.map_err(anyhow_document_api_error)?;
        }
        let mut documents = native
            .docs_api
            .list()
            .await
            .map_err(anyhow_document_api_error)?;
        let mut document_exists = false;
        while let Some(document) = documents.next().await {
            let (document_id, _) = document.map_err(anyhow_document_api_error)?;
            if document_id == namespace {
                document_exists = true;
                break;
            }
        }
        drop(documents);
        if document_exists {
            native
                .docs_api
                .drop_doc(namespace)
                .await
                .map_err(anyhow_document_api_error)?;
        }
        if native
            .docs_api
            .author_export(author)
            .await
            .map_err(anyhow_document_api_error)?
            .is_some()
        {
            native
                .docs_api
                .author_delete(author)
                .await
                .map_err(anyhow_document_api_error)?;
        }
        Ok(())
    }

    /// Releases every durable pin owned by one execution.
    pub(super) async fn release_execution_pins(&self, execution_id: FlowExecutionId) -> Result<(), WorkspaceError> {
        self.store
            .tags()
            .delete_prefix(format!("{EXECUTION_PATH}/{execution_id}/"))
            .await
            .map_err(WorkspaceError::blob)?;
        Ok(())
    }

    /// Releases only the selected checkpoint pin, retaining other execution roots.
    pub(super) async fn release_checkpoint_pin(
        &self,
        execution_id: FlowExecutionId,
        sequence: u64,
    ) -> Result<(), WorkspaceError> {
        self.store
            .tags()
            .delete(format!("{EXECUTION_PATH}/{execution_id}/checkpoint/{sequence}"))
            .await
            .map_err(WorkspaceError::blob)?;
        Ok(())
    }

    /// Imports one regular file or a directory tree. A file is represented by
    /// its basename; directory entries are represented relative to its root.
    pub(super) async fn import_path(&self, source: PathBuf) -> Result<ContentAddressedRoot, WorkspaceError> {
        self.import_path_with_tag(source, WORKSPACE_PATH).await
    }

    async fn import_path_with_tag(
        &self,
        source: PathBuf,
        tag_prefix: &str,
    ) -> Result<ContentAddressedRoot, WorkspaceError> {
        let metadata =
            fs::symlink_metadata(&source).map_err(|source_error| WorkspaceError::io(source.clone(), source_error))?;
        if metadata.file_type().is_symlink() {
            return Err(WorkspaceError::SymbolicLink { path: source });
        }
        if metadata.is_file() {
            let entries = [(
                PathBuf::from(
                    source
                        .file_name()
                        .ok_or_else(|| WorkspaceError::MissingFileName { path: source.clone() })?,
                ),
                source.clone(),
            )];
            return self.import_entries_with_tag(&entries, tag_prefix).await;
        } else if metadata.is_dir() {
            let mut entries = Vec::new();
            collect_files(&source, &source, &mut entries)?;
            return self.import_entries_with_tag(&entries, tag_prefix).await;
        } else {
            Err(WorkspaceError::UnsupportedSource { path: source })
        }
    }

    /// Imports each file before validating its collection path, preserving ingress error order.
    pub(super) async fn import_entries_with_tag(
        &self,
        entries: &[(PathBuf, PathBuf)],
        tag_prefix: &str,
    ) -> Result<ContentAddressedRoot, WorkspaceError> {
        let mut collection = Collection::default();
        for (relative, file) in entries {
            let hash = self.import_file(file).await?;
            collection.push(path_name(relative)?, hash);
        }
        self.store_collection_and_pin(collection, tag_prefix).await
    }

    /// Loads and validates every relative path in a native immutable collection.
    pub(super) async fn collection_paths(
        &self,
        root: &ContentAddressedRoot,
    ) -> Result<BTreeSet<PathBuf>, WorkspaceError> {
        let collection = self.load_collection(parse_hash(root)?).await?;
        collection
            .iter()
            .map(|(name, _)| checked_relative(Path::new(name)))
            .collect()
    }

    /// Reads one collection entry directly from the native blob store as UTF-8 text.
    pub(super) async fn read_collection_text(
        &self,
        root: &ContentAddressedRoot,
        relative: &str,
    ) -> Result<String, WorkspaceError> {
        let relative = checked_relative(Path::new(relative))?;
        let collection = self.load_collection(parse_hash(root)?).await?;
        let hash = collection
            .iter()
            .find_map(|(name, hash)| {
                checked_relative(Path::new(name))
                    .is_ok_and(|candidate| candidate.as_path() == relative.as_path())
                    .then_some(*hash)
            })
            .ok_or_else(|| WorkspaceError::ContentPathNotFound {
                root: root.clone(),
                path: relative.to_string_lossy().into_owned(),
            })?;
        let bytes = self
            .store
            .get_bytes(hash)
            .await
            .map_err(WorkspaceError::blob)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| WorkspaceError::InvalidUtf8 { path: relative })
    }

    /// Stores UTF-8 bytes as a one-entry workspace collection and pins its root.
    pub(super) async fn import_text_at(
        &self,
        relative: &str,
        source: &str,
    ) -> Result<ContentAddressedPath, WorkspaceError> {
        let relative = checked_relative(Path::new(relative))?;
        let imported = self
            .store
            .add_slice(source.as_bytes())
            .await
            .map_err(WorkspaceError::blob)?;
        let mut collection = Collection::default();
        collection.push(path_name(&relative)?, imported.hash);
        let root = self
            .store_collection_and_pin(collection, WORKSPACE_PATH)
            .await?;
        Ok(ContentAddressedPath {
            root,
            path: relative.to_string_lossy().into_owned(),
        })
    }

    /// Replaces an existing collection path after importing the new bytes.
    pub(super) async fn replace_collection_text(
        &self,
        root: &ContentAddressedRoot,
        relative: &str,
        source: &str,
    ) -> Result<ContentAddressedPath, WorkspaceError> {
        let relative = checked_relative(Path::new(relative))?;
        let replacement_hash = self
            .store
            .add_slice(source.as_bytes())
            .await
            .map(|imported| imported.hash)
            .map_err(WorkspaceError::blob)?;

        let collection = self.load_collection(parse_hash(root)?).await?;
        let mut replaced = false;
        let mut paths = BTreeSet::new();
        let mut updated = Collection::default();
        for (name, hash) in collection.iter() {
            let existing = checked_relative(Path::new(name))?;
            if !paths.insert(existing.clone()) {
                return Err(WorkspaceError::DuplicatePath { path: existing });
            }
            if existing == relative {
                updated.push(path_name(&existing)?, replacement_hash);
                replaced = true;
            } else {
                updated.push(path_name(&existing)?, *hash);
            }
        }
        if !replaced {
            return Err(WorkspaceError::ContentPathNotFound {
                root: root.clone(),
                path: relative.to_string_lossy().into_owned(),
            });
        }
        let updated_root = self
            .store_collection_and_pin(updated, WORKSPACE_PATH)
            .await?;
        Ok(ContentAddressedPath {
            root: updated_root,
            path: relative.to_string_lossy().into_owned(),
        })
    }

    /// Pins then exports a collection into an ordinary writable directory projection.
    pub(super) async fn materialize(
        &self,
        root: &ContentAddressedRoot,
        target: &Path,
        pin_name: &str,
    ) -> Result<(), WorkspaceError> {
        let hash = parse_hash(root)?;
        self.store
            .tags()
            .set(pin_name, iroh_blobs::HashAndFormat::from(hash))
            .await
            .map_err(WorkspaceError::blob)?;
        let collection = self.load_collection(hash).await?;
        fs::create_dir_all(target).map_err(|source| WorkspaceError::io(target.to_path_buf(), source))?;
        let mut seen = HashSet::new();
        for (name, child) in collection.iter() {
            let relative = checked_relative(Path::new(name))?;
            if !seen.insert(relative.clone()) {
                return Err(WorkspaceError::DuplicatePath { path: relative });
            }
            self.export_to_destination(*child, &target.join(relative))
                .await?;
        }
        Ok(())
    }

    /// Materializes a checkpoint into a new projection before replacing the live directory.
    ///
    /// Recovery must not inspect or hash the crash-surviving directory: it may contain files
    /// that were never part of the committed checkpoint. The old projection is only removed
    /// after the new one has been completely exported.
    pub(super) async fn materialize_clean(
        &self,
        root: &ContentAddressedRoot,
        target: &Path,
        pin_name: &str,
    ) -> Result<(), WorkspaceError> {
        let projection = self.prepare_clean_projection(target)?;
        if let Err(error) = self.materialize(root, &projection, pin_name).await {
            let _ = fs::remove_dir_all(&projection);
            return Err(error);
        }
        self.commit_clean_projection(target, &projection)
    }

    /// Replaces a live directory with a clean empty projection.
    fn materialize_empty_clean(&self, target: &Path) -> Result<(), WorkspaceError> {
        let projection = self.prepare_clean_projection(target)?;
        fs::create_dir_all(&projection).map_err(|source| WorkspaceError::io(projection.clone(), source))?;
        self.commit_clean_projection(target, &projection)
    }

    /// Validates and pins all inputs before exporting their collision-free projection.
    pub(super) async fn materialize_inputs(
        &self,
        execution_id: FlowExecutionId,
        target: &Path,
        inputs: &[WorkspaceInput],
    ) -> Result<(), WorkspaceError> {
        let mut entries = BTreeMap::<PathBuf, MaterializedEntry>::new();
        let mut pinned = HashSet::new();
        for input in inputs {
            let (root, selected_path, role) = match input {
                WorkspaceInput::Root { root, role } => (root, None, *role),
                WorkspaceInput::Path { root, path, role } => (root, Some(checked_relative(Path::new(path))?), *role),
            };
            let hash = parse_hash(root)?;
            let pin = format!("{EXECUTION_PATH}/{execution_id}/{role}/{}", root.digest.as_str());
            if pinned.insert(pin.clone()) {
                self.store
                    .tags()
                    .set(pin, iroh_blobs::HashAndFormat::from(hash))
                    .await
                    .map_err(WorkspaceError::blob)?;
            }
            let collection = self.load_collection(hash).await?;
            let mut selected = selected_path.is_none();
            for (name, child) in collection.iter() {
                let source_path = checked_relative(Path::new(name))?;
                let Some(destination) = selected_path
                    .as_ref()
                    .map_or(Some(source_path.clone()), |path| {
                        (path == &source_path).then(|| mapped_input_path(role, path))
                    })
                else {
                    continue;
                };
                selected = true;
                if let Some(existing) = entries.get(&destination) {
                    if existing.hash != *child {
                        return Err(WorkspaceError::InputCollision {
                            path: destination,
                            first_root: existing.root.clone(),
                            second_root: root.clone(),
                        });
                    }
                    continue;
                }
                entries.insert(
                    destination,
                    MaterializedEntry {
                        hash: *child,
                        root: root.clone(),
                    },
                );
            }
            if !selected {
                return Err(WorkspaceError::ContentPathNotFound {
                    root: root.clone(),
                    path: selected_path
                        .expect("selected path exists")
                        .to_string_lossy()
                        .into_owned(),
                });
            }
        }
        fs::create_dir_all(target).map_err(|source| WorkspaceError::io(target.to_path_buf(), source))?;
        for (relative, entry) in entries {
            self.export_to_destination(entry.hash, &target.join(relative))
                .await?;
        }
        Ok(())
    }

    async fn export_to_destination(&self, hash: Hash, destination: &Path) -> Result<(), WorkspaceError> {
        let parent = destination
            .parent()
            .expect("validated relative path has parent");
        fs::create_dir_all(parent).map_err(|source| WorkspaceError::io(parent.to_path_buf(), source))?;
        self.store
            .export(hash, destination)
            .await
            .map(|_| ())
            .map_err(WorkspaceError::blob)
    }

    async fn load_collection(&self, hash: Hash) -> Result<Collection, WorkspaceError> {
        Collection::load(hash, self.store.as_ref())
            .await
            .map_err(WorkspaceError::blob)
    }

    /// Exports inputs into a clean projection before replacing the live directory.
    pub(super) async fn materialize_inputs_clean(
        &self,
        execution_id: FlowExecutionId,
        target: &Path,
        inputs: &[WorkspaceInput],
    ) -> Result<(), WorkspaceError> {
        let projection = self.prepare_clean_projection(target)?;
        if let Err(error) = self
            .materialize_inputs(execution_id, &projection, inputs)
            .await
        {
            let _ = fs::remove_dir_all(&projection);
            return Err(error);
        }
        self.commit_clean_projection(target, &projection)
    }

    /// Allocates a fresh sibling directory so recovery never trusts crash residue.
    fn prepare_clean_projection(&self, target: &Path) -> Result<PathBuf, WorkspaceError> {
        let parent = target
            .parent()
            .ok_or_else(|| WorkspaceError::PathEscapesRoot {
                path: target.to_path_buf(),
            })?;
        fs::create_dir_all(parent).map_err(|source| WorkspaceError::io(parent.to_path_buf(), source))?;
        let name = target
            .file_name()
            .map_or_else(|| "execution".to_owned(), |value| value.to_string_lossy().into_owned());
        let projection = parent.join(format!(".{name}.restore-{}", Uuid::new_v4()));
        Ok(projection)
    }

    /// Replaces the target with a completed projection; this is not a rollback transaction.
    fn commit_clean_projection(&self, target: &Path, projection: &Path) -> Result<(), WorkspaceError> {
        if target.exists() {
            fs::remove_dir_all(target).map_err(|source| WorkspaceError::io(target.to_path_buf(), source))?;
        }
        if let Err(source) = fs::rename(projection, target) {
            let _ = fs::remove_dir_all(projection);
            return Err(WorkspaceError::io(target.to_path_buf(), source));
        }
        Ok(())
    }

    /// Captures the exact live path set as an immutable collection and advances the
    /// execution document only for changed and deleted paths.
    pub(super) async fn capture_checkpoint(
        &self,
        execution_id: FlowExecutionId,
        execution_root: &Path,
        document: &ExecutionDocumentLink,
        previous_entries: &BTreeMap<String, ContentHash>,
        sequence: u64,
    ) -> Result<(ContentAddressedRoot, BTreeMap<String, ContentHash>), WorkspaceError> {
        let mut files = Vec::new();
        collect_files(execution_root, execution_root, &mut files)?;
        let mut current = BTreeMap::new();
        let mut collection = Collection::default();
        for (relative, file) in files {
            let key = path_name(&relative)?;
            let metadata = fs::metadata(&file).map_err(|source| WorkspaceError::io(file.clone(), source))?;
            // Read the live bytes before considering the previous hash. Reusing an existing
            // blob solely because it is still present would restore stale in-place edits.
            let imported = self.import_file(&file).await?;
            let content_hash = ContentHash::try_from(imported.to_string()).expect("Iroh hash is valid ContentHash");
            let hash = imported;
            if previous_entries.get(&key) != Some(&content_hash) {
                // Docs reserves zero-length content for tombstones; the collection retains empty files.
                if metadata.len() == 0 {
                    if previous_entries.contains_key(&key) {
                        self.delete_document_prefix(document, &key).await?;
                    }
                } else {
                    self.insert_document_entry(document, &key, hash, metadata.len())
                        .await?;
                }
            }
            collection.push(key.clone(), hash);
            current.insert(key, content_hash);
        }
        for key in previous_entries
            .keys()
            .filter(|key| !current.contains_key(*key))
        {
            self.delete_document_prefix(document, key).await?;
        }
        let root = collection
            .store(self.store.as_ref())
            .await
            .map_err(WorkspaceError::blob)?;
        let pin = format!("{EXECUTION_PATH}/{execution_id}/checkpoint/{sequence}");
        self.store
            .tags()
            .set(pin, root.hash_and_format())
            .await
            .map_err(WorkspaceError::blob)?;
        Ok((
            ContentAddressedRoot {
                digest: ContentHash::try_from(root.hash().to_string()).expect("Iroh hash is valid ContentHash"),
            },
            current,
        ))
    }

    /// Imports a file as a raw copied blob, retaining native blob error sources.
    async fn import_file(&self, file: &Path) -> Result<Hash, WorkspaceError> {
        self.store
            .add_path_with_opts(AddPathOptions {
                path: file.to_path_buf(),
                mode: ImportMode::Copy,
                format: BlobFormat::Raw,
            })
            .await
            .map(|imported| imported.hash)
            .map_err(WorkspaceError::blob)
    }

    /// Stores a collection and pins its root below the given tag prefix.
    async fn store_collection_and_pin(
        &self,
        collection: Collection,
        tag_prefix: &str,
    ) -> Result<ContentAddressedRoot, WorkspaceError> {
        let root = collection
            .store(self.store.as_ref())
            .await
            .map_err(WorkspaceError::blob)?;
        let digest = root.hash().to_string();
        self.store
            .tags()
            .set(format!("{tag_prefix}/{digest}"), root.hash_and_format())
            .await
            .map_err(WorkspaceError::blob)?;
        Ok(ContentAddressedRoot {
            digest: digest.try_into().expect("Iroh hash is valid ContentHash"),
        })
    }

    /// Downloads one native Collection-format ticket and registers it as a workspace root.
    pub(super) async fn download_collection_ticket(
        &self,
        ticket_value: String,
    ) -> Result<ContentAddressedRoot, WorkspaceError> {
        let ticket: BlobTicket = ticket_value
            .parse()
            .map_err(|_| WorkspaceError::InvalidTicket)?;
        let format = ticket.format();
        if !format.is_hash_seq() {
            return Err(WorkspaceError::UnsupportedTicketFormat {
                format: format.to_string(),
            });
        }

        let endpoint = self.native.read().await.endpoint.clone();
        if !self.connectivity.is_enabled() {
            return Err(WorkspaceError::ProviderUnavailable);
        }
        let connection = endpoint
            .connect(ticket.addr().clone(), iroh_blobs::ALPN)
            .await
            .map_err(|source| WorkspaceError::ProviderUnreachable {
                source: Box::new(source),
            })?;
        self.store
            .remote()
            .fetch(connection, ticket.hash_and_format())
            .complete()
            .await
            .map_err(|source| WorkspaceError::IntegrityMismatch {
                expected: ticket.hash().to_string(),
                source: Box::new(source),
            })?;
        let root_hash = ticket.hash();

        Collection::load(root_hash, self.store.as_ref())
            .await
            .map_err(|source| WorkspaceError::IntegrityMismatch {
                expected: root_hash.to_string(),
                source: Box::new(source),
            })?;
        let digest = root_hash.to_string();
        self.store
            .tags()
            .set(format!("{WORKSPACE_PATH}/{digest}"), ticket.hash_and_format())
            .await
            .map_err(WorkspaceError::blob)?;

        Ok(ContentAddressedRoot {
            digest: digest.try_into().expect("Iroh hash is valid ContentHash"),
        })
    }

    /// Fetches a collection root and every child blob over the native Blobs protocol.
    async fn fetch_collection(
        &self,
        connection: &Connection,
        root: &ContentAddressedRoot,
    ) -> Result<(), WorkspaceError> {
        let hash = parse_hash(root)?;
        self.fetch_hash(connection, hash).await?;
        let collection = Collection::load(hash, self.store.as_ref())
            .await
            .map_err(|source| WorkspaceError::IntegrityMismatch {
                expected: root.digest.as_str().to_owned(),
                source: Box::new(source),
            })?;
        for (_, child) in collection.iter() {
            self.fetch_hash(connection, *child).await?;
        }
        Ok(())
    }

    /// Fetches one blob and reports failures against its expected hash.
    async fn fetch_hash(&self, connection: &Connection, hash: Hash) -> Result<(), WorkspaceError> {
        self.store
            .remote()
            .fetch(connection.clone(), iroh_blobs::HashAndFormat::from(hash))
            .complete()
            .await
            .map_err(|source| WorkspaceError::IntegrityMismatch {
                expected: hash.to_string(),
                source: Box::new(source),
            })
            .map(|_| ())
    }
}

pub(super) fn anyhow_document_api_error(error: wasmtime::anyhow::Error) -> WorkspaceError {
    WorkspaceError::Document {
        source: error.into_boxed_dyn_error(),
    }
}

fn parse_namespace(value: &str) -> Result<NamespaceId, WorkspaceError> {
    value
        .parse()
        .map_err(|_| WorkspaceError::InvalidDocumentIdentity {
            value: value.to_owned(),
        })
}

fn parse_author(value: &str) -> Result<AuthorId, WorkspaceError> {
    value
        .parse()
        .map_err(|_| WorkspaceError::InvalidDocumentIdentity {
            value: value.to_owned(),
        })
}

fn parse_hash_value(value: &ContentHash) -> Result<Hash, WorkspaceError> {
    value
        .as_str()
        .parse()
        .map_err(|_| WorkspaceError::InvalidContentIdentity {
            value: value.as_str().to_owned(),
        })
}

fn document_key(path: &str) -> Vec<u8> {
    let mut key = path.as_bytes().to_vec();
    key.push(0);
    key
}

fn parse_hash(root: &ContentAddressedRoot) -> Result<Hash, WorkspaceError> {
    root.digest
        .as_str()
        .parse()
        .map_err(|_| WorkspaceError::ContentPathNotFound {
            root: root.clone(),
            path: String::new(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn open_test(data_root: &Path) -> Result<WorkspaceStore, WorkspaceError> {
        let fixture = super::super::fixtures::NativeDocsFixture::open(data_root).await;
        let mut store = WorkspaceStore::open(data_root, fixture.sync_services()).await?;
        store.attach_native_fixture(fixture);
        Ok(store)
    }

    #[tokio::test]
    async fn imports_directory_with_deterministic_relative_paths_and_materializes_known_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        std::fs::create_dir_all(source.join("nested")).unwrap();
        std::fs::write(source.join("script.ts"), "export const source = 'known';").unwrap();
        std::fs::write(source.join("nested/state.json"), "{\"known\":true}").unwrap();
        let store = open_test(directory.path()).await.unwrap();

        let root = store.import_path(source).await.unwrap();
        let projection = directory.path().join("projection");
        store
            .materialize(&root, &projection, "test-pin")
            .await
            .unwrap();

        assert_eq!(
            std::fs::read_to_string(projection.join("script.ts")).unwrap(),
            "export const source = 'known';"
        );
        assert_eq!(
            std::fs::read_to_string(projection.join("nested/state.json")).unwrap(),
            "{\"known\":true}"
        );
    }

    #[tokio::test]
    async fn imports_single_file_at_its_basename() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.ts");
        std::fs::write(&source, "export const source = 'known';").unwrap();
        let store = open_test(directory.path()).await.unwrap();

        let root = store.import_path(source).await.unwrap();
        let projection = directory.path().join("projection");
        store
            .materialize(&root, &projection, "test-pin")
            .await
            .unwrap();

        assert_eq!(
            std::fs::read_to_string(projection.join("source.ts")).unwrap(),
            "export const source = 'known';"
        );
    }

    #[tokio::test]
    async fn clean_materialization_discards_crash_surviving_files() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::write(source.join("committed.txt"), "checkpoint").unwrap();
        let store = open_test(directory.path()).await.unwrap();
        let root = store.import_path(source).await.unwrap();
        let projection = directory.path().join("executions").join("execution");
        std::fs::create_dir_all(&projection).unwrap();
        std::fs::write(projection.join("dirty.txt"), "never checkpointed").unwrap();

        store
            .materialize_clean(&root, &projection, "test-clean-recovery")
            .await
            .unwrap();

        assert_eq!(
            std::fs::read_to_string(projection.join("committed.txt")).unwrap(),
            "checkpoint"
        );
        assert!(!projection.join("dirty.txt").exists());
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_capture_reimports_overwritten_file_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let execution_root = directory.path().join("execution");
        std::fs::create_dir_all(&execution_root).unwrap();
        let file = execution_root.join("report.txt");
        std::fs::write(&file, "v1").unwrap();
        let store = open_test(directory.path()).await.unwrap();
        let document = store.create_execution_document().await.unwrap();
        let execution_id = super::super::types::FlowExecutionId(uuid::Uuid::new_v4());

        let (_, entries) = store
            .capture_checkpoint(execution_id, &execution_root, &document, &BTreeMap::new(), 0)
            .await
            .unwrap();
        std::fs::write(&file, "v2").unwrap();
        let (second, _) = store
            .capture_checkpoint(execution_id, &execution_root, &document, &entries, 1)
            .await
            .unwrap();
        let projection = directory.path().join("projection");
        store
            .materialize(&second, &projection, "test-overwrite-pin")
            .await
            .unwrap();

        assert_eq!(std::fs::read_to_string(projection.join("report.txt")).unwrap(), "v2");
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn deleting_document_key_preserves_sibling_prefix_entry() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.txt");
        std::fs::write(&source, "document contents").unwrap();
        let store = open_test(directory.path()).await.unwrap();
        let root = store.import_path(source).await.unwrap();
        let collection = Collection::load(parse_hash(&root).unwrap(), store.store.as_ref())
            .await
            .unwrap();
        let (_, hash) = collection.iter().next().unwrap();
        let document = store.create_execution_document().await.unwrap();
        store
            .insert_document_entry(&document, "a", *hash, 17)
            .await
            .unwrap();
        store
            .insert_document_entry(&document, "ab", *hash, 17)
            .await
            .unwrap();
        store.delete_document_prefix(&document, "a").await.unwrap();

        let namespace = parse_namespace(&document.namespace_id).unwrap();
        let author = parse_author(&document.author_id).unwrap();
        let native = store.native.read().await;
        let document_api = native.docs_api.open(namespace).await.unwrap().unwrap();
        let sibling = document_api
            .get_exact(author, document_key("ab"), true)
            .await
            .unwrap()
            .expect("sibling entry ab survives prefix delete of a");
        assert_eq!(sibling.content_hash(), *hash);
        assert_eq!(sibling.content_len(), 17);

        let deleted = document_api
            .get_exact(author, document_key("a"), true)
            .await
            .unwrap()
            .expect("deleting a records an empty tombstone");
        assert!(deleted.is_empty());
        document_api.close().await.unwrap();
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_capture_preserves_empty_files_without_docs_content_entries() {
        let directory = tempfile::tempdir().unwrap();
        let execution_root = directory.path().join("execution");
        std::fs::create_dir_all(execution_root.join("nested")).unwrap();
        std::fs::create_dir_all(execution_root.join("empty-directory/deep")).unwrap();
        std::fs::write(execution_root.join("nested/empty.txt"), b"").unwrap();
        std::fs::write(execution_root.join("report.txt"), "report").unwrap();
        let store = open_test(directory.path()).await.unwrap();
        let document = store.create_execution_document().await.unwrap();
        let execution_id = super::super::types::FlowExecutionId(uuid::Uuid::new_v4());

        let (checkpoint, entries) = store
            .capture_checkpoint(execution_id, &execution_root, &document, &BTreeMap::new(), 0)
            .await
            .unwrap();
        let empty_hash = ContentHash::try_from(Hash::EMPTY.to_string()).unwrap();
        assert_eq!(entries.get("nested/empty.txt"), Some(&empty_hash));
        assert!(entries.contains_key("report.txt"));
        let (unchanged, unchanged_entries) = store
            .capture_checkpoint(execution_id, &execution_root, &document, &entries, 1)
            .await
            .unwrap();
        assert_eq!(checkpoint, unchanged);
        assert_eq!(entries, unchanged_entries);

        let projection = directory.path().join("projection");
        store
            .materialize_clean(&checkpoint, &projection, "test-empty-file-pin")
            .await
            .unwrap();
        assert!(projection.join("nested/empty.txt").is_file());
        assert_eq!(
            std::fs::metadata(projection.join("nested/empty.txt"))
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            std::fs::read_to_string(projection.join("report.txt")).unwrap(),
            "report"
        );

        let native = store.native.read().await;
        let document_api = native
            .docs_api
            .open(parse_namespace(&document.namespace_id).unwrap())
            .await
            .unwrap()
            .unwrap();
        assert!(
            document_api
                .get_exact(
                    parse_author(&document.author_id).unwrap(),
                    document_key("nested/empty.txt"),
                    true
                )
                .await
                .unwrap()
                .is_none()
        );
        document_api.close().await.unwrap();
        drop(native);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_capture_truncates_file_without_retaining_stale_docs_content() {
        let directory = tempfile::tempdir().unwrap();
        let execution_root = directory.path().join("execution");
        std::fs::create_dir_all(&execution_root).unwrap();
        let file = execution_root.join("report.txt");
        std::fs::write(&file, "original").unwrap();
        std::fs::write(execution_root.join("report.txt.bak"), "sibling").unwrap();
        let store = open_test(directory.path()).await.unwrap();
        let document = store.create_execution_document().await.unwrap();
        let execution_id = super::super::types::FlowExecutionId(uuid::Uuid::new_v4());
        let (_, first_entries) = store
            .capture_checkpoint(execution_id, &execution_root, &document, &BTreeMap::new(), 0)
            .await
            .unwrap();

        std::fs::write(&file, b"").unwrap();
        let (checkpoint, empty_entries) = store
            .capture_checkpoint(execution_id, &execution_root, &document, &first_entries, 1)
            .await
            .unwrap();
        assert_eq!(
            empty_entries.get("report.txt"),
            Some(&ContentHash::try_from(Hash::EMPTY.to_string()).unwrap())
        );
        let projection = directory.path().join("projection");
        store
            .materialize_clean(&checkpoint, &projection, "test-truncated-file-pin")
            .await
            .unwrap();
        assert!(projection.join("report.txt").is_file());
        assert_eq!(
            std::fs::metadata(projection.join("report.txt"))
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            std::fs::read_to_string(projection.join("report.txt.bak")).unwrap(),
            "sibling"
        );

        let native = store.native.read().await;
        let document_api = native
            .docs_api
            .open(parse_namespace(&document.namespace_id).unwrap())
            .await
            .unwrap()
            .unwrap();
        let author = parse_author(&document.author_id).unwrap();
        let entry = document_api
            .get_exact(author, document_key("report.txt"), true)
            .await
            .unwrap()
            .unwrap();
        assert!(entry.is_empty());
        let sibling = document_api
            .get_exact(author, document_key("report.txt.bak"), false)
            .await
            .unwrap()
            .expect("truncation must not delete a sibling path");
        assert_eq!(sibling.content_len(), 7);
        document_api.close().await.unwrap();
        drop(native);

        std::fs::write(&file, "replacement").unwrap();
        let (replacement, replacement_entries) = store
            .capture_checkpoint(execution_id, &execution_root, &document, &empty_entries, 2)
            .await
            .unwrap();
        assert_ne!(replacement_entries.get("report.txt"), empty_entries.get("report.txt"));
        store
            .materialize_clean(&replacement, &projection, "test-repopulated-file-pin")
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(projection.join("report.txt")).unwrap(),
            "replacement"
        );
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_capture_accepts_workspace_with_only_empty_directories() {
        let directory = tempfile::tempdir().unwrap();
        let execution_root = directory.path().join("execution");
        std::fs::create_dir_all(execution_root.join("nested/empty")).unwrap();
        let store = open_test(directory.path()).await.unwrap();
        let document = store.create_execution_document().await.unwrap();
        let (checkpoint, entries) = store
            .capture_checkpoint(
                super::super::types::FlowExecutionId(uuid::Uuid::new_v4()),
                &execution_root,
                &document,
                &BTreeMap::new(),
                0,
            )
            .await
            .unwrap();
        assert!(entries.is_empty());
        let projection = directory.path().join("projection");
        store
            .materialize_clean(&checkpoint, &projection, "test-empty-workspace-pin")
            .await
            .unwrap();
        assert!(projection.is_dir());
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn execution_document_identity_reopens_and_records_deletions() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.txt");
        std::fs::write(&source, "document contents").unwrap();
        let store = open_test(directory.path()).await.unwrap();
        let root = store.import_path(source).await.unwrap();
        let collection = Collection::load(parse_hash(&root).unwrap(), store.store.as_ref())
            .await
            .unwrap();
        let (_, hash) = collection.iter().next().unwrap();
        let link = store.create_execution_document().await.unwrap();
        store
            .insert_document_entry(&link, "source.txt", *hash, 17)
            .await
            .unwrap();
        store.reopen_execution_document(&link).await.unwrap();
        store
            .delete_document_prefix(&link, "source.txt")
            .await
            .unwrap();
        store.remove_execution_document(&link).await.unwrap();
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn removing_an_already_absent_execution_document_is_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let store = open_test(directory.path()).await.unwrap();
        let document = store.create_execution_document().await.unwrap();

        store.remove_execution_document(&document).await.unwrap();
        store.remove_execution_document(&document).await.unwrap();

        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_capture_reuses_unchanged_files_and_records_deletions() {
        let directory = tempfile::tempdir().unwrap();
        let execution_root = directory.path().join("execution");
        std::fs::create_dir_all(&execution_root).unwrap();
        std::fs::write(execution_root.join("kept.txt"), "kept").unwrap();
        std::fs::write(execution_root.join("removed.txt"), "removed").unwrap();
        let store = open_test(directory.path()).await.unwrap();
        let document = store.create_execution_document().await.unwrap();

        let (first, entries) = store
            .capture_checkpoint(
                super::super::types::FlowExecutionId(uuid::Uuid::new_v4()),
                &execution_root,
                &document,
                &BTreeMap::new(),
                0,
            )
            .await
            .unwrap();
        std::fs::remove_file(execution_root.join("removed.txt")).unwrap();
        std::fs::write(execution_root.join("new.txt"), "new").unwrap();
        let (second, updated) = store
            .capture_checkpoint(
                super::super::types::FlowExecutionId(uuid::Uuid::new_v4()),
                &execution_root,
                &document,
                &entries,
                1,
            )
            .await
            .unwrap();

        assert_ne!(first, second);
        assert!(updated.contains_key("kept.txt"));
        assert!(updated.contains_key("new.txt"));
        assert!(!updated.contains_key("removed.txt"));
        store.shutdown().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_symbolic_links_during_import() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        std::fs::create_dir_all(&source).unwrap();
        std::os::unix::fs::symlink("missing", source.join("link")).unwrap();
        let store = open_test(directory.path()).await.unwrap();

        assert!(matches!(
            store.import_path(source).await,
            Err(WorkspaceError::SymbolicLink { .. })
        ));
    }

    #[tokio::test]
    async fn rejects_non_ticket_input_before_opening_provider() {
        let root = tempfile::tempdir().unwrap();
        let store = open_test(root.path()).await.unwrap();

        let error = store
            .download_collection_ticket("not-a-ticket".to_string())
            .await
            .unwrap_err();

        assert!(matches!(error, WorkspaceError::InvalidTicket));
    }
}
