use iroh::EndpointAddr;
use iroh_blobs::{BlobFormat, HashAndFormat, api::Store, format::collection::Collection};

use crate::auth::MachineIssuerId;

use super::actor::Actor;
use super::types::{ContentEntry, ContentHash, ContentPeer, ContentRoot, MachineError};

impl Actor {
    /// Imports and pins raw content while its temporary tag protects it from collection.
    pub(super) async fn import_content(&self, tag: String, bytes: Vec<u8>) -> Result<ContentRoot, MachineError> {
        validate_tag(&tag)?;
        let store = self.content_store()?;
        let temp = store
            .add_bytes(bytes)
            .temp_tag()
            .await
            .map_err(blob_error)?;
        let root = ContentRoot::Raw(temp.hash());
        self.pin_content(tag, root).await?;
        Ok(root)
    }

    /// Stores and pins a named collection of existing content hashes.
    pub(super) async fn import_content_collection(
        &self,
        tag: String,
        entries: Vec<ContentEntry>,
    ) -> Result<ContentRoot, MachineError> {
        validate_tag(&tag)?;
        let store = self.content_store()?;
        let mut collection = Collection::default();
        for entry in entries {
            collection.push(entry.name, entry.hash);
        }
        let temp = collection.store(&store).await.map_err(blob_error)?;
        let root = ContentRoot::Collection(temp.hash());
        self.pin_content(tag, root).await?;
        Ok(root)
    }

    /// Reads a blob from the local store without network access.
    pub(super) async fn read_content(&self, hash: ContentHash) -> Result<Vec<u8>, MachineError> {
        let bytes = self
            .content_store()?
            .get_bytes(hash.0)
            .await
            .map_err(blob_error)?;
        Ok(bytes.to_vec())
    }

    /// Reads the named entries of a local collection.
    pub(super) async fn read_content_collection(&self, hash: ContentHash) -> Result<Vec<ContentEntry>, MachineError> {
        let store = self.content_store()?;
        let collection =
            Collection::load(hash.0, &store)
                .await
                .map_err(|source| MachineError::InvalidContentCollection {
                    hash: hash.0,
                    source: Box::new(source),
                })?;
        Ok(collection
            .iter()
            .map(|(name, hash)| ContentEntry {
                name: name.clone(),
                hash: *hash,
            })
            .collect())
    }

    /// Pins a raw or collection root under the content namespace.
    pub(super) async fn pin_content(&self, tag: String, root: ContentRoot) -> Result<(), MachineError> {
        validate_tag(&tag)?;
        self.content_store()?
            .tags()
            .set(tag_name(&tag), hash_and_format(root))
            .await
            .map_err(blob_error)
    }

    /// Removes only the named content pin.
    pub(super) async fn unpin_content(&self, tag: String) -> Result<(), MachineError> {
        validate_tag(&tag)?;
        self.content_store()?
            .tags()
            .delete(tag_name(&tag))
            .await
            .map(|_| ())
            .map_err(blob_error)
    }

    /// Fetches from an admitted remote peer and pins the root before fetching children.
    pub(super) async fn fetch_content_from_peer(
        &self,
        peer: ContentPeer,
        tag: String,
        root: ContentRoot,
    ) -> Result<ContentRoot, MachineError> {
        validate_tag(&tag)?;
        let peer_id = peer.0;
        let peer = EndpointAddr::new(peer_id);
        let issuer = MachineIssuerId::from(peer_id);
        if !self
            .registry
            .current_issuers()
            .into_iter()
            .skip(1)
            .any(|approved| approved.issuer == issuer)
        {
            return Err(MachineError::ContentPeerRefused {
                peer: peer_id.to_string(),
            });
        }
        let services = self
            .peer_services
            .as_ref()
            .ok_or(MachineError::ContentConnectivityDisabled)?;
        let endpoint = services.native.read().await.endpoint.clone();
        if !services.connectivity.is_enabled() {
            return Err(MachineError::ContentConnectivityDisabled);
        }
        let connection = endpoint
            .connect(peer, iroh_blobs::ALPN)
            .await
            .map_err(|source| MachineError::ContentPeerUnreachable {
                peer: peer_id.to_string(),
                source: Box::new(source),
            })?;
        let store = self.content_store()?;
        // Fetch the root as raw first: a HashSeq fetch otherwise downloads its children
        // before the persistent collection pin has been installed.
        let root_hash = match root {
            ContentRoot::Raw(hash) | ContentRoot::Collection(hash) => hash,
        };
        let _temp = store
            .tags()
            .temp_tag(HashAndFormat::from(root_hash))
            .await
            .map_err(blob_error)?;
        fetch_verified(&store, &connection, HashAndFormat::from(root_hash)).await?;
        self.pin_content(tag.clone(), root).await?;
        let result = async {
            if let ContentRoot::Collection(hash) = root {
                fetch_verified(&store, &connection, hash_and_format(root)).await?;
                self.read_content_collection(ContentHash(hash)).await?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            let _ = self.unpin_content(tag).await;
            return Err(error);
        }
        Ok(root)
    }

    fn content_store(&self) -> Result<Store, MachineError> {
        let services = self
            .peer_services
            .as_ref()
            .ok_or(MachineError::ContentUnavailable)?;
        Ok(services.blobs.clone().into())
    }
}

async fn fetch_verified(
    store: &Store,
    connection: &iroh::endpoint::Connection,
    value: HashAndFormat,
) -> Result<(), MachineError> {
    store
        .remote()
        .fetch(connection.clone(), value)
        .complete()
        .await
        .map_err(|source| MachineError::ContentIntegrity {
            hash: value.hash,
            source: Box::new(source),
        })
        .map(|_| ())
}

fn hash_and_format(root: ContentRoot) -> HashAndFormat {
    match root {
        ContentRoot::Raw(hash) => HashAndFormat::from(hash),
        ContentRoot::Collection(hash) => HashAndFormat::new(hash, BlobFormat::HashSeq),
    }
}

fn tag_name(tag: &str) -> String {
    format!("content/{tag}")
}

fn validate_tag(tag: &str) -> Result<(), MachineError> {
    let valid = !tag.is_empty()
        && tag.split('/').all(|segment| {
            !segment.is_empty()
                && !segment.starts_with('.')
                && !segment.ends_with('.')
                && !segment.contains("..")
                && segment
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'))
        });
    if valid {
        Ok(())
    } else {
        Err(MachineError::InvalidContentTag { tag: tag.to_owned() })
    }
}

fn blob_error(source: impl std::error::Error + Send + Sync + 'static) -> MachineError {
    MachineError::ContentBlob {
        source: Box::new(source),
    }
}
