use std::fs;
use std::io::ErrorKind;
use std::path::Path;

use chrono::{DateTime, Utc};
use iroh::{EndpointAddr, PublicKey};

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PeerConfig {
    pub(super) peers: Vec<PeerRecord>,
}

/// Durable peer identity and address state owned by Auth.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PeerRecord {
    pub peer: PublicKey,
    pub endpoint: PeerEndpoint,
    pub state: PeerTrustState,
    pub updated_at: DateTime<Utc>,
}

/// Local Auth policy for an Iroh peer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PeerTrustState {
    Confirmed,
    Revoked,
}

impl PeerConfig {
    pub(super) fn load(path: &Path) -> Result<Self, PeerConfigError> {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Self::default()),
            Err(error) => {
                return Err(PeerConfigError::Read { source: error });
            }
        };
        serde_json::from_slice(&bytes).map_err(|source| PeerConfigError::Deserialize { source })
    }

    pub(super) fn persist(&self, path: &Path) -> Result<(), PeerConfigError> {
        let bytes = serde_json::to_vec_pretty(self).map_err(|source| PeerConfigError::Serialize { source })?;
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|source| PeerConfigError::Write { source })?;
        }
        let temporary_path = path.with_extension("tmp");
        fs::write(&temporary_path, bytes).map_err(|source| PeerConfigError::Write { source })?;
        if let Err(error) = fs::rename(&temporary_path, path) {
            let _ = fs::remove_file(&temporary_path);
            return Err(PeerConfigError::Rename { source: error });
        }
        Ok(())
    }
}

/// Iroh address retained for a trusted peer.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PeerEndpoint {
    endpoint: EndpointAddr,
}

impl PeerEndpoint {
    pub(super) fn new(endpoint: EndpointAddr) -> Self {
        Self { endpoint }
    }

    pub(super) fn endpoint(&self) -> &EndpointAddr {
        &self.endpoint
    }

    pub(super) fn public_key(&self) -> PublicKey {
        self.endpoint.id
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum PeerConfigError {
    #[error("Auth peer configuration could not be read")]
    Read {
        #[source]
        source: std::io::Error,
    },
    #[error("Auth peer configuration is malformed")]
    Deserialize {
        #[source]
        source: serde_json::Error,
    },
    #[error("Auth peer configuration could not be serialized")]
    Serialize {
        #[source]
        source: serde_json::Error,
    },
    #[error("Auth peer configuration could not be written")]
    Write {
        #[source]
        source: std::io::Error,
    },
    #[error("Auth peer configuration could not be installed")]
    Rename {
        #[source]
        source: std::io::Error,
    },
}
