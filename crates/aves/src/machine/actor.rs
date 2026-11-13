use std::fmt::Display;

use tokio::sync::mpsc;

use crate::auth::{AuthAdmissionRegistry, MachineIssuerId};
use crate::bus::BusRpcContext;
use crate::config::{
    GetConfigSectionInput, SaveConfigSectionInput, dispatch_get_config_section, dispatch_save_config_section,
};

use super::messages::{MachineBroadcast, MachineRequest, MachineRequestPayload, MachineResponse};
use super::peering::MachinePeerServices;
use super::types::{
    MachineBootOptions, MachineError, MachineInventory, MachineRecord, MachineSettings, MachineStatus, OnlineState,
    PeerAvailability, hex_key,
};

/// The Config section that owns this machine's name.
pub(super) const SECTION: &str = "machine";

/// Single owner of this machine's Config-backed state and admission projection.
///
/// Settings load lazily on the first request that needs them, so construction never reads Config.
pub(super) struct Actor {
    machine: MachineIssuerId,
    default_name: String,
    context: BusRpcContext,
    /// Set when Config registration failed; existing non-lifecycle requests report it.
    unavailable: Option<String>,
    settings: Option<MachineSettings>,
    pub(super) registry: AuthAdmissionRegistry,
    pub(super) peer_services: Option<MachinePeerServices>,
    server_running: Option<bool>,
    requests: mpsc::Receiver<MachineRequest>,
    broadcasts: mpsc::Sender<MachineBroadcast>,
}

impl Actor {
    pub(super) fn new(
        options: MachineBootOptions,
        context: BusRpcContext,
        unavailable: Option<String>,
        registry: AuthAdmissionRegistry,
        peer_services: Option<MachinePeerServices>,
        requests: mpsc::Receiver<MachineRequest>,
        broadcasts: mpsc::Sender<MachineBroadcast>,
    ) -> Self {
        Self {
            machine: options.machine,
            default_name: options.name,
            context,
            unavailable,
            settings: None,
            registry,
            peer_services,
            server_running: None,
            requests,
            broadcasts,
        }
    }

    async fn handle_message(&mut self, payload: MachineRequestPayload) -> Result<MachineResponse, MachineError> {
        if !matches!(
            payload,
            MachineRequestPayload::StartServer | MachineRequestPayload::StopServer
        ) && let Some(cause) = &self.unavailable
        {
            return Err(MachineError::Unavailable { cause: cause.clone() });
        }
        match payload {
            MachineRequestPayload::StartServer => self.start_server().await,
            MachineRequestPayload::StopServer => self.stop_server().await,
            MachineRequestPayload::ImportContent { tag, bytes } => Ok(MachineResponse::ContentImported {
                root: self.import_content(tag, bytes).await?,
            }),
            MachineRequestPayload::ImportContentCollection { tag, entries } => Ok(MachineResponse::ContentImported {
                root: self.import_content_collection(tag, entries).await?,
            }),
            MachineRequestPayload::ReadContent { hash } => Ok(MachineResponse::ContentBytes {
                bytes: self.read_content(hash).await?,
            }),
            MachineRequestPayload::ReadContentCollection { hash } => Ok(MachineResponse::ContentCollection {
                entries: self.read_content_collection(hash).await?,
            }),
            MachineRequestPayload::PinContent { tag, root } => {
                self.pin_content(tag, root).await?;
                Ok(MachineResponse::Success)
            }
            MachineRequestPayload::UnpinContent { tag } => {
                self.unpin_content(tag).await?;
                Ok(MachineResponse::Success)
            }
            MachineRequestPayload::FetchContentFromPeer { peer, tag, root } => Ok(MachineResponse::ContentImported {
                root: self.fetch_content_from_peer(peer, tag, root).await?,
            }),
            MachineRequestPayload::EncryptContent {
                execution_id,
                format_version,
                plaintext,
                recipients,
            } => Ok(MachineResponse::ContentEncrypted {
                content: self
                    .encrypt_content(execution_id, format_version, plaintext, recipients)
                    .await?,
            }),
            MachineRequestPayload::DecryptContent { content } => Ok(MachineResponse::ContentDecrypted {
                plaintext: self.decrypt_content(content).await?,
            }),
            MachineRequestPayload::AddContentRecipient { content, recipient } => {
                Ok(MachineResponse::ContentEncrypted {
                    content: self.add_content_recipient(content, recipient).await?,
                })
            }
            MachineRequestPayload::ReadContentRecipients { content } => Ok(MachineResponse::ContentRecipients {
                recipients: self.read_content_recipients(content).await?,
            }),
            MachineRequestPayload::ReadMachineInventory {} => Ok(MachineResponse::MachineInventory {
                inventory: self.inventory().await?,
            }),
            MachineRequestPayload::ReadMachineStatus {} => Ok(MachineResponse::MachineStatus {
                status: self.status().await?,
            }),
            MachineRequestPayload::RenameMachine { name } => self.rename(&name).await,
        }
    }

    async fn start_server(&mut self) -> Result<MachineResponse, MachineError> {
        self.peer_services
            .as_ref()
            .ok_or(MachineError::PeerServiceUnavailable)?
            .start_server()
            .await?;
        if self.server_running != Some(true) {
            self.server_running = Some(true);
            let _ = self.broadcasts.send(MachineBroadcast::ServerStarted).await;
        }
        Ok(MachineResponse::ServerStarted)
    }

    async fn stop_server(&mut self) -> Result<MachineResponse, MachineError> {
        self.peer_services
            .as_ref()
            .ok_or(MachineError::PeerServiceUnavailable)?
            .stop_server()
            .await?;
        if self.server_running != Some(false) {
            self.server_running = Some(false);
            let _ = self.broadcasts.send(MachineBroadcast::ServerStopped).await;
        }
        Ok(MachineResponse::ServerStopped)
    }

    /// Loads the section once through Config.
    async fn settings(&mut self) -> Result<MachineSettings, MachineError> {
        if let Some(settings) = &self.settings {
            return Ok(settings.clone());
        }
        let section = dispatch_get_config_section(
            &self.context,
            GetConfigSectionInput {
                section: SECTION.to_owned(),
            },
        )
        .await?;
        let settings: MachineSettings = serde_json::from_value(section.value).map_err(section_error)?;
        self.settings = Some(settings.clone());
        Ok(settings)
    }

    /// Saves the complete section through Config before adopting it, so memory never runs ahead of the saved value.
    async fn commit(&mut self, next: MachineSettings) -> Result<(), MachineError> {
        let value = serde_json::to_value(&next).map_err(section_error)?;
        dispatch_save_config_section(
            &self.context,
            SaveConfigSectionInput {
                section: SECTION.to_owned(),
                value,
            },
        )
        .await?;
        self.settings = Some(next);
        Ok(())
    }

    async fn inventory(&mut self) -> Result<MachineInventory, MachineError> {
        let settings = self.settings().await?;
        Ok(self.inventory_for(&settings))
    }

    fn inventory_for(&self, settings: &MachineSettings) -> MachineInventory {
        MachineInventory {
            local: MachineRecord {
                machine_key: hex_key(self.machine.verification_key()),
                name: settings
                    .name
                    .clone()
                    .unwrap_or_else(|| self.default_name.clone()),
            },
        }
    }

    /// Composes the secret-free status from the inventory and admission registry.
    async fn status(&mut self) -> Result<MachineStatus, MachineError> {
        let settings = self.settings().await?;
        Ok(MachineStatus {
            inventory: self.inventory_for(&settings),
            transport_identity: self.machine.clone(),
            peers: peers(&self.registry, &self.machine),
        })
    }

    async fn rename(&mut self, name: &str) -> Result<MachineResponse, MachineError> {
        let trimmed = validated_name(name)?;
        let mut next = self.settings().await?;
        next.name = Some(trimmed.clone());
        self.commit(next).await?;
        let _ = self
            .broadcasts
            .send(MachineBroadcast::MachineRenamed { name: trimmed.clone() })
            .await;
        Ok(MachineResponse::MachineRenamed { name: trimmed })
    }
}

/// Reads admitted remote issuers from Auth's registry; no liveness is inferred.
fn peers(registry: &AuthAdmissionRegistry, machine: &MachineIssuerId) -> PeerAvailability {
    let admitted_remote_machines = registry
        .current_issuers()
        .into_iter()
        .filter(|approved| &approved.issuer != machine)
        .map(|approved| hex_key(approved.issuer.verification_key()))
        .collect();
    PeerAvailability {
        admitted_remote_machines,
        online: OnlineState::Unavailable,
    }
}

/// Trims a requested name and rejects one with no visible characters.
pub(super) fn validated_name(name: &str) -> Result<String, MachineError> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(MachineError::InvalidName);
    }
    Ok(trimmed.to_owned())
}

fn section_error(error: impl Display) -> MachineError {
    MachineError::Section {
        cause: error.to_string(),
    }
}

/// Serves requests in arrival order; each reply is sent exactly once.
pub(super) async fn run_actor(mut actor: Actor) {
    while let Some(request) = actor.requests.recv().await {
        let response = actor.handle_message(request.payload).await;
        let _ = request.reply_to.send(response);
    }
}

#[cfg(test)]
mod tests;
