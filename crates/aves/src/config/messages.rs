use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::oneshot;

use crate::bus::BusRpcContext;

use super::domain::ConfigDomain;
use super::types::{ConfigError, SectionRegistration};

/// Config actor request routed over the normal bus.
pub struct ConfigRequest {
    pub payload: ConfigRequestPayload,
    pub reply_to: oneshot::Sender<Result<ConfigResponse, ConfigError>>,
}

/// Section operations and privileged registration requests.
#[aves::bus_requests(marker = ConfigDomain, context = BusRpcContext, root_error = ConfigError, domain_error = ConfigError)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigRequestPayload {
    /// A Rust owner registers its section and validation behavior during setup.
    #[privileged(reason = "Config section registration is internal setup authority, not public RPC")]
    #[serde(skip)]
    RegisterSection { registration: SectionRegistration },

    /// The embedding application closes registration before runtime start.
    #[privileged(reason = "Closing Config registration is internal runtime control, not public RPC")]
    #[serde(skip)]
    SealRegistration,

    /// Returns one owner's effective section, including defaults without writing them.
    #[operation(name = "getConfigSection", kind = "query", response = ConfigSectionOutput, response_variant = "ConfigSection")]
    GetConfigSection { section: String },

    /// Merges an owner-scoped object, validates it, and saves the file.
    #[operation(name = "saveConfigSection", kind = "mutation", response = ConfigSectionSavedOutput, response_variant = "ConfigSectionSaved")]
    SaveConfigSection { section: String, value: Value },
}

/// Responses to Config actor requests.
#[aves::bus_responses]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigResponse {
    /// One owner's effective configuration value.
    ConfigSection {
        /// The normalized value of the section.
        value: Value,
    },

    /// A section was saved successfully.
    ConfigSectionSaved {
        /// The name of the section that was saved.
        section: String,
    },

    /// Internal registration completed.
    Registered,
}

/// A successful change broadcasts only the affected owner's effective section.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigBroadcast {
    /// Typed-owner section identifier and its complete effective value.
    ConfigChanged {
        /// The name of the section that was changed.
        section: String,
        /// The normalized value of the section.
        value: Value,
    },
}
