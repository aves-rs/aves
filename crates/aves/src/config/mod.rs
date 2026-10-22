//! Generic typed-section configuration persistence.

mod actor;
mod domain;
mod handle;
mod messages;
mod types;

pub use domain::{ConfigDirectory, ConfigDomain, ConfigRegistrationSudoService};
pub use messages::{
    ConfigBroadcast, ConfigRequest, ConfigRequestPayload, ConfigResponse, GetConfigSectionInput, RegisterSectionInput,
    SaveConfigSectionInput, SealRegistrationInput, config_operation_descriptors_with_root_error,
    dispatch_get_config_section, dispatch_save_config_section, rpc_router,
};
pub use types::{ConfigError, SectionRegistration};
