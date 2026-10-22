use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use specta::Type;
use thiserror::Error;

use crate::auth::AvesAuthError;
use crate::bus::DispatchContextError;
use crate::runtime::GeneratedCallError;

type ChangeFuture = Pin<Box<dyn Future<Output = Result<(), ConfigError>> + Send>>;
type ChangeHook = Arc<dyn Fn(Value, Value) -> ChangeFuture + Send + Sync>;
type Validator = Arc<dyn Fn(Value) -> Result<Value, ConfigError> + Send + Sync>;

fn adapt_hook<T, F, Fut>(hook: F) -> ChangeHook
where
    T: DeserializeOwned + Send + 'static,
    F: Fn(T, T) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), ConfigError>> + Send + 'static,
{
    let hook = Arc::new(hook);
    Arc::new(move |old, new| {
        let hook = hook.clone();
        Box::pin(async move { hook(serde_json::from_value(old)?, serde_json::from_value(new)?).await })
    })
}

/// An owner's typed section contract and optional live-change behavior.
#[derive(Clone)]
pub struct SectionRegistration {
    pub(super) name: String,
    pub(super) default: Value,
    validate: Validator,
    before_write: Option<ChangeHook>,
    on_change: Option<ChangeHook>,
}

impl fmt::Debug for SectionRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SectionRegistration")
            .field("name", &self.name)
            .finish()
    }
}

impl SectionRegistration {
    /// Declares one section using its owner's serde struct as the validation boundary.
    pub fn new<T>(name: impl Into<String>) -> Result<Self, ConfigError>
    where
        T: Default + Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let name = name.into();
        if name.is_empty() || name.contains('.') {
            return Err(ConfigError::InvalidSection(name));
        }
        let default = serde_json::to_value(T::default())?;
        if !default.is_object() {
            return Err(ConfigError::InvalidSection(name));
        }
        let section_name = name.clone();
        Ok(Self {
            name,
            default,
            validate: Arc::new(move |value| {
                let settings: T = serde_json::from_value(value).map_err(|source| ConfigError::InvalidValue {
                    section: section_name.clone(),
                    message: source.to_string(),
                })?;
                Ok(serde_json::to_value(settings)?)
            }),
            before_write: None,
            on_change: None,
        })
    }

    /// Applies a live gate before saving. Rejection leaves the file unchanged.
    pub fn before_write<T, F, Fut>(mut self, hook: F) -> Self
    where
        T: DeserializeOwned + Send + 'static,
        F: Fn(T, T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), ConfigError>> + Send + 'static,
    {
        self.before_write = Some(adapt_hook::<T, F, Fut>(hook));
        self
    }

    /// Runs after persistence; a failed hook does not revert the saved file.
    pub fn on_change<T, F, Fut>(mut self, hook: F) -> Self
    where
        T: DeserializeOwned + Send + 'static,
        F: Fn(T, T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), ConfigError>> + Send + 'static,
    {
        self.on_change = Some(adapt_hook::<T, F, Fut>(hook));
        self
    }

    pub(super) fn normalize(&self, value: Value) -> Result<Value, ConfigError> {
        (self.validate)(value)
    }

    pub(super) async fn prepare(&self, old: Value, new: Value) -> Result<(), ConfigError> {
        if let Some(hook) = &self.before_write {
            hook(old, new).await?;
        }
        Ok(())
    }

    pub(super) async fn notify(&self, old: Value, new: Value) -> Result<(), ConfigError> {
        if let Some(hook) = &self.on_change {
            hook(old, new).await?;
        }
        Ok(())
    }
}

/// Configuration persistence, registration and validation failures.
#[derive(Debug, Error, Serialize, Deserialize, Type)]
pub enum ConfigError {
    /// An RPC dispatch context was rejected before reaching Config.
    #[error(transparent)]
    #[serde(skip)]
    #[specta(skip)]
    DispatchContext(#[from] DispatchContextError),
    /// Writes are unavailable until the host application finishes registration.
    #[error("configuration registration is not complete")]
    RegistrationOpen,
    /// Registrations are closed after the embedding application starts.
    #[error("configuration registration has ended")]
    RegistrationClosed,
    /// Authentication failed before the request reached Config.
    #[error(transparent)]
    #[serde(skip)]
    #[specta(skip)]
    Auth(#[from] AvesAuthError),
    /// The generated actor request could not be delivered.
    #[error("config channel send failed for {target_domain}.{operation}: {cause}")]
    ChannelSend {
        target_domain: String,
        operation: String,
        cause: String,
    },
    /// The generated actor reply could not be received.
    #[error("config channel receive failed for {target_domain}.{operation}: {cause}")]
    ChannelRecv {
        target_domain: String,
        operation: String,
        cause: String,
    },
    /// A generated request exceeded its reply timeout.
    #[error("config reply timed out for {target_domain}.{operation} after {timeout}")]
    OneshotReplyTimeout {
        target_domain: String,
        operation: String,
        timeout: String,
    },
    /// The actor dropped a generated reply channel.
    #[error("config reply was dropped for {target_domain}.{operation}: {cause}")]
    OneshotReplyMissing {
        target_domain: String,
        operation: String,
        cause: String,
    },
    /// The requested section has not been registered.
    #[error("unknown configuration section: {0}")]
    UnknownSection(String),
    /// An owner attempted to claim a registered section twice.
    #[error("duplicate configuration section: {0}")]
    DuplicateSection(String),
    /// A section name is not a single TOML table name.
    #[error("invalid configuration section: {0}")]
    InvalidSection(String),
    /// A patch could not be decoded by its owner.
    #[error("invalid configuration value for {section}: {message}")]
    InvalidValue { section: String, message: String },
    /// An owner accepted persistence but could not apply the new value live.
    #[error("live update for section {section} failed: {reason}")]
    OwnerUpdate {
        section: String,
        reason: String,
        code: Option<String>,
    },
    /// Reading or writing the configuration file failed.
    #[error(transparent)]
    #[serde(skip)]
    #[specta(skip)]
    Io(#[from] std::io::Error),
    /// TOML parsing failed.
    #[error(transparent)]
    #[serde(skip)]
    #[specta(skip)]
    TomlDecode(#[from] toml::de::Error),
    /// TOML serialization failed.
    #[error(transparent)]
    #[serde(skip)]
    #[specta(skip)]
    TomlEncode(#[from] toml::ser::Error),
    /// JSON conversion failed.
    #[error(transparent)]
    #[serde(skip)]
    #[specta(skip)]
    Json(#[from] serde_json::Error),
}

impl ConfigError {
    /// Stable client-facing error code for Config RPC errors.
    pub fn rpc_code(&self) -> &str {
        match self {
            Self::Io(_) => "Io",
            Self::TomlDecode(_) => "Parse",
            Self::TomlEncode(_) | Self::Json(_) => "Serialize",
            Self::InvalidValue { .. }
            | Self::InvalidSection(_)
            | Self::UnknownSection(_)
            | Self::DuplicateSection(_)
            | Self::RegistrationOpen
            | Self::RegistrationClosed => "InvalidValue",
            Self::OwnerUpdate { code: Some(code), .. } => code.as_str(),
            Self::OwnerUpdate { code: None, .. } => "InvalidValue",
            Self::Auth(_)
            | Self::DispatchContext(_)
            | Self::ChannelSend { .. }
            | Self::ChannelRecv { .. }
            | Self::OneshotReplyTimeout { .. }
            | Self::OneshotReplyMissing { .. } => "Infrastructure",
        }
    }
}

impl From<GeneratedCallError<ConfigError>> for ConfigError {
    fn from(error: GeneratedCallError<ConfigError>) -> Self {
        match error {
            GeneratedCallError::DomainError(error) => error,
            GeneratedCallError::Authentication(error) => error.into(),
            GeneratedCallError::InputClosed => Self::ChannelSend {
                target_domain: "config".to_owned(),
                operation: "dispatch".to_owned(),
                cause: "bus input closed".to_owned(),
            },
            GeneratedCallError::ReplyDropped => Self::ChannelRecv {
                target_domain: "config".to_owned(),
                operation: "dispatch".to_owned(),
                cause: "reply dropped".to_owned(),
            },
            GeneratedCallError::UnexpectedResponse { expected, actual } => Self::InvalidValue {
                section: "config".to_owned(),
                message: format!("expected {expected}, received {actual}"),
            },
        }
    }
}

impl rspc::Error for ConfigError {
    fn into_procedure_error(self) -> rspc::ProcedureError {
        rspc::ProcedureError::Resolver(rspc::ResolverError::new(self.to_string(), Some(self)))
    }
}
