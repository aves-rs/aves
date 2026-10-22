use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};
use tokio::sync::mpsc;

use super::messages::{ConfigBroadcast, ConfigRequest, ConfigRequestPayload, ConfigResponse};
use super::types::{ConfigError, SectionRegistration};

/// Single owner of the loaded file and the registered-section projection.
pub(super) struct Actor {
    directory: PathBuf,
    loaded: Map<String, Value>,
    sections: BTreeMap<String, SectionRegistration>,
    sealed: bool,
    requests: mpsc::Receiver<ConfigRequest>,
    broadcasts: mpsc::Sender<ConfigBroadcast>,
}

impl Actor {
    pub(super) fn new(
        directory: PathBuf,
        requests: mpsc::Receiver<ConfigRequest>,
        broadcasts: mpsc::Sender<ConfigBroadcast>,
    ) -> Result<Self, ConfigError> {
        let loaded = read_config_file(&directory)?;
        Ok(Self {
            directory,
            loaded,
            sections: BTreeMap::new(),
            sealed: false,
            requests,
            broadcasts,
        })
    }

    pub(super) async fn handle_message(
        &mut self,
        payload: ConfigRequestPayload,
    ) -> Result<ConfigResponse, ConfigError> {
        match payload {
            ConfigRequestPayload::GetConfigSection { section } => self.handle_get_section(section),
            ConfigRequestPayload::RegisterSection { registration } => self.handle_register(registration),
            ConfigRequestPayload::SealRegistration => self.handle_seal(),
            ConfigRequestPayload::SaveConfigSection { section, value } => {
                self.handle_save_section(section, value).await
            }
        }
    }

    fn handle_register(&mut self, registration: SectionRegistration) -> Result<ConfigResponse, ConfigError> {
        if self.sealed {
            return Err(ConfigError::RegistrationClosed);
        }
        if self.sections.contains_key(&registration.name) {
            return Err(ConfigError::DuplicateSection(registration.name));
        }
        self.sections
            .insert(registration.name.clone(), registration);
        Ok(ConfigResponse::Registered)
    }

    fn handle_seal(&mut self) -> Result<ConfigResponse, ConfigError> {
        self.sealed = true;
        Ok(ConfigResponse::Registered)
    }

    fn handle_get_section(&self, section: String) -> Result<ConfigResponse, ConfigError> {
        let registration = self.section(&section)?;
        let mut value = registration.default.clone();
        if let Some(raw) = self.loaded.get(&section) {
            merge(&mut value, raw.clone(), &section)?;
        }
        Ok(ConfigResponse::ConfigSection {
            value: registration.normalize(value)?,
        })
    }

    async fn handle_save_section(&mut self, section: String, value: Value) -> Result<ConfigResponse, ConfigError> {
        if !self.sealed {
            return Err(ConfigError::RegistrationOpen);
        }
        let registration = self.section(&section)?.clone();
        let mut snapshot = self.snapshot()?;
        let old = snapshot[&section].clone();
        let mut next = old.clone();
        merge(&mut next, value, &section)?;
        let next = registration.normalize(next)?;
        if next == old {
            return Ok(ConfigResponse::ConfigSectionSaved { section });
        }
        registration.prepare(old.clone(), next.clone()).await?;
        snapshot
            .as_object_mut()
            .expect("config snapshot object")
            .insert(section.clone(), next.clone());
        self.save(&snapshot)?;
        self.loaded = snapshot
            .as_object()
            .expect("config snapshot object")
            .clone();
        let _ = self
            .broadcasts
            .send(ConfigBroadcast::ConfigChanged {
                section: section.clone(),
                value: next.clone(),
            })
            .await;
        registration.notify(old, next).await?;
        Ok(ConfigResponse::ConfigSectionSaved { section })
    }

    fn snapshot(&self) -> Result<Value, ConfigError> {
        let mut values = Map::new();
        for (name, registration) in &self.sections {
            let mut value = registration.default.clone();
            if let Some(raw) = self.loaded.get(name) {
                merge(&mut value, raw.clone(), name)?;
            }
            values.insert(name.clone(), registration.normalize(value)?);
        }
        Ok(Value::Object(values))
    }

    fn section(&self, name: &str) -> Result<&SectionRegistration, ConfigError> {
        self.sections
            .get(name)
            .ok_or_else(|| ConfigError::UnknownSection(name.to_owned()))
    }

    fn save(&self, next: &Value) -> Result<(), ConfigError> {
        fs::create_dir_all(&self.directory)?;
        let mut persisted = next.clone();
        omit_null_fields(&mut persisted);
        let contents = toml::to_string_pretty(&persisted)?;
        fs::write(self.path(), contents)?;
        Ok(())
    }

    fn path(&self) -> PathBuf {
        self.directory.join("config.toml")
    }
}

fn omit_null_fields(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            fields.retain(|_, field| !field.is_null());
            for field in fields.values_mut() {
                omit_null_fields(field);
            }
        }
        Value::Array(elements) => {
            for element in elements {
                omit_null_fields(element);
            }
        }
        _ => {}
    }
}

fn read_config_file(directory: &Path) -> Result<Map<String, Value>, ConfigError> {
    let contents = match fs::read_to_string(directory.join("config.toml")) {
        Ok(contents) => contents,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Map::new()),
        Err(error) => return Err(error.into()),
    };
    let table: toml::Table = toml::from_str(&contents)?;
    let Value::Object(loaded) = serde_json::to_value(table)? else {
        unreachable!("TOML root table must serialize as a JSON object")
    };
    Ok(loaded)
}

/// Merge section fields; submitted object fields replace their previous contents.
fn merge(target: &mut Value, patch: Value, section: &str) -> Result<(), ConfigError> {
    let (Some(target), Value::Object(patch)) = (target.as_object_mut(), patch) else {
        return Err(ConfigError::InvalidValue {
            section: section.to_owned(),
            message: "section values must be objects".to_owned(),
        });
    };
    for (key, value) in patch {
        target.insert(key, value);
    }
    Ok(())
}

/// Drives the one Config actor while its generated bus handler remains open.
pub(super) async fn run_actor(mut actor: Actor) {
    while let Some(request) = actor.requests.recv().await {
        let result = actor.handle_message(request.payload).await;
        let _ = request.reply_to.send(result);
    }
}

#[cfg(test)]
mod tests;
