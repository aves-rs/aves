use std::sync::{Mutex, OnceLock};

static SUDO_REGISTRY: OnceLock<Mutex<SudoRegistry>> = OnceLock::new();

#[derive(Default)]
struct SudoRegistry {
    requests: Vec<RegisteredRequest>,
}

pub(super) struct RegisteredRequest {
    pub(super) name: String,
    pub(super) domain: String,
    pub(super) privileged: Vec<String>,
}

fn sudo_registry() -> &'static Mutex<SudoRegistry> {
    SUDO_REGISTRY.get_or_init(|| Mutex::new(SudoRegistry::default()))
}

pub(super) fn register_services(request: &str, domain: &str, mapped: Vec<String>) -> Result<(), String> {
    let registry = sudo_registry()
        .lock()
        .map_err(|_| "Horme sudo registry lock was poisoned".to_string())?;
    if let Some(registered) = registry
        .requests
        .iter()
        .find(|registered| registered.name == request && registered.domain == domain)
    {
        for variant in &mapped {
            if !registered
                .privileged
                .iter()
                .any(|candidate| candidate == variant)
            {
                return Err(format!(
                    "domain `{domain}` sudo service mapping contains non-privileged request variant `{variant}`"
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn register_request(request: RegisteredRequest) -> Result<(), String> {
    let mut registry = sudo_registry()
        .lock()
        .map_err(|_| "Horme sudo registry lock was poisoned".to_string())?;
    if !registry
        .requests
        .iter()
        .any(|existing| existing.name == request.name && existing.domain == request.domain)
    {
        registry.requests.push(request);
    }
    Ok(())
}
