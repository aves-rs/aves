use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use agent_client_protocol::schema::v1::{
    CreateElicitationRequest, CreateElicitationResponse, RequestPermissionRequest, RequestPermissionResponse, SessionId,
};
use agent_client_protocol::{Agent, ConnectionTo, Responder};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;

use super::plugins::ConnectionStrategy;
use super::types::{
    DiscussError, ProviderRuntimeFailure, ProviderRuntimeState, ProviderRuntimeStatus, SluiceSessionId,
};

/// Maximum number of automatic Provider runtime restart attempts per process.
const MAX_PROVIDER_AUTO_RESTART_ATTEMPTS: u8 = 3;

/// Process-local automatic restart budget for one Provider.
///
/// The cap is an implementation invariant and is intentionally not serialized;
/// clients receive only the consumed attempt count through the runtime state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProviderRestartBudget {
    consumed: u8,
}

impl ProviderRestartBudget {
    /// Create a fresh budget with no automatic restart attempts consumed.
    const fn fresh() -> Self {
        Self { consumed: 0 }
    }

    /// Return the number of consumed automatic restart attempts.
    const fn consumed(self) -> u8 {
        self.consumed
    }

    /// Consume one automatic restart attempt, if the budget has room.
    const fn consume(&mut self) -> bool {
        if self.consumed < MAX_PROVIDER_AUTO_RESTART_ATTEMPTS {
            self.consumed += 1;
            true
        } else {
            false
        }
    }
}

/// The owner of an Agent-to-Client callback on a Provider connection.
///
/// Callback routes are always owned by a Sluice chat session.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum CallbackRouteTarget {
    /// A chat session, including an ephemeral inspector viewer.
    Chat { sluice_session_id: SluiceSessionId },
}

/// Callback payload delivered to the owning shared-connection worker.
pub(super) enum CallbackEvent {
    Permission {
        request: RequestPermissionRequest,
        responder: Responder<RequestPermissionResponse>,
        target: CallbackRouteTarget,
    },
    Elicitation {
        request: CreateElicitationRequest,
        acp_session_id: SessionId,
        responder: Responder<CreateElicitationResponse>,
        target: CallbackRouteTarget,
    },
    ElicitationComplete,
}

/// Provider-owned elicitation identity used for completion notifications.
///
/// ACP completion notifications may contain only this provider identity. It
/// is intentionally distinct from [`ElicitationRequestId`], which is the
/// Sluice-facing identity used to answer a pending UI request.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct ElicitationCorrelationId(String);

impl ElicitationCorrelationId {
    /// Construct a correlation key from the provider's opaque identifier.
    pub(super) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

/// Explicit callback correlations owned by one ACP connection.
///
/// Every lookup is exact. Registration refuses to replace a live route with a
/// different owner, so an ambiguous or stale callback has no route and must be
/// dropped by the connection-level dispatcher.
#[derive(Default)]
pub(super) struct CallbackRouter {
    session_routes: HashMap<SessionId, CallbackRouteTarget>,
    elicitation_routes: HashMap<ElicitationCorrelationId, CallbackRouteTarget>,
}

/// Per-connection queues for the typed callback targets.
pub(super) struct CallbackRouteRegistry {
    pub(super) callback_router: Arc<Mutex<CallbackRouter>>,
    callback_queues: HashMap<CallbackRouteTarget, mpsc::Sender<CallbackEvent>>,
}

impl CallbackRouteRegistry {
    pub(super) fn new(callback_router: Arc<Mutex<CallbackRouter>>) -> Self {
        Self {
            callback_router,
            callback_queues: HashMap::new(),
        }
    }

    pub(super) fn register_queue(&mut self, target: CallbackRouteTarget, queue: mpsc::Sender<CallbackEvent>) -> bool {
        match self.callback_queues.get(&target) {
            Some(_) => false,
            None => {
                self.callback_queues.insert(target, queue);
                true
            }
        }
    }

    pub(super) fn queue_for(&self, target: &CallbackRouteTarget) -> Option<mpsc::Sender<CallbackEvent>> {
        self.callback_queues.get(target).cloned()
    }

    pub(super) fn remove_target(&mut self, target: &CallbackRouteTarget) {
        self.callback_queues.remove(target);
        if let Ok(mut router) = self.callback_router.try_lock() {
            router.remove_target(target);
        }
    }

    pub(super) fn clear(&mut self) {
        self.callback_queues.clear();
        if let Ok(mut router) = self.callback_router.try_lock() {
            *router = CallbackRouter::default();
        }
    }
}

impl CallbackRouter {
    /// Record an ACP session owner, refusing conflicting ownership.
    pub(super) fn register_session(&mut self, acp_session_id: SessionId, target: CallbackRouteTarget) -> bool {
        Self::register_route(&mut self.session_routes, acp_session_id, target)
    }

    /// Resolve an ACP session callback without changing the registry.
    pub(super) fn session_target(&self, acp_session_id: &SessionId) -> Option<&CallbackRouteTarget> {
        self.session_routes.get(acp_session_id)
    }

    /// Record the owner needed by a later elicitation completion notification.
    pub(super) fn register_elicitation(
        &mut self,
        correlation_id: ElicitationCorrelationId,
        target: CallbackRouteTarget,
    ) -> bool {
        Self::register_route(&mut self.elicitation_routes, correlation_id, target)
    }

    /// Resolve and consume an out-of-order elicitation completion correlation.
    pub(super) fn take_elicitation(
        &mut self,
        correlation_id: &ElicitationCorrelationId,
    ) -> Option<CallbackRouteTarget> {
        self.elicitation_routes.remove(correlation_id)
    }

    pub(super) fn elicitation_target(&self, correlation_id: &ElicitationCorrelationId) -> Option<CallbackRouteTarget> {
        self.elicitation_routes.get(correlation_id).cloned()
    }

    /// Remove all correlations owned by a detached route target.
    pub(super) fn remove_target(&mut self, target: &CallbackRouteTarget) {
        self.session_routes.retain(|_, route| route != target);
        self.elicitation_routes.retain(|_, route| route != target);
    }

    fn register_route<K, V>(routes: &mut HashMap<K, V>, key: K, value: V) -> bool
    where
        K: Eq + std::hash::Hash,
        V: PartialEq,
    {
        match routes.get(&key) {
            Some(existing) => existing == &value,
            None => {
                routes.insert(key, value);
                true
            }
        }
    }
}

/// Internal lifecycle phases used to claim Provider transitions in the actor.
///
/// The public projection intentionally collapses `Idle` and `Stopping` into
/// client-safe states; these phases remain private to the actor's serialized
/// state machine so a transition cannot be claimed twice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ProviderSupervisorLifecycle {
    Idle,
    Starting,
    Running,
    AutoRestarting,
    Stopping,
    Stopped,
}

/// Runtime resources owned by one Provider supervisor.
///
/// The shared connection handle and owner task are retained by the supervisor;
/// the watcher is only an unexpected-exit reporter. Isolated Providers retain
/// explicit child sets so the actor can release every child without discovering
/// them externally.
pub(super) enum ProviderSupervisorResources {
    Shared {
        connection: Option<ConnectionTo<Agent>>,
        connection_owner: Option<JoinHandle<()>>,
        connection_watcher: Option<JoinHandle<()>>,
        mcp_acp: bool,
        mcp_http: bool,
        callback_router: Arc<Mutex<CallbackRouter>>,
        callback_routes: Option<Arc<Mutex<CallbackRouteRegistry>>>,
    },
    Isolated {
        child_sessions: HashSet<SluiceSessionId>,
    },
}

/// Actor-owned state for one exact installed Provider.
pub(super) struct ProviderSupervisor {
    pub(super) provider_name: String,
    pub(super) admission_enabled: bool,
    pub(super) lifecycle: ProviderSupervisorLifecycle,
    restart_budget: ProviderRestartBudget,
    pub(super) last_failure: Option<ProviderRuntimeFailure>,
    pub(super) resources: ProviderSupervisorResources,
    pub(super) chat_sessions: HashSet<SluiceSessionId>,
}

impl ProviderSupervisor {
    pub(super) fn new(provider_name: String, strategy: ConnectionStrategy) -> Self {
        let resources = match strategy {
            ConnectionStrategy::Shared => ProviderSupervisorResources::Shared {
                connection: None,
                connection_owner: None,
                connection_watcher: None,
                mcp_acp: false,
                mcp_http: false,
                callback_router: Arc::new(Mutex::new(CallbackRouter::default())),
                callback_routes: None,
            },
            ConnectionStrategy::Isolated => ProviderSupervisorResources::Isolated {
                child_sessions: HashSet::new(),
            },
        };
        Self {
            provider_name,
            admission_enabled: false,
            lifecycle: ProviderSupervisorLifecycle::Idle,
            restart_budget: ProviderRestartBudget::fresh(),
            last_failure: None,
            resources,
            chat_sessions: HashSet::new(),
        }
    }

    pub(super) fn claim_start(&mut self) -> Result<bool, DiscussError> {
        match self.lifecycle {
            ProviderSupervisorLifecycle::Idle | ProviderSupervisorLifecycle::Stopped => {
                self.lifecycle = ProviderSupervisorLifecycle::Starting;
                self.admission_enabled = false;
                Ok(true)
            }
            ProviderSupervisorLifecycle::Starting
            | ProviderSupervisorLifecycle::Running
            | ProviderSupervisorLifecycle::AutoRestarting => Ok(false),
            ProviderSupervisorLifecycle::Stopping => Err(self.transition_error("start")),
        }
    }

    pub(super) fn complete_start(&mut self) -> Result<(), DiscussError> {
        if self.lifecycle != ProviderSupervisorLifecycle::Starting {
            return Err(self.transition_error("complete start"));
        }
        self.lifecycle = ProviderSupervisorLifecycle::Running;
        self.admission_enabled = true;
        Ok(())
    }

    pub(super) fn claim_stop(&mut self) -> bool {
        match self.lifecycle {
            ProviderSupervisorLifecycle::Idle | ProviderSupervisorLifecycle::Stopped => false,
            ProviderSupervisorLifecycle::Starting
            | ProviderSupervisorLifecycle::Running
            | ProviderSupervisorLifecycle::AutoRestarting => {
                self.admission_enabled = false;
                self.lifecycle = ProviderSupervisorLifecycle::Stopping;
                true
            }
            ProviderSupervisorLifecycle::Stopping => false,
        }
    }

    pub(super) fn complete_stop(&mut self) -> Result<(), DiscussError> {
        if self.lifecycle != ProviderSupervisorLifecycle::Stopping {
            return Err(self.transition_error("complete stop"));
        }
        self.lifecycle = ProviderSupervisorLifecycle::Stopped;
        self.admission_enabled = false;
        Ok(())
    }

    pub(super) fn claim_auto_restart(&mut self) -> Result<bool, DiscussError> {
        if self.lifecycle != ProviderSupervisorLifecycle::Running {
            return Ok(false);
        }
        if !self.restart_budget.consume() {
            self.admission_enabled = false;
            self.lifecycle = ProviderSupervisorLifecycle::Stopped;
            let error = DiscussError::ProviderRestartExhausted {
                provider_name: self.provider_name.clone(),
            };
            self.record_failure(&error);
            return Err(error);
        }
        self.admission_enabled = false;
        self.lifecycle = ProviderSupervisorLifecycle::AutoRestarting;
        Ok(true)
    }

    pub(super) fn complete_auto_restart(&mut self) -> Result<(), DiscussError> {
        if self.lifecycle != ProviderSupervisorLifecycle::AutoRestarting {
            return Err(self.transition_error("complete automatic restart"));
        }
        self.lifecycle = ProviderSupervisorLifecycle::Running;
        self.admission_enabled = true;
        Ok(())
    }

    pub(super) fn reset_restart_budget(&mut self) {
        self.restart_budget = ProviderRestartBudget::fresh();
        self.last_failure = None;
    }

    pub(super) fn record_failure(&mut self, error: &DiscussError) {
        let (code, message) = match error {
            DiscussError::ProviderRuntime { message, .. } => ("ProviderRuntime", message.clone()),
            DiscussError::ProviderAdmission { message, .. } => ("ProviderAdmission", message.clone()),
            DiscussError::ProviderProcessExit { message, .. } => ("ProviderProcessExit", message.clone()),
            DiscussError::ProviderRestartExhausted { .. } => (
                "ProviderRestartExhausted",
                "The Provider exhausted its automatic restart budget".to_string(),
            ),
            DiscussError::ProviderNotRunning { .. } => {
                ("ProviderNotRunning", "The Provider is not running".to_string())
            }
            _ => ("ProviderRuntime", "The Provider runtime failed".to_string()),
        };
        self.last_failure = Some(ProviderRuntimeFailure {
            code: code.to_string(),
            message,
        });
    }

    pub(super) const fn accepts_work(&self) -> bool {
        self.admission_enabled
    }

    pub(super) fn associate_chat(&mut self, session_id: SluiceSessionId) {
        self.chat_sessions.insert(session_id);
    }

    pub(super) fn disassociate_chat(&mut self, session_id: &SluiceSessionId) {
        self.chat_sessions.remove(session_id);
    }

    pub(super) fn projection(&self) -> ProviderRuntimeState {
        let status = match self.lifecycle {
            ProviderSupervisorLifecycle::Idle | ProviderSupervisorLifecycle::Stopped => ProviderRuntimeStatus::Stopped,
            ProviderSupervisorLifecycle::Starting | ProviderSupervisorLifecycle::Stopping => {
                ProviderRuntimeStatus::Starting
            }
            ProviderSupervisorLifecycle::Running => ProviderRuntimeStatus::Running,
            ProviderSupervisorLifecycle::AutoRestarting => ProviderRuntimeStatus::AutoRestarting,
        };
        ProviderRuntimeState {
            provider_name: self.provider_name.clone(),
            status,
            restart_count: self.restart_budget.consumed(),
            last_failure: self.last_failure.clone(),
        }
    }

    fn transition_error(&self, operation: &str) -> DiscussError {
        DiscussError::ProviderAdmission {
            provider_name: self.provider_name.clone(),
            message: format!("cannot {operation} while in {:?}", self.lifecycle),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chat_target() -> CallbackRouteTarget {
        CallbackRouteTarget::Chat {
            sluice_session_id: SluiceSessionId::new(),
        }
    }

    #[test]
    fn session_routes_are_exact_and_conflicting_registration_fails_closed() {
        let mut router = CallbackRouter::default();
        let session_id = SessionId::from("shared-session".to_string());
        let owner = chat_target();
        let other_owner = chat_target();

        assert!(router.register_session(session_id.clone(), owner.clone()));
        assert_eq!(router.session_target(&session_id), Some(&owner));
        assert!(!router.register_session(session_id.clone(), other_owner));
        assert_eq!(router.session_target(&session_id), Some(&owner));
    }

    #[test]
    fn elicitation_completion_uses_its_recorded_owner_out_of_order() {
        let mut router = CallbackRouter::default();
        let first = ElicitationCorrelationId::new("first");
        let second = ElicitationCorrelationId::new("second");
        let first_owner = chat_target();
        let second_owner = chat_target();

        assert!(router.register_elicitation(first.clone(), first_owner.clone()));
        assert!(router.register_elicitation(second.clone(), second_owner.clone()));
        assert_eq!(router.take_elicitation(&second), Some(second_owner));
        assert_eq!(router.take_elicitation(&first), Some(first_owner));
        assert_eq!(router.take_elicitation(&first), None);
    }
}
