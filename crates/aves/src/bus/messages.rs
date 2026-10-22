use std::{fmt, sync::Arc};

use thiserror::Error;
use tokio::sync::mpsc;

use crate::auth::Jwt;

use super::event_bus::OpaqueRequestEnvelope;

/// Normal ingress for an in-process bus request.
pub type BusIngress = mpsc::Sender<OpaqueRequestEnvelope>;

/// Opaque proof that a dispatch context was authenticated by its creator.
struct AuthenticationProof;

/// Context for bus-owned Specta/rspc procedures.
pub struct BusRpcContext {
    bus: Option<BusIngress>,
    proof: Option<Arc<AuthenticationProof>>,
    jwt: Option<Jwt>,
}

impl BusRpcContext {
    /// Returns the bus ingress for this context, if one is available.
    pub(crate) fn ingress(&self) -> Option<BusIngress> {
        self.bus.clone()
    }

    /// Returns the bus sender for this context, or an error if the context is unavailable.
    pub fn bus_sender(&self) -> Result<BusIngress, DispatchContextError> {
        self.admit()?;
        self.bus.clone().ok_or(DispatchContextError::Unavailable)
    }

    /// Creates an authenticated context backed by the generic bus sender.
    pub fn authenticated(bus: BusIngress, jwt: Jwt) -> Self {
        Self {
            bus: Some(bus),
            proof: Some(Arc::new(AuthenticationProof)),
            jwt: Some(jwt),
        }
    }

    /// Returns this authenticated context with a transport-supplied JWT.
    pub fn with_jwt(&self, jwt: Jwt) -> Self {
        Self {
            bus: self.bus.clone(),
            proof: self.proof.clone(),
            jwt: Some(jwt),
        }
    }

    /// Creates an unavailable dispatch context.
    pub fn unavailable() -> Self {
        Self {
            bus: None,
            proof: None,
            jwt: None,
        }
    }

    pub fn jwt(&self) -> Result<Jwt, DispatchContextError> {
        self.admit()?;
        self.jwt
            .clone()
            .ok_or(DispatchContextError::Unauthenticated)
    }

    pub fn admit(&self) -> Result<(), DispatchContextError> {
        if self.bus.is_none() {
            return Err(DispatchContextError::Unavailable);
        }
        if self.proof.is_none() || self.jwt.is_none() {
            return Err(DispatchContextError::Unauthenticated);
        }
        Ok(())
    }
}

impl Clone for BusRpcContext {
    fn clone(&self) -> Self {
        Self {
            bus: self.bus.clone(),
            proof: self.proof.clone(),
            jwt: self.jwt.clone(),
        }
    }
}

impl fmt::Debug for BusRpcContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BusRpcContext")
            .field("bus", &self.bus.as_ref().map(|_| "<present>"))
            .field("proof", &self.proof.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// Errors raised when a bus dispatch context cannot be used.
#[derive(Debug, Error)]
pub enum DispatchContextError {
    /// No dispatch context is available.
    #[error("dispatch context is unavailable")]
    Unavailable,
    /// The dispatch context has not been authenticated.
    #[error("dispatch context is unauthenticated")]
    Unauthenticated,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authenticated_context_succeeds() {
        let (bus, _) = mpsc::channel(1);
        assert!(
            BusRpcContext::authenticated(bus, Jwt::from_opaque("test-jwt".to_owned()))
                .admit()
                .is_ok()
        );
    }

    #[test]
    fn unavailable_context_returns_unavailable_error() {
        assert!(matches!(
            BusRpcContext::unavailable().admit(),
            Err(DispatchContextError::Unavailable)
        ));
    }

    #[test]
    fn sender_without_proof_returns_unauthenticated_error() {
        let (bus, _) = mpsc::channel(1);
        let context = BusRpcContext {
            bus: Some(bus),
            proof: None,
            jwt: None,
        };
        assert!(matches!(context.admit(), Err(DispatchContextError::Unauthenticated)));
    }

    #[test]
    fn debug_output_redacts_authentication_proof() {
        let (bus, _) = mpsc::channel(1);
        let debug = format!(
            "{:?}",
            BusRpcContext::authenticated(bus, Jwt::from_opaque("test-jwt".to_owned()))
        );
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("AuthenticationProof"));
    }
}
