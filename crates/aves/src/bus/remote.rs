//! Native Iroh transport for generated Bus operations.

use std::{
    error::Error as StdError,
    future::Future,
    io::{Error as IoError, ErrorKind},
    pin::Pin,
    sync::Arc,
};

use iroh::endpoint::{Connection, ConnectionError};
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh::{Endpoint, EndpointAddr};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::auth::{AuthAdmissionRegistry, AvesAuthError, Jwt, MachineIssuerId, NativeProtocol, issuer_from_jwt};

use super::messages::{BusIngress, BusRpcContext};
use super::operations::{DomainPublication, RequestOperationDescriptor};

/// ALPN negotiated by generated public Bus operations.
pub(crate) const BUS_ALPN: &[u8] = b"aves_app/bus";

const MAX_OPERATION_BYTES: usize = 512 * 1024;
const MAX_REQUEST_SIZE: usize = 1024 * 1024;
const MAX_RESPONSE_SIZE: usize = 1024 * 1024;

type RequestFuture = Pin<Box<dyn Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send>>;
type RequestHandler = dyn Fn(iroh::PublicKey, Vec<u8>) -> RequestFuture + Send + Sync;

#[derive(Clone)]
pub struct IrohProtocolHandler {
    request_handler: Arc<RequestHandler>,
}

impl IrohProtocolHandler {
    fn new(request_handler: Arc<RequestHandler>) -> Self {
        Self { request_handler }
    }
}

impl std::fmt::Debug for IrohProtocolHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("IrohProtocolHandler").finish()
    }
}

impl ProtocolHandler for IrohProtocolHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let remote_node_id = connection.remote_id();
        loop {
            let (mut send, mut recv) = match connection.accept_bi().await {
                Ok(streams) => streams,
                Err(ConnectionError::LocallyClosed) | Err(ConnectionError::ApplicationClosed(_)) => return Ok(()),
                Err(source) => return Err(AcceptError::from_err(source)),
            };
            let request = recv
                .read_to_end(MAX_REQUEST_SIZE)
                .await
                .map_err(AcceptError::from_err)?;
            let response = (self.request_handler)(remote_node_id, request)
                .await
                .map_err(AcceptError::from_boxed)?;
            if response.len() > MAX_RESPONSE_SIZE {
                return Err(AcceptError::from_err(IoError::new(
                    ErrorKind::InvalidData,
                    "iroh Bus response exceeds the configured limit",
                )));
            }
            send.write_all(&response)
                .await
                .map_err(AcceptError::from_err)?;
            send.finish().map_err(AcceptError::from_err)?;
        }
    }
}

/// The minimal wire request for one generated public Bus operation.
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct RemoteOperationRequest {
    pub(crate) qualified_operation: String,
    pub(crate) input: serde_json::Value,
    pub(crate) jwt: Jwt,
}

/// Runtime-owned installer for the generated public Bus operation surface.
/// It captures descriptors only; transport ownership remains with IrohSubstrate.
pub struct RemoteOperationInstaller {
    install: Box<dyn Fn(BusIngress, AuthAdmissionRegistry) -> IrohProtocolHandler + Send + Sync>,
}

impl RemoteOperationInstaller {
    pub fn for_operations<RootError>(
        descriptors: Vec<&'static [RequestOperationDescriptor<BusRpcContext, RootError>]>,
    ) -> Self
    where
        RootError: serde::Serialize + Send + Sync + 'static,
    {
        Self {
            install: Box::new(move |bus, registry| {
                let publications = Arc::new(
                    descriptors
                        .iter()
                        .map(|operations| DomainPublication {
                            server_name: "remote",
                            route_slug: "remote",
                            operations,
                        })
                        .collect::<Vec<_>>(),
                );
                let handler = move |peer: iroh::PublicKey, request: Vec<u8>| {
                    let publications = publications.clone();
                    let bus = bus.clone();
                    let registry = registry.clone();
                    Box::pin(async move {
                        let result: Result<Vec<u8>, Box<dyn StdError + Send + Sync>> =
                            dispatch(peer, &request, &publications, bus, &registry)
                                .await
                                .map_err(|error| Box::new(error) as Box<dyn StdError + Send + Sync>);
                        result
                    }) as RequestFuture
                };
                IrohProtocolHandler::new(Arc::new(handler))
            }),
        }
    }

    pub(crate) fn install(self, bus: BusIngress, registry: AuthAdmissionRegistry) -> IrohProtocolHandler {
        (self.install)(bus, registry)
    }
}

/// Dispatches one serialized request through the existing generated public
/// operation descriptors. Privileged operations are absent from `publications`.
pub(crate) async fn dispatch<RootError>(
    peer: iroh::PublicKey,
    request: &[u8],
    publications: &[DomainPublication<BusRpcContext, RootError>],
    bus: BusIngress,
    registry: &AuthAdmissionRegistry,
) -> Result<Vec<u8>, RemoteOperationError>
where
    RootError: Serialize + Send + 'static,
{
    if request.len() > MAX_OPERATION_BYTES {
        return Err(RemoteOperationError::TooLarge {
            limit: MAX_OPERATION_BYTES,
            actual: request.len(),
        });
    }

    let request =
        serde_json::from_slice::<RemoteOperationRequest>(request).map_err(RemoteOperationError::Deserialize)?;
    let peer_issuer = MachineIssuerId::from(peer);
    let jwt_issuer = issuer_from_jwt(&request.jwt).map_err(RemoteOperationError::Issuer)?;
    if jwt_issuer != peer_issuer {
        return Err(RemoteOperationError::IssuerMismatch {
            jwt_issuer,
            peer_issuer,
        });
    }
    let Some(operation) = publications
        .iter()
        .flat_map(|publication| publication.operations.iter())
        .find(|operation| operation.qualified_operation == request.qualified_operation)
    else {
        let reply = RemoteOperationReply::<RootError>::Rejected(RemoteOperationRejection::UnknownOperation);
        return serialize_reply(&reply);
    };

    if !registry.allows_protocol(&peer_issuer, NativeProtocol::Bus)
        || !registry.allows_operation(&peer_issuer, &request.qualified_operation)
    {
        return serialize_reply(&RemoteOperationReply::<RootError>::Rejected(
            RemoteOperationRejection::NotAuthorized,
        ));
    }

    let context = BusRpcContext::authenticated(bus, request.jwt);
    let reply = match (operation.dispatch)(context, request.input).await {
        Ok(output) => RemoteOperationReply::Success(output),
        Err(error) => RemoteOperationReply::DomainError(error),
    };
    serialize_reply(&reply)
}

fn serialize_reply<RootError>(reply: &RemoteOperationReply<RootError>) -> Result<Vec<u8>, RemoteOperationError>
where
    RootError: Serialize,
{
    let bytes = serde_json::to_vec(reply).map_err(RemoteOperationError::ReplySerialize)?;
    if bytes.len() > MAX_OPERATION_BYTES {
        return Err(RemoteOperationError::ReplyTooLarge {
            limit: MAX_OPERATION_BYTES,
            actual: bytes.len(),
        });
    }
    Ok(bytes)
}

/// Sends one generated operation directly on a native Iroh bidirectional
/// stream. The generated request and response types are the wire payloads.
pub(crate) async fn call<Request, Response>(
    endpoint: &Endpoint,
    target: EndpointAddr,
    request: &Request,
) -> Result<Response, RemoteOperationError>
where
    Request: Serialize,
    Response: DeserializeOwned,
{
    let bytes = serde_json::to_vec(request).map_err(RemoteOperationError::Serialize)?;
    if bytes.len() > MAX_OPERATION_BYTES {
        return Err(RemoteOperationError::TooLarge {
            limit: MAX_OPERATION_BYTES,
            actual: bytes.len(),
        });
    }
    let expected = target.id;
    let connection = endpoint
        .connect(target, BUS_ALPN)
        .await
        .map_err(|error| RemoteOperationError::Connect(Box::new(error)))?;
    let actual = connection.remote_id();
    if actual != expected {
        return Err(RemoteOperationError::IdentityMismatch { expected, actual });
    }
    let (mut send, mut receive) = connection
        .open_bi()
        .await
        .map_err(|error| RemoteOperationError::OpenBi(Box::new(error)))?;
    send.write_all(&bytes)
        .await
        .map_err(|error| RemoteOperationError::Write(Box::new(error)))?;
    send.finish()
        .map_err(|error| RemoteOperationError::Finish(Box::new(error)))?;
    let response = receive
        .read_to_end(MAX_OPERATION_BYTES)
        .await
        .map_err(|error| RemoteOperationError::Read(Box::new(error)))?;
    serde_json::from_slice(&response).map_err(RemoteOperationError::Deserialize)
}

/// Typed protocol-level rejection which is independent of any domain error.
#[derive(Debug, Deserialize, Serialize)]
pub enum RemoteOperationRejection {
    MalformedRequest,
    UnknownOperation,
    NotAuthorized,
}

/// Reply returned by the generated public-operation dispatch path.
#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum RemoteOperationReply<RootError> {
    Success(serde_json::Value),
    DomainError(RootError),
    Rejected(RemoteOperationRejection),
}

#[derive(Debug, Error)]
pub enum RemoteOperationError {
    #[error("peer connectivity is disabled")]
    ConnectivityDisabled,
    #[error("failed to serialize remote operation: {0}")]
    Serialize(#[source] serde_json::Error),
    #[error("remote operation payload exceeds {limit} bytes (got {actual})")]
    TooLarge { limit: usize, actual: usize },
    #[error("failed to connect to remote Bus endpoint: {0}")]
    Connect(#[source] Box<dyn StdError + Send + Sync>),
    #[error("failed to open remote Bus stream: {0}")]
    OpenBi(#[source] Box<dyn StdError + Send + Sync>),
    #[error("failed to write remote Bus operation: {0}")]
    Write(#[source] Box<dyn StdError + Send + Sync>),
    #[error("failed to finish remote Bus operation: {0}")]
    Finish(#[source] Box<dyn StdError + Send + Sync>),
    #[error("failed to read remote Bus operation: {0}")]
    Read(#[source] Box<dyn StdError + Send + Sync>),
    #[error("failed to deserialize remote operation: {0}")]
    Deserialize(#[source] serde_json::Error),
    #[error("failed to serialize remote operation reply: {0}")]
    ReplySerialize(#[source] serde_json::Error),
    #[error("remote operation reply exceeds {limit} bytes (got {actual})")]
    ReplyTooLarge { limit: usize, actual: usize },
    #[error("failed to read JWT issuer: {0}")]
    Issuer(#[source] AvesAuthError),
    #[error("remote operation JWT issuer {jwt_issuer} does not match Iroh peer {peer_issuer}")]
    IssuerMismatch {
        jwt_issuer: MachineIssuerId,
        peer_issuer: MachineIssuerId,
    },
    #[error("remote operation was rejected: {0:?}")]
    Rejected(RemoteOperationRejection),
    #[error("remote operation returned a domain error: {0}")]
    Domain(serde_json::Value),
    #[error("failed to issue peer authorization: {0}")]
    Auth(#[source] Box<dyn StdError + Send + Sync>),
    #[error("remote operation connected to unexpected endpoint: expected {expected}, got {actual}")]
    IdentityMismatch {
        expected: iroh::EndpointId,
        actual: iroh::EndpointId,
    },
}

#[cfg(test)]
mod tests {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

    use super::*;
    use crate::auth::{ApprovedIssuer, OperationGrants, PeerGrants, QualifiedOperation, RelationshipKey};
    use crate::bus::{OperationDispatchFuture, RequestOperationKind};

    fn dispatch_test(_: BusRpcContext, _: serde_json::Value) -> OperationDispatchFuture<String> {
        Box::pin(async { Ok(serde_json::json!("invoked")) })
    }

    static OPERATIONS: [RequestOperationDescriptor<BusRpcContext, String>; 1] = [RequestOperationDescriptor {
        domain: "test",
        operation: "read",
        qualified_operation: "test.read",
        canonical_variant: "TestRequest::Read",
        kind: RequestOperationKind::Query,
        mcp_eligible: true,
        input_type: "ReadInput",
        response_type: "ReadOutput",
        response_variant: "Read",
        input_schema: schemars::Schema::default,
        output_schema: schemars::Schema::default,
        dispatch: dispatch_test,
    }];

    #[tokio::test]
    async fn remote_dispatch_enforces_current_grants_before_invocation() {
        let local = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let peer = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let issuer = MachineIssuerId::from(peer);
        let registry = AuthAdmissionRegistry::new(ApprovedIssuer {
            issuer: local.into(),
            relationships: Default::default(),
        });
        let relationship = RelationshipKey("sharing".into());
        let bootstrap = PeerGrants {
            protocols: [NativeProtocol::AuthFirstContact, NativeProtocol::Bus]
                .into_iter()
                .collect(),
            operations: OperationGrants::Only(
                [QualifiedOperation("test.bootstrap".into())]
                    .into_iter()
                    .collect(),
            ),
        };
        registry
            .upsert_remote(ApprovedIssuer {
                issuer: issuer.clone(),
                relationships: [(relationship.clone(), bootstrap)].into_iter().collect(),
            })
            .unwrap();
        assert!(!registry.allows_protocol(&issuer, NativeProtocol::Gossip));
        assert!(!registry.allows_protocol(&issuer, NativeProtocol::Blobs));

        let claims = serde_json::json!({"iss": issuer.to_string(), "aud": uuid::Uuid::nil(), "sub": "test", "jti": uuid::Uuid::nil(), "exp": 1, "cap": ["all_bus"]});
        let jwt = Jwt::from_opaque(format!(
            "x.{}.x",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap())
        ));
        let request = |name: &str| {
            serde_json::to_vec(&RemoteOperationRequest {
                qualified_operation: name.into(),
                input: serde_json::Value::Null,
                jwt: jwt.clone(),
            })
            .unwrap()
        };
        let publications = [DomainPublication {
            server_name: "test",
            route_slug: "test",
            operations: &OPERATIONS,
        }];
        let (bus, _receiver) = tokio::sync::mpsc::channel(1);
        let reply = dispatch(peer, &request("test.read"), &publications, bus.clone(), &registry)
            .await
            .unwrap();
        assert!(matches!(
            serde_json::from_slice::<RemoteOperationReply<String>>(&reply).unwrap(),
            RemoteOperationReply::Rejected(RemoteOperationRejection::NotAuthorized)
        ));
        let reply = dispatch(peer, &request("test.unknown"), &publications, bus.clone(), &registry)
            .await
            .unwrap();
        assert!(matches!(
            serde_json::from_slice::<RemoteOperationReply<String>>(&reply).unwrap(),
            RemoteOperationReply::Rejected(RemoteOperationRejection::UnknownOperation)
        ));

        let without_bus = PeerGrants {
            protocols: [NativeProtocol::AuthFirstContact].into_iter().collect(),
            operations: OperationGrants::All,
        };
        registry
            .upsert_remote(ApprovedIssuer {
                issuer: issuer.clone(),
                relationships: [(relationship.clone(), without_bus)].into_iter().collect(),
            })
            .unwrap();
        let reply = dispatch(peer, &request("test.read"), &publications, bus.clone(), &registry)
            .await
            .unwrap();
        assert!(matches!(
            serde_json::from_slice::<RemoteOperationReply<String>>(&reply).unwrap(),
            RemoteOperationReply::Rejected(RemoteOperationRejection::NotAuthorized)
        ));

        let sharing = PeerGrants {
            protocols: [
                NativeProtocol::AuthFirstContact,
                NativeProtocol::Bus,
                NativeProtocol::Gossip,
                NativeProtocol::Blobs,
            ]
            .into_iter()
            .collect(),
            operations: OperationGrants::Only(
                [QualifiedOperation("test.read".into())]
                    .into_iter()
                    .collect(),
            ),
        };
        registry
            .upsert_remote(ApprovedIssuer {
                issuer: issuer.clone(),
                relationships: [(relationship.clone(), sharing)].into_iter().collect(),
            })
            .unwrap();
        assert!(registry.allows_protocol(&issuer, NativeProtocol::Gossip));
        assert!(registry.allows_protocol(&issuer, NativeProtocol::Blobs));
        let reply = dispatch(peer, &request("test.read"), &publications, bus.clone(), &registry)
            .await
            .unwrap();
        assert!(matches!(
            serde_json::from_slice::<RemoteOperationReply<String>>(&reply).unwrap(),
            RemoteOperationReply::Success(_)
        ));
        registry
            .upsert_remote(ApprovedIssuer {
                issuer: issuer.clone(),
                relationships: [(relationship, PeerGrants::full())].into_iter().collect(),
            })
            .unwrap();
        let reply = dispatch(peer, &request("test.read"), &publications, bus, &registry)
            .await
            .unwrap();
        assert!(matches!(
            serde_json::from_slice::<RemoteOperationReply<String>>(&reply).unwrap(),
            RemoteOperationReply::Success(_)
        ));
        registry.remove_remote(&issuer).unwrap();
        let (bus, _receiver) = tokio::sync::mpsc::channel(1);
        let reply = dispatch(peer, &request("test.read"), &publications, bus, &registry)
            .await
            .unwrap();
        assert!(matches!(
            serde_json::from_slice::<RemoteOperationReply<String>>(&reply).unwrap(),
            RemoteOperationReply::Rejected(RemoteOperationRejection::NotAuthorized)
        ));
    }
}
