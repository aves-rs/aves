//! Native Iroh transport for generated Bus operations.

use std::{error::Error as StdError, sync::Arc};

use iroh::{Endpoint, EndpointAddr};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::auth::{AvesAuthError, Jwt, MachineIssuerId, issuer_from_jwt};

use super::iroh::{IrohProtocolHandler, RequestFuture};
use super::messages::{BusIngress, BusRpcContext};
use super::operations::{DomainPublication, RequestOperationDescriptor};

/// ALPN negotiated by generated public Bus operations.
pub(crate) const BUS_ALPN: &[u8] = b"sluice/bus";

const MAX_OPERATION_BYTES: usize = 512 * 1024;

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
    install: Box<dyn Fn(BusIngress) -> IrohProtocolHandler + Send + Sync>,
}

impl RemoteOperationInstaller {
    pub fn for_operations<RootError>(
        descriptors: Vec<&'static [RequestOperationDescriptor<BusRpcContext, RootError>]>,
    ) -> Self
    where
        RootError: serde::Serialize + Send + Sync + 'static,
    {
        Self {
            install: Box::new(move |bus| {
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
                    Box::pin(async move {
                        let result: Result<Vec<u8>, Box<dyn StdError + Send + Sync>> =
                            dispatch(peer, &request, &publications, bus)
                                .await
                                .map_err(|error| Box::new(error) as Box<dyn StdError + Send + Sync>);
                        result
                    }) as RequestFuture
                };
                IrohProtocolHandler::new(Arc::new(handler))
            }),
        }
    }

    pub(crate) fn install(self, bus: BusIngress) -> IrohProtocolHandler {
        (self.install)(bus)
    }
}

/// Dispatches one serialized request through the existing generated public
/// operation descriptors. Privileged operations are absent from `publications`.
pub(crate) async fn dispatch<RootError>(
    peer: iroh::PublicKey,
    request: &[u8],
    publications: &[DomainPublication<BusRpcContext, RootError>],
    bus: BusIngress,
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
