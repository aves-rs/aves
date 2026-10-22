extern crate self as aves;

mod migrations;

pub mod auth;
pub mod bus;
pub mod config;
pub mod flow;
pub mod runtime;
pub mod tasks;
pub mod utils;

pub use auth::{
    ApprovedIssuer, AuthAdmissionRegistry, AuthCapability, AuthProjection, AuthSudoService, AvesAuthError, Jwt,
    MachineIssuerId, PairingInvitation, PeerEndpoint, PeerRecord, PeerTrustState, messages,
    messages::{AUTH_REQUEST_PAYLOAD_OPERATIONS as AUTH_OPERATIONS, AuthRequestPayload, rpc_router as auth_rpc_router},
};
pub use aves_macros::{ErrorCode, bus_requests, bus_responses, domain};
pub use bus::{
    BroadcastAdapter, BusControlError, BusIngress, BusMcpEndpoint, BusRpcContext, BusSudoService, CloseMcpInput,
    CloseRpcInput, DispatchContextError, DomainPublication, EventBus, McpClosedOutput, McpOpenedOutput,
    McpRouteMetadata, OpaqueRequestEnvelope, OpenMcpInput, OpenRpcInput, OperationDispatchFuture,
    PeerConnectivitySetOutput, RemoteOperationError, RemoteOperationInstaller, RemoteOperationRejection,
    RequestOperationDescriptor, RequestOperationKind, RpcClosedOutput, RpcOpenedOutput, SetPeerConnectivityInput,
    TransportBindings,
};
pub use runtime::{
    ConstructionFacade, ConstructionInputs, DomainCallbacks, DomainDescriptor, DomainDescriptorFactory, DomainFuture,
    DomainProviderMetadata, GeneratedCallError, ResponseProjectionError, Runtime, RuntimeOptions, ServicePublisher,
    ServiceReader, dispatch_lifecycle_request,
};
