mod control;
mod event_bus;
mod handle;
mod iroh;
mod messages;
mod operations;
mod remote;

pub(crate) use event_bus::AuthAdmissionConfiguration;
pub(crate) use handle::Handle as BusHandle;
pub(crate) use iroh::PeerConnectivity;
pub(crate) use iroh::{IrohSubstrate, RunningIrohSubstrate};
pub(crate) use remote::{RemoteOperationReply, RemoteOperationRequest, call};

pub use control::{
    BusControlError, BusMcpEndpoint, BusSudoService, CloseMcpInput, CloseRpcInput, McpClosedOutput, McpOpenedOutput,
    McpRouteMetadata, OpenMcpInput, OpenRpcInput, PeerConnectivitySetOutput, RpcClosedOutput, RpcOpenedOutput,
    SetPeerConnectivityInput, TransportBindings,
};
pub use event_bus::{BroadcastAdapter, EventBus, OpaqueRequestEnvelope};
pub use messages::{BusIngress, BusRpcContext, DispatchContextError};
pub use operations::{DomainPublication, OperationDispatchFuture, RequestOperationDescriptor, RequestOperationKind};
pub use remote::{RemoteOperationError, RemoteOperationInstaller, RemoteOperationRejection};
