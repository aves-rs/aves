use crate::bus::{BusRpcContext, EventBus};
use crate::flow::NodeRegistrationSupply;
use crate::runtime::{
    ConstructionFacade, DomainCallbacks, DomainDescriptorFactory, DomainFuture, dispatch_lifecycle_request,
};

use super::client::GuestRpcBindingFactoryHandle;
use super::handle::Handle;
use super::messages::{
    CancelScriptExecutionInput, ScriptNodeTickedOutput, StartRuntimeInput, StartScriptInput, StartedOutput,
    StopRuntimeInput, SuccessOutput, TasksBroadcast, TasksPrivilegedCancelScriptExecution, TasksPrivilegedStartRuntime,
    TasksPrivilegedStartScript, TasksPrivilegedStopRuntime, TasksPrivilegedTickScript, TasksRequest,
    TasksRequestPayload, TasksSudoServicePrivilegeMapping, TickScriptInput,
};
use super::sandbox::ScriptSandbox;
use super::types::TasksError;

/// Aves Tasks domain marker.
pub struct TasksDomain;

aves::domain! {
    marker TasksDomain;

    request TasksRequestPayload => TasksResponse
    error = TasksError
    transport = TasksError
    target = "tasks";

    pub service TasksLifecycleSudoService {
        StartScript => Started,
        TickScript => ScriptNodeTicked,
        CancelScriptExecution => Success,
        StartRuntime => Success,
        StopRuntime => Success,
    }
}

pub(super) use __Tasks_generated_routing::TasksGeneratedHandler;

impl<Root> DomainDescriptorFactory<EventBus<Root>> for TasksDomain
where
    Root: Clone + Send + 'static,
    TasksBroadcast: Into<Root> + Clone + Send + 'static,
{
    fn callbacks() -> DomainCallbacks<EventBus<Root>> {
        fn new_domain<'a, 'b, Root>(facade: &'a mut ConstructionFacade<'b, EventBus<Root>>) -> DomainFuture<'a>
        where
            Root: Clone + Send + 'static,
            TasksBroadcast: Into<Root> + Clone + Send + 'static,
        {
            let sandbox = ScriptSandbox::new().expect("Tasks sandbox runner construction must succeed");
            let guest_rpc_factory = facade
                .input::<GuestRpcBindingFactoryHandle>()
                .cloned()
                .expect("Tasks guest RPC binding factory construction input");
            let lifecycle_service = facade.publisher();
            let node_registration_supply = facade
                .input::<NodeRegistrationSupply>()
                .cloned()
                .expect("Tasks node registration supply construction input");
            let node_bus = facade.root_context().ingress();
            let future = Handle::new(
                facade.bus_mut(),
                sandbox,
                lifecycle_service,
                node_registration_supply,
                guest_rpc_factory,
                node_bus,
            );
            Box::pin(async move {
                let handle = future.await;
                tokio::spawn(handle.run());
            })
        }
        DomainCallbacks {
            new: new_domain::<Root>,
            start: |context| Box::pin(TasksDomain::start(context)),
            stop: |context| Box::pin(TasksDomain::stop(context)),
        }
    }
}

impl TasksDomain {
    /// Start the Tasks runtime through its typed lifecycle request.
    async fn start(context: BusRpcContext) {
        let _ = dispatch_lifecycle_request::<TasksDomain, _, _, _>(context, |reply_to| TasksRequest {
            reply_to,
            payload: TasksRequestPayload::StartRuntime,
        })
        .await;
    }

    /// Stop the Tasks runtime through its typed lifecycle request.
    async fn stop(context: BusRpcContext) {
        let _ = dispatch_lifecycle_request::<TasksDomain, _, _, _>(context, |reply_to| TasksRequest {
            reply_to,
            payload: TasksRequestPayload::StopRuntime,
        })
        .await;
    }
}
