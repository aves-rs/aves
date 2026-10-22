use tokio::sync::mpsc;

use crate::bus::{BusIngress, EventBus};
use crate::flow::NodeRegistrationSupply;
use crate::runtime::ServicePublisher;

use super::actor::{Actor, run_actor};
use super::adapter::script_node_registration;
use super::client::GuestRpcBindingFactoryHandle;
use super::domain::{TasksDomain, TasksGeneratedHandler, TasksLifecycleSudoService, TasksLifecycleSudoServiceImpl};
use super::messages::{TasksBroadcast, TasksRequest};
use super::sandbox::ScriptSandbox;

pub(super) struct Handle<Root>
where
    Root: Clone + Send + 'static,
    TasksBroadcast: Into<Root> + Clone + Send + 'static,
{
    handler: TasksGeneratedHandler<Root, TasksBroadcast>,
}
impl<Root: Clone + Send + 'static> Handle<Root>
where
    TasksBroadcast: Into<Root> + Clone + Send + 'static,
{
    pub(super) async fn new(
        bus: &mut EventBus<Root>,
        sandbox: ScriptSandbox,
        lifecycle_service: ServicePublisher<dyn TasksLifecycleSudoService>,
        node_registration_supply: NodeRegistrationSupply,
        guest_rpc_factory: GuestRpcBindingFactoryHandle,
        node_bus: Option<BusIngress>,
    ) -> Self {
        let channels = bus.register::<TasksDomain, TasksRequest, TasksBroadcast>();

        let (sender, receiver) = mpsc::channel(64);
        let (lifecycle_service_impl, lifecycle_service_rx) = TasksLifecycleSudoServiceImpl::channel(64);

        node_registration_supply.push(script_node_registration(lifecycle_service_impl.clone(), node_bus));
        let actor = Actor::new(receiver, sandbox, guest_rpc_factory);
        tokio::spawn(run_actor(actor));

        let _ = lifecycle_service.publish(lifecycle_service_impl);

        Self {
            handler: TasksGeneratedHandler::new(
                channels.requests_rx,
                sender,
                channels.broadcasts_tx,
                lifecycle_service_rx,
            ),
        }
    }

    pub(super) async fn run(self) {
        self.handler.run().await;
    }
}
