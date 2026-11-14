use crate::auth::{AuthSudoService, PairingOwner};
use crate::bus::{BusRpcContext, BusSudoService, EventBus};
use crate::config::{
    ConfigError, ConfigRegistrationSudoService, GetConfigSectionInput, RegisterSectionInput, SectionRegistration,
    dispatch_get_config_section,
};
use crate::machine::{MachinePeerService, MachinePeerServices};
use crate::runtime::{
    ConstructionFacade, DomainCallbacks, DomainDescriptorFactory, DomainFuture, ServicePublisher, ServiceReader,
    dispatch_lifecycle_request,
};

use super::handle::{ensure_caller_has_capabilities, new as construct_flow_domain};
use super::messages::{
    FireSourceTriggerInput, FlowBroadcast, FlowPrivilegedFireSourceTrigger, FlowPrivilegedListSourceTriggers,
    FlowPrivilegedStartRuntime, FlowPrivilegedStopRuntime, FlowPrivilegedUpdateRetention, FlowRequest,
    FlowRequestPayload, FlowSudoServicePrivilegeMapping, ListSourceTriggersInput, RetentionUpdatedOutput,
    RuntimeStartedOutput, RuntimeStoppedOutput, SourceTriggersListedOutput, StartRuntimeInput, StartedOutput,
    StopRuntimeInput, UpdateRetentionInput,
};
use super::registration::NodeRegistrationSupply;
use super::triggers::TriggerRegistry;
use super::types::{FlowBootOptions, FlowError, FlowSettings, FlowWorkspaceSourceService};

/// Aves Flow domain marker.
pub struct FlowDomain;

/// Runtime descriptor name used to preserve the mobility shutdown boundary.
impl FlowDomain {
    pub(crate) const RUNTIME_DOMAIN_NAME: &'static str = "FlowDomain";
}

/// Published and late-bound kernel services used by Flow.
pub(super) struct FlowKernelServices {
    pub(super) trigger_source_service: ServicePublisher<dyn FlowTriggerSourceService>,
    pub(super) service: ServicePublisher<dyn FlowControlService>,
    pub(super) auth_service: ServiceReader<dyn AuthSudoService>,
    pub(super) mcp_bus_service: ServiceReader<dyn BusSudoService>,
    pub(super) workspace_source_service: ServiceReader<dyn FlowWorkspaceSourceService>,
    pub(super) sync_services: ServiceReader<dyn MachinePeerService>,
}

aves::domain! {
    marker FlowDomain;

    request FlowRequestPayload => FlowResponse
    error = FlowError
    transport = FlowError
    target = "flow";

    authorization_hook = ensure_caller_has_capabilities;

    pub service FlowControlService {
        UpdateRetention => RetentionUpdated,
        StartRuntime => RuntimeStarted,
        StopRuntime => RuntimeStopped,
    }

    pub service FlowTriggerSourceService {
      ListSourceTriggers => SourceTriggersListed,
        FireSourceTrigger => Started,
    }

    outbound FlowBroadcast => FlowBroadcast {
        TriggersChanged => TriggersChanged,
        DefinitionChanged { flow } => DefinitionChanged { flow },
        ExecutionStatusChanged { flow_name, snapshot } => ExecutionStatusChanged { flow_name, snapshot },
        Ready { correlation } => Ready { correlation },
        Failed { correlation, failure } => Failed { correlation, failure },
        Succeeded { correlation } => Succeeded { correlation },
        Started { execution_id, flow_name } => Started { execution_id, flow_name },
        Aborted { execution_id } => Aborted { execution_id },
    }
}

pub(super) use __Flow_generated_routing::FlowGeneratedHandler;

impl<Root> DomainDescriptorFactory<EventBus<Root>> for FlowDomain
where
    Root: Clone + Send + 'static,
    FlowBroadcast: Into<Root> + Clone + Send + 'static,
{
    fn callbacks() -> DomainCallbacks<EventBus<Root>> {
        fn construct<'facade, 'bus, Root>(
            facade: &'facade mut ConstructionFacade<'bus, EventBus<Root>>,
        ) -> DomainFuture<'facade>
        where
            Root: Clone + Send + 'static,
            FlowBroadcast: Into<Root> + Clone + Send + 'static,
        {
            let config_context = facade.root_context().clone();
            let config_registration = facade.reader::<dyn ConfigRegistrationSudoService>();

            let registration = FlowDomain::settings_registration(facade.reader::<dyn FlowControlService>());

            let mut options = facade
                .input::<FlowBootOptions>()
                .expect("Flow boot options must be supplied explicitly")
                .clone();
            options.node_registration_supply = facade
                .input::<NodeRegistrationSupply>()
                .cloned()
                .unwrap_or_default();

            let trigger_source_service = facade.publisher();
            let service = facade.publisher();

            let auth_service = facade.reader::<dyn AuthSudoService>();
            let sync_services = facade.reader::<dyn MachinePeerService>();
            let mcp_bus_service = facade.reader::<dyn BusSudoService>();
            let workspace_source_service = facade.reader::<dyn FlowWorkspaceSourceService>();

            let services = FlowKernelServices {
                trigger_source_service,
                service,
                auth_service,
                mcp_bus_service,
                workspace_source_service,
                sync_services,
            };

            let pairing_owner = facade
                .input::<PairingOwner>()
                .expect("Auth pairing owner must be supplied by the runtime")
                .clone();
            let trigger_registry = facade
                .input::<TriggerRegistry>()
                .cloned()
                .unwrap_or_default();

            // TODO: when the "default" impl of the workspace trait moves into sluice
            // revert this hard wiring so that machine is an optional peer to flow and
            // flow simply turns off the machine sync services when not connected.
            //
            // The only reason this block is here is because the current default workspace
            // implementation requires blobs and docs.
            let machine = facade
                .input::<MachinePeerServices>()
                .expect("peer services must be supplied")
                .clone();
            let bus = facade.bus_mut();

            Box::pin(async move {
                config_registration
                    .get()
                    .expect("Config registration service must be published first")
                    .register_section(RegisterSectionInput { registration })
                    .await
                    .expect("Flow must register its configuration section");
                let config = dispatch_get_config_section(
                    &config_context,
                    GetConfigSectionInput {
                        section: "flow".to_owned(),
                    },
                )
                .await
                .expect("Flow settings must load from Config");

                let settings: FlowSettings =
                    serde_json::from_value(config.value).expect("Flow settings must match their registration");

                let handler = construct_flow_domain(
                    bus,
                    options.with_retention_completed_runs(settings.retention_completed_runs),
                    services,
                    trigger_registry,
                    config_context,
                    pairing_owner,
                    &machine,
                )
                .await;

                tokio::spawn(async move { handler.run().await });
            })
        }

        DomainCallbacks {
            new: construct::<Root>,
            start: |context| Box::pin(FlowDomain::start(context)),
            stop: |context| Box::pin(FlowDomain::stop(context)),
        }
    }
}

impl FlowDomain {
    fn settings_registration(flow_control: ServiceReader<dyn FlowControlService>) -> SectionRegistration {
        SectionRegistration::new::<FlowSettings>("flow")
            .expect("valid Flow settings section")
            .on_change::<FlowSettings, _, _>(move |_, after| {
                let control = flow_control.clone();
                async move {
                    let service = control.get().ok_or_else(|| ConfigError::OwnerUpdate {
                        section: "flow".to_owned(),
                        reason: "Flow control unavailable".to_owned(),
                        code: Some("FlowServiceUnavailable".to_owned()),
                    })?;
                    service
                        .update_retention(UpdateRetentionInput {
                            retention_completed_runs: after.retention_completed_runs,
                        })
                        .await
                        .map(|_| ())
                        .map_err(|error| ConfigError::OwnerUpdate {
                            section: "flow".to_owned(),
                            reason: error.to_string(),
                            code: Some("FlowRetentionRejected".to_owned()),
                        })
                }
            })
    }

    /// Sends the typed start request through the generated lifecycle channel.
    async fn start(context: BusRpcContext) {
        let _ = dispatch_lifecycle_request::<FlowDomain, _, _, _>(context, |reply_to| FlowRequest {
            reply_to,
            payload: FlowRequestPayload::StartRuntime,
        })
        .await;
    }

    /// Sends the typed stop request through the generated lifecycle channel.
    async fn stop(context: BusRpcContext) {
        let _ = dispatch_lifecycle_request::<FlowDomain, _, _, _>(context, |reply_to| FlowRequest {
            reply_to,
            payload: FlowRequestPayload::StopRuntime,
        })
        .await;
    }
}
