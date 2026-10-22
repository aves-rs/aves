use aves::auth::AuthSudoService;
use aves::bus::{BusRpcContext, BusSudoService, EventBus};
use aves::config::{ConfigRegistrationSudoService, RegisterSectionInput, SectionRegistration};
use aves::flow::NodeRegistrationSupply;
use aves::runtime::{
    ConstructionFacade, DomainCallbacks, DomainDescriptorFactory, DomainFuture, ServicePublisher, ServiceReader,
    dispatch_lifecycle_request,
};
use aves::utils::errors::SharedInfraError;

use super::boot::{DiscussBootOptions, DiscussSettings};
use super::handle::new as construct_discuss_domain;
use super::messages::{
    AgentNodeStartedOutput, AgentNodeTickedOutput, AnswerElicitationInput, AnswerPermissionInput,
    CancelAgentExecutionInput, CancelElicitationInput, CancelSessionInput, CancelTurnInput, DiscussBroadcast,
    DiscussPrivilegedAnswerElicitation, DiscussPrivilegedAnswerPermission, DiscussPrivilegedCancelAgentExecution,
    DiscussPrivilegedCancelElicitation, DiscussPrivilegedCancelSession, DiscussPrivilegedCancelTurn,
    DiscussPrivilegedDisposeInspector, DiscussPrivilegedInstallProviderConfig, DiscussPrivilegedRemoveProviderConfig,
    DiscussPrivilegedReserveInspector, DiscussPrivilegedSendUserTurn, DiscussPrivilegedStartAgent,
    DiscussPrivilegedStartInspector, DiscussPrivilegedTickAgent, DiscussRequest, DiscussRequestPayload,
    DiscussResponse, DiscussSudoServicePrivilegeMapping, DisposeInspectorInput, InstallProviderConfigInput,
    RemoveProviderConfigInput, ReserveInspectorInput, ReserveInspectorOutput, SendUserTurnInput, StartAgentInput,
    StartInspectorInput, SuccessOutput, TickAgentInput,
};
use super::types::{DiscussError, WorkspaceCopySourcePort};

/// Empty lifecycle marker. Runtime state remains owned by the domain handle.
pub struct DiscussDomain;

/// Typed construction services for the Discuss domain.
pub(super) struct DiscussKernelServices {
    pub(super) root_context: BusRpcContext,
    pub(super) boot: DiscussBootOptions,
    pub(super) bus_service: ServiceReader<dyn BusSudoService>,
    pub(super) auth_service: ServiceReader<dyn AuthSudoService>,
    pub(super) streaming_service: ServicePublisher<dyn DiscussStreamingService>,
    pub(super) provider_config_service: ServicePublisher<dyn DiscussProviderConfigSudoService>,
    pub(super) workspace_copy_source_port: ServicePublisher<WorkspaceCopySourcePort>,
    pub(super) agent_node_lifecycle_service: ServicePublisher<dyn DiscussAgentNodeLifecycleService>,
    pub(super) agent_node_streaming: ServiceReader<dyn DiscussStreamingService>,
    pub(super) agent_node_copy_sources: ServiceReader<WorkspaceCopySourcePort>,
    pub(super) node_registration_supply: NodeRegistrationSupply,
}

aves::domain! {
    marker DiscussDomain;

    request DiscussRequestPayload => DiscussResponse
    error = DiscussError
    transport = SharedInfraError
    target = "discuss";

    pub service DiscussStreamingService {
        SendUserTurn => Success,
        AnswerPermission => Success,
        AnswerElicitation => Success,
        CancelElicitation => Success,
        CancelTurn => Success,
        CancelSession => Success,
        ReserveInspector => ReserveInspector,
        StartInspector => Success,
        DisposeInspector => Success,
    }

    pub service DiscussProviderConfigSudoService {
        InstallProviderConfig => Success,
        RemoveProviderConfig => Success,
    }

    pub service DiscussAgentNodeLifecycleService {
        StartAgent => AgentNodeStarted,
        TickAgent => AgentNodeTicked,
        CancelAgentExecution => Success,
    }

    outbound DiscussResponse => DiscussBroadcast {
        DiscussResponse::NewSession { session_id } => DiscussBroadcast::SessionReady {
            sluice_session_id: session_id,
        },
        DiscussResponse::SessionWarning {
            sluice_session_id,
            acp_session_id,
            warning,
        } => DiscussBroadcast::SessionWarning {
            sluice_session_id,
            acp_session_id,
            warning,
        },
        DiscussResponse::McpStatus {
            sluice_session_id,
            acp_session_id,
            status,
        } => DiscussBroadcast::McpStatus {
            sluice_session_id,
            acp_session_id,
            status,
        },
        DiscussResponse::ServerStarted => DiscussBroadcast::ServerStarted,
        DiscussResponse::ServerStopped => DiscussBroadcast::ServerStopped,
        DiscussResponse::ProviderRuntimeChanged { state } => DiscussBroadcast::ProviderRuntimeChanged { state },
    }
}

pub(super) use __Discuss_generated_routing::DiscussGeneratedHandler;

impl DiscussDomain {
    async fn construct_and_run<Root>(bus: &mut EventBus<Root>, services: DiscussKernelServices)
    where
        Root: Clone + Send + 'static,
        DiscussBroadcast: Into<Root> + Clone + Send + 'static,
    {
        let handler = construct_discuss_domain(bus, services).await;
        tokio::spawn(async move { handler.run().await });
    }

    async fn start(context: BusRpcContext) {
        dispatch_lifecycle_request::<DiscussDomain, _, _, _>(context, |reply_to| DiscussRequest {
            reply_to,
            payload: DiscussRequestPayload::StartServer,
        })
        .await;
    }

    async fn stop(context: BusRpcContext) {
        dispatch_lifecycle_request::<DiscussDomain, _, _, _>(context, |reply_to| DiscussRequest {
            reply_to,
            payload: DiscussRequestPayload::StopServer,
        })
        .await;
    }
}

impl<Root> DomainDescriptorFactory<EventBus<Root>> for DiscussDomain
where
    Root: Clone + Send + 'static,
    DiscussBroadcast: Into<Root> + Clone + Send + 'static,
{
    fn callbacks() -> DomainCallbacks<EventBus<Root>> {
        fn new_domain<'facade, 'bus, Root>(
            facade: &'facade mut ConstructionFacade<'bus, EventBus<Root>>,
        ) -> DomainFuture<'facade>
        where
            Root: Clone + Send + 'static,
            DiscussBroadcast: Into<Root> + Clone + Send + 'static,
        {
            let config_registration = facade.reader::<dyn ConfigRegistrationSudoService>();

            let boot = facade
                .input::<DiscussBootOptions>()
                .expect("Discuss boot options must be supplied explicitly")
                .clone();

            let services = DiscussKernelServices {
                root_context: facade.root_context().clone(),
                boot,
                bus_service: facade.reader(),
                auth_service: facade.reader(),
                streaming_service: facade.publisher(),
                provider_config_service: facade.publisher(),
                workspace_copy_source_port: facade.publisher(),
                agent_node_lifecycle_service: facade.publisher(),
                agent_node_streaming: facade.reader(),
                agent_node_copy_sources: facade.reader(),
                node_registration_supply: facade
                    .input::<NodeRegistrationSupply>()
                    .cloned()
                    .expect("Discuss node registration supply construction input"),
            };

            let future = DiscussDomain::construct_and_run(facade.bus_mut(), services);

            Box::pin(async move {
                config_registration
                    .get()
                    .expect("Config registration service must be published first")
                    .register_section(RegisterSectionInput {
                        registration: SectionRegistration::new::<DiscussSettings>("discuss")
                            .expect("valid Discuss settings section"),
                    })
                    .await
                    .expect("Discuss settings section must be unique");
                future.await;
            })
        }

        DomainCallbacks {
            new: new_domain,
            start: |context| Box::pin(DiscussDomain::start(context)),
            stop: |context| Box::pin(DiscussDomain::stop(context)),
        }
    }
}
