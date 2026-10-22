use std::path::PathBuf;

use aves::bus::{BusRpcContext, EventBus};
use aves::config::{
    ConfigBroadcast, ConfigDirectory, ConfigRegistrationSudoService, GetConfigSectionInput, RegisterSectionInput,
    SectionRegistration, dispatch_get_config_section,
};
use aves::flow::FlowWorkspaceSourceService;
use aves::runtime::{
    ConstructionFacade, DomainCallbacks, DomainDescriptorFactory, DomainFuture, ServicePublisher,
    dispatch_lifecycle_request,
};
use aves::utils::errors::SharedInfraError;

use super::documents::create_workspace_source;
use super::handle::Handle as KnowledgeHandle;
use super::messages::{KnowledgeBroadcast, KnowledgeRequest, KnowledgeRequestPayload, KnowledgeResponse};
use super::types::{KnowledgeConfig, KnowledgeError};

/// Empty lifecycle marker. Runtime state remains owned by the domain handle.
pub struct KnowledgeDomain;

aves::domain! {
    marker KnowledgeDomain;

    request KnowledgeRequestPayload => KnowledgeResponse
    error = KnowledgeError
    transport = SharedInfraError
    target = "knowledge";

    outbound KnowledgeResponse => KnowledgeBroadcast {
        KnowledgeCreated { knowledge: _ } => RecordsChanged,
        KnowledgeRecordsChanged => RecordsChanged,
        KnowledgeUpdated { knowledge: _ } => RecordsChanged,
        KnowledgeDeleted { result: _ } => RecordsChanged,
        CanvasDashboardCreated { dashboard } => CanvasDashboardCreated { dashboard_id: dashboard.id },
        CanvasDashboardUpdated { dashboard } => CanvasDashboardUpdated { dashboard_id: dashboard.id },
        CanvasDashboardDeleted { id, result: _ } => CanvasDashboardDeleted { dashboard_id: id },
    }
}

pub(super) use __Knowledge_generated_routing::KnowledgeGeneratedHandler;

impl KnowledgeDomain {
    async fn construct_and_run<Root>(
        bus: &mut EventBus<Root>,
        root_context: BusRpcContext,
        workspace_source_publisher: ServicePublisher<dyn FlowWorkspaceSourceService>,
        config: KnowledgeConfig,
        config_dir: PathBuf,
    ) where
        Root: Clone + Send + 'static,
        KnowledgeBroadcast: Into<Root> + Clone + Send + 'static,
        ConfigBroadcast: TryFrom<Root>,
    {
        let (workspace_source, shared_root) = create_workspace_source(&config, &config_dir);
        assert!(workspace_source_publisher.publish(workspace_source).is_ok());
        let handle = KnowledgeHandle::new(bus, root_context, config_dir, config, shared_root).await;
        tokio::spawn(async move { handle.run().await });
    }

    async fn start(_: BusRpcContext) {}

    async fn stop(context: BusRpcContext) {
        let _ = dispatch_lifecycle_request::<KnowledgeDomain, _, _, _>(context, |reply_to| KnowledgeRequest {
            reply_to,
            payload: KnowledgeRequestPayload::StopRuntime,
        })
        .await;
    }
}

impl<Root> DomainDescriptorFactory<EventBus<Root>> for KnowledgeDomain
where
    Root: Clone + Send + 'static,
    KnowledgeBroadcast: Into<Root> + Clone + Send + 'static,
    ConfigBroadcast: TryFrom<Root>,
{
    fn callbacks() -> DomainCallbacks<EventBus<Root>> {
        fn new_domain<'facade, 'bus, Root>(
            facade: &'facade mut ConstructionFacade<'bus, EventBus<Root>>,
        ) -> DomainFuture<'facade>
        where
            Root: Clone + Send + 'static,
            KnowledgeBroadcast: Into<Root> + Clone + Send + 'static,
            ConfigBroadcast: TryFrom<Root>,
        {
            let root_context = facade.root_context().clone();
            let publisher = facade.publisher::<dyn FlowWorkspaceSourceService>();

            let directory = facade
                .input::<ConfigDirectory>()
                .expect("config directory")
                .0
                .clone();

            let registration = facade.reader::<dyn ConfigRegistrationSudoService>();
            let bus = facade.bus_mut();

            Box::pin(async move {
                registration
                    .get()
                    .expect("Config registration service must be published first")
                    .register_section(RegisterSectionInput {
                        registration: SectionRegistration::new::<KnowledgeConfig>("knowledge")
                            .expect("valid Knowledge section"),
                    })
                    .await
                    .expect("Knowledge section must be unique");

                let config = dispatch_get_config_section(
                    &root_context,
                    GetConfigSectionInput {
                        section: "knowledge".to_owned(),
                    },
                )
                .await
                .expect("Knowledge configuration must load");

                let config = serde_json::from_value(config.value).expect("Knowledge section validated by Config");
                KnowledgeDomain::construct_and_run(bus, root_context, publisher, config, directory).await;
            })
        }

        DomainCallbacks {
            new: new_domain::<Root>,
            start: |context| Box::pin(KnowledgeDomain::start(context)),
            stop: |context| Box::pin(KnowledgeDomain::stop(context)),
        }
    }
}
