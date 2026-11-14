use tokio::sync::mpsc;

use crate::auth::AuthAdmissionRegistry;
use crate::bus::{BusRpcContext, EventBus};
use crate::config::{ConfigRegistrationSudoService, RegisterSectionInput, SectionRegistration};
use crate::runtime::ServiceReader;

use super::actor::{Actor, SECTION, run_actor};
use super::domain::{MachineDomain, MachineGeneratedHandler};
use super::messages::{MachineBroadcast, MachineRequest};
use super::peering::MachinePeerServices;
use super::types::{MachineError, MachineSettings};

/// Registers Machine's bus marker and its Config section before the runtime seals Config,
/// then starts the actor. A failed registration is kept as a typed unavailable state, not a panic.
pub(super) async fn construct<Root>(
    bus: &mut EventBus<Root>,
    options: super::types::MachineBootOptions,
    config: ServiceReader<dyn ConfigRegistrationSudoService>,
    context: BusRpcContext,
    registry: AuthAdmissionRegistry,
    peer_services: Option<MachinePeerServices>,
) where
    Root: Clone + Send + 'static,
    MachineBroadcast: Into<Root>,
{
    let channels = bus.register::<MachineDomain, MachineRequest, MachineBroadcast>();
    let (actor_sender, actor_receiver) = mpsc::channel::<MachineRequest>(64);
    let (broadcasts, broadcast_receiver) = mpsc::channel::<MachineBroadcast>(64);

    let unavailable = register_section(&config)
        .await
        .err()
        .map(|error| error.to_string());

    let actor = Actor::new(
        options,
        context,
        unavailable,
        registry,
        peer_services,
        actor_receiver,
        broadcasts,
    );
    tokio::spawn(run_actor(actor));

    let handler = MachineGeneratedHandler::new(
        channels.requests_rx,
        actor_sender,
        channels.broadcasts_tx,
        broadcast_receiver,
    );
    tokio::spawn(handler.run());
}

/// Declares Machine's section to Config; Config must be constructed before Machine.
async fn register_section(config: &ServiceReader<dyn ConfigRegistrationSudoService>) -> Result<(), MachineError> {
    let Some(registration) = config.get() else {
        return Err(MachineError::Unavailable {
            cause: "Config registration service is not published".to_owned(),
        });
    };

    let section = SectionRegistration::new::<MachineSettings>(SECTION)?;

    registration
        .register_section(RegisterSectionInput { registration: section })
        .await
        .map_err(|error| MachineError::Unavailable {
            cause: error.to_string(),
        })?;

    Ok(())
}
