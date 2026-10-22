use std::path::PathBuf;

use tokio::sync::mpsc::Receiver;
use uuid::Uuid;

use crate::bus::{BusRpcContext, EventBus};
use crate::runtime::{ConstructionFacade, DomainCallbacks, DomainDescriptorFactory, DomainFuture, ServicePublisher};

use super::handle::{self, ConstructionResult};
use super::messages::{
    AuthPrivilegedCreateInvitation, AuthPrivilegedFirstContact, AuthPrivilegedIssueJwt, AuthPrivilegedIssuePeerJwt,
    AuthPrivilegedIssueRootJwt, AuthPrivilegedListPairingCandidates, AuthPrivilegedListPeers, AuthPrivilegedPairPeer,
    AuthPrivilegedRevokeJti, AuthPrivilegedRevokePeer, AuthPrivilegedVerifyJwt, AuthRequest, AuthRequestPayload,
    AuthSudoServicePrivilegeMapping, CreateInvitationInput, DelegatedJwtIssuedOutput, FirstContactInput,
    FirstContactedOutput, InvitationOutput, IssueJwtInput, IssuePeerJwtInput, IssueRootJwtInput, JwtIssuedOutput,
    ListPairingCandidatesInput, ListPeersInput, PairPeerInput, PairingCandidatesOutput, PeerJwtIssuedOutput,
    PeersOutput, RevokeJtiInput, RevokePeerInput, SuccessOutput, VerifiedOutput, VerifyJwtInput,
};
use super::types::AvesAuthError;

/// Empty lifecycle marker for the built-in Auth domain.
pub struct AuthDomain;

/// Construction-only inputs for the built-in Auth domain.
pub struct AuthBootOptions {
    pub namespace: String,
    pub audience: Uuid,
    pub peer_config_path: PathBuf,
}

aves::domain! {
    marker AuthDomain;

    request AuthRequestPayload => AuthResponse
    error = AvesAuthError
    transport = AvesAuthError
    target = "auth";

    pub service AuthSudoService {
        IssueJwt => DelegatedJwtIssued (operation = "issueJwt"),
        IssueRootJwt => JwtIssued (operation = "issueRootJwt"),
        IssuePeerJwt => PeerJwtIssued (operation = "issuePeerJwt"),
        VerifyJwt => Verified (operation = "verifyJwt"),
        RevokeJti => Success (operation = "revokeJti"),
        PairPeer => Success (operation = "pairPeer"),
        RevokePeer => Success (operation = "revokePeer"),
        ListPeers => Peers (operation = "listPeers"),
        ListPairingCandidates => PairingCandidates (operation = "listPairingCandidates"),
        CreateInvitation => Invitation (operation = "createInvitation"),
        FirstContact => FirstContacted (operation = "firstContact"),
    }
}

impl<Root> DomainDescriptorFactory<EventBus<Root>> for AuthDomain
where
    Root: Clone + Send + 'static,
{
    fn callbacks() -> DomainCallbacks<EventBus<Root>> {
        fn start(_context: BusRpcContext) -> DomainFuture<'static> {
            Box::pin(async {})
        }

        fn stop(_context: BusRpcContext) -> DomainFuture<'static> {
            Box::pin(async {})
        }

        fn construct<'facade, 'bus, Root>(
            facade: &'facade mut ConstructionFacade<'bus, EventBus<Root>>,
        ) -> DomainFuture<'facade>
        where
            Root: Clone + Send + 'static,
        {
            let options = {
                let options = facade
                    .auth_boot_options()
                    .expect("built-in Auth requires runtime boot options");
                AuthBootOptions {
                    namespace: options.namespace.clone(),
                    audience: options.audience,
                    peer_config_path: options.peer_config_path.clone(),
                }
            };

            let channels = facade.bus_mut().register::<AuthDomain, AuthRequest, Root>();
            let publisher = facade.publisher::<dyn AuthSudoService>();

            let published = construct_auth_domain(&options, publisher).map(|(construction, sudo_requests)| {
                let handler = __Auth_generated_routing::AuthGeneratedHandler::new(
                    channels.requests_rx,
                    construction.actor_sender.clone(),
                    channels.broadcasts_tx,
                    sudo_requests,
                );
                tokio::spawn(handler.run());
                construction
            });
            facade.set_auth_construction(published);

            Box::pin(async {})
        }

        DomainCallbacks {
            new: construct::<Root>,
            start,
            stop,
        }
    }
}

/// Constructs Auth and returns its machine identity to the runtime.
fn construct_auth_domain(
    options: &AuthBootOptions,
    publisher: ServicePublisher<dyn AuthSudoService>,
) -> Result<(ConstructionResult, Receiver<AuthRequest>), AvesAuthError> {
    let (construction, sudo_requests) =
        handle::construct(&options.namespace, options.audience, &options.peer_config_path)?;

    publisher
        .publish(construction.service.clone())
        .map_err(|_| AvesAuthError::Internal("Auth service was already published".to_owned()))?;

    Ok((construction, sudo_requests))
}
