use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use tokio::sync::{broadcast, mpsc, oneshot};

use aves::bus::{BusRpcContext, EventBus};
use aves::config::ConfigBroadcast;

use super::actor::{Actor, run_actor};
use super::domain::{KnowledgeDomain, KnowledgeGeneratedHandler};
use super::messages::{KnowledgeBroadcast, KnowledgeRequest, KnowledgeRequestPayload};
use super::types::KnowledgeConfig;

pub(super) struct Handle<Root>
where
    Root: Clone + Send + 'static,
    KnowledgeBroadcast: Into<Root> + Clone + Send + 'static,
{
    generated_handler: KnowledgeGeneratedHandler<Root, KnowledgeBroadcast>,
    config_rx: broadcast::Receiver<Root>,
    actor_tasks: mpsc::Sender<KnowledgeRequest>,
}

impl<Root> Handle<Root>
where
    Root: Clone + Send + 'static,
    KnowledgeBroadcast: Into<Root> + Clone + Send + 'static,
    ConfigBroadcast: TryFrom<Root>,
{
    pub(super) async fn new(
        driver: &mut EventBus<Root>,
        root_context: BusRpcContext,
        config_dir: PathBuf,
        config: KnowledgeConfig,
        shared_root: Arc<RwLock<PathBuf>>,
    ) -> Self {
        let (actor_tasks, actor_tasks_rx) = mpsc::channel(64);

        let channels = driver.register::<KnowledgeDomain, KnowledgeRequest, KnowledgeBroadcast>();

        let (broadcast_tx, actor_broadcast_rx) = mpsc::channel(64);
        let config_rx = channels.broadcasts_tx.subscribe();

        let generated_handler = KnowledgeGeneratedHandler::new(
            channels.requests_rx,
            actor_tasks.clone(),
            channels.broadcasts_tx,
            actor_broadcast_rx,
        );

        let actor = Actor::new(
            actor_tasks_rx,
            root_context,
            config_dir,
            config,
            shared_root,
            broadcast_tx,
        )
        .await;
        tokio::spawn(run_actor(actor));

        Self {
            generated_handler,
            config_rx,
            actor_tasks,
        }
    }

    pub(super) async fn run(mut self) {
        let handler = self.generated_handler.run();
        tokio::pin!(handler);
        loop {
            tokio::select! {
                _ = &mut handler => break,
                event = self.config_rx.recv() => match event {
                    Ok(root) => {
                        let Ok(ConfigBroadcast::ConfigChanged { section, .. }) = ConfigBroadcast::try_from(root) else {
                            continue;
                        };
                        let payload = match section.as_str() {
                            "knowledge" => KnowledgeRequestPayload::KnowledgeConfigChanged,
                            _ => continue,
                        };
                        let (reply_to, _reply) = oneshot::channel();
                        if self.actor_tasks.send(KnowledgeRequest { payload, reply_to }).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                },
            }
        }
    }
}
