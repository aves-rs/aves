use std::sync::Arc;

use slack_morphism::prelude::{
    SlackClient, SlackClientEventsListenerEnvironment, SlackClientEventsUserState, SlackClientHyperHttpsConnector,
    SlackClientSocketModeConfig, SlackClientSocketModeListener, SlackEventCallbackBody, SlackEventId,
    SlackInteractionEvent, SlackMessageEvent, SlackPushEventCallback, SlackSocketModeListenerCallbacks, SlackTeamId,
    UserCallbackResult,
};
use tokio::sync::mpsc;
use tracing::error;

use crate::channels::{InboundEvent, MessagingSubscription, MessagingTask};
use crate::messages::ListenerType;
use crate::types::{MessagingInteraction, MessagingSubscriptionError, NormalizedMessage};

use super::connection::{SlackConnection, connect};
use super::filter::SlackFilter;
use super::helpers::{interaction_to_message, normalize_update};

struct CallbackState {
    tx: mpsc::Sender<InboundEvent>,
    interaction_tx: mpsc::Sender<MessagingInteraction>,
}

pub(super) async fn listen(
    connection: Arc<SlackConnection>,
    tx: mpsc::Sender<InboundEvent>,
    interaction_tx: mpsc::Sender<MessagingInteraction>,
) -> Result<MessagingTask, MessagingSubscriptionError> {
    let delivery = tx.clone();
    let interaction_delivery = interaction_tx.clone();
    let state = CallbackState { tx, interaction_tx };
    let environment =
        Arc::new(SlackClientEventsListenerEnvironment::new(connection.client.clone()).with_user_state(state));

    let config = SlackClientSocketModeConfig::new();
    let callbacks = SlackSocketModeListenerCallbacks::<SlackClientHyperHttpsConnector>::new()
        .with_push_events(on_push_event)
        .with_interaction_events(on_interaction);
    let listener = SlackClientSocketModeListener::new(&config, environment, callbacks);

    connect(listener.listen_for(&connection.token)).await?;

    Ok(tokio::spawn(async move {
        tokio::select! {
            _ = delivery.closed() => Ok(()),
            _ = interaction_delivery.closed() => Ok(()),
            _ = listener.start() => Err(MessagingSubscriptionError::StreamClosed {
              listener_type: ListenerType::Slack,
              message: "Slack Socket Mode connection closed".to_owned()
            }),
        }
    }))
}

pub(super) fn run_filters(
    subscription: MessagingSubscription,
    mut rx: mpsc::Receiver<NormalizedMessage>,
    tx: mpsc::Sender<NormalizedMessage>,
) -> Result<MessagingTask, MessagingSubscriptionError> {
    subscription.validate(ListenerType::Slack)?;
    let policy = SlackFilter::new(subscription.subscribed_channels).with_senders(subscription.allowed_senders);
    Ok(tokio::spawn(async move {
        while let Some(message) = tokio::select! { _ = tx.closed() => None, message = rx.recv() => message } {
            if policy.accepts_message(&message) && tx.send(message).await.is_err() {
                break;
            }
        }
        Ok(())
    }))
}

async fn on_push_event(
    push_event: SlackPushEventCallback,
    _client: Arc<SlackClient<SlackClientHyperHttpsConnector>>,
    state_storage: SlackClientEventsUserState,
) -> UserCallbackResult<()> {
    let raw_payload = serde_json::to_string(&push_event).unwrap_or_default();
    let team_id = push_event.team_id.clone();
    let event_id = push_event.event_id.clone();
    if let SlackEventCallbackBody::Message(message) = push_event.event {
        let state_guard = state_storage.read().await;
        if let Some(state) = state_guard.get_user_state::<CallbackState>() {
            handle_message_event(message, &team_id, &event_id, &raw_payload, &state.tx).await;
        } else {
            tracing::error!("Slack callback state missing from user state storage");
        }
    }
    Ok(())
}

async fn on_interaction(
    interaction: SlackInteractionEvent,
    _client: Arc<SlackClient<SlackClientHyperHttpsConnector>>,
    state_storage: SlackClientEventsUserState,
) -> UserCallbackResult<()> {
    deliver_interaction(&interaction, &state_storage).await;
    Ok(())
}

async fn deliver_interaction(interaction: &SlackInteractionEvent, state_storage: &SlackClientEventsUserState) {
    let mapped = interaction_to_message(interaction);
    if let Some(interaction) = mapped {
        let state_guard = state_storage.read().await;
        if let Some(state) = state_guard.get_user_state::<CallbackState>()
            && state.interaction_tx.try_send(interaction).is_err()
        {
            error!("Slack interaction queue is unavailable");
        }
    }
}

async fn handle_message_event(
    msg_event: SlackMessageEvent,
    team_id: &SlackTeamId,
    event_id: &SlackEventId,
    raw_payload: &str,
    tx: &mpsc::Sender<InboundEvent>,
) {
    let channel_id = match &msg_event.origin.channel {
        Some(ch) => ch.clone(),
        None => {
            tracing::debug!("Slack message event has no channel ID; skipping");
            return;
        }
    };

    if msg_event.subtype.is_some() {
        tracing::debug!("Slack message event has subtype; skipping");
        return;
    }

    let event = InboundEvent {
        raw_payload: raw_payload.as_bytes().to_vec(),
        normalized: normalize_update(&msg_event, channel_id, team_id, event_id),
    };
    if tx.send(event).await.is_err() {
        error!("Slack message mpsc receiver closed; listener will exit");
    }
}

#[cfg(test)]
pub(super) async fn test_deliver_interaction(
    interaction: &SlackInteractionEvent,
    interaction_tx: mpsc::Sender<MessagingInteraction>,
) {
    use slack_morphism::listener::SlackClientEventsUserStateStorage;

    let (tx, _) = mpsc::channel(1);
    let state = SlackClientEventsUserState::new(SlackClientEventsUserStateStorage::new());
    state
        .write()
        .await
        .set_user_state(CallbackState { tx, interaction_tx });
    deliver_interaction(interaction, &state).await;
}

#[cfg(test)]
pub(super) async fn test_handle_message_event(
    msg_event: SlackMessageEvent,
    team_id: &SlackTeamId,
    event_id: &SlackEventId,
    raw_payload: &str,
    tx: &mpsc::Sender<InboundEvent>,
) {
    handle_message_event(msg_event, team_id, event_id, raw_payload, tx).await;
}
