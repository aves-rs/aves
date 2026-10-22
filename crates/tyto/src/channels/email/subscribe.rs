use std::collections::HashMap;
use std::pin;
use std::time::Duration;

use async_imap::types::NameAttribute;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::{MissedTickBehavior, interval, timeout};
use tokio_stream::StreamExt;

use crate::channels::{InboundEvent, MessagingSubscription, MessagingSubscriptionError, MessagingTask};
use crate::messages::ListenerType;
use crate::types::NormalizedMessage;

use super::connection::{ImapConnection, ImapSession, email_protocol, email_timeout};
use super::filter::EmailFilter;
use super::helpers::normalize_update;

const POLL_INTERVAL: Duration = Duration::from_secs(30);
const IO_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Copy)]
struct MailboxCursor {
    validity: Option<u32>,
    uid: u32,
}

impl MailboxCursor {
    fn pending_range(&mut self, validity: Option<u32>, uid_next: Option<u32>) -> Option<(u32, u32)> {
        if self.validity != validity {
            self.validity = validity;
            self.uid = uid_next.unwrap_or(1).saturating_sub(1);
        }
        let first = self.uid.checked_add(1)?;
        let next = uid_next?;
        (first < next).then_some((first, next))
    }
}

fn select_new_uid(uid: Option<u32>, previous_uid: u32) -> Option<u32> {
    uid.filter(|uid| *uid > previous_uid)
}

#[cfg(test)]
pub(super) fn test_cursor_range(
    validity: Option<u32>,
    uid: u32,
    next_validity: Option<u32>,
    uid_next: Option<u32>,
) -> (Option<(u32, u32)>, u32) {
    let mut cursor = MailboxCursor { validity, uid };
    let range = cursor.pending_range(next_validity, uid_next);
    (range, cursor.uid)
}

#[cfg(test)]
pub(super) fn test_new_uid(uid: Option<u32>, previous_uid: u32) -> bool {
    select_new_uid(uid, previous_uid).is_some()
}

pub(super) async fn listen(
    connection: ImapConnection,
    tx: mpsc::Sender<InboundEvent>,
) -> Result<MessagingTask, MessagingSubscriptionError> {
    Ok(tokio::spawn(async move {
        let mut cursors = HashMap::<String, MailboxCursor>::new();
        let mut ticks = interval(POLL_INTERVAL);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! { _ = tx.closed() => return Ok(()), _ = ticks.tick() => {} }
            let mut slot = connection.lock().await;
            let session = slot
                .as_mut()
                .ok_or_else(|| email_protocol("IMAP connection closed".to_owned()))?;
            let result = tokio::select! {
                _ = tx.closed() => return Ok(()),
                result = scan_mailboxes(session, &mut cursors, &tx) => result,
            };
            if result.is_err() {
                *slot = None;
            }
            result?;
        }
    }))
}

pub(super) fn run_filters(
    subscription: MessagingSubscription,
    mut rx: mpsc::Receiver<NormalizedMessage>,
    tx: mpsc::Sender<NormalizedMessage>,
) -> Result<MessagingTask, MessagingSubscriptionError> {
    subscription.validate(ListenerType::Email)?;
    let mut policy = EmailFilter::new(subscription.allowed_senders);
    policy.subscribed_channels = subscription.subscribed_channels;
    Ok(tokio::spawn(async move {
        while let Some(message) = tokio::select! { _ = tx.closed() => None, message = rx.recv() => message } {
            if let Some(accepted) = policy.apply(message)
                && tx.send(accepted).await.is_err()
            {
                break;
            }
        }
        Ok(())
    }))
}

async fn scan_mailboxes(
    session: &mut ImapSession,
    cursors: &mut HashMap<String, MailboxCursor>,
    tx: &mpsc::Sender<InboundEvent>,
) -> Result<(), MessagingSubscriptionError> {
    let folders = timeout(IO_TIMEOUT, selectable_folders(session))
        .await
        .map_err(|_| email_timeout("imap_list", IO_TIMEOUT))??;
    cursors.retain(|folder, _| folders.contains(folder));
    for folder in folders {
        let mailbox = timeout(IO_TIMEOUT, session.examine(&folder))
            .await
            .map_err(|_| email_timeout("imap_examine", IO_TIMEOUT))?
            .map_err(|error| email_protocol(format!("IMAP EXAMINE failed: {error}")))?;
        let initial = MailboxCursor {
            validity: mailbox.uid_validity,
            uid: mailbox.uid_next.unwrap_or(1).saturating_sub(1),
        };
        let cursor = cursors.entry(folder.clone()).or_insert(initial);
        let Some((first, next)) = cursor.pending_range(mailbox.uid_validity, mailbox.uid_next) else {
            continue;
        };
        let previous_uid = cursor.uid;
        let range = format!("{first}:{}", next - 1);
        let mut fetches = timeout(IO_TIMEOUT, session.uid_fetch(range, "(UID BODY.PEEK[])"))
            .await
            .map_err(|_| email_timeout("imap_uid_fetch", IO_TIMEOUT))?
            .map_err(|error| email_protocol(format!("IMAP UID FETCH failed: {error}")))?;
        while let Some(fetch) = timeout(IO_TIMEOUT, fetches.next())
            .await
            .map_err(|_| email_timeout("imap_fetch", IO_TIMEOUT))?
        {
            let fetch = fetch.map_err(|error| email_protocol(format!("IMAP FETCH stream failed: {error}")))?;
            let Some(uid) = select_new_uid(fetch.uid, previous_uid) else {
                continue;
            };
            cursor.uid = cursor.uid.max(uid);
            if let Some(raw) = fetch.body() {
                let mut normalized = normalize_update(raw);
                if let Some(message) = normalized.as_mut() {
                    message.metadata["channel_id"] = Value::String(folder.clone());
                }
                if tx
                    .send(InboundEvent {
                        raw_payload: raw.to_vec(),
                        normalized,
                    })
                    .await
                    .is_err()
                {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

async fn selectable_folders(session: &mut ImapSession) -> Result<Vec<String>, MessagingSubscriptionError> {
    let names = session
        .list(Some(""), Some("*"))
        .await
        .map_err(|error| email_protocol(format!("IMAP LIST failed: {error}")))?;
    let mut names = pin::pin!(names);
    let mut folders = Vec::new();
    while let Some(name) = names.next().await {
        let name = name.map_err(|error| email_protocol(format!("IMAP LIST stream failed: {error}")))?;
        if !name.attributes().contains(&NameAttribute::NoSelect) {
            folders.push(name.name().to_owned());
        }
    }
    Ok(folders)
}
