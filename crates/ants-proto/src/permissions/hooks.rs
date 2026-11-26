//! What the credentials of hooks acting on the bus's events from outside allow: a webhook's, delivering its events
//! to its [`Destination`]s ([`Permissions::for_webhook`]), and an observer's, reading them
//! ([`Permissions::for_observer`]).

use super::{Permissions, ack_subjects, kv_stream, ordered_consumer_subjects};
#[cfg(doc)]
use crate::StatusUpdate;
use crate::{
    CHANNELS_BUCKET, COLONY_BUCKET, ChannelId, Feed, OWNERS_BUCKET, STREAM, UserId, WEBHOOKS_BUCKET, inbox_consumer,
    inbox_prefix, subject,
};

/// Where a webhook delivers its events: an ant's inbox, or a channel.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Destination {
    Ant(UserId),
    Channel(ChannelId),
}

impl Destination {
    /// The subject `webhook` publishes on to reach the destination: one of a webhook's ([`subject::webhook_inbox`],
    /// [`subject::webhook_chan`]), never one an ant sends on.
    pub fn subject(&self, webhook: &UserId) -> String {
        match self {
            Self::Ant(ant) => subject::webhook_inbox(ant, webhook),
            Self::Channel(channel) => subject::webhook_chan(channel, webhook),
        }
    }
}

impl Permissions {
    /// What the credentials of `webhook` allow: delivering its events to `destinations` only, as itself and as
    /// a webhook, on [`Destination::subject`], never on a subject an ant or a human sends on, so it never passes for
    /// one; remembering the events it delivered, under its own keys of [`WEBHOOKS_BUCKET`]
    /// only, and recording the owner its config names for each thread it starts in an ant's inbox ([`OWNERS_BUCKET`],
    /// under that ant's keys only). It reads the entry of each channel among `destinations`, for the members a
    /// broadcast mentions, and the colony entry of each ant among them ([`COLONY_BUCKET`]), for whether the ant is
    /// paused, as a schedule's firing for a paused ant waits or is skipped; and hears its own reply inboxes, for the
    /// stream's acks. It asks for its own renewal
    /// ([`subject::renew`]) and hears the answer. Nothing else: it neither writes
    /// `ants_channels` nor `ants_humans`, so it can neither change a channel nor become a human, nor reads any message.
    /// It gets no stream info, which lists every subject stored: it reads and records its events by key alone.
    pub fn for_webhook(webhook: &UserId, destinations: &[Destination]) -> Self {
        let seen = kv_stream(WEBHOOKS_BUCKET);
        let channels = kv_stream(CHANNELS_BUCKET);
        let own = subject::kv(WEBHOOKS_BUCKET, format_args!("{webhook}.>"));
        let mut publish: Vec<String> = Vec::new();
        for destination in destinations {
            let subject = destination.subject(webhook);
            if !publish.contains(&subject) {
                publish.push(subject);
            }
            match destination {
                Destination::Channel(channel) => {
                    publish.push(format!("$JS.API.DIRECT.GET.{channels}.{}", subject::channel_entry(channel)));
                }
                Destination::Ant(ant) => publish.push(subject::kv_get(COLONY_BUCKET, ant)),
            }
        }
        publish.extend([
            // Finding JetStream on connecting: the info of a consumer named after it, which none creates. No stream
            // info, which lists every subject stored, so who messages whom and the events of other webhooks.
            format!("$JS.API.CONSUMER.INFO.{STREAM}.{}", inbox_consumer(webhook)),
            // Get and create of its own events.
            format!("$JS.API.DIRECT.GET.{seen}.{own}"),
            own,
        ]);
        let ants = destinations.iter().filter_map(|d| match d {
            Destination::Ant(ant) => Some(ant),
            Destination::Channel(_) => None,
        });
        // Recording the owner of the threads it starts in an ant's inbox, under that ant's keys, where no record is.
        publish.extend(ants.map(|ant| subject::kv(OWNERS_BUCKET, format_args!("*.{ant}"))));
        publish.push(subject::renew(webhook.as_str()));
        publish.dedup();
        Self {
            publish,
            subscribe: vec![format!("{}.>", inbox_prefix(webhook)), subject::renew_reply(webhook.as_str())],
        }
    }

    /// What the read-only credentials of `observer` allow, for hooks that act on the bus's events from outside
    /// (`ants auth user --observer`): reading every approval request, approval decision and dead letter from the
    /// stream through its own consumers ([`Feed::consumer`], each with its filter pinned in the subject that creates
    /// it), and hearing them live; hearing every ant's status ([`subject::status`]), escalations among them
    /// ([`StatusUpdate::Escalated`]); reading and watching the colony, where each ant's daemon says it serves the ant
    /// and how busy it is. It hears its own reply inboxes and asks for its own renewal ([`subject::renew`]).
    ///
    /// Nothing else: it publishes no message, decides no approval, cancels nothing and writes no bucket, so it can
    /// neither act for an ant or a human nor change what they see. It reads no inbox, notes or channel, so no
    /// conversation as stored, nor the sessions, attempts, owners, links, chains, humans, channels, webhooks, read,
    /// devices or presence buckets: where the devices of humans are is for humans alone to see. It gets no stream info,
    /// which lists every subject stored, so who messages whom, nor message gets, which take any sequence: it finds the
    /// stream through the info of a consumer named after it, as a webhook does.
    pub fn for_observer(observer: &UserId) -> Self {
        let mut publish = vec![format!("$JS.API.CONSUMER.INFO.{STREAM}.{}", inbox_consumer(observer))];
        for feed in Feed::ALL {
            let consumer = feed.consumer(observer);
            publish.extend([
                format!("$JS.API.CONSUMER.CREATE.{STREAM}.{consumer}.{}", feed.filter()),
                format!("$JS.API.CONSUMER.INFO.{STREAM}.{consumer}"),
                format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{consumer}"),
            ]);
            publish.extend(ack_subjects(STREAM, &consumer));
            publish.push(format!("$JS.API.CONSUMER.DELETE.{STREAM}.{consumer}"));
        }
        let colony = kv_stream(COLONY_BUCKET);
        publish.extend([format!("$JS.API.STREAM.INFO.{colony}"), format!("$JS.API.DIRECT.GET.{colony}.>")]);
        publish.extend(ordered_consumer_subjects(&colony));
        publish.push(subject::renew(observer.as_str()));
        let subscribe = vec![
            format!("{}.>", inbox_prefix(observer)),
            subject::ALL_APPROVAL_REQUESTS.to_owned(),
            subject::ALL_APPROVAL_DECISIONS.to_owned(),
            subject::DEAD_ALL.to_owned(),
            subject::ALL_STATUS.to_owned(),
            subject::renew_reply(observer.as_str()),
        ];
        Self { publish, subscribe }
    }
}
