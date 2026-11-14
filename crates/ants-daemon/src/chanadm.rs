//! The channel manager (`ants channel serve`): it applies the changes the owner of a channel asks for.
//!
//! Only humans and the channel manager write the channels bucket. An ant that owns a channel ([`ChannelEntry::owner`],
//! set by a human) asks for changes to its members and messenger with a [`ChannelAdminRequest`] on
//! [`subject::chanadm`], core NATS, from its MCP tool `manage_channel` ([`Admin`]). The manager answers only on the
//! requester's own reply inboxes, and applies the change by compare-and-swap, checking the owner again on every
//! attempt, so a request racing a human's transfer of the channel is refused rather than applied to the new owner's
//! channel. Each change applied is announced in the channel by [`UserId::channel_manager`], mentioning nobody.

use std::collections::VecDeque;
use std::sync::Arc;

use ants_proto::{
    CHANADM_REPEAT, CHANADM_WAIT, CHANNELS_BUCKET, ChannelAdminOutcome, ChannelAdminReply, ChannelAdminRequest,
    ChannelEntry, ChannelPost, Refusal, UserId, subject,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::{self, kv};
use chrono::Utc;
use futures::StreamExt as _;
use futures::future::BoxFuture;

use crate::bus;
use crate::channel::{self, Stored};
use crate::humans::Registry;
use crate::mcp::Roster;

/// Queue group of the channel managers: one of them takes each request.
const QUEUE: &str = "chanadm";
/// Attempts at a compare-and-swap write before answering [`Refusal::Busy`].
const ATTEMPTS: usize = 10;
/// Answers remembered for requests repeated with the same id; the oldest are forgotten first.
const MAX_ANSWERED: usize = 1024;

/// Serve channel management requests on `client` until its subscription ends. `js` reads and writes the channels
/// bucket, reads the `roster` and posts the announcements; `humans` should be watched.
pub async fn serve(client: async_nats::Client, js: jetstream::Context, roster: Roster, humans: Registry) -> Result<()> {
    let kv = js.get_key_value(CHANNELS_BUCKET).await.context(
        "opening the channels bucket; `ants channel create` as a human makes it, then the channel manager can start",
    )?;
    let mut requests =
        client.queue_subscribe(subject::ALL_CHANADM, QUEUE.to_owned()).await.context("subscribing to requests")?;
    let mut manager = Manager { client, js, kv, roster, humans, answered: VecDeque::new() };
    tracing::info!("serving channel management requests");
    while let Some(msg) = requests.next().await {
        manager.handle(msg).await;
    }
    bail!("the subscription to channel management requests ended")
}

struct Manager {
    client: async_nats::Client,
    js: jetstream::Context,
    kv: kv::Store,
    roster: Roster,
    humans: Registry,
    /// Answers sent, by request id and requester, oldest first.
    answered: VecDeque<(ants_proto::Uuid, UserId, Vec<u8>)>,
}

impl Manager {
    async fn handle(&mut self, msg: async_nats::Message) {
        let Some(reply) = msg.reply.clone() else {
            tracing::warn!(subject = %msg.subject, "dropping a channel management request without a reply subject");
            return;
        };
        let request = serde_json::from_slice::<ChannelAdminRequest>(&msg.payload)
            .map_err(|e| e.to_string())
            .and_then(|r| r.check_subject(&msg.subject).map(|()| r).map_err(|e| e.to_string()));
        let request = match request {
            Ok(request) => request,
            Err(error) => {
                tracing::warn!(subject = %msg.subject, error, "dropping a channel management request");
                return;
            }
        };
        if !request.answers_on(&reply) {
            tracing::warn!(ant = %request.ant, %reply, "dropping a request whose reply subject is not the requester's");
            return;
        }
        if let Some((.., answer)) = self.answered.iter().find(|(id, ant, _)| *id == request.id && *ant == request.ant) {
            self.answer(reply, answer.clone()).await;
            return;
        }
        let (outcome, announce) = match self.decide(&request).await {
            Ok(decided) => decided,
            Err(e) => {
                // No answer: the requester repeats the request, and a later attempt may get through.
                tracing::warn!(ant = %request.ant, channel = %request.channel, error = format!("{e:#}"), "applying");
                return;
            }
        };
        match &outcome {
            ChannelAdminOutcome::Applied { changed, .. } if !changed.is_empty() => {
                tracing::info!(ant = %request.ant, channel = %request.channel, ?changed, "applied a channel change");
            }
            ChannelAdminOutcome::Applied { .. } => {}
            ChannelAdminOutcome::Refused { refusal } => {
                tracing::info!(ant = %request.ant, channel = %request.channel, %refusal, "refused a channel change");
            }
        }
        let busy = matches!(outcome, ChannelAdminOutcome::Refused { refusal: Refusal::Busy });
        let answer = serde_json::to_vec(&ChannelAdminReply { request_id: request.id, outcome }).expect("an answer");
        if !busy {
            if self.answered.len() == MAX_ANSWERED {
                self.answered.pop_front();
            }
            self.answered.push_back((request.id, request.ant.clone(), answer.clone()));
        }
        self.answer(reply, answer).await;
        if let Some(post) = announce {
            let id = format!("chanadm-{}", request.id);
            if let Err(e) = bus::store(&self.js, post.subject(), &post, &id).await {
                tracing::warn!(channel = %request.channel, error = format!("{e:#}"), "announcing the change");
            }
        }
    }

    async fn answer(&self, reply: async_nats::Subject, answer: Vec<u8>) {
        if let Err(e) = self.client.publish(reply, answer.into()).await {
            tracing::warn!(error = %e, "answering a channel management request");
        }
    }

    /// What to answer `request`, and the post announcing what it changed, if anything.
    async fn decide(&self, request: &ChannelAdminRequest) -> Result<(ChannelAdminOutcome, Option<ChannelPost>)> {
        let refused = |refusal| Ok((ChannelAdminOutcome::Refused { refusal }, None));
        if request.is_expired(Utc::now()) {
            return refused(Refusal::Expired);
        }
        if let Err(refusal) = request.check_ops() {
            return refused(refusal);
        }
        let (ant, channel) = (&request.ant, &request.channel);
        let humans = self.humans.humans();
        for _ in 0..ATTEMPTS {
            let (mut entry, revision) = match channel::load(&self.kv, channel).await? {
                Stored::Missing => return refused(Refusal::NoSuchChannel),
                Stored::Unreadable(_) => return refused(Refusal::Unreadable),
                Stored::Entry(entry, revision) => (entry, revision),
            };
            let before = entry.clone();
            let changed = match entry.apply(ant, &request.ops, &humans) {
                Ok(changed) => changed,
                Err(refusal) => return refused(refusal),
            };
            if changed.is_empty() {
                return Ok((ChannelAdminOutcome::Applied { entry, revision, changed }, None));
            }
            if let Some(missing) = self.missing_from_roster(&before, &entry).await? {
                return refused(Refusal::NotInRoster(missing));
            }
            let value = serde_json::to_vec(&entry)?;
            match self.kv.update(entry.key(), value.into(), revision).await {
                Ok(revision) => {
                    let post = announcement(ant, &entry, &changed);
                    return Ok((ChannelAdminOutcome::Applied { entry, revision, changed }, Some(post)));
                }
                Err(e) if e.kind() == kv::UpdateErrorKind::WrongLastRevision => {
                    tracing::debug!(%channel, "the channel changed meanwhile; checking the request again");
                }
                Err(e) => return Err(e).with_context(|| format!("updating channel {channel}")),
            }
        }
        refused(Refusal::Busy)
    }

    /// The first ant that `after` makes a member or the messenger and `before` did not, when it is not in the roster.
    async fn missing_from_roster(&self, before: &ChannelEntry, after: &ChannelEntry) -> Result<Option<UserId>> {
        let messenger = after.messenger.as_ref().filter(|m| before.messenger.as_ref() != Some(*m));
        let members = after.members.iter().filter(|m| !before.is_member(m));
        let added: Vec<&UserId> = members.chain(messenger).collect();
        if added.is_empty() {
            return Ok(None);
        }
        let roster = (self.roster)().await.context("reading the roster")?;
        Ok(added.into_iter().find(|ant| !roster.iter().any(|e| e.ant == **ant)).cloned())
    }
}

/// The post announcing that `owner` made the `changed` changes to `entry`'s channel: top-level, by the channel manager,
/// naming ants without `@` so it mentions, and wakes, nobody.
pub fn announcement(owner: &UserId, entry: &ChannelEntry, changed: &[String]) -> ChannelPost {
    let text = format!("{owner}, the channel's owner, changed #{}: {}.", entry.channel, changed.join("; "));
    let mut post = ChannelPost::new(entry.channel.clone(), UserId::channel_manager(), text);
    post.mentions.clear();
    post
}

/// How an ant's MCP tools reach the channels: reading them, and asking the channel manager for changes.
pub trait Admin: Send + Sync {
    /// Every channel.
    fn channels(&self) -> BoxFuture<'_, Result<Vec<ChannelEntry>>>;
    /// The channel manager's answer to `request`, None when no manager answered within [`CHANADM_WAIT`].
    fn request(&self, request: ChannelAdminRequest) -> BoxFuture<'_, Result<Option<ChannelAdminReply>>>;
}

/// [`Admin`] over NATS: requests from `client`, whose replies come back on its own reply inboxes, and the channels
/// read through `js`.
pub fn nats_admin(client: async_nats::Client, js: jetstream::Context) -> Arc<dyn Admin> {
    Arc::new(NatsAdmin { client, js })
}

struct NatsAdmin {
    client: async_nats::Client,
    js: jetstream::Context,
}

impl Admin for NatsAdmin {
    fn channels(&self) -> BoxFuture<'_, Result<Vec<ChannelEntry>>> {
        Box::pin(async move {
            let kv = self.js.get_key_value(CHANNELS_BUCKET).await.context("opening the channels bucket")?;
            channel::list(&kv).await
        })
    }

    fn request(&self, request: ChannelAdminRequest) -> BoxFuture<'_, Result<Option<ChannelAdminReply>>> {
        Box::pin(async move {
            let payload = serde_json::to_vec(&request)?;
            let deadline = tokio::time::Instant::now() + CHANADM_WAIT;
            while tokio::time::Instant::now() < deadline {
                let next = tokio::time::Instant::now() + CHANADM_REPEAT;
                let asked = self.client.request(request.subject(), payload.clone().into());
                match tokio::time::timeout(CHANADM_REPEAT, asked).await {
                    Ok(Ok(msg)) => match serde_json::from_slice::<ChannelAdminReply>(&msg.payload) {
                        Ok(reply) if reply.request_id == request.id => return Ok(Some(reply)),
                        Ok(_) => tracing::warn!("ignoring an answer to another channel management request"),
                        Err(e) => tracing::warn!(error = %e, "ignoring a malformed channel management answer"),
                    },
                    Ok(Err(e)) => tracing::debug!(error = %e, "no channel manager answered yet"),
                    Err(_) => tracing::debug!("no channel manager answered in time"),
                }
                tokio::time::sleep_until(next.min(deadline)).await;
            }
            Ok(None)
        })
    }
}
