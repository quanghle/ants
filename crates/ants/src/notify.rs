//! Notifications of `ants webhook serve`: the tool requests and escalations of the humans its config lists
//! (`[[human]]`), pushed to their phones and chats, with approve and reject buttons where the service has them.
//!
//! Each human is a NATS user of its own, with its human credentials, and takes its notifications from its durable
//! consumer [`notify_consumer`]: every ant's tool requests (`ants.approvals.<ant>`) and the escalations reaching its
//! inbox. Every process listing the human shares that consumer, so each request or escalation is notified once,
//! whichever daemon raised it, and the consumer starts with what arrives from its creation on. A request already
//! decided or expired, or an escalation older than [`ESCALATION_MAX_AGE`], is skipped; a sink that fails is logged
//! and the others still get the notice.
//!
//! Every sink kind takes the same [`Notice`] through the same path, [`dispatch`]: [`Sink::request`] puts it in the
//! kind's form, a [`Delivery`] (a JSON `POST` for ntfy, Slack, Telegram and `http`, a command for `exec`), and
//! [`send_to`] delivers it within the sink's timeout, with errors that never name a URL, token or argument. An `exec`
//! sink gets the notice as JSON on its standard input, an `http` one the same JSON in a `POST` signed with
//! HMAC-SHA256 ([`notice_json`]).
//!
//! [`dispatch`]: sinks::dispatch
//! [`Sink::request`]: config::Sink::request
//! [`Delivery`]: sinks::Delivery
//! [`send_to`]: sinks::send_to
//! [`notice_json`]: notices::notice_json
//!
//! A sink may be the push target of one device of the human (`device`): it gets no notice of what that device follows
//! as it comes, the human's inbox for an escalation and the tool requests for an approval, while the device's
//! [`ants_proto::Presence`] says it follows it ([`Present`]). A sink of no device gets every notice.
//!
//! With `public_url` and an action secret, an approval notice carries one link per verdict the request offers: the
//! request's ant, id and stream sequence, the verdict, the human and the request's expiry, signed with HMAC-SHA256
//! ([`Links`]). `GET` on a link only shows the request and a button that `POST`s to it, so a link preview decides
//! nothing; `POST` checks the signature and expiry and stores the decision with the human's credentials, exactly as
//! `ants tools approve` or `reject` does (the first decision wins). ntfy's buttons `POST` directly; Slack's and
//! Telegram's open the confirmation page.
//!
//! Each human's connection also answers ants asking for the reactions to a message, in one queue group across every
//! process and human: only humans' credentials read the reactions bucket, and the answer lists them only for a message
//! the ant reads ([`ants_daemon::records::reactions::serve`]).
//!
//! A notice a snooze or quiet hours hold back is dropped. With `digest = true`, it is counted first in the human's
//! [`HeldNotices`] in [`NOTIFY_BUCKET`], with compare-and-set, by whichever process took it; once neither holds any
//! more, checked every [`DIGEST_EVERY`] and before the next notice sent, one process claims the count by deleting it
//! at the revision it read and sends one digest notice: the counts per kind and the tool requests still pending.
//!
//! A reply is not notified as it comes but once its thread has been quiet for the human's `completion_window`, folded
//! with the other replies on the thread meanwhile ([`replies`]): held in memory by the process that took it, so a
//! restart drops what waits.
//!
//! Each human's reminders ([`ants_proto::Reminders`]) are checked every [`REMIND_EVERY`]: one process marks those due
//! pushed with compare-and-set and sends them, neither a snooze nor quiet hours holding them back.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ants_daemon::bus;
use ants_daemon::delivery::unserved::Watch;
use ants_daemon::people::humans::{Registry, Watching};
use ants_daemon::records::direct_kv::{DirectKv, Lookup};
use ants_daemon::sync::MutexExt as _;
use ants_proto::{
    ApprovalRequest, Envelope, Following, HeldNotices, JsDomain, NOTIFY_BUCKET, NotifyPrefs, Role, STREAM, UserId,
    Uuid, held_key, notify_consumer, subject,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, consumer, kv, stream::Stream};
use chrono::{DateTime, TimeDelta, Utc};
use futures::StreamExt as _;
use tokio::sync::OnceCell;

use self::notices::{
    About, Notice, action_notice, approval_notice, completion_notice, digest_notice, escalation_notice, held, holding,
    question_notice, relayed, reminder_notice, unserved_notice,
};
use self::sinks::send;
use crate::people::presence::Present;
use crate::tools;

pub use self::config::{Human, HumanEntry, resolve_program};
pub use self::links::{Answers, Links};
pub use self::sinks::{probe_links, test};

mod config;
mod links;
mod notices;
mod replies;
mod sinks;

/// Where approval links point, under the config's `path`. A webhook name starts with a letter or digit, so no
/// webhook is named like it.
pub const APPROVAL_PATH: &str = "/_approval";
/// How long one sink may take to take a notice, unless its `timeout` says otherwise.
const SEND_TIMEOUT: Duration = Duration::from_secs(10);
/// The longest `timeout` of a sink, well within [`ACK_WAIT`].
const MAX_SEND_TIMEOUT: Duration = Duration::from_secs(30);
/// The version of the JSON notice exec and http sinks get ([`notice_json`](notices::notice_json)).
const NOTICE_VERSION: u32 = 1;
/// Shortest signing secret of an http sink.
const MIN_SECRET: usize = 16;
/// How long the consumer waits for a notice to be sent before handing it out again.
const ACK_WAIT: Duration = Duration::from_secs(60);
const MAX_DELIVER: i64 = 3;
/// How long the consumer of a human no process lists any more stays.
const INACTIVE: Duration = Duration::from_hours(7 * 24);
/// Escalations older than this, as after a long outage, are not notified.
const ESCALATION_MAX_AGE: TimeDelta = TimeDelta::days(1);
/// Longest text taken from a request's input or an escalation.
const MAX_TEXT: usize = 1500;
/// How many notified stream sequences a human remembers, against a redelivery after a lost ack.
const RECENT: usize = 256;
/// Most of a sink's answer read, for its error.
const MAX_ANSWER: usize = 512;
/// How often a human's held notices are checked for a digest due, besides before each notice sent.
const DIGEST_EVERY: Duration = if cfg!(test) { Duration::from_millis(200) } else { Duration::from_secs(60) };
/// How often the replies waiting are checked for notices due.
const REPLIES_EVERY: Duration = if cfg!(test) { Duration::from_millis(100) } else { Duration::from_secs(1) };
/// How long a thread is quiet before its replies are notified, unless `completion_window` says otherwise.
const COMPLETION_WINDOW: Duration = Duration::from_mins(1);
/// The longest `completion_window`.
const MAX_COMPLETION_WINDOW: Duration = Duration::from_hours(1);
/// Most pending tool requests a digest's text lists.
const DIGEST_PENDING: usize = 10;
/// How often a human's reminders are checked for those due.
const REMIND_EVERY: Duration = if cfg!(test) { Duration::from_millis(200) } else { Duration::from_secs(30) };
/// How many times a held notice is counted again when another process counted or claimed meanwhile.
const CONFLICTS: usize = 10;

// ---- watching --------------------------------------------------------------------------------------------------

/// A human's connection to the hub.
struct HumanBus {
    client: async_nats::Client,
    js: jetstream::Context,
    stream: Stream,
    present: Present,
    /// The human's holds ([`NotifyPrefs`]).
    prefs: DirectKv,
    /// The humans of the team, kept current while `_watching` lives.
    humans: Registry,
    _watching: Watching,
    /// Where the notices held back are counted for a digest ([`HeldNotices`]); none without `digest`, or when the
    /// bucket cannot be created.
    held: Option<kv::Store>,
}

/// A human of the config, and its connection once made.
pub struct Notified {
    pub config: Human,
    bus: OnceCell<HumanBus>,
    recent: Mutex<VecDeque<u64>>,
    replies: Mutex<replies::Replies>,
}

impl Notified {
    pub fn new(config: Human) -> Self {
        Self { config, bus: OnceCell::new(), recent: Mutex::default(), replies: Mutex::default() }
    }

    /// Whether its connection is made and up.
    pub fn connected(&self) -> bool {
        self.bus.get().is_some_and(|bus| ants_daemon::monitoring::metrics::connected(&bus.client))
    }

    /// Whether the message at `seq` was notified, recording it if not.
    fn first_time(&self, seq: u64) -> bool {
        let mut recent = self.recent.locked();
        if recent.contains(&seq) {
            return false;
        }
        if recent.len() == RECENT {
            recent.pop_front();
        }
        recent.push_back(seq);
        true
    }
}

/// Connect `human` as itself and notify it of what reaches its consumer, until the process ends. Fails only when the
/// connection does.
pub async fn run(
    human: Arc<Notified>,
    links: Option<Arc<Links>>,
    nats: String,
    js_domain: Option<JsDomain>,
) -> Result<()> {
    let name = &human.config.name;
    let creds = human.config.creds.as_deref();
    let (client, js) =
        bus::connect_retrying(&nats, creds, js_domain.as_ref(), name, "ants-notify", std::future::pending())
            .await?
            .context("stopped")?;
    let mut delay = Duration::from_secs(1);
    let stream = loop {
        match js.get_stream(STREAM).await {
            Ok(stream) => break stream,
            Err(e) => {
                tracing::warn!(human = %name, error = %e, retry_in = ?delay, "no stream {STREAM} yet: `ants init` creates it");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(30));
            }
        }
    };
    // Through a leaf node, a direct get of a missing bucket goes unanswered rather than finding none.
    if human.config.devices().next().is_some()
        && let Err(e) = crate::people::presence::ensure_bucket(&js).await
    {
        tracing::warn!(human = %name, "sinks of a device get every notice: {e:#}");
    }
    let present = Present::new(&js, js_domain.as_ref()).await?;
    // Through a leaf node, a direct get of a missing bucket goes unanswered rather than finding none.
    let bucket = crate::people::quiet::ensure_bucket(&js).await.inspect_err(|e| tracing::warn!(human = %name, "{e:#}"));
    let held = bucket.ok().filter(|_| human.config.digest);
    let prefs = DirectKv::new(&js, js_domain.as_ref(), NOTIFY_BUCKET).await?;
    let unserved = unserved_watch(&human.config, &js, js_domain.as_ref()).await;
    let humans = Registry::new(name.clone()).verifying_grants(ants_daemon::people::humans::account(creds));
    let _watching = humans.watch(js.clone(), ants_daemon::people::humans::RETRY);
    let _ = human.bus.set(HumanBus { client, js, stream, present, prefs, humans, _watching, held });
    let bus = human.bus.get().expect("just set");
    tracing::info!(human = %name, sinks = ?human.config.sinks, "notifying");
    let mut unserved = Box::pin(tell_unserved(&human.config, bus, unserved));
    let mut reactions = Box::pin(answer_reactions(name, bus));
    let mut digests = Box::pin(digests(&human.config, bus));
    let mut reminders = Box::pin(remind(&human.config, bus, js_domain.as_ref()));
    let mut replies = Box::pin(replies_due(&human, bus));
    let mut delay = Duration::from_secs(1);
    loop {
        let watched = tokio::select! {
            watched = watch(&human, bus, links.as_deref()) => watched,
            never = &mut unserved => match never {},
            never = &mut reactions => match never {},
            never = &mut digests => match never {},
            never = &mut reminders => match never {},
            never = &mut replies => match never {},
        };
        match watched {
            Ok(()) => delay = Duration::from_secs(1),
            Err(e) => {
                tracing::warn!(human = %name, retry_in = ?delay, "notifications: {e:#}");
                delay = (delay * 2).min(Duration::from_secs(30));
            }
        }
        tokio::time::sleep(delay).await;
    }
}

/// Take notifications from the human's consumer, created or updated first, until its messages end.
async fn watch(human: &Notified, bus: &HumanBus, links: Option<&Links>) -> Result<()> {
    let name = notify_consumer(&human.config.name);
    let consumer: consumer::PullConsumer = bus
        .stream
        .create_consumer(consumer::pull::Config {
            durable_name: Some(name.clone()),
            filter_subjects: human.config.subjects(),
            deliver_policy: consumer::DeliverPolicy::New,
            ack_policy: consumer::AckPolicy::Explicit,
            ack_wait: ACK_WAIT,
            max_deliver: MAX_DELIVER,
            inactive_threshold: INACTIVE,
            ..Default::default()
        })
        .await
        .with_context(|| format!("creating consumer {name}"))?;
    let mut messages = consumer.messages().await.with_context(|| format!("reading consumer {name}"))?;
    while let Some(message) = messages.next().await {
        let message = match message {
            Ok(message) => message,
            Err(e) => {
                tracing::debug!(human = %human.config.name, error = %e, "notifications");
                continue;
            }
        };
        let seq = message.info().map_or_default(|i| i.stream_sequence);
        if human.first_time(seq) {
            notify(human, bus, links, &message.subject, &message.payload, seq).await;
        }
        if let Err(e) = message.ack().await {
            tracing::warn!(human = %human.config.name, error = %e, "acking a notification");
        }
    }
    Ok(())
}

/// Notify `human` of the message at `seq` on `subject`, if it is a pending tool request it wants, or an escalation,
/// question or reply it takes.
async fn notify(human: &Notified, bus: &HumanBus, links: Option<&Links>, subject: &str, payload: &[u8], seq: u64) {
    let config = &human.config;
    let now = Utc::now();
    let inbox = subject::parse_inbox(subject).is_some_and(|(to, _)| to == config.name);
    let notice = if inbox {
        match serde_json::from_slice::<Envelope>(payload) {
            Ok(envelope) if envelope.check_subject(subject).is_ok() && is_recent(&envelope.id, now) => {
                match (envelope.escalation.is_some(), envelope.question.is_some()) {
                    (true, _) if config.escalations => {
                        escalation_notice(&envelope).filter(|notice| !config.gets_relayed_request(&notice.about))
                    }
                    (false, true) if config.questions => question_notice(&envelope),
                    (false, false) if envelope.notice.is_some() => {
                        action_notice(&envelope).filter(|_| config.escalations)
                    }
                    (false, false) if config.completions => completion_notice(&envelope),
                    _ => None,
                }
            }
            _ => None,
        }
    } else {
        let request = match serde_json::from_slice::<ApprovalRequest>(payload) {
            Ok(request) if request.check_subject(subject).is_ok() => request,
            _ => {
                tracing::debug!(%subject, seq, "skipping an unreadable or forged tool request");
                return;
            }
        };
        if !config.wants_request_of(&request.ant) || request.is_expired(now) {
            return;
        }
        match tools::state(&bus.js, &bus.stream, seq, &request, now).await {
            Ok(tools::State::Pending) => {
                let suggested = tools::suggestion(&bus.stream, seq, &request).await.unwrap_or_else(|e| {
                    tracing::warn!(human = %config.name, error = format!("{e:#}"), "counting earlier approvals");
                    None
                });
                Some(approval_notice(&request, seq, &config.name, links, suggested.as_ref(), now))
            }
            Ok(_) => None,
            Err(e) => {
                tracing::warn!(human = %config.name, error = format!("{e:#}"), "reading a tool request's state");
                Some(approval_notice(&request, seq, &config.name, links, None, now))
            }
        }
    };
    let Some(notice) = notice else { return };
    if matches!(notice.about, About::Completion { .. }) && !config.completion_window.is_zero() {
        human.replies.locked().add(notice, seq, now);
        return;
    }
    deliver(config, bus, &notice, seq, inbox, now).await;
}

/// Send `notice`, of the message at `seq` (in the human's inbox if `inbox`, else a tool request), at `now`, unless a
/// mute, a snooze or quiet hours hold it back, to the sinks of the devices not reading what it is about.
async fn deliver(config: &Human, bus: &HumanBus, notice: &Notice, seq: u64, inbox: bool, now: DateTime<Utc>) {
    let prefs = holds(config, bus).await;
    if let Some(why) = held(config, &notice.about, &prefs, now) {
        // Dropped, not sent later: the message stays unread and a request pending, as for a notice never sent.
        tracing::info!(human = %config.name, why, title = notice.title, "held back a notice");
        // Counted for the human's digest even when relayed to its stand-in: the digest tells what it missed.
        if why != "muted thread" {
            count_held(config, bus, notice.about.kind(), now).await;
        }
        relay(config, bus, notice, why, seq).await;
        return;
    }
    digest(config, bus, &prefs, now).await;
    let what = if inbox { Following::Inbox } else { Following::Approvals };
    let reading = bus.present.following(&config.name, config.devices(), what).await;
    if !reading.is_empty() {
        tracing::debug!(human = %config.name, devices = ?reading, what = what.as_str(), "skipping devices reading");
    }
    send(config, notice, |device| !reading.contains(device)).await;
}

/// Check every [`REPLIES_EVERY`] which of the replies waiting for `human` are due, and send their notices.
async fn replies_due(human: &Notified, bus: &HumanBus) -> std::convert::Infallible {
    let window = human.config.completion_window;
    if !window.is_zero() {
        loop {
            tokio::time::sleep(REPLIES_EVERY).await;
            let due = human.replies.locked().due(Utc::now(), window);
            for (notice, seq) in due {
                deliver(&human.config, bus, &notice, seq, true, Utc::now()).await;
            }
        }
    }
    std::future::pending().await
}

/// Relay `notice`, held back from `human` by `why`, to its stand-in ([`relayed`]) when `human` is away: no device of
/// its sinks follows what the notice is about. The relay is stored once, whichever process of `human` relays it.
async fn relay(human: &Human, bus: &HumanBus, notice: &Notice, why: &str, seq: u64) {
    let Some(relay) = relayed(human, notice, why, |user| bus.humans.is_human(user)) else { return };
    let what = if matches!(notice.about, About::Approval { .. }) { Following::Approvals } else { Following::Inbox };
    let reading = bus.present.following(&human.name, human.devices(), what).await;
    if !reading.is_empty() {
        tracing::debug!(human = %human.name, devices = ?reading, "not away: no relay to the stand-in");
        return;
    }
    let id = format!("standin-{}-{seq}", human.name);
    match bus::store(&bus.js, relay.subject(), &relay, &id).await {
        Ok(_) => {
            tracing::info!(human = %human.name, stand_in = %relay.to, title = notice.title, "relayed to the stand-in");
        }
        Err(e) => tracing::warn!(
            human = %human.name,
            stand_in = %relay.to,
            error = format!("{e:#}"),
            "relaying to the stand-in"
        ),
    }
}

/// The watch of the messages `human` sends ([`unserved`]), when it takes escalations.
async fn unserved_watch(human: &Human, js: &jetstream::Context, js_domain: Option<&JsDomain>) -> Option<Watch> {
    if !human.escalations {
        return None;
    }
    let name = &human.name;
    let colony = ants_daemon::mcp::jetstream_colony(js.clone());
    Watch::new(js, js_domain, name.clone(), Role::Human, colony)
        .await
        .inspect_err(|e| tracing::warn!(human = %name, error = format!("{e:#}"), "watching the messages sent"))
        .ok()
}

/// Tell `human`, through its sinks, of the messages it sent that keep waiting for an ant no daemon serves, as `watch`
/// finds them: those held back by a snooze or quiet hours wait for them to end.
async fn tell_unserved(human: &Human, bus: &HumanBus, watch: Option<Watch>) -> std::convert::Infallible {
    if let Some(mut watch) = watch {
        loop {
            for found in watch.next(|user| bus.humans.is_human(user)).await {
                let now = Utc::now();
                let notice = unserved_notice(&found, &human.name, now);
                if let Some(why) = held(human, &notice.about, &holds(human, bus).await, now) {
                    tracing::debug!(human = %human.name, why, title = notice.title, "holding back a notice");
                    continue;
                }
                if let Some(found) = watch.claim(found).await {
                    send(human, &unserved_notice(&found, &human.name, Utc::now()), |_| true).await;
                }
            }
        }
    }
    std::future::pending().await
}

/// Answer the ants asking for the reactions to the messages they read, with the credentials of `human`, which read
/// every reaction, while the process runs; `ants signer` alone changes an ant's reactions.
async fn answer_reactions(human: &UserId, bus: &HumanBus) -> std::convert::Infallible {
    let mut delay = Duration::from_secs(1);
    loop {
        let answering = ants_daemon::records::reactions::Answering::Lists;
        if let Err(e) =
            ants_daemon::records::reactions::serve(bus.client.clone(), bus.js.clone(), None, answering).await
        {
            tracing::warn!(human = %human, retry_in = ?delay, "answering reactions requests: {e:#}");
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}

/// The notices held back from `human` and the revision they were read at, 0 when never stored: none when missing or
/// deleted, empty counts when malformed.
async fn load_held(kv: &kv::Store, human: &UserId) -> Result<(Option<HeldNotices>, u64)> {
    let Some(entry) = kv.entry(held_key(human)).await.context("reading the notices held back")? else {
        return Ok((None, 0));
    };
    if entry.operation != kv::Operation::Put {
        return Ok((None, entry.revision));
    }
    let held = serde_json::from_slice(&entry.value).unwrap_or_else(|e| {
        tracing::warn!(%human, error = %e, "malformed notices held back: starting over");
        HeldNotices::default()
    });
    Ok((Some(held), entry.revision))
}

/// Count a notice of `kind` held back from `human` at `now`, for its digest, on top of what other processes counted
/// meanwhile; a failure is logged.
async fn count_held(human: &Human, bus: &HumanBus, kind: &str, now: DateTime<Utc>) {
    let Some(kv) = &bus.held else { return };
    let name = &human.name;
    for _ in 0..CONFLICTS {
        let (held, revision) = match load_held(kv, name).await {
            Ok(loaded) => loaded,
            Err(e) => {
                tracing::warn!(human = %name, error = format!("{e:#}"), "counting a notice held back");
                return;
            }
        };
        let mut held = held.unwrap_or_default();
        held.add(kind, now);
        let Ok(value) = serde_json::to_vec(&held) else { return };
        match kv.update(held_key(name), value.into(), revision).await {
            Ok(_) => return,
            Err(e) if e.kind() == kv::UpdateErrorKind::WrongLastRevision => {}
            Err(e) => {
                tracing::warn!(human = %name, error = %e, "counting a notice held back");
                return;
            }
        }
    }
    tracing::warn!(human = %name, "counting a notice held back: other processes kept changing the count");
}

/// Send `human` one digest of the notices held back, if any, once a snooze or quiet hours no longer hold at `now`
/// ([`holding`]), as `prefs` and its config say. The process whose delete of the count, at the revision it read, is
/// stored sends it, so one of those notifying the human does, at most once; the others find it gone or changed.
/// Nothing is marked read or decided.
async fn digest(human: &Human, bus: &HumanBus, prefs: &NotifyPrefs, now: DateTime<Utc>) {
    let Some(kv) = &bus.held else { return };
    if holding(human, prefs, now) {
        return;
    }
    let name = &human.name;
    let (held, revision) = match load_held(kv, name).await {
        Ok((Some(held), revision)) => (held, revision),
        Ok((None, _)) => return,
        Err(e) => {
            tracing::warn!(human = %name, error = format!("{e:#}"), "reading the notices held back");
            return;
        }
    };
    match kv.delete_expect_revision(held_key(name), Some(revision)).await {
        Ok(()) => {}
        Err(e) if e.kind() == kv::UpdateErrorKind::WrongLastRevision => return,
        Err(e) => {
            tracing::warn!(human = %name, error = %e, "claiming the notices held back");
            return;
        }
    }
    if held.total() == 0 {
        return;
    }
    let pending = match human.approvals {
        None => Vec::new(),
        Some(_) => match tools::pending(&bus.js, &bus.stream, now).await {
            Ok(pending) => pending.into_iter().filter(|r| human.wants_request_of(&r.ant)).collect(),
            Err(e) => {
                tracing::warn!(human = %name, error = format!("{e:#}"), "reading the tool requests still pending");
                Vec::new()
            }
        },
    };
    let notice = digest_notice(human, held, &pending, now);
    tracing::info!(human = %name, title = notice.title, "sending the digest of the notices held back");
    send(human, &notice, |_| true).await;
}

/// Check every [`DIGEST_EVERY`] whether the digest of what was held back from `human` is due, and send it if so.
async fn digests(human: &Human, bus: &HumanBus) -> std::convert::Infallible {
    if bus.held.is_some() {
        loop {
            tokio::time::sleep(DIGEST_EVERY).await;
            let prefs = holds(human, bus).await;
            digest(human, bus, &prefs, Utc::now()).await;
        }
    }
    std::future::pending().await
}

/// Push `human` each of its reminders once due, checked every [`REMIND_EVERY`] through `bus`, made for JetStream domain
/// `js_domain`: the process whose
/// compare-and-set marks it pushed sends it, so one of those notifying the human does, at most once. Neither a snooze
/// nor quiet hours hold one back, as the human set it for then.
async fn remind(human: &Human, bus: &HumanBus, js_domain: Option<&JsDomain>) -> std::convert::Infallible {
    let meta = crate::threads::thread_meta::Meta::open(&bus.js, js_domain, &human.name)
        .await
        .inspect_err(|e| tracing::warn!(human = %human.name, "no reminders pushed: {e:#}"));
    if let Ok(meta) = meta {
        loop {
            tokio::time::sleep(REMIND_EVERY).await;
            let now = Utc::now();
            let due = std::sync::Mutex::new(Vec::new());
            let pushed = meta.change_reminders(|reminders| {
                let mut due = due.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                *due = reminders.push(now);
                !due.is_empty()
            });
            if let Err(e) = pushed.await {
                tracing::warn!(human = %human.name, error = format!("{e:#}"), "marking reminders pushed");
                continue;
            }
            for (thread, reminder) in due.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner) {
                tracing::info!(human = %human.name, %thread, "pushing a reminder");
                send(human, &reminder_notice(&human.name, &thread, &reminder), |_| true).await;
            }
        }
    }
    std::future::pending().await
}

/// The holds `human` set, read anew for each notice: none when they cannot be read, so a failure sends a notice too
/// many, never one too few.
async fn holds(human: &Human, bus: &HumanBus) -> NotifyPrefs {
    match bus.prefs.lookup(human.name.as_str()).await {
        Ok(Lookup::Value(value)) => serde_json::from_slice(&value).unwrap_or_else(|e| {
            tracing::warn!(human = %human.name, error = %e, "malformed snooze and muted threads: ignored");
            NotifyPrefs::default()
        }),
        Ok(Lookup::NoValue | Lookup::NoBucket) => NotifyPrefs::default(),
        Err(e) => {
            tracing::warn!(human = %human.name, error = format!("{e:#}"), "reading the snooze and muted threads");
            NotifyPrefs::default()
        }
    }
}

/// Whether the message of id `id`, a UUIDv7 stamped when sent, is younger than [`ESCALATION_MAX_AGE`].
fn is_recent(id: &Uuid, now: DateTime<Utc>) -> bool {
    let sent = id.get_timestamp().and_then(|t| {
        let (secs, nanos) = t.to_unix();
        DateTime::from_timestamp(i64::try_from(secs).ok()?, nanos)
    });
    sent.is_none_or(|sent| now - sent < ESCALATION_MAX_AGE)
}

#[cfg(test)]
mod tests;
