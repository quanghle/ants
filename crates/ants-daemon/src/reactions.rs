//! Reactions to messages, from ants and humans: `eyes`, `check` and the like, kept in [`REACTIONS_BUCKET`] apart from
//! the messages, each under its reactor's key ([`reaction_key`]), so credentials let each user add and remove only its
//! own. A reaction is no message: it reaches no inbox, wakes no ant, marks nothing read and ends no wait for a reply.
//! Humans read them all ([`load`], [`watch`]) and add and remove their own ([`add`], [`remove`]). Ants read those to the
//! messages they read, and add and remove their own, by asking ([`ask`], [`request`]): `ants signer`, or for a list a
//! human's `ants webhook serve`, answers ([`serve`]) once it found the ant reads the message ([`readable`]); a daemon
//! on a server without credentials answers its ants itself ([`answer`]).

use std::collections::{BTreeMap, HashMap};
use std::ops::ControlFlow;
use std::time::{Duration, SystemTime};

pub use ants_proto::Reacted;
use ants_proto::{
    Acting, JsDomain, REACTIONS_BUCKET, REACTIONS_REPEAT, REACTIONS_WAIT, Reaction, ReactionChange, ReactionEntry,
    ReactionsOutcome, ReactionsReply, ReactionsRequest, ReactionsResponder, UserId, Uuid, parse_reaction_key,
    reaction_key, reactions_to, subject,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::stream::{ConsumerError, ConsumerErrorKind};
use async_nats::jetstream::{self, ErrorCode, consumer};
use futures::{Stream, StreamExt as _};
use serde::{Deserialize, Serialize};

use crate::bus::DUPLICATE_WINDOW;
use crate::direct_kv::{DirectKv, Lookup};
use crate::reading::{self, Reading};

/// How long a read of the reactions waits for each one the bucket said it has.
const WAIT: Duration = Duration::from_secs(5);
/// Most messages whose reactions one read filters on the server; for more, it reads every reaction.
const MAX_FILTERS: usize = 64;
/// Most reactions one read takes.
const MAX_REACTIONS: u64 = 100_000;
/// How long before the time in its id a message may be stored, its sender's clock running ahead.
const CLOCK_SKEW: Duration = Duration::from_mins(5);
/// Queue group of the processes answering reactions requests: one of them takes each request.
const QUEUE: &str = "reactions";
/// Reactions requests one process answers at once.
const CONCURRENT: usize = 4;
/// The reactions shown as suggestions, such as in completions; any other token is taken too.
pub const SUGGESTED: [&str; 6] = ["check", "eyes", "heart", "rocket", "thumbsup", "x"];
const MISSING: &str = "there is no bucket ants_reactions: `ants init` as a human creates it";

/// The reactions bucket as `by`'s credentials reach it: its own keys only.
async fn own(js: &jetstream::Context, js_domain: Option<&JsDomain>) -> Result<DirectKv> {
    DirectKv::new(js, js_domain, REACTIONS_BUCKET).await
}

/// React `reaction` to message `message` as `by`. Whether the reaction is new: reacting the same again changes nothing.
pub async fn add(
    js: &jetstream::Context,
    js_domain: Option<&JsDomain>,
    by: &UserId,
    message: &Uuid,
    reaction: &Reaction,
) -> Result<bool> {
    add_for(js, js_domain, by, message, reaction, None).await
}

/// [`add`], made for `on_behalf_of` when `by` is its assistant ([`ReactionEntry::on_behalf_of`]).
pub async fn add_for(
    js: &jetstream::Context,
    js_domain: Option<&JsDomain>,
    by: &UserId,
    message: &Uuid,
    reaction: &Reaction,
    on_behalf_of: Option<UserId>,
) -> Result<bool> {
    let kv = own(js, js_domain).await?;
    let key = reaction_key(by, message, reaction);
    match kv.lookup(&key).await.context("reading the reaction")? {
        Lookup::Value(_) => Ok(false),
        Lookup::NoBucket => bail!(MISSING),
        Lookup::NoValue => {
            let entry = ReactionEntry::new(by.clone(), *message, reaction.clone()).on_behalf(on_behalf_of);
            kv.put(&key, serde_json::to_vec(&entry)?).await.context("storing the reaction")?;
            Ok(true)
        }
    }
}

/// Remove `by`'s reaction `reaction` to message `message`. Whether there was one: removing it again changes nothing.
pub async fn remove(
    js: &jetstream::Context,
    js_domain: Option<&JsDomain>,
    by: &UserId,
    message: &Uuid,
    reaction: &Reaction,
) -> Result<bool> {
    let kv = own(js, js_domain).await?;
    let key = reaction_key(by, message, reaction);
    match kv.lookup(&key).await.context("reading the reaction")? {
        Lookup::NoValue => Ok(false),
        Lookup::NoBucket => bail!(MISSING),
        Lookup::Value(_) => {
            kv.delete(&key).await.context("removing the reaction")?;
            Ok(true)
        }
    }
}

/// The reactions to each message that has any, each in order of name.
pub type Reactions = HashMap<Uuid, Vec<Reacted>>;

/// The reactions to `messages`, as the bucket keeps them: none while there is no bucket. Only a human's credentials
/// read others' reactions.
pub async fn load(js: &jetstream::Context, messages: &[Uuid]) -> Result<Reactions> {
    if messages.is_empty() {
        return Ok(Reactions::new());
    }
    let stream = js.get_stream_no_info(format!("KV_{REACTIONS_BUCKET}")).await.context("naming the reactions")?;
    let prefix = format!("$KV.{REACTIONS_BUCKET}.");
    let filter_subjects = if messages.len() <= MAX_FILTERS {
        messages.iter().map(|m| format!("{prefix}{}", reactions_to(m))).collect()
    } else {
        vec![format!("{prefix}>")]
    };
    let consumer = match stream
        .create_consumer(consumer::push::OrderedConfig {
            deliver_subject: js.client().new_inbox(),
            filter_subjects,
            headers_only: true,
            deliver_policy: consumer::DeliverPolicy::LastPerSubject,
            replay_policy: consumer::ReplayPolicy::Instant,
            ..Default::default()
        })
        .await
    {
        Ok(consumer) => consumer,
        Err(e) if not_found(&e) => {
            tracing::debug!(error = %e, "reading the reactions");
            return Ok(Reactions::new());
        }
        Err(e) => return Err(e).context("reading the reactions"),
    };
    let pending = consumer.cached_info().num_pending.min(MAX_REACTIONS);
    let mut stream = consumer.messages().await.context("reading the reactions")?;
    let wanted: std::collections::HashSet<&Uuid> = messages.iter().collect();
    let mut found: HashMap<Uuid, BTreeMap<Reaction, Vec<UserId>>> = HashMap::new();
    for _ in 0..pending {
        let msg = tokio::time::timeout(WAIT, stream.next())
            .await
            .context("waiting for the reactions")?
            .context("the reactions ended early")?
            .map_err(|e| anyhow::anyhow!(e))?;
        let Some(change) = Change::of(&msg) else { continue };
        if change.removed || !wanted.contains(&change.message) {
            continue;
        }
        found.entry(change.message).or_default().entry(change.reaction).or_default().push(change.by);
    }
    Ok(found
        .into_iter()
        .map(|(message, reactions)| {
            let grouped = reactions
                .into_iter()
                .map(|(reaction, mut by)| {
                    by.sort();
                    by.dedup();
                    Reacted::new(reaction, by)
                })
                .collect();
            (message, grouped)
        })
        .collect())
}

fn not_found(e: &ConsumerError) -> bool {
    matches!(e.kind(), ConsumerErrorKind::JetStream(e) if e.error_code() == ErrorCode::STREAM_NOT_FOUND)
}

/// A reaction added or removed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Change {
    pub message: Uuid,
    pub by: UserId,
    pub reaction: Reaction,
    pub removed: bool,
    /// What a reader made of the [`ReactionEntry::on_behalf_of`] of a reaction added, which the change itself does
    /// not carry: the reactor's own until a reader checks it.
    #[serde(skip_serializing_if = "Acting::is_own")]
    pub acting: Acting,
}

impl Change {
    /// The change a message of the bucket's stream makes, by its key and `KV-Operation` header; None for a key that
    /// is no reaction.
    fn of(msg: &async_nats::Message) -> Option<Self> {
        let key = msg.subject.strip_prefix(&format!("$KV.{REACTIONS_BUCKET}."))?;
        let (by, message, reaction) = parse_reaction_key(key)?;
        let removed = msg.headers.as_ref().and_then(|h| h.get("KV-Operation")).is_some_and(|op| op.as_str() != "PUT");
        Some(Self { message, by, reaction, removed, acting: Acting::Own })
    }
}

/// Every reaction added or removed from now on, while there is a bucket.
pub async fn watch(js: &jetstream::Context) -> Result<impl Stream<Item = Change> + use<>> {
    let stream = js.get_stream_no_info(format!("KV_{REACTIONS_BUCKET}")).await.context("naming the reactions")?;
    let consumer = stream
        .create_consumer(consumer::push::OrderedConfig {
            deliver_subject: js.client().new_inbox(),
            filter_subject: format!("$KV.{REACTIONS_BUCKET}.>"),
            headers_only: true,
            deliver_policy: consumer::DeliverPolicy::New,
            replay_policy: consumer::ReplayPolicy::Instant,
            ..Default::default()
        })
        .await
        .context("watching the reactions")?;
    let messages = consumer.messages().await.context("watching the reactions")?;
    Ok(messages.filter_map(async |msg| match msg {
        Ok(msg) => Change::of(&msg),
        Err(e) => {
            tracing::debug!(error = %e, "watching the reactions");
            None
        }
    }))
}

/// Whether the ant `ant` reads message `message`, as far as the stream keeps it: the first message, notes post or
/// channel post stored with that id is on a subject the ant's credentials read ([`subject::readable_by`]). Senders
/// choose ids, so a later copy of a message, which an ant can store on a subject it reads once it learned the id, is
/// no proof; nobody learns an id before its message is stored. The read takes a human's credentials, and looks only
/// at what was stored from [`CLOCK_SKEW`] before the time in the id (a UUIDv7's, as every ants id is) to
/// [`DUPLICATE_WINDOW`] after it, the latest a message's copy from a leaf node's outbox may land: a copy stored once
/// its message has gone is no proof either.
pub async fn readable(js: &jetstream::Context, ant: &UserId, message: Uuid) -> Result<bool> {
    let Some((secs, nanos)) = message.get_timestamp().filter(|_| message.get_version_num() == 7).map(|t| t.to_unix())
    else {
        return Ok(false);
    };
    let made = SystemTime::UNIX_EPOCH + Duration::new(secs, nanos);
    let from = made.checked_sub(CLOCK_SKEW).unwrap_or(SystemTime::UNIX_EPOCH);
    let until = made + DUPLICATE_WINDOW;
    if from > SystemTime::now() {
        return Ok(false);
    }
    #[derive(Deserialize)]
    struct Id {
        id: Uuid,
    }
    let mut first = None;
    let start = consumer::DeliverPolicy::ByStartTime { start_time: from.into() };
    reading::read_back(js, "", Reading::Gets, subject::ALL, start, |_, stored, subject, payload| {
        if stored > until {
            return ControlFlow::Break(());
        }
        if serde_json::from_slice::<Id>(payload).is_ok_and(|p| p.id == message) {
            first = Some(subject::readable_by(ant, subject));
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    })
    .await
    .context("finding the message")?;
    Ok(first == Some(true))
}

/// The answer to `request`, if the ant reads its message ([`readable`]): the reactions to it, or with a change, that
/// change made to the ant's own reactions ([`add`], [`remove`] as the ant), else [`ReactionsOutcome::Unknown`] and no
/// change. Takes a human's credentials to list, and those of [`Permissions::for_reactions_responder`] to change.
///
/// [`Permissions::for_reactions_responder`]: ants_proto::Permissions::for_reactions_responder
pub async fn answer(
    js: &jetstream::Context,
    js_domain: Option<&JsDomain>,
    request: &ReactionsRequest,
) -> Result<ReactionsOutcome> {
    if !readable(js, &request.ant, request.message).await? {
        return Ok(ReactionsOutcome::Unknown);
    }
    let (ant, message) = (&request.ant, &request.message);
    Ok(match &request.change {
        Some(ReactionChange { reaction, remove: true, .. }) => {
            ReactionsOutcome::Applied { changed: remove(js, js_domain, ant, message, reaction).await? }
        }
        Some(ReactionChange { reaction, remove: false, on_behalf_of }) => {
            let changed = add_for(js, js_domain, ant, message, reaction, on_behalf_of.clone()).await?;
            ReactionsOutcome::Applied { changed }
        }
        None => {
            let reactions = load(js, &[request.message]).await?.remove(&request.message).unwrap_or_default();
            ReactionsOutcome::Listed { reactions }
        }
    })
}

/// Which reactions requests a process answers ([`serve`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answering {
    /// Only those for the reactions to a message, as a human's connection may.
    Lists,
    /// Those to change an ant's own reactions as well, as `ants signer`'s connection may.
    ListsAndChanges,
}

impl Answering {
    /// The kind of process answering so: `ants webhook serve` lists, `ants signer` changes as well.
    pub fn responder(self) -> ReactionsResponder {
        match self {
            Self::Lists => ReactionsResponder::Webhook,
            Self::ListsAndChanges => ReactionsResponder::Signer,
        }
    }
}

/// Answer the reactions requests of ants on `client`, in the queue group of every process that does, until the
/// subscription ends. Each answer goes to the asking ant's [`subject::reactions_reply`] only.
pub async fn serve(
    client: async_nats::Client,
    js: jetstream::Context,
    js_domain: Option<JsDomain>,
    answering: Answering,
) -> Result<()> {
    let subscribe = async |subject: &'static str| {
        client.queue_subscribe(subject, QUEUE.to_owned()).await.context("subscribing to reactions requests")
    };
    let lists = subscribe(subject::ALL_REACTIONS_REQUESTS).await?;
    let requests = match answering {
        Answering::Lists => lists.boxed(),
        Answering::ListsAndChanges => {
            futures::stream::select(lists, subscribe(subject::ALL_REACT_REQUESTS).await?).boxed()
        }
    };
    let responder = answering.responder();
    requests.for_each_concurrent(CONCURRENT, |msg| respond(&client, &js, js_domain.as_ref(), responder, msg)).await;
    bail!("the subscription to reactions requests ended")
}

async fn respond(
    client: &async_nats::Client,
    js: &jetstream::Context,
    js_domain: Option<&JsDomain>,
    responder: ReactionsResponder,
    msg: async_nats::Message,
) {
    let request = serde_json::from_slice::<ReactionsRequest>(&msg.payload)
        .map_err(|e| e.to_string())
        .and_then(|r| r.check_subject(&msg.subject).map(|()| r).map_err(|e| e.to_string()));
    let request = match request {
        Ok(request) => request,
        Err(error) => {
            tracing::warn!(subject = %msg.subject, error, "dropping a reactions request");
            return;
        }
    };
    let outcome = match answer(js, js_domain, &request).await {
        Ok(outcome) => outcome,
        Err(e) => {
            // No answer: the ant repeats the request, and a later attempt may get through.
            tracing::warn!(ant = %request.ant, message = %request.message, error = format!("{e:#}"), "answering reactions");
            return;
        }
    };
    if outcome == ReactionsOutcome::Unknown {
        tracing::debug!(ant = %request.ant, message = %request.message, "reactions asked of a message the ant does not read");
    }
    let reply =
        ReactionsReply { request_id: request.id, ant: request.ant, message: request.message, outcome, responder };
    let payload = serde_json::to_vec(&reply).expect("an answer");
    if let Err(e) = client.publish(reply.subject(), payload.into()).await {
        tracing::warn!(error = %e, "answering a reactions request");
    }
}

/// Ask, as the ant `ant` on `client`, for the reactions to message `message` ([`ReactionsRequest::new`]).
pub async fn ask(client: &async_nats::Client, ant: &UserId, message: Uuid) -> Result<Option<ReactionsOutcome>> {
    request(client, &ReactionsRequest::new(ant.clone(), message)).await
}

/// Make `request` on `client`, as its ant, repeating it every [`REACTIONS_REPEAT`]: the answer, None when nothing
/// answered within [`REACTIONS_WAIT`]. An answer to a repeated change may say it changed nothing, an earlier attempt
/// having made it.
pub async fn request(client: &async_nats::Client, request: &ReactionsRequest) -> Result<Option<ReactionsOutcome>> {
    Ok(request_within(client, request, REACTIONS_WAIT).await?.map(|reply| reply.outcome))
}

/// Make `request` as [`request`] does, waiting at most `wait`: the whole answer, saying who answered.
pub async fn request_within(
    client: &async_nats::Client,
    request: &ReactionsRequest,
    wait: Duration,
) -> Result<Option<ReactionsReply>> {
    let mut replies = client.subscribe(request.reply_subject()).await.context("subscribing to the answers")?;
    let payload = serde_json::to_vec(request)?;
    let deadline = tokio::time::Instant::now() + wait;
    while tokio::time::Instant::now() < deadline {
        client.publish(request.subject(), payload.clone().into()).await.context("asking about the reactions")?;
        let next = (tokio::time::Instant::now() + REACTIONS_REPEAT).min(deadline);
        while let Ok(msg) = tokio::time::timeout_at(next, replies.next()).await {
            let msg = msg.context("the answers ended")?;
            match serde_json::from_slice::<ReactionsReply>(&msg.payload) {
                Ok(reply) if reply.request_id == request.id && reply.check_subject(&msg.subject).is_ok() => {
                    return Ok(Some(reply));
                }
                Ok(_) => tracing::debug!("ignoring an answer to another reactions request"),
                Err(e) => tracing::warn!(error = %e, "ignoring a malformed reactions answer"),
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use ants_test_support::NatsServer;

    use super::*;
    use crate::{bus, hub};

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    fn r(s: &str) -> Reaction {
        s.parse().unwrap()
    }

    fn reacted(reaction: &str, by: &[&str]) -> Reacted {
        Reacted::new(r(reaction), by.iter().map(|b| p(b)).collect())
    }

    #[tokio::test]
    async fn reactions_persist_group_by_message_and_add_and_remove_idempotently() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
        let (one, two) = (Uuid::now_v7(), Uuid::now_v7());
        assert!(load(&js, &[one]).await.unwrap().is_empty(), "no bucket yet");
        let missing = add(&js, None, &p("alice"), &one, &r("eyes")).await.unwrap_err();
        assert!(missing.to_string().contains("ants init"), "{missing:#}");
        hub::init(&js, |_| true).await.unwrap();

        // Participants react at once, one of them several ways.
        let adds = [("alice", one, "eyes"), ("dev-a", one, "eyes"), ("dev-a", one, "check"), ("bob", two, "x")];
        let added = futures::future::join_all(
            adds.iter().map(async |(by, message, reaction)| add(&js, None, &p(by), message, &r(reaction)).await),
        )
        .await;
        assert!(added.into_iter().all(|a| a.unwrap()), "each new");
        assert!(!add(&js, None, &p("alice"), &one, &r("eyes")).await.unwrap(), "the same again is no new one");

        // Across a reconnect.
        drop(js);
        let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
        let all = load(&js, &[one, two, Uuid::now_v7()]).await.unwrap();
        assert_eq!(all[&one], [reacted("check", &["dev-a"]), reacted("eyes", &["alice", "dev-a"])]);
        assert_eq!(all[&two], [reacted("x", &["bob"])]);
        assert_eq!(all.len(), 2, "messages with none are left out");

        let mut live = Box::pin(watch(&js).await.unwrap());
        assert!(remove(&js, None, &p("dev-a"), &one, &r("eyes")).await.unwrap());
        assert!(!remove(&js, None, &p("dev-a"), &one, &r("eyes")).await.unwrap(), "removing again is a no-op");
        assert!(!remove(&js, None, &p("dev-a"), &one, &r("heart")).await.unwrap(), "as is removing none");
        assert!(add(&js, None, &p("dev-a"), &one, &r("eyes")).await.unwrap(), "added back");
        let changes: Vec<_> = tokio::time::timeout(WAIT, (&mut live).take(2).collect::<Vec<_>>()).await.unwrap();
        let change =
            |removed| Change { message: one, by: p("dev-a"), reaction: r("eyes"), removed, acting: Acting::Own };
        assert_eq!(changes, [change(true), change(false)]);
        assert_eq!(load(&js, &[one]).await.unwrap()[&one].len(), 2);

        // Read through the whole bucket for many messages.
        let many: Vec<_> = std::iter::repeat_with(Uuid::now_v7).take(MAX_FILTERS).chain([two]).collect();
        assert_eq!(load(&js, &many).await.unwrap().into_keys().collect::<Vec<_>>(), [two]);
    }

    /// No one wipes the bucket by purging or rolling it up.
    #[tokio::test]
    async fn the_bucket_denies_purges_and_rollups() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
        hub::init(&js, |_| true).await.unwrap();
        let message = Uuid::now_v7();
        add(&js, None, &p("alice"), &message, &r("eyes")).await.unwrap();
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("KV-Operation", "PURGE");
        headers.insert("Nats-Rollup", "all");
        let subject = format!("$KV.{REACTIONS_BUCKET}.{}", reaction_key(&p("dev-a"), &message, &r("x")));
        let rollup = js.publish_with_headers(subject, headers, "".into()).await.unwrap().await;
        assert!(rollup.is_err(), "a rollup is refused");
        assert!(js.get_stream(format!("KV_{REACTIONS_BUCKET}")).await.unwrap().purge().await.is_err());
        assert_eq!(load(&js, &[message]).await.unwrap()[&message], [reacted("eyes", &["alice"])]);
    }

    /// An ant learns and changes the reactions to the messages it reads, and nothing of the others, not even by storing a copy of
    /// one under its id where it reads.
    #[tokio::test]
    async fn ants_read_and_change_reactions_to_the_messages_they_read_only() {
        use ants_proto::{ChannelPost, Envelope, NotesPost};
        let Some(server) = NatsServer::start() else { return };
        let (client, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
        hub::init(&js, |_| true).await.unwrap();
        let (dev_a, dev_b, dev_c) = (p("dev-a"), p("dev-b"), p("dev-c"));
        let to_a = Envelope::new(UserId::human(), dev_a.clone(), "for dev-a");
        let between = Envelope::new(dev_b.clone(), dev_c.clone(), "between dev-b and dev-c");
        let notes = NotesPost::new(between.thread_id.clone(), dev_c.clone(), "notes anyone reads");
        let post = ChannelPost::new("ops".parse().unwrap(), dev_b.clone(), "a channel anyone reads");
        for envelope in [&to_a, &between] {
            bus::publish(&js, envelope).await.unwrap();
        }
        bus::store(&js, notes.subject(), &notes, &notes.id.to_string()).await.unwrap();
        bus::store(&js, post.subject(), &post, &post.id.to_string()).await.unwrap();
        // dev-a learned the id, and stores a copy of the message in its own inbox, under another msg id.
        let copy = Envelope { id: between.id, ..Envelope::new(dev_c.clone(), dev_a.clone(), "copy") };
        bus::store(&js, copy.subject(), &copy, "copy").await.unwrap();
        // A message stored long after the time its id says.
        let (secs, _) = between.id.get_timestamp().unwrap().to_unix();
        let stale = uuid::Uuid::new_v7(uuid::Timestamp::from_unix(uuid::NoContext, secs - 2 * 24 * 3600, 0));
        let late = Envelope { id: stale, ..Envelope::new(UserId::human(), dev_a.clone(), "late") };
        bus::publish(&js, &late).await.unwrap();
        for message in [to_a.id, between.id, notes.id, post.id, stale] {
            add(&js, None, &p("alice"), &message, &r("eyes")).await.unwrap();
        }

        let outcome = async |ant: &UserId, message| {
            answer(&js, None, &ReactionsRequest::new(ant.clone(), message)).await.unwrap()
        };
        let listed = ReactionsOutcome::Listed { reactions: vec![reacted("eyes", &["alice"])] };
        for message in [to_a.id, notes.id, post.id] {
            assert_eq!(outcome(&dev_a, message).await, listed, "{message}");
        }
        assert_eq!(outcome(&dev_a, between.id).await, ReactionsOutcome::Unknown, "neither its sender nor recipient");
        assert_eq!(outcome(&dev_b, between.id).await, listed, "its sender reads it");
        assert_eq!(outcome(&dev_c, between.id).await, listed, "as does its recipient");
        assert_eq!(outcome(&dev_b, to_a.id).await, ReactionsOutcome::Unknown);
        assert_eq!(outcome(&dev_a, Uuid::now_v7()).await, ReactionsOutcome::Unknown, "no such message");
        assert_eq!(outcome(&dev_a, uuid::Uuid::new_v4()).await, ReactionsOutcome::Unknown, "no ants id");
        assert_eq!(outcome(&dev_a, stale).await, ReactionsOutcome::Unknown, "stored too late to tell");
        let quiet = Envelope::new(UserId::human(), dev_a.clone(), "no reactions");
        bus::publish(&js, &quiet).await.unwrap();
        assert_eq!(outcome(&dev_a, quiet.id).await, ReactionsOutcome::Listed { reactions: Vec::new() });

        // An ant changes its own reactions to the messages it reads only, as itself.
        let change = |reaction: &str, remove| ReactionChange::new(r(reaction), remove);
        let changing = async |ant: &UserId, message, reaction, remove| {
            answer(&js, None, &ReactionsRequest::change(ant.clone(), message, change(reaction, remove))).await.unwrap()
        };
        let applied = |changed| ReactionsOutcome::Applied { changed };
        assert_eq!(changing(&dev_a, to_a.id, "check", false).await, applied(true));
        assert_eq!(changing(&dev_a, to_a.id, "check", false).await, applied(false), "already");
        assert_eq!(changing(&dev_a, between.id, "x", false).await, ReactionsOutcome::Unknown);
        assert_eq!(changing(&dev_a, notes.id, "eyes", true).await, applied(false), "none to remove");
        assert_eq!(load(&js, &[to_a.id, between.id]).await.unwrap()[&to_a.id][0], reacted("check", &["dev-a"]));
        assert_eq!(load(&js, &[between.id]).await.unwrap()[&between.id], [reacted("eyes", &["alice"])], "unchanged");
        assert_eq!(changing(&dev_a, to_a.id, "check", true).await, applied(true));

        // Over NATS: the ant asks on its own subjects and hears the answers on its own reply subject; a process
        // answering lists only leaves changes to another.
        let lists = tokio::spawn(serve(client.clone(), js.clone(), None, Answering::Lists));
        let (ant, _) = bus::connect(&server.url, None, None, &dev_a, "test").await.unwrap();
        assert_eq!(ask(&ant, &dev_a, to_a.id).await.unwrap(), Some(listed.clone()));
        assert_eq!(ask(&ant, &dev_a, between.id).await.unwrap(), Some(ReactionsOutcome::Unknown));
        let mut replies = ant.subscribe(subject::reactions_reply(&dev_a)).await.unwrap();
        let react = ReactionsRequest::change(dev_a.clone(), to_a.id, change("rocket", false));
        ant.publish(react.subject(), serde_json::to_vec(&react).unwrap().into()).await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(500), replies.next()).await.is_err(), "no change answered");
        lists.abort();
        let serving = tokio::spawn(serve(client, js.clone(), None, Answering::ListsAndChanges));
        assert_eq!(request(&ant, &react).await.unwrap(), Some(applied(true)));
        assert_eq!(
            ask(&ant, &dev_a, to_a.id).await.unwrap(),
            Some(ReactionsOutcome::Listed {
                reactions: vec![reacted("eyes", &["alice"]), reacted("rocket", &["dev-a"])]
            })
        );
        // A request naming another ant than its subject does goes unanswered, and changes nothing.
        let mut replies = ant.subscribe(subject::reactions_reply(&dev_b)).await.unwrap();
        for forged in [
            ReactionsRequest::new(dev_b.clone(), between.id),
            ReactionsRequest::change(dev_b.clone(), between.id, change("x", false)),
        ] {
            let payload = serde_json::to_vec(&forged).unwrap();
            ant.publish(subject::reactions(&dev_a), payload.clone().into()).await.unwrap();
            ant.publish(subject::react(&dev_a), payload.into()).await.unwrap();
        }
        assert!(tokio::time::timeout(Duration::from_millis(500), replies.next()).await.is_err());
        assert_eq!(load(&js, &[between.id]).await.unwrap()[&between.id], [reacted("eyes", &["alice"])]);
        serving.abort();
    }
}
