//! Reactions to messages, from ants and humans: `eyes`, `check` and the like, kept in [`REACTIONS_BUCKET`] apart from
//! the messages, each under its reactor's key ([`reaction_key`]), so credentials let each user add and remove only its
//! own. A reaction is no message: it reaches no inbox, wakes no ant, marks nothing read and ends no wait for a reply.
//! Humans read them all ([`load`], [`watch`]); ants only add and remove their own.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use ants_proto::{
    JsDomain, REACTIONS_BUCKET, Reaction, ReactionEntry, UserId, Uuid, parse_reaction_key, reaction_key, reactions_to,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::stream::{ConsumerError, ConsumerErrorKind};
use async_nats::jetstream::{self, ErrorCode, consumer};
use futures::{Stream, StreamExt as _};
use serde::Serialize;

use crate::direct_kv::{DirectKv, Lookup};

/// How long a read of the reactions waits for each one the bucket said it has.
const WAIT: Duration = Duration::from_secs(5);
/// Most messages whose reactions one read filters on the server; for more, it reads every reaction.
const MAX_FILTERS: usize = 64;
/// Most reactions one read takes.
const MAX_REACTIONS: u64 = 100_000;
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
    let kv = own(js, js_domain).await?;
    let key = reaction_key(by, message, reaction);
    match kv.lookup(&key).await.context("reading the reaction")? {
        Lookup::Value(_) => Ok(false),
        Lookup::NoBucket => bail!(MISSING),
        Lookup::NoValue => {
            let entry = ReactionEntry::new(by.clone(), *message, reaction.clone());
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

/// One reaction to a message and who reacted so, in order of name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Reacted {
    pub reaction: Reaction,
    pub by: Vec<UserId>,
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
                    Reacted { reaction, by }
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
}

impl Change {
    /// The change a message of the bucket's stream makes, by its key and `KV-Operation` header; None for a key that
    /// is no reaction.
    fn of(msg: &async_nats::Message) -> Option<Self> {
        let key = msg.subject.strip_prefix(&format!("$KV.{REACTIONS_BUCKET}."))?;
        let (by, message, reaction) = parse_reaction_key(key)?;
        let removed = msg.headers.as_ref().and_then(|h| h.get("KV-Operation")).is_some_and(|op| op.as_str() != "PUT");
        Some(Self { message, by, reaction, removed })
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
        Reacted { reaction: r(reaction), by: by.iter().map(|b| p(b)).collect() }
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
        let change = |removed| Change { message: one, by: p("dev-a"), reaction: r("eyes"), removed };
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
}
