//! Telling the sender of messages that keep waiting for an ant no daemon serves.
//!
//! `ants ask` and the MCP tools warn at send time of an ant the roster lists no live daemon for; when the ant's daemon
//! goes away later, or never comes, the message waits in the ant's inbox consumer with no word. A [`Watch`] reads back
//! the messages one user sent ([`subject::inbox_from`]), and finds those still waiting ([`MessageMark`]: none or
//! started) [`AFTER`] they were sent, for an ant it has seen absent from the roster for [`AFTER`] too: the watcher
//! of an ant is its daemon, that of a human `ants webhook serve` or `ants inbox --follow`. Whoever tells of a message
//! first creates its marker ([`unserved_key`]), so it is told once across watchers and restarts.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write as _;
use std::ops::ControlFlow;
use std::time::{Duration, SystemTime};

use ants_proto::{
    ATTEMPTS_BUCKET, Envelope, JsDomain, MessageMark, Role, RosterEntry, ThreadId, UserId, Uuid, message_mark_key,
    sent_reader, subject, unserved_key,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, consumer::DeliverPolicy};
use chrono::{DateTime, TimeDelta, Utc};
use tokio::time::Instant;

use crate::direct_kv::DirectKv;
use crate::mcp::Roster;
use crate::reading::{self, Reading};

/// How long a message waits, and its ant stays absent from the roster, before the sender is told.
pub const AFTER: Duration = Duration::from_mins(15);
/// How often a watcher looks.
pub const CHECK_EVERY: Duration = Duration::from_mins(1);
/// How far back a watcher reads the messages sent before it started.
const LOOKBACK: Duration = Duration::from_hours(24);
/// The longest a watcher waits to look again after failing to.
const MAX_BACKOFF: Duration = Duration::from_hours(1);
/// The most messages a watcher keeps track of; the oldest go first.
const MAX_TRACKED: usize = 10_000;
/// The longest excerpt of a message a notice quotes.
const EXCERPT: usize = 80;
/// The most peers a notice suggests.
const MAX_PEERS: usize = 5;

/// The subject a human's credentials list to tell of its waiting messages, as they list it: the put of its markers.
pub fn marker_subject(sender: &UserId) -> String {
    format!("$KV.{ATTEMPTS_BUCKET}.{sender}.unserved.*")
}

/// A message the watched user sent, still to be accounted for.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Sent {
    id: Uuid,
    to: UserId,
    thread: ThreadId,
    /// The owner the message names, when an ant sent it: the human to tell.
    owner: Option<UserId>,
    sent_at: DateTime<Utc>,
    excerpt: String,
}

/// A message waiting for an ant no daemon serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Waiting {
    pub id: Uuid,
    pub thread: ThreadId,
    pub sent_at: DateTime<Utc>,
    pub excerpt: String,
    seq: u64,
}

/// An ant that offers some of the capabilities of the absent one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub ant: UserId,
    pub shared: Vec<String>,
}

/// Messages of the watched user that keep waiting for `ant`, which no daemon has served since `absent_since`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unserved {
    pub ant: UserId,
    /// The owner the messages name, when an ant sent them.
    pub owner: Option<UserId>,
    pub messages: Vec<Waiting>,
    pub absent_since: DateTime<Utc>,
    /// The capabilities `ant` advertised when last seen, if it was.
    pub capabilities: Option<Vec<String>>,
    /// Live ants advertising some of those capabilities, the most shared first.
    pub peers: Vec<Peer>,
}

impl Unserved {
    /// The notice of these messages of `sender` at `now`.
    pub fn text(&self, sender: &UserId, now: DateTime<Utc>) -> String {
        let ant = &self.ant;
        let (count, wait) = match self.messages.len() {
            1 => ("1 message".to_owned(), "waits"),
            n => (format!("{n} messages"), "wait"),
        };
        let mut text = format!(
            "[ants] {count} from {sender} {wait} for {ant}, which no daemon has served for at least {}:",
            span(now - self.absent_since)
        );
        for m in &self.messages {
            let _ = write!(
                text,
                "\n- message {} on thread {}, sent {} ago: {:?}",
                m.id,
                m.thread,
                span(now - m.sent_at),
                m.excerpt
            );
        }
        let _ = write!(
            text,
            "\nEach waits until a daemon serves {ant}; `ants withdraw {ant} MESSAGE` withdraws one before {ant} is \
             prompted with it."
        );
        match &self.capabilities {
            _ if !self.peers.is_empty() => {
                let peers: Vec<String> =
                    self.peers.iter().map(|p| format!("{} ({})", p.ant, p.shared.join(", "))).collect();
                let _ = write!(text, "\nLive ants with the same capabilities: {}.", peers.join(", "));
            }
            Some(capabilities) if !capabilities.is_empty() => {
                let _ =
                    write!(text, "\nNo live ant advertises the capabilities of {ant} ({}).", capabilities.join(", "));
            }
            _ => {}
        }
        text
    }
}

/// Minutes, hours or days, rounded down.
fn span(delta: TimeDelta) -> String {
    let minutes = delta.num_minutes().max(0);
    match minutes {
        ..120 => format!("{minutes}m"),
        120..2880 => format!("{}h", minutes / 60),
        _ => format!("{}d", minutes / 1440),
    }
}

/// Watches the messages one user sends for those that keep waiting for an ant no daemon serves.
pub struct Watch {
    sender: UserId,
    role: Role,
    js: jetstream::Context,
    attempts: DirectKv,
    roster: Roster,
    /// The stream sequence to read on from; none before anything was read.
    next: Option<u64>,
    sent: BTreeMap<u64, Sent>,
    /// When each ant messages wait for was first seen absent from the roster.
    absent_since: HashMap<UserId, DateTime<Utc>>,
    /// The capabilities each ant advertised when last seen live.
    capabilities: HashMap<UserId, Vec<String>>,
    check_at: Instant,
    backoff: Duration,
}

impl Watch {
    /// The watch of the messages `sender`, in `role`, sends, read as its credentials allow (an ant through its own
    /// [`sent_reader`], one read at a time; a human by direct gets), with `roster` read for the ants they wait for.
    pub async fn new(
        js: &jetstream::Context,
        js_domain: Option<&JsDomain>,
        sender: UserId,
        role: Role,
        roster: Roster,
    ) -> Result<Self> {
        let attempts = DirectKv::new(js, js_domain, ATTEMPTS_BUCKET).await?;
        Ok(Self::with(sender, role, js.clone(), attempts, roster))
    }

    fn with(sender: UserId, role: Role, js: jetstream::Context, attempts: DirectKv, roster: Roster) -> Self {
        Self {
            sender,
            role,
            js,
            attempts,
            roster,
            next: None,
            sent: BTreeMap::new(),
            absent_since: HashMap::new(),
            capabilities: HashMap::new(),
            check_at: Instant::now() + CHECK_EVERY,
            backoff: CHECK_EVERY,
        }
    }

    pub fn sender(&self) -> &UserId {
        &self.sender
    }

    /// Wait for the next look, then the messages found waiting, by ant and owner, of which none was told yet. A
    /// failure is logged, and the watcher looks again later, the later the more often it fails.
    pub async fn next(&mut self, is_human: impl Fn(&UserId) -> bool) -> Vec<Unserved> {
        tokio::time::sleep_until(self.check_at).await;
        match self.check(&is_human, Utc::now()).await {
            Ok(found) => {
                self.backoff = CHECK_EVERY;
                self.check_at = Instant::now() + CHECK_EVERY;
                found
            }
            Err(e) => {
                let error = format!("{e:#}");
                if self.backoff == CHECK_EVERY {
                    tracing::warn!(error, "looking for messages waiting for an ant no daemon serves");
                } else {
                    tracing::debug!(error, "looking for messages waiting for an ant no daemon serves");
                }
                self.check_at = Instant::now() + self.backoff;
                self.backoff = (self.backoff * 2).min(MAX_BACKOFF);
                Vec::new()
            }
        }
    }

    /// Claim the telling of `found`: the messages of it no other watcher told of first, None when none is left. Each
    /// message is forgotten once claimed, by this watcher or another; one whose claim failed is looked at again.
    pub async fn claim(&mut self, mut found: Unserved) -> Option<Unserved> {
        let marker = serde_json::json!({ "at": Utc::now() }).to_string();
        let mut won = Vec::new();
        for message in std::mem::take(&mut found.messages) {
            match self.attempts.create(unserved_key(&self.sender, &message.id), marker.clone()).await {
                Ok(first) => {
                    self.sent.remove(&message.seq);
                    if first {
                        won.push(message);
                    }
                }
                Err(e) => tracing::warn!(message = %message.id, error = format!("{e:#}"), "marking a message told of"),
            }
        }
        found.messages = won;
        if found.messages.is_empty() {
            return None;
        }
        tracing::info!(ant = %found.ant, messages = found.messages.len(), "telling of messages waiting for an absent ant");
        Some(found)
    }

    async fn check(&mut self, is_human: &impl Fn(&UserId) -> bool, now: DateTime<Utc>) -> Result<Vec<Unserved>> {
        self.read_sent(is_human, now).await?;
        let horizon = now - crate::bus::RETENTION;
        self.sent.retain(|_, sent| sent.sent_at > horizon);
        if self.sent.is_empty() {
            self.absent_since.clear();
            return Ok(Vec::new());
        }
        let entries = (self.roster)().await.context("reading the roster")?;
        self.observe(&entries, now);
        let after = TimeDelta::from_std(AFTER)?;
        let due: Vec<u64> =
            self.sent.iter().filter(|(_, sent)| now - sent.sent_at >= after).map(|(&seq, _)| seq).collect();
        let mut found: BTreeMap<(UserId, Option<UserId>), Vec<Waiting>> = BTreeMap::new();
        for seq in due {
            let sent = &self.sent[&seq];
            if self.waits(sent).await? {
                if !self.absent_since.get(&sent.to).is_some_and(|since| now - *since >= after) {
                    continue;
                }
                if self.attempts.get(unserved_key(&self.sender, &sent.id)).await?.is_none() {
                    let waiting = Waiting {
                        id: sent.id,
                        thread: sent.thread.clone(),
                        sent_at: sent.sent_at,
                        excerpt: sent.excerpt.clone(),
                        seq,
                    };
                    found.entry((sent.to.clone(), sent.owner.clone())).or_default().push(waiting);
                    continue;
                }
            }
            self.sent.remove(&seq);
        }
        Ok(found
            .into_iter()
            .map(|((ant, owner), messages)| self.unserved(ant, owner, messages, &entries, now))
            .collect())
    }

    /// Read on the messages the sender sent since the last read, keeping track of those to an ant.
    async fn read_sent(&mut self, is_human: &impl Fn(&UserId) -> bool, now: DateTime<Utc>) -> Result<()> {
        let start = match self.next {
            Some(start_sequence) => DeliverPolicy::ByStartSequence { start_sequence },
            None => DeliverPolicy::ByStartTime { start_time: (SystemTime::from(now) - LOOKBACK).into() },
        };
        let (sender, mut read, mut next) = (&self.sender, Vec::new(), self.next);
        let filter = subject::inbox_from(sender);
        let reader = sent_reader(sender);
        let reading = if self.role.is_human() { Reading::Gets } else { Reading::Consumer };
        let _one = if self.role.is_human() { None } else { Some(crate::history::SENT_READS.lock().await) };
        let role = self.role;
        reading::read_back(&self.js, &reader, reading, &filter, start, |seq, _, subject, payload| {
            next = Some(seq + 1);
            if let Some(sent) = sent(sender, role, subject, payload, is_human) {
                read.push((seq, sent));
            }
            ControlFlow::Continue(())
        })
        .await
        .context("reading back the messages sent")?;
        self.next = next;
        self.sent.extend(read);
        while self.sent.len() > MAX_TRACKED {
            self.sent.pop_first();
        }
        Ok(())
    }

    /// Note which ants messages wait for are absent from `entries`, the roster at `now`, and the capabilities of
    /// those live.
    fn observe(&mut self, entries: &[RosterEntry], now: DateTime<Utc>) {
        let live: HashSet<&UserId> =
            entries.iter().filter(|e| crate::roster::is_live(e, now)).map(|e| &e.ant).collect();
        for entry in entries.iter().filter(|e| live.contains(&e.ant)) {
            self.capabilities.insert(entry.ant.clone(), entry.capabilities.clone());
        }
        let awaited: HashSet<&UserId> = self.sent.values().map(|sent| &sent.to).collect();
        self.absent_since.retain(|ant, _| awaited.contains(ant) && !live.contains(ant));
        for ant in awaited.into_iter().filter(|ant| !live.contains(ant)) {
            self.absent_since.entry(ant.clone()).or_insert(now);
        }
    }

    /// Whether `sent` still waits: its ant neither finished it nor was it withdrawn.
    async fn waits(&self, sent: &Sent) -> Result<bool> {
        let Some(value) = self.attempts.get(message_mark_key(&sent.to, &sent.id)).await? else { return Ok(true) };
        Ok(match serde_json::from_slice::<MessageMark>(&value) {
            Ok(MessageMark::Started { .. }) | Err(_) => true,
            Ok(MessageMark::Finished { .. } | MessageMark::Withdrawn { .. }) => false,
        })
    }

    fn unserved(
        &self,
        ant: UserId,
        owner: Option<UserId>,
        messages: Vec<Waiting>,
        entries: &[RosterEntry],
        now: DateTime<Utc>,
    ) -> Unserved {
        let capabilities = self.capabilities.get(&ant).cloned();
        let peers = capabilities.as_deref().map_or_default(|c| peers(c, entries, &[&ant, &self.sender], now));
        let absent_since = self.absent_since.get(&ant).copied().unwrap_or(now);
        Unserved { ant, owner, messages, absent_since, capabilities, peers }
    }
}

/// What to keep track of of the message `sender`, in `role`, stored on `subject` as `payload`: a genuine message, to an
/// ant other than the sender, that is no decision on a tool request.
fn sent(
    sender: &UserId,
    role: Role,
    subject: &str,
    payload: &[u8],
    is_human: impl Fn(&UserId) -> bool,
) -> Option<Sent> {
    let envelope: Envelope = serde_json::from_slice(payload).ok()?;
    if envelope.check_subject(subject).is_err()
        || envelope.approval.is_some()
        || &envelope.to == sender
        || is_human(&envelope.to)
    {
        return None;
    }
    let line = envelope.text.split_whitespace().collect::<Vec<_>>().join(" ");
    let excerpt = match line.char_indices().nth(EXCERPT) {
        Some((at, _)) => format!("{}…", &line[..at]),
        None => line,
    };
    Some(Sent {
        id: envelope.id,
        to: envelope.to,
        thread: envelope.thread_id,
        owner: if role.is_human() { None } else { envelope.owner },
        sent_at: envelope.sent_at,
        excerpt,
    })
}

/// The live ants of `entries` but `not`, advertising some of `capabilities`: the most shared first, then by id.
fn peers(capabilities: &[String], entries: &[RosterEntry], not: &[&UserId], now: DateTime<Utc>) -> Vec<Peer> {
    let mut peers: Vec<Peer> = entries
        .iter()
        .filter(|e| !not.contains(&&e.ant) && crate::roster::is_live(e, now))
        .filter_map(|e| {
            let shared: Vec<String> =
                capabilities.iter().filter(|c| crate::roster::has_capability(e, c)).cloned().collect();
            (!shared.is_empty()).then(|| Peer { ant: e.ant.clone(), shared })
        })
        .collect();
    peers.sort_by(|a, b| b.shared.len().cmp(&a.shared.len()).then_with(|| a.ant.cmp(&b.ant)));
    peers.truncate(MAX_PEERS);
    peers
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use ants_proto::Permissions;
    use ants_test_support::NatsServer;

    use super::*;
    use crate::bus;
    use crate::sync::MutexExt as _;

    fn p(name: &str) -> UserId {
        name.parse().unwrap()
    }

    fn entry(ant: &str, capabilities: &[&str], heartbeat_at: DateTime<Utc>) -> RosterEntry {
        serde_json::from_value(serde_json::json!({
            "ant": ant, "machine": "m1", "harness": "copilot", "capabilities": capabilities,
            "started_at": heartbeat_at, "heartbeat_at": heartbeat_at,
        }))
        .unwrap()
    }

    #[test]
    fn only_genuine_messages_to_another_ant_are_kept_track_of() {
        let (dev_a, dev_b, alice) = (p("dev-a"), p("dev-b"), p("alice"));
        let is_human = |user: &UserId| *user == alice;
        let take = |envelope: &Envelope, subject: &str, role| {
            sent(&dev_a, role, subject, &serde_json::to_vec(envelope).unwrap(), is_human)
        };
        let long = format!("fix   the\nbuild {}", "x".repeat(200));
        let message = Envelope::new(dev_a.clone(), dev_b.clone(), long).with_owner(Some(alice.clone()));
        let kept = take(&message, &message.subject(), Role::Ant).unwrap();
        assert_eq!((kept.to, kept.owner), (dev_b.clone(), Some(alice.clone())));
        assert!(kept.excerpt.starts_with("fix the build xx") && kept.excerpt.ends_with('…'), "{}", kept.excerpt);
        assert_eq!(kept.excerpt.chars().count(), EXCERPT + 1);
        assert_eq!(take(&message, &message.subject(), Role::Human).unwrap().owner, None, "a human tells itself");
        assert_eq!(take(&message, &subject::inbox(&dev_b, &p("dev-c")), Role::Ant), None, "forged");
        let to_human = Envelope::new(dev_a.clone(), alice.clone(), "done");
        assert_eq!(take(&to_human, &to_human.subject(), Role::Ant), None);
        let to_itself = Envelope::new(dev_a.clone(), dev_a.clone(), "note");
        assert_eq!(take(&to_itself, &to_itself.subject(), Role::Ant), None);
    }

    #[test]
    fn peers_share_the_most_capabilities_first_and_are_live() {
        let now = Utc::now();
        let stale = now - TimeDelta::minutes(5);
        let entries = [
            entry("dev-a", &["rust"], now),
            entry("dev-c", &["Rust"], now),
            entry("dev-d", &["rust", "docs"], now),
            entry("dev-e", &["rust", "docs"], stale),
            entry("dev-f", &["go"], now),
        ];
        let capabilities = ["rust".to_owned(), "docs".to_owned()];
        let found = peers(&capabilities, &entries, &[&p("dev-a"), &p("dev-b")], now);
        let found: Vec<(&str, usize)> = found.iter().map(|peer| (peer.ant.as_str(), peer.shared.len())).collect();
        assert_eq!(found, [("dev-d", 2), ("dev-c", 1)]);
    }

    #[test]
    fn the_notice_lists_the_messages_and_how_to_withdraw_them() {
        let now = Utc::now();
        let message = |id| Waiting {
            id: Uuid::from_u128(id),
            thread: "t1".parse().unwrap(),
            sent_at: now - TimeDelta::minutes(20),
            excerpt: "fix the build".to_owned(),
            seq: 1,
        };
        let mut found = Unserved {
            ant: p("dev-b"),
            owner: None,
            messages: vec![message(1), message(2)],
            absent_since: now - TimeDelta::hours(3),
            capabilities: Some(vec!["rust".to_owned()]),
            peers: vec![Peer { ant: p("dev-c"), shared: vec!["rust".to_owned()] }],
        };
        let text = found.text(&p("dev-a"), now);
        assert!(
            text.starts_with(
                "[ants] 2 messages from dev-a wait for dev-b, which no daemon has served for at least 3h:"
            )
        );
        assert!(
            text.contains(&format!("message {} on thread t1, sent 20m ago: \"fix the build\"", Uuid::from_u128(2)))
        );
        assert!(text.contains("`ants withdraw dev-b MESSAGE`"), "{text}");
        assert!(text.ends_with("Live ants with the same capabilities: dev-c (rust)."), "{text}");
        found.peers.clear();
        assert!(found.text(&p("dev-a"), now).ends_with("No live ant advertises the capabilities of dev-b (rust)."));
        found.capabilities = None;
        assert!(found.text(&p("dev-a"), now).ends_with("prompted with it."));
    }

    #[test]
    fn a_humans_credentials_list_the_put_of_its_own_markers() {
        let alice = p("alice");
        let publish = Permissions::for_user(&alice, Role::Human).publish;
        assert!(publish.contains(&marker_subject(&alice)), "{publish:?}");
        let key = format!("$KV.{ATTEMPTS_BUCKET}.{}", unserved_key(&alice, &Uuid::now_v7()));
        assert_eq!(key.rsplit_once('.').unwrap().0, marker_subject(&alice).trim_end_matches(".*"));
    }

    /// The watch of an ant's messages tells once of the one waiting for an ant absent long enough, suggesting the
    /// live ants with its capabilities, and not of one finished, nor of one to an ant served; nor does another watch.
    #[tokio::test]
    async fn tells_once_of_a_message_waiting_for_an_absent_ant() {
        let Some(server) = NatsServer::start() else { return };
        let (dev_a, dev_b, dev_c, alice) = (p("dev-a"), p("dev-b"), p("dev-c"), p("alice"));
        let (_client, js) = bus::connect(&server.url, None, None, &dev_a, "test").await.unwrap();
        bus::ensure_stream(&js).await.unwrap();
        let attempts = bus::ensure_attempts(&js, None, &dev_a).await.unwrap();
        let t0 = Utc::now();
        let later = move |minutes| t0 + TimeDelta::minutes(minutes);
        // dev-b is live at first, then gone; dev-c and dev-d stay.
        let gone = Arc::new(Mutex::new(false));
        let roster: Roster = {
            let gone = gone.clone();
            Arc::new(move || {
                let mut entries = vec![entry("dev-c", &[], later(60)), entry("dev-d", &["rust"], later(60))];
                if !*gone.locked() {
                    entries.push(entry("dev-b", &["rust"], t0));
                }
                Box::pin(async move { Ok(entries) })
            })
        };
        let waiting = Envelope::new(dev_a.clone(), dev_b.clone(), "fix the build").with_owner(Some(alice.clone()));
        let finished = Envelope::new(dev_a.clone(), dev_b.clone(), "done already");
        let served = Envelope::new(dev_a.clone(), dev_c.clone(), "busy");
        let to_human = Envelope::new(dev_a.clone(), alice.clone(), "hello");
        for envelope in [&waiting, &finished, &served, &to_human] {
            bus::publish(&js, envelope).await.unwrap();
        }
        let mark = MessageMark::Finished { thread_id: finished.thread_id.clone(), at: t0 };
        attempts.put(message_mark_key(&dev_b, &finished.id), serde_json::to_vec(&mark).unwrap()).await.unwrap();
        let is_human = |user: &UserId| *user == alice;
        let new_watch = || async { Watch::new(&js, None, dev_a.clone(), Role::Ant, roster.clone()).await.unwrap() };

        let mut watch = new_watch().await;
        assert_eq!(watch.check(&is_human, t0).await.unwrap(), []);
        assert_eq!(watch.sent.len(), 3, "the message to a human is not kept");
        *gone.locked() = true;
        assert_eq!(watch.check(&is_human, later(1)).await.unwrap(), [], "absent for no time yet");
        assert_eq!(watch.check(&is_human, later(15)).await.unwrap(), [], "absent for 14 minutes");
        let found = watch.check(&is_human, later(16)).await.unwrap();
        assert_eq!(found.len(), 1, "{found:?}");
        let found = found.into_iter().next().unwrap();
        assert_eq!((&found.ant, &found.owner, found.absent_since), (&dev_b, &Some(alice.clone()), later(1)));
        assert_eq!(found.messages.iter().map(|m| m.id).collect::<Vec<_>>(), [waiting.id]);
        assert_eq!(found.peers, [Peer { ant: p("dev-d"), shared: vec!["rust".to_owned()] }]);
        let told = watch.claim(found.clone()).await.expect("first to claim");
        assert_eq!(told.messages.len(), 1);
        assert_eq!(watch.sent.values().map(|s| &s.to).collect::<Vec<_>>(), [&dev_c], "the finished one is dropped");
        assert_eq!(watch.claim(found).await, None, "claimed already");
        assert_eq!(watch.check(&is_human, later(17)).await.unwrap(), []);

        // Another watcher, or the same one restarted, finds it told.
        let mut other = new_watch().await;
        other.check(&is_human, later(1)).await.unwrap();
        assert_eq!(other.check(&is_human, later(17)).await.unwrap(), []);
        assert_eq!(other.sent.len(), 1, "{:?}", other.sent);
    }
}
