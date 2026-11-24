//! Markers of attempts at inbox messages and channel posts, in the KV bucket [`ants_proto::ATTEMPTS_BUCKET`].
//!
//! Delivery is at least once: a daemon that stops after its harness ran tools but before the ack gets the message
//! again. Before prompting, the daemon marks the attempt as started under the message's stream sequence, and once the
//! harness has answered it records the reply in the marker before storing it in the stream. A redelivery that finds a
//! started marker is prompted with a warning that tools may already have run; one that finds a reply stores that reply
//! again (under the same message id, so the stream keeps one copy) and is acked without a prompt. The marker is
//! removed once the message is acked or dead-lettered; one left behind expires with the stream's messages.
//!
//! The markers live on the hub, out of reach while a leaf node's uplink is down, and so is the ack. A reply stored
//! then goes to the machine's outbox, and the message stays unacked. [`Answered`] keeps, on the machine itself, which
//! messages have their reply stored until their ack is in, so their redelivery, even to a restarted daemon, is acked
//! without being answered again.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use ants_proto::{
    Attachment, ChannelPost, Envelope, Hold, MessageMark, NoReplyWhy, NotesPost, ThreadId, UserId, Uuid,
    assisted_withdrawal_key, attempt_key, message_mark_key,
};
use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};

use crate::records::direct_kv::{DirectKv, Lookup};

/// What an earlier attempt at a message left.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Marker {
    /// It started prompting and stored no reply.
    Started,
    /// It had its reply, which may not have reached the stream.
    Replied(Box<Reply>),
}

/// The reply to a message: an envelope to its sender, a post on the thread of a channel post, or a post to the notes
/// of a thread, as a member of an escalation group other than its leader answers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Reply {
    Message(Box<Envelope>),
    Post(ChannelPost),
    Notes(NotesPost),
}

impl Reply {
    pub fn subject(&self) -> String {
        match self {
            Self::Message(envelope) => envelope.subject(),
            Self::Post(post) => post.subject(),
            Self::Notes(post) => post.subject(),
        }
    }

    pub fn thread_id(&self) -> &ThreadId {
        match self {
            Self::Message(envelope) => &envelope.thread_id,
            Self::Post(post) => &post.thread_id,
            Self::Notes(post) => &post.thread_id,
        }
    }
}

/// What became of a message [`Attempts::take_up`] marked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TakenUp {
    /// Marked started, instead of a `retrying` mark holding it for this when one did.
    Started(Option<Hold>),
    /// Withdrawn first, by `by` for `on_behalf_of` when an assistant withdrew it, while held for `held`.
    Withdrawn { by: UserId, on_behalf_of: Option<UserId>, held: Option<Hold> },
}

#[derive(Serialize, Deserialize)]
struct Record {
    /// The message attempted. A recreated stream reuses sequences, so a marker naming another message is stale.
    message: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reply: Option<Reply>,
}

/// The attempt markers of one ant, under its own keys.
pub struct Attempts {
    kv: DirectKv,
    ant: UserId,
}

impl Attempts {
    pub fn new(kv: DirectKv, ant: UserId) -> Self {
        Self { kv, ant }
    }

    /// What an earlier attempt at `envelope`, stored at stream sequence `seq`, left.
    pub async fn get(&self, seq: u64, envelope: &Envelope) -> Result<Option<Marker>> {
        let Some(value) = self.kv.get(attempt_key(&self.ant, seq)).await.context("reading the attempt marker")? else {
            return Ok(None);
        };
        let record: Record = match serde_json::from_slice(&value) {
            Ok(record) => record,
            Err(e) => {
                // Something started; warning without cause is safer than repeating effects without warning.
                tracing::warn!(seq, error = %e, "unreadable attempt marker; taking it as started");
                return Ok(Some(Marker::Started));
            }
        };
        if record.message != envelope.id {
            return Ok(None);
        }
        Ok(Some(record.reply.map_or(Marker::Started, |r| Marker::Replied(Box::new(r)))))
    }

    /// Mark an attempt at `envelope` as started.
    pub async fn start(&self, seq: u64, envelope: &Envelope) -> Result<()> {
        self.put(seq, &Record { message: envelope.id, reply: None }).await.context("marking the attempt as started")
    }

    /// Record the reply of an attempt at `envelope`.
    pub async fn replied(&self, seq: u64, envelope: &Envelope, reply: &Reply) -> Result<()> {
        let record = Record { message: envelope.id, reply: Some(reply.clone()) };
        self.put(seq, &record).await.context("recording the reply with the attempt marker")
    }

    /// Remove the marker of a message that is acked or dead-lettered. A failure only leaves it to expire.
    pub async fn forget(&self, seq: u64) {
        if let Err(e) = self.kv.delete(attempt_key(&self.ant, seq)).await {
            tracing::warn!(seq, error = %e, "removing the attempt marker");
        }
    }

    /// Mark `envelope` as started ([`MessageMark`]) unless it has a mark, or instead of its `retrying` mark, right
    /// before handling it. A message started already goes on as a redelivery does.
    pub async fn take_up(&self, envelope: &Envelope) -> Result<TakenUp> {
        let started = MessageMark::Started { thread_id: envelope.thread_id.clone(), at: chrono::Utc::now() };
        Ok(match mark(&self.kv, &self.ant, &envelope.id, &started).await? {
            Ok(held) => TakenUp::Started(held),
            Err(MessageMark::Withdrawn { by, on_behalf_of, held, .. }) => TakenUp::Withdrawn { by, on_behalf_of, held },
            Err(MessageMark::Started { .. } | MessageMark::Retrying { .. } | MessageMark::Finished { .. }) => {
                TakenUp::Started(None)
            }
        })
    }

    /// Mark `envelope`, started, as awaiting a retry after a failed attempt ([`MessageMark::Retrying`]), right before
    /// handing it back for one or holding it for `held`, a sign-in or a provider limit, so it can be withdrawn until it
    /// is taken up again. A failure only leaves it started.
    pub async fn retrying(&self, envelope: &Envelope, held: Option<Hold>) {
        let key = message_mark_key(&self.ant, &envelope.id);
        let retrying = MessageMark::Retrying { thread_id: envelope.thread_id.clone(), at: chrono::Utc::now(), held };
        let swap = async {
            let (found, last) = self.kv.lookup_last(&key).await?;
            let started = match found {
                Lookup::Value(found) => matches!(serde_json::from_slice(&found), Ok(MessageMark::Started { .. })),
                Lookup::NoValue | Lookup::NoBucket => false,
            };
            anyhow::Ok(started && self.kv.update(&key, serde_json::to_vec(&retrying)?, last).await?)
        };
        match swap.await {
            Ok(true) => {}
            Ok(false) => {
                tracing::info!(message = %envelope.id, "the message's mark is no longer started; not retrying");
            }
            Err(e) => tracing::warn!(message = %envelope.id, error = format!("{e:#}"), "marking the message retrying"),
        }
    }

    /// The human `assistant` withdrew `envelope` for under a key of its own ([`record_withdrawal`]), with the stream
    /// sequence of the message's started mark, when the withdrawal was stored before that mark: the message was not
    /// taken up when the assistant was told the withdrawal holds. The human is as the record claims it, unchecked.
    pub async fn withdrawal_by(&self, envelope: &Envelope, assistant: &UserId) -> Result<Option<(UserId, u64)>> {
        let key = assisted_withdrawal_key(&self.ant, &envelope.id, assistant);
        let (Lookup::Value(record), recorded) = self.kv.lookup_last(&key).await.context("reading a withdrawal")? else {
            return Ok(None);
        };
        let (Lookup::Value(mark), started) =
            self.kv.lookup_last(message_mark_key(&self.ant, &envelope.id)).await.context("reading the mark")?
        else {
            return Ok(None);
        };
        if recorded > started || !matches!(serde_json::from_slice(&mark), Ok(MessageMark::Started { .. })) {
            return Ok(None);
        }
        match serde_json::from_slice(&record) {
            Ok(MessageMark::Withdrawn { by, on_behalf_of: Some(human), .. }) if &by == assistant => {
                Ok(Some((human, started)))
            }
            _ => {
                tracing::warn!(%key, "ignoring a withdrawal naming no human, or another assistant");
                Ok(None)
            }
        }
    }

    /// Mark `envelope`, whose started mark is at stream sequence `started`, withdrawn by `assistant` for `human`
    /// instead, unless the mark changed meanwhile; `held` is what that mark took it up from. Whether it did.
    pub async fn withdraw_started(
        &self,
        envelope: &Envelope,
        assistant: &UserId,
        human: &UserId,
        started: u64,
        held: Option<Hold>,
    ) -> Result<bool> {
        let (by, on_behalf_of) = (assistant.clone(), Some(human.clone()));
        let mark = MessageMark::Withdrawn { by, at: chrono::Utc::now(), on_behalf_of, held };
        let key = message_mark_key(&self.ant, &envelope.id);
        self.kv.update(key, serde_json::to_vec(&mark)?, started).await.context("marking the message withdrawn")
    }

    /// Withdraw `message` for `by`, or for `on_behalf_of` whose assistant `by` is, as [`withdraw`] does.
    pub async fn withdraw(
        &self,
        message: &Uuid,
        by: &UserId,
        on_behalf_of: Option<&UserId>,
    ) -> Result<Option<MessageMark>> {
        withdraw(&self.kv, &self.ant, message, by, on_behalf_of).await
    }

    /// Mark `envelope`, acked or dead-lettered, as finished, so a withdrawal tells so. A failure only leaves it started.
    pub async fn finish(&self, envelope: &Envelope) {
        let mark = MessageMark::Finished { thread_id: envelope.thread_id.clone(), at: chrono::Utc::now() };
        let put = async { self.kv.put(message_mark_key(&self.ant, &envelope.id), serde_json::to_vec(&mark)?).await };
        if let Err(e) = put.await {
            tracing::warn!(message = %envelope.id, error = format!("{e:#}"), "marking the message as finished");
        }
    }

    /// Remove the mark of `envelope`, handed back unprompted to wait (for a sign-in or a provider limit), so it can be
    /// withdrawn meanwhile. A failure only leaves it started.
    pub async fn release(&self, envelope: &Envelope) {
        if let Err(e) = self.kv.delete(message_mark_key(&self.ant, &envelope.id)).await {
            tracing::warn!(message = %envelope.id, error = format!("{e:#}"), "removing the message's mark");
        }
    }

    async fn put(&self, seq: u64, record: &Record) -> Result<()> {
        self.kv.put(attempt_key(&self.ant, seq), serde_json::to_vec(record)?).await?;
        Ok(())
    }
}

/// Withdraw message `message` from the queue of `ant` for `by` (`ants withdraw`), or for the human `on_behalf_of` whose
/// assistant `by` is, in `kv`, the attempts bucket, unless it has a mark already but `retrying`: a message awaiting a
/// retry after a failed attempt is withdrawn unless its redelivery is taken up first, and so is one held for a sign-in
/// or a provider limit, its hold kept in the withdrawal. Returns the mark found instead: the daemon took the message up
/// first, or it was withdrawn.
pub async fn withdraw(
    kv: &DirectKv,
    ant: &UserId,
    message: &Uuid,
    by: &UserId,
    on_behalf_of: Option<&UserId>,
) -> Result<Option<MessageMark>> {
    let on_behalf_of = on_behalf_of.cloned();
    let withdrawn = MessageMark::Withdrawn { by: by.clone(), at: chrono::Utc::now(), on_behalf_of, held: None };
    Ok(mark(kv, ant, message, &withdrawn).await?.err())
}

/// What became of a withdrawal [`record_withdrawal`] stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Recorded {
    /// It holds: the daemon of the ant honours it before taking the message up, the grant still naming the assistant.
    Holds,
    /// The message has this mark already, of a withdrawal or one stored before the record: the record holds no more.
    Marked(MessageMark),
}

/// Record the withdrawal of message `message` from the queue of `ant` by the assistant `assistant` for `human`, in
/// `kv`, the attempts bucket, while no daemon of the ant answers its cancel request: under a key of the assistant's
/// own ([`assisted_withdrawal_key`]), which the ant's daemon reads before taking up a message of `human`'s
/// ([`Attempts::withdrawal_by`]). A message taken up or finished before the record is stored keeps going: the record
/// is deleted again.
pub async fn record_withdrawal(
    kv: &DirectKv,
    ant: &UserId,
    message: &Uuid,
    assistant: &UserId,
    human: &UserId,
) -> Result<Recorded> {
    let key = assisted_withdrawal_key(ant, message, assistant);
    let (by, on_behalf_of) = (assistant.clone(), Some(human.clone()));
    let record = MessageMark::Withdrawn { by, at: chrono::Utc::now(), on_behalf_of, held: None };
    kv.create(&key, serde_json::to_vec(&record)?).await.context("recording the withdrawal")?;
    let (_, recorded) = kv.lookup_last(&key).await.context("reading the withdrawal")?;
    let (mark, marked) = kv.lookup_last(message_mark_key(ant, message)).await.context("reading the message's mark")?;
    let Lookup::Value(mark) = mark else { return Ok(Recorded::Holds) };
    let mark: MessageMark = serde_json::from_slice(&mark).context("reading the message's mark")?;
    // A message awaiting a retry is started again after the record, on its redelivery.
    if matches!(mark, MessageMark::Retrying { .. }) || matches!(mark, MessageMark::Started { .. }) && marked > recorded
    {
        return Ok(Recorded::Holds);
    }
    if !matches!(mark, MessageMark::Withdrawn { .. })
        && let Err(e) = kv.delete(&key).await
    {
        tracing::warn!(%key, error = format!("{e:#}"), "deleting a withdrawal that came too late");
    }
    Ok(Recorded::Marked(mark))
}

/// Create the mark of `message` to `ant` as `mark` unless it has one, or put it instead of a
/// [`MessageMark::Retrying`] mark: by compare-and-set on that mark, so of a daemon taking the message up again and a
/// withdrawal exactly one wins; a withdrawal keeps what the message was held for. Returns that, or the mark found
/// instead.
async fn mark(
    kv: &DirectKv,
    ant: &UserId,
    message: &Uuid,
    mark: &MessageMark,
) -> Result<Result<Option<Hold>, MessageMark>> {
    let key = message_mark_key(ant, message);
    // A mark removed ([`Attempts::release`]) or changed meanwhile is read again.
    for _ in 0..3 {
        if kv.create(&key, serde_json::to_vec(mark)?).await.context("marking the message")? {
            return Ok(Ok(None));
        }
        let (Lookup::Value(found), last) = kv.lookup_last(&key).await.context("reading the message's mark")? else {
            continue;
        };
        let found: MessageMark = serde_json::from_slice(&found).context("reading the message's mark")?;
        let MessageMark::Retrying { held, .. } = found else { return Ok(Err(found)) };
        let mut mark = mark.clone();
        if let MessageMark::Withdrawn { held: withdrawn_from, .. } = &mut mark {
            *withdrawn_from = held;
        }
        if kv.update(&key, serde_json::to_vec(&mark)?, last).await.context("marking the message")? {
            return Ok(Ok(held));
        }
    }
    anyhow::bail!("the mark of message {message} keeps changing")
}

/// Messages whose reply is stored, in the hub's stream or the machine's outbox, while their ack may not be: one file
/// per message, named after its envelope id, holding the reply, in `<home>/answered` of the ant. Beside them, in
/// `<home>/unsent`, the replies the hub refused to store with their attempt marker, as while its JetStream store is
/// full: the message's retry sends that reply rather than prompting the harness again. In `<home>/given`, the answers
/// the harness gave whose reply a later step failed to build or record, as an escalation the full store refused
/// ([`Given`]): the retry makes the reply of that answer, with the title and the files its turn gave it, rather than
/// prompting again. A message withdrawn or dead-lettered keeps neither ([`Answered::forget_kept`]).
pub struct Answered {
    stored: Notes,
    unsent: Notes,
    given: Notes,
}

/// What the harness answered a message with, kept on the machine until its reply is recorded ([`Answered`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Given {
    /// The answer, with the notices it carries, redacted and cut to fit.
    pub text: String,
    /// The limit or cap the prompt reached, escalated with the reply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<String>,
    /// Why the turn sends no reply, if it sends none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unanswered: Option<(NoReplyWhy, Option<String>)>,
    /// The tokens the prompt spent, for a summary the reply carries.
    #[serde(default)]
    pub tokens: u64,
    /// The title the turn suggested for the thread, for the reply to carry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// The files the turn attached to the answer (`attach_to_reply`), for the reply to carry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<GivenFiles>,
}

/// The files a turn attached to its answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GivenFiles {
    /// The directory the thread's session worked in, which the reply reads them from.
    pub dir: PathBuf,
    /// Their paths in it.
    pub paths: Vec<String>,
    /// Their references once an attempt at the reply stored them, for a retry's reply to carry, nothing stored again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stored: Option<StoredFiles>,
}

/// The files of an answer in the attachments' store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredFiles {
    /// Whose handles they are stored under: the handle of the reply's attachment of nil id, as a reply sent elsewhere
    /// by then has other handles.
    pub under: String,
    /// Their references.
    pub files: Vec<Attachment>,
}

impl Answered {
    pub fn new(home: &Path) -> Self {
        Self {
            stored: Notes::new(home, "answered"),
            unsent: Notes::new(home, "unsent"),
            given: Notes::new(home, "given"),
        }
    }

    /// Whether the reply to `request` is stored.
    pub async fn has(&self, request: &Envelope) -> bool {
        self.stored.has(request).await
    }

    /// Note that `reply` to `request` is stored, durably, before the request is acked.
    pub async fn add(&self, request: &Envelope, reply: &Reply) -> Result<()> {
        self.stored.add(request, reply).await.context("noting the stored reply")
    }

    /// Forget `request` once it is acked. A failure only leaves the note until [`prune`](Self::prune).
    pub async fn forget(&self, request: &Envelope) {
        self.stored.forget(request).await;
    }

    /// Keep `reply` to `request` on the machine, durably, as its attempt marker could not record it.
    pub async fn keep_unsent(&self, request: &Envelope, reply: &Reply) -> Result<()> {
        self.unsent.add(request, reply).await.context("keeping the reply on this machine")
    }

    /// The reply to `request` kept on the machine ([`Self::keep_unsent`]), if any.
    pub async fn unsent(&self, request: &Envelope) -> Option<Reply> {
        self.unsent.get(request).await
    }

    /// Forget the reply to `request` kept on the machine, once it is stored.
    pub async fn forget_unsent(&self, request: &Envelope) {
        self.unsent.forget(request).await;
    }

    /// Keep `given`, the harness's answer to `request`, on the machine, durably, as a step before its reply was
    /// recorded failed.
    pub async fn keep_given(&self, request: &Envelope, given: &Given) -> Result<()> {
        self.given.add(request, given).await.context("keeping the answer on this machine")
    }

    /// The answer to `request` kept on the machine ([`Self::keep_given`]), if any.
    pub async fn given(&self, request: &Envelope) -> Option<Given> {
        self.given.get(request).await
    }

    /// Forget the answer to `request` kept on the machine, once its reply is recorded or the message done with.
    pub async fn forget_given(&self, request: &Envelope) {
        self.given.forget(request).await;
    }

    /// Forget the answer to `request` and the reply to it kept on the machine, as the message is withdrawn or a dead
    /// letter and neither is ever sent.
    pub async fn forget_kept(&self, request: &Envelope) {
        self.given.forget(request).await;
        self.unsent.forget(request).await;
    }

    /// Remove notes older than `max_age`, by when the stream no longer keeps their messages.
    pub async fn prune(&self, max_age: Duration) {
        self.stored.prune(max_age).await;
        self.unsent.prune(max_age).await;
        self.given.prune(max_age).await;
    }
}

/// Notes a daemon keeps on the machine itself about messages it could not ack: one JSON file per message, named after
/// its envelope id, in a directory of the ant's home.
pub struct Notes {
    dir: PathBuf,
}

impl Notes {
    pub fn new(home: &Path, name: &str) -> Self {
        Self { dir: home.join(name) }
    }

    /// Whether `message` has a note.
    pub async fn has(&self, message: &Envelope) -> bool {
        let path = self.path(message);
        tokio::fs::try_exists(&path).await.unwrap_or_else(|e| {
            tracing::warn!(path = %path.display(), error = %e, "looking for a note");
            false
        })
    }

    /// The note of `message`, if it has a readable one.
    pub async fn get<T: serde::de::DeserializeOwned>(&self, message: &Envelope) -> Option<T> {
        let path = self.path(message);
        let read = async { anyhow::Ok(serde_json::from_slice(&tokio::fs::read(&path).await?)?) };
        match read.await {
            Ok(note) => Some(note),
            Err(e) if e.downcast_ref::<std::io::Error>().is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) => {
                None
            }
            Err(e) => {
                tracing::warn!(path = %path.display(), error = format!("{e:#}"), "reading a note");
                None
            }
        }
    }

    /// Note `note` about `message`, durably.
    pub async fn add<T: Serialize>(&self, message: &Envelope, note: &T) -> Result<()> {
        let path = self.path(message);
        let tmp = path.with_extension("tmp");
        let write = async {
            tokio::fs::create_dir_all(&self.dir).await?;
            let mut file = tokio::fs::File::create(&tmp).await?;
            tokio::io::AsyncWriteExt::write_all(&mut file, &serde_json::to_vec(note)?).await?;
            file.sync_all().await?;
            tokio::fs::rename(&tmp, &path).await?;
            anyhow::Ok(())
        };
        write.await.with_context(|| format!("writing {}", path.display()))
    }

    /// Every readable note, with the envelope id of its message.
    pub async fn all<T: serde::de::DeserializeOwned>(&self) -> Vec<(Uuid, T)> {
        let mut notes = Vec::new();
        let Ok(mut entries) = tokio::fs::read_dir(&self.dir).await else { return notes };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            let id =
                path.extension().filter(|ext| *ext == "json").and_then(|_| path.file_stem()?.to_str()?.parse().ok());
            let Some(id) = id else { continue };
            let read = async { anyhow::Ok(serde_json::from_slice(&tokio::fs::read(&path).await?)?) };
            match read.await {
                Ok(note) => notes.push((id, note)),
                Err(e) => tracing::warn!(path = %path.display(), error = format!("{e:#}"), "reading a note"),
            }
        }
        notes
    }

    /// Remove the note of `message`. A failure only leaves it until [`prune`](Self::prune).
    pub async fn forget(&self, message: &Envelope) {
        self.forget_id(message.id).await;
    }

    /// Remove the note of the message with envelope id `id`, as [`forget`](Self::forget) does.
    pub async fn forget_id(&self, id: Uuid) {
        let path = self.path_of(id);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => tracing::warn!(path = %path.display(), error = %e, "removing a note"),
        }
    }

    /// Remove notes older than `max_age`.
    pub async fn prune(&self, max_age: Duration) {
        let Ok(mut entries) = tokio::fs::read_dir(&self.dir).await else { return };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let modified = entry.metadata().await.and_then(|m| m.modified());
            let age = modified.ok().and_then(|m| SystemTime::now().duration_since(m).ok());
            if age.is_some_and(|age| age > max_age)
                && let Err(e) = tokio::fs::remove_file(entry.path()).await
            {
                tracing::warn!(path = %entry.path().display(), error = %e, "removing an old note");
            }
        }
    }

    fn path(&self, message: &Envelope) -> PathBuf {
        self.path_of(message.id)
    }

    fn path_of(&self, id: Uuid) -> PathBuf {
        self.dir.join(format!("{id}.json"))
    }
}

#[cfg(test)]
mod tests {
    use ants_test_support::NatsServer;

    use super::*;

    async fn mark_of(kv: &DirectKv, ant: &UserId, envelope: &Envelope) -> Option<MessageMark> {
        let found = kv.get(message_mark_key(ant, &envelope.id)).await.unwrap();
        found.map(|found| serde_json::from_slice(&found).unwrap())
    }

    #[tokio::test]
    async fn a_withdrawal_and_the_retry_race_for_a_message_awaiting_it_and_exactly_one_wins() {
        let Some(server) = NatsServer::start() else { return };
        let (human, ant): (UserId, UserId) = (ants_test_support::human(), "dev-a".parse().unwrap());
        let (_, js) = crate::bus::connect(&server.url, None, None, &human, "test").await.unwrap();
        let kv = crate::bus::ensure_attempts(&js, None, &ant).await.unwrap();
        let attempts = Attempts::new(kv.clone(), ant.clone());

        let started = Envelope::new(human.clone(), ant.clone(), "started");
        assert_eq!(attempts.take_up(&started).await.unwrap(), TakenUp::Started(None));
        assert!(matches!(
            withdraw(&kv, &ant, &started.id, &human, None).await.unwrap(),
            Some(MessageMark::Started { .. })
        ));
        let withdrawn = Envelope::new(human.clone(), ant.clone(), "withdrawn");
        assert_eq!(withdraw(&kv, &ant, &withdrawn.id, &human, None).await.unwrap(), None);
        attempts.retrying(&withdrawn, None).await;
        assert!(
            matches!(mark_of(&kv, &ant, &withdrawn).await, Some(MessageMark::Withdrawn { .. })),
            "only a started mark"
        );

        let (mut retried, mut withdrew) = (0, 0);
        for round in 0..20 {
            let failed = Envelope::new(human.clone(), ant.clone(), "failed");
            assert_eq!(attempts.take_up(&failed).await.unwrap(), TakenUp::Started(None));
            // Every other one held for a sign-in, which the winner keeps.
            let held = (round % 4 < 2).then_some(Hold::SignIn);
            attempts.retrying(&failed, held).await;
            assert!(
                matches!(mark_of(&kv, &ant, &failed).await, Some(MessageMark::Retrying { held: h, .. }) if h == held)
            );
            let (take_up, withdrawal) = (attempts.take_up(&failed), withdraw(&kv, &ant, &failed.id, &human, None));
            // Each going first in turn.
            let (taken, refused) = if round % 2 == 0 {
                tokio::join!(take_up, withdrawal)
            } else {
                let (refused, taken) = tokio::join!(withdrawal, take_up);
                (taken, refused)
            };
            match (taken.unwrap(), refused.unwrap(), mark_of(&kv, &ant, &failed).await) {
                (TakenUp::Started(h), Some(MessageMark::Started { .. }), Some(MessageMark::Started { .. }))
                    if h == held =>
                {
                    retried += 1;
                }
                (
                    TakenUp::Withdrawn { by, on_behalf_of: None, held: h },
                    None,
                    Some(MessageMark::Withdrawn { held: marked, .. }),
                ) if by == human && h == held && marked == held => withdrew += 1,
                other => panic!("both or neither won: {other:?}"),
            }
        }
        assert_eq!(retried + withdrew, 20);
    }
}
