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
    ChannelPost, Envelope, MessageMark, NotesPost, ThreadId, UserId, Uuid, attempt_key, message_mark_key,
};
use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};

use crate::direct_kv::DirectKv;

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
    Message(Envelope),
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

    /// Mark `envelope` as started ([`MessageMark`]) unless it has a mark, right before handling it. Returns who
    /// withdrew it, when a human's withdrawal came first; a message started already goes on as a redelivery does.
    pub async fn take_up(&self, envelope: &Envelope) -> Result<Option<UserId>> {
        let started = MessageMark::Started { thread_id: envelope.thread_id.clone(), at: chrono::Utc::now() };
        Ok(match mark(&self.kv, &self.ant, &envelope.id, &started).await? {
            Some(MessageMark::Withdrawn { by, .. }) => Some(by),
            Some(MessageMark::Started { .. } | MessageMark::Finished { .. }) | None => None,
        })
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

/// Withdraw message `message` from the queue of `ant` for `by` (`ants withdraw`), in `kv`, the attempts bucket, unless
/// it has a mark already. Returns the mark found instead: the daemon took the message up first, or it was withdrawn.
pub async fn withdraw(kv: &DirectKv, ant: &UserId, message: &Uuid, by: &UserId) -> Result<Option<MessageMark>> {
    mark(kv, ant, message, &MessageMark::Withdrawn { by: by.clone(), at: chrono::Utc::now() }).await
}

/// Create the mark of `message` to `ant` as `mark` unless it has one. Returns the mark found instead.
async fn mark(kv: &DirectKv, ant: &UserId, message: &Uuid, mark: &MessageMark) -> Result<Option<MessageMark>> {
    let key = message_mark_key(ant, message);
    let value = serde_json::to_vec(mark)?;
    // A mark removed between the create and the read ([`Attempts::release`]) is created anew.
    for _ in 0..3 {
        if kv.create(&key, value.clone()).await.context("marking the message")? {
            return Ok(None);
        }
        if let Some(found) = kv.get(&key).await.context("reading the message's mark")? {
            return serde_json::from_slice(&found).map(Some).context("reading the message's mark");
        }
    }
    anyhow::bail!("the mark of message {message} keeps changing")
}

/// Messages whose reply is stored, in the hub's stream or the machine's outbox, while their ack may not be: one file
/// per message, named after its envelope id, holding the reply, in `<home>/answered` of the ant.
pub struct Answered(Notes);

impl Answered {
    pub fn new(home: &Path) -> Self {
        Self(Notes::new(home, "answered"))
    }

    /// Whether the reply to `request` is stored.
    pub async fn has(&self, request: &Envelope) -> bool {
        self.0.has(request).await
    }

    /// Note that `reply` to `request` is stored, durably, before the request is acked.
    pub async fn add(&self, request: &Envelope, reply: &Reply) -> Result<()> {
        self.0.add(request, reply).await.context("noting the stored reply")
    }

    /// Forget `request` once it is acked. A failure only leaves the note until [`prune`](Self::prune).
    pub async fn forget(&self, request: &Envelope) {
        self.0.forget(request).await
    }

    /// Remove notes older than `max_age`, by when the stream no longer keeps their messages.
    pub async fn prune(&self, max_age: Duration) {
        self.0.prune(max_age).await
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
        self.forget_id(message.id).await
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
