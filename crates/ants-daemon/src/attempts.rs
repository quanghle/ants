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

use ants_proto::{ChannelPost, Envelope, PersonaId, PlanPost, ThreadId, Uuid, attempt_key};
use anyhow::{Context as _, Result};
use async_nats::jetstream::kv;
use serde::{Deserialize, Serialize};

/// What an earlier attempt at a message left.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Marker {
    /// It started prompting and stored no reply.
    Started,
    /// It had its reply, which may not have reached the stream.
    Replied(Reply),
}

/// The reply to a message: an envelope to its sender, a post on the thread of a channel post, or a post to the plan
/// room of a thread, as a member of an escalation group other than its leader answers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Reply {
    Message(Envelope),
    Post(ChannelPost),
    Plan(PlanPost),
}

impl Reply {
    pub fn subject(&self) -> String {
        match self {
            Self::Message(envelope) => envelope.subject(),
            Self::Post(post) => post.subject(),
            Self::Plan(post) => post.subject(),
        }
    }

    pub fn thread_id(&self) -> &ThreadId {
        match self {
            Self::Message(envelope) => &envelope.thread_id,
            Self::Post(post) => &post.thread_id,
            Self::Plan(post) => &post.thread_id,
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

/// The attempt markers of one persona, under its own keys.
pub struct Attempts {
    kv: kv::Store,
    persona: PersonaId,
}

impl Attempts {
    pub fn new(kv: kv::Store, persona: PersonaId) -> Self {
        Self { kv, persona }
    }

    /// What an earlier attempt at `envelope`, stored at stream sequence `seq`, left.
    pub async fn get(&self, seq: u64, envelope: &Envelope) -> Result<Option<Marker>> {
        let Some(value) = self.kv.get(attempt_key(&self.persona, seq)).await.context("reading the attempt marker")?
        else {
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
        Ok(Some(record.reply.map_or(Marker::Started, Marker::Replied)))
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
        if let Err(e) = self.kv.delete(attempt_key(&self.persona, seq)).await {
            tracing::warn!(seq, error = %e, "removing the attempt marker");
        }
    }

    async fn put(&self, seq: u64, record: &Record) -> Result<()> {
        self.kv.put(attempt_key(&self.persona, seq), serde_json::to_vec(record)?.into()).await?;
        Ok(())
    }
}

/// Messages whose reply is stored, in the hub's stream or the machine's outbox, while their ack may not be: one file
/// per message, named after its envelope id, holding the reply, in `<home>/answered` of the persona.
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
/// its envelope id, in a directory of the persona's home.
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

    /// Remove the note of `message`. A failure only leaves it until [`prune`](Self::prune).
    pub async fn forget(&self, message: &Envelope) {
        let path = self.path(message);
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
        self.dir.join(format!("{}.json", message.id))
    }
}
