//! Markers of attempts at inbox messages, in the KV bucket [`ants_proto::ATTEMPTS_BUCKET`].
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

use ants_proto::{Envelope, PersonaId, Uuid, attempt_key};
use anyhow::{Context as _, Result};
use async_nats::jetstream::kv;
use serde::{Deserialize, Serialize};

/// What an earlier attempt at a message left.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Marker {
    /// It started prompting and stored no reply.
    Started,
    /// It had its reply, which may not have reached the stream.
    Replied(Envelope),
}

#[derive(Serialize, Deserialize)]
struct Record {
    /// The message attempted. A recreated stream reuses sequences, so a marker naming another message is stale.
    message: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reply: Option<Envelope>,
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
    pub async fn replied(&self, seq: u64, envelope: &Envelope, reply: &Envelope) -> Result<()> {
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
pub struct Answered {
    dir: PathBuf,
}

impl Answered {
    pub fn new(home: &Path) -> Self {
        Self { dir: home.join("answered") }
    }

    /// Whether the reply to `request` is stored.
    pub async fn has(&self, request: &Envelope) -> bool {
        let path = self.path(request);
        tokio::fs::try_exists(&path).await.unwrap_or_else(|e| {
            tracing::warn!(path = %path.display(), error = %e, "looking for a stored reply");
            false
        })
    }

    /// Note that `reply` to `request` is stored, durably, before the request is acked.
    pub async fn add(&self, request: &Envelope, reply: &Envelope) -> Result<()> {
        let path = self.path(request);
        let tmp = path.with_extension("tmp");
        let write = async {
            tokio::fs::create_dir_all(&self.dir).await?;
            let mut file = tokio::fs::File::create(&tmp).await?;
            tokio::io::AsyncWriteExt::write_all(&mut file, &serde_json::to_vec(reply)?).await?;
            file.sync_all().await?;
            tokio::fs::rename(&tmp, &path).await?;
            anyhow::Ok(())
        };
        write.await.with_context(|| format!("noting the stored reply in {}", path.display()))
    }

    /// Forget `request` once it is acked. A failure only leaves the note until [`prune`](Self::prune).
    pub async fn forget(&self, request: &Envelope) {
        let path = self.path(request);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => tracing::warn!(path = %path.display(), error = %e, "removing the note of a stored reply"),
        }
    }

    /// Remove notes older than `max_age`, by when the stream no longer keeps their messages.
    pub async fn prune(&self, max_age: Duration) {
        let Ok(mut entries) = tokio::fs::read_dir(&self.dir).await else { return };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let modified = entry.metadata().await.and_then(|m| m.modified());
            let age = modified.ok().and_then(|m| SystemTime::now().duration_since(m).ok());
            if age.is_some_and(|age| age > max_age)
                && let Err(e) = tokio::fs::remove_file(entry.path()).await
            {
                tracing::warn!(path = %entry.path().display(), error = %e, "removing an old note of a stored reply");
            }
        }
    }

    fn path(&self, request: &Envelope) -> PathBuf {
        self.dir.join(format!("{}.json", request.id))
    }
}
