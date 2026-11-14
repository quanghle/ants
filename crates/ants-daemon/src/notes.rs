//! Thread notes: the posts of a thread live in the ANTS stream on `ants.notes.<thread>.<from>`.
//!
//! A reader reads the notes of a thread back through its own consumer `notes-<user>`, created for the read and deleted
//! after it, so its credentials need not let it touch any other consumer. Posts whose sender or thread disagrees with
//! their subject are dropped. A post states the stream sequence of the last post it saw in the notes, so two agents
//! spending the same turn cannot both succeed: the second one reads the notes again.

use std::time::Duration;

use ants_proto::{NotesPost, STREAM, ThreadId, UserId, notes_consumer, subject};
use anyhow::{Context as _, Result};
use async_nats::jetstream::context::PublishErrorKind;
use async_nats::jetstream::{self, consumer};
use futures::StreamExt as _;
use futures::future::BoxFuture;

use crate::bus;

/// How long a consumer left behind by an interrupted read lives on.
const INACTIVE: Duration = Duration::from_secs(30);
/// Messages asked for at once.
const BATCH: u64 = 256;
/// Reads of busy notes before [`spend`] gives up.
const SPEND_TRIES: usize = 5;
/// Header naming the subjects `Nats-Expected-Last-Subject-Sequence` applies to (nats-server 2.11).
const EXPECTED_LAST_SUBJECT: &str = "Nats-Expected-Last-Subject-Sequence-Subject";

/// The notes of one thread: its posts, oldest first.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Notes {
    pub posts: Vec<NotesPost>,
    /// Stream sequence of the last post, 0 for empty notes.
    pub last_seq: u64,
}

impl Notes {
    pub fn last(&self) -> Option<&NotesPost> {
        self.posts.last()
    }
}

/// Reads and writes thread notes.
pub trait NotesStore: Send + Sync {
    fn read<'a>(&'a self, thread: &'a ThreadId) -> BoxFuture<'a, Result<Notes>>;

    /// Store `post` unless its notes got another post after `last_seq`; false then.
    fn post<'a>(&'a self, post: &'a NotesPost, last_seq: u64) -> BoxFuture<'a, Result<bool>>;
}

/// Thread notes in the ANTS stream, as `reader` sees them.
pub struct JetStreamNotes {
    js: jetstream::Context,
    reader: UserId,
    /// One read at a time: they share the reader's consumer.
    reading: tokio::sync::Mutex<()>,
}

impl JetStreamNotes {
    pub fn new(js: jetstream::Context, reader: UserId) -> Self {
        Self { js, reader, reading: tokio::sync::Mutex::new(()) }
    }
}

impl NotesStore for JetStreamNotes {
    fn read<'a>(&'a self, thread: &'a ThreadId) -> BoxFuture<'a, Result<Notes>> {
        Box::pin(async move {
            let _one = self.reading.lock().await;
            read(&self.js, &self.reader, thread).await
        })
    }

    fn post<'a>(&'a self, post: &'a NotesPost, last_seq: u64) -> BoxFuture<'a, Result<bool>> {
        Box::pin(publish(&self.js, post, Some(last_seq)))
    }
}

/// The notes of `thread`, read through the consumer of `reader`. Only one read per reader may run at a time.
pub async fn read(js: &jetstream::Context, reader: &UserId, thread: &ThreadId) -> Result<Notes> {
    let name = notes_consumer(reader);
    // A consumer left by an interrupted read would carry on where it stopped.
    let _ = js.delete_consumer_from_stream(&name, STREAM).await;
    let config = consumer::pull::Config {
        name: Some(name.clone()),
        filter_subject: subject::notes_of(thread),
        ack_policy: consumer::AckPolicy::None,
        inactive_threshold: INACTIVE,
        ..Default::default()
    };
    let consumer: consumer::PullConsumer = js
        .create_consumer_on_stream(config, STREAM)
        .await
        .with_context(|| format!("reading the notes of thread {thread}"))?;
    let notes = collect(&consumer, thread).await;
    if let Err(e) = js.delete_consumer_from_stream(&name, STREAM).await {
        tracing::debug!(error = %e, consumer = name, "deleting the notes consumer");
    }
    notes
}

async fn collect(consumer: &consumer::PullConsumer, thread: &ThreadId) -> Result<Notes> {
    let mut notes = Notes::default();
    let mut left = consumer.cached_info().num_pending;
    while left > 0 {
        let mut batch = consumer.fetch().max_messages(left.min(BATCH) as usize).messages().await?;
        let mut got = 0;
        while let Some(msg) = batch.next().await {
            let msg = msg.map_err(|e| anyhow::anyhow!(e))?;
            got += 1;
            if let Ok(info) = msg.info() {
                notes.last_seq = info.stream_sequence;
            }
            let post = serde_json::from_slice::<NotesPost>(&msg.payload)
                .map_err(|e| e.to_string())
                .and_then(|post| post.check_subject(&msg.subject).map(|()| post).map_err(|e| e.to_string()));
            match post {
                Ok(post) => notes.posts.push(post),
                Err(error) => tracing::warn!(%thread, subject = %msg.subject, error, "dropping a notes post"),
            }
        }
        if got == 0 {
            break;
        }
        left = left.saturating_sub(got);
    }
    Ok(notes)
}

/// Store `post` in the notes of its thread once JetStream has it. With `last_seq`, only if the last post is still that
/// one (0: the notes are empty); false otherwise. Without, the post may wait in the machine's outbox while the hub is
/// out of reach ([`bus::store`]); a post that takes a turn cannot, as only the hub knows whose turn it is.
pub async fn publish(js: &jetstream::Context, post: &NotesPost, last_seq: Option<u64>) -> Result<bool> {
    let Some(seq) = last_seq else {
        bus::store(js, post.subject(), post, &post.id.to_string()).await?;
        return Ok(true);
    };
    let message = jetstream::message::PublishMessage::build()
        .payload(serde_json::to_vec(post)?.into())
        .expected_last_subject_sequence(seq)
        .header(EXPECTED_LAST_SUBJECT, subject::notes_of(&post.thread_id).as_str());
    let ack = js.send_publish(post.subject(), message).await.context("posting")?;
    match ack.await {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == PublishErrorKind::WrongLastSequence => Ok(false),
        Err(e) => Err(e).context("waiting for JetStream to store the post"),
    }
}

/// Post `text` from `from` to the notes of `thread`, spending one of its turns, at most `budget` left: the notes are
/// read again when another post took the turn first. None when the notes have no turns left.
pub async fn spend(
    store: &dyn NotesStore,
    thread: &ThreadId,
    from: &UserId,
    text: &str,
    budget: u32,
) -> Result<Option<NotesPost>> {
    for _ in 0..SPEND_TRIES {
        let notes = store.read(thread).await?;
        let Ok(post) = NotesPost::spend(thread.clone(), from.clone(), text, budget, notes.last()) else {
            return Ok(None);
        };
        if store.post(&post, notes.last_seq).await? {
            return Ok(Some(post));
        }
    }
    anyhow::bail!("the notes of thread {thread} kept changing")
}
