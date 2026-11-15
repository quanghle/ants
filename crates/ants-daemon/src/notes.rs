//! Thread notes: the posts of a thread live in the ANTS stream on `ants.notes.<thread>.<from>`.
//!
//! An ant's daemon reads the notes of a thread back through the ant's own consumer `notes-<ant>`, created for the read
//! and deleted after it, so its credentials need not let it touch any other consumer; the CLI with the ant's
//! credentials reads through a spare of it, and a human by message gets ([`Reading`]). Posts whose sender or thread
//! disagrees with their subject are dropped. A post states the stream sequence of the last post it saw in the notes, so
//! two ants spending the same turn cannot both succeed: the second one reads the notes again.

use std::ops::ControlFlow;

use ants_proto::{Attachment, NotesPost, ThreadId, UserId, notes_consumer, subject};
use anyhow::{Context as _, Result};
use async_nats::jetstream::context::PublishErrorKind;
use async_nats::jetstream::{self, consumer};
use futures::future::BoxFuture;

use crate::bus;
use crate::reading::{self, Reading};

/// Reads of busy notes before [`spend`] gives up.
const SPEND_TRIES: usize = 5;
/// Header naming the subjects `Nats-Expected-Last-Subject-Sequence` applies to.
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

/// Thread notes in the ANTS stream, as `reader`, an ant's daemon, sees them.
pub struct JetStreamNotes {
    js: jetstream::Context,
    reader: UserId,
    /// One read at a time: they share the reader's own consumer.
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
            read(&self.js, &self.reader, Reading::Consumer, thread).await
        })
    }

    fn post<'a>(&'a self, post: &'a NotesPost, last_seq: u64) -> BoxFuture<'a, Result<bool>> {
        Box::pin(publish(&self.js, post, Some(last_seq)))
    }
}

/// The notes of `thread`, read as `reading` says, through `reader`'s notes consumer ([`notes_consumer`]) or its spares
/// for a consumer. Only one read through the reader's own consumer may run at a time.
pub async fn read(js: &jetstream::Context, reader: &UserId, reading: Reading, thread: &ThreadId) -> Result<Notes> {
    let mut notes = Notes::default();
    let filter = subject::notes_of(thread);
    let start = consumer::DeliverPolicy::All;
    reading::read_back(js, &notes_consumer(reader), reading, &filter, start, |seq, _, subject, payload| {
        notes.last_seq = seq;
        let post = serde_json::from_slice::<NotesPost>(payload)
            .map_err(|e| e.to_string())
            .and_then(|post| post.check_subject(subject).map(|()| post).map_err(|e| e.to_string()));
        match post {
            Ok(post) => notes.posts.push(post),
            Err(error) => tracing::warn!(%thread, subject, error, "dropping a notes post"),
        }
        ControlFlow::Continue(())
    })
    .await
    .with_context(|| format!("reading the notes of thread {thread}"))?;
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
        Err(e) => Err(bus::explain(js, &post.subject(), e).await).context("waiting for JetStream to store the post"),
    }
}

/// Post `text` from `from` to the notes of `thread`, suggesting `title` for the thread ([`NotesPost::title`]) and
/// carrying `attachments`, stored already under the thread's notes handles, spending one of its turns, at most `budget`
/// left: the notes are read again when another post took the turn first. None when the notes have no turns left.
pub async fn spend(
    store: &dyn NotesStore,
    thread: &ThreadId,
    from: &UserId,
    text: &str,
    budget: u32,
    title: Option<String>,
    attachments: Vec<Attachment>,
) -> Result<Option<NotesPost>> {
    for _ in 0..SPEND_TRIES {
        let notes = store.read(thread).await?;
        let Ok(post) = NotesPost::spend(thread.clone(), from.clone(), text, budget, notes.last()) else {
            return Ok(None);
        };
        let post = NotesPost { title: title.clone(), attachments: attachments.clone(), ..post };
        if store.post(&post, notes.last_seq).await? {
            return Ok(Some(post));
        }
    }
    anyhow::bail!("the notes of thread {thread} kept changing")
}
