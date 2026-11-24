//! The bytes of attachments in their store ([`FILES_STREAM`]): [`put`] stores those of an attachment as chunks under
//! its handle before the message carrying it is sent, out of the hub's reach in the machine's outbox, as [`bus::store`]
//! keeps messages; [`get`] reads them back by direct gets, which is all an ant's credentials allow, checking them
//! against the reference.

use std::time::Duration;

use ants_proto::{
    ATTACHMENT_CHUNK, ATTACHMENT_HEADER, Attachment, AttachmentHandle, FILES_STREAM, MAX_ATTACHMENTS, Uuid,
};
use anyhow::{Context as _, Result, bail, ensure};
use async_nats::jetstream;
use async_nats::jetstream::context::PublishErrorKind;
use async_nats::jetstream::message::PublishMessage;
use async_nats::jetstream::stream::DirectGetErrorKind;
use chrono::{DateTime, Utc};
use data_encoding::HEXLOWER;

use crate::bus::{self, Stored};

/// How long a reader waits for the missing chunks of a recent attachment ([`RECENT`]) to arrive: the hub's stream and
/// the attachments' store take what a leaf's outbox kept apart, so a message may come before its chunks.
pub const SETTLE: Duration = Duration::from_secs(60);
/// How old a message may be for a reader to wait for the missing chunks of its attachments ([`SETTLE`]).
pub const RECENT: Duration = Duration::from_hours(1);
/// How often a reader looks for the missing chunks of a recent attachment again.
const RETRY: Duration = Duration::from_millis(500);

/// The SHA-256 of `bytes` in lowercase hex, as [`Attachment::sha256`] holds it.
pub fn sha256(bytes: &[u8]) -> String {
    HEXLOWER.encode(ring::digest::digest(&ring::digest::SHA256, bytes).as_ref())
}

/// The time an attachment id holds ([`attachment_id`]) is rounded down to a multiple of this.
pub const ID_PERIOD: Duration = Duration::from_hours(1);

/// The reference of a new attachment of `bytes` named `name` of media type `mime`, with the id [`attachment_id`]
/// derives from them and the current [`ID_PERIOD`].
pub fn reference(name: &str, mime: &str, bytes: &[u8]) -> Result<Attachment> {
    let mut attachment = Attachment::new(name, mime, bytes.len() as u64, sha256(bytes))?;
    let now = u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default();
    let period = ID_PERIOD.as_millis() as u64;
    attachment.id = attachment_id(&attachment, now - now % period, 0);
    Ok(attachment)
}

/// The id of `attachment` made in the [`ID_PERIOD`] starting at `period` (Unix milliseconds), the `nth` of the same
/// file on one message: a UUIDv7 of that time whose other bits come from the SHA-256 of its name, media type and bytes'
/// SHA-256, `period` and `nth`. A message sent again in the same period, after a store refused part of the first try,
/// so stores its attachments under the same handles, where the chunks already stored are kept ([`put`]); a file's
/// bytes never differ under one handle, as the handle's id names their digest.
pub fn attachment_id(attachment: &Attachment, period: u64, nth: u32) -> Uuid {
    let Attachment { name, mime, sha256, .. } = attachment;
    let seed = format!("ants attachment\0{name}\0{mime}\0{sha256}\0{period}\0{nth}");
    let digest = ring::digest::digest(&ring::digest::SHA256, seed.as_bytes());
    let bits: [u8; 10] = digest.as_ref()[..10].try_into().expect("a SHA-256 has 32 bytes");
    uuid::Builder::from_unix_timestamp_millis(period, &bits).into_uuid()
}

/// Give each of `attachments`, those of one message, an id of its own: one repeating an earlier one's, the same file
/// attached again, takes the next [`attachment_id`] of its period not taken.
pub fn distinct<'a>(attachments: impl IntoIterator<Item = &'a mut Attachment>) {
    let mut taken = std::collections::HashSet::new();
    for attachment in attachments {
        let period = attachment.id.get_timestamp().map_or(0, |t| {
            let (secs, nanos) = t.to_unix();
            secs * 1000 + u64::from(nanos / 1_000_000)
        });
        let mut nth = 0;
        while !taken.insert(attachment.id) {
            nth += 1;
            attachment.id = attachment_id(attachment, period, nth);
        }
    }
}

/// The `Nats-Msg-Id` of chunk `n` under `handle`, the same for every copy of it, so the store keeps one.
pub fn chunk_id(handle: &AttachmentHandle, n: usize) -> String {
    format!("{handle}.{n}")
}

/// Store `bytes`, those of `attachment`, under `handle`, chunk by chunk, the first carrying the reference
/// ([`ATTACHMENT_HEADER`]), and say where. Each chunk is stored once: a chunk found there already was stored by an
/// earlier try, as only the sender writes under its handles and an id names the bytes it holds ([`attachment_id`]).
///
/// A chunk the hub's store does not take as its publish finds no stream (the leaf node's uplink is down) or none in
/// time is kept in the machine's outbox instead ([`AttachmentHandle::outbox_chunk_subject`]), for the hub to source
/// into the store once the uplink is back ([`Stored::Outbox`]), as [`bus::store`] keeps a message; send the message
/// carrying the attachment after this returns, so it follows its chunks. Every copy of a chunk carries the same
/// `Nats-Msg-Id`, so the store keeps one, should one that timed out have landed after all. The outbox copy expects
/// nothing of the store, as an expectation the store refused would stall the source.
pub async fn put(
    js: &jetstream::Context,
    handle: &AttachmentHandle,
    attachment: &Attachment,
    bytes: &[u8],
) -> Result<Stored> {
    attachment.check()?;
    ensure!(handle.id == attachment.id, "the handle {handle} names another attachment than {}", attachment.id);
    ensure!(
        bytes.len() as u64 == attachment.size && sha256(bytes) == attachment.sha256,
        "the bytes of {} differ from its reference",
        attachment.name
    );
    let reference = serde_json::to_string(attachment)?;
    // An empty file is one empty chunk, which carries the reference all the same.
    let chunks: Vec<&[u8]> = if bytes.is_empty() { vec![&[]] } else { bytes.chunks(ATTACHMENT_CHUNK).collect() };
    let message = |n: usize| {
        let message = PublishMessage::build().payload(chunks[n].to_vec().into()).message_id(chunk_id(handle, n));
        if n == 0 { message.header(ATTACHMENT_HEADER, reference.as_str()) } else { message }
    };
    let mut acks = Vec::new();
    for n in 0..chunks.len() {
        let subject = handle.chunk_subject(n as u64);
        let ack = js.send_publish(subject.clone(), message(n).expected_last_subject_sequence(0)).await;
        acks.push((n, subject, ack));
    }
    let mut unreachable = None;
    let mut kept = Vec::new();
    for (n, subject, ack) in acks {
        let stored = match ack {
            Ok(ack) => ack.await.map(drop),
            Err(e) => Err(e),
        };
        match stored {
            Ok(()) => {}
            Err(e) if e.kind() == PublishErrorKind::WrongLastSequence => {}
            Err(e) if bus::out_of_reach(&e) => {
                kept.push(n);
                unreachable.get_or_insert((subject, e));
            }
            Err(e) => {
                return Err(bus::explain(js, &subject, e).await)
                    .with_context(|| format!("storing attachment {}", attachment.name));
            }
        }
    }
    let Some((subject, unreachable)) = unreachable else {
        return Ok(Stored::Hub);
    };
    let uncaptured = unreachable.kind() == PublishErrorKind::StreamNotFound && bus::uncaptured(js).await;
    let mut acks = Vec::new();
    for n in kept {
        acks.push(js.send_publish(handle.outbox_chunk_subject(n as u64), message(n)).await);
    }
    for ack in acks {
        let kept = match ack {
            Ok(ack) => ack.await.map(drop),
            Err(e) => Err(e),
        };
        if let Err(e) = kept {
            let context = || format!("storing attachment {}", attachment.name);
            if uncaptured {
                return Err(bus::explained(unreachable, &subject, uncaptured)).with_context(context);
            }
            return Err(anyhow::anyhow!(e))
                .with_context(|| {
                    format!(
                        "the hub's attachments' store did not store {subject} ({unreachable}), nor did this machine's \
                         outbox (`ants leaf outbox` sets it up on a leaf node)"
                    )
                })
                .with_context(context);
        }
    }
    if uncaptured {
        bus::warn_uncaptured(&subject, "kept in this machine's outbox");
    } else {
        tracing::warn!(
            %handle,
            "the hub's attachments' store is out of reach ({unreachable}); kept in this machine's outbox"
        );
    }
    Ok(Stored::Outbox)
}

/// Read the bytes of the attachment under `handle`, and its reference as stored with them. Given `expected`, the
/// reference the message carrying it holds, the bytes must match that one. Chunks missing of an attachment made within
/// [`RECENT`] (by the time in its id) may still be on their way from a leaf's outbox: the read waits up to [`SETTLE`]
/// for them.
pub async fn get(
    js: &jetstream::Context,
    handle: &AttachmentHandle,
    expected: Option<&Attachment>,
) -> Result<(Attachment, Vec<u8>)> {
    get_stored(js, handle, expected, None).await
}

/// [`get`] of an attachment of a message the hub stored at `stored`: missing chunks are waited for when the attachment
/// or the message is recent, as a message from a leaf's outbox comes as late as the uplink is back.
pub async fn get_stored(
    js: &jetstream::Context,
    handle: &AttachmentHandle,
    expected: Option<&Attachment>,
    stored: Option<DateTime<Utc>>,
) -> Result<(Attachment, Vec<u8>)> {
    let wait = if recent(handle, stored, Utc::now()) { SETTLE } else { Duration::ZERO };
    get_within(js, handle, expected, wait).await
}

/// Whether the attachment under `handle`, made in the [`ID_PERIOD`] starting at the time in its id, or the message
/// carrying it, stored at `stored`, is within [`RECENT`] of `now`. A time ahead of `now`, from a clock running ahead,
/// is recent.
pub fn recent(handle: &AttachmentHandle, stored: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
    let period = handle.id.get_timestamp().and_then(|t| {
        let (secs, nanos) = t.to_unix();
        DateTime::from_timestamp(i64::try_from(secs).ok()?, nanos)
    });
    let made = period.map(|at| at + chrono::Duration::from_std(ID_PERIOD).expect("an hour"));
    let recent = chrono::Duration::from_std(RECENT).expect("an hour");
    made.into_iter().chain(stored).any(|at| now - at < recent)
}

/// [`get`], waiting up to `wait` for chunks missing.
pub async fn get_within(
    js: &jetstream::Context,
    handle: &AttachmentHandle,
    expected: Option<&Attachment>,
    wait: Duration,
) -> Result<(Attachment, Vec<u8>)> {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        match read(js, handle, expected).await {
            Err(e) if e.is::<Missing>() && tokio::time::Instant::now() + RETRY <= deadline => {
                tokio::time::sleep(RETRY).await;
            }
            read => return read,
        }
    }
}

/// A chunk of an attachment is not in the store.
#[derive(Debug)]
struct Missing(AttachmentHandle);

impl std::fmt::Display for Missing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "attachment {} is not stored: kept only as long as the messages, or not stored yet", self.0)
    }
}

impl std::error::Error for Missing {}

/// Whether `e` is a read's failing for a chunk not in the store ([`get`]).
pub fn missing(e: &anyhow::Error) -> bool {
    e.is::<Missing>()
}

/// When the hub stored `msg`, as [`get_stored`] takes it; None for one that is not JetStream's.
pub fn stored(msg: &jetstream::Message) -> Option<DateTime<Utc>> {
    msg.info().ok().map(|info| std::time::SystemTime::from(info.published).into())
}

async fn read(
    js: &jetstream::Context,
    handle: &AttachmentHandle,
    expected: Option<&Attachment>,
) -> Result<(Attachment, Vec<u8>)> {
    let store = js.get_stream_no_info(FILES_STREAM).await.context("opening the attachments' store")?;
    let chunk = async |n: u64| match store.direct_get_last_for_subject(handle.chunk_subject(n)).await {
        Ok(message) => Ok(message),
        Err(e) if e.kind() == DirectGetErrorKind::NotFound => bail!(Missing(handle.clone())),
        Err(e) => Err(e).with_context(|| format!("reading attachment {handle}")),
    };
    let first = chunk(0).await?;
    let stored =
        first.headers.get(ATTACHMENT_HEADER).with_context(|| format!("attachment {handle} has no reference"))?;
    let stored: Attachment = serde_json::from_str(stored.as_str())
        .with_context(|| format!("reading the reference of attachment {handle}"))?;
    stored.check()?;
    ensure!(stored.id == handle.id, "attachment {handle} holds attachment {}", stored.id);
    let attachment = expected.unwrap_or(&stored);
    let mut bytes = first.payload.to_vec();
    for n in 1..attachment.chunks() {
        ensure!(bytes.len() as u64 <= attachment.size, "attachment {handle} is longer than its reference");
        bytes.extend_from_slice(&chunk(n).await?.payload);
    }
    ensure!(
        bytes.len() as u64 == attachment.size && sha256(&bytes) == attachment.sha256,
        "attachment {handle} does not match its reference: it changed or is incomplete"
    );
    Ok((stored, bytes))
}

/// The media types of attachments a harness taking images is handed as images: those every model reading images
/// reads.
pub const PROMPT_IMAGES: [&str; 4] = ants_proto::IMAGE_MIMES;

/// The files attached to the message a prompt hands the harness, each with the handle the subject of that message
/// gives it. Their bytes are read only as the prompt runs, once it is known whether the harness takes images.
pub struct Attached {
    js: jetstream::Context,
    files: Vec<(AttachmentHandle, Attachment)>,
    /// When the hub stored the message, for the wait for chunks still on their way ([`get_stored`]).
    stored: Option<DateTime<Utc>>,
}

/// What a prompt shows of the files attached to its message ([`Attached::show`]).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Shown {
    /// A line for each attachment, naming it and how to read it, for the end of the prompt's text.
    pub lines: String,
    /// The images to hand the harness after the text, as media type and bytes.
    pub images: Vec<(String, Vec<u8>)>,
}

impl Attached {
    /// The `files` attached to a message the hub stored at `stored`, read from the store through `js`; None when
    /// there are none.
    pub fn new(
        js: jetstream::Context,
        files: Vec<(AttachmentHandle, Attachment)>,
        stored: Option<DateTime<Utc>>,
    ) -> Option<Self> {
        (!files.is_empty()).then_some(Self { js, files, stored })
    }

    /// What the prompt shows of the attachments: to a harness taking `images`, each image ([`PROMPT_IMAGES`]) that
    /// reads back as its reference says; for every attachment, a line naming it, its type, size and handle, and the
    /// read_attachment tool, or why it is unavailable.
    pub async fn show(self, images: bool) -> Shown {
        let mut shown = Shown::default();
        let mut lines = Vec::new();
        for (handle, attachment) in self.files.into_iter().take(MAX_ATTACHMENTS) {
            let what = named(&handle, &attachment);
            let mime = &attachment.mime;
            if let Err(e) = attachment.check() {
                lines.push(format!("{what}: unavailable ({e})."));
                continue;
            }
            if !images || !PROMPT_IMAGES.contains(&mime.as_str()) {
                lines.push(format!("{what}; call read_attachment with the handle to read it."));
                continue;
            }
            match get_stored(&self.js, &handle, Some(&attachment), self.stored).await {
                Ok((_, bytes)) => {
                    lines.push(format!("{what}; shown as an image after this text."));
                    shown.images.push((mime.clone(), bytes));
                }
                Err(e) => {
                    tracing::warn!(%handle, error = format!("{e:#}"), "reading an attachment for a prompt");
                    lines.push(format!(
                        "{what}: unavailable now ({e:#}); call read_attachment with the handle to try again."
                    ));
                }
            }
        }
        shown.lines = lines.join("\n");
        shown
    }
}

/// How a prompt names `attachment`, under `handle`: its name, media type, size and handle.
fn named(handle: &AttachmentHandle, attachment: &Attachment) -> String {
    let Attachment { name, mime, size, .. } = attachment;
    format!("[ants] Attached: {name:?} ({mime}, {size} bytes), handle {handle}")
}

/// The lines a prompt shows under an earlier message or post of the thread for its `attachments`, each with the handle
/// `handle_of` gives its id, those of the subject of that message or post: one per attachment, naming it and the
/// read_attachment tool, or why it is unavailable. Nothing is read from the store.
pub fn earlier_lines(attachments: &[Attachment], handle_of: impl Fn(Uuid) -> AttachmentHandle) -> Vec<String> {
    let mut ids = std::collections::HashSet::new();
    attachments
        .iter()
        .take(MAX_ATTACHMENTS)
        .map(|attachment| {
            let what = named(&handle_of(attachment.id), attachment);
            match attachment.check() {
                Err(e) => format!("{what}: unavailable ({e})."),
                Ok(()) if !ids.insert(attachment.id) => format!("{what}: unavailable (its id is repeated)."),
                Ok(()) => format!("{what}; call read_attachment with the handle to read it."),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use ants_proto::AttachmentScope;
    use ants_test_support::NatsServer;

    use super::*;
    use crate::{bus, hub};

    #[tokio::test]
    async fn attachments_come_back_as_stored_and_only_once() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &ants_test_support::human(), "test").await.unwrap();
        js.create_stream(hub::files_config()).await.unwrap();
        let handle_of = |attachment: &Attachment| AttachmentHandle {
            from: "alice".parse().unwrap(),
            scope: AttachmentScope::Inbox("dev-a".parse().unwrap()),
            id: attachment.id,
        };

        let bytes: Vec<u8> = (0..ATTACHMENT_CHUNK * 2 + 10).map(|i| i as u8).collect();
        let big = reference("ünïcode – log.txt", "text/plain", &bytes).unwrap();
        put(&js, &handle_of(&big), &big, &bytes).await.unwrap();
        put(&js, &handle_of(&big), &big, &bytes).await.expect("a repeated put stores nothing");
        let (stored, read) = get(&js, &handle_of(&big), Some(&big)).await.unwrap();
        assert_eq!((stored, read.len()), (big.clone(), bytes.len()));
        assert_eq!(read, bytes);
        assert_eq!(get(&js, &handle_of(&big), None).await.unwrap().1, bytes, "the first chunk holds the reference");

        let empty = reference("empty", Attachment::UNKNOWN_MIME, b"").unwrap();
        put(&js, &handle_of(&empty), &empty, b"").await.unwrap();
        assert_eq!(get(&js, &handle_of(&empty), Some(&empty)).await.unwrap(), (empty.clone(), Vec::new()));

        let other = reference("other", "text/plain", b"other bytes").unwrap();
        let lying = Attachment { id: big.id, ..other.clone() };
        assert!(get(&js, &handle_of(&big), Some(&lying)).await.is_err(), "the bytes match the message's reference");
        assert!(put(&js, &handle_of(&other), &other, b"changed").await.is_err());
        let missing = get_within(&js, &handle_of(&other), None, Duration::ZERO).await.unwrap_err().to_string();
        assert!(missing.contains("is not stored"), "{missing}");

        // Chunks on their way from a leaf's outbox are waited for, those of an old attachment not.
        let late = reference("late.txt", "text/plain", b"late").unwrap();
        let reading = tokio::spawn({
            let (js, handle, late) = (js.clone(), handle_of(&late), late.clone());
            async move { get(&js, &handle, Some(&late)).await }
        });
        tokio::time::sleep(RETRY * 2).await;
        assert_eq!(put(&js, &handle_of(&late), &late, b"late").await.unwrap(), Stored::Hub);
        assert_eq!(reading.await.unwrap().unwrap().1, b"late");
        let old = uuid::Uuid::new_v7(uuid::Timestamp::from_unix(uuid::NoContext, 1_700_000_000, 0));
        let started = std::time::Instant::now();
        assert!(get(&js, &handle_of(&Attachment { id: old, ..other }), None).await.is_err());
        assert!(started.elapsed() < SETTLE / 2, "not waited for");

        // Unless the message carrying it was stored just now, as one kept in a leaf's outbox through a long outage.
        let kept = Attachment { id: old, ..reference("kept.txt", "text/plain", b"kept").unwrap() };
        let reading = tokio::spawn({
            let (js, handle, kept) = (js.clone(), handle_of(&kept), kept.clone());
            async move { get_stored(&js, &handle, Some(&kept), Some(Utc::now())).await }
        });
        tokio::time::sleep(RETRY * 2).await;
        put(&js, &handle_of(&kept), &kept, b"kept").await.unwrap();
        assert_eq!(reading.await.unwrap().unwrap().1, b"kept");
    }

    #[test]
    fn an_attachment_id_names_the_file_and_its_period() {
        let shot = reference("shot.png", "image/png", b"png").unwrap();
        assert_eq!(shot.id.get_version_num(), 7);
        assert_eq!(reference("shot.png", "image/png", b"png").unwrap().id, shot.id, "the same file, the same id");
        for other in [
            reference("shot.png", "image/png", b"other").unwrap(),
            reference("other.png", "image/png", b"png").unwrap(),
            reference("shot.png", "image/webp", b"png").unwrap(),
        ] {
            assert_ne!(other.id, shot.id, "{other:?}");
        }
        let period = ID_PERIOD.as_millis() as u64;
        let (secs, nanos) = shot.id.get_timestamp().unwrap().to_unix();
        let at = secs * 1000 + u64::from(nanos / 1_000_000);
        assert_eq!(at % period, 0, "the period's start");
        assert_ne!(attachment_id(&shot, at + period, 0), shot.id, "another period");

        let mut twice = [shot.clone(), reference("log.txt", "text/plain", b"log").unwrap(), shot.clone()];
        distinct(twice.iter_mut());
        assert_eq!((twice[0].id, twice[2].id), (shot.id, attachment_id(&shot, at, 1)));
        let mut again = [shot.clone(), shot.clone(), shot];
        distinct(again.iter_mut());
        assert_eq!(again[1].id, twice[2].id, "the same each time");
        assert_eq!(again.iter().map(|a| a.id).collect::<std::collections::HashSet<_>>().len(), 3);
    }

    /// A store too full for an attachment keeps the chunks it took; the same file sent again stores only the rest.
    #[tokio::test]
    async fn an_attachment_sent_again_after_a_full_store_stores_no_second_copy() {
        let Some(server) = NatsServer::spawn_with(|dir| {
            format!("jetstream {{ store_dir: {:?}, max_file_store: 1MB }}\n", dir.join("js"))
        }) else {
            return;
        };
        let (_, js) = bus::connect(&server.url, None, None, &ants_test_support::human(), "test").await.unwrap();
        let files = js.create_stream(hub::files_config()).await.unwrap();
        let filler = jetstream::stream::Config {
            name: "FILLER".to_owned(),
            subjects: vec!["filler".to_owned()],
            ..Default::default()
        };
        js.create_stream(filler).await.unwrap();
        js.publish("filler", vec![b'f'; 600_000].into()).await.unwrap().await.unwrap();
        let bytes: Vec<u8> = (0..ATTACHMENT_CHUNK * 5).map(|i| i as u8).collect();
        let handle_of = |attachment: &Attachment| AttachmentHandle {
            from: "alice".parse().unwrap(),
            scope: AttachmentScope::Inbox("dev-a".parse().unwrap()),
            id: attachment.id,
        };

        let first = reference("big.bin", Attachment::UNKNOWN_MIME, &bytes).unwrap();
        let refused = put(&js, &handle_of(&first), &first, &bytes).await.unwrap_err();
        assert!(bus::is_store_full(refused.as_ref()), "{refused:#}");
        let kept = files.get_info().await.unwrap().state.messages;
        assert!(kept < 5, "{kept} chunks taken");

        js.delete_stream("FILLER").await.unwrap();
        let again = reference("big.bin", Attachment::UNKNOWN_MIME, &bytes).unwrap();
        assert_eq!(again.id, first.id, "the same handle");
        put(&js, &handle_of(&again), &again, &bytes).await.unwrap();
        assert_eq!(files.get_info().await.unwrap().state.messages, 5, "one copy of each chunk");
        assert_eq!(get(&js, &handle_of(&again), Some(&again)).await.unwrap().1, bytes);
    }

    #[test]
    fn missing_chunks_are_waited_for_when_the_attachment_or_its_message_is_recent() {
        let now = Utc::now();
        let at = |ago: chrono::Duration| {
            let at = now - ago;
            let id = uuid::Uuid::new_v7(uuid::Timestamp::from_unix(
                uuid::NoContext,
                at.timestamp().try_into().unwrap(),
                at.timestamp_subsec_nanos(),
            ));
            AttachmentHandle {
                from: "alice".parse().unwrap(),
                scope: AttachmentScope::Inbox("dev-a".parse().unwrap()),
                id,
            }
        };
        let (minute, day) = (chrono::Duration::minutes(1), chrono::Duration::days(1));
        assert!(recent(&at(minute), None, now));
        assert!(recent(&at(-minute), None, now), "a clock ahead");
        assert!(!recent(&at(day), None, now));
        let period = chrono::Duration::from_std(ID_PERIOD).unwrap();
        assert!(recent(&at(minute + period), None, now), "made late in the period its id names");
        assert!(!recent(&at(minute * 2 + period * 2), None, now));
        assert!(recent(&at(day), Some(now - minute), now), "kept in an outbox for a day, stored just now");
        assert!(!recent(&at(day), Some(now - day), now));
    }
}
