//! The bytes of attachments in their store ([`FILES_STREAM`]): [`put`] stores those of an attachment as chunks under
//! its handle before the message carrying it is sent, [`get`] reads them back by direct gets, which is all an ant's
//! credentials allow, checking them against the reference.

use ants_proto::{ATTACHMENT_CHUNK, ATTACHMENT_HEADER, Attachment, AttachmentHandle, FILES_STREAM, MAX_ATTACHMENTS};
use anyhow::{Context as _, Result, bail, ensure};
use async_nats::jetstream;
use async_nats::jetstream::context::PublishErrorKind;
use async_nats::jetstream::message::PublishMessage;
use async_nats::jetstream::stream::DirectGetErrorKind;
use data_encoding::HEXLOWER;

/// The SHA-256 of `bytes` in lowercase hex, as [`Attachment::sha256`] holds it.
pub fn sha256(bytes: &[u8]) -> String {
    HEXLOWER.encode(ring::digest::digest(&ring::digest::SHA256, bytes).as_ref())
}

/// The reference of a new attachment of `bytes` named `name` of media type `mime`, with a fresh id.
pub fn reference(name: &str, mime: &str, bytes: &[u8]) -> Result<Attachment> {
    Ok(Attachment::new(name, mime, bytes.len() as u64, sha256(bytes))?)
}

/// Store `bytes`, those of `attachment`, under `handle`, chunk by chunk, the first carrying the reference
/// ([`ATTACHMENT_HEADER`]). Each chunk is stored once: a chunk found there already was stored by an earlier try, as
/// only the sender writes under its handles and each attachment has a fresh id.
pub async fn put(
    js: &jetstream::Context,
    handle: &AttachmentHandle,
    attachment: &Attachment,
    bytes: &[u8],
) -> Result<()> {
    attachment.check()?;
    ensure!(handle.id == attachment.id, "the handle {handle} names another attachment than {}", attachment.id);
    ensure!(
        bytes.len() as u64 == attachment.size && sha256(bytes) == attachment.sha256,
        "the bytes of {} differ from its reference",
        attachment.name
    );
    let mut acks = Vec::new();
    // An empty file is one empty chunk, which carries the reference all the same.
    let chunks: Vec<&[u8]> = if bytes.is_empty() { vec![&[]] } else { bytes.chunks(ATTACHMENT_CHUNK).collect() };
    for (n, chunk) in chunks.into_iter().enumerate() {
        let mut message = PublishMessage::build().payload(chunk.to_vec().into()).expected_last_subject_sequence(0);
        if n == 0 {
            message = message.header(ATTACHMENT_HEADER, serde_json::to_string(attachment)?.as_str());
        }
        let subject = handle.chunk_subject(n as u64);
        let ack = js.send_publish(subject.clone(), message).await;
        acks.push((subject, ack));
    }
    for (subject, ack) in acks {
        let stored = match ack {
            Ok(ack) => ack.await.map(drop),
            Err(e) => Err(e),
        };
        match stored {
            Err(e) if e.kind() == PublishErrorKind::WrongLastSequence => {}
            Err(e) => {
                return Err(crate::bus::explain(js, &subject, e).await)
                    .with_context(|| format!("storing attachment {}", attachment.name));
            }
            Ok(()) => {}
        }
    }
    Ok(())
}

/// Read the bytes of the attachment under `handle`, and its reference as stored with them. Given `expected`, the
/// reference the message carrying it holds, the bytes must match that one.
pub async fn get(
    js: &jetstream::Context,
    handle: &AttachmentHandle,
    expected: Option<&Attachment>,
) -> Result<(Attachment, Vec<u8>)> {
    let store = js.get_stream_no_info(FILES_STREAM).await.context("opening the attachments' store")?;
    let chunk = async |n: u64| match store.direct_get_last_for_subject(handle.chunk_subject(n)).await {
        Ok(message) => Ok(message),
        Err(e) if e.kind() == DirectGetErrorKind::NotFound => {
            bail!("attachment {handle} is not stored: kept only as long as the messages, or not stored yet")
        }
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
pub const PROMPT_IMAGES: [&str; 4] = ["image/png", "image/jpeg", "image/gif", "image/webp"];

/// The files attached to the message a prompt hands the harness, each with the handle the subject of that message
/// gives it. Their bytes are read only as the prompt runs, once it is known whether the harness takes images.
pub struct Attached {
    js: jetstream::Context,
    files: Vec<(AttachmentHandle, Attachment)>,
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
    /// The `files` attached to a message, read from the store through `js`; None when there are none.
    pub fn new(js: jetstream::Context, files: Vec<(AttachmentHandle, Attachment)>) -> Option<Self> {
        (!files.is_empty()).then_some(Self { js, files })
    }

    /// What the prompt shows of the attachments: to a harness taking `images`, each image ([`PROMPT_IMAGES`]) that
    /// reads back as its reference says; for every attachment, a line naming it, its type, size and handle, and the
    /// read_attachment tool, or why it is unavailable.
    pub async fn show(self, images: bool) -> Shown {
        let mut shown = Shown::default();
        let mut lines = Vec::new();
        for (handle, attachment) in self.files.into_iter().take(MAX_ATTACHMENTS) {
            let Attachment { name, mime, size, .. } = &attachment;
            let what = format!("[ants] Attached: {name:?} ({mime}, {size} bytes), handle {handle}");
            if let Err(e) = attachment.check() {
                lines.push(format!("{what}: unavailable ({e})."));
                continue;
            }
            if !images || !PROMPT_IMAGES.contains(&mime.as_str()) {
                lines.push(format!("{what}; call read_attachment with the handle to read it."));
                continue;
            }
            match get(&self.js, &handle, Some(&attachment)).await {
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

#[cfg(test)]
mod tests {
    use ants_proto::{AttachmentScope, UserId};
    use ants_test_support::NatsServer;

    use super::*;
    use crate::{bus, hub};

    #[tokio::test]
    async fn attachments_come_back_as_stored_and_only_once() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
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
        let missing = get(&js, &handle_of(&other), None).await.unwrap_err().to_string();
        assert!(missing.contains("is not stored"), "{missing}");
    }
}
