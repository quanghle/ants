//! The bytes admin orders and their outcomes carry by reference ([`AdminPayload`]), stored in the attachments' store
//! as chunks: an export of an ant's provider, which an export order's daemon stores for the admin to read, and an
//! import order's admin stores for the daemon.

use ants_proto::{ATTACHMENT_CHUNK, AdminPayload, FILES_STREAM, MAX_ADMIN_PAYLOAD, Machine, Uuid};
use anyhow::{Context as _, Result, bail, ensure};
use async_nats::jetstream;
use async_nats::jetstream::context::PublishErrorKind;
use async_nats::jetstream::message::PublishMessage;
use async_nats::jetstream::stream::DirectGetErrorKind;

use crate::bus;
use crate::files::sha256;

/// Store `bytes` for an order to `machine`, chunk by chunk under a fresh id, and say where.
pub async fn put(js: &jetstream::Context, machine: &Machine, bytes: &[u8]) -> Result<AdminPayload> {
    let size = bytes.len() as u64;
    ensure!(size <= MAX_ADMIN_PAYLOAD, "{size} bytes are over the {MAX_ADMIN_PAYLOAD} an admin order carries");
    let payload = AdminPayload { id: Uuid::now_v7(), machine: machine.clone(), size, sha256: sha256(bytes) };
    let chunks: Vec<&[u8]> = if bytes.is_empty() { vec![&[]] } else { bytes.chunks(ATTACHMENT_CHUNK).collect() };
    let mut acks = Vec::new();
    for (n, chunk) in chunks.into_iter().enumerate() {
        let subject = payload.chunk_subject(n as u64);
        let message = PublishMessage::build()
            .payload(chunk.to_vec().into())
            .message_id(subject.clone())
            .expected_last_subject_sequence(0);
        acks.push((subject.clone(), js.send_publish(subject, message).await));
    }
    for (subject, ack) in acks {
        let stored = match ack {
            Ok(ack) => ack.await.map(drop),
            Err(e) => Err(e),
        };
        match stored {
            Ok(()) => {}
            Err(e) if e.kind() == PublishErrorKind::WrongLastSequence => {}
            Err(e) => return Err(bus::explain(js, &subject, e).await).context("storing an admin order's payload"),
        }
    }
    Ok(payload)
}

/// The bytes stored at `payload`, checked against it.
pub async fn get(js: &jetstream::Context, payload: &AdminPayload) -> Result<Vec<u8>> {
    ensure!(payload.size <= MAX_ADMIN_PAYLOAD, "payload {} is over the {MAX_ADMIN_PAYLOAD} bytes allowed", payload.id);
    let store = js.get_stream_no_info(FILES_STREAM).await.context("opening the attachments' store")?;
    let mut bytes = Vec::with_capacity(payload.size as usize);
    for n in 0..payload.chunks() {
        let chunk = match store.direct_get_last_for_subject(payload.chunk_subject(n)).await {
            Ok(message) => message.payload,
            Err(e) if e.kind() == DirectGetErrorKind::NotFound => {
                bail!("payload {} is gone from the attachments' store, or was never stored whole", payload.id)
            }
            Err(e) => return Err(e).with_context(|| format!("reading payload {}", payload.id)),
        };
        bytes.extend_from_slice(&chunk);
        ensure!(bytes.len() as u64 <= payload.size, "payload {} is longer than its reference", payload.id);
    }
    ensure!(
        bytes.len() as u64 == payload.size && sha256(&bytes) == payload.sha256,
        "payload {} does not match its reference: it changed or is incomplete",
        payload.id
    );
    Ok(bytes)
}
