//! Dead letters replayed and dropped, as `ants dead` and `ants chat` do.

use ants_daemon::delivery::dead;
use anyhow::{Context as _, Result};
use async_nats::jetstream;

pub(crate) async fn stream(js: &jetstream::Context) -> Result<jetstream::stream::Stream> {
    js.get_stream(ants_proto::STREAM).await.context("opening the ANTS stream")
}

/// Send the message of dead letter `seq` to its inbox again, as it was, and remove the dead letter, as `ants dead
/// replay` does: the line it prints.
pub(crate) async fn replayed(js: &jetstream::Context, seq: u64) -> Result<String> {
    let envelope = dead::replay(js, &stream(js).await?, seq).await?;
    Ok(format!("replayed to {} on thread {}", envelope.to, envelope.thread_id))
}

/// Remove dead letter `seq` without replaying it, as `ants dead drop` does: the line it prints.
pub(crate) async fn dropped(js: &jetstream::Context, seq: u64) -> Result<String> {
    dead::discard(&stream(js).await?, seq).await?;
    Ok(format!("dropped dead letter {seq}"))
}
