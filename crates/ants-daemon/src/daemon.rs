//! The inbox loop of one persona.

use std::path::PathBuf;
use std::time::Duration;

use ants_proto::{Envelope, Error as ProtoError, PersonaId, subject};
use anyhow::Result;
use async_nats::jetstream::{self, AckKind};
use futures::StreamExt as _;

use crate::bus;
use crate::harness::{self, HarnessKind, PermissionPolicy};

/// Redelivery window. The daemon extends it with progress acks while a prompt runs, so a crash mid-task redelivers.
const ACK_WAIT: Duration = Duration::from_secs(5 * 60);
const PROGRESS_EVERY: Duration = Duration::from_secs(60);
/// Deliveries before a message is given up on.
const MAX_DELIVER: i64 = 5;
const RETRY_DELAY: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct Config {
    pub persona: PersonaId,
    pub nats_url: String,
    /// Credentials file of the persona, when the server requires them.
    pub creds: Option<PathBuf>,
    /// Directory the harness works in.
    pub workdir: PathBuf,
    pub harness: HarnessKind,
    pub permissions: PermissionPolicy,
}

/// Serve the persona's inbox until Ctrl-C. A message in hand is finished first.
pub async fn run(config: Config) -> Result<()> {
    let (client, js) =
        bus::connect(&config.nats_url, config.creds.as_deref(), &config.persona, &format!("antsd-{}", config.persona))
            .await?;
    let stream = bus::ensure_stream(&js).await?;
    let mut messages = bus::inbox_consumer(&stream, &config.persona, ACK_WAIT, MAX_DELIVER).await?;
    tracing::info!(persona = %config.persona, harness = %config.harness, workdir = %config.workdir.display(), "serving inbox");

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            next = messages.next() => match next {
                None => break,
                Some(Err(e)) => tracing::warn!(error = %e, "inbox stream"),
                Some(Ok(msg)) => {
                    if let Err(e) = handle(&client, &js, &config, msg).await {
                        tracing::error!(error = format!("{e:#}"), "handling message");
                    }
                }
            }
        }
    }
    tracing::info!("stopped");
    Ok(())
}

async fn handle(
    client: &async_nats::Client,
    js: &jetstream::Context,
    config: &Config,
    msg: jetstream::Message,
) -> Result<()> {
    let envelope: Envelope = match serde_json::from_slice(&msg.payload) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(error = %e, subject = %msg.subject, "dropping malformed message");
            return msg.ack_with(AckKind::Term).await.map_err(|e| anyhow::anyhow!(e));
        }
    };
    if let Err(ProtoError::BudgetExhausted(_)) = envelope.reply(config.persona.clone(), "") {
        // Out of turns: hand the thread to the human instead of spending another prompt.
        let mut escalation = Envelope::on_thread(
            envelope.thread_id.clone(),
            config.persona.clone(),
            PersonaId::human(),
            format!("Turn budget spent; needs a decision. Last message from {}:\n{}", envelope.from, envelope.text),
        );
        escalation.in_reply_to = Some(envelope.id);
        bus::publish(js, &escalation).await?;
        return msg.double_ack().await.map_err(|e| anyhow::anyhow!(e));
    }

    let status = subject::status(&config.persona, &envelope.thread_id);
    tracing::info!(thread = %envelope.thread_id, from = %envelope.from, "prompting {}", config.harness);
    client.publish(status.clone(), "working".into()).await?;

    let prompt = harness::prompt_once(config.harness, &config.workdir, &envelope.text, config.permissions);
    tokio::pin!(prompt);
    let mut progress = tokio::time::interval(PROGRESS_EVERY);
    progress.tick().await;
    let result = loop {
        tokio::select! {
            r = &mut prompt => break r,
            _ = progress.tick() => {
                if let Err(e) = msg.ack_with(AckKind::Progress).await {
                    tracing::warn!(error = %e, "progress ack");
                }
            }
        }
    };

    let text = match result {
        Ok(text) => text,
        Err(e) => {
            client.publish(status, "failed".into()).await?;
            msg.ack_with(AckKind::Nak(Some(RETRY_DELAY))).await.map_err(|e| anyhow::anyhow!(e))?;
            return Err(e);
        }
    };

    let reply = envelope.reply(config.persona.clone(), text)?;
    // Store the reply before acking: a crash in between redelivers rather than loses the message.
    bus::publish(js, &reply).await?;
    msg.double_ack().await.map_err(|e| anyhow::anyhow!(e))?;
    client.publish(status, "idle".into()).await?;
    Ok(())
}
