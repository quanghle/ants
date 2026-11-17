//! The part of an export the ant's provider holds ([`Export::provider`]): exported and imported by admin orders to the
//! daemon of this machine ([`AdminOp::ExportProvider`], [`AdminOp::ImportProvider`]), as only a daemon reaches the
//! providers of its ants. The provider's export travels through the attachments' store, by reference
//! ([`AdminPayload`]), never through the order or its outcome.

use std::path::Path;
use std::time::Duration;

use ants_daemon::admin::payload;
use ants_proto::provider::Export as ProviderPart;
use ants_proto::{AdminOp, Machine, OrderState, ProviderExport, UserId};
use anyhow::{Context as _, Result, bail, ensure};
use async_nats::jetstream;

#[cfg(doc)]
use super::Export;
#[cfg(doc)]
use ants_proto::AdminPayload;

use crate::admin::orders;
use crate::connection::Nats;

/// How long the CLI waits for the daemon to export or import the provider's part.
const WAIT: Duration = Duration::from_secs(90);
const LEAVE_OUT: &str = "--no-provider exports the rest without it";

/// This machine's name, as the config file at `path` and its daemon take it.
pub async fn machine(path: &Path) -> Result<Machine> {
    let loaded = ants_daemon::config::load(path)?;
    Ok(loaded.host.named(&ants_daemon::colony::machine().await))
}

/// The export of the provider of `ant`, whose identity record's id is `identity`, by the daemon of `machine`: none,
/// with a note, when the ant has no provider or its provider exports nothing.
pub async fn export(
    nats: &Nats<'_>,
    js: &jetstream::Context,
    machine: Machine,
    ant: &UserId,
    identity: &str,
) -> Result<Option<ProviderPart>> {
    let op = AdminOp::ExportProvider { identity: identity.to_owned() };
    let (order, outcome) = orders::send(nats, js, Some(machine), ant, op, WAIT)
        .await
        .with_context(|| format!("asking for the export of the provider of {ant} ({LEAVE_OUT})"))?;
    let Some(outcome) = outcome else {
        bail!(
            "the daemon of {} did not answer within {}s: it exports the provider's part of {ant} only while online, \
             and the order is refused as stale once it waited {}m; {LEAVE_OUT}",
            order.machine,
            WAIT.as_secs(),
            ants_proto::MEMORIES_FRESH.num_minutes()
        );
    };
    if outcome.state != OrderState::Applied {
        bail!(
            "the daemon of {} refused to export the provider's part of {ant}: {}; {LEAVE_OUT}",
            order.machine,
            orders::reason(&outcome)
        );
    }
    let result: ProviderExport = serde_json::from_value(outcome.result.unwrap_or_default())
        .context("reading the outcome of the provider's export")?;
    let (name, stored) = match (result.provider, result.payload) {
        (None, _) => {
            eprintln!("note: {ant} has no provider: the export carries no provider part");
            return Ok(None);
        }
        (Some(name), None) => {
            eprintln!("note: provider {name} of {ant} exports nothing: the export carries no provider part");
            return Ok(None);
        }
        (Some(name), Some(stored)) => (name, stored),
    };
    let bytes = payload::get(js, &stored).await.context("reading the provider's export")?;
    let part: ProviderPart = serde_json::from_slice(&bytes).context("reading the provider's export")?;
    ensure!(part.provider == name, "provider {name} of {ant} exported as provider {}", part.provider);
    Ok(Some(part))
}

/// Hand `part` to the provider of `ant`, whose identity record's id is `identity`, through the daemon of `machine`:
/// what it came to, told as a line for the human; an error only when the order cannot be sent.
pub async fn import(
    nats: &Nats<'_>,
    js: &jetstream::Context,
    machine: Machine,
    ant: &UserId,
    identity: &str,
    part: &ProviderPart,
) -> Result<Imported> {
    let admin = orders::Admin::of(nats)?;
    let bytes = serde_json::to_vec(part)?;
    let stored = payload::put(js, &machine, &bytes).await.context("storing the provider's part for the daemon")?;
    let op = AdminOp::ImportProvider { identity: identity.to_owned(), payload: stored };
    let (order, outcome) = admin.send(js, Some(machine), ant, op, WAIT).await?;
    Ok(match outcome {
        None => Imported::Pending(orders::pending_note(&order)),
        Some(outcome) if outcome.state == OrderState::Applied => Imported::Done,
        Some(outcome) => Imported::Refused(format!(
            "the daemon of {} refused the part of provider {} of {ant}: {}",
            order.machine,
            part.provider,
            orders::reason(&outcome)
        )),
    })
}

/// What an import of the provider's part came to.
#[derive(Debug, PartialEq, Eq)]
pub enum Imported {
    Done,
    /// The daemon has not decided the order yet, which it carries out once it takes it.
    Pending(String),
    Refused(String),
}
