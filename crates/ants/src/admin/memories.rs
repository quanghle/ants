//! `ants ant memories`: an ant's memories, which an admin order asks the daemon of its machine to read
//! ([`AdminOp::ReadMemories`]): its core memories (whoami, memory, tasks), which a set order edits, and its harness's
//! memory files and bd memories, read-only, and what its provider remembers of it, when the provider shows that.

use std::fmt::Write as _;
use std::path::Path;
use std::time::Duration;

use ants_daemon::records::direct_kv::{DirectKv, Lookup};
use ants_proto::{AdminOp, AntIdentity, IDENTITIES_BUCKET, Machine, Memories, OrderState, UserId};
use anyhow::{Result, bail};
use async_nats::jetstream;

use crate::admin::orders;
use crate::connection::Nats;
use crate::messages::attachments::size;

/// How long the CLI waits for the daemon's answer.
pub const WAIT: Duration = Duration::from_secs(15);

/// Ask the daemon of `machine`, else of the machine that last recorded serving `ant`, for the memories of `ant`, with
/// the admin credentials of `nats`, waiting up to [`WAIT`] for its answer.
pub async fn read(
    nats: &Nats<'_>,
    js: &jetstream::Context,
    machine: Option<Machine>,
    ant: &UserId,
) -> Result<Memories> {
    let op = AdminOp::ReadMemories { identity: identity_of(nats, js, ant).await };
    let (order, outcome) = orders::send(nats, js, machine, ant, op, WAIT).await?;
    let Some(outcome) = outcome else {
        bail!(
            "the daemon of {} did not answer within {}s: memories are read only while it is online, and the order is \
             refused as stale once it waited {}m",
            order.machine,
            WAIT.as_secs(),
            ants_proto::MEMORIES_FRESH.num_minutes()
        );
    };
    match (outcome.state, outcome.result) {
        (OrderState::Applied, Some(result)) => Ok(serde_json::from_value(result)?),
        (OrderState::Applied, None) => bail!("the daemon of {} answered without memories", order.machine),
        (_, _) => bail!(
            "the daemon of {} refused to read the memories of {ant}: {}",
            order.machine,
            outcome.reason.as_deref().unwrap_or("no reason given")
        ),
    }
}

/// The id of the identity record of `ant`, which its provider knows it by and only humans read: None, the daemon
/// noting the provider's memories unread, when it cannot be read.
async fn identity_of(nats: &Nats<'_>, js: &jetstream::Context, ant: &UserId) -> Option<String> {
    let kv = DirectKv::whole(js, nats.domain, IDENTITIES_BUCKET).await.ok()?;
    match kv.lookup_last(ant.as_str()).await.ok()?.0 {
        Lookup::Value(value) => serde_json::from_slice::<AntIdentity>(&value).ok().map(|record| record.id),
        Lookup::NoBucket | Lookup::NoValue => None,
    }
}

/// `ants ant memories ANT`: the memories of `ant` as text, or with `json` as one JSON object.
pub async fn show(nats: &Nats<'_>, ant: &UserId, machine: Option<Machine>, json: bool) -> Result<()> {
    let (_, js) = nats.connect("ants-memories").await?;
    let memories = read(nats, &js, machine, ant).await?;
    if json {
        println!("{}", serde_json::to_string(&memories)?);
    } else {
        print!("{}", render(ant, &memories));
    }
    Ok(())
}

/// The memories of `ant` as text: the core ones, the harness's files, the bd memories, its provider's and what was
/// left out.
pub fn render(ant: &UserId, memories: &Memories) -> String {
    let mut out = format!("core memories of {ant} (a set order of whoami, memory or tasks edits them)\n");
    let core = &memories.core;
    for (name, text) in [("whoami", &core.whoami), ("memory", &core.memory), ("tasks", &core.tasks)] {
        match text {
            Some(text) => _ = write!(out, "{name}\n{}", indent(text)),
            None => _ = writeln!(out, "{name} (unset)"),
        }
    }
    let workdir = memories.workdir.as_deref().map(Path::new);
    match &memories.harness {
        Some(harness) => _ = writeln!(out, "\nmemory files of harness {harness}"),
        None => out.push_str("\nmemory files\n"),
    }
    if memories.files.is_empty() {
        out.push_str("  none found\n");
    }
    for file in &memories.files {
        let path = Path::new(&file.path);
        let path = workdir.and_then(|dir| path.strip_prefix(dir).ok()).unwrap_or(path);
        let cut = if file.truncated { ", cut short" } else { "" };
        _ = write!(out, "── {} ({}{cut})\n{}", path.display(), size(file.size), indent(&file.text));
    }
    if let Some(bd) = &memories.bd {
        _ = writeln!(out, "\nbd memories of {}", bd.db);
        if let Some(error) = &bd.error {
            _ = writeln!(out, "  {error}");
        } else if bd.memories.is_empty() {
            out.push_str("  none\n");
        }
        for memory in &bd.memories {
            let cut = if memory.truncated { " (cut short)" } else { "" };
            _ = write!(out, "── {}{cut}\n{}", memory.key, indent(&memory.content));
        }
    }
    if let Some(provider) = &memories.provider {
        _ = writeln!(out, "\nmemories of provider {}", provider.provider);
        if let Some(error) = &provider.error {
            _ = writeln!(out, "  {error}");
        } else if provider.memories.is_empty() {
            out.push_str("  none\n");
        }
        for memory in &provider.memories {
            let cut = if memory.truncated { " (cut short)" } else { "" };
            _ = write!(out, "── {}{cut}\n{}", memory.key, indent(&memory.content));
        }
    }
    if !memories.skipped.is_empty() {
        out.push_str("\nleft out\n");
        for skipped in &memories.skipped {
            _ = writeln!(out, "  {}: {}", skipped.what, skipped.why);
        }
    }
    out
}

fn indent(text: &str) -> String {
    let mut out = String::new();
    for line in text.lines() {
        out.push_str("  ");
        out.push_str(line);
        out.push('\n');
    }
    if out.is_empty() {
        out.push_str("  (empty)\n");
    }
    out
}
