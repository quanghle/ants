//! `ants ant memories`: an ant's memories, which an admin order asks the daemon of its machine to read
//! ([`AdminOp::ReadMemories`]): its core memories (whoami, memory, tasks), which a set order edits, and its harness's
//! memory files and bd memories, read-only, and what its provider remembers of it, when the provider shows that.
//! Without the admin role a human asks the daemon serving the ant instead, over core NATS ([`MemoriesRequest`]), for
//! the core memories and the provider's alone.

use std::fmt::Write as _;
use std::path::Path;
use std::time::Duration;

use ants_client::attachments::size;
use ants_daemon::bus::relayed;
use ants_daemon::profile::identity;
use ants_proto::{
    AdminOp, MEMORIES_FRESH, Machine, Memories, MemoriesOutcome, MemoriesReply, MemoriesRequest, MemoriesShown,
    Skipped, UserId,
};
use anyhow::{Result, bail};
use async_nats::jetstream;
use futures::StreamExt as _;

use crate::admin::orders::{self, Admin, Asked};
use crate::connection::Nats;

/// How long the CLI waits for the daemon's answer.
pub const WAIT: Duration = Duration::from_secs(15);

/// Why a read without the admin role shows no memory files and no bd memories.
const ADMIN_ONLY: &str = "read only by an admin order, which credentials with the admin role send";

/// Ask the daemon of `machine`, else of the machine that last recorded serving `ant`, for the memories of `ant`, with
/// the admin credentials of `nats`, waiting up to [`WAIT`] for its answer. Without the admin role, and with no
/// `machine` named, ask the daemon serving `ant` for its core and provider memories alone ([`shown`]).
pub async fn read(
    nats: &Nats<'_>,
    js: &jetstream::Context,
    machine: Option<Machine>,
    ant: &UserId,
) -> Result<Memories> {
    if machine.is_none() && !orders::is_admin(nats) {
        let MemoriesShown { core, provider, mut skipped } = shown(&js.client(), ant, &nats.me.id).await?;
        skipped.push(Skipped { what: "memory files and bd memories".to_owned(), why: ADMIN_ONLY.to_owned() });
        return Ok(Memories { core, provider, skipped, ..Memories::default() });
    }
    let op = AdminOp::ReadMemories { identity: identity::id_of(js, nats.domain, ant).await };
    let asked = Asked {
        online: "memories are read only while it is online",
        fresh: MEMORIES_FRESH,
        to: &format!("read the memories of {ant}"),
        lacking: "memories",
    };
    Admin::of(nats)?.ask(js, machine, ant, op, WAIT, &asked).await
}

/// The core and provider memories of `ant`, as the daemon serving it answers `me`, any human, within [`WAIT`].
async fn shown(client: &async_nats::Client, ant: &UserId, me: &UserId) -> Result<MemoriesShown> {
    let request = MemoriesRequest::new(ant.clone(), me.clone());
    let mut answers = relayed::subscribe(client, request.reply_subject()).await?;
    client.publish(request.subject(), serde_json::to_vec(&request)?.into()).await?;
    client.flush().await?;
    let deadline = tokio::time::Instant::now() + WAIT;
    while let Ok(Some(message)) = tokio::time::timeout_at(deadline, answers.next()).await {
        let Ok(reply) = serde_json::from_slice::<MemoriesReply>(&message.payload) else { continue };
        if reply.request_id != request.id || reply.check_subject(&message.subject).is_err() {
            continue;
        }
        match reply.outcome {
            MemoriesOutcome::Read(shown) => return Ok(shown),
            MemoriesOutcome::Refused { error } => {
                bail!("the daemon serving {ant} refused to read its memories: {error}")
            }
        }
    }
    bail!("no daemon serving {ant} answered within {}s: memories are read only while it is served", WAIT.as_secs())
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
    // A read without the admin role names no workdir and reads no files.
    let files_read = workdir.is_some() || !memories.files.is_empty();
    match &memories.harness {
        Some(harness) => _ = writeln!(out, "\nmemory files of harness {harness}"),
        None if files_read => out.push_str("\nmemory files\n"),
        None => {}
    }
    if files_read && memories.files.is_empty() {
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
