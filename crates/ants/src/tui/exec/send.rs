//! The messages the bar sends: an ask, a reply, a post, a note and a message handed on, each as `ants ask`, `ants post`
//! and `ants notes` send them, with the files staged in the bar stored before the message.

use ants_daemon::bus;
use ants_proto::{ChannelId, ThreadId, UserId};
use anyhow::{Result, anyhow};

use super::{Ctx, Done, Quoted};
use crate::messages::attachments::{self, File};
use crate::messages::forward;
use crate::messages::send;
use crate::people::humans;
use crate::threads::thread;
use crate::tui::work::entries::{dm_with, reply_to, stuck};

/// How the chat attaches a file, as [`send::check_fits`] suggests it.
const ATTACH: &str = "attach it with /attach";

/// The warning for a message kept in the leaf node's outbox.
pub(super) fn outbox(stored: bus::Stored) -> Vec<String> {
    match stored {
        bus::Stored::Outbox => vec![send::OUTBOX.to_owned()],
        bus::Stored::Hub => Vec::new(),
    }
}

/// Send `text` to `to` on `thread`, or a new one, as `ants ask` does, with the warnings it prints for an ant no daemon
/// serves now; quoting the message it replies to if any, and with `queue` waiting behind the prompt running on the
/// thread; `files` go with it, stored before it as `ants ask --attach` stores them.
pub(super) async fn ask(
    ctx: &Ctx,
    to: send::Recipient,
    thread: Option<ThreadId>,
    text: String,
    quote: Option<Quoted>,
    queue: bool,
    files: &[File],
) -> Result<Done> {
    let new = thread.is_none();
    let mut envelope = send::ask_envelope(&ctx.me, thread, to.to.clone(), text);
    envelope.queue = queue;
    deliver(ctx, to, envelope, quote, files, new).await
}

/// Send `envelope` from [`send::ask_envelope`] to `to` as [`ask`] does, opening its thread when `new`.
async fn deliver(
    ctx: &Ctx,
    to: send::Recipient,
    mut envelope: ants_proto::Envelope,
    quote: Option<Quoted>,
    files: &[File],
    new: bool,
) -> Result<Done> {
    if let Some(q) = &quote {
        envelope = envelope.quoting(q.id, q.quote.clone());
    }
    envelope.attachments = attachments::references(files);
    // Sealed to a human, what is stored is each file's ciphertext.
    let mut stored = files.to_vec();
    send::seal_to_human(&ctx.js, &ctx.me, &ctx.sealing, &mut envelope, &mut stored).await?;
    // Sealing took the quote into the sealed body; one left is in the clear.
    if let Some(q) = quote.filter(|_| envelope.quote.is_some()) {
        envelope.quote = Some(q.quote(false));
    }
    send::check_fits(&ctx.js, &envelope, ATTACH)?;
    let (mut warnings, _) = send::unserved(&ctx.js, &ctx.me, &envelope.to).await;
    attachments::store(&ctx.js, &stored, |id| envelope.attachment_handle(id)).await?;
    let stored = send::deliver(&ctx.js, ctx.domain.as_ref(), &envelope).await?;
    warnings.extend(outbox(stored));
    Ok(Done {
        line: format!("{} {to} on thread {}{}", sent(envelope.queue), envelope.thread_id, with(files)),
        warnings,
        open: new.then_some(envelope.thread_id),
        ..Done::default()
    })
}

/// Post `text` to `channel` on `thread`, or a new one, as `ants post` does, with the warnings it prints; quoting the
/// post or message it replies to if any, and with `queue` waiting behind the prompt running on the thread; `files` go
/// with it, stored before it as `ants post --attach` stores them.
pub(super) async fn post(
    ctx: &Ctx,
    channel: ChannelId,
    thread: Option<ThreadId>,
    text: String,
    quote: Option<Quoted>,
    queue: bool,
    files: &[File],
) -> Result<Done> {
    let new = thread.is_none();
    let mut posting = send::Posting::compose(&ctx.js, &ctx.me, channel, thread, text).await?;
    if let Some(q) = quote {
        posting.post = posting.post.quoting(q.id, q.quote(false));
    }
    posting.post.attachments = attachments::references(files);
    posting.post.queue = queue;
    send::check_fits(&ctx.js, &posting.post, ATTACH)?;
    attachments::store(&ctx.js, files, |id| posting.post.attachment_handle(id)).await?;
    let (stored, notified) = posting.deliver(&ctx.js, &ctx.me).await?;
    let post = &posting.post;
    let posted = if queue { "queued a post to" } else { "posted to" };
    let mut line = format!("{posted} #{} on thread {}{}", post.channel, post.thread_id, with(files));
    if !notified.is_empty() {
        let names: Vec<&str> = notified.iter().map(UserId::as_str).collect();
        line = format!("{line}; notified {}", names.join(", "));
    }
    let mut warnings = posting.warnings.clone();
    if stored.is_none() {
        warnings.push(send::OUTBOX.to_owned());
    }
    Ok(Done { line, warnings, open: new.then(|| post.thread_id.clone()), ..Done::default() })
}

/// How a sent line begins: a message sent, or with `queue` one waiting its turn.
fn sent(queue: bool) -> &'static str {
    if queue { "queued for" } else { "sent to" }
}

/// How a sent line tells of the `files` attached.
fn with(files: &[File]) -> String {
    match files.len() {
        0 => String::new(),
        1 => " with 1 file".to_owned(),
        n => format!(" with {n} files"),
    }
}

/// Reply on `thread` to `to`, else to whoever the human talks with there, else as an unaddressed message.
pub(super) async fn reply(
    ctx: &Ctx,
    thread: ThreadId,
    to: Option<UserId>,
    text: String,
    quote: Option<Quoted>,
    queue: bool,
    files: &[File],
) -> Result<Done> {
    if let Some(to) = to {
        return ask(ctx, to.into(), Some(thread), text, quote, queue, files).await;
    }
    let me = &ctx.me.id;
    let entries = thread::history(&ctx.js, &thread, None, thread::Activity::None, None).await?;
    let humans = humans::load(&ctx.js, &ctx.me).await;
    let human = |u: &UserId| humans.contains(u);
    let to = match reply_to(&entries, me, human).or_else(|| dm_with(&entries, me, human)) {
        Some(to) => to.into(),
        None => send::unaddressed(&ctx.js, &ctx.me, ctx.account.as_deref()).await,
    };
    ask(ctx, to, Some(thread), text, quote, queue, files).await
}

/// Ask the ant stuck on `thread` again with `text`, granting what it waits for.
pub(super) async fn grant(ctx: &Ctx, thread: ThreadId, text: String, files: &[File]) -> Result<Done> {
    let entries = thread::history(&ctx.js, &thread, None, thread::Activity::None, None).await?;
    let ant = stuck(&entries).ok_or_else(|| anyhow!("no ant is stuck on thread {thread}"))?;
    ask(ctx, ant.into(), Some(thread), text, None, false, files).await
}

/// Hand `source` to `to` on a new thread, as `ants ask --forward` does.
pub(super) async fn forward(
    ctx: &Ctx,
    to: UserId,
    source: &forward::Source,
    note: &str,
    files: &[File],
) -> Result<Done> {
    let files = source.files(&ctx.js, ctx.keyring.as_deref(), files.to_vec()).await?;
    let envelope = source.envelope(&ctx.me, to.clone(), note);
    deliver(ctx, to.into(), envelope, None, &files, true).await
}

/// Post `text` to the notes of `thread` as `ants notes` does, `files` going with it.
pub(super) async fn notes(ctx: &Ctx, thread: ThreadId, text: String, files: &[File]) -> Result<Done> {
    let post = send::post_notes(&ctx.js, &ctx.me.id, thread, text, files, ATTACH).await?;
    Ok(Done::line(format!("{}{}", send::notes_posted(&post), with(files))))
}
