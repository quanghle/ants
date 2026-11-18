//! What the command bar's sends and slash commands do over the CLI's connection, in the background while the UI goes
//! on: the same library code as `ants ask`, `ants post`, `ants notes`, `ants answer`, `ants react`, `ants cancel`,
//! `ants thread done|title|pin|mute...` and `ants trail` ([`super::trail`]) run, each returning the line those print
//! and the warnings they log.

use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use ants_daemon::bus;
use ants_daemon::records::reactions;
use ants_proto::{
    ChannelId, ChannelNotify, EscalationReason, JsDomain, Pick, Quote, Reaction, ThreadId, TrailName, UserId, Uuid,
};
use anyhow::{Result, anyhow, bail};
use async_nats::jetstream;
use futures::future::LocalBoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt as _};

use super::{attach, trail};
use crate::admin::orders;
use crate::admin::pause;
use crate::cmd::{cancel, send};
use crate::messages::attachments::{self, File, Listed};
use crate::messages::forward;
use crate::people::sealed::{Keyring, Sealing};
use crate::people::{humans, quiet};
use crate::threads::thread::{self, Entry, Kind};
use crate::threads::thread_meta::{self, Marking, Meta};

/// What a job needs: the UI's connection and its human.
pub struct Ctx {
    pub client: async_nats::Client,
    pub js: jetstream::Context,
    pub me: humans::Me,
    pub domain: Option<JsDomain>,
    /// The thread titles, done states and marks, or why they cannot be had.
    pub meta: Result<Meta, String>,
    /// What seals messages to another human, and titles on a thread between two humans.
    pub sealing: Sealing,
    /// The account that signed the CLI's credentials, for whose team the human's assistant grant is verified.
    pub account: Option<String>,
    /// What opens the attachments of sealed messages, if anything.
    pub keyring: Option<Arc<Keyring>>,
    /// The human's admin credentials, which sign admin orders, or why there are none.
    pub admin: Result<orders::Admin, String>,
}

impl Ctx {
    fn meta(&self) -> Result<&Meta> {
        self.meta.as_ref().map_err(|e| anyhow!("{e}"))
    }
}

/// The message or post a send replies to, quoting it ([`Envelope::quoting`]).
///
/// [`Envelope::quoting`]: ants_proto::Envelope::quoting
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quoted {
    pub id: Uuid,
    pub quote: Quote,
    /// Whether the message was sealed: only a sealed reply carries its excerpt ([`Quote::withheld`]).
    pub sealed: bool,
}

impl Quoted {
    /// The quote as a reply carries it, `sealed` or not.
    pub fn quote(self, sealed: bool) -> Quote {
        if self.sealed && !sealed { self.quote.withheld() } else { self.quote }
    }
}

/// What the bar asks to be done.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Job {
    /// Send `text` to `to` as `ants ask` does, on `thread` or a new one, quoting the message it replies to if any.
    Ask {
        to: UserId,
        thread: Option<ThreadId>,
        text: String,
        quote: Option<Quoted>,
    },
    /// Send `text`, which names no one, on a new thread as `ants ask` does: to the human's assistant, else the
    /// messenger ([`send::unaddressed`]).
    Unaddressed {
        text: String,
    },
    /// Post `text` to `channel` as `ants post` does, on `thread` or a new one, quoting the post or message it replies
    /// to if any.
    Post {
        channel: ChannelId,
        thread: Option<ThreadId>,
        text: String,
        quote: Option<Quoted>,
    },
    /// Reply `text` on `thread` to `to`, else to the ant that last messaged the human there ([`reply_to`]), else to the
    /// other human ([`dm_with`]), else as [`Job::Unaddressed`] does; quoting the message it replies to if any.
    Reply {
        thread: ThreadId,
        to: Option<UserId>,
        text: String,
        quote: Option<Quoted>,
    },
    /// Send `text` to the ant stuck on `thread` ([`stuck`]), which the human's message gives fresh turns.
    Grant {
        thread: ThreadId,
        text: String,
    },
    /// Cancel the prompts `ants` run on `thread`, as `ants cancel` does.
    Cancel {
        thread: ThreadId,
        ants: Vec<UserId>,
    },
    /// Answer the questions asked of the human on `thread` in the message `question` with `picks`, one per question,
    /// as `ants answer` does: refused when the message is gone from the thread or answered already ([`waiting`]).
    Answer {
        thread: ThreadId,
        question: Uuid,
        picks: Vec<Pick>,
    },
    Done {
        thread: ThreadId,
        done: bool,
    },
    Mark {
        thread: ThreadId,
        marking: Marking,
    },
    Mute {
        thread: ThreadId,
        hold: Duration,
    },
    Unmute {
        thread: ThreadId,
    },
    /// Change the entry of `channel` as `ants channel set|add|remove` does, refused as it refuses it.
    Channel {
        channel: ChannelId,
        change: crate::channel::Change,
    },
    /// Set the human's notify level of `channel`, as `ants channel notify` does.
    ChannelNotify {
        channel: ChannelId,
        level: ChannelNotify,
    },
    Title {
        thread: ThreadId,
        text: String,
    },
    /// Post `text` to the notes of `thread` as `ants notes THREAD TEXT` does.
    Notes {
        thread: ThreadId,
        text: String,
    },
    /// React to `message`, else to the latest message on `thread` not from the human ([`latest`]).
    React {
        thread: ThreadId,
        message: Option<Uuid>,
        reaction: Reaction,
    },
    /// Open the attachment with the system's viewer ([`attach::open`]).
    Open(Box<Listed>),
    /// Save the attachment as `ants attachment get` does, to the path given, else under its name here.
    Save(Box<Listed>, Option<std::path::PathBuf>),
    /// Order the daemon of `ant` to restart its harnesses, cancelling their prompts if `now`, as `ants ant restart`
    /// does.
    Restart {
        ant: UserId,
        now: bool,
    },
    /// Pause `ant`, or resume it, as `ants ant pause|resume` does.
    Pause {
        ant: UserId,
        paused: bool,
    },
    /// Send as `job`, an [`Job::Ask`], [`Job::Post`], [`Job::Reply`], [`Job::Grant`] or [`Job::Notes`], does, with
    /// `files` attached.
    Attached {
        job: Box<Job>,
        files: Rc<[File]>,
    },
    /// Retire `ant` by admin order, as `ants ant retire` does.
    Retire {
        ant: UserId,
    },
    /// Send as `job`, a [`Job::Reply`] or a [`Job::Post`] on a thread, does, waiting behind the prompt running there
    /// rather than interrupting it, as `ants ask --queue` does.
    Later(Box<Job>),
    /// Withdraw `message` from the queue of each of `ants`, as `ants withdraw` does; with `edit`, its text and files
    /// come back into the bar.
    Withdraw {
        ants: Vec<UserId>,
        message: Uuid,
        edit: Option<Box<Edit>>,
    },
    /// Hand `source` to `to` on a new thread with `note`, as `ants ask --forward` does.
    Forward {
        to: UserId,
        source: forward::Source,
        note: String,
    },
    /// List the runs of trails, as `ants trail runs` does.
    TrailRuns,
    /// Plan a run of the saved trail `trail` with `args` on `conductor`, as `ants trail run` sums it up before asking to
    /// start it ([`Done::planned`]).
    TrailPlan {
        trail: TrailName,
        conductor: Option<UserId>,
        args: Option<serde_json::Value>,
    },
    /// Ask the conductor to start the run planned, as `ants trail run` does once confirmed.
    TrailStart(Box<crate::cmd::trail::start::Start>),
    /// Stop the run picked, as `ants trail stop` does.
    TrailStop(crate::cmd::trail::runs::Pick),
    /// Resume the run picked, as `ants trail resume` does.
    TrailResume(crate::cmd::trail::runs::Pick),
}

/// A message withdrawn to send again: its text and attachments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edit {
    pub text: String,
    pub files: Vec<Listed>,
}

/// What a job did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Done {
    /// What the CLI prints for it.
    pub line: String,
    /// What the CLI warns of.
    pub warnings: Vec<String>,
    /// The thread it started, to show.
    pub open: Option<ThreadId>,
    /// The message it withdrew.
    pub withdrew: Option<Uuid>,
    /// The text, and the files fetched, of a message withdrawn to edit, for the bar.
    pub edit: Option<(String, Vec<PathBuf>)>,
    /// The channel whose entry it changed, to read back.
    pub channel: Option<ChannelId>,
    /// What to show in the pager: a table the CLI prints.
    pub page: Option<String>,
    /// The run of a trail planned, to start once confirmed.
    pub planned: Option<Box<trail::Planned>>,
}

impl Done {
    fn line(line: String) -> Self {
        Self { line, ..Self::default() }
    }
}

/// How a job went, with the bar's text it came from and the message it replied to, quoting it, on its thread.
#[derive(Debug)]
pub struct Outcome {
    pub result: Result<Done>,
    pub text: String,
    pub quoting: Option<(ThreadId, Quoted)>,
}

/// The jobs running.
pub struct Exec {
    ctx: Rc<Ctx>,
    jobs: FuturesUnordered<LocalBoxFuture<'static, Outcome>>,
}

impl Exec {
    pub fn new(ctx: Ctx) -> Self {
        Self { ctx: Rc::new(ctx), jobs: FuturesUnordered::new() }
    }

    /// Run `job`, which the bar's `text` asked for.
    pub fn run(&mut self, job: Job, text: String) {
        let ctx = Rc::clone(&self.ctx);
        let quoting = job.quoting();
        self.jobs.push(async move { Outcome { result: job.run(&ctx).await, text, quoting } }.boxed_local());
    }

    /// The next job to end; pending while none runs.
    pub async fn next(&mut self) -> Outcome {
        match self.jobs.next().await {
            Some(outcome) => outcome,
            None => std::future::pending().await,
        }
    }
}

impl Job {
    /// Whether the job sends a message, which files staged in the bar go with.
    pub fn sends(&self) -> bool {
        matches!(
            self,
            Self::Ask { .. }
                | Self::Unaddressed { .. }
                | Self::Post { .. }
                | Self::Reply { .. }
                | Self::Grant { .. }
                | Self::Notes { .. }
                | Self::Later(_)
                | Self::Forward { .. }
        )
    }

    /// The message the job replies to, quoting it, with its thread.
    pub fn quoting(&self) -> Option<(ThreadId, Quoted)> {
        let (thread, quote) = match self {
            Self::Attached { job, .. } | Self::Later(job) => return job.quoting(),
            Self::Ask { thread, quote, .. } | Self::Post { thread, quote, .. } => (thread.as_ref()?, quote),
            Self::Reply { thread, quote, .. } => (thread, quote),
            _ => return None,
        };
        Some((thread.clone(), quote.clone()?))
    }

    async fn run(self, ctx: &Ctx) -> Result<Done> {
        let me = &ctx.me.id;
        let (job, files) = match self {
            Self::Attached { job, files } => (*job, files),
            job => (job, Rc::from([])),
        };
        let (job, queue) = match job {
            Self::Later(job) => (*job, true),
            job => (job, false),
        };
        match job {
            Self::Ask { to, thread, text, quote } => ask(ctx, to.into(), thread, text, quote, false, &files).await,
            Self::Unaddressed { text } => {
                let to = send::unaddressed(&ctx.js, &ctx.me, ctx.account.as_deref()).await;
                ask(ctx, to, None, text, None, false, &files).await
            }
            Self::Post { channel, thread, text, quote } => post(ctx, channel, thread, text, quote, queue, &files).await,
            Self::Reply { thread, to, text, quote } => reply(ctx, thread, to, text, quote, queue, &files).await,
            Self::Grant { thread, text } => {
                let entries = thread::history(&ctx.js, &thread, None, thread::Activity::None, None).await?;
                let ant = stuck(&entries).ok_or_else(|| anyhow!("no ant is stuck on thread {thread}"))?;
                ask(ctx, ant.into(), Some(thread), text, None, false, &files).await
            }
            Self::Restart { ant, now } => {
                let admin = ctx.admin.as_ref().map_err(|e| anyhow!("{e}"))?;
                let (order, outcome) = crate::admin::restart::send(admin, &ctx.js, None, &ant, now).await?;
                Ok(Done::line(crate::admin::restart::told(&order, outcome)?))
            }
            Self::Cancel { thread, ants } => cancel_all(ctx, &thread, ants).await,
            Self::Answer { thread, question, picks } => {
                let entries = thread::history(&ctx.js, &thread, None, thread::Activity::None, None).await?;
                waiting(&entries, me, &thread, question)?;
                let envelope = send::answering(&ctx.js, me, question, send::Picks::Made(picks)).await?;
                let stored = send::deliver(&ctx.js, ctx.domain.as_ref(), &envelope).await?;
                Ok(Done {
                    line: format!("answered {} on thread {thread}", envelope.to),
                    warnings: outbox(stored),
                    ..Done::default()
                })
            }
            Self::Done { thread, done } => {
                Ok(Done::line(thread_meta::marked_done(&ctx.js, ctx.meta()?, &thread, done).await?))
            }
            Self::Mark { thread, marking } => {
                Ok(Done::line(thread_meta::marked(&ctx.js, ctx.meta()?, &thread, marking).await?))
            }
            Self::Mute { thread, hold } => Ok(Done::line(quiet::muted(&ctx.js, me, &thread, hold).await?)),
            Self::Unmute { thread } => Ok(Done::line(quiet::unmuted(&ctx.js, me, &thread).await?)),
            Self::Channel { channel, change } => change_channel(ctx, channel, &change).await,
            Self::ChannelNotify { channel, level } => {
                Ok(Done::line(quiet::channel_notify(&ctx.js, me, &channel, level).await?))
            }
            Self::Title { thread, text } => {
                Ok(Done::line(thread_meta::retitle(&ctx.js, ctx.meta()?, &ctx.sealing, &thread, &text, false).await?))
            }
            Self::Notes { thread, text } => {
                let post = send::post_notes(&ctx.js, me, thread, text, &files).await?;
                Ok(Done::line(format!("{}{}", send::notes_posted(&post), with(&files))))
            }
            Self::React { thread, message, reaction } => {
                let message = match message {
                    Some(message) => message,
                    None => {
                        let entries = thread::history(&ctx.js, &thread, None, thread::Activity::None, None).await?;
                        latest(&entries, me).ok_or_else(|| anyhow!("no message on thread {thread} to react to"))?
                    }
                };
                let changed = reactions::add(&ctx.js, ctx.domain.as_ref(), me, &message, &reaction).await?;
                Ok(Done::line(send::reacted_line(&reaction, message, false, changed)))
            }
            Self::Open(listed) => Ok(Done::line(attach::open(&ctx.js, ctx.keyring.as_deref(), &listed).await?)),
            Self::Save(listed, to) => {
                Ok(Done::line(attach::save(&ctx.js, ctx.keyring.as_deref(), &listed, to.as_deref()).await?))
            }
            Self::Retire { ant } => retire(ctx, &ant).await,
            Self::Forward { to, source, note } => forward(ctx, to, &source, &note, &files).await,
            Self::Pause { ant, paused } => {
                let admin = ctx.admin.as_ref().map_err(|e| anyhow!("{e}"))?;
                Ok(Done::line(pause::order(admin, &ctx.js, &ant, None, paused, pause::WAIT).await?))
            }
            Self::Attached { .. } => Err(anyhow!("files are attached to one message")),
            Self::Later(_) => Err(anyhow!("a message waits its turn once")),
            Self::Withdraw { ants, message, edit } => withdraw(ctx, &ants, message, edit.map(|e| *e)).await,
            Self::TrailRuns => trail::listed(ctx).await,
            Self::TrailPlan { trail, conductor, args } => trail::planned(ctx, &trail, conductor, args).await,
            Self::TrailStart(start) => trail::started(ctx, &start).await,
            Self::TrailStop(run) => trail::stopped(ctx, &run).await,
            Self::TrailResume(run) => trail::resumed(ctx, &run).await,
        }
    }
}

/// Write `change` to the entry of `channel` with the human's credentials ([`crate::channel::Change::write`]).
pub async fn change_channel(ctx: &Ctx, channel: ChannelId, change: &crate::channel::Change) -> Result<Done> {
    let kv = crate::channel::open(&ctx.js).await?;
    let humans = humans::load(&ctx.js, &ctx.me).await;
    let changed = change.write(&ctx.js, &kv, &channel, &humans).await?;
    let line = changed_line(&channel, change, &changed);
    Ok(Done { line, warnings: changed.warnings, channel: Some(channel), ..Done::default() })
}

/// What a change to `channel`'s entry did, in a line.
fn changed_line(channel: &ChannelId, change: &crate::channel::Change, changed: &crate::channel::Changed) -> String {
    let names = |users: &[UserId]| users.iter().map(UserId::as_str).collect::<Vec<_>>().join(", ");
    let mut parts = Vec::new();
    if let Some(members) = &change.members {
        parts.push(format!("members now {}", names(members)));
    }
    if !changed.added.is_empty() {
        parts.push(format!("added {}", names(&changed.added)));
    }
    if !changed.removed.is_empty() {
        parts.push(format!("removed {}", names(&changed.removed)));
    }
    match &change.messenger {
        Some(Some(messenger)) => parts.push(format!("messenger now {messenger}")),
        Some(None) => parts.push("messenger back to the default".into()),
        None => {}
    }
    match &change.owner {
        Some(Some(owner)) => parts.push(format!("owner now {owner}")),
        Some(None) => parts.push("no owner".into()),
        None => {}
    }
    match (&change.description, &changed.entry.description) {
        (Some(_), Some(_)) => parts.push("description changed".into()),
        (Some(_), None) => parts.push("no description".into()),
        (None, _) => {}
    }
    if parts.is_empty() {
        format!("nothing changed in #{channel}")
    } else {
        format!("#{channel}: {}", parts.join(", "))
    }
}

/// Reply on `thread` to `to`, else to whoever the human talks with there, else as an unaddressed message.
async fn reply(
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

/// Cancel the prompts of `ants` on `thread`, failing only when none is cancelled.
async fn cancel_all(ctx: &Ctx, thread: &ThreadId, ants: Vec<UserId>) -> Result<Done> {
    let mut lines = Vec::new();
    let mut failed = None;
    for ant in ants {
        match cancel::cancelled(&ctx.client, &ctx.me.id, ant, thread.clone()).await {
            Ok(line) => lines.push(line),
            Err(e) => failed = Some(e),
        }
    }
    match failed {
        Some(e) if lines.is_empty() => Err(e),
        failed => {
            let warnings = failed.map(|e| format!("{e:#}")).into_iter().collect();
            Ok(Done { line: lines.join("; "), warnings, ..Done::default() })
        }
    }
}

/// Withdraw `message` from the queue of each of `ants`, fetching the files of `edit` for the bar. Refused by one ant,
/// the message stays withdrawn from the others, the refusal a warning; refused by all, the job fails.
async fn withdraw(ctx: &Ctx, ants: &[UserId], message: Uuid, edit: Option<Edit>) -> Result<Done> {
    let (mut lines, mut warnings, mut failed) = (Vec::new(), Vec::new(), None);
    for ant in ants {
        match crate::queue::withdrawn(&ctx.js, ctx.domain.as_ref(), &ctx.me.id, ant, message).await {
            Ok(line) => lines.push(line),
            Err(e) if ants.len() > 1 => warnings.push(format!("{e:#}")),
            Err(e) => failed = Some(e),
        }
    }
    if let Some(e) = failed {
        return Err(e);
    }
    if lines.is_empty() {
        bail!("{}", warnings.join("; "));
    }
    let line = lines.join("; ");
    let edit = match edit {
        Some(Edit { text, files }) => {
            let mut paths = Vec::new();
            for listed in &files {
                match attach::fetched(&ctx.js, ctx.keyring.as_deref(), listed).await {
                    Ok(path) => paths.push(path),
                    Err(e) => warnings.push(format!("{} not attached again: {e:#}", listed.attachment.name)),
                }
            }
            Some((text, paths))
        }
        None => None,
    };
    Ok(Done { line, warnings, withdrew: Some(message), edit, ..Done::default() })
}

/// Hand `source` to `to` on a new thread, as `ants ask --forward` does.
async fn forward(ctx: &Ctx, to: UserId, source: &forward::Source, note: &str, files: &[File]) -> Result<Done> {
    let files = source.files(&ctx.js, ctx.keyring.as_deref(), files.to_vec()).await?;
    let envelope = source.envelope(&ctx.me, to.clone(), note);
    deliver(ctx, to.into(), envelope, None, &files, true).await
}

/// Retire `ant` by admin order, as `ants ant retire` does.
async fn retire(ctx: &Ctx, ant: &UserId) -> Result<Done> {
    let admin = ctx.admin.as_ref().map_err(|e| anyhow!("{e}"))?;
    let said =
        crate::admin::orders::retire::retire(admin, &ctx.js, ant, None, crate::admin::orders::retire::WAIT).await?;
    Ok(Done::line(said.replace('\n', " ")))
}

/// The warning for a message kept in the leaf node's outbox.
fn outbox(stored: bus::Stored) -> Vec<String> {
    match stored {
        bus::Stored::Outbox => vec![send::OUTBOX.to_owned()],
        bus::Stored::Hub => Vec::new(),
    }
}

/// Send `text` to `to` on `thread`, or a new one, as `ants ask` does, with the warnings it prints for an ant no daemon
/// serves now; quoting the message it replies to if any, and with `queue` waiting behind the prompt running on the
/// thread; `files` go with it, stored before it as `ants ask --attach` stores them.
async fn ask(
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
async fn post(
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

/// Who a reply on a thread whose `entries` are these goes to: the last ant that messaged `me` there, else the last ant
/// `me` messaged there; `human` tells the humans.
pub fn reply_to(entries: &[Entry], me: &UserId, human: impl Fn(&UserId) -> bool) -> Option<UserId> {
    let messages = || entries.iter().rev().filter(|e| matches!(e.kind, Kind::Message | Kind::Escalation) && !e.forged);
    let ant = |user: &Option<UserId>| user.as_ref().filter(|u| *u != me && !human(u)).cloned();
    messages()
        .filter(|e| e.to.as_ref() == Some(me))
        .find_map(|e| ant(&e.from))
        .or_else(|| messages().filter(|e| e.from.as_ref() == Some(me)).find_map(|e| ant(&e.to)))
}

/// The other human of the latest message between `me` and another human among `entries`, for a reply on a thread
/// with no ant; `human` tells the humans.
pub fn dm_with(entries: &[Entry], me: &UserId, human: impl Fn(&UserId) -> bool) -> Option<UserId> {
    let other = |from: &UserId, to: &UserId| match (from == me, to == me) {
        (true, false) => Some(to.clone()),
        (false, true) => Some(from.clone()),
        _ => None,
    };
    entries
        .iter()
        .rev()
        .filter(|e| e.kind == Kind::Message && !e.forged)
        .find_map(|e| other(e.from.as_ref()?, e.to.as_ref()?).filter(|u| human(u)))
}

/// The ant stuck on a thread whose `entries` are these: that of its latest escalation but for a tool request's, or
/// the ant of its latest dead letter.
pub fn stuck(entries: &[Entry]) -> Option<UserId> {
    entries.iter().rev().find_map(|e| match e.kind {
        Kind::Escalation if e.reason != Some(EscalationReason::Approval) && !e.forged => e.stuck.clone(),
        Kind::Dead => e.to.clone(),
        _ => None,
    })
}

/// Whether the question asked of `me` in the message `id` on `thread`, whose `entries` are these, waits for an answer:
/// an error when it is gone from the thread or a message of `me` answered it already.
fn waiting(entries: &[Entry], me: &UserId, thread: &ThreadId, id: Uuid) -> Result<()> {
    if !entries.iter().any(|e| e.id == Some(id) && e.question.is_some() && e.to.as_ref() == Some(me) && !e.forged) {
        bail!("question {id} to {me} is gone from thread {thread}");
    }
    if entries.iter().any(|e| e.from.as_ref() == Some(me) && e.answer.as_ref().is_some_and(|a| a.question == id)) {
        bail!("question {id} on thread {thread} is answered already");
    }
    Ok(())
}

/// The latest message or post among `entries` not from `me`, to react to.
pub fn latest(entries: &[Entry], me: &UserId) -> Option<Uuid> {
    entries
        .iter()
        .rev()
        .filter(|e| matches!(e.kind, Kind::Message | Kind::Escalation | Kind::Post) && e.from.as_ref() != Some(me))
        .find_map(|e| e.id)
}

#[cfg(test)]
mod tests {
    use ants_proto::Question;

    use super::*;
    use crate::connection::Nats;
    use crate::test_support::live::{message, user};

    #[test]
    fn a_reply_goes_to_the_ant_that_last_wrote_else_the_one_last_written_to() {
        let (thread, me) = (ThreadId::generate(), user("alice"));
        let human = |u: &UserId| u.as_str() == "bob";
        let mut entries = vec![message(1, &thread, "alice", "dev-a", "deploy")];
        assert_eq!(reply_to(&entries, &me, human), Some(user("dev-a")));
        entries.push(message(2, &thread, "dev-b", "alice", "done"));
        entries.push(message(3, &thread, "bob", "alice", "thanks"));
        assert_eq!(reply_to(&entries, &me, human), Some(user("dev-b")), "a human is no ant to reply to");
        assert_eq!(reply_to(&entries[2..], &me, human), None);
        assert_eq!(dm_with(&entries, &me, human), Some(user("bob")), "on a thread with no ant, the other human");
        assert_eq!(dm_with(&entries[..2], &me, human), None);
    }

    /// What the bar sends to another human, a reply on their thread with a file attached and its title are sealed for
    /// both; a reply to an ant quoting a sealed message carries no excerpt of it, and notes posted there go in the
    /// clear with their files, as `ants notes` posts them.
    #[tokio::test]
    async fn the_bar_seals_messages_to_a_human_their_files_and_titles_on_their_thread() {
        use ants_proto::Role;

        let Some(server) = crate::test_support::Server::start() else { return };
        let (alice, bob) = (user("alice"), user("bob"));
        let creds = [server.creds_as(&alice, Role::Human), server.creds_as(&bob, Role::Human)];
        let nats: Vec<_> = creds.iter().map(|c| Nats::new(&server.url, Some(c), None).unwrap()).collect();
        let (client, js) = nats[0].connect("test").await.unwrap();
        let (_, bob_js) = nats[1].connect("test").await.unwrap();
        for config in [ants_daemon::hub::threads_config(), ants_daemon::hub::files_config()] {
            let _ = js.create_stream(config).await;
        }
        let meta = thread_meta::open(&nats[0], &js).await.map_err(|e| format!("{e:#}"));
        let (me, sealing) = (nats[0].me.clone(), Sealing::of(&nats[0]));
        let admin = Err("unused".into());
        let ctx = Ctx { client, js, me, domain: None, meta, sealing, account: None, keyring: None, admin };
        let source = server.dir().join("menu.txt");
        std::fs::write(&source, b"soup").unwrap();
        let files: Rc<[File]> = attachments::read_all(std::slice::from_ref(&source)).unwrap().into();

        let ask = Job::Ask { to: bob.clone(), thread: None, text: "lunch?".into(), quote: None };
        let thread = Box::pin(ask.run(&ctx)).await.unwrap().open.unwrap();
        let quote = Quoted { id: Uuid::now_v7(), quote: Quote::new(alice.clone(), "lunch?"), sealed: true };
        let reply = Job::Reply { thread: thread.clone(), to: None, text: "at noon".into(), quote: Some(quote.clone()) };
        let reply = Job::Attached { job: Box::new(reply), files: Rc::clone(&files) };
        let line = Box::pin(reply.run(&ctx)).await.unwrap().line;
        assert_eq!(line, format!("sent to bob on thread {thread} with 1 file"));
        let title = Job::Title { thread: thread.clone(), text: "Team lunch".into() };
        Box::pin(title.run(&ctx)).await.unwrap();
        let notes = Job::Notes { thread: thread.clone(), text: "the menu".into() };
        let line = Box::pin(Job::Attached { job: Box::new(notes), files }.run(&ctx)).await.unwrap().line;
        assert_eq!(line, format!("posted to the notes of thread {thread} (8 turns left) with 1 file"));
        let to_ant = Job::Ask {
            to: user("dev-a"),
            thread: Some(thread.clone()),
            text: "book".into(),
            quote: Some(quote.clone()),
        };
        Box::pin(to_ant.run(&ctx)).await.unwrap();

        let unread = crate::messages::inbox::Inbox::open(&bob_js, &bob)
            .await
            .unwrap()
            .unread(&Default::default())
            .await
            .unwrap();
        assert!(unread.iter().all(|e| e.sealed.is_some() && e.text.is_empty() && e.quote.is_none()), "{unread:?}");
        let keyring = crate::people::sealed::Keyring::load(&nats[1], &bob_js).await.unwrap();
        let texts: Vec<String> =
            unread.into_iter().map(|e| crate::people::sealed::read(e, Some(&keyring)).0.text).collect();
        assert_eq!(texts, ["lunch?", "at noon"]);
        let keyring = Arc::new(keyring);
        let entries =
            thread::history(&bob_js, &thread, None, thread::Activity::None, Some(Arc::clone(&keyring))).await.unwrap();
        assert_eq!(entries[1].quote, Some(quote.quote.clone()), "the quote is sealed with the text");
        let to_ant = entries.iter().find(|e| e.to == Some(user("dev-a"))).unwrap();
        assert_eq!((to_ant.sealed, &to_ant.quote), (None, &Some(quote.quote.withheld())), "no excerpt in the clear");
        let note = entries.iter().find(|e| e.kind == Kind::Note).unwrap();
        assert_eq!(
            (&note.from, note.text.as_str(), note.sealed, note.turns),
            (&Some(alice), "the menu", None, Some(8))
        );
        let [noted] = &note.attachments[..] else { panic!("{note:?}") };
        assert!(noted.attachment.sealed.is_none(), "{noted:?}");
        let [listed] = &entries[1].attachments[..] else { panic!("{entries:?}") };
        assert!(listed.attachment.sealed.is_some(), "{listed:?}");
        assert_eq!((listed.attachment.name.as_str(), listed.size()), ("menu.txt", 4));
        let (stored, _) = attachments::fetch(&bob_js, &listed.handle, None, None).await.unwrap();
        assert_eq!(stored.name, ants_proto::SealedAttachment::STORED_NAME, "stored sealed");
        let path = attach::fetched(&bob_js, Some(&keyring), listed).await.unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"soup");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
        let title = thread_meta::open(&nats[1], &bob_js).await.unwrap().title(&thread).await.unwrap().unwrap();
        assert_eq!((title.title.as_str(), title.sealed), ("Team lunch", None), "opened on bob's device");
        let unopened = Meta::open(&bob_js, None, &bob).await.unwrap().title(&thread).await.unwrap().unwrap();
        assert_eq!(unopened.title, "[sealed]", "stored sealed");
    }

    #[test]
    fn the_stuck_ant_is_that_of_the_latest_escalation_or_dead_letter() {
        let thread = ThreadId::generate();
        let mut escalated = message(1, &thread, "dev-a", "alice", "out of turns");
        (escalated.kind, escalated.reason, escalated.stuck) =
            (Kind::Escalation, Some(EscalationReason::TurnBudget), Some(user("dev-a")));
        let mut approval = escalated.clone();
        approval.reason = Some(EscalationReason::Approval);
        assert_eq!(stuck(&[escalated.clone(), approval.clone()]), Some(user("dev-a")));
        assert_eq!(stuck(&[approval]), None);
        let mut dead = message(2, &thread, "alice", "dev-b", "hi");
        dead.kind = Kind::Dead;
        assert_eq!(stuck(&[escalated, dead]), Some(user("dev-b")));
    }

    #[test]
    fn the_message_to_react_to_is_the_latest_of_another() {
        let (thread, me) = (ThreadId::generate(), user("alice"));
        let question = Question::new("which?", &["a".into(), "b".into()], true).unwrap();
        let mut asked = message(1, &thread, "dev-a", "alice", "which?");
        asked.question = Some(question.into());
        let answered = message(2, &thread, "alice", "dev-a", "a");
        assert_eq!(latest(&[asked.clone(), answered], &me), asked.id);
    }

    /// A digit on a question card answers that question, though a later one was stored before the answer went out;
    /// once answered, the same key is refused, as is a question the thread does not hold.
    #[tokio::test]
    async fn a_key_answers_the_question_its_card_shows_not_a_later_one() {
        use crate::tui::model::{Action, Key, To};
        use crate::tui::question::Keyed;

        let Some(server) = crate::test_support::Server::start() else { return };
        let creds = server.creds_as(&user("alice"), ants_proto::Role::Human);
        let nats = Nats::new(&server.url, Some(&creds), None).unwrap();
        let (client, js) = nats.connect("test").await.unwrap();
        js.create_stream(ants_daemon::hub::stream_config()).await.unwrap();
        let thread = ThreadId::generate();
        let ask = |text: &str| {
            let question = Question::new(text, &["yes".to_owned(), "no".to_owned()], false).unwrap();
            ants_proto::Envelope::asking(thread.clone(), user("dev-a"), user("alice"), question)
        };
        let (first, second) = (ask("Ship it?"), ask("Tag it?"));
        let store = async |e: &ants_proto::Envelope| {
            js.publish(e.subject(), serde_json::to_vec(e).unwrap().into()).await.unwrap().await.unwrap();
        };
        store(&first).await;
        let shown = thread::history(&js, &thread, None, thread::Activity::None, None).await.unwrap();
        let m = crate::tui::test_support::model();
        let key = super::super::question::thread_key(Key::Char('2'), &m.data, &thread, &shown, chrono::Utc::now());
        let Some(Keyed::To(to)) = key else { panic!("{key:?}") };
        let To::Do(Action::Run(job, _)) = *to else { panic!("{to:?}") };
        store(&second).await;

        let (me, sealing) = (nats.me.clone(), Sealing::of(&nats));
        let (meta, admin) = (Err("unused".into()), Err("unused".into()));
        let ctx = Ctx { client, js: js.clone(), me, domain: None, meta, sealing, account: None, keyring: None, admin };
        let done = job.clone().run(&ctx).await.unwrap();
        assert_eq!(done.line, format!("answered dev-a on thread {thread}"));
        let entries = thread::history(&js, &thread, None, thread::Activity::None, None).await.unwrap();
        let answers: Vec<(Uuid, &str)> =
            entries.iter().filter_map(|e| Some((e.answer.as_ref()?.question, e.text.as_str()))).collect();
        assert_eq!(answers, [(first.id, "no")], "the question shown, not the later one");

        let again = job.run(&ctx).await.unwrap_err().to_string();
        assert_eq!(again, format!("question {} on thread {thread} is answered already", first.id));
        let gone = Job::Answer { thread: thread.clone(), question: Uuid::now_v7(), picks: vec![Pick::option(1)] };
        let gone = gone.run(&ctx).await.unwrap_err().to_string();
        assert!(gone.contains("is gone from thread"), "{gone}");
    }

    #[tokio::test]
    async fn an_ask_from_the_bar_stores_its_files_before_the_message() {
        let Some(server) = crate::test_support::Server::start() else { return };
        let creds = server.creds_as(&user("alice"), ants_proto::Role::Human);
        let nats = Nats::new(&server.url, Some(&creds), None).unwrap();
        let (client, js) = nats.connect("test").await.unwrap();
        for config in [ants_daemon::hub::stream_config(), ants_daemon::hub::files_config()] {
            js.create_stream(config).await.unwrap();
        }
        let source = server.dir().join("plan.md");
        std::fs::write(&source, b"# plan").unwrap();
        let files: Rc<[File]> = attachments::read_all(std::slice::from_ref(&source)).unwrap().into();
        let (me, sealing) = (nats.me.clone(), Sealing::of(&nats));
        let meta = Err("unused".into());
        let admin = Err("unused".into());
        let ctx = Ctx { client, js: js.clone(), me, domain: None, meta, sealing, account: None, keyring: None, admin };
        let ask = Job::Ask { to: user("dev-a"), thread: None, text: "see the plan".into(), quote: None };
        let done = Job::Attached { job: Box::new(ask), files: Rc::clone(&files) }.run(&ctx).await.unwrap();
        assert!(
            done.line.starts_with("sent to dev-a on thread ") && done.line.ends_with(" with 1 file"),
            "{}",
            done.line
        );
        let thread = done.open.unwrap();
        let entries = thread::history(&js, &thread, None, thread::Activity::None, None).await.unwrap();
        let [listed] = &entries[0].attachments[..] else { panic!("{entries:?}") };
        assert_eq!(listed.attachment, files[0].attachment);
        let (_, bytes) =
            attachments::fetch(&js, &listed.handle, Some(&listed.attachment), listed.stored).await.unwrap();
        assert_eq!(bytes, b"# plan");
    }

    /// The members editor's writes change the channel's entry as `ants channel add|remove|set` does, refused as it
    /// refuses them, each telling the channel to read back.
    #[tokio::test]
    async fn a_channel_job_writes_the_entry_as_ants_channel_does() {
        use ants_proto::ChannelEntry;

        use crate::channel::{self, Change};

        let Some(server) = crate::test_support::Server::start() else { return };
        let (alice, bob) = (user("alice"), user("bob"));
        let creds = [server.creds_as(&alice, ants_proto::Role::Human), server.creds_as(&bob, ants_proto::Role::Human)];
        let nats: Vec<_> = creds.iter().map(|c| Nats::new(&server.url, Some(c), None).unwrap()).collect();
        let (client, js) = nats[0].connect("test").await.unwrap();
        let (_, bob_js) = nats[1].connect("test").await.unwrap();
        humans::ensure(&js).await.unwrap();
        humans::register(&js, &nats[0].me).await.unwrap();
        humans::register(&bob_js, &nats[1].me).await.unwrap();
        let kv = channel::open(&js).await.unwrap();
        let ops: ChannelId = "ops".parse().unwrap();
        channel::create(&kv, &ChannelEntry { members: vec![alice.clone()], ..ChannelEntry::new(ops.clone()) })
            .await
            .unwrap();
        let (me, sealing) = (nats[0].me.clone(), Sealing::of(&nats[0]));
        let (meta, admin) = (Err("unused".into()), Err("unused".into()));
        let ctx = Ctx { client, js, me, domain: None, meta, sealing, account: None, keyring: None, admin };
        let run = |change: Change| Job::Channel { channel: ops.clone(), change }.run(&ctx);

        let add = Change { add: vec![user("dev-b"), bob.clone()], ..Change::default() };
        let done = run(add.clone()).await.unwrap();
        assert_eq!((done.line.as_str(), done.channel), ("#ops: added dev-b, bob", Some(ops.clone())));
        let again = run(add).await.unwrap();
        assert_eq!(again.line, "nothing changed in #ops");
        assert_eq!(again.warnings[..2], ["dev-b is already a member of #ops", "bob is already a member of #ops"]);
        let removed = run(Change { remove: vec![bob.clone()], ..Change::default() }).await.unwrap();
        assert_eq!(removed.line, "#ops: removed bob");
        let set = Change {
            messenger: Some(Some(user("dev-m"))),
            owner: Some(Some(user("dev-a"))),
            description: Some(Some("  Deploys  ".to_owned())),
            ..Change::default()
        };
        let done = run(set).await.unwrap();
        assert_eq!(done.line, "#ops: messenger now dev-m, owner now dev-a, description changed");
        let stored = channel::get(&kv, &ops).await.unwrap().unwrap();
        let expected = ChannelEntry {
            members: vec![alice.clone(), user("dev-b")],
            messenger: Some(user("dev-m")),
            owner: Some(user("dev-a")),
            description: Some("Deploys".to_owned()),
            ..ChannelEntry::new(ops.clone())
        };
        assert_eq!(stored, expected);

        let human = run(Change { owner: Some(Some(bob.clone())), ..Change::default() }).await.unwrap_err();
        assert_eq!(format!("{human:#}"), "bob is a human, who needs no ownership: humans change every channel");
        let long = Change { description: Some(Some("x".repeat(1001))), ..Change::default() };
        assert!(run(long).await.is_err(), "over 1000 characters");
        let cleared = Change { messenger: Some(None), owner: Some(None), description: Some(None), ..Change::default() };
        let done = run(cleared).await.unwrap();
        assert_eq!(done.line, "#ops: messenger back to the default, no owner, no description");
        let stored = channel::get(&kv, &ops).await.unwrap().unwrap();
        assert_eq!((stored.messenger, stored.owner, stored.description), (None, None, None));
    }
}
