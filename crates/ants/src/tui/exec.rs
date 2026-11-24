//! What the command bar's sends and slash commands do over the CLI's connection, in the background while the UI goes
//! on: the same library code as `ants ask`, `ants post`, `ants notes`, `ants answer`, `ants react`, `ants cancel`,
//! `ants thread done|title|pin|mute...`, `ants dead replay|drop`, `ants notify` ([`super::notify`]), `ants remind list`
//! ([`super::remind`]) and `ants trail` ([`super::trail`]) run, each returning the line those print and the warnings
//! they log. The sends are in [`send`].

use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use ants_client::attachments::Listed;
use ants_daemon::records::reactions;
use ants_proto::{ChannelId, ChannelNotify, JsDomain, Machine, Pick, Reaction, ThreadId, UserId, Uuid};
use anyhow::{Result, anyhow, bail};
use async_nats::jetstream;
use futures::future::LocalBoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt as _};

use super::work::entries::{latest, waiting};
use super::{attach, notify, remind, trail};
use crate::admin::orders;
use crate::admin::pause;
use crate::messages::attachments::File;
use crate::messages::forward;
use crate::messages::send as cli;
use crate::messages::{cancel, dead};
use crate::people::sealed::{Keyring, Sealing};
use crate::people::{humans, quiet};
use crate::threads::thread::{self};
use crate::threads::thread_meta::{self, Marking, Meta};

mod send;

pub(crate) use cli::Quoted;

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
    pub(super) fn meta(&self) -> Result<&Meta> {
        self.meta.as_ref().map_err(|e| anyhow!("{e}"))
    }
}

/// What the bar asks to be done.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Job {
    /// Send `text` to `to` as `ants ask` does, on `thread` or a new one, quoting the message it replies to if any;
    /// with `team`, the team `to` leads, which the message was addressed to ([`ants_client::sending::addressed`]).
    Ask {
        to: UserId,
        thread: Option<ThreadId>,
        text: String,
        quote: Option<Quoted>,
        team: Option<UserId>,
    },
    /// Send `text`, which names no one, on a new thread as `ants ask` does: to the human's assistant, else the
    /// messenger ([`cli::unaddressed`]).
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
    ///
    /// [`reply_to`]: super::work::entries::reply_to
    /// [`dm_with`]: super::work::entries::dm_with
    Reply {
        thread: ThreadId,
        to: Option<UserId>,
        text: String,
        quote: Option<Quoted>,
    },
    /// Send `text` to the ant stuck on `thread` ([`stuck`]), which the human's message gives fresh turns.
    ///
    /// [`stuck`]: super::work::entries::stuck
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
    /// Change `team` as `ants team add|remove|set` does, refused as it refuses it.
    Team {
        team: UserId,
        change: crate::teams::change::Change,
    },
    /// Set the human's notify level of `channel`, as `ants channel notify` does.
    ChannelNotify {
        channel: ChannelId,
        level: ChannelNotify,
    },
    /// Change or show what holds the human's push notifications back, as `ants notify` does.
    Notify(notify::Job),
    /// List the human's reminders, as `ants remind list` does.
    Reminders,
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
    Add(AddAnt),
    Clone(CloneAnt),
    /// Retire `ant` by admin order, as `ants ant retire` does.
    Retire {
        ant: UserId,
    },
    /// Send the message of the dead letter at this stream sequence to its ant again and remove the letter, as `ants
    /// dead replay` does.
    DeadReplay(u64),
    /// Remove the dead letter at this stream sequence unreplayed, as `ants dead drop` does.
    DeadDrop(u64),
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
    /// Ask of the saved trails and the runs of trails what `ants trail` does.
    Trail(trail::Job),
}

/// The ant `name` to add on `machine` with `settings` by admin order, waiting up to `wait` for its outcome, as `ants
/// ant add` does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddAnt {
    pub name: UserId,
    pub machine: Machine,
    pub settings: serde_json::Map<String, serde_json::Value>,
    pub wait: Duration,
}

impl AddAnt {
    async fn run(self, ctx: &Ctx) -> Result<Done> {
        let Self { name, machine, settings, wait } = self;
        let said = orders::add::add(admin(ctx)?, &ctx.js, &name, machine, settings, wait).await?;
        Ok(Done::line(said.replace('\n', " ")))
    }
}

/// The ant `src` to clone as `new` with `settings` over its own by admin order to `machine`, else to the machine
/// serving it, waiting up to `wait` for its outcome, as `ants ant clone` does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloneAnt {
    pub src: UserId,
    pub new: UserId,
    pub machine: Option<Machine>,
    pub settings: serde_json::Map<String, serde_json::Value>,
    pub wait: Duration,
}

impl CloneAnt {
    async fn run(self, ctx: &Ctx) -> Result<Done> {
        let Self { src, new, machine, settings, wait } = self;
        let said = orders::clone::clone(admin(ctx)?, &ctx.js, &src, &new, machine, settings, wait).await?;
        Ok(Done::line(said.replace('\n', " ")))
    }
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
    /// Whether it changed the team directory, to read back.
    pub teams: bool,
    /// What to show in the pager: a table the CLI prints.
    pub page: Option<String>,
    /// The run of a trail planned, to start once confirmed.
    pub planned: Option<Box<trail::Planned>>,
    /// The dead letter it replayed or dropped, by stream sequence.
    pub dead: Option<u64>,
}

impl Done {
    pub(super) fn line(line: String) -> Self {
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
            Self::Ask { to, thread, text, quote, team } => {
                let to = ants_client::sending::Addressed { to, team };
                send::ask_addressed(ctx, to, thread, text, quote, false, &files).await
            }
            Self::Unaddressed { text } => {
                let to = cli::unaddressed(&ctx.js, &ctx.me, ctx.account.as_deref()).await;
                send::ask(ctx, to, None, text, None, false, &files).await
            }
            Self::Post { channel, thread, text, quote } => {
                send::post(ctx, channel, thread, text, quote, queue, &files).await
            }
            Self::Reply { thread, to, text, quote } => send::reply(ctx, thread, to, text, quote, queue, &files).await,
            Self::Grant { thread, text } => send::grant(ctx, thread, text, &files).await,
            Self::Restart { ant, now } => restart(ctx, &ant, now).await,
            Self::Cancel { thread, ants } => cancel_all(ctx, &thread, ants).await,
            Self::Answer { thread, question, picks } => answer(ctx, &thread, question, picks).await,
            Self::Done { thread, done } => {
                Ok(Done::line(thread_meta::marked_done(&ctx.js, ctx.meta()?, &thread, done).await?))
            }
            Self::Mark { thread, marking } => {
                Ok(Done::line(thread_meta::marked(&ctx.js, ctx.meta()?, &thread, marking).await?))
            }
            Self::Mute { thread, hold } => Ok(Done::line(quiet::muted(&ctx.js, me, &thread, hold).await?)),
            Self::Unmute { thread } => Ok(Done::line(quiet::unmuted(&ctx.js, me, &thread).await?)),
            Self::Channel { channel, change } => change_channel(ctx, channel, &change).await,
            Self::Team { team, change } => change_team(ctx, &team, &change).await,
            Self::ChannelNotify { channel, level } => {
                Ok(Done::line(quiet::channel_notify(&ctx.js, me, &channel, level).await?))
            }
            Self::Notify(job) => job.run(ctx).await,
            Self::Reminders => remind::listed(ctx).await,
            Self::Title { thread, text } => {
                Ok(Done::line(thread_meta::retitle(&ctx.js, ctx.meta()?, &ctx.sealing, &thread, &text, false).await?))
            }
            Self::Notes { thread, text } => send::notes(ctx, thread, text, &files).await,
            Self::React { thread, message, reaction } => react(ctx, &thread, message, reaction).await,
            Self::Open(listed) => Ok(Done::line(attach::open(&ctx.js, ctx.keyring.as_deref(), &listed).await?)),
            Self::Save(listed, to) => {
                Ok(Done::line(attach::save(&ctx.js, ctx.keyring.as_deref(), &listed, to.as_deref()).await?))
            }
            Self::Add(add) => add.run(ctx).await,
            Self::Clone(clone) => clone.run(ctx).await,
            Self::Retire { ant } => retire(ctx, &ant).await,
            Self::DeadReplay(seq) => {
                Ok(Done { line: dead::replayed(&ctx.js, seq).await?, dead: Some(seq), ..Done::default() })
            }
            Self::DeadDrop(seq) => {
                Ok(Done { line: dead::dropped(&ctx.js, seq).await?, dead: Some(seq), ..Done::default() })
            }
            Self::Forward { to, source, note } => send::forward(ctx, to, &source, &note, &files).await,
            Self::Pause { ant, paused } => pause(ctx, &ant, paused).await,
            Self::Attached { .. } => Err(anyhow!("files are attached to one message")),
            Self::Later(_) => Err(anyhow!("a message waits its turn once")),
            Self::Withdraw { ants, message, edit } => withdraw(ctx, &ants, message, edit.map(|e| *e)).await,
            Self::Trail(job) => job.run(ctx).await,
        }
    }
}

/// Send `ant` a restart order, at once when `now`.
async fn restart(ctx: &Ctx, ant: &UserId, now: bool) -> Result<Done> {
    let sent = crate::admin::restart::send(admin(ctx)?, &ctx.js, None, ant, now).await?;
    Ok(Done::line(crate::admin::restart::told(sent)?))
}

/// Answer the questions of message `question` on `thread`, still waiting on the human, with `picks`.
async fn answer(ctx: &Ctx, thread: &ThreadId, question: Uuid, picks: Vec<Pick>) -> Result<Done> {
    let me = &ctx.me.id;
    let entries = thread::history(&ctx.js, thread, None, thread::Activity::None, None).await?;
    waiting(&entries, me, thread, question)?;
    let envelope = cli::answering(&ctx.js, me, question, cli::Picks::Made(picks)).await?;
    let stored = cli::deliver(&ctx.js, ctx.domain.as_ref(), &envelope).await?;
    Ok(Done {
        line: format!("answered {} on thread {thread}", envelope.to),
        warnings: send::outbox(stored),
        ..Done::default()
    })
}

/// React with `reaction` to `message`, else to the latest message on `thread` not the human's.
async fn react(ctx: &Ctx, thread: &ThreadId, message: Option<Uuid>, reaction: Reaction) -> Result<Done> {
    let me = &ctx.me.id;
    let message = match message {
        Some(message) => message,
        None => {
            let entries = thread::history(&ctx.js, thread, None, thread::Activity::None, None).await?;
            latest(&entries, me).ok_or_else(|| anyhow!("no message on thread {thread} to react to"))?
        }
    };
    let changed = reactions::add(&ctx.js, ctx.domain.as_ref(), me, &message, &reaction).await?;
    Ok(Done::line(cli::reacted_line(&reaction, message, false, changed)))
}

/// Send `ant` an order pausing it, or resuming it unless `paused`.
async fn pause(ctx: &Ctx, ant: &UserId, paused: bool) -> Result<Done> {
    Ok(Done::line(pause::order(admin(ctx)?, &ctx.js, ant, None, paused, pause::WAIT).await?))
}

/// Write `change` to the entry of `channel` with the human's credentials ([`crate::channel::Change::write`]).
async fn change_channel(ctx: &Ctx, channel: ChannelId, change: &crate::channel::Change) -> Result<Done> {
    let kv = crate::channel::open(&ctx.js).await?;
    let humans = humans::load(&ctx.js, &ctx.me).await;
    let changed = change.write(&ctx.js, &kv, &channel, &humans).await?;
    let line = changed_line(&channel, change, &changed);
    Ok(Done { line, warnings: changed.warnings, channel: Some(channel), ..Done::default() })
}

/// Write `change` to `team` with the human's credentials ([`crate::teams::change::Change::write`]).
pub async fn change_team(ctx: &Ctx, team: &UserId, change: &crate::teams::change::Change) -> Result<Done> {
    let kv = crate::teams::open(&ctx.js, ctx.domain.as_ref()).await?;
    let humans = humans::load(&ctx.js, &ctx.me).await;
    let changed = change.write(&ctx.js, ctx.domain.as_ref(), &kv, team, &humans).await?;
    Ok(Done { line: change.line(team, &changed), teams: true, ..Done::default() })
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
        match crate::ants::queue::withdrawn(&ctx.js, ctx.domain.as_ref(), &ctx.me.id, ant, message).await {
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

/// The human's admin credentials, which sign admin orders, or why there are none.
fn admin(ctx: &Ctx) -> Result<&orders::Admin> {
    ctx.admin.as_ref().map_err(|e| anyhow!("{e}"))
}

/// Retire `ant` by admin order, as `ants ant retire` does.
async fn retire(ctx: &Ctx, ant: &UserId) -> Result<Done> {
    let said =
        orders::retire::retire(admin(ctx)?, &ctx.js, ctx.domain.as_ref(), ant, None, orders::retire::WAIT).await?;
    Ok(Done::line(said.replace('\n', " ")))
}

#[cfg(test)]
mod tests {
    use ants_client::entry::Kind;
    use ants_daemon::bus;
    use ants_proto::{Question, Quote};

    use super::*;
    use crate::connection::Nats;
    use crate::messages::attachments;
    use crate::test_support::live::user;

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

        let ask = Job::Ask { to: bob.clone(), thread: None, text: "lunch?".into(), quote: None, team: None };
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
            team: None,
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
        let ask = Job::Ask { to: user("dev-a"), thread: None, text: "see the plan".into(), quote: None, team: None };
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

        // One over the server's max_payload is refused before its files are stored, naming its size and the limit.
        let big = "x".repeat(1024 * 1024);
        let files_before =
            js.get_stream(ants_daemon::hub::files_config().name).await.unwrap().info().await.unwrap().state.messages;
        for job in [
            Job::Ask { to: user("dev-a"), thread: Some(thread.clone()), text: big.clone(), quote: None, team: None },
            Job::Post { channel: "ops".parse().unwrap(), thread: None, text: big.clone(), quote: None },
            Job::Notes { thread: thread.clone(), text: big.clone() },
        ] {
            let refused = Job::Attached { job: Box::new(job), files: Rc::clone(&files) }.run(&ctx).await.unwrap_err();
            let refused = refused.to_string();
            assert!(refused.starts_with("the message is 1.0 MiB; one message carries at most 1023 KiB"), "{refused}");
            assert!(refused.ends_with("put the text in a file and attach it with /attach instead"), "{refused}");
        }
        let files_after =
            js.get_stream(ants_daemon::hub::files_config().name).await.unwrap().info().await.unwrap().state.messages;
        assert_eq!(files_after, files_before, "nothing stored");
        let after = thread::history(&js, &thread, None, thread::Activity::None, None).await.unwrap();
        assert_eq!(after.len(), entries.len(), "nothing sent");
    }

    /// The bar replays a dead letter to its ant, or drops it, as `ants dead replay|drop` do: once gone, it is no more.
    #[tokio::test]
    async fn a_dead_letter_is_replayed_or_dropped_as_ants_dead_does() {
        use ants_proto::{DeadLetter, Envelope};

        let Some(server) = crate::test_support::Server::start() else { return };
        let (alice, dev_a) = (user("alice"), user("dev-a"));
        let creds = server.creds_as(&alice, ants_proto::Role::Human);
        let nats = Nats::new(&server.url, Some(&creds), None).unwrap();
        let (client, js) = nats.connect("test").await.unwrap();
        let _ = js.create_stream(ants_daemon::hub::stream_config()).await;
        let (_, ant_js) = server.connect(&dev_a).await;
        let ask = Envelope::new(alice, dev_a.clone(), "build it");
        let bury = async || {
            let dead = DeadLetter::of_envelope(dev_a.clone(), 1, 5, "harness crashed", ask.clone());
            bus::publish_json(&ant_js, dead.subject(), &dead, None).await.unwrap()
        };
        let (me, sealing) = (nats.me.clone(), Sealing::of(&nats));
        let (meta, admin) = (Err("unused".into()), Err("unused".into()));
        let ctx = Ctx { client, js: js.clone(), me, domain: None, meta, sealing, account: None, keyring: None, admin };

        let seq = bury().await;
        let done = Job::DeadReplay(seq).run(&ctx).await.unwrap();
        assert_eq!((done.line, done.dead), (format!("replayed to dev-a on thread {}", ask.thread_id), Some(seq)));
        let stream = js.get_stream(ants_proto::STREAM).await.unwrap();
        let replayed = stream.get_last_raw_message_by_subject(&ask.subject()).await.unwrap();
        assert_eq!(serde_json::from_slice::<Envelope>(&replayed.payload).unwrap(), ask, "sent again as it was");
        let gone = format!("{:#}", Job::DeadReplay(seq).run(&ctx).await.unwrap_err());
        assert!(gone.contains(&format!("reading message {seq}")), "{gone}");

        let seq = bury().await;
        let done = Job::DeadDrop(seq).run(&ctx).await.unwrap();
        assert_eq!((done.line, done.dead), (format!("dropped dead letter {seq}"), Some(seq)));
        assert!(ants_daemon::delivery::dead::list(&stream).await.unwrap().is_empty());
    }

    /// `/add` and `/clone` send the orders `ants ant add` and `ants ant clone` send, signed with the human's admin
    /// credentials, and tell what came of them as those print it: the ant added once `ants signer` signed its key on
    /// the order, its table holding the flags given; a clone into a name listed already refused, with why.
    #[tokio::test]
    async fn add_and_clone_from_the_bar_send_the_orders_of_ants_ant_add_and_clone() {
        use ants_daemon::admin::ConfigFile;
        use ants_proto::Role;

        use crate::auth::{Issue, Registered};
        use crate::credentials::issued::Kind;
        use crate::test_support::orders::{WAIT, host_of_m1, with_signer};
        use crate::tui::model::Action;

        let Some(server) = crate::test_support::Server::start() else { return };
        let (alice, bob) = (user("alice"), user("bob"));
        let (_, h_js) = server.connect_as(&bob, Role::Human).await;
        ants_daemon::bus::ensure_stream(&h_js).await.unwrap();
        ants_daemon::bus::ensure_colony(&h_js, ants_proto::COLONY_TTL).await.unwrap();
        h_js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
        h_js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
        h_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
        let creds = server.dir().join("alice-admin.creds");
        let device = Registered { label: Some("laptop"), admin: true, ..Registered::default() };
        let rights = ants_proto::Permissions::for_user(&alice, Role::Human);
        Issue::new(&server.dir().join("auth"), &alice, Kind::Human, rights).with(&device).into_file(&creds).unwrap();
        let nats = Nats::new(&server.url, Some(&creds), None).unwrap();
        let (client, js) = nats.connect("test").await.unwrap();
        let work = server.dir().join("work");
        std::fs::create_dir_all(&work).unwrap();
        let path = server.dir().join("config.toml");
        std::fs::write(&path, format!("harness = \"copilot\"\n\n[[ant]]\nname = \"dev-a\"\nworkdir = {work:?}\n"))
            .unwrap();
        let triggers = crate::reload::Triggers::new(path.clone(), None, Duration::from_secs(3600)).unwrap();
        let (ants, task) = host_of_m1(&server, &js, true, Some(ConfigFile::new(path.clone(), triggers.edits()))).await;
        let (me, sealing) = (nats.me.clone(), Sealing::of(&nats));
        let (meta, admin) = (Err("unused".into()), orders::Admin::of(&nats).map_err(|e| format!("{e:#}")));
        let ctx = Ctx { client, js, me, domain: None, meta, sealing, account: None, keyring: None, admin };
        let mut m = crate::tui::test_support::model();
        let mut job = |text: String| match m.submit(text.clone(), chrono::Utc::now()) {
            Some(Action::Run(job, _)) => job,
            other => panic!("{text}: {other:?}"),
        };

        let add = job(format!("/add @dev-b --on m1 --workdir {} --model gpt-5", work.display()));
        let done = with_signer(&server, Box::pin(add.run(&ctx))).await.unwrap();
        let line = &done.line;
        assert!(
            line.starts_with("dev-b added on m1 with key U") && line.ends_with("; its daemon starts it."),
            "{line}"
        );
        let loaded = ants_daemon::config::load(&path).unwrap();
        let ant = loaded.ants.iter().find(|a| a.name == user("dev-b")).expect("listed");
        assert_eq!(ant.model.as_deref(), Some("gpt-5"));
        assert_eq!(ant.workdir.canonicalize().unwrap(), work.canonicalize().unwrap());

        let clone = job(format!("/clone dev-a dev-b --home {}", server.dir().join("home-b").display()));
        let refused = format!("{:#}", Box::pin(clone.run(&ctx)).await.unwrap_err());
        assert!(refused.starts_with("the daemon of m1 refused to clone dev-a as dev-b: "), "{refused}");
        assert!(refused.contains("lists dev-b already"), "{refused}");
        drop(ants);
        tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
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

    /// The team editor's writes change the team as `ants team add|remove|set` does, refused as it refuses them, each
    /// telling the teams to read back.
    #[tokio::test]
    async fn a_team_job_changes_the_team_as_ants_team_does() {
        use ants_daemon::records::direct_kv::DirectKv;
        use ants_proto::{AntIdentity, IDENTITIES_BUCKET, Team, TeamRefusal};

        use crate::teams::change::Change;

        let Some(server) = crate::test_support::Server::start() else { return };
        let (alice, bob) = (user("alice"), user("bob"));
        let creds = [server.creds_as(&alice, ants_proto::Role::Human), server.creds_as(&bob, ants_proto::Role::Human)];
        let nats: Vec<_> = creds.iter().map(|c| Nats::new(&server.url, Some(c), None).unwrap()).collect();
        let (client, js) = nats[0].connect("test").await.unwrap();
        let (_, bob_js) = nats[1].connect("test").await.unwrap();
        humans::ensure(&js).await.unwrap();
        humans::register(&js, &nats[0].me).await.unwrap();
        humans::register(&bob_js, &nats[1].me).await.unwrap();
        ants_daemon::hub::create_identities(&js).await.unwrap();
        ants_daemon::hub::ensure_bucket(&js, ants_daemon::hub::Object::Teams).await.unwrap();
        let identities = DirectKv::whole(&js, None, IDENTITIES_BUCKET).await.unwrap();
        for ant in ["dev-a", "dev-b"] {
            let record = AntIdentity::new(user(ant), format!("{ant}-id"), chrono::Utc::now());
            identities.put(ant, serde_json::to_vec(&record).unwrap()).await.unwrap();
        }
        let kv = crate::teams::open(&js, None).await.unwrap();
        let backend = user("backend");
        let team = Team::new(backend.clone(), user("dev-a"), alice.clone());
        ants_daemon::teams::modify(&kv, |d| d.create(team.clone(), None)).await.unwrap();
        let (me, sealing) = (nats[0].me.clone(), Sealing::of(&nats[0]));
        let (meta, admin) = (Err("unused".into()), Err("unused".into()));
        let ctx = Ctx { client, js, me, domain: None, meta, sealing, account: None, keyring: None, admin };
        let run = |change: Change| Job::Team { team: backend.clone(), change }.run(&ctx);

        let done = run(Change { add: vec![user("dev-b")], ..Change::default() }).await.unwrap();
        assert_eq!((done.line.as_str(), done.teams), ("team backend: added dev-b", true));
        let unknown = run(Change { add: vec![user("dev-z")], ..Change::default() }).await.unwrap_err();
        assert_eq!(unknown.to_string(), TeamRefusal::UnknownAnt { name: user("dev-z") }.to_string());
        let human = run(Change { add: vec![bob.clone()], ..Change::default() }).await.unwrap_err();
        assert!(human.to_string().starts_with("bob is a human"), "{human}");
        let lead = run(Change { remove: vec![user("dev-a")], ..Change::default() }).await.unwrap_err();
        assert!(lead.to_string().contains("dev-a"), "the lead stays: {lead}");
        let set = Change {
            lead: Some(user("dev-b")),
            owner: Some(Some(bob.clone())),
            description: Some(Some("The API.".to_owned())),
            remove: vec![user("dev-a")],
            ..Change::default()
        };
        let done = run(set).await.unwrap();
        assert_eq!(done.line, "team backend: removed dev-a, lead now dev-b, owner now bob, description changed");
        let read = crate::teams::read_of(&ctx.js, None).await.unwrap().unwrap();
        let stored = read.team(&backend).unwrap();
        assert_eq!((stored.lead.clone(), stored.members.clone()), (Some(user("dev-b")), vec![user("dev-b")]));
        assert_eq!((stored.owner.clone(), stored.description.as_deref()), (Some(bob), Some("The API.")));
    }
}
