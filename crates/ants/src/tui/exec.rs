//! What the command bar's sends and slash commands do over the CLI's connection, in the background while the UI goes
//! on: the same library code as `ants ask`, `ants post`, `ants answer`, `ants react`, `ants cancel`, `ants thread
//! done|title|pin|mute...` run, each returning the line those print and the warnings they log.

use std::rc::Rc;
use std::time::Duration;

use ants_daemon::{bus, reactions};
use ants_proto::{ChannelId, Choice, EscalationReason, JsDomain, Reaction, ThreadId, UserId, Uuid};
use anyhow::{Result, anyhow};
use async_nats::jetstream;
use futures::future::LocalBoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt as _};

use crate::cmd::{cancel, send};
use crate::sealed::Sealing;
use crate::thread::{self, Entry, Kind};
use crate::thread_meta::{self, Marking, Meta};
use crate::{humans, quiet};

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
}

impl Ctx {
    fn meta(&self) -> Result<&Meta> {
        self.meta.as_ref().map_err(|e| anyhow!("{e}"))
    }
}

/// What the bar asks to be done.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Job {
    /// Send `text` to `to` as `ants ask` does, on `thread` or a new one; with `steer` steering the ant.
    Ask {
        to: UserId,
        thread: Option<ThreadId>,
        text: String,
        steer: bool,
    },
    /// Post `text` to `channel` as `ants post` does, on `thread` or a new one.
    Post {
        channel: ChannelId,
        thread: Option<ThreadId>,
        text: String,
    },
    /// Reply `text` on `thread` to the ant that last messaged the human there ([`reply_to`]).
    Reply {
        thread: ThreadId,
        text: String,
        steer: bool,
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
    /// Answer the question asked of the human on `thread` ([`open_question`]), as `ants answer` does.
    Answer {
        thread: ThreadId,
        choice: Choice,
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
    Title {
        thread: ThreadId,
        text: String,
    },
    /// React to `message`, else to the latest message on `thread` not from the human ([`latest`]).
    React {
        thread: ThreadId,
        message: Option<Uuid>,
        reaction: Reaction,
    },
}

/// What a job did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Done {
    /// What the CLI prints for it.
    pub line: String,
    /// What the CLI warns of.
    pub warnings: Vec<String>,
    /// The thread it started, to show.
    pub open: Option<ThreadId>,
}

impl Done {
    fn line(line: String) -> Self {
        Self { line, warnings: Vec::new(), open: None }
    }
}

/// How a job went, with the bar's text it came from.
#[derive(Debug)]
pub struct Outcome {
    pub result: Result<Done>,
    pub text: String,
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
        self.jobs.push(async move { Outcome { result: job.run(&ctx).await, text } }.boxed_local());
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
    async fn run(self, ctx: &Ctx) -> Result<Done> {
        let me = &ctx.me.id;
        match self {
            Self::Ask { to, thread, text, steer } => ask(ctx, to, thread, text, steer).await,
            Self::Post { channel, thread, text } => post(ctx, channel, thread, text).await,
            Self::Reply { thread, text, steer } => {
                let entries = thread::history(&ctx.js, &thread, None, false, None).await?;
                let humans = humans::load(&ctx.js, &ctx.me).await;
                let human = |u: &UserId| humans.contains(u);
                let to = reply_to(&entries, me, human).or_else(|| dm_with(&entries, me, human));
                let to = to.unwrap_or_else(UserId::messenger);
                ask(ctx, to, Some(thread), text, steer).await
            }
            Self::Grant { thread, text } => {
                let entries = thread::history(&ctx.js, &thread, None, false, None).await?;
                let ant = stuck(&entries).ok_or_else(|| anyhow!("no ant is stuck on thread {thread}"))?;
                ask(ctx, ant, Some(thread), text, false).await
            }
            Self::Cancel { thread, ants } => {
                let mut lines = Vec::new();
                let mut failed = None;
                for ant in ants {
                    match cancel::cancelled(&ctx.client, me, ant, thread.clone()).await {
                        Ok(line) => lines.push(line),
                        Err(e) => failed = Some(e),
                    }
                }
                match failed {
                    Some(e) if lines.is_empty() => Err(e),
                    failed => {
                        let warnings = failed.map(|e| format!("{e:#}")).into_iter().collect();
                        Ok(Done { line: lines.join("; "), warnings, open: None })
                    }
                }
            }
            Self::Answer { thread, choice } => {
                let entries = thread::history(&ctx.js, &thread, None, false, None).await?;
                let question = open_question(&entries, me)
                    .ok_or_else(|| anyhow!("no question to {me} waits for an answer on thread {thread}"))?;
                let envelope = send::answering(&ctx.js, me, question, choice).await?;
                let stored = send::deliver(&ctx.js, ctx.domain.as_ref(), &envelope).await?;
                Ok(Done {
                    line: format!("answered {} on thread {thread}", envelope.to),
                    warnings: outbox(stored),
                    open: None,
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
            Self::Title { thread, text } => {
                Ok(Done::line(thread_meta::retitle(&ctx.js, ctx.meta()?, &ctx.sealing, &thread, &text, false).await?))
            }
            Self::React { thread, message, reaction } => {
                let message = match message {
                    Some(message) => message,
                    None => {
                        let entries = thread::history(&ctx.js, &thread, None, false, None).await?;
                        latest(&entries, me).ok_or_else(|| anyhow!("no message on thread {thread} to react to"))?
                    }
                };
                let changed = reactions::add(&ctx.js, ctx.domain.as_ref(), me, &message, &reaction).await?;
                Ok(Done::line(send::reacted_line(&reaction, message, false, changed)))
            }
        }
    }
}

/// The warning for a message kept in the leaf node's outbox.
fn outbox(stored: bus::Stored) -> Vec<String> {
    match stored {
        bus::Stored::Outbox => vec![send::OUTBOX.to_owned()],
        bus::Stored::Hub => Vec::new(),
    }
}

/// Send `text` to `to` on `thread`, or a new one, as `ants ask` does, with the warnings it prints for an ant no daemon
/// serves now.
async fn ask(ctx: &Ctx, to: UserId, thread: Option<ThreadId>, text: String, steer: bool) -> Result<Done> {
    let new = thread.is_none();
    let envelope = send::ask_envelope(&ctx.me, thread, to, text);
    let mut envelope = if steer { envelope.steering() } else { envelope };
    send::seal_to_human(&ctx.js, &ctx.me, &ctx.sealing, &mut envelope).await?;
    let (mut warnings, _) = send::unserved(&ctx.js, &ctx.me, &envelope.to).await;
    let stored = send::deliver(&ctx.js, ctx.domain.as_ref(), &envelope).await?;
    warnings.extend(outbox(stored));
    let verb = if steer { "steered" } else { "sent to" };
    Ok(Done {
        line: format!("{verb} {} on thread {}", envelope.to, envelope.thread_id),
        warnings,
        open: new.then_some(envelope.thread_id),
    })
}

/// Post `text` to `channel` on `thread`, or a new one, as `ants post` does, with the warnings it prints.
async fn post(ctx: &Ctx, channel: ChannelId, thread: Option<ThreadId>, text: String) -> Result<Done> {
    let new = thread.is_none();
    let posting = send::Posting::compose(&ctx.js, &ctx.me, channel, thread, text).await?;
    let (stored, notified) = posting.deliver(&ctx.js, &ctx.me).await?;
    let post = &posting.post;
    let mut line = format!("posted to #{} on thread {}", post.channel, post.thread_id);
    if !notified.is_empty() {
        let names: Vec<&str> = notified.iter().map(UserId::as_str).collect();
        line = format!("{line}; notified {}", names.join(", "));
    }
    let mut warnings = posting.warnings.clone();
    if stored.is_none() {
        warnings.push(send::OUTBOX.to_owned());
    }
    Ok(Done { line, warnings, open: new.then(|| post.thread_id.clone()) })
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

/// The latest question asked of `me` among `entries` that no later message of `me` answered.
pub fn open_question(entries: &[Entry], me: &UserId) -> Option<Uuid> {
    let answered = |id: Uuid| {
        entries.iter().any(|e| e.from.as_ref() == Some(me) && e.answer.as_ref().is_some_and(|a| a.question == id))
    };
    entries
        .iter()
        .rev()
        .filter(|e| e.question.is_some() && e.to.as_ref() == Some(me))
        .filter_map(|e| e.id)
        .find(|id| !answered(*id))
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
    use ants_proto::{Answer, Question};

    use super::*;
    use crate::live::tests::{message, user};

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

    /// What the bar sends to another human, a reply on their thread and its title are sealed for both.
    #[tokio::test]
    async fn the_bar_seals_messages_to_a_human_and_titles_on_their_thread() {
        use ants_proto::Role;

        let Some(server) = crate::auth::tests::Server::start() else { return };
        let (alice, bob) = (user("alice"), user("bob"));
        let creds = [server.creds_as(&alice, Role::Human), server.creds_as(&bob, Role::Human)];
        let nats: Vec<_> = creds.iter().map(|c| crate::Nats::new(&server.url, Some(c), None).unwrap()).collect();
        let (client, js) = nats[0].connect("test").await.unwrap();
        let (_, bob_js) = nats[1].connect("test").await.unwrap();
        let _ = js.create_stream(ants_daemon::hub::threads_config()).await;
        let meta = thread_meta::open(&nats[0], &js).await.map_err(|e| format!("{e:#}"));
        let (me, sealing) = (nats[0].me.clone(), Sealing::of(&nats[0]));
        let ctx = Ctx { client, js, me, domain: None, meta, sealing };

        let ask = Job::Ask { to: bob.clone(), thread: None, text: "lunch?".into(), steer: false };
        let thread = Box::pin(ask.run(&ctx)).await.unwrap().open.unwrap();
        let reply = Job::Reply { thread: thread.clone(), text: "at noon".into(), steer: false };
        assert_eq!(Box::pin(reply.run(&ctx)).await.unwrap().line, format!("sent to bob on thread {thread}"));
        let title = Job::Title { thread: thread.clone(), text: "Team lunch".into() };
        Box::pin(title.run(&ctx)).await.unwrap();

        let unread = crate::inbox::Inbox::open(&bob_js, &bob).await.unwrap().unread(&Default::default()).await.unwrap();
        assert!(unread.iter().all(|e| e.sealed.is_some() && e.text.is_empty()), "{unread:?}");
        let keyring = crate::sealed::Keyring::load(&nats[1], &bob_js).await.unwrap();
        let texts: Vec<String> = unread.into_iter().map(|e| crate::sealed::read(e, Some(&keyring)).0.text).collect();
        assert_eq!(texts, ["lunch?", "at noon"]);
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
    fn the_question_to_answer_is_the_latest_unanswered_and_the_message_to_react_to_the_latest_of_another() {
        let (thread, me) = (ThreadId::generate(), user("alice"));
        let question = Question { text: "which?".into(), options: vec!["a".into(), "b".into()], free_text: true };
        let mut asked = message(1, &thread, "dev-a", "alice", "which?");
        asked.question = Some(question);
        let mut answered = message(2, &thread, "alice", "dev-a", "a");
        answered.answer = Some(Answer { question: asked.id.unwrap(), option: Some(1) });
        assert_eq!(open_question(std::slice::from_ref(&asked), &me), asked.id);
        assert_eq!(open_question(&[asked.clone(), answered.clone()], &me), None);
        assert_eq!(latest(&[asked.clone(), answered], &me), asked.id);
    }
}
