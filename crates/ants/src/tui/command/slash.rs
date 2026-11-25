//! What the slash commands do: the action each takes on the thread open or selected, or on what its arguments name,
//! and what each needs of the view shown to take it ([`Needs`]), for want of which it is refused and the palette dims
//! it.

use ants_proto::{ApprovalRequest, ChannelId, ChannelNotify, Questions, ThreadId, UserId, Uuid, Verdict};
use chrono::{DateTime, Utc};

use super::{COMMANDS, Goto, Needs, Notify, OnThread, Slash, Trail, parse};
use crate::cmd::assistant::AssistantCommand;
use crate::messages::forward;
use crate::people::quiet;
use crate::tui::exec::Job;
use crate::tui::model::{Action, Focus, Model, Page, Prompt};
use crate::tui::{assistant, attach, nav, thread, trail};
use crate::{live, tools};

/// What `/grant` without a text sends the stuck ant.
pub const GRANT: &str = "Carry on: fresh turns granted.";

/// Why a slash command cannot act on the view shown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unmet {
    /// As the bar's status line tells it once the command is run.
    told: String,
    /// As the palette says it beside the command it dims, where it words it otherwise.
    listed: Option<String>,
}

impl Unmet {
    /// As the palette says it.
    pub fn listed(self) -> String {
        self.listed.unwrap_or(self.told)
    }
}

impl From<String> for Unmet {
    fn from(told: String) -> Self {
        Self { told, listed: None }
    }
}

impl From<Unmet> for String {
    fn from(unmet: Unmet) -> Self {
        unmet.told
    }
}

impl Model {
    /// The action the slash command `slash` takes, or why it cannot be taken.
    pub(super) fn command(&mut self, slash: Slash, now: DateTime<Utc>) -> Result<Option<Action>, String> {
        let run = |job| Ok(Some(Action::Run(job, String::new())));
        match slash {
            Slash::Approve { id, always } => {
                let verdict = if always { Verdict::AllowAlways } else { Verdict::AllowOnce };
                self.decision(id.as_deref(), verdict, now).map(Some)
            }
            Slash::Reject { id } => self.decision(id.as_deref(), Verdict::Reject, now).map(Some),
            Slash::Grant(text) => {
                run(Job::Grant { thread: self.target(now)?, text: text.unwrap_or_else(|| GRANT.to_owned()) })
            }
            Slash::Cancel => {
                let (thread, ants) = self.at_work(now)?;
                run(Job::Cancel { thread, ants })
            }
            Slash::Answer(text) => run(self.answering(&text, now)?),
            Slash::Done(done) => run(Job::Done { thread: self.target(now)?, done }),
            Slash::Mark(marking) => run(Job::Mark { thread: self.target(now)?, marking }),
            Slash::Mute(hold) => run(Job::Mute { thread: self.target(now)?, hold: hold.unwrap_or(quiet::MUTE_FOR) }),
            Slash::Unmute => run(Job::Unmute { thread: self.target(now)? }),
            Slash::Notify(Notify::Level(level)) => {
                let (channel, _) =
                    self.channel_on_view().ok_or("/notify sets a channel's notify level: show the channel first")?;
                Ok(Some(self.set_level(channel, level)))
            }
            Slash::Notify(Notify::Push(job)) => run(Job::Notify(job)),
            Slash::Snooze(duration) => Ok(Some(self.set_snooze(self.target(now)?, Some(now + duration), now))),
            Slash::Unsnooze => Ok(Some(self.set_snooze(self.target(now)?, None, now))),
            Slash::Remind(text) => {
                let message = match self.page() {
                    Page::Thread(page) => page.selected_id(),
                    _ => None,
                };
                self.remind_as(self.target(now)?, message, &text, now).map(Some)
            }
            Slash::Unremind => Ok(Some(self.set_reminder(self.target(now)?, None))),
            Slash::Reminders => run(Job::Reminders),
            Slash::Schedules => run(Job::Schedules),
            Slash::Title(text) => run(Job::Title { thread: self.target(now)?, text }),
            Slash::Notes(text) => run(Job::Notes { thread: self.target(now)?, text }),
            Slash::React(reaction) => {
                let message = match self.page() {
                    Page::Channel(page) => page.post(&self.data, &self.ui).map(|p| p.id),
                    _ => None,
                };
                run(Job::React { thread: self.target(now)?, message, reaction })
            }
            Slash::Goto(to) => self.goto(to),
            Slash::Search(query) => self.search(query, now).map(Some),
            Slash::Digest(since) => self.digest(since.as_deref(), now).map(Some),
            Slash::Tldr => Ok(self.tldr(self.target(now)?)),
            Slash::Tree => {
                let thread = self.target(now)?;
                self.tree(thread);
                Ok(None)
            }
            Slash::Forward { to, note } => {
                let source = self.forwardable(now)?;
                source.check().map_err(|e| format!("{e:#}"))?;
                let ants_client::sending::Addressed { to, team } = self.addressed(to)?;
                run(Job::Forward { to, source, note, team })
            }
            Slash::Save(to) => run(Job::Save(Box::new(self.marked()?), to.map(|to| attach::expand(&to)))),
            Slash::Add(add) => run(Job::Add(add)),
            Slash::Clone(clone) => run(Job::Clone(clone)),
            Slash::Retire(ant) => run(Job::Retire { ant }),
            Slash::Channel { channel, manage } => run(Job::ManageChannel { channel, manage }),
            Slash::Assistant(AssistantCommand::Disable(args)) if !args.retire => {
                self.prompt = Some(Prompt::Disable(assistant::Disabling(args)));
                Ok(None)
            }
            Slash::Assistant(command) => run(Job::Assistant(command)),
            Slash::Dead(dead) => self.dead(dead, now),
            Slash::Trail(Trail::Run { trail, conductor, args, limits, thread }) => {
                let thread = thread.map(|on| self.on_thread(on, now)).transpose()?;
                run(Job::Trail(trail::Job::Plan { trail, conductor, args, limits, thread }))
            }
            Slash::Trail(Trail::Job(job)) => run(Job::Trail(job)),
            Slash::Attach(_) | Slash::Detach => Ok(None),
            Slash::Quit => Ok(Some(Action::Quit)),
        }
    }

    /// The answer `/answer TEXT` gives the card the keys answer: its question alone takes all of `text`, several a
    /// word each, quotes holding a typed answer together.
    fn answering(&self, text: &str, now: DateTime<Utc>) -> Result<Job, String> {
        let (thread, question, asked) = self.question(now)?;
        let words = match asked.questions.len() {
            1 => vec![text.to_owned()],
            _ => parse::quoted_words(text),
        };
        let picks = asked.picks(&words).map_err(|e| e.to_string())?;
        let (answer, _) = asked.answer(question, picks).map_err(|e| e.to_string())?;
        Ok(Job::Answer { thread, question, picks: answer.picks })
    }

    /// Why what a slash command `needs`, given no argument naming another target, is not here at `now`, if it is not:
    /// what the command taking it is refused with, and what the palette dims it with.
    pub(super) fn need(&self, needs: Needs, now: DateTime<Utc>) -> Result<(), Unmet> {
        match needs {
            Needs::Nothing => {}
            Needs::Thread => _ = self.target(now)?,
            Needs::Request => _ = self.request(now)?,
            Needs::BusyAnt => _ = self.at_work(now)?,
            Needs::OpenThread => self.thread_open()?,
            Needs::Question => _ = self.question(now)?,
            Needs::Attachment => _ = self.marked()?,
            Needs::Letter => _ = self.letter(None, now)?,
            Needs::Forwardable => _ = self.forwardable(now)?,
        }
        Ok(())
    }

    /// Why the slash command `name`, without the arguments naming another target, cannot act here at `now`, if it
    /// cannot, as the palette says it.
    pub(in crate::tui) fn unfit(&self, name: &str, now: DateTime<Utc>) -> Option<String> {
        let row = COMMANDS.iter().find(|row| row.name == name)?;
        self.need(row.needs, now).err().map(Unmet::listed)
    }

    /// The thread slash commands act on: the open one, else the one selected in the view.
    pub fn target(&self, now: DateTime<Utc>) -> Result<ThreadId, String> {
        let data = &self.data;
        let thread = match self.page() {
            Page::Thread(page) => Some(page.history.thread.clone()),
            Page::Home(page) => page.thread(data, now),
            Page::Work(page) => page.thread(data),
            Page::Channel(page) => page.post(data, &self.ui).map(|p| p.thread.clone()),
            Page::Tree(page) => Some(page.root.clone()),
            Page::Ant(page) => page.thread(data),
            Page::Dm(_) | Page::Search(_) | Page::Credentials(_) | Page::Held(_) | Page::Team(_) => None,
        };
        thread.ok_or_else(|| "no thread open or selected".to_owned())
    }

    /// The first tool request waiting on the thread slash commands act on.
    fn request(&self, now: DateTime<Utc>) -> Result<&ApprovalRequest, Unmet> {
        let thread = self.target(now)?;
        self.data.live.approvals.iter().find(|r| r.thread_id == thread).ok_or_else(|| Unmet {
            told: format!("no tool request waits on thread {thread}"),
            listed: Some("no tool request waits on the thread".to_owned()),
        })
    }

    /// The thread slash commands act on, with the ants at work on it now.
    fn at_work(&self, now: DateTime<Utc>) -> Result<(ThreadId, Vec<UserId>), Unmet> {
        let thread = self.target(now)?;
        let ants = self.busy(&thread);
        if ants.is_empty() {
            return Err(Unmet {
                told: format!("no ant is at work on thread {thread}"),
                listed: Some("no ant is at work on the thread".to_owned()),
            });
        }
        Ok((thread, ants))
    }

    /// What `/forward` hands on: the message selected on the thread open, else the TL;DR of the thread slash commands
    /// act on.
    fn forwardable(&self, now: DateTime<Utc>) -> Result<forward::Source, Unmet> {
        if let Page::Thread(page) = self.page()
            && let Some(entry) = page.chosen()
        {
            return Ok(forward::Source::Message(Box::new(entry.clone())));
        }
        let thread = self.target(now)?;
        let tldr = self.data.cards.tldr.get(&thread).ok_or_else(|| Unmet {
            told: format!("no TL;DR of thread {thread} yet: S or /tldr asks for one, then /forward again"),
            listed: Some("no message selected and no TL;DR of the thread yet".to_owned()),
        })?;
        Ok(forward::Source::Thread(Box::new(tldr.clone())))
    }

    /// Whether a thread is open, as `/later` needs to reply on it.
    pub(super) fn thread_open(&self) -> Result<(), Unmet> {
        match self.page() {
            Page::Thread(_) => Ok(()),
            _ => Err(Unmet {
                told: "/later replies on an open thread: open one first".to_owned(),
                listed: Some("no thread open".to_owned()),
            }),
        }
    }

    /// The marked attachment of the message selected in the view.
    fn marked(&self) -> Result<ants_client::attachments::Listed, Unmet> {
        self.attachment().ok_or_else(|| "no attachment on the selected message".to_owned().into())
    }

    /// The marked attachment of the message selected in the view, if any.
    pub fn attachment(&self) -> Option<ants_client::attachments::Listed> {
        match self.page() {
            Page::Thread(page) => page.attachment().cloned(),
            Page::Channel(page) => page.attachment(&self.data, &self.ui).cloned(),
            _ => None,
        }
    }

    /// The questions `/answer` answers at `now`, the message asking them and its thread: on an open thread, those of
    /// the card the keys answer; else the latest to the human with no answer read on the thread selected, those its
    /// keys answer on home.
    fn question(&self, now: DateTime<Utc>) -> Result<(ThreadId, Uuid, Questions), String> {
        let thread = self.target(now)?;
        let asked = match self.page() {
            Page::Thread(page) => page.question(&self.data, now).map(|(id, asked)| (id, asked.clone())),
            Page::Home(page) => page.question(&self.data, now).map(|asked| (asked.id, asked.question.clone())),
            _ => self.data.question(&thread).map(|asked| (asked.id, asked.question.clone())),
        };
        let me = self.data.me();
        asked
            .map(|(id, asked)| (thread.clone(), id, asked))
            .ok_or_else(|| format!("no question to {me} waits for an answer on thread {thread}"))
    }

    /// The ants at work on `thread` now, as their progress tells.
    fn busy(&self, thread: &ThreadId) -> Vec<UserId> {
        self.data.live.progress.get(thread).map_or_default(|p| p.status.busy().cloned().collect())
    }

    /// Answer a pending tool request with `verdict`: the one `id` picks as `ants tools approve` does, else the first
    /// on the thread acted on.
    fn decision(&self, id: Option<&str>, verdict: Verdict, now: DateTime<Utc>) -> Result<Action, String> {
        let approvals = &self.data.live.approvals;
        let request: ApprovalRequest = match id {
            Some(id) => {
                let query = tools::Query::parse(id).map_err(|e| format!("{e:#}"))?;
                tools::select(approvals.clone(), &query).map_err(|e| format!("{e:#}"))?
            }
            None => self.request(now)?.clone(),
        };
        match live::refusal(&request, &tools::State::Pending, verdict) {
            Some(why) => Err(why),
            None => Ok(Action::Decide(Box::new(request), verdict)),
        }
    }

    /// Show what `/goto` names.
    fn goto(&mut self, to: Goto) -> Result<Option<Action>, String> {
        let item = match to {
            Goto::Channel(channel) => {
                if !self.data.channels.iter().any(|c| c.channel == channel) {
                    return Err(format!("not a member of #{channel}"));
                }
                nav::Item::Channel(channel)
            }
            Goto::User(user) if self.data.is_human(&user) => nav::Item::Person(user),
            Goto::User(user) => nav::Item::Ant(user),
            Goto::Thread(prefix) => {
                let thread = self.read_thread(&prefix)?;
                self.focus = Focus::View;
                self.push(Page::Thread(thread::Page::new(thread.clone())));
                return Ok(Some(Action::Open(thread)));
            }
        };
        self.focus = Focus::View;
        Ok(self.select(item))
    }

    /// The thread `prefix` names: the one thread read whose id starts with it, else a whole id, as given.
    fn read_thread(&self, prefix: &str) -> Result<ThreadId, String> {
        let found: Vec<&ThreadId> = self.data.live.order.iter().filter(|t| t.as_str().starts_with(prefix)).collect();
        match found[..] {
            [thread] => Ok(thread.clone()),
            [] if ants_proto::Uuid::try_parse(prefix).is_ok() => prefix.parse().map_err(|e| format!("{e}")),
            [] => Err(format!("no thread {prefix} read")),
            _ => Err(format!("{} threads start {prefix}: give more of the id", found.len())),
        }
    }

    /// The thread `/trail run --thread` names: one read as `/goto` takes it, the most recently active one read, or the
    /// one slash commands act on.
    fn on_thread(&self, on: OnThread, now: DateTime<Utc>) -> Result<ThreadId, String> {
        match on {
            OnThread::Id(prefix) => self.read_thread(&prefix),
            OnThread::Last => self.data.live.order.first().cloned().ok_or_else(|| "no thread read".to_owned()),
            OnThread::Here => self.target(now),
        }
    }

    /// Set the human's notify level of `channel` here at once, and on all its devices.
    pub(in crate::tui) fn set_level(&mut self, channel: ChannelId, level: ChannelNotify) -> Action {
        if level == ChannelNotify::default() {
            self.data.channel_levels.remove(&channel);
        } else {
            self.data.channel_levels.insert(channel.clone(), level);
        }
        Action::Run(Job::ChannelNotify { channel, level }, String::new())
    }
}
