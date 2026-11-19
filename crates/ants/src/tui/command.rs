//! The command bar under the view: Ctrl-K focuses it, and @ or # typed in a view focuses it with that character. Its
//! text starts a thread with `@user` as `ants ask` does (on an open channel thread or a channel's feed, posts there
//! mentioning the user), posts a new thread with `#channel` as `ants post` does, and else replies on the open thread,
//! posts a new thread on the channel on view, or asks the human's assistant, else the messenger; `/` starts a slash
//! command over the CLI's verbs, acting on the selected item or open thread ([`COMMANDS`]). Sends run in the background
//! ([`super::exec`]); the bar's status line tells how they went, and a failed send's text, with the message it quoted, comes back into the bar.
//! n on a tool request opens the bar on a note ([`Note`]): Enter rejects the request, then sends the note to its ant.
//! The bar's title names where Enter sends the text as it stands ([`Route`]), and each conversation keeps its own
//! draft ([`Drafts`]).

use ants_proto::{ApprovalRequest, ChannelId, ChannelNotify, Questions, ThreadId, UserId, Uuid, Verdict};
use chrono::{DateTime, Utc};

use super::approvals::Asking;
use super::exec::{Job, Outcome, Quoted};
use super::model::{Action, Focus, Key, Model, Page};
use super::{attach, complete, nav, snooze, thread};
use crate::messages::{attachments, forward};
use crate::people::quiet;
use crate::{live, tools};

mod bar;
mod drafts;
mod history;
mod parse;
mod route;

pub use bar::{Bar, Draft, Edited, Note, Tone};
pub use drafts::{Conversation, Drafts};
pub use history::{FILE as HISTORY_FILE, History};
pub use parse::{COMMANDS, Command, Dead, Goto, Notify, OnThread, Slash, Trail, parse};
pub use route::Route;

/// A paste of more bytes than this is staged as a text file rather than put in the bar.
const PASTE_ATTACH: usize = 64 * 1024;

/// What `/grant` without a text sends the stuck ant.
pub const GRANT: &str = "Carry on: fresh turns granted.";

/// Send the note `text` on `request` to its ant on its thread, as any message of the human's there.
pub fn note_job(request: &ApprovalRequest, text: &str) -> Job {
    let (to, thread) = (request.ant.clone(), Some(request.thread_id.clone()));
    Job::Ask { to, thread, text: text.trim().to_owned(), quote: None }
}

impl Model {
    /// Take `key` for the bar when it is focused or `key` focuses it: Ctrl-K, or @ or # in a view. Returns None when
    /// the key is not the bar's.
    pub fn bar_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Option<Action>> {
        if !self.bar.focused {
            match key {
                Key::Ctrl('k') if self.bar.is_empty() => self.bar.insert('/'),
                Key::Ctrl('k') => {}
                Key::Char(c @ ('@' | '#')) if self.focus == Focus::View => self.bar.insert(c),
                _ => return None,
            }
            self.bar.focused = true;
            return Some(None);
        }
        if self.history_key(key) {
            return Some(None);
        }
        if let Some(taken) = self.palette_key(key, now) {
            return Some(taken);
        }
        if self.complete_key(key) {
            return Some(None);
        }
        let before = self.bar.text();
        let edited = self.bar.key(key);
        self.escaped(key, now);
        if self.bar.text() != before {
            self.bar.pick = complete::Pick::default();
        }
        // Leaving the bar empty drops the reply `>` or the note set up.
        if key == Key::Esc && self.bar.is_empty() {
            self.bar.quoting = None;
            self.bar.noting = None;
        }
        Some(match edited {
            Edited::Done => None,
            Edited::Submit(text) => self.submit(text, now),
            Edited::Editor(text) => Some(Action::Edit(text)),
        })
    }

    /// Do what the bar's `text` says, emptying the bar; else keep it and tell why on its status line.
    pub(super) fn submit(&mut self, text: String, now: DateTime<Utc>) -> Option<Action> {
        if let Some(note) = self.bar.noting.clone() {
            return self.note(note, text);
        }
        let command = match self.channel_on_view() {
            Some(_) if text.trim_start().starts_with('@') => Ok(Command::Say(text.trim().to_owned())),
            _ => parse(&text),
        };
        if let Ok(Command::Slash(slash @ (Slash::Attach(_) | Slash::Detach))) = &command {
            self.stage(slash);
            return None;
        }
        let slash = matches!(command, Ok(Command::Slash(_)));
        // Before the command moves the view, and so what it acts on.
        let sealed = self.sealed(self.data.drafts.here(), &text);
        let route = self.route(&text);
        let done = command.and_then(|command| match command {
            Command::Slash(command) => self.command(command, now),
            message => self.message(message, route),
        });
        let action = match done {
            Ok(action) => action,
            Err(why) => {
                if why.starts_with("usage:") || why.starts_with("no command") {
                    self.bar.refused = Some(self.bar.raw());
                    self.bar.pick = complete::Pick::default();
                }
                self.bar.set_status(why, Tone::Error);
                return None;
            }
        };
        self.data.history.push(&text, sealed);
        self.bar.set_text("");
        self.bar.status = None;
        if slash {
            self.bar.focused = false;
        } else {
            self.bar.quoting = None;
        }
        match action {
            Some(Action::Run(job, _)) => Some(self.run_job(job, text, slash)),
            Some(Action::Decide(request, verdict)) => {
                self.bar.set_status(format!("answering request {}…", tools::qualified(&request)), Tone::Busy);
                Some(Action::Decide(request, verdict))
            }
            other => other,
        }
    }

    /// Run `job`, which the bar's `text` asked for, with the files staged if it sends a message.
    fn run_job(&mut self, job: Job, text: String, slash: bool) -> Action {
        let here = self.data.drafts.here().clone();
        self.data.drafts.sending(&text, here);
        let job = match job.sends().then(|| self.bar.staged.take(&text)).flatten() {
            Some(files) => {
                let count = if files.len() == 1 { "1 file".to_owned() } else { format!("{} files", files.len()) };
                self.bar.set_status(format!("sending with {count}…"), Tone::Busy);
                Job::Attached { job: Box::new(job), files }
            }
            None => {
                self.bar.set_status(if slash { "running…" } else { "sending…" }, Tone::Busy);
                job
            }
        };
        Action::Run(job, text)
    }

    /// Take Enter on the note `text` about a tool request: reject the request, the note to be sent once the rejection
    /// is stored ([`Model::answered`]); or, the rejection refused, send the note as a reply on the request's thread.
    fn note(&mut self, note: Note, text: String) -> Option<Action> {
        let request = match note {
            Note::Reply(request) => {
                self.bar.sending_note(&text, request.clone());
                self.bar.set_text("");
                self.bar.noting = None;
                return Some(self.run_job(note_job(&request, &text), text, false));
            }
            Note::Reject(request) => request,
        };
        if let Some(storing) = self.asking.as_ref().filter(|a| a.storing) {
            let id = tools::qualified(&storing.request);
            self.bar.set_status(
                format!("storing the decision on request {id}: send the note once it is stored"),
                Tone::Error,
            );
            return None;
        }
        let mut asking = match Asking::decide((*request).clone(), Verdict::Reject) {
            Ok(asking) => asking,
            Err(refusal) => {
                let ant = &request.ant;
                self.bar.set_status(format!("{refusal}: Enter sends the note to {ant} as a reply"), Tone::Error);
                self.bar.noting = Some(Note::Reply(request));
                return None;
            }
        };
        asking.note = Some(text);
        self.asking = Some(asking);
        self.bar.set_text("");
        self.bar.noting = None;
        self.bar.focused = false;
        self.bar.set_status(format!("rejecting request {}…", tools::qualified(&request)), Tone::Busy);
        Some(Action::Decide(request, Verdict::Reject))
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

    /// The action sending the message `command` the way `route`, the route of the bar's text, goes.
    fn message(&self, command: Command, route: Route) -> Result<Option<Action>, String> {
        let run = |job| Ok(Some(Action::Run(job, String::new())));
        match command {
            Command::Ask { text, .. } | Command::Post { text, .. } | Command::Say(text) => run(self.job(route, text)?),
            Command::Later(text) => match self.job(route, text)? {
                job @ (Job::Reply { .. } | Job::Post { thread: Some(_), .. }) => run(Job::Later(Box::new(job))),
                _ => Err("/later replies on an open thread: open one first".to_owned()),
            },
            Command::Slash(_) => Err("nothing to send".to_owned()),
        }
    }

    /// The action the slash command `slash` takes, or why it cannot be taken.
    fn command(&mut self, slash: Slash, now: DateTime<Utc>) -> Result<Option<Action>, String> {
        let run = |job| Ok(Some(Action::Run(job, String::new())));
        match slash {
            Slash::Approve { id, always } => {
                let verdict = if always { Verdict::AllowAlways } else { Verdict::AllowOnce };
                self.decision(id.as_deref(), verdict).map(Some)
            }
            Slash::Reject { id } => self.decision(id.as_deref(), Verdict::Reject).map(Some),
            Slash::Grant(text) => {
                run(Job::Grant { thread: self.target()?, text: text.unwrap_or_else(|| GRANT.to_owned()) })
            }
            Slash::Cancel => {
                let thread = self.target()?;
                let ants = self.busy(&thread);
                if ants.is_empty() {
                    return Err(format!("no ant is at work on thread {thread}"));
                }
                run(Job::Cancel { thread, ants })
            }
            Slash::Answer(text) => run(self.answering(&text, now)?),
            Slash::Done(done) => run(Job::Done { thread: self.target()?, done }),
            Slash::Mark(marking) => run(Job::Mark { thread: self.target()?, marking }),
            Slash::Mute(hold) => run(Job::Mute { thread: self.target()?, hold: hold.unwrap_or(quiet::MUTE_FOR) }),
            Slash::Unmute => run(Job::Unmute { thread: self.target()? }),
            Slash::Notify(Notify::Level(level)) => {
                let (channel, _) =
                    self.channel_on_view().ok_or("/notify sets a channel's notify level: show the channel first")?;
                Ok(Some(self.set_level(channel, level)))
            }
            Slash::Notify(Notify::Snooze(hold)) => run(Job::NotifySnooze(hold)),
            Slash::Notify(Notify::Resume) => run(Job::NotifyResume),
            Slash::Notify(Notify::Status) => run(Job::NotifyStatus),
            Slash::Snooze(duration) => Ok(Some(self.set_snooze(self.target()?, Some(now + duration), now))),
            Slash::Unsnooze => Ok(Some(self.set_snooze(self.target()?, None, now))),
            Slash::Remind(text) => {
                let message = match self.page() {
                    Page::Thread(page) => page.selected_id(),
                    _ => None,
                };
                self.remind_as(self.target()?, message, &text, now).map(Some)
            }
            Slash::Unremind => Ok(Some(self.set_reminder(self.target()?, None))),
            Slash::Reminders => run(Job::Reminders),
            Slash::Title(text) => run(Job::Title { thread: self.target()?, text }),
            Slash::Notes(text) => run(Job::Notes { thread: self.target()?, text }),
            Slash::React(reaction) => {
                let message = match self.page() {
                    Page::Channel(page) => page.post(&self.data).map(|p| p.id),
                    _ => None,
                };
                run(Job::React { thread: self.target()?, message, reaction })
            }
            Slash::Goto(to) => self.goto(to),
            Slash::Search(query) => self.search(query, now).map(Some),
            Slash::Digest(since) => self.digest(since.as_deref(), now).map(Some),
            Slash::Tldr => Ok(self.tldr(self.target()?)),
            Slash::Tree => {
                let thread = self.target()?;
                self.tree(thread);
                Ok(None)
            }
            Slash::Forward { to, note } => {
                let chosen = match self.page() {
                    Page::Thread(page) => page.chosen().cloned(),
                    _ => None,
                };
                let source = match chosen {
                    Some(entry) => forward::Source::Message(Box::new(entry)),
                    None => {
                        let thread = self.target()?;
                        let tldr = self.data.cards.tldr.get(&thread).ok_or_else(|| {
                            format!("no TL;DR of thread {thread} yet: S or /tldr asks for one, then /forward again")
                        })?;
                        forward::Source::Thread(Box::new(tldr.clone()))
                    }
                };
                source.check().map_err(|e| format!("{e:#}"))?;
                run(Job::Forward { to, source, note })
            }
            Slash::Save(to) => {
                let listed = self.attachment().ok_or("no attachment on the selected message")?;
                run(Job::Save(Box::new(listed), to.map(|to| attach::expand(&to))))
            }
            Slash::Add { name, machine, settings, wait } => run(Job::Add { name, machine, settings, wait }),
            Slash::Clone { src, new, machine, settings, wait } => run(Job::Clone { src, new, machine, settings, wait }),
            Slash::Retire(ant) => run(Job::Retire { ant }),
            Slash::Dead(dead) => self.dead(dead),
            Slash::Trail(trail) => run(match trail {
                Trail::List => Job::TrailList,
                Trail::Show(what) => Job::TrailShow(what),
                Trail::Runs => Job::TrailRuns,
                Trail::Run { trail, conductor, args, limits, thread } => {
                    let thread = thread.map(|on| self.on_thread(on)).transpose()?;
                    Job::TrailPlan { trail, conductor, args, limits, thread }
                }
                Trail::Stop(run) => Job::TrailStop(run),
                Trail::Resume { run, limits } => Job::TrailResume { run, limits },
            }),
            Slash::Attach(_) | Slash::Detach => Ok(None),
            Slash::Quit => Ok(Some(Action::Quit)),
        }
    }

    /// Stage a file for the next message, or drop those staged, the bar kept open to write it.
    fn stage(&mut self, slash: &Slash) {
        let staged = &mut self.bar.staged;
        let line = match slash {
            Slash::Attach(path) => match staged.add(path) {
                Ok(line) => line,
                Err(e) => return self.bar.set_status(format!("{e:#}"), Tone::Error),
            },
            _ => match staged.clear() {
                0 => "no files attached".to_owned(),
                1 => "dropped the file attached".to_owned(),
                n => format!("dropped the {n} files attached"),
            },
        };
        self.bar.set_text("");
        self.bar.set_status(line, Tone::Done);
    }

    /// Take `text` pasted into the bar, focusing it: image files dropped on the terminal, or a paste of over
    /// [`PASTE_ATTACH`] bytes as a text file, staged as `/attach` does; else the text at the cursor.
    pub(super) fn paste_in_bar(&mut self, text: &str) {
        self.bar.focused = true;
        let staged = if let Some(paths) = attach::dropped_images(text) {
            paths
                .iter()
                .map(|path| self.bar.staged.add_path(path))
                .collect::<anyhow::Result<Vec<_>>>()
                .map(|l| l.join(" · "))
        } else if text.len() > PASTE_ATTACH {
            self.bar.staged.add_paste(text)
        } else {
            self.bar.paste(text);
            self.bar.pick = complete::Pick::default();
            return;
        };
        match staged {
            Ok(line) => self.bar.set_status(line, Tone::Done),
            Err(e) => self.bar.set_status(format!("{e:#}"), Tone::Error),
        }
    }

    /// The marked attachment of the message selected in the view, if any.
    pub fn attachment(&self) -> Option<attachments::Listed> {
        match self.page() {
            Page::Thread(page) => page.attachment().cloned(),
            Page::Channel(page) => page.attachment(&self.data).cloned(),
            _ => None,
        }
    }

    /// The empty bar's hint: what Enter does with text alone in the view on screen, as [`Model::said`] routes it,
    /// then what else may be typed.
    pub fn enter_hint(&self) -> String {
        let enter = match self.said() {
            Route::Post { channel, thread: Some(_) } => format!("Enter posts on the thread in #{channel}"),
            Route::Post { channel, thread: None } => format!("Enter starts a thread in #{channel}"),
            Route::Reply { .. } => "Enter replies on the thread".to_owned(),
            Route::Ask { to, thread: None } => format!("Enter writes to {to} on a new thread"),
            Route::Ask { to, thread: Some(_) } => format!("Enter writes to {to} on the thread"),
            Route::Unaddressed | Route::Command(_) | Route::Unnamed => {
                "Enter asks the assistant or the messenger".to_owned()
            }
        };
        format!("{enter} · / commands · @ ant · # channel · Ctrl-J new line")
    }

    /// Open the bar to reply on `thread` to the message `quoted`, its text begun with an @mention of `mention` in
    /// place of any it began with.
    pub(super) fn quote(&mut self, thread: ThreadId, quoted: Quoted, mention: Option<&UserId>) {
        let text = self.bar.raw();
        let rest = match text.strip_prefix('@') {
            Some(after) => after.split_once(char::is_whitespace).map_or("", |(_, rest)| rest.trim_start()),
            None => &text,
        };
        let mention = mention.map_or_default(|m| format!("@{m} "));
        self.bar.set_raw(&format!("{mention}{rest}"));
        self.bar.quoting = Some((thread, quoted));
        self.bar.focused = true;
    }

    /// The message the next send replies to, quoting it, with its thread: the one `>` picked, while that thread is
    /// open.
    fn quoting(&self) -> Option<(ThreadId, Quoted)> {
        let (thread, quoted) = self.bar.quoting.as_ref()?;
        match self.page() {
            Page::Thread(page) if page.history.thread == *thread => Some((thread.clone(), quoted.clone())),
            _ => None,
        }
    }

    /// The channel the bar posts to: that of the open channel thread, with the thread, else the channel whose feed is
    /// on view.
    fn channel_on_view(&self) -> Option<(ChannelId, Option<ThreadId>)> {
        match self.page() {
            Page::Thread(page) => {
                let thread = &page.history.thread;
                let channel = self.data.live.summary(thread)?.channels.first()?.clone();
                Some((channel, Some(thread.clone())))
            }
            Page::Channel(page) => Some((page.channel.clone(), None)),
            _ => None,
        }
    }

    /// Set the human's notify level of `channel` here at once, and on all its devices.
    pub(super) fn set_level(&mut self, channel: ChannelId, level: ChannelNotify) -> Action {
        if level == ChannelNotify::default() {
            self.data.channel_levels.remove(&channel);
        } else {
            self.data.channel_levels.insert(channel.clone(), level);
        }
        Action::Run(Job::ChannelNotify { channel, level }, String::new())
    }

    /// The thread slash commands act on: the open one, else the one selected in the view.
    pub fn target(&self) -> Result<ThreadId, String> {
        let data = &self.data;
        let thread = match self.page() {
            Page::Thread(page) => Some(page.history.thread.clone()),
            Page::Home(page) => page.thread(data),
            Page::Work(page) => page.thread(data),
            Page::Channel(page) => page.post(data).map(|p| p.thread.clone()),
            Page::Tree(page) => Some(page.root.clone()),
            Page::Ant(page) => page.thread(data),
            Page::Dm(_) | Page::Search(_) | Page::Credentials(_) | Page::Held(_) => None,
        };
        thread.ok_or_else(|| "no thread open or selected".to_owned())
    }

    /// Why the slash command `name`, without the arguments naming another target, cannot act here at `now`, if it
    /// cannot.
    pub(super) fn unfit(&self, name: &str, now: DateTime<Utc>) -> Option<String> {
        let target = self.target();
        match name {
            "approve" | "reject" => match target {
                Ok(thread) if !self.data.live.approvals.iter().any(|r| r.thread_id == thread) => {
                    Some("no tool request waits on the thread".to_owned())
                }
                found => found.err(),
            },
            "cancel" => match target {
                Ok(thread) if self.busy(&thread).is_empty() => Some("no ant is at work on the thread".to_owned()),
                found => found.err(),
            },
            "later" => (!matches!(self.page(), Page::Thread(_))).then(|| "no thread open".to_owned()),
            "answer" => self.question(now).err(),
            "forward" if matches!(self.page(), Page::Thread(page) if page.chosen().is_some()) => None,
            "save" => self.attachment().is_none().then(|| "no attachment on the selected message".to_owned()),
            "dead" => self.letter(None).err(),
            "grant" | "done" | "undone" | "pin" | "unpin" | "mute" | "unmute" | "snooze" | "unsnooze" | "remind"
            | "unremind" | "archive" | "unarchive" | "title" | "notes" | "react" | "tldr" | "tree" | "forward" => {
                target.err()
            }
            _ => None,
        }
    }

    /// The questions `/answer` answers at `now`, the message asking them and its thread: on an open thread, those of
    /// the card the keys answer; else the latest to the human with no answer read on the thread selected, those its
    /// keys answer on home.
    fn question(&self, now: DateTime<Utc>) -> Result<(ThreadId, Uuid, Questions), String> {
        let thread = self.target()?;
        let asked = match self.page() {
            Page::Thread(page) => page.question(&self.data, now).map(|(id, asked)| (id, asked.clone())),
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
    fn decision(&self, id: Option<&str>, verdict: Verdict) -> Result<Action, String> {
        let approvals = &self.data.live.approvals;
        let request: ApprovalRequest = match id {
            Some(id) => {
                let query = tools::Query::parse(id).map_err(|e| format!("{e:#}"))?;
                tools::select(approvals.clone(), &query).map_err(|e| format!("{e:#}"))?
            }
            None => {
                let thread = self.target()?;
                let request = approvals.iter().find(|r| r.thread_id == thread);
                request.cloned().ok_or_else(|| format!("no tool request waits on thread {thread}"))?
            }
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
                self.pages.push(Page::Thread(thread::Page::new(thread.clone())));
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
    fn on_thread(&self, on: OnThread) -> Result<ThreadId, String> {
        match on {
            OnThread::Id(prefix) => self.read_thread(&prefix),
            OnThread::Last => self.data.live.order.first().cloned().ok_or_else(|| "no thread read".to_owned()),
            OnThread::Here => self.target(),
        }
    }

    /// Take how a job the bar ran went: its line and warnings on the status line, opening the thread it started, the
    /// text and files of a message withdrawn to edit into the bar, the run of a trail planned to confirm, the dead
    /// letter it replayed or dropped, and what it shows in the pager; on failure, the error, and its text and quote
    /// back as the draft of the conversation it was typed in if that holds none.
    pub fn outcome(&mut self, outcome: Outcome) -> Option<Action> {
        self.follow_drafts();
        self.bar.staged.sent(&outcome.text, outcome.result.is_err());
        let note = self.bar.note_sent(&outcome.text);
        let typed = self.data.drafts.sent(&outcome.text).filter(|typed| typed != self.data.drafts.here());
        match outcome.result {
            Ok(mut done) => {
                self.data.withdrawn.extend(done.withdrew);
                self.data.dead_cleared.extend(done.dead);
                if let Some((text, files)) = done.edit.take() {
                    self.bar.set_text(&text);
                    for path in files {
                        if let Err(e) = self.bar.staged.add(&path.to_string_lossy()) {
                            done.warnings.push(format!("{e:#}"));
                        }
                    }
                    self.bar.focused = true;
                }
                let tone = if done.warnings.is_empty() { Tone::Done } else { Tone::Warn };
                let text = std::iter::once(done.line).chain(done.warnings).collect::<Vec<_>>().join(" · ");
                self.bar.set_status(text, tone);
                if let Some(planned) = done.planned {
                    self.starting = Some(*planned);
                }
                if let Some(page) = done.page {
                    return Some(Action::Page(page));
                }
                let thread = done.open?;
                if matches!(self.page(), Page::Thread(page) if page.history.thread == thread) {
                    return None;
                }
                self.pages.push(Page::Thread(thread::Page::new(thread.clone())));
                Some(Action::Open(thread))
            }
            Err(e) => {
                self.bar.set_status(format!("{e:#}"), Tone::Error);
                if let Some(typed) = typed
                    && !outcome.text.is_empty()
                {
                    let draft = Draft::of(&outcome.text, outcome.quoting, note.map(Note::Reply));
                    self.data.drafts.keep(typed, draft);
                } else if self.bar.is_empty() && !outcome.text.is_empty() {
                    self.bar.set_text(&outcome.text);
                    self.bar.quoting = outcome.quoting;
                    self.bar.noting = note.map(Note::Reply);
                    self.bar.focused = true;
                }
                None
            }
        }
    }

    /// Take the outcome of a decision on a tool request.
    pub fn decided(&mut self, decided: anyhow::Result<String>) {
        match decided {
            Ok(line) => self.bar.set_status(line, Tone::Done),
            Err(e) => self.bar.set_status(format!("{e:#}"), Tone::Error),
        }
    }

    /// Take the text edited in the editor, or why editing failed.
    pub fn edited(&mut self, edited: anyhow::Result<String>) {
        match edited {
            Ok(text) => {
                self.bar.set_text(&text);
                self.bar.focused = true;
            }
            Err(e) => self.bar.set_status(format!("{e:#}"), Tone::Error),
        }
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::{ChannelEntry, StatusEvent, StatusUpdate};

    use super::bar::Status;
    use super::*;
    use crate::test_support::live::{at, message, request, user};
    use crate::tui::acts::Change;
    use crate::tui::exec::Done;
    use crate::tui::reads::Read;
    use crate::tui::test_support::model;

    fn typed(m: &mut Model, text: &str) {
        for c in text.chars() {
            m.key(if c == '\n' { Key::Newline } else { Key::Char(c) }, at(0));
        }
    }

    #[test]
    fn a_paste_goes_into_the_bar_whole_its_line_breaks_new_lines_rather_than_enter() {
        let mut m = model();
        m.paste("q\r\nnow\tplease\x1b\rdone", at(0));
        assert!(m.bar.focused, "a paste where nothing is typed focuses the bar");
        assert_eq!(m.bar.text(), "q\nnow    please\ndone");
        m.key(Key::Home, at(0));
        m.paste("so\n", at(0));
        assert_eq!(m.bar.text(), "q\nnow    please\nso\ndone", "pasted at the cursor, Home going to its line's start");
        m.key(Key::Esc, at(0));
        m.help = Some(crate::tui::help::Help::default());
        m.paste("q", at(0));
        assert!(m.help.is_some() && !m.bar.focused, "help takes no paste");
    }

    #[test]
    fn a_paste_too_long_for_the_bar_or_of_dropped_images_is_attached() {
        let mut m = model();
        let log = "a\tlog line\n".repeat(7000);
        m.paste(&log, at(0));
        assert!(m.bar.focused && m.bar.is_empty());
        assert_eq!(m.bar.staged.names(), ["paste-1.txt"]);
        assert_eq!(m.bar.status.as_ref().unwrap().text, "attached paste-1.txt (75 KiB): sent with the next message");
        let files = m.bar.staged.take("see").unwrap();
        assert_eq!((files[0].bytes.as_slice(), files[0].attachment.mime.as_str()), (log.as_bytes(), "text/plain"));

        let dir = ants_test_support::scratch_dir("pasted");
        let shot = dir.join("shot.png");
        std::fs::write(&shot, b"png").unwrap();
        m.paste(&format!("'{}' ", shot.display()), at(0));
        assert!(m.bar.is_empty());
        assert_eq!(m.bar.staged.names(), ["shot.png"]);
        m.paste(&format!("{} ", shot.display()), at(0));
        assert_eq!(m.bar.staged.names(), ["shot.png", "shot.png"]);
        m.paste(&"x".repeat(9 << 20), at(0));
        let status = m.bar.status.as_ref().unwrap();
        assert!(status.tone == Tone::Error && status.text.contains("an attachment is at most"), "{status:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ctrl_k_or_at_or_hash_focus_the_bar_which_then_takes_every_key() {
        let mut m = model();
        assert_eq!(m.key(Key::Ctrl('k'), at(0)), None);
        assert!(m.bar.focused && m.bar.text() == "/", "on the palette");
        m.key(Key::Backspace, at(0));
        assert_eq!(m.key(Key::Char('q'), at(0)), None, "q types");
        assert_eq!(m.key(Key::Esc, at(0)), None);
        assert!(!m.bar.focused && m.bar.text() == "q", "Esc leaves the bar, keeping its text");
        m.bar.set_text("");
        m.key(Key::Char('@'), at(0));
        assert!(m.bar.focused && m.bar.text() == "@");
        typed(&mut m, "dev-a deploy");
        let action = m.key(Key::Enter, at(0));
        let job = Job::Ask { to: user("dev-a"), thread: None, text: "deploy".into(), quote: None };
        assert_eq!(action, Some(Action::Run(job, "@dev-a deploy".into())));
        assert!(m.bar.focused && m.bar.is_empty(), "the bar stays for the next message");
        assert_eq!(m.bar.status.as_ref().map(|s| s.tone), Some(Tone::Busy));
        m.key(Key::Esc, at(0));
        m.key(Key::Tab, at(0));
        m.key(Key::Char('#'), at(0));
        assert!(!m.bar.focused, "# in the nav is no bar's");
    }

    #[test]
    fn plain_text_replies_on_the_open_thread_and_else_goes_unaddressed() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.bar.focused = true;
        typed(&mut m, "who owns billing?");
        let job = Job::Unaddressed { text: "who owns billing?".into() };
        assert_eq!(m.key(Key::Enter, at(0)), Some(Action::Run(job, "who owns billing?".into())));
        m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
        typed(&mut m, "go on");
        let reply = Job::Reply { thread: thread.clone(), to: None, text: "go on".into(), quote: None };
        assert_eq!(m.key(Key::Enter, at(0)), Some(Action::Run(reply, "go on".into())));
        let mut post = message(1, &thread, "dev-a", "ops", "hi");
        (post.kind, post.channel) = (crate::threads::thread::Kind::Post, Some("ops".parse().unwrap()));
        m.stored(vec![post], at(1));
        typed(&mut m, "go on");
        let post = Job::Post {
            channel: "ops".parse().unwrap(),
            thread: Some(thread.clone()),
            text: "go on".into(),
            quote: None,
        };
        assert_eq!(m.key(Key::Enter, at(1)), Some(Action::Run(post, "go on".into())), "a channel thread gets a post");
        typed(&mut m, "@dev-b take a look");
        let text = "@dev-b take a look";
        let post = Job::Post { channel: "ops".parse().unwrap(), thread: Some(thread), text: text.into(), quote: None };
        assert_eq!(m.key(Key::Enter, at(1)), Some(Action::Run(post, text.into())), "a leading @mention posts there");
        m.pages.push(Page::Channel(crate::tui::channel::Page::new("ops".parse().unwrap())));
        for text in ["status?", "@dev-b take a look"] {
            typed(&mut m, text);
            let post = Job::Post { channel: "ops".parse().unwrap(), thread: None, text: text.into(), quote: None };
            assert_eq!(m.key(Key::Enter, at(1)), Some(Action::Run(post, text.into())), "the feed's channel");
        }
    }

    #[test]
    fn n_on_a_dm_focuses_the_bar_which_asks_its_person() {
        let mut m = model();
        m.pages.push(Page::Dm(crate::tui::dm::Page::new(user("bob"))));
        m.key(Key::Up, at(0));
        typed(&mut m, "n");
        assert!(m.bar.focused && m.bar.is_empty(), "n on a thread selected writes in the bar");
        typed(&mut m, "n");
        assert_eq!(m.bar.text(), "n", "the focused bar takes the keys of the DM");
        let job = Job::Ask { to: user("bob"), thread: None, text: "n".into(), quote: None };
        assert_eq!(m.key(Key::Enter, at(0)), Some(Action::Run(job, "n".into())));
    }

    #[test]
    fn slash_commands_act_on_the_selection_or_open_thread() {
        let thread = ThreadId::generate();
        let mut m = model();
        let run = |m: &mut Model, text: &str| {
            m.follow_drafts();
            m.bar.focused = true;
            m.bar.set_text(text);
            m.key(Key::Enter, at(0))
        };
        assert_eq!(run(&mut m, "/done"), None);
        assert_eq!(m.bar.status.as_ref().unwrap().text, "no thread open or selected");
        assert_eq!(m.bar.text(), "/done", "a command that fails keeps its text");
        m.select(nav::Item::Work(None));
        m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy")], at(1));
        let done = Job::Done { thread: thread.clone(), done: true };
        assert_eq!(run(&mut m, "/done"), Some(Action::Run(done, "/done".into())));
        assert!(!m.bar.focused, "a slash command leaves the bar");
        assert_eq!(run(&mut m, "/cancel"), None);
        assert_eq!(m.bar.status.as_ref().unwrap().text, format!("no ant is at work on thread {thread}"));
        let working = StatusEvent::new(user("dev-a"), thread.clone(), StatusUpdate::Thought { text: "hm".into() });
        m.take(live::Event::Status(working), at(2));
        let cancel = Job::Cancel { thread: thread.clone(), ants: vec![user("dev-a")] };
        assert_eq!(run(&mut m, "/cancel"), Some(Action::Run(cancel, "/cancel".into())));
        let grant = Job::Grant { thread: thread.clone(), text: GRANT.into() };
        assert_eq!(run(&mut m, "/grant"), Some(Action::Run(grant, "/grant".into())));
        let snooze = Change::Snooze(Some(at(2 * 3600)));
        assert_eq!(run(&mut m, "/snooze 2h"), Some(Action::Change(thread.clone(), snooze, 1)));
        assert!(m.data.cards.snoozed.get(&thread, at(1)).is_some(), "snoozed at once");
        assert_eq!(run(&mut m, "/unsnooze"), Some(Action::Change(thread.clone(), Change::Snooze(None), 1)));
        assert!(m.data.cards.snoozed.get(&thread, at(1)).is_none(), "woken at once");
        assert_eq!(run(&mut m, "/nope"), None);
        assert_eq!(m.bar.status.as_ref().unwrap().text, "no command /nope: the palette lists them");
        assert_eq!(run(&mut m, "/quit"), Some(Action::Quit));
    }

    #[test]
    fn answer_answers_the_question_of_the_card_the_keys_answer() {
        let thread = ThreadId::generate();
        let mut m = model();
        let ask = |seq, text: &str| {
            let mut asked = message(seq, &thread, "dev-a", "alice", text);
            let options = ["yes".to_owned(), "no".to_owned()];
            asked.question = Some(ants_proto::Question::new(text, &options, true).unwrap().into());
            asked
        };
        let (first, second) = (ask(1, "Ship it?"), ask(2, "Tag it?"));
        let mut page = thread::Page::new(thread.clone());
        page.history.loaded(&thread, Ok(vec![first.clone()])).unwrap();
        m.pages.push(Page::Thread(page));
        let answer = |m: &mut Model| {
            m.follow_drafts();
            m.bar.focused = true;
            m.bar.set_text("/answer after lunch");
            match m.key(Key::Enter, at(2)) {
                Some(Action::Run(Job::Answer { question, .. }, _)) => question,
                other => panic!("{other:?}"),
            }
        };
        assert_eq!(answer(&mut m), first.id.unwrap());
        m.stored(vec![second.clone()], at(2));
        assert_eq!(answer(&mut m), second.id.unwrap(), "the keys go to the latest card shown");
    }

    #[test]
    fn answer_takes_a_pick_for_each_of_several_questions() {
        let thread = ThreadId::generate();
        let mut m = model();
        let mut asked = message(1, &thread, "dev-a", "alice", "two questions");
        let options = ["eu".to_owned(), "us".to_owned(), "ap".to_owned()];
        let regions = ants_proto::Question::new("Which regions?", &options, false).unwrap().multi();
        let ship = ants_proto::Question::new("Ship it?", &["yes".to_owned(), "no".to_owned()], true).unwrap();
        asked.question = Some(Questions::new(vec![regions, ship]).unwrap());
        let mut page = thread::Page::new(thread.clone());
        page.history.loaded(&thread, Ok(vec![asked.clone()])).unwrap();
        m.pages.push(Page::Thread(page));
        let answer = |m: &mut Model, text: &str| {
            m.follow_drafts();
            m.bar.focused = true;
            m.bar.set_text(text);
            m.key(Key::Enter, at(2))
        };
        let picks = vec![ants_proto::Pick::Options(vec![1, 3]), ants_proto::Pick::Typed("after lunch".into())];
        let job = Job::Answer { thread: thread.clone(), question: asked.id.unwrap(), picks };
        let sent = answer(&mut m, "/answer 3,1 'after lunch'");
        assert!(matches!(sent, Some(Action::Run(ref sent, _)) if *sent == job), "{sent:?}");
        assert_eq!(answer(&mut m, "/answer 1"), None);
        let status = &m.bar.status.as_ref().unwrap().text;
        assert!(status.contains("1 answers to 2 questions"), "{status}");
        assert_eq!(answer(&mut m, "/answer 4 no"), None);
        let status = &m.bar.status.as_ref().unwrap().text;
        assert!(status.contains("question 1: no option 4"), "{status}");
    }

    #[test]
    fn approve_answers_the_request_on_the_thread_or_the_one_named() {
        let mut m = model();
        let push = request("dev-a", "git push", vec![Verdict::AllowOnce]);
        m.take(live::Event::Approval(tools::Update::Pending(push.clone())), at(0));
        let run = |m: &mut Model, text: &str| {
            m.follow_drafts();
            m.bar.focused = true;
            m.bar.set_text(text);
            m.key(Key::Enter, at(0))
        };
        assert_eq!(
            run(&mut m, "/approve"),
            Some(Action::Decide(Box::new(push.clone()), Verdict::AllowOnce)),
            "home selects it"
        );
        assert_eq!(run(&mut m, "/approve --always"), None);
        assert!(m.bar.status.as_ref().unwrap().text.starts_with("allow-always is not offered"));
        let id = push.id.to_string();
        let named = format!("/reject dev-a/{}", &id[..8]);
        assert_eq!(run(&mut m, &named), Some(Action::Decide(Box::new(push), Verdict::Reject)));
        m.decided(Ok("answered request".into()));
        assert_eq!(m.bar.status, Some(Status { text: "answered request".into(), tone: Tone::Done }));
        assert_eq!(run(&mut m, "/reject 0000"), None);
        assert_eq!(m.bar.status.as_ref().unwrap().text, "no tool request 0000");
    }

    #[test]
    fn n_rejects_a_tool_request_then_sends_the_note_to_its_ant_on_its_thread() {
        let mut m = model();
        let push = request("dev-a", "git push", vec![Verdict::AllowOnce, Verdict::Reject]);
        m.take(live::Event::Approval(tools::Update::Pending(push.clone())), at(0));
        m.bar.set_text("draft");
        assert_eq!(m.key(Key::Char('n'), at(0)), None);
        assert!(m.bar.noting.is_none() && m.bar.status.as_ref().unwrap().text.starts_with("the bar holds a draft"));
        m.bar.set_text("");

        m.compact = true;
        assert_eq!(m.key(Key::Char('n'), at(0)), None, "the top request");
        assert!(m.bar.focused && m.bar.noting == Some(Note::Reject(Box::new(push.clone()))));
        assert!(!m.compact, "the note written in the full view");
        let note = "open a pull request instead";
        typed(&mut m, note);
        assert_eq!(m.key(Key::Enter, at(0)), Some(Action::Decide(Box::new(push.clone()), Verdict::Reject)));
        assert!(m.bar.is_empty() && m.bar.noting.is_none() && !m.bar.focused);
        assert!(m.asking.as_ref().is_some_and(|a| a.storing && a.note.as_deref() == Some(note)));
        assert!(m.asking.as_ref().and_then(Asking::status).unwrap().to_string().contains("then sending the note"));

        let sent = m.take(live::Event::Decided(Ok("rejected".into())), at(1));
        let ask =
            Job::Ask { to: push.ant.clone(), thread: Some(push.thread_id.clone()), text: note.into(), quote: None };
        assert_eq!(sent, [Action::Run(ask.clone(), note.into())], "the note goes once the rejection is stored");
        assert!(m.asking.is_none());
        m.outcome(Outcome { result: Err(anyhow::anyhow!("no route")), text: note.into(), quoting: None });
        assert_eq!((m.bar.text(), &m.bar.noting), (note.to_owned(), &Some(Note::Reply(Box::new(push)))));
        assert_eq!(m.key(Key::Enter, at(1)), Some(Action::Run(ask, note.into())), "sent again as a reply");
    }

    #[test]
    fn a_refused_rejection_sends_no_note_and_puts_it_back_in_the_bar() {
        let mut m = model();
        let push = request("dev-a", "git push", vec![Verdict::AllowOnce, Verdict::Reject]);
        m.take(live::Event::Approval(tools::Update::Pending(push.clone())), at(0));
        m.key(Key::Char('n'), at(0));
        let note = "open a pull request instead";
        typed(&mut m, note);
        assert!(matches!(m.key(Key::Enter, at(0)), Some(Action::Decide(_, Verdict::Reject))));
        let refused = anyhow::anyhow!("request dev-a/{} was already answered by bob: allow-once", push.id);
        assert_eq!(m.take(live::Event::Decided(Err(refused)), at(1)), [], "no note sent");
        assert_eq!((m.bar.text(), &m.bar.noting), (note.to_owned(), &Some(Note::Reply(Box::new(push.clone())))));
        let status = m.bar.status.as_ref().unwrap();
        assert!(status.text.contains("by bob") && status.text.contains("the note was not sent"), "{}", status.text);
        assert!(m.bar.focused && m.asking.is_none());
        let ask = Job::Ask { to: push.ant.clone(), thread: Some(push.thread_id), text: note.into(), quote: None };
        assert_eq!(m.key(Key::Enter, at(1)), Some(Action::Run(ask, note.into())), "Enter sends it as a reply");
        assert!(m.bar.is_empty() && m.bar.noting.is_none());
    }

    #[test]
    fn goto_shows_a_thread_a_channel_or_a_user() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy")], at(1));
        let ops = ChannelEntry {
            channel: "ops".parse().unwrap(),
            members: vec![user("alice")],
            messenger: None,
            owner: None,
            description: None,
        };
        m.read(Read::Directory(Ok((vec![ops], ants_proto::Humans::default()))), at(1));
        let run = |m: &mut Model, text: &str| {
            m.follow_drafts();
            m.bar.focused = true;
            m.bar.set_text(text);
            m.key(Key::Enter, at(1))
        };
        assert_eq!(run(&mut m, &format!("/goto {}", &thread.as_str()[..10])), Some(Action::Open(thread)));
        assert_eq!(m.page().name(), "thread");
        assert!(matches!(run(&mut m, "/goto #ops"), Some(Action::Reactions(_))));
        assert_eq!(m.nav, nav::Item::Channel("ops".parse().unwrap()));
        assert_eq!(run(&mut m, "/goto #sales"), None);
        assert_eq!(m.bar.status.as_ref().unwrap().text, "not a member of #sales");
        run(&mut m, "/goto @dev-a");
        assert_eq!(m.nav, nav::Item::Ant(user("dev-a")));
        assert_eq!(run(&mut m, "/goto ffff"), None);
        assert_eq!(m.bar.status.as_ref().unwrap().text, "no thread ffff read");
    }

    #[test]
    fn later_replies_on_the_open_thread_waiting_its_turn() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.bar.focused = true;
        typed(&mut m, "/later then the docs");
        assert_eq!(m.key(Key::Enter, at(0)), None);
        assert_eq!(m.bar.status.as_ref().unwrap().text, "/later replies on an open thread: open one first");
        m.bar.set_text("");
        m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
        typed(&mut m, "/later then the docs");
        let reply = Job::Reply { thread, to: None, text: "then the docs".into(), quote: None };
        let job = Job::Later(Box::new(reply));
        assert!(job.sends());
        assert_eq!(m.key(Key::Enter, at(0)), Some(Action::Run(job, "/later then the docs".into())));
        assert!(m.bar.focused, "the bar stays for the next message, as after a send");
    }

    #[test]
    fn i_in_a_thread_is_written_and_with_a_message_selected_focuses_the_bar_to_reply_there() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy")], at(1));
        m.focus = Focus::View;
        m.key(Key::Char('i'), at(1));
        assert!(!m.bar.focused, "home has no conversation to write in");
        m.pages.push(Page::Thread(thread::Page::new(thread)));
        m.key(Key::Char('i'), at(1));
        assert_eq!((m.bar.text(), m.bar.focused), ("i".to_owned(), true), "written");
        m.key(Key::Ctrl('u'), at(1));
        m.key(Key::Esc, at(1));
        m.key(Key::Up, at(1));
        assert!(!m.typing());
        m.key(Key::Char('i'), at(1));
        assert!(m.bar.focused && m.typing());
        assert_eq!(m.bar.text(), "", "i is not written then");
    }

    #[test]
    fn gt_in_a_thread_replies_to_the_selected_message_quoting_it_and_mentioning_its_ant() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
        let entries = vec![
            message(1, &thread, "alice", "dev-a", "deploy"),
            message(2, &thread, "dev-a", "alice", "deployed\n  to staging"),
            message(3, &thread, "dev-b", "alice", "tests pass"),
        ];
        let ids: Vec<_> = entries.iter().map(|e| e.id.unwrap()).collect();
        m.stored(entries, at(3));
        m.focus = Focus::View;
        m.key(Key::Up, at(3));
        m.key(Key::Up, at(3));
        m.key(Key::Char('>'), at(3));
        assert!(m.bar.focused);
        assert_eq!(m.bar.text(), "@dev-a ", "the bar addresses the author of the message selected");
        typed(&mut m, "why staging?");
        let quote = Some(Quoted {
            id: ids[1],
            quote: ants_proto::Quote::new(user("dev-a"), "deployed to staging"),
            sealed: false,
        });
        let job = Job::Ask { to: user("dev-a"), thread: Some(thread.clone()), text: "why staging?".into(), quote };
        let sent = Some(Action::Run(job.clone(), "@dev-a why staging?".into()));
        assert_eq!(m.key(Key::Enter, at(3)), sent, "on the thread");
        assert_eq!(m.bar.quoting, None, "sent once");
        let failed = Outcome {
            result: Err(anyhow::anyhow!("no route")),
            text: "@dev-a why staging?".into(),
            quoting: job.quoting(),
        };
        m.outcome(failed);
        assert_eq!(m.bar.text(), "@dev-a why staging?");
        assert_eq!(
            m.bar.quoting.as_ref().map(|(t, q)| (t, q.id)),
            Some((&thread, ids[1])),
            "a failed send quotes again"
        );
        m.key(Key::Ctrl('u'), at(3));
        m.key(Key::Esc, at(3));
        assert_eq!(m.bar.quoting, None);
        m.bar.focused = true;

        m.key(Key::Esc, at(3));
        m.key(Key::Up, at(3));
        m.key(Key::Up, at(3));
        m.bar.set_text("@dev-b and the docs");
        m.key(Key::Char('>'), at(3));
        assert_eq!(m.bar.text(), "and the docs", "a human's message gets no mention, and the one typed goes");
        let quote = Some(Quoted { id: ids[0], quote: ants_proto::Quote::new(user("alice"), "deploy"), sealed: false });
        let reply = Job::Reply { thread, to: None, text: "and the docs".into(), quote };
        assert_eq!(m.key(Key::Enter, at(3)), Some(Action::Run(reply, "and the docs".into())));

        m.key(Key::Esc, at(3));
        m.key(Key::Up, at(3));
        m.key(Key::Char('>'), at(3));
        assert!(m.bar.quoting.is_some());
        m.key(Key::Ctrl('u'), at(3));
        m.key(Key::Esc, at(3));
        assert_eq!(m.bar.quoting, None, "leaving the bar empty drops the reply");
    }

    #[test]
    fn f_forwards_the_selected_message_else_the_thread_s_tldr() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
        let entries =
            vec![message(1, &thread, "alice", "dev-a", "deploy"), message(2, &thread, "dev-a", "alice", "done")];
        m.stored(entries.clone(), at(2));
        m.focus = Focus::View;
        m.bar.focused = true;
        typed(&mut m, "/forward @");
        assert_eq!((m.bar.text(), m.bar.focused), ("/forward @".to_owned(), true));
        typed(&mut m, "dev-b");
        assert_eq!(m.key(Key::Enter, at(2)), None, "following the latest, the thread goes, by its TL;DR");
        let refused = &m.bar.status.as_ref().unwrap().text;
        assert_eq!(*refused, format!("no TL;DR of thread {thread} yet: S or /tldr asks for one, then /forward again"));
        let tldr = ants_proto::ThreadSummary {
            thread_id: thread.clone(),
            text: "deployed".into(),
            by: user("messenger"),
            asker: user("alice"),
            at: at(2),
            upto: 2,
            tokens: 9,
        };
        m.data.cards.tldr.insert(thread, tldr.clone());
        let source = forward::Source::Thread(Box::new(tldr));
        let job = Job::Forward { to: user("dev-b"), source, note: String::new() };
        assert_eq!(m.key(Key::Enter, at(2)), Some(Action::Run(job, "/forward @dev-b".into())));

        m.key(Key::Up, at(2));
        m.key(Key::Char('f'), at(2));
        typed(&mut m, "dev-b over to you");
        let source = forward::Source::Message(Box::new(entries[1].clone()));
        let job = Job::Forward { to: user("dev-b"), source, note: "over to you".into() };
        assert_eq!(m.key(Key::Enter, at(2)), Some(Action::Run(job, "/forward @dev-b over to you".into())));
    }

    #[test]
    fn tree_shows_the_delegation_tree_of_the_thread_open() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy")], at(1));
        m.follow_drafts();
        m.bar.focused = true;
        m.bar.set_text("/tree");
        assert_eq!(m.key(Key::Enter, at(1)), None);
        assert_eq!(m.bar.status.as_ref().unwrap().text, "no thread open or selected");
        m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
        m.follow_drafts();
        m.bar.set_text("/tree");
        m.key(Key::Enter, at(1));
        assert_eq!(m.page().name(), "tree");
        assert_eq!(m.tree_shown().unwrap().0, thread);
    }

    #[test]
    fn a_failed_send_comes_back_into_the_bar_and_a_new_thread_opens() {
        let mut m = model();
        let failed = Outcome { result: Err(anyhow::anyhow!("no route")), text: "@dev-a deploy".into(), quoting: None };
        assert_eq!(m.outcome(failed), None);
        assert_eq!((m.bar.text(), m.bar.focused), ("@dev-a deploy".into(), true));
        assert_eq!(m.bar.status, Some(Status { text: "no route".into(), tone: Tone::Error }));
        let thread = ThreadId::generate();
        let warnings = vec!["dev-a is served by no daemon".to_owned()];
        let done = Done { line: "sent to dev-a".into(), warnings, open: Some(thread.clone()), ..Done::default() };
        let sent = Outcome { result: Ok(done), text: "@dev-a deploy".into(), quoting: None };
        assert_eq!(m.outcome(sent), Some(Action::Open(thread)));
        assert_eq!(m.page().name(), "thread");
        assert_eq!(m.bar.text(), "@dev-a deploy", "a success leaves the bar's text alone");
        let status = m.bar.status.as_ref().unwrap();
        assert_eq!((status.text.as_str(), status.tone), ("sent to dev-a · dev-a is served by no daemon", Tone::Warn));
    }

    #[test]
    fn a_message_withdrawn_to_edit_comes_back_into_the_bar_with_its_files() {
        let dir = ants_test_support::scratch_dir("bar-withdrawn");
        std::fs::write(dir.join("notes.txt"), b"notes").unwrap();
        let mut m = model();
        let withdrawn = ants_proto::Uuid::new_v4();
        let edit = Some(("deploy the site".to_owned(), vec![dir.join("notes.txt"), dir.join("gone.txt")]));
        let done = Done { line: "withdrew".into(), withdrew: Some(withdrawn), edit, ..Done::default() };
        m.outcome(Outcome { result: Ok(done), text: String::new(), quoting: None });
        assert!(m.data.withdrawn.contains(&withdrawn));
        assert!(m.bar.focused && m.bar.text() == "deploy the site");
        assert_eq!(m.bar.staged.names(), ["notes.txt"]);
        let status = m.bar.status.as_ref().unwrap();
        assert!(status.tone == Tone::Warn && status.text.starts_with("withdrew · "), "{}", status.text);

        let thread = ThreadId::generate();
        let entries = vec![message(1, &thread, "alice", "dev-a", "deploy the docs")];
        m.stored(entries.clone(), at(1));
        let mut page = thread::Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries)).unwrap();
        m.pages.push(Page::Thread(page));
        m.key(Key::Esc, at(1));
        assert!(m.bar.is_empty(), "the draft stays with home");
        m.bar.set_text("deploy the docs too");
        m.key(Key::Up, at(1));
        assert_eq!(m.key(Key::Char('E'), at(1)), None, "a draft in the bar is kept");
        assert_eq!(m.bar.text(), "deploy the docs too");
        assert!(m.bar.status.as_ref().unwrap().text.starts_with("the bar holds a draft"));
    }

    #[test]
    fn attach_stages_files_for_the_next_message_and_a_failed_send_stages_them_again() {
        let dir = ants_test_support::scratch_dir("bar-attach");
        std::fs::write(dir.join("notes.txt"), b"notes").unwrap();
        let mut m = model();
        m.bar.focused = true;
        typed(&mut m, &format!("/attach {}/no", dir.display()));
        assert_eq!(m.completions()[0].word, format!("{}/notes.txt", dir.display()), "paths complete");
        typed(&mut m, "tes.txt");
        assert_eq!(m.key(Key::Enter, at(0)), None);
        assert!(m.bar.focused && m.bar.is_empty(), "the bar stays open to write the message");
        assert_eq!(m.bar.status.as_ref().unwrap().text, "attached notes.txt (5 B): sent with the next message");
        assert_eq!(m.bar.staged.names(), ["notes.txt"]);
        typed(&mut m, &format!("/attach {}/missing", dir.display()));
        m.key(Key::Enter, at(0));
        assert_eq!(m.bar.status.as_ref().unwrap().tone, Tone::Error);
        m.bar.set_text("");

        typed(&mut m, "@dev-a read this");
        let Some(Action::Run(Job::Attached { job, files }, text)) = m.key(Key::Enter, at(0)) else { panic!() };
        assert_eq!(*job, Job::Ask { to: user("dev-a"), thread: None, text: "read this".into(), quote: None });
        assert_eq!((files.len(), text.as_str()), (1, "@dev-a read this"));
        assert_eq!(m.bar.status.as_ref().unwrap().text, "sending with 1 file…");
        assert!(m.bar.staged.names().is_empty(), "the files go with one message");
        m.outcome(Outcome { result: Err(anyhow::anyhow!("no route")), text, quoting: None });
        assert_eq!((m.bar.text(), m.bar.staged.names()), ("@dev-a read this".to_owned(), vec!["notes.txt"]));

        let thread = ThreadId::generate();
        m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
        m.follow_drafts();
        m.bar.set_text("/done");
        assert_eq!(m.key(Key::Enter, at(0)), Some(Action::Run(Job::Done { thread, done: true }, "/done".into())));
        assert_eq!(m.bar.staged.names(), ["notes.txt"], "only a message takes the files");
        m.bar.focused = true;
        typed(&mut m, "/detach");
        m.key(Key::Enter, at(0));
        assert_eq!(m.bar.status.as_ref().unwrap().text, "dropped the file attached");
        assert!(m.bar.staged.names().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn notes_posts_to_the_notes_of_the_thread_with_the_files_attached() {
        let dir = ants_test_support::scratch_dir("bar-notes");
        std::fs::write(dir.join("plan.md"), b"# plan").unwrap();
        let thread = ThreadId::generate();
        let mut m = model();
        let run = |m: &mut Model, text: &str| {
            m.follow_drafts();
            m.bar.focused = true;
            m.bar.set_text(text);
            m.key(Key::Enter, at(0))
        };
        assert_eq!(run(&mut m, "/notes split the work"), None);
        assert_eq!(m.bar.status.as_ref().unwrap().text, "no thread open or selected");
        m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
        let notes = Job::Notes { thread: thread.clone(), text: "split the work".into() };
        assert_eq!(run(&mut m, "/notes split the work"), Some(Action::Run(notes, "/notes split the work".into())));
        assert!(!m.bar.focused, "a slash command leaves the bar");
        run(&mut m, &format!("/attach {}/plan.md", dir.display()));
        let Some(Action::Run(Job::Attached { job, files }, _)) = run(&mut m, "/notes the plan") else { panic!() };
        assert_eq!((*job, files.len()), (Job::Notes { thread, text: "the plan".into() }, 1));
        assert_eq!(m.bar.status.as_ref().unwrap().text, "sending with 1 file…");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn save_saves_the_marked_attachment_of_the_open_thread() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
        assert_eq!(m.command(Slash::Save(None), at(0)), Err("no attachment on the selected message".into()));
        let mut sent = message(1, &thread, "dev-a", "alice", "the log");
        let envelope = ants_proto::Envelope::new(user("dev-a"), user("alice"), "x");
        let log = ants_daemon::files::reference("build.log", "text/plain", b"ok").unwrap();
        sent.attachments = crate::messages::attachments::listed(&[log], |id| envelope.attachment_handle(id));
        let Page::Thread(page) = m.pages.last_mut().unwrap() else { panic!() };
        page.history.loaded(&thread, Ok(vec![sent.clone()])).unwrap();
        let Ok(Command::Slash(save)) = parse("/save ~/logs") else { panic!() };
        let saved = m.command(save, at(0)).unwrap();
        let to = std::env::home_dir().map(|home| home.join("logs"));
        assert_eq!(saved, Some(Action::Run(Job::Save(Box::new(sent.attachments[0].clone()), to), String::new())));
    }
}
