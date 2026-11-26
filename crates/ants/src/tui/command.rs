//! The command bar under the view: Ctrl-K focuses it, and @ or # typed in a view focuses it with that character. Its
//! text starts a thread with `@user` as `ants ask` does (on an open channel thread or a channel's feed, posts there
//! mentioning the user), posts a new thread with `#channel` as `ants post` does, and else replies on the open thread,
//! posts a new thread on the channel on view, or asks the human's assistant, else the messenger; `/` starts a slash
//! command over the CLI's verbs, acting on the selected item or open thread ([`COMMANDS`]). Sends run in the background
//! ([`super::exec`]); the bar's status line tells how they went, and a failed send's text, with the message it quoted, comes back into the bar.
//! n on a tool request opens the bar on a note ([`Note`]): Enter rejects the request, then sends the note to its ant.
//! The bar's title names where Enter sends the text as it stands ([`Route`], as every client routes it), and each
//! conversation keeps its own draft ([`Drafts`]).

use ants_client::sending::bar::Route;
use ants_proto::{ApprovalRequest, ChannelId, ThreadId, UserId, Verdict};
use chrono::{DateTime, Utc};

use super::approvals::Asking;
use super::exec::{Job, Quoted};
use super::model::{Action, Focus, Key, Model, Page};
use super::{attach, complete};
use crate::tools;

mod bar;
mod drafts;
mod history;
mod outcome;
mod parse;
mod route;
mod slash;

pub use bar::{Bar, Draft, Edited, Note, Tone};
pub use drafts::{Conversation, Drafts};
pub use history::{FILE as HISTORY_FILE, History};
pub use parse::{COMMANDS, Command, Dead, Goto, Needs, Notify, OnThread, Row, Slash, Trail, parse};
pub use slash::GRANT;

/// A paste of more bytes than this is staged as a text file rather than put in the bar.
const PASTE_ATTACH: usize = 64 * 1024;

/// Send the note `text` on `request` to its ant on its thread, as any message of the human's there.
pub fn note_job(request: &ApprovalRequest, text: &str) -> Job {
    let (to, thread) = (request.ant.clone(), Some(request.thread_id.clone()));
    Job::Ask { to, thread, text: text.trim().to_owned(), quote: None, team: None }
}

impl Model {
    /// Take `key` for the bar when it is focused or `key` focuses it: Ctrl-K, or @ or # in a view. Returns None when
    /// the key is not the bar's.
    pub fn bar_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Option<Action>> {
        if !self.ui.bar.focused {
            match key {
                Key::Ctrl('k') if self.ui.bar.is_empty() => self.ui.bar.insert('/'),
                Key::Ctrl('k') => {}
                Key::Char(c @ ('@' | '#')) if self.focus == Focus::View => self.ui.bar.insert(c),
                _ => return None,
            }
            self.ui.bar.focused = true;
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
        let before = self.ui.bar.text();
        let edited = self.ui.bar.key(key);
        self.escaped(key, now);
        if self.ui.bar.text() != before {
            self.ui.bar.pick = complete::Pick::default();
        }
        // Leaving the bar empty drops the reply `>` or the note set up.
        if key == Key::Esc && self.ui.bar.is_empty() {
            self.ui.bar.quoting = None;
            self.ui.bar.noting = None;
        }
        Some(match edited {
            Edited::Done => None,
            Edited::Submit(text) => self.submit(text, now),
            Edited::Editor(text) => Some(Action::Edit(text)),
        })
    }

    /// Do what the bar's `text` says, emptying the bar; else keep it and tell why on its status line.
    pub(super) fn submit(&mut self, text: String, now: DateTime<Utc>) -> Option<Action> {
        if let Some(note) = self.ui.bar.noting.clone() {
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
        let sealed = self.sealed(self.ui.drafts.here(), &text, now);
        let done = command.and_then(|command| match command {
            Command::Slash(command) => self.command(command, now),
            _ => self.job(&text).map(|job| Some(Action::Run(job, String::new()))),
        });
        let action = match done {
            Ok(action) => action,
            Err(why) => {
                if why.starts_with("usage:") || why.starts_with("no command") {
                    self.ui.bar.refused = Some(self.ui.bar.raw());
                    self.ui.bar.pick = complete::Pick::default();
                }
                self.ui.bar.set_status(why, Tone::Error);
                return None;
            }
        };
        self.ui.history.push(&text, sealed);
        self.ui.bar.set_text("");
        self.ui.bar.status = None;
        if slash {
            self.ui.bar.focused = false;
        } else {
            self.ui.bar.quoting = None;
        }
        match action {
            Some(Action::Run(job, _)) => Some(self.run_job(job, text, slash)),
            Some(Action::Decide(request, verdict)) => {
                self.ui.bar.set_status(format!("answering request {}…", tools::qualified(&request)), Tone::Busy);
                Some(Action::Decide(request, verdict))
            }
            other => other,
        }
    }

    /// Run `job`, which the bar's `text` asked for, with the files staged if it sends a message.
    fn run_job(&mut self, job: Job, text: String, slash: bool) -> Action {
        let here = self.ui.drafts.here().clone();
        self.ui.drafts.sending(&text, here);
        let job = match job.sends().then(|| self.ui.bar.staged.take(&text)).flatten() {
            Some(files) => {
                let count = if files.len() == 1 { "1 file".to_owned() } else { format!("{} files", files.len()) };
                self.ui.bar.set_status(format!("sending with {count}…"), Tone::Busy);
                Job::Attached { job: Box::new(job), files }
            }
            None => {
                self.ui.bar.set_status(if slash { "running…" } else { "sending…" }, Tone::Busy);
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
                self.ui.bar.sending_note(&text, request.clone());
                self.ui.bar.set_text("");
                self.ui.bar.noting = None;
                return Some(self.run_job(note_job(&request, &text), text, false));
            }
            Note::Reject(request) => request,
        };
        if let Some(storing) = self.asking().filter(|a| a.storing) {
            let id = tools::qualified(&storing.request);
            self.ui.bar.set_status(
                format!("storing the decision on request {id}: send the note once it is stored"),
                Tone::Error,
            );
            return None;
        }
        let mut asking = match Asking::decide((*request).clone(), Verdict::Reject) {
            Ok(asking) => asking,
            Err(refusal) => {
                let ant = &request.ant;
                self.ui.bar.set_status(format!("{refusal}: Enter sends the note to {ant} as a reply"), Tone::Error);
                self.ui.bar.noting = Some(Note::Reply(request));
                return None;
            }
        };
        asking.note = Some(text);
        self.ask(asking);
        self.ui.bar.set_text("");
        self.ui.bar.noting = None;
        self.ui.bar.focused = false;
        self.ui.bar.set_status(format!("rejecting request {}…", tools::qualified(&request)), Tone::Busy);
        Some(Action::Decide(request, Verdict::Reject))
    }

    /// Stage a file for the next message, or drop those staged, the bar kept open to write it.
    fn stage(&mut self, slash: &Slash) {
        let staged = &mut self.ui.bar.staged;
        let line = match slash {
            Slash::Attach(path) => match staged.add(path) {
                Ok(line) => line,
                Err(e) => return self.ui.bar.set_status(format!("{e:#}"), Tone::Error),
            },
            _ => match staged.clear() {
                0 => "no files attached".to_owned(),
                1 => "dropped the file attached".to_owned(),
                n => format!("dropped the {n} files attached"),
            },
        };
        self.ui.bar.set_text("");
        self.ui.bar.set_status(line, Tone::Done);
    }

    /// Take `text` pasted into the bar, focusing it: image files dropped on the terminal, or a paste of over
    /// [`PASTE_ATTACH`] bytes as a text file, staged as `/attach` does; else the text at the cursor.
    pub(super) fn paste_in_bar(&mut self, text: &str) {
        self.ui.bar.focused = true;
        let staged = if let Some(paths) = attach::dropped_images(text) {
            paths
                .iter()
                .map(|path| self.ui.bar.staged.add_path(path))
                .collect::<anyhow::Result<Vec<_>>>()
                .map(|l| l.join(" · "))
        } else if text.len() > PASTE_ATTACH {
            self.ui.bar.staged.add_paste(text)
        } else {
            self.ui.bar.paste(text);
            self.ui.bar.pick = complete::Pick::default();
            return;
        };
        match staged {
            Ok(line) => self.ui.bar.set_status(line, Tone::Done),
            Err(e) => self.ui.bar.set_status(format!("{e:#}"), Tone::Error),
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
            Route::Unaddressed | Route::Command { .. } | Route::Unnamed => {
                "Enter asks the assistant or the messenger".to_owned()
            }
        };
        format!("{enter} · / commands · @ ant · # channel · Ctrl-J new line")
    }

    /// Open the bar to reply on `thread` to the message `quoted`, its text begun with an @mention of `mention` in
    /// place of any it began with.
    pub(super) fn quote(&mut self, thread: ThreadId, quoted: Quoted, mention: Option<&UserId>) {
        let text = self.ui.bar.raw();
        let rest = match text.strip_prefix('@') {
            Some(after) => after.split_once(char::is_whitespace).map_or("", |(_, rest)| rest.trim_start()),
            None => &text,
        };
        let mention = mention.map_or_default(|m| format!("@{m} "));
        self.ui.bar.set_raw(&format!("{mention}{rest}"));
        self.ui.bar.quoting = Some((thread, quoted));
        self.ui.bar.focused = true;
    }

    /// The message the next send replies to, quoting it, with its thread: the one `>` picked, while that thread is
    /// open.
    fn quoting(&self) -> Option<(ThreadId, Quoted)> {
        let (thread, quoted) = self.ui.bar.quoting.as_ref()?;
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
}

#[cfg(test)]
mod tests;
