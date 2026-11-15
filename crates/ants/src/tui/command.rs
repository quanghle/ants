//! The command bar under the view: Ctrl-K focuses it, and @ or # typed in a view focuses it with that character. Its
//! text starts a thread with `@user` as `ants ask` does, posts a new thread with `#channel` as `ants post` does, and
//! else replies on the open thread, or asks the messenger; `/` starts a slash command over the CLI's verbs, acting on
//! the selected item or open thread ([`COMMANDS`]). Sends run in the background ([`super::exec`]); the bar's status
//! line tells how they went, and a failed send's text comes back into the bar.

use std::time::Duration;

use ants_proto::{ApprovalRequest, ChannelId, Choice, Reaction, ThreadId, UserId, Verdict};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Position, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use super::exec::{Job, Outcome};
use super::frame;
use super::model::{Action, Focus, Key, Model, Page};
use super::{complete, nav, thread};
use crate::thread_meta::Marking;
use crate::{live, quiet, tools};

/// The slash commands: name, arguments and what each does, as the list shows them.
pub const COMMANDS: &[(&str, &str, &str)] = &[
    ("approve", "[ID] [--always]", "allow a tool request, once or for the session"),
    ("reject", "[ID]", "refuse a tool request"),
    ("grant", "[TEXT]", "fresh turns for the stuck ant"),
    ("cancel", "", "stop the prompt an ant runs"),
    ("steer", "TEXT", "redirect the ant at work"),
    ("answer", "N|TEXT", "answer the question asked"),
    ("done", "", "mark the thread done"),
    ("undone", "", "mark the thread not done"),
    ("pin", "", "pin the thread"),
    ("unpin", "", "unpin the thread"),
    ("mute", "[DURATION]", "hold back the thread's notifications"),
    ("unmute", "", "notify of the thread again"),
    ("archive", "", "archive the thread"),
    ("unarchive", "", "unarchive the thread"),
    ("title", "TEXT", "title the thread"),
    ("react", "NAME", "react to the latest message"),
    ("goto", "THREAD|#channel|@user", "show a thread, channel or user"),
    ("search", "QUERY", "search the threads"),
    ("digest", "[SINCE]", "catch-up summary"),
    ("tldr", "", "summary of the thread"),
    ("tree", "", "the thread's delegation tree"),
    ("quit", "", "leave ants chat"),
];

/// What `/grant` without a text sends the stuck ant.
pub const GRANT: &str = "Carry on: fresh turns granted.";
/// The most lines of text the bar shows at once.
const MAX_LINES: usize = 6;
/// What the empty bar shows.
const HINT: &str = "Ctrl-K commands · @ ant · # channel";

/// What the bar's text says to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Start a thread with the user.
    Ask {
        to: UserId,
        text: String,
    },
    /// Post a new thread to the channel.
    Post {
        channel: ChannelId,
        text: String,
    },
    /// Reply on the open thread, else ask the messenger.
    Say(String),
    Slash(Slash),
}

/// A slash command and its arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Slash {
    Approve { id: Option<String>, always: bool },
    Reject { id: Option<String> },
    Grant(Option<String>),
    Cancel,
    Steer(String),
    Answer(Choice),
    Done(bool),
    Mark(Marking),
    Mute(Option<Duration>),
    Unmute,
    Title(String),
    React(Reaction),
    Goto(Goto),
    Search(String),
    Digest(Option<String>),
    Tldr,
    Tree,
    Quit,
}

/// Where `/goto` leads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Goto {
    /// A thread, by its id or a start of one.
    Thread(String),
    Channel(ChannelId),
    User(UserId),
}

/// `text`'s first word and the rest, trimmed.
fn first_word(text: &str) -> (&str, &str) {
    text.split_once(char::is_whitespace).map_or((text, ""), |(word, rest)| (word, rest.trim()))
}

/// What the bar's `text` says to do, or why it says nothing that can be done.
pub fn parse(text: &str) -> Result<Command, String> {
    let text = text.trim();
    if text.is_empty() {
        return Err("nothing to send".to_owned());
    }
    if let Some(literal) = text.strip_prefix("//") {
        return Ok(Command::Say(format!("/{literal}")));
    }
    if let Some(command) = text.strip_prefix('/') {
        return slash(command).map(Command::Slash);
    }
    if let Some(rest) = text.strip_prefix('@') {
        let (name, text) = first_word(rest);
        let to: UserId = name.parse().map_err(|e| format!("{e}"))?;
        if text.is_empty() {
            return Err(format!("nothing to send to @{to}"));
        }
        return Ok(Command::Ask { to, text: text.to_owned() });
    }
    if let Some(rest) = text.strip_prefix('#') {
        let (name, text) = first_word(rest);
        let channel: ChannelId = name.parse().map_err(|e| format!("{e}"))?;
        if text.is_empty() {
            return Err(format!("nothing to post to #{channel}"));
        }
        return Ok(Command::Post { channel, text: text.to_owned() });
    }
    Ok(Command::Say(text.to_owned()))
}

/// The slash command `text`, after its `/`.
fn slash(text: &str) -> Result<Slash, String> {
    let (name, args) = first_word(text);
    let name = name.to_ascii_lowercase();
    let usage = || {
        let args = COMMANDS.iter().find(|(n, ..)| *n == name).map_or("", |(_, args, _)| *args);
        format!("usage: /{name} {args}").trim_end().to_owned()
    };
    let bare = |command: Slash| if args.is_empty() { Ok(command) } else { Err(usage()) };
    let text = || if args.is_empty() { Err(usage()) } else { Ok(args.to_owned()) };
    let optional = || (!args.is_empty()).then(|| args.to_owned());
    match name.as_str() {
        "approve" => {
            let (mut id, mut always) = (None, false);
            for word in args.split_whitespace() {
                match word {
                    "--always" if !always => always = true,
                    word if id.is_none() && !word.starts_with('-') => id = Some(word.to_owned()),
                    _ => return Err(usage()),
                }
            }
            Ok(Slash::Approve { id, always })
        }
        "reject" if args.split_whitespace().count() <= 1 && !args.starts_with('-') => {
            Ok(Slash::Reject { id: optional() })
        }
        "grant" => Ok(Slash::Grant(optional())),
        "cancel" => bare(Slash::Cancel),
        "steer" => text().map(Slash::Steer),
        "answer" => text().map(|answer| Slash::Answer(Choice::parse(&answer))),
        "done" => bare(Slash::Done(true)),
        "undone" => bare(Slash::Done(false)),
        "pin" => bare(Slash::Mark(Marking::Pin)),
        "unpin" => bare(Slash::Mark(Marking::Unpin)),
        "archive" => bare(Slash::Mark(Marking::Archive)),
        "unarchive" => bare(Slash::Mark(Marking::Unarchive)),
        "mute" if args.is_empty() => Ok(Slash::Mute(None)),
        "mute" => ants_proto::duration::parse(args).map(|hold| Slash::Mute(Some(hold))),
        "unmute" => bare(Slash::Unmute),
        "title" => text().map(Slash::Title),
        "react" => text()?.parse().map(Slash::React).map_err(|e| e.to_string()),
        "goto" => goto(&text()?).map(Slash::Goto),
        "search" => text().map(Slash::Search),
        "digest" => Ok(Slash::Digest(optional())),
        "tldr" => bare(Slash::Tldr),
        "tree" => bare(Slash::Tree),
        "quit" => bare(Slash::Quit),
        _ if COMMANDS.iter().any(|(n, ..)| *n == name) => Err(usage()),
        _ => Err(format!("no command /{name}")),
    }
}

fn goto(to: &str) -> Result<Goto, String> {
    let error = |e: ants_proto::Error| format!("{e}");
    if let Some(channel) = to.strip_prefix('#') {
        return channel.parse().map(Goto::Channel).map_err(error);
    }
    if let Some(user) = to.strip_prefix('@') {
        return user.parse().map(Goto::User).map_err(error);
    }
    let thread = to.to_ascii_lowercase();
    thread.parse::<ThreadId>().map_err(error)?;
    Ok(Goto::Thread(thread))
}

/// How the bar's status line reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    /// Something runs.
    Busy,
    Done,
    /// Done, with warnings.
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    pub text: String,
    pub tone: Tone,
}

/// What a key did to the bar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Edited {
    /// Nothing for the UI to do.
    Done,
    /// Enter on this text.
    Submit(String),
    /// Ctrl-E on this text: edit it in the editor.
    Editor(String),
}

/// The bar's text, the cursor in it, and its status line.
#[derive(Debug, Default)]
pub struct Bar {
    /// Whether keys go to the bar.
    pub focused: bool,
    chars: Vec<char>,
    /// Where the cursor is, in characters.
    cursor: usize,
    pub status: Option<Status>,
    /// Whether the list of commands shows above the bar.
    pub list: bool,
    /// The completion selected in the list of those of the word at the cursor.
    pub pick: complete::Pick,
}

impl Bar {
    pub fn text(&self) -> String {
        self.chars.iter().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.chars.is_empty()
    }

    /// Replace the text with `text`, the cursor at its end.
    pub fn set_text(&mut self, text: &str) {
        self.chars = text.chars().collect();
        self.cursor = self.chars.len();
    }

    pub fn insert(&mut self, c: char) {
        self.chars.insert(self.cursor, c);
        self.cursor += 1;
    }

    /// Where the word at the cursor starts, and its text up to the cursor.
    pub fn word(&self) -> (usize, String) {
        let start = self.chars[..self.cursor].iter().rposition(|c| c.is_whitespace()).map_or(0, |at| at + 1);
        (start, self.chars[start..self.cursor].iter().collect())
    }

    /// Put `word` and a space in place of the word at the cursor, the cursor after them.
    pub fn complete(&mut self, word: &str) {
        let (start, _) = self.word();
        let end = self.chars[self.cursor..]
            .iter()
            .position(|c| c.is_whitespace())
            .map_or(self.chars.len(), |at| self.cursor + at);
        let spaced = self.chars.get(end).is_some_and(|c| *c == ' ');
        let mut put: Vec<char> = word.chars().collect();
        if !spaced {
            put.push(' ');
        }
        self.cursor = start + put.len() + usize::from(spaced);
        self.chars.splice(start..end, put);
        self.pick = complete::Pick::default();
    }

    pub fn set_status(&mut self, text: impl Into<String>, tone: Tone) {
        self.status = Some(Status { text: text.into(), tone });
    }

    /// Take `key`, the bar being focused.
    pub fn key(&mut self, key: Key) -> Edited {
        match key {
            Key::Char(c) => self.insert(c),
            Key::Newline => self.insert('\n'),
            Key::Backspace if self.cursor > 0 => {
                self.cursor -= 1;
                self.chars.remove(self.cursor);
            }
            Key::Delete if self.cursor < self.chars.len() => {
                self.chars.remove(self.cursor);
            }
            Key::Left => self.cursor = self.cursor.saturating_sub(1),
            Key::Right => self.cursor = (self.cursor + 1).min(self.chars.len()),
            Key::Home | Key::Ctrl('a') => self.cursor = 0,
            Key::End => self.cursor = self.chars.len(),
            Key::Ctrl('u') => self.set_text(""),
            Key::Ctrl('w') => {
                let mut start = self.cursor;
                while start > 0 && self.chars[start - 1].is_whitespace() {
                    start -= 1;
                }
                while start > 0 && !self.chars[start - 1].is_whitespace() {
                    start -= 1;
                }
                self.chars.drain(start..self.cursor);
                self.cursor = start;
            }
            Key::Ctrl('k') => self.list = !self.list,
            Key::Ctrl('e') => return Edited::Editor(self.text()),
            Key::Enter if !self.text().trim().is_empty() => return Edited::Submit(self.text()),
            Key::Esc => {
                self.focused = false;
                self.list = false;
            }
            _ => {}
        }
        Edited::Done
    }

    /// The lines the text takes at `width`, and the cursor's line and column among them.
    fn lines(&self, width: usize) -> (Vec<String>, (usize, usize)) {
        let width = width.max(1);
        let (mut lines, mut line) = (Vec::new(), String::new());
        let (mut column, mut cursor) = (0, (0, 0));
        for (at, c) in self.chars.iter().enumerate() {
            if at == self.cursor {
                cursor = (lines.len(), column);
            }
            if *c == '\n' {
                lines.push(std::mem::take(&mut line));
                column = 0;
                continue;
            }
            if column == width {
                lines.push(std::mem::take(&mut line));
                column = 0;
                if at == self.cursor {
                    cursor = (lines.len(), 0);
                }
            }
            line.push(*c);
            column += 1;
        }
        if self.cursor == self.chars.len() {
            if column == width {
                lines.push(std::mem::take(&mut line));
                column = 0;
            }
            cursor = (lines.len(), column);
        }
        lines.push(line);
        (lines, cursor)
    }

    /// How high the bar is at `width`, its border included.
    pub fn height(&self, width: u16) -> u16 {
        let (lines, _) = self.lines(usize::from(width.saturating_sub(2)));
        u16::try_from(lines.len().min(MAX_LINES)).unwrap_or(1) + 2
    }

    /// Draw the bar in `area`, and above it the `completions` of the word at the cursor, else the list of commands
    /// when shown.
    pub fn draw(&self, frame: &mut Frame, area: Rect, completions: &[complete::Candidate]) {
        let width = usize::from(area.width.saturating_sub(2));
        let (lines, (row, column)) = self.lines(width);
        let top = (row + 1).saturating_sub(MAX_LINES);
        let mut block = frame::block(" > ".to_owned(), self.focused);
        if let Some(status) = &self.status {
            let style = match status.tone {
                Tone::Busy => Style::new().fg(Color::Cyan),
                Tone::Done => Style::new().fg(Color::Green),
                Tone::Warn => Style::new().fg(Color::Yellow),
                Tone::Error => Style::new().fg(Color::Red),
            };
            let room = usize::from(area.width.saturating_sub(4));
            let text: String = status.text.replace('\n', " ").chars().take(room).collect();
            block = block.title_bottom(Line::styled(format!(" {text} "), style));
        }
        let shown: Vec<Line> = if self.chars.is_empty() {
            let pad = width.saturating_sub(HINT.chars().count());
            vec![Line::from(vec![Span::raw(" ".repeat(pad)), Span::styled(HINT, frame::dim())])]
        } else {
            lines.into_iter().skip(top).take(MAX_LINES).map(Line::raw).collect()
        };
        let inner = block.inner(area);
        frame.render_widget(Paragraph::new(shown).block(block), area);
        if self.focused {
            let x = inner.x.saturating_add(u16::try_from(column).unwrap_or(u16::MAX));
            let y = inner.y.saturating_add(u16::try_from(row - top).unwrap_or(u16::MAX));
            frame.set_cursor_position(Position { x: x.min(inner.right().saturating_sub(1)), y });
        }
        if !completions.is_empty() {
            complete::draw(frame, area, completions, self.pick.selected);
        } else if self.list {
            draw_list(frame, area);
        }
    }
}

/// The slash commands, over the view just above the bar at `bar`.
fn draw_list(frame: &mut Frame, bar: Rect) {
    let lines: Vec<Line> = COMMANDS
        .iter()
        .map(|(name, args, what)| {
            Line::from(vec![
                Span::styled(format!("/{name:<10}"), Style::new().bold()),
                Span::raw(format!("{args:<24}")),
                Span::styled(*what, frame::dim()),
            ])
        })
        .collect();
    let height = (u16::try_from(lines.len()).unwrap_or(u16::MAX) + 2).min(bar.y);
    let area = Rect { x: bar.x, y: bar.y.saturating_sub(height), width: bar.width.min(84), height };
    frame.render_widget(Clear, area);
    frame.render_widget(Paragraph::new(lines).block(frame::block(" commands ".to_owned(), true)), area);
}

impl Model {
    /// Take `key` for the bar when it is focused or `key` focuses it: Ctrl-K, or @ or # in a view. Returns None when
    /// the key is not the bar's.
    pub fn bar_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Option<Action>> {
        if !self.bar.focused {
            match key {
                Key::Ctrl('k') => {}
                Key::Char(c @ ('@' | '#')) if self.focus == Focus::View => self.bar.insert(c),
                _ => return None,
            }
            self.bar.focused = true;
            return Some(None);
        }
        if self.complete_key(key) {
            return Some(None);
        }
        let before = self.bar.text();
        let edited = self.bar.key(key);
        if self.bar.text() != before {
            self.bar.pick = complete::Pick::default();
        }
        Some(match edited {
            Edited::Done => None,
            Edited::Submit(text) => self.submit(text, now),
            Edited::Editor(text) => Some(Action::Edit(text)),
        })
    }

    /// Do what the bar's `text` says, emptying the bar; else keep it and tell why on its status line.
    fn submit(&mut self, text: String, now: DateTime<Utc>) -> Option<Action> {
        let command = parse(&text);
        let slash = matches!(command, Ok(Command::Slash(_)));
        let done = command.and_then(|command| self.command(command, now));
        let action = match done {
            Ok(action) => action,
            Err(why) => {
                self.bar.list = why.starts_with("usage:") || why.starts_with("no command");
                self.bar.set_status(why, Tone::Error);
                return None;
            }
        };
        self.bar.set_text("");
        self.bar.list = false;
        self.bar.status = None;
        if slash {
            self.bar.focused = false;
        }
        match action {
            Some(Action::Run(job, _)) => {
                self.bar.set_status(if slash { "running…" } else { "sending…" }, Tone::Busy);
                Some(Action::Run(job, text))
            }
            Some(Action::Decide(request, verdict)) => {
                self.bar.set_status(format!("answering request {}…", tools::qualified(&request)), Tone::Busy);
                Some(Action::Decide(request, verdict))
            }
            other => other,
        }
    }

    /// The action `command` takes, or why it cannot be taken.
    fn command(&mut self, command: Command, now: DateTime<Utc>) -> Result<Option<Action>, String> {
        let run = |job| Ok(Some(Action::Run(job, String::new())));
        let slash = match command {
            Command::Ask { to, text } => return run(Job::Ask { to, thread: None, text, steer: false }),
            Command::Post { channel, text } => return run(Job::Post { channel, thread: None, text }),
            Command::Say(text) => return run(self.say(text)),
            Command::Slash(slash) => slash,
        };
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
            Slash::Steer(text) => {
                let thread = self.target()?;
                match self.busy(&thread).into_iter().next() {
                    Some(to) => run(Job::Ask { to, thread: Some(thread), text, steer: true }),
                    None => run(Job::Reply { thread, text, steer: true }),
                }
            }
            Slash::Answer(choice) => run(Job::Answer { thread: self.target()?, choice }),
            Slash::Done(done) => run(Job::Done { thread: self.target()?, done }),
            Slash::Mark(marking) => run(Job::Mark { thread: self.target()?, marking }),
            Slash::Mute(hold) => run(Job::Mute { thread: self.target()?, hold: hold.unwrap_or(quiet::MUTE_FOR) }),
            Slash::Unmute => run(Job::Unmute { thread: self.target()? }),
            Slash::Title(text) => run(Job::Title { thread: self.target()?, text }),
            Slash::React(reaction) => {
                let message = match self.page() {
                    Page::Channel(page) => page.post(&self.data).map(|p| p.id),
                    _ => None,
                };
                run(Job::React { thread: self.target()?, message, reaction })
            }
            Slash::Goto(to) => self.goto(to),
            Slash::Search(query) => self.search(query, now).map(Some),
            Slash::Digest(_) => Err("/digest is not in ants chat yet".to_owned()),
            Slash::Tldr => Err("/tldr is not in ants chat yet".to_owned()),
            Slash::Tree => Err("/tree is not in ants chat yet".to_owned()),
            Slash::Quit => Ok(Some(Action::Quit)),
        }
    }

    /// What plain text does: reply on the open thread, as a post on a channel's thread, else ask the messenger.
    fn say(&self, text: String) -> Job {
        let page = match self.page() {
            Page::Thread(page) => page,
            Page::Dm(page) => return Job::Ask { to: page.with.clone(), thread: None, text, steer: false },
            _ => return Job::Ask { to: UserId::messenger(), thread: None, text, steer: false },
        };
        let thread = page.history.thread.clone();
        match self.data.live.summary(&thread).and_then(|s| s.channels.first()) {
            Some(channel) => Job::Post { channel: channel.clone(), thread: Some(thread), text },
            None => Job::Reply { thread, text, steer: false },
        }
    }

    /// The thread slash commands act on: the open one, else the one selected in the view.
    pub fn target(&self) -> Result<ThreadId, String> {
        let data = &self.data;
        let thread = match self.page() {
            Page::Thread(page) => Some(page.history.thread.clone()),
            Page::Home(page) => page.thread(data),
            Page::Work(page) => page.thread(data),
            Page::Channel(page) => page.post(data).map(|p| p.thread.clone()),
            Page::Dm(_) | Page::Search(_) => None,
        };
        thread.ok_or_else(|| "no thread open or selected".to_owned())
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
                let found: Vec<&ThreadId> =
                    self.data.live.order.iter().filter(|t| t.as_str().starts_with(&prefix)).collect();
                let thread = match found[..] {
                    [thread] => (*thread).clone(),
                    [] if ants_proto::Uuid::try_parse(&prefix).is_ok() => prefix.parse().map_err(|e| format!("{e}"))?,
                    [] => return Err(format!("no thread {prefix} read")),
                    _ => return Err(format!("{} threads start {prefix}: give more of the id", found.len())),
                };
                self.focus = Focus::View;
                self.pages.push(Page::Thread(thread::Page::new(thread.clone())));
                return Ok(Some(Action::Open(thread)));
            }
        };
        self.focus = Focus::View;
        Ok(self.select(item))
    }

    /// Take how a job the bar ran went: its line and warnings on the status line, opening the thread it started; on
    /// failure, the error, and its text back into the bar if empty.
    pub fn outcome(&mut self, outcome: Outcome) -> Option<Action> {
        match outcome.result {
            Ok(done) => {
                let tone = if done.warnings.is_empty() { Tone::Done } else { Tone::Warn };
                let text = std::iter::once(done.line).chain(done.warnings).collect::<Vec<_>>().join(" · ");
                self.bar.set_status(text, tone);
                let thread = done.open?;
                if matches!(self.page(), Page::Thread(page) if page.history.thread == thread) {
                    return None;
                }
                self.pages.push(Page::Thread(thread::Page::new(thread.clone())));
                Some(Action::Open(thread))
            }
            Err(e) => {
                self.bar.set_status(format!("{e:#}"), Tone::Error);
                if self.bar.is_empty() && !outcome.text.is_empty() {
                    self.bar.set_text(&outcome.text);
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

    use super::*;
    use crate::live::tests::{at, message, request, user};
    use crate::tui::exec::Done;
    use crate::tui::model::tests::model;
    use crate::tui::reads::Read;

    fn typed(m: &mut Model, text: &str) {
        for c in text.chars() {
            m.key(if c == '\n' { Key::Newline } else { Key::Char(c) }, at(0));
        }
    }

    #[test]
    fn text_asks_a_user_posts_to_a_channel_or_is_said_on_the_thread() {
        let ask = parse(" @dev-a check\nthe build ").unwrap();
        assert_eq!(ask, Command::Ask { to: user("dev-a"), text: "check\nthe build".into() });
        assert_eq!(
            parse("#ops deploy?").unwrap(),
            Command::Post { channel: "ops".parse().unwrap(), text: "deploy?".into() }
        );
        assert_eq!(parse("looks good").unwrap(), Command::Say("looks good".into()));
        assert_eq!(parse("//not a command").unwrap(), Command::Say("/not a command".into()));
        assert_eq!(parse("@dev-a").unwrap_err(), "nothing to send to @dev-a");
        assert_eq!(parse("#ops  ").unwrap_err(), "nothing to post to #ops");
        assert!(parse("@Dev A hi").is_err());
    }

    #[test]
    fn slash_commands_take_their_arguments_and_refuse_others() {
        let ok = |text: &str| match parse(text) {
            Ok(Command::Slash(slash)) => slash,
            other => panic!("{text}: {other:?}"),
        };
        assert_eq!(ok("/approve"), Slash::Approve { id: None, always: false });
        assert_eq!(ok("/APPROVE dev-a/019a --always"), Slash::Approve { id: Some("dev-a/019a".into()), always: true });
        assert_eq!(ok("/reject 019a"), Slash::Reject { id: Some("019a".into()) });
        assert_eq!(ok("/grant"), Slash::Grant(None));
        assert_eq!(ok("/answer 2"), Slash::Answer(Choice::Option(2)));
        assert_eq!(ok("/answer ship it"), Slash::Answer(Choice::Typed("ship it".into())));
        assert_eq!(ok("/mute 2h"), Slash::Mute(Some(Duration::from_hours(2))));
        assert_eq!(ok("/react eyes"), Slash::React("eyes".parse().unwrap()));
        assert_eq!(ok("/goto #ops"), Slash::Goto(Goto::Channel("ops".parse().unwrap())));
        assert_eq!(ok("/goto @bob"), Slash::Goto(Goto::User(user("bob"))));
        assert_eq!(ok("/goto 019A"), Slash::Goto(Goto::Thread("019a".into())));
        assert_eq!(ok("/archive"), Slash::Mark(Marking::Archive));
        assert_eq!(parse("/steer").unwrap_err(), "usage: /steer TEXT");
        assert_eq!(parse("/done now").unwrap_err(), "usage: /done");
        assert_eq!(parse("/approve a b").unwrap_err(), "usage: /approve [ID] [--always]");
        assert_eq!(parse("/frobnicate").unwrap_err(), "no command /frobnicate");
        assert!(parse("/mute forever").is_err());
        assert!(parse("/react Thumbs Up").is_err());
    }

    #[test]
    fn the_bar_edits_its_text() {
        let mut bar = Bar::default();
        for c in "say hi there".chars() {
            bar.key(Key::Char(c));
        }
        bar.key(Key::Ctrl('w'));
        assert_eq!(bar.text(), "say hi ");
        bar.key(Key::Backspace);
        bar.key(Key::Left);
        bar.key(Key::Left);
        bar.key(Key::Char('X'));
        bar.key(Key::Delete);
        assert_eq!(bar.text(), "say Xi");
        bar.key(Key::End);
        bar.key(Key::Newline);
        bar.key(Key::Char('!'));
        assert_eq!(bar.key(Key::Enter), Edited::Submit("say Xi\n!".into()));
        assert_eq!(bar.key(Key::Ctrl('e')), Edited::Editor("say Xi\n!".into()));
        bar.key(Key::Ctrl('u'));
        assert_eq!(bar.key(Key::Enter), Edited::Done, "nothing to send");
    }

    #[test]
    fn the_text_wraps_with_the_cursor_where_it_is() {
        let mut bar = Bar::default();
        bar.set_text("abcdef\ngh");
        assert_eq!(bar.lines(4), (vec!["abcd".into(), "ef".into(), "gh".into()], (2, 2)));
        bar.set_text("abcd");
        assert_eq!(bar.lines(4), (vec!["abcd".into(), String::new()], (1, 0)), "a full line moves the cursor on");
        assert_eq!(bar.height(6), 4);
        bar.key(Key::Home);
        assert_eq!(bar.lines(4).1, (0, 0));
        assert_eq!(bar.height(6), 3);
    }

    #[test]
    fn ctrl_k_or_at_or_hash_focus_the_bar_which_then_takes_every_key() {
        let mut m = model();
        assert_eq!(m.key(Key::Ctrl('k'), at(0)), None);
        assert!(m.bar.focused);
        assert_eq!(m.key(Key::Char('q'), at(0)), None, "q types");
        assert_eq!(m.key(Key::Esc, at(0)), None);
        assert!(!m.bar.focused && m.bar.text() == "q", "Esc leaves the bar, keeping its text");
        m.bar.set_text("");
        m.key(Key::Char('@'), at(0));
        assert!(m.bar.focused && m.bar.text() == "@");
        typed(&mut m, "dev-a deploy");
        let action = m.key(Key::Enter, at(0));
        let job = Job::Ask { to: user("dev-a"), thread: None, text: "deploy".into(), steer: false };
        assert_eq!(action, Some(Action::Run(job, "@dev-a deploy".into())));
        assert!(m.bar.focused && m.bar.is_empty(), "the bar stays for the next message");
        assert_eq!(m.bar.status.as_ref().map(|s| s.tone), Some(Tone::Busy));
        m.key(Key::Esc, at(0));
        m.key(Key::Tab, at(0));
        m.key(Key::Char('#'), at(0));
        assert!(!m.bar.focused, "# in the nav is no bar's");
    }

    #[test]
    fn plain_text_replies_on_the_open_thread_and_else_asks_the_messenger() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.key(Key::Ctrl('k'), at(0));
        typed(&mut m, "who owns billing?");
        let job = Job::Ask { to: UserId::messenger(), thread: None, text: "who owns billing?".into(), steer: false };
        assert_eq!(m.key(Key::Enter, at(0)), Some(Action::Run(job, "who owns billing?".into())));
        m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
        typed(&mut m, "go on");
        let reply = Job::Reply { thread: thread.clone(), text: "go on".into(), steer: false };
        assert_eq!(m.key(Key::Enter, at(0)), Some(Action::Run(reply, "go on".into())));
        let mut post = message(1, &thread, "dev-a", "ops", "hi");
        (post.kind, post.channel) = (crate::thread::Kind::Post, Some("ops".parse().unwrap()));
        m.stored(vec![post], at(1));
        typed(&mut m, "go on");
        let post = Job::Post { channel: "ops".parse().unwrap(), thread: Some(thread), text: "go on".into() };
        assert_eq!(m.key(Key::Enter, at(1)), Some(Action::Run(post, "go on".into())), "a channel thread gets a post");
    }

    #[test]
    fn a_dm_being_written_keeps_its_keys_and_the_bar_asks_its_person() {
        let mut m = model();
        m.pages.push(Page::Dm(crate::tui::dm::Page::new(user("bob"))));
        typed(&mut m, "n@");
        assert!(!m.bar.focused, "@ goes to the message being written");
        m.key(Key::Esc, at(0));
        m.key(Key::Ctrl('k'), at(0));
        typed(&mut m, "n");
        assert_eq!(m.bar.text(), "n", "the focused bar takes the keys of the DM");
        let job = Job::Ask { to: user("bob"), thread: None, text: "n".into(), steer: false };
        assert_eq!(m.key(Key::Enter, at(0)), Some(Action::Run(job, "n".into())));
    }

    #[test]
    fn slash_commands_act_on_the_selection_or_open_thread() {
        let thread = ThreadId::generate();
        let mut m = model();
        let run = |m: &mut Model, text: &str| {
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
        let steer = Job::Ask { to: user("dev-a"), thread: Some(thread.clone()), text: "left".into(), steer: true };
        assert_eq!(run(&mut m, "/steer left"), Some(Action::Run(steer, "/steer left".into())));
        let grant = Job::Grant { thread, text: GRANT.into() };
        assert_eq!(run(&mut m, "/grant"), Some(Action::Run(grant, "/grant".into())));
        assert_eq!(run(&mut m, "/nope"), None);
        assert!(m.bar.list, "an unknown command shows the list");
        assert_eq!(run(&mut m, "/quit"), Some(Action::Quit));
    }

    #[test]
    fn approve_answers_the_request_on_the_thread_or_the_one_named() {
        let mut m = model();
        let push = request("dev-a", "git push", vec![Verdict::AllowOnce]);
        m.take(live::Event::Approval(tools::Update::Pending(push.clone())), at(0));
        let run = |m: &mut Model, text: &str| {
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
    fn goto_shows_a_thread_a_channel_or_a_user() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy")], at(1));
        let ops = ChannelEntry {
            channel: "ops".parse().unwrap(),
            members: vec![user("alice")],
            messenger: None,
            owner: None,
        };
        m.read(Read::Directory(Ok((vec![ops], ants_proto::Humans::default()))), at(1));
        let run = |m: &mut Model, text: &str| {
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
    fn a_failed_send_comes_back_into_the_bar_and_a_new_thread_opens() {
        let mut m = model();
        let failed = Outcome { result: Err(anyhow::anyhow!("no route")), text: "@dev-a deploy".into() };
        assert_eq!(m.outcome(failed), None);
        assert_eq!((m.bar.text(), m.bar.focused), ("@dev-a deploy".into(), true));
        assert_eq!(m.bar.status, Some(Status { text: "no route".into(), tone: Tone::Error }));
        let thread = ThreadId::generate();
        let warnings = vec!["dev-a is served by no daemon".to_owned()];
        let done = Done { line: "sent to dev-a".into(), warnings, open: Some(thread.clone()) };
        let sent = Outcome { result: Ok(done), text: "@dev-a deploy".into() };
        assert_eq!(m.outcome(sent), Some(Action::Open(thread)));
        assert_eq!(m.page().name(), "thread");
        assert_eq!(m.bar.text(), "@dev-a deploy", "a success leaves the bar's text alone");
        let status = m.bar.status.as_ref().unwrap();
        assert_eq!((status.text.as_str(), status.tone), ("sent to dev-a · dev-a is served by no daemon", Tone::Warn));
    }
}
