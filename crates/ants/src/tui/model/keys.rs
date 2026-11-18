//! How a key changes the model: which of the prompts, the bar, the nav or the shown view takes it, and where a view's
//! key leads ([`To`]).

use ants_proto::{ApprovalRequest, Verdict};
use chrono::{DateTime, Utc};

use super::{Action, Focus, Key, Level, Model, Page, To};
use crate::tui::approvals::{Answer, Asking};
use crate::tui::command::{Note, Tone};
use crate::tui::exec::Job;
use crate::tui::react::{self, Pick, Picker};
use crate::tui::{channel, credentials, held, nav, thread, work};

impl Model {
    pub fn key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Action> {
        self.follow_drafts();
        if key == Key::Interrupt {
            return Some(Action::Quit);
        }
        if self.esc_again(key, now) {
            return None;
        }
        if self.asking.as_ref().is_some_and(Asking::takes_keys) && !self.help {
            return self.answer(key, now);
        }
        if self.compact {
            return self.compact_key(key, now);
        }
        if self.help {
            self.help = false;
            return None;
        }
        if self.catchup.shown() {
            return self.digest_key(key);
        }
        if self.interrupting.is_some() {
            return self.confirm_key(key);
        }
        if self.snoozing.is_some() {
            return self.snooze_key(key, now);
        }
        if let Some(picker) = &mut self.picking {
            return match picker.key(key) {
                Pick::Typing => None,
                Pick::Close => {
                    self.picking = None;
                    None
                }
                Pick::React(message, reaction) => {
                    self.picking = None;
                    let reacted = self.data.reactions.get(&message).map_or(&[][..], Vec::as_slice);
                    let remove = react::reacted(reacted, self.data.me(), &reaction);
                    Some(Action::React(message, reaction, remove))
                }
            };
        }
        if let Some(action) = self.fold_key(key, now) {
            return Some(action);
        }
        if let Some(action) = self.typed_key(key, now) {
            return action;
        }
        if !self.bar.focused
            && self.focus == Focus::View
            && let Some(action) = self.card_key(key, now)
        {
            return action;
        }
        if let Some(action) = self.bar_key(key, now) {
            return action;
        }
        match key {
            Key::Char('q') => Some(Action::Quit),
            Key::Char('/') | Key::Ctrl('f') => {
                self.bar.focused = true;
                if self.bar.is_empty() {
                    self.bar.set_text("/search ");
                }
                None
            }
            Key::Char('?') => {
                self.help = true;
                None
            }
            Key::Char('R') => Some(Action::Refresh),
            Key::Tab => {
                self.focus = match self.focus {
                    Focus::Nav => Focus::View,
                    Focus::View => Focus::Nav,
                };
                None
            }
            _ => match self.focus {
                Focus::Nav => self.nav_key(key, now),
                Focus::View => self.view_key(key, now),
            },
        }
    }

    /// Take `key` if it is typed into what the view on screen holds: the text found in a thread, a DM's message, an
    /// ant's prompt or a channel's editor.
    fn typed_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Option<Action>> {
        if self.bar.focused || self.focus != Focus::View {
            return None;
        }
        match self.pages.last_mut()? {
            Page::Thread(page) => page.find_key(key).then_some(None),
            Page::Dm(page) => page.write(key),
            Page::Ant(page) => page.write(key),
            Page::Channel(page) => {
                let to = page.write(key, &self.data)?;
                Some(to.and_then(|to| self.go(to, now)))
            }
            _ => None,
        }
    }

    /// Take `text` pasted: into the prompt or the draft being typed, on one line, else into the bar, focused for it,
    /// line breaks and all; dropped where only keys are answered (help, the digest, a tool request, a confirmation) and
    /// in the compact view, rather than taken as keys.
    pub fn paste(&mut self, text: &str, now: DateTime<Utc>) {
        self.follow_drafts();
        let text = pasted(text);
        if text.is_empty()
            || self.compact
            || self.help
            || self.catchup.shown()
            || self.interrupting.is_some()
            || self.asking.as_ref().is_some_and(Asking::takes_keys)
        {
            return;
        }
        let line = text.replace(['\n', '\t'], " ");
        if self.snoozing.is_some() {
            line.chars().for_each(|c| _ = self.snooze_key(Key::Char(c), now));
            return;
        }
        if let Some(picker) = &mut self.picking {
            line.chars().for_each(|c| _ = picker.key(Key::Char(c)));
            return;
        }
        if !self.bar.focused
            && self.focus == Focus::View
            && (self.card_paste(&line, now) || self.pages.last_mut().is_some_and(|page| write(page, &line)))
        {
            return;
        }
        self.paste_in_bar(&text);
    }

    /// Take `key` on the question card focused in the view shown, if any: where it leads, if anywhere.
    fn card_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Option<Action>> {
        let data = &self.data;
        let to = match self.pages.last_mut()? {
            Page::Home(page) => page.card_key(key, data, now)?,
            Page::Thread(page) => page.card_key(key, data, now)?,
            _ => return None,
        };
        Some(to.and_then(|to| self.go(to, now)))
    }

    /// Type `text` pasted on the question card focused in the view shown, if any: whether it took it.
    fn card_paste(&mut self, text: &str, now: DateTime<Utc>) -> bool {
        let data = &self.data;
        match self.pages.last_mut() {
            Some(Page::Home(page)) => page.card_paste(text, data, now),
            Some(Page::Thread(page)) => page.card_paste(text, data, now),
            _ => false,
        }
    }

    /// Take `key` while a tool request is being answered.
    fn answer(&mut self, key: Key, now: DateTime<Utc>) -> Option<Action> {
        let asking = self.asking.as_mut()?;
        match asking.key(key) {
            Answer::Keep => {}
            Answer::Close => self.asking = None,
            Answer::Refused(refusal) => self.notify(refusal, Level::Error, now),
            Answer::Decide(verdict) => {
                return self.asking.as_ref().map(|a| Action::Decide(Box::new(a.request.clone()), verdict));
            }
            Answer::Note => {
                let request = self.asking.take()?.request;
                return self.note_on(request, now);
            }
            Answer::Open(thread) => {
                self.asking = None;
                if self.open() != Some(&thread) {
                    self.focus = Focus::View;
                    return self.go(To::Thread(thread), now);
                }
            }
        }
        None
    }

    /// Open the bar on a note rejecting `request`, unless it holds a draft or `request` can no longer be rejected.
    fn note_on(&mut self, request: ApprovalRequest, now: DateTime<Utc>) -> Option<Action> {
        if let Some(refusal) = crate::live::refusal(&request, &crate::tools::State::Pending, Verdict::Reject) {
            self.notify(refusal, Level::Error, now);
            return None;
        }
        if !self.bar.is_empty() && self.bar.noting.as_ref().is_none_or(|note| note.request() != &request) {
            self.bar.set_status("the bar holds a draft: send or clear it before writing a note", Tone::Error);
            return None;
        }
        self.bar.noting = Some(Note::Reject(Box::new(request)));
        self.bar.quoting = None;
        self.bar.focused = true;
        None
    }

    /// Answer `request` with `verdict` at once, unless another decision is still being stored.
    fn decide(&mut self, request: ApprovalRequest, verdict: Verdict, now: DateTime<Utc>) -> Option<Action> {
        if let Some(storing) = self.asking.as_ref().filter(|a| a.storing) {
            let id = crate::tools::qualified(&storing.request);
            self.notify(
                format!("storing the decision on request {id}: answer again once it is stored"),
                Level::Error,
                now,
            );
            return None;
        }
        match Asking::decide(request, verdict) {
            Ok(asking) => {
                let action = Action::Decide(Box::new(asking.request.clone()), verdict);
                self.asking = Some(asking);
                Some(action)
            }
            Err(refusal) => {
                self.notify(refusal, Level::Error, now);
                None
            }
        }
    }

    fn nav_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Action> {
        let by = match key {
            Key::Enter | Key::Esc => {
                self.focus = Focus::View;
                return None;
            }
            Key::Up | Key::Char('k') => -1,
            Key::Down | Key::Char('j') => 1,
            Key::Home => isize::MIN,
            Key::End => isize::MAX,
            _ => return None,
        };
        let items = nav::items(&self.data, now);
        let at = items.iter().position(|i| *i == self.nav).unwrap_or(0);
        let to = items.get(at.saturating_add_signed(by).min(items.len().saturating_sub(1)))?.clone();
        if to == self.nav {
            return None;
        }
        self.select(to)
    }

    pub(in crate::tui) fn view_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Action> {
        if key == Key::Esc {
            if self.pages.len() > 1 {
                self.pages.pop();
                return self.shown_reactions();
            } else {
                self.focus = Focus::Nav;
            }
            return None;
        }
        let data = &self.data;
        let to = match self.pages.last_mut()? {
            Page::Home(page) => page.key(key, data, now),
            Page::Work(page) => page.key(key, data),
            Page::Channel(page) => page.key(key, data),
            Page::Thread(page) => page.key(key, data, now),
            Page::Dm(page) => page.key(key, data),
            Page::Search(page) => page.key(key),
            Page::Tree(page) => page.key(key, data, now),
            Page::Ant(page) => page.key(key, data),
            Page::Credentials(page) => page.key(key, data),
            Page::Held(page) => page.key(key, data),
        }?;
        self.go(to, now)
    }

    /// Go where a view's key leads.
    fn go(&mut self, to: To, now: DateTime<Utc>) -> Option<Action> {
        match to {
            To::Thread(thread) => {
                self.pages.push(Page::Thread(thread::Page::new(thread.clone())));
                Some(Action::Open(thread))
            }
            To::Message(thread, message) => {
                self.pages.push(Page::Thread(thread::Page::at(thread.clone(), message)));
                Some(Action::Open(thread))
            }
            To::Work(filter) => {
                self.pages.push(Page::Work(work::Page::new(filter)));
                None
            }
            To::React(message) => {
                self.picking = Some(Picker::new(message));
                None
            }
            To::Ask(request, None) => {
                self.asking = Some(Asking::full(request));
                None
            }
            To::Ask(request, Some(verdict)) => self.decide(request, verdict, now),
            To::Note(request) => self.note_on(request, now),
            To::Tree(thread) => {
                self.tree(thread);
                None
            }
            To::Interrupt(thread) => self.interrupt(thread),
            To::Reply(thread) => {
                self.bar.focused = true;
                self.go(To::Thread(thread), now)
            }
            To::Quote(thread, quoted, mention) => {
                self.quote(thread, quoted, mention.as_ref());
                None
            }
            To::Snooze(thread) => self.snooze(thread, now),
            To::Unread(thread, from) => self.mark_unread(thread, from, now),
            To::Remind(thread, message) => self.remind(thread, message),
            To::Unremind(thread) => Some(self.set_reminder(thread, None)),
            To::Bar(text) => {
                self.bar.set_text(&text);
                self.bar.focused = true;
                None
            }
            To::Tldr(thread) => self.tldr(thread),
            To::Credentials => {
                self.pages.push(Page::Credentials(credentials::Page::default()));
                None
            }
            To::Do(Action::Credentials(order)) => {
                self.data.credentials.busy = Some(order.clone());
                Some(Action::Credentials(order))
            }
            To::Held => {
                self.pages.push(Page::Held(held::Page::default()));
                None
            }
            To::Do(Action::Answer(answer)) => {
                self.data.held.busy = Some(answer.clone());
                Some(Action::Answer(answer))
            }
            To::Digest => self.home_digest(now),
            To::Tools => {
                self.data.tools = self.data.tools.next();
                self.notify(format!("tool calls: {}", self.data.tools.label()), Level::Info, now);
                Some(Action::KeepTools(self.data.tools))
            }
            To::Do(Action::Run(Job::Open(listed), text)) => {
                self.bar.set_status(format!("opening {}…", listed.attachment.name), Tone::Busy);
                Some(Action::Run(Job::Open(listed), text))
            }
            To::Do(Action::Run(Job::Withdraw { edit: Some(_), .. }, _)) if !self.bar.is_empty() => {
                self.bar.set_status("the bar holds a draft: send or clear it before editing a message", Tone::Error);
                None
            }
            To::Do(Action::KeepChannelTab(channel, tab)) => {
                channel::keep_tab(&mut self.data.channel_tabs, channel.clone(), tab);
                Some(Action::KeepChannelTab(channel, tab))
            }
            To::Do(Action::Run(Job::ChannelNotify { channel, level }, _)) => {
                self.bar.set_status("running…", Tone::Busy);
                Some(self.set_level(channel, level))
            }
            To::Do(action @ Action::Run(..)) => {
                self.bar.set_status("running…", Tone::Busy);
                Some(action)
            }
            To::Do(action) => Some(action),
            To::Refused(why) => {
                self.bar.set_status(why, Tone::Error);
                None
            }
        }
    }
}

/// `text` as pasted: its line breaks `\n` whichever the terminal sent, and no control characters but those and tabs,
/// nor the private-use ones the bar's placeholders take.
fn pasted(text: &str) -> String {
    let text = text.replace("\r\n", "\n").replace('\r', "\n");
    let kept = |c: &char| matches!(c, '\n' | '\t') || !(c.is_control() || ('\u{F0000}'..='\u{FFFFD}').contains(c));
    text.chars().filter(kept).collect()
}

/// Type `line` into the draft `page` is writing, if any: whether it took it.
fn write(page: &mut Page, line: &str) -> bool {
    if let Page::Channel(page) = page {
        return page.paste(line);
    }
    line.chars().all(|c| match page {
        Page::Dm(page) => page.write(Key::Char(c)).is_some(),
        Page::Ant(page) => page.write(Key::Char(c)).is_some(),
        Page::Thread(page) => page.finding() && page.find_key(Key::Char(c)),
        _ => false,
    })
}
