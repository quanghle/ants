//! How a key changes the model: which of the prompts, the bar, the nav or the shown view takes it, and where a view's
//! key leads ([`To`]).

use chrono::{DateTime, Utc};

use super::{Action, Focus, Key, Level, Model, Page, To};
use crate::tui::approvals::{Answer, Asking};
use crate::tui::command::Tone;
use crate::tui::exec::Job;
use crate::tui::react::{self, Pick, Picker};
use crate::tui::{credentials, held, nav, thread, work};

impl Model {
    pub fn key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Action> {
        if key == Key::Interrupt {
            return Some(Action::Quit);
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
        if !self.bar.focused
            && self.focus == Focus::View
            && let Some(Page::Dm(page)) = self.pages.last_mut()
            && let Some(action) = page.write(key)
        {
            return action;
        }
        if !self.bar.focused
            && self.focus == Focus::View
            && let Some(Page::Ant(page)) = self.pages.last_mut()
            && let Some(action) = page.write(key)
        {
            return action;
        }
        if let Some(action) = self.bar_key(key, now) {
            return action;
        }
        match key {
            Key::Char('q') => Some(Action::Quit),
            Key::Char('/') => {
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

    /// Take `key` while a tool request is being answered.
    fn answer(&mut self, key: Key, now: DateTime<Utc>) -> Option<Action> {
        let asking = self.asking.as_mut()?;
        match asking.key(key) {
            Answer::Keep => {}
            Answer::Close(line) => {
                self.asking = None;
                if let Some(line) = line {
                    self.notify(line, Level::Info, now);
                }
            }
            Answer::Note(line) => self.notify(line, Level::Info, now),
            Answer::Refused(refusal) => self.notify(refusal, Level::Error, now),
            Answer::Decide(verdict) => {
                return self.asking.as_ref().map(|a| Action::Decide(Box::new(a.request.clone()), verdict));
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

    fn nav_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Action> {
        let by = match key {
            Key::Enter => {
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
            To::Ask(request, verdict) => {
                match Asking::new(request, verdict) {
                    Ok(asking) => self.asking = Some(asking),
                    Err(refusal) => self.notify(refusal, Level::Error, now),
                }
                None
            }
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
            To::Do(action @ Action::Run(..)) => {
                self.bar.set_status("running…", Tone::Busy);
                Some(action)
            }
            To::Do(action) => Some(action),
        }
    }
}
