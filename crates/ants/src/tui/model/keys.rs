//! How a key changes the model: which of the prompts, the bar, the nav or the shown view takes it, and where a view's
//! key leads ([`To`]).

use ants_proto::{ApprovalRequest, Verdict};
use chrono::{DateTime, Utc};

use super::{Action, Focus, Key, Level, Model, Overlay, Page, Prompt, QUIT_WITHIN, To};
use crate::tui::approvals::{Answer, Asking};
use crate::tui::command::{Note, Tone};
use crate::tui::exec::Job;
use crate::tui::help::Help;
use crate::tui::react::Picker;
use crate::tui::{channel, credentials, held, nav, notices, thread, work};

impl Model {
    pub fn key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Action> {
        if key == Key::Ctrl('z') {
            return Some(Action::Suspend);
        }
        let choosing_copy = self.copying().is_some();
        let action = self.dispatch(key, now);
        self.settle();
        if !choosing_copy
            && let Some(Action::Copy(text)) = &action
            && let Some(picker) = crate::tui::clipboard::Picker::new(text)
        {
            self.overlay(Overlay::Copy(picker));
            return None;
        }
        action
    }

    fn dispatch(&mut self, key: Key, now: DateTime<Utc>) -> Option<Action> {
        self.follow_drafts();
        let quitting = self.quitting.take().is_some_and(|at| now - at < QUIT_WITHIN);
        self.notice.take_if(|n| n.level == Level::Error);
        if key == Key::Interrupt {
            return self.ctrl_c(quitting, now);
        }
        if self.esc_again(key, now) {
            return None;
        }
        if key == Key::Char('q') && self.overlaid() {
            self.closed_by_q = Some(now);
        }
        if self.asking.as_ref().is_some_and(Asking::takes_keys) && self.help().is_none() {
            return self.answer(key, now);
        }
        if self.compact {
            return self.compact_key(key, now);
        }
        if let Some(taken) = self.overlay_key(key) {
            return taken;
        }
        if self.catchup.shown() {
            return self.digest_key(key);
        }
        if let Some(taken) = self.prompt_key(key, now) {
            return taken;
        }
        if let Some(action) = self.fold_key(key, now) {
            return Some(action);
        }
        if key == Key::Ctrl('l')
            && let Some(Page::Thread(page)) = self.pages.last_mut()
        {
            page.toggle_waiting();
            self.focus = Focus::View;
            self.bar.focused = false;
            return None;
        }
        if key == Key::Ctrl('t') {
            return self.next_unread(now);
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
        if self.bar.focused
            && self.focus == Focus::View
            && matches!(key, Key::PageUp | Key::PageDown | Key::CtrlHome | Key::CtrlEnd)
            && matches!(self.page(), Page::Thread(_))
        {
            return self.view_key(key, now);
        }
        if let Some(action) = self.conversation_key(key, now) {
            return action;
        }
        if let Some(action) = self.bar_key(key, now) {
            return action;
        }
        match key {
            Key::Char('q') => self.quit(now),
            Key::Char('/') | Key::Ctrl('f') => {
                self.bar.focused = true;
                if self.bar.is_empty() {
                    self.bar.set_text(if key == Key::Ctrl('f') { "/search " } else { "/" });
                }
                None
            }
            Key::Char('?') => {
                self.overlay(Overlay::Help(Help::default()));
                None
            }
            Key::Char('N') => {
                self.overlay(Overlay::Log(notices::Log::default()));
                None
            }
            Key::Char('R') => self.reread(),
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

    /// Take `key` if it is typed into what the view on screen holds: the text found in a thread, an ant's prompt
    /// or a channel's editor.
    fn typed_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Option<Action>> {
        if self.bar.focused || self.focus != Focus::View {
            return None;
        }
        match self.pages.last_mut()? {
            Page::Thread(page) => page.find_key(key).then_some(None),
            Page::Ant(page) => page.write(key),
            Page::Channel(page) => {
                let to = page.write(key, &self.data)?;
                Some(to.and_then(|to| self.go(to, now)))
            }
            _ => None,
        }
    }

    /// R: read everything again, the shown ant's memories, MCP servers and settings too.
    fn reread(&mut self) -> Option<Action> {
        if let Some(Page::Ant(page)) = self.pages.last_mut() {
            page.reread();
        }
        Some(Action::Refresh)
    }

    /// Whether the help, the notices, the digest or a tool request in full covers the view.
    fn overlaid(&self) -> bool {
        self.covered()
            || self.catchup.shown()
            || self.asking.as_ref().is_some_and(Asking::takes_keys)
            || matches!(self.page(), Page::Thread(page) if page.listing())
    }

    /// q: quit, unless the bar holds text or q closed an overlay within [`QUIT_WITHIN`], which a q coming that soon
    /// again extends.
    pub(in crate::tui) fn quit(&mut self, now: DateTime<Utc>) -> Option<Action> {
        if self.closed_by_q.is_some_and(|at| now - at < QUIT_WITHIN) {
            self.closed_by_q = Some(now);
            return None;
        }
        if !self.bar.is_empty() {
            self.notify("q quits with the bar empty: Ctrl-C empties it".to_owned(), Level::Info, now);
            return None;
        }
        Some(Action::Quit)
    }

    /// Ctrl-C: close what is open, else empty the bar, else quit when `quitting`, the Ctrl-C before having asked for
    /// it, else ask for a second one.
    fn ctrl_c(&mut self, quitting: bool, now: DateTime<Utc>) -> Option<Action> {
        if self.close(now) {
            return None;
        }
        if !self.bar.is_empty() || self.bar.quoting.is_some() || self.bar.noting.is_some() {
            self.empty_into_history();
            return None;
        }
        if quitting {
            return Some(Action::Quit);
        }
        self.quitting = Some(now);
        None
    }

    /// Close what is open, the topmost first: the help or the notices, the digest, a tool request in full, a prompt, the
    /// bar's list or the palette, or what the view asks, finds or has being written. Whether anything was.
    fn close(&mut self, now: DateTime<Utc>) -> bool {
        if self.close_overlay() {
            return true;
        }
        if let Some(digest) = self.catchup.digest.as_mut().filter(|d| d.shown) {
            digest.shown = false;
            return true;
        }
        if self.asking.as_ref().is_some_and(Asking::takes_keys) {
            self.answer(Key::Esc, now);
            return true;
        }
        if self.prompt.take().is_some() {
            return true;
        }
        if self.bar.focused && !self.completions().is_empty() {
            self.bar.pick.closed = true;
            return true;
        }
        if !self.palette(now).is_empty() {
            self.close_palette();
            return true;
        }
        if let Some(Page::Thread(page)) = self.pages.last_mut()
            && page.listing()
        {
            page.toggle_waiting();
            self.bar.focused = false;
            return true;
        }
        if self.bar.focused || self.focus != Focus::View {
            return false;
        }
        let data = &self.data;
        self.pages.last_mut().is_some_and(|page| page.close(data)) || self.card_key(Key::Esc, now).is_some()
    }

    /// Take `text` pasted: into the prompt or the draft being typed, on one line, else into the bar, focused for it,
    /// line breaks and all; dropped where only keys are answered (help, the digest, a tool request, a confirmation) and
    /// in the compact view, rather than taken as keys.
    pub fn paste(&mut self, text: &str, now: DateTime<Utc>) {
        self.follow_drafts();
        let text = pasted(text);
        if text.is_empty()
            || self.compact
            || self.drops_paste()
            || self.catchup.shown()
            || self.asking.as_ref().is_some_and(Asking::takes_keys)
            || (!self.bar.focused && matches!(self.page(), Page::Thread(page) if page.listing()))
        {
            return;
        }
        let line = text.replace(['\n', '\t'], " ");
        if self.prompt_paste(&line, now) {
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
        if self.still_storing(now) {
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

    /// Allow each of `requests` once, in turn, as one batch, unless another decision is still being stored.
    fn allow_all(&mut self, requests: Vec<ApprovalRequest>, now: DateTime<Utc>) -> Option<Action> {
        if self.still_storing(now) {
            return None;
        }
        match Asking::allow_all(requests) {
            Ok(asking) => {
                let mut requests = vec![asking.request.clone()];
                requests.extend(asking.batch.iter().cloned());
                self.asking = Some(asking);
                Some(Action::AllowAll(requests))
            }
            Err(refusal) => {
                self.notify(refusal, Level::Error, now);
                None
            }
        }
    }

    /// Whether a decision is still being stored, which the status line then says: one at a time.
    fn still_storing(&mut self, now: DateTime<Utc>) -> bool {
        let Some(storing) = self.asking.as_ref().filter(|a| a.storing) else { return false };
        let text = match storing.batch.len() {
            0 => format!("storing the decision on request {}", crate::tools::qualified(&storing.request)),
            n => format!("allowing {} requests of {} once", n + 1, storing.request.ant),
        };
        self.notify(format!("{text}: answer again once it is stored"), Level::Error, now);
        true
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
        let items = nav::items(&self.data, &self.nav, now);
        let at = items.iter().position(|i| *i == self.nav).unwrap_or(0);
        let to = items.get(at.saturating_add_signed(by).min(items.len().saturating_sub(1)))?.clone();
        if to == self.nav {
            return None;
        }
        self.select(to)
    }

    pub(in crate::tui) fn view_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Action> {
        if key == Key::Esc {
            if self.pages.last_mut().is_some_and(Page::disarm) {
                return None;
            }
            if self.pages.len() > 1 {
                self.pop();
                return self.shown_reactions();
            } else {
                self.focus = Focus::Nav;
            }
            return None;
        }
        let (data, ui) = (&self.data, &self.ui);
        let to = self.pages.last_mut()?.key(key, data, ui, now)?;
        self.go(to, now)
    }

    /// Go where a view's key leads.
    fn go(&mut self, to: To, now: DateTime<Utc>) -> Option<Action> {
        match to {
            To::Thread(thread) => {
                self.push(Page::Thread(thread::Page::new(thread.clone())));
                Some(Action::Open(thread))
            }
            To::Message(thread, message) => {
                self.push(Page::Thread(thread::Page::at(thread.clone(), message)));
                Some(Action::Open(thread))
            }
            To::Work(filter) => {
                self.push(Page::Work(work::Page::new(filter)));
                None
            }
            To::React(message) => {
                self.prompt = Some(Prompt::React(Picker::new(message)));
                None
            }
            To::Ask(request, None) => {
                self.asking = Some(Asking::full(request));
                None
            }
            To::Ask(request, Some(verdict)) => self.decide(request, verdict, now),
            To::AllowAll(requests) => self.allow_all(requests, now),
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
            To::Write => {
                self.bar.focused = true;
                None
            }
            To::Bar(text) => {
                self.bar.set_text(&text);
                self.bar.focused = true;
                None
            }
            To::Forward => self.go(To::Bar("/forward @".to_owned()), now),
            To::Tldr(thread) => self.tldr(thread),
            To::Credentials => {
                self.push(Page::Credentials(credentials::Page::default()));
                None
            }
            To::Held => {
                self.push(Page::Held(held::Page::default()));
                None
            }
            To::Digest => self.home_digest(now),
            To::Tools => {
                self.ui.tools = self.ui.tools.next();
                self.notify(format!("tool calls: {}", self.ui.tools.label()), Level::Info, now);
                Some(Action::KeepTools(self.ui.tools))
            }
            To::Do(action) => self.run(action),
            To::Refused(why) => {
                self.bar.set_status(why, Tone::Error);
                None
            }
        }
    }

    /// The action a view's key asks for, once marked busy where it is shown and on the bar.
    fn run(&mut self, action: Action) -> Option<Action> {
        match action {
            Action::Credentials(order) => {
                self.data.credentials.busy = Some(order.clone());
                Some(Action::Credentials(order))
            }
            Action::Answer(answer) => {
                self.data.held.busy = Some(answer.clone());
                Some(Action::Answer(answer))
            }
            Action::Run(Job::Open(listed), text) => {
                self.bar.set_status(format!("opening {}…", listed.attachment.name), Tone::Busy);
                Some(Action::Run(Job::Open(listed), text))
            }
            Action::Run(Job::Withdraw { edit: Some(_), .. }, _) if !self.bar.is_empty() => {
                self.bar.set_status("the bar holds a draft: send or clear it before editing a message", Tone::Error);
                None
            }
            Action::KeepChannelTab(channel, tab) => {
                channel::keep_tab(&mut self.ui.channel_tabs, channel.clone(), tab);
                Some(Action::KeepChannelTab(channel, tab))
            }
            Action::Run(Job::ChannelNotify { channel, level }, _) => {
                self.bar.set_status("running…", Tone::Busy);
                Some(self.set_level(channel, level))
            }
            action @ Action::Run(..) => {
                self.bar.set_status("running…", Tone::Busy);
                Some(action)
            }
            action => Some(action),
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
        Page::Ant(page) => page.write(Key::Char(c)).is_some(),
        Page::Thread(page) => page.finding() && page.find_key(Key::Char(c)),
        _ => false,
    })
}
