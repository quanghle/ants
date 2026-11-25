//! What waits over the view for keys: a prompt in the footer ([`Prompt`]), asking for `y` or for what to type, and
//! the overlays over the screen ([`Overlay`]), the latest on top. Which of them takes a key, shows in the footer, is
//! drawn over which and holds back an error is decided here.

use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::text::Line;

use super::{Action, Data, Key, Model};
use crate::tui::approvals::{self, Asking};
use crate::tui::help::Help;
use crate::tui::react::{self, Pick, Picker};
use crate::tui::{assistant, catchup, clipboard, dead, interrupt, notices, snooze, trail};

/// A prompt in the footer taking the keys, at most one at a time.
#[derive(Debug)]
pub enum Prompt {
    /// The reaction being picked.
    React(Picker),
    /// The cancel waiting for y while an ant runs a tool call.
    Interrupt(interrupt::Confirm),
    /// The dead letter `/dead drop` picked, waiting for y.
    Drop(dead::Letter),
    /// `/assistant disable` without --retire, waiting for y to retire the dedicated assistant too or n not to.
    Disable(assistant::Disabling),
    /// The run of a trail `/trail run` planned, waiting for y to start, its summary in a box over the view.
    Start(trail::Planned),
    /// How long to snooze a thread from home, or when to be reminded of it, being asked.
    Snooze(snooze::Prompt),
}

impl Prompt {
    /// The footer asking.
    pub fn line(&self, data: &Data) -> Line<'static> {
        match self {
            Self::React(picker) => picker.line(),
            Self::Interrupt(confirm) => confirm.line(),
            Self::Drop(letter) => letter.line(),
            Self::Disable(disabling) => disabling.line(),
            Self::Start(planned) => planned.line(),
            Self::Snooze(prompt) => prompt.line(data),
        }
    }

    /// Whether it covers the view with a box taking the keys alone, rather than only taking the footer.
    pub fn covers(&self) -> bool {
        matches!(self, Self::Start(_))
    }

    fn draw(&self, frame: &mut Frame) {
        if let Self::Start(planned) = self {
            planned.draw(frame);
        }
    }

    /// Take `key`: what is done, and the prompt if it still asks.
    fn key(self, key: Key, model: &mut Model, now: DateTime<Utc>) -> (Option<Self>, Option<Action>) {
        match self {
            Self::React(mut picker) => match picker.key(key) {
                Pick::Typing => (Some(Self::React(picker)), None),
                Pick::Close => (None, None),
                Pick::React(message, reaction) => {
                    let reacted = model.data.reactions.get(&message).map_or(&[][..], Vec::as_slice);
                    let remove = react::reacted(reacted, model.data.me(), &reaction);
                    (None, Some(Action::React(message, reaction, remove)))
                }
            },
            Self::Interrupt(confirm) => (None, confirm.key(key, model)),
            Self::Drop(letter) => (None, letter.key(key, model)),
            Self::Disable(disabling) => (None, disabling.key(key, model)),
            Self::Start(planned) => {
                let (planned, action) = planned.key(key, model);
                (planned.map(Self::Start), action)
            }
            Self::Snooze(prompt) => {
                let (prompt, action) = prompt.key(key, model, now);
                (prompt.map(Self::Snooze), action)
            }
        }
    }
}

/// What shows over the whole screen, taking the keys while on top.
#[derive(Debug)]
pub enum Overlay {
    Help(Help),
    /// The notices of the run, `N`.
    Log(notices::Log),
    /// The parts of a message to copy.
    Copy(clipboard::Picker),
    /// The tool request being answered, while it shows in full.
    Request(Box<Asking>),
    /// The catch-up digest, scrolled as [`Ui::digest`](super::Ui::digest) has it.
    Digest,
}

impl Overlay {
    fn draw(&self, frame: &mut Frame, model: &Model, now: DateTime<Utc>) {
        match self {
            Self::Help(help) => help.draw(frame, model, now),
            Self::Log(log) => log.draw(frame, &model.ui.notices),
            Self::Copy(picker) => picker.draw(frame, &model.ui.mouse),
            Self::Request(asking) => approvals::draw_full(frame, asking, now),
            Self::Digest => catchup::draw_digest(frame, model, now),
        }
    }
}

impl Model {
    /// The help, while shown.
    #[cfg(test)]
    pub fn help(&self) -> Option<&Help> {
        self.overlays.iter().find_map(|o| if let Overlay::Help(help) = o { Some(help) } else { None })
    }

    /// The parts of a message to copy, while being picked.
    pub fn copying(&mut self) -> Option<&mut clipboard::Picker> {
        self.overlays.iter_mut().find_map(|o| if let Overlay::Copy(picker) = o { Some(picker) } else { None })
    }

    /// Show `overlay` over the rest.
    pub fn overlay(&mut self, overlay: Overlay) {
        self.overlays.push(overlay);
    }

    /// The tool request being answered, shown in full or its decision being stored.
    pub fn asking(&self) -> Option<&Asking> {
        let full =
            self.overlays.iter().find_map(|o| if let Overlay::Request(asking) = o { Some(&**asking) } else { None });
        full.or(self.storing.as_ref())
    }

    pub(in crate::tui) fn asking_mut(&mut self) -> Option<&mut Asking> {
        let full = self
            .overlays
            .iter_mut()
            .find_map(|o| if let Overlay::Request(asking) = o { Some(&mut **asking) } else { None });
        full.or(self.storing.as_mut())
    }

    /// Answer the request `asking` holds, in place of the one being answered if any: over the screen while it shows in
    /// full.
    pub(in crate::tui) fn ask(&mut self, asking: Asking) {
        self.unask();
        if asking.full {
            self.overlays.push(Overlay::Request(Box::new(asking)));
        } else {
            self.storing = Some(asking);
        }
    }

    /// End the answering: the request that was being answered, if any.
    pub(in crate::tui) fn unask(&mut self) -> Option<Asking> {
        let full = self.overlays.iter().position(|o| matches!(o, Overlay::Request(_)));
        if let Some(Overlay::Request(asking)) = full.map(|at| self.overlays.remove(at)) {
            return Some(*asking);
        }
        self.storing.take()
    }

    /// Whether a box over the view takes the keys: an overlay, or a prompt covering the view.
    pub(in crate::tui) fn covered(&self) -> bool {
        !self.overlays.is_empty() || self.prompt.as_ref().is_some_and(Prompt::covers)
    }

    /// Take `key` in the overlay on top, if any.
    pub(super) fn overlay_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Option<Action>> {
        let (open, action) = match self.overlays.last_mut()? {
            Overlay::Request(_) => return Some(self.answer(key, now)),
            Overlay::Digest => return Some(self.digest_key(key)),
            Overlay::Help(help) => (help.key(key), None),
            Overlay::Log(log) => (log.key(key), None),
            Overlay::Copy(_) if matches!(key, Key::Esc | Key::Char('q')) => (false, None),
            Overlay::Copy(picker) => {
                let action = picker.key(key);
                (action.is_none(), action)
            }
        };
        if !open {
            self.overlays.pop();
        }
        Some(action)
    }

    /// Take `key` in the prompt, if one asks.
    pub(super) fn prompt_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Option<Action>> {
        let prompt = self.prompt.take()?;
        let (prompt, action) = prompt.key(key, self, now);
        if prompt.is_some() {
            self.prompt = prompt;
        }
        Some(action)
    }

    /// Close the overlay on top, a tool request in full as Esc closes it: whether one was open.
    pub(super) fn close_overlay(&mut self, now: DateTime<Utc>) -> bool {
        match self.overlays.last() {
            None => return false,
            Some(Overlay::Request(_)) => _ = self.answer(Key::Esc, now),
            Some(_) => _ = self.overlays.pop(),
        }
        true
    }

    /// Whether text pasted is dropped: an overlay shows, or a prompt only answered by keys asks.
    pub(super) fn drops_paste(&self) -> bool {
        !self.overlays.is_empty()
            || matches!(
                self.prompt,
                Some(Prompt::Interrupt(_) | Prompt::Drop(_) | Prompt::Disable(_) | Prompt::Start(_))
            )
    }

    /// Take `line` pasted in the prompt, if one asks for text: whether it took it.
    pub(super) fn prompt_paste(&mut self, line: &str, now: DateTime<Utc>) -> bool {
        match &mut self.prompt {
            Some(Prompt::Snooze(_)) => line.chars().for_each(|c| _ = self.prompt_key(Key::Char(c), now)),
            Some(Prompt::React(picker)) => line.chars().for_each(|c| _ = picker.key(Key::Char(c))),
            _ => return false,
        }
        true
    }

    /// The footer of the prompt asking, if one does.
    pub(in crate::tui) fn prompt_line(&self) -> Option<Line<'static>> {
        self.prompt.as_ref().map(|prompt| prompt.line(&self.data))
    }

    /// Draw a prompt's box, then the overlays, the latest on top, each over the whole screen as the mouse sees it.
    pub(in crate::tui) fn draw_over(&self, frame: &mut Frame, now: DateTime<Utc>) {
        if let Some(prompt) = self.prompt.as_ref().filter(|p| p.covers()) {
            self.ui.mouse.add(frame.area(), crate::tui::mouse::Target::Overlay);
            prompt.draw(frame);
        }
        for overlay in &self.overlays {
            self.ui.mouse.add(frame.area(), crate::tui::mouse::Target::Overlay);
            overlay.draw(frame, self, now);
        }
    }
}
