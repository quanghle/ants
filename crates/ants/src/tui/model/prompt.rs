//! What waits over the view for keys: a prompt in the footer ([`Prompt`]), asking for `y` or for what to type, and
//! the overlays over the screen ([`Overlay`]), the latest on top. Which of them takes a key, shows in the footer, is
//! drawn over which and holds back an error is decided here.

use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::text::Line;

use super::{Action, Data, Key, Model};
use crate::tui::help::Help;
use crate::tui::react::{self, Pick, Picker};
use crate::tui::{clipboard, dead, interrupt, notices, snooze, trail};

/// A prompt in the footer taking the keys, at most one at a time.
#[derive(Debug)]
pub enum Prompt {
    /// The reaction being picked.
    React(Picker),
    /// The cancel waiting for y while an ant runs a tool call.
    Interrupt(interrupt::Confirm),
    /// The dead letter `/dead drop` picked, waiting for y.
    Drop(dead::Letter),
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
}

impl Overlay {
    /// Take `key`: what is done, and whether the overlay stays open.
    fn key(&mut self, key: Key) -> (bool, Option<Action>) {
        match self {
            Self::Help(help) => (help.key(key), None),
            Self::Log(log) => (log.key(key), None),
            Self::Copy(_) if matches!(key, Key::Esc | Key::Char('q')) => (false, None),
            Self::Copy(picker) => {
                let action = picker.key(key);
                (action.is_none(), action)
            }
        }
    }

    fn draw(&self, frame: &mut Frame, model: &Model, now: DateTime<Utc>) {
        match self {
            Self::Help(help) => help.draw(frame, model, now),
            Self::Log(log) => log.draw(frame, &model.notices),
            Self::Copy(picker) => picker.draw(frame, &model.ui.mouse),
        }
    }
}

impl Model {
    /// The help, while shown.
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

    /// Whether a box over the view takes the keys: an overlay, or a prompt covering the view.
    pub(in crate::tui) fn covered(&self) -> bool {
        !self.overlays.is_empty() || self.prompt.as_ref().is_some_and(Prompt::covers)
    }

    /// Take `key` in the overlay on top, if any.
    pub(super) fn overlay_key(&mut self, key: Key) -> Option<Option<Action>> {
        let top = self.overlays.last_mut()?;
        let (open, action) = top.key(key);
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

    /// Close the overlay on top: whether one was open.
    pub(super) fn close_overlay(&mut self) -> bool {
        self.overlays.pop().is_some()
    }

    /// Whether text pasted is dropped: an overlay shows, or a prompt only answered by keys asks.
    pub(super) fn drops_paste(&self) -> bool {
        !self.overlays.is_empty()
            || matches!(self.prompt, Some(Prompt::Interrupt(_) | Prompt::Drop(_) | Prompt::Start(_)))
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
