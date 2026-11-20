//! Replaying and dropping dead letters from the command bar, as `ants dead replay` and `ants dead drop` do: `/dead
//! replay|drop [SEQ]` acts on the dead letter at SEQ, else on the one selected in a thread (its `gave up` line, or the
//! daemon's notice of giving up on the message), else on the latest of the thread open or selected, such as home's
//! stuck item; a drop waits for `y`. A dead letter replayed or dropped, here or before the read, and the notice of it,
//! leave the thread stuck no more.

use ants_proto::NoticeKind;
use ratatui::style::{Color, Style};
use ratatui::text::Line;

use super::command::{Dead, Tone};
use super::exec::Job;
use super::model::{Action, Key, Model, Page, Prompt};
use crate::text::excerpt;
use crate::threads::thread::{Entry, Kind};

/// A dead letter `/dead` acts on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Letter {
    /// Its stream sequence, as `ants dead list` lists it.
    pub seq: u64,
    /// The text of the message given up on, when read.
    pub text: Option<String>,
}

impl Letter {
    fn of(entry: &Entry) -> Self {
        Self { seq: entry.seq, text: Some(entry.text.clone()) }
    }

    /// The footer asking for `y` to drop it.
    pub fn line(&self) -> Line<'static> {
        let letter = match &self.text {
            Some(text) => format!("dead letter {} (\"{}\")", self.seq, excerpt(text, 40)),
            None => format!("dead letter {}", self.seq),
        };
        let text = format!(" {letter}: y drops it unreplayed; any other key keeps it");
        Line::styled(text, Style::new().fg(Color::Yellow))
    }

    /// Take `key`: `y` drops it, another key keeps it.
    pub fn key(self, key: Key, model: &mut Model) -> Option<Action> {
        if key != Key::Char('y') {
            return None;
        }
        model.bar.set_status(format!("dropping dead letter {}…", self.seq), Tone::Busy);
        Some(Action::Run(Job::DeadDrop(self.seq), String::new()))
    }
}

impl Model {
    /// The dead letter `/dead` acts on: the one at `seq`, else the one selected in the thread view, else the latest of
    /// the thread acted on ([`Model::target`]) not replayed or dropped here.
    pub(super) fn letter(&self, seq: Option<u64>) -> Result<Letter, String> {
        let (entries, chosen) = match self.page() {
            Page::Thread(page) => (&page.history.entries[..], page.chosen()),
            _ => (&[][..], None),
        };
        let dead = |e: &&Entry| e.kind == Kind::Dead;
        let picked = match (seq, chosen) {
            (Some(seq), _) => {
                Some(entries.iter().filter(dead).find(|e| e.seq == seq).map_or(Letter { seq, text: None }, Letter::of))
            }
            (None, Some(e)) if e.kind == Kind::Dead => Some(Letter::of(e)),
            // The notice of giving up on a message follows its dead letter.
            (None, Some(e)) if e.notice == Some(NoticeKind::GaveUp) && e.in_reply_to.is_some() => {
                entries.iter().rev().filter(dead).find(|d| d.seq < e.seq).map(Letter::of)
            }
            _ => None,
        };
        let cleared = &self.data.dead_cleared;
        if let Some(letter) = picked {
            if cleared.contains(&letter.seq) {
                return Err(format!("dead letter {} is replayed or dropped already", letter.seq));
            }
            return Ok(letter);
        }
        let thread = self.target()?;
        let read = entries.iter().rev().filter(dead).find(|e| !e.forged && !cleared.contains(&e.seq)).map(Letter::of);
        read.or_else(|| self.data.dead_letter(&thread).map(|a| Letter { seq: a.seq, text: Some(a.text.clone()) }))
            .ok_or_else(|| format!("no dead letter on thread {thread}"))
    }

    /// Do what `/dead` asks: replay the dead letter at once, or ask for `y` to drop it.
    pub(super) fn dead(&mut self, dead: Dead) -> Result<Option<Action>, String> {
        match dead {
            Dead::Replay(seq) => Ok(Some(Action::Run(Job::DeadReplay(self.letter(seq)?.seq), String::new()))),
            Dead::Drop(seq) => {
                self.prompt = Some(Prompt::Drop(self.letter(seq)?));
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests;
