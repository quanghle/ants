//! How what the bar ran went: the outcome of a job on the status line, in the bar and in the view, of a decision on a
//! tool request, and of the text edited in the editor.

use super::{Draft, Note, Tone};
use crate::tui::exec::Outcome;
use crate::tui::model::{Action, Model, Page, Prompt};
use crate::tui::thread;

impl Model {
    /// Take how a job the bar ran went: its line and warnings on the status line, opening the thread it started, the
    /// text and files of a message withdrawn to edit into the bar, the run of a trail planned to confirm, the dead
    /// letter it replayed or dropped, and what it shows in the pager; on failure, the error, and its text and quote
    /// back as the draft of the conversation it was typed in if that holds none.
    pub fn outcome(&mut self, outcome: Outcome) -> Option<Action> {
        self.follow_drafts();
        self.ui.bar.staged.sent(&outcome.text, outcome.result.is_err());
        let note = self.ui.bar.note_sent(&outcome.text);
        let typed = self.ui.drafts.sent(&outcome.text).filter(|typed| typed != self.ui.drafts.here());
        match outcome.result {
            Ok(mut done) => {
                self.data.withdrawn.extend(done.withdrew);
                self.data.dead_cleared.extend(done.dead);
                if let Some((text, files)) = done.edit.take() {
                    self.ui.bar.set_text(&text);
                    for path in files {
                        if let Err(e) = self.ui.bar.staged.add(&path.to_string_lossy()) {
                            done.warnings.push(format!("{e:#}"));
                        }
                    }
                    self.ui.bar.focused = true;
                }
                let tone = if done.warnings.is_empty() { Tone::Done } else { Tone::Warn };
                let text = std::iter::once(done.line).chain(done.warnings).collect::<Vec<_>>().join(" · ");
                self.ui.bar.set_status(text, tone);
                if let Some(planned) = done.planned {
                    self.prompt = Some(Prompt::Start(*planned));
                }
                if let Some(page) = done.page {
                    return Some(Action::Page(page));
                }
                let thread = done.open?;
                if matches!(self.page(), Page::Thread(page) if page.history.thread == thread) {
                    return None;
                }
                self.push(Page::Thread(thread::Page::new(thread.clone())));
                Some(Action::Open(thread))
            }
            Err(e) => {
                self.ui.bar.set_status(format!("{e:#}"), Tone::Error);
                if let Some(typed) = typed
                    && !outcome.text.is_empty()
                {
                    let draft = Draft::of(&outcome.text, outcome.quoting, note.map(Note::Reply));
                    self.ui.drafts.keep(typed, draft);
                } else if self.ui.bar.is_empty() && !outcome.text.is_empty() {
                    self.ui.bar.set_text(&outcome.text);
                    self.ui.bar.quoting = outcome.quoting;
                    self.ui.bar.noting = note.map(Note::Reply);
                    self.ui.bar.focused = true;
                }
                None
            }
        }
    }

    /// Take the outcome of a decision on a tool request.
    pub fn decided(&mut self, decided: anyhow::Result<String>) {
        match decided {
            Ok(line) => self.ui.bar.set_status(line, Tone::Done),
            Err(e) => self.ui.bar.set_status(format!("{e:#}"), Tone::Error),
        }
    }

    /// Take the text edited in the editor, or why editing failed.
    pub fn edited(&mut self, edited: anyhow::Result<String>) {
        match edited {
            Ok(text) => {
                self.ui.bar.set_text(&text);
                self.ui.bar.focused = true;
            }
            Err(e) => self.ui.bar.set_status(format!("{e:#}"), Tone::Error),
        }
    }
}
