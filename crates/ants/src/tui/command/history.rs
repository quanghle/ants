//! The history of the bar: the texts sent from it, latest first, which Ctrl-P and Ctrl-N recall and Ctrl-R searches,
//! and those Esc twice emptied it of. The latest [`KEPT`] are kept on the device in [`FILE`] beside
//! `chat-state.json`, readable by its owner only, with the drafts of the conversations; a text or draft addressed to
//! another human, which goes sealed, or a slash command acting on a thread where it would, is kept for the run only.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use ants_proto::ThreadId;
use anyhow::Result;
use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};

use super::{Conversation, Draft, Tone};
use crate::tui::model::{Data, Key, Model};

/// How many texts the history keeps.
pub const KEPT: usize = 200;
/// The file beside `chat-state.json` keeping the history and the drafts.
pub const FILE: &str = "chat-bar.json";
/// How soon a second Esc empties the bar the first one left.
const ESC_AGAIN: TimeDelta = TimeDelta::seconds(1);

#[derive(Debug, Default)]
pub struct History {
    /// The texts, latest first.
    texts: VecDeque<Text>,
    /// Which text Ctrl-P and Ctrl-N went to, and the bar's text before.
    browsing: Option<(usize, String)>,
    searching: Option<Search>,
    /// When an Esc left the bar holding text.
    escaped: Option<DateTime<Utc>>,
    /// Where the history and the drafts are kept, if anywhere.
    file: Option<PathBuf>,
    /// Whether the history changed since it was last kept.
    unsaved: bool,
}

#[derive(Debug)]
struct Text {
    text: String,
    /// Addressed to another human: never written to disk.
    private: bool,
}

/// A reverse incremental search of the history (Ctrl-R).
#[derive(Debug)]
struct Search {
    query: String,
    /// The text found, if any.
    at: Option<usize>,
    /// The bar's text before.
    before: String,
}

/// What [`FILE`] keeps.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Kept {
    /// The texts, latest first.
    #[serde(default)]
    sent: Vec<String>,
    #[serde(default)]
    drafts: Vec<KeptDraft>,
}

#[derive(Debug, Serialize, Deserialize)]
struct KeptDraft {
    conversation: Conversation,
    text: String,
}

impl History {
    /// Add `text` as the latest, `private` when it is addressed to another human.
    pub(super) fn push(&mut self, text: &str, private: bool) {
        if text.trim().is_empty() {
            return;
        }
        self.texts.retain(|kept| kept.text != text);
        self.texts.push_front(Text { text: text.to_owned(), private });
        self.texts.truncate(KEPT);
        self.unsaved = true;
    }

    /// The index of the latest text from `from` on holding `query`, whatever its case.
    fn find(&self, query: &str, from: usize) -> Option<usize> {
        let query = query.to_lowercase();
        (from..self.texts.len()).find(|at| self.texts[*at].text.to_lowercase().contains(&query))
    }

    /// What the bar's title says while searching.
    pub fn searching(&self) -> Option<String> {
        let search = self.searching.as_ref()?;
        let found = if search.at.is_some() || search.query.is_empty() { "" } else { ": no match" };
        Some(format!("search sent \"{}\"{found}", search.query))
    }

    /// Whether the history changed since it was last kept.
    pub fn unsaved(&self) -> bool {
        self.unsaved
    }
}

impl Data {
    /// Whether `text` typed in `conversation` is addressed to another human, and so goes sealed: in the DMs with a
    /// person, on a thread not in a channel where another human is (or one not read yet), or mentioning one.
    pub fn private(&self, conversation: &Conversation, text: &str) -> bool {
        let other = |user: &ants_proto::UserId| self.is_human(user) && user != self.me();
        let mentioned = text.split('@').skip(1).any(|after| {
            let name: String =
                after.chars().take_while(|c| c.is_alphanumeric() || matches!(c, '-' | '_' | '.')).collect();
            name.trim_end_matches('.').parse().is_ok_and(|user| other(&user))
        });
        mentioned
            || match conversation {
                Conversation::Person(_) => true,
                Conversation::Thread(thread) => self.live.summary(thread).is_none_or(|summary| {
                    summary.channels.is_empty() && (summary.sealed || summary.users.iter().any(other))
                }),
                Conversation::Channel(_) | Conversation::None => false,
            }
    }
}

/// The slash commands acting on no thread.
const THREADLESS: &[&str] =
    &["goto", "search", "digest", "attach", "detach", "add", "clone", "retire", "quit", "notify", "reminders", "trail"];

impl Model {
    /// Whether `text` typed in `conversation` is kept for the run only: addressed to another human ([`Data::private`]),
    /// or a slash command acting on a thread where a text would be: the one open or selected for the conversation
    /// shown, the conversation's own for another, and any thread when not known.
    pub(super) fn sealed(&self, conversation: &Conversation, text: &str) -> bool {
        let data = &self.data;
        if data.private(conversation, text) {
            return true;
        }
        let Some(command) = text.trim_start().strip_prefix('/').filter(|c| !c.starts_with('/')) else { return false };
        let name = command.split_whitespace().next().unwrap_or_default().to_ascii_lowercase();
        if THREADLESS.contains(&name.as_str()) {
            return false;
        }
        let target: Option<ThreadId> = match conversation {
            Conversation::Thread(thread) => Some(thread.clone()),
            _ if conversation == self.ui.drafts.here() => self.target().ok(),
            _ => None,
        };
        target.is_none_or(|thread| data.private(&Conversation::Thread(thread), text))
    }

    /// Add `text`, typed in the conversation shown, to the bar's history as the latest; kept for the run only when
    /// [`Self::sealed`].
    pub fn push_history(&mut self, text: &str) {
        let private = self.sealed(self.ui.drafts.here(), text);
        self.ui.history.push(text, private);
    }

    /// Empty the bar into its history, and so the draft of the conversation shown: what Esc twice and Ctrl-C do to a
    /// bar holding text.
    pub fn empty_into_history(&mut self) {
        let text = self.ui.bar.text();
        self.push_history(&text);
        self.ui.bar.clear();
        self.ui.history.browsing = None;
        self.follow_drafts();
        if !text.trim().is_empty() {
            self.ui.bar.set_status("kept in the history: Ctrl-P recalls the text", Tone::Done);
        }
    }

    /// Note that the bar's `key` at `now` left it holding text, so that another Esc soon empties it.
    pub(super) fn escaped(&mut self, key: Key, now: DateTime<Utc>) {
        if key == Key::Esc && !self.ui.bar.focused && !self.ui.bar.is_empty() {
            self.ui.history.escaped = Some(now);
        }
    }

    /// Take `key` at `now` when it is the second Esc on a bar holding text, emptying it into its history.
    pub fn esc_again(&mut self, key: Key, now: DateTime<Utc>) -> bool {
        let escaped = self.ui.history.escaped.take();
        if key != Key::Esc || self.ui.bar.is_empty() || !escaped.is_some_and(|at| now - at <= ESC_AGAIN) {
            return false;
        }
        self.empty_into_history();
        true
    }

    /// Take `key` for the history of the focused bar: Ctrl-P and Ctrl-N recall the texts sent, Ctrl-R searches them,
    /// and a search takes the keys until it ends. Returns false when the key is not the history's.
    pub(super) fn history_key(&mut self, key: Key) -> bool {
        if self.ui.history.searching.is_some() {
            return self.search_key(key);
        }
        let history = &mut self.ui.history;
        match key {
            Key::Ctrl('p') => {
                let at = history.browsing.as_ref().map_or(0, |(at, _)| at + 1);
                let Some(text) = history.texts.get(at).map(|kept| kept.text.clone()) else { return true };
                let before = history.browsing.take().map_or_else(|| self.ui.bar.text(), |(_, before)| before);
                history.browsing = Some((at, before));
                self.ui.bar.set_text(&text);
            }
            Key::Ctrl('n') => match history.browsing.take() {
                Some((0, before)) => self.ui.bar.set_text(&before),
                Some((at, before)) => {
                    let text = history.texts.get(at - 1).map_or_else(String::new, |kept| kept.text.clone());
                    history.browsing = Some((at - 1, before));
                    self.ui.bar.set_text(&text);
                }
                None => {}
            },
            Key::Ctrl('r') => {
                history.browsing = None;
                history.searching = Some(Search { query: String::new(), at: None, before: self.ui.bar.text() });
            }
            _ => {
                history.browsing = None;
                return false;
            }
        }
        true
    }

    /// Take `key` for the search running: typed text narrows it, Ctrl-R goes to an earlier text, Enter takes the
    /// text found and Esc or Ctrl-G puts back the text before; any other key takes the text found and goes on.
    fn search_key(&mut self, key: Key) -> bool {
        let history = &mut self.ui.history;
        let Some(Search { query, at, before }) = history.searching.take() else { return false };
        let (query, from) = match key {
            Key::Char(c) => (format!("{query}{c}"), at.unwrap_or(0)),
            Key::Backspace => {
                let mut query = query;
                query.pop();
                (query, 0)
            }
            Key::Ctrl('r') => (query, at.map_or(0, |at| at + 1)),
            Key::Esc | Key::Ctrl('g') => {
                self.ui.bar.set_text(&before);
                return true;
            }
            Key::Enter => return true,
            _ => return false,
        };
        let found =
            if query.is_empty() { None } else { history.find(&query, from).or(at.filter(|_| key == Key::Ctrl('r'))) };
        let text = match found {
            Some(at) => Some(history.texts[at].text.clone()),
            None => query.is_empty().then(|| before.clone()),
        };
        history.searching = Some(Search { query, at: found, before });
        if let Some(text) = text {
            self.ui.bar.set_text(&text);
        }
        true
    }

    /// Take the history and the drafts kept in the file at `path`, and keep them there from now on.
    pub fn load_history(&mut self, path: Option<PathBuf>) {
        let Some(path) = path else { return };
        let kept: Kept = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|e| {
                tracing::debug!("ignoring {}: {e}", path.display());
                Kept::default()
            }),
            Err(_) => Kept::default(),
        };
        let history = &mut self.ui.history;
        history.texts = kept.sent.into_iter().take(KEPT).map(|text| Text { text, private: false }).collect();
        history.file = Some(path);
        for KeptDraft { conversation, text } in kept.drafts {
            let draft = Draft::of(&text, None, None);
            if conversation == *self.ui.drafts.here() {
                if self.ui.bar.is_empty() {
                    self.ui.bar.swap(draft);
                }
            } else {
                self.ui.drafts.keep(conversation, draft);
            }
        }
        self.follow_drafts();
    }

    /// Keep the history and the drafts in the file they were taken from, leaving out those [`Self::sealed`].
    pub fn keep_history(&mut self) {
        let Some(path) = self.ui.history.file.clone() else { return };
        self.ui.history.unsaved = false;
        if let Err(e) = self.write_history(&path) {
            tracing::debug!("not keeping the bar's history: {e:#}");
        }
    }

    fn write_history(&self, path: &Path) -> Result<()> {
        let ui = &self.ui;
        let sent = ui.history.texts.iter().filter(|kept| !kept.private).map(|kept| kept.text.clone()).collect();
        let here = (ui.drafts.here(), self.ui.bar.text());
        let kept = ui.drafts.kept().map(|(conversation, draft)| (conversation, draft.text()));
        let drafts = std::iter::once(here)
            .chain(kept)
            .filter(|(conversation, text)| !text.trim().is_empty() && !self.sealed(conversation, text))
            .map(|(conversation, text)| KeptDraft { conversation: conversation.clone(), text })
            .collect();
        if let Some(dir) = path.parent().filter(|dir| !dir.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir)?;
        }
        crate::auth::replace_secret(path, &serde_json::to_string_pretty(&Kept { sent, drafts })?)
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::ThreadId;

    use super::*;
    use crate::test_support::live::{at, message, user};
    use crate::tui::model::Page;
    use crate::tui::test_support::{model, typed};
    use crate::tui::{dm, thread};

    fn sent(m: &mut Model, text: &str) {
        m.ui.bar.focused = true;
        typed(m, text);
        m.key(Key::Enter, at(1));
        assert!(m.ui.bar.is_empty(), "{text}: {:?}", m.ui.bar.status);
    }

    #[test]
    fn ctrl_p_and_ctrl_n_recall_the_texts_sent_latest_first() {
        let mut m = model();
        sent(&mut m, "@dev-a deploy");
        sent(&mut m, "/search deploy");
        sent(&mut m, "@dev-a deploy");
        m.ui.bar.focused = true;
        typed(&mut m, "half");
        m.key(Key::Ctrl('p'), at(1));
        assert_eq!(m.ui.bar.text(), "@dev-a deploy", "the latest, sent twice, once");
        m.key(Key::Ctrl('p'), at(1));
        assert_eq!(m.ui.bar.text(), "/search deploy");
        m.key(Key::Ctrl('p'), at(1));
        assert_eq!(m.ui.bar.text(), "/search deploy", "none earlier");
        m.key(Key::Ctrl('n'), at(1));
        assert_eq!(m.ui.bar.text(), "@dev-a deploy");
        m.key(Key::Ctrl('n'), at(1));
        assert_eq!(m.ui.bar.text(), "half", "back to the text typed");
    }

    #[test]
    fn ctrl_r_searches_the_texts_sent() {
        let mut m = model();
        for text in ["@dev-a deploy the site", "@dev-b review", "@dev-a deploy the docs"] {
            sent(&mut m, text);
        }
        m.ui.bar.focused = true;
        typed(&mut m, "draft");
        m.key(Key::Ctrl('r'), at(1));
        typed(&mut m, "DEPLOY");
        assert_eq!(m.ui.bar.text(), "@dev-a deploy the docs");
        assert_eq!(m.bar_title(), "search sent \"DEPLOY\"");
        m.key(Key::Ctrl('r'), at(1));
        assert_eq!(m.ui.bar.text(), "@dev-a deploy the site", "an earlier one");
        m.key(Key::Ctrl('r'), at(1));
        assert_eq!(m.ui.bar.text(), "@dev-a deploy the site", "none earlier");
        typed(&mut m, "x");
        assert_eq!(m.bar_title(), "search sent \"DEPLOYx\": no match");
        m.key(Key::Esc, at(1));
        assert_eq!((m.ui.bar.text().as_str(), m.ui.bar.focused), ("draft", true), "Esc puts the text back");
        m.key(Key::Ctrl('r'), at(1));
        typed(&mut m, "review");
        m.key(Key::Enter, at(1));
        assert_eq!(m.ui.bar.text(), "@dev-b review", "Enter takes it, sending nothing");
        assert_eq!(m.bar_title(), "to dev-b on a new thread");
    }

    #[test]
    fn esc_twice_empties_the_bar_into_the_history() {
        let mut m = model();
        m.ui.bar.focused = true;
        typed(&mut m, "@dev-a later maybe");
        m.key(Key::Esc, at(1));
        assert!(!m.ui.bar.focused && m.ui.bar.text() == "@dev-a later maybe", "one Esc leaves the bar");
        m.key(Key::Esc, at(1));
        assert!(m.ui.bar.is_empty());
        m.ui.bar.focused = true;
        m.key(Key::Ctrl('p'), at(1));
        assert_eq!(m.ui.bar.text(), "@dev-a later maybe");
        m.key(Key::Esc, at(1));
        m.key(Key::Esc, at(3));
        assert_eq!(m.ui.bar.text(), "@dev-a later maybe", "too late for a second Esc");
    }

    #[test]
    fn ctrl_c_empties_the_bar_into_the_history() {
        let mut m = model();
        m.ui.bar.focused = true;
        typed(&mut m, "@dev-a later maybe");
        m.key(Key::Interrupt, at(1));
        assert!(m.ui.bar.is_empty(), "Ctrl-C empties the bar");
        assert!(!m.ui.drafts.holds(m.ui.drafts.here()), "and so the draft");
        m.ui.bar.focused = true;
        m.key(Key::Ctrl('p'), at(2));
        assert_eq!(m.ui.bar.text(), "@dev-a later maybe", "Ctrl-P recalls it");
    }

    #[test]
    fn the_history_and_the_drafts_outlive_the_run_but_not_what_is_addressed_to_a_human() {
        let dir = ants_test_support::scratch_dir("bar-history");
        let path = dir.join(FILE);
        let (ours, theirs) = (ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        m.load_history(Some(path.clone()));
        m.stored(
            vec![message(1, &ours, "alice", "dev-a", "deploy"), message(2, &theirs, "alice", "bob", "lunch")],
            at(1),
        );
        sent(&mut m, "@dev-a deploy the site");
        sent(&mut m, "@bob lunch at noon?");
        m.pages.push(Page::Thread(thread::Page::new(ours.clone())));
        typed(&mut m, "ship it");
        m.pages.push(Page::Thread(thread::Page::new(theirs)));
        m.follow_drafts();
        typed(&mut m, "the usual place");
        m.pages.push(Page::Dm(dm::Page::new(user("bob"))));
        m.follow_drafts();
        typed(&mut m, "see you there");
        m.pages.truncate(1);
        m.follow_drafts();
        typed(&mut m, "who owns billing?");
        m.keep_history();
        let kept = std::fs::read_to_string(&path).unwrap();
        for private in ["lunch", "usual", "see you"] {
            assert!(!kept.contains(private), "{private} is in {kept}");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        }

        let mut m = model();
        m.load_history(Some(path));
        assert_eq!(m.ui.bar.text(), "who owns billing?", "home's draft");
        m.ui.bar.focused = true;
        m.ui.bar.set_text("");
        m.key(Key::Ctrl('p'), at(1));
        assert_eq!(m.ui.bar.text(), "@dev-a deploy the site");
        m.pages.push(Page::Thread(thread::Page::new(ours)));
        m.follow_drafts();
        assert_eq!(m.ui.bar.text(), "ship it", "the thread's draft is in its bar");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_command_for_a_sealed_thread_selected_on_home_stays_out_of_the_file() {
        let dir = ants_test_support::scratch_dir("bar-sealed-command");
        let path = dir.join(FILE);
        let theirs = ThreadId::generate();
        let mut m = model();
        m.load_history(Some(path.clone()));
        m.stored(vec![message(1, &theirs, "bob", "alice", "lunch?")], at(1));
        assert_eq!(m.target(), Ok(theirs), "home selects the thread between two humans");
        sent(&mut m, "/title Bob surprise party");
        sent(&mut m, "/search budget");
        m.ui.bar.focused = true;
        typed(&mut m, "/title Bob cake");
        m.key(Key::Esc, at(1));
        m.key(Key::Esc, at(1));
        assert!(m.ui.bar.is_empty(), "Esc twice empties the bar into the history");
        m.ui.bar.focused = true;
        typed(&mut m, "/title Bob balloons");
        m.keep_history();
        let kept = std::fs::read_to_string(&path).unwrap();
        for private in ["surprise", "cake", "balloons"] {
            assert!(!kept.contains(private), "{private} is in {kept}");
        }
        assert!(kept.contains("/search budget"), "a command acting on no thread is kept: {kept}");
        m.ui.bar.set_text("");
        m.key(Key::Ctrl('p'), at(1));
        assert_eq!(m.ui.bar.text(), "/title Bob cake", "kept for the run");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
