//! The palette: `/` starting the bar's text, as `/` or Ctrl-K from a view puts it, lists what can be done in the view
//! on screen, the slash commands with their arguments ([`COMMANDS`]) and the view's keys as the key table has them
//! ([`keymap`]), then the places ([`places`]), narrowed by the text after the `/` up to the cursor; what cannot act here
//! shows dim with the reason. Up and Down pick, Enter runs the entry picked (a command needing an argument is completed
//! for it) or goes to the place, Tab completes a command and Esc closes the list.

mod places;

use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Style, Stylize as _};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use super::command::{COMMANDS, Tone};
use super::complete::{self, narrow};
use super::keymap;
use super::model::{Action, Key, Model};
use super::style;
pub use places::Place;

/// Most entries shown at once.
const ROWS: usize = 10;

/// The widest the entries' names show.
const NAME: usize = 16;

/// One entry of the palette.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Its name: a /command, or the keys as the help writes them.
    pub word: String,
    /// What it does: a command's arguments and purpose, a key's description.
    pub detail: String,
    /// Why it cannot act here, if it cannot.
    pub why: Option<String>,
    pub act: Act,
}

/// What Enter on an entry does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Act {
    /// Run the slash command when `run`, it needing no argument, else complete it for its arguments.
    Command { name: &'static str, run: bool },
    /// Leave the bar and press the key.
    Key(Key),
    /// Leave the bar and show the place.
    Go(Place),
}

impl Model {
    /// The palette's entries narrowed by the text after the `/` starting the focused bar, the command or place typed
    /// whole first, then those that can act here, a key matched only by its description last; none when the bar's text does not start with `/` up to the cursor or Esc closed the list.
    pub fn palette(&self, now: DateTime<Utc>) -> Vec<Entry> {
        let Some(typed) = self.palette_typed() else {
            return Vec::new();
        };
        let commands = COMMANDS.iter().map(|&(name, args, what)| {
            let run = args.is_empty() || args.starts_with('[');
            let entry = Entry {
                word: format!("/{name}"),
                detail: format!("{args:<24}{what}"),
                why: self.unfit(name, now),
                act: Act::Command { name, run },
            };
            (name.to_owned(), (entry, false))
        });
        if self.refused().is_some() {
            // A command refused lists those it may have meant, else all of them.
            let commands: Vec<_> = commands.collect();
            let meant = narrow(&typed, commands.clone());
            let listed = if meant.is_empty() { commands } else { meant };
            return listed.into_iter().map(|(_, (e, _))| e).collect();
        }
        let lower = typed.to_lowercase();
        let views = keymap::views(self);
        let keys = views.into_iter().flat_map(keymap::entries).chain(keymap::GLOBAL);
        let keys = keys.filter(|e| !e.keys.is_empty() && !e.keys.contains(&Key::Ctrl('k'))).map(|e| {
            let entry = Entry {
                word: e.label.to_owned(),
                detail: e.long.to_owned(),
                why: e.applies(self, now).err().map(str::to_owned),
                act: Act::Key(e.keys[0]),
            };
            let weak = !e.short.to_lowercase().contains(&lower);
            (format!("{} {}", e.short, e.long), (entry, weak))
        });
        let places = self.places(now).into_iter().map(|s| {
            let entry = Entry { word: s.word, detail: s.detail, why: None, act: Act::Go(s.place) };
            (s.name, (entry, false))
        });
        let mut entries = narrow(&typed, commands.chain(keys).chain(places));
        // A key matched by its description alone comes after the commands and places matched by name.
        entries.sort_by_key(|(name, (e, weak))| (!name.eq_ignore_ascii_case(&typed), e.why.is_some(), *weak));
        entries.into_iter().map(|(_, (e, _))| e).collect()
    }

    /// The text after the `/` starting the focused bar, up to the cursor, while the palette shows: the name of a
    /// command refused while the bar holds it unchanged.
    fn palette_typed(&self) -> Option<String> {
        if !self.ui.bar.focused || self.ui.bar.pick.closed || self.ui.bar.noting.is_some() {
            return None;
        }
        if let Some(name) = self.refused() {
            return Some(name);
        }
        let (start, typed) = self.ui.bar.word();
        if start != 0 {
            return None;
        }
        typed.strip_prefix('/').map(str::to_owned)
    }

    /// The name typed of the slash command refused, while the bar holds it unchanged.
    fn refused(&self) -> Option<String> {
        let refused = self.ui.bar.refused.as_ref().filter(|text| **text == self.ui.bar.raw())?;
        let name = refused.trim_start().trim_start_matches('/').split_whitespace().next();
        Some(name.unwrap_or_default().to_owned())
    }

    /// Take `key` for the palette when it shows: Up and Down pick, Enter runs the entry picked or completes the
    /// command, Tab completes the command or runs the key, and Esc closes the list. None when the key is not the
    /// palette's.
    pub(super) fn palette_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Option<Action>> {
        let mut entries = self.palette(now);
        if entries.is_empty() {
            return None;
        }
        let selected = self.ui.bar.pick.selected.min(entries.len() - 1);
        match key {
            Key::Up => self.ui.bar.pick.selected = selected.checked_sub(1).unwrap_or(entries.len() - 1),
            Key::Down => self.ui.bar.pick.selected = (selected + 1) % entries.len(),
            Key::Esc => self.close_palette(),
            Key::Tab | Key::Enter => return Some(self.pick(entries.swap_remove(selected), key == Key::Tab, now)),
            _ => return None,
        }
        Some(None)
    }

    /// Run `entry`, or complete it when `tab` and it is a command: a command run where it cannot act fails as typed
    /// would, a key says why.
    fn pick(&mut self, entry: Entry, tab: bool, now: DateTime<Utc>) -> Option<Action> {
        match entry.act {
            Act::Command { name, run: true } if !tab => {
                self.ui.bar.set_text(&format!("/{name}"));
                return self.submit(format!("/{name}"), now);
            }
            // The arguments of a command refused go with it.
            Act::Command { name, .. } if self.refused().is_some() => {
                self.ui.bar.set_text(&format!("/{name} "));
                self.ui.bar.pick = complete::Pick::default();
            }
            Act::Command { name, .. } => self.ui.bar.complete(&format!("/{name}")),
            Act::Key(_) if entry.why.is_some() => self.ui.bar.set_status(entry.why.unwrap_or_default(), Tone::Error),
            Act::Key(key) => {
                self.ui.bar.set_text("");
                self.ui.bar.focused = false;
                // A letter picked while writing acts as on the message selected, else the latest.
                if !matches!(key, Key::Char(_)) || !self.typing() {
                    return self.key(key, now);
                }
                self.ui.selecting = Some(self.conversation());
                let action = self.key(key, now);
                if self.ui.selecting.is_some() {
                    self.unselect(false);
                }
                return action;
            }
            Act::Go(place) => {
                self.ui.bar.set_text("");
                self.ui.bar.focused = false;
                return self.visit(place);
            }
        }
        None
    }

    /// Close the palette: a bar holding only the word typed for it is emptied and left, else (or for a command refused)
    /// the list closes.
    pub(super) fn close_palette(&mut self) {
        if self.ui.bar.raw().contains(char::is_whitespace) || self.refused().is_some() {
            self.ui.bar.pick.closed = true;
        } else {
            self.ui.bar.set_text("");
            self.ui.bar.focused = false;
        }
    }
}

/// Draw `entries` over the view just above the bar at `bar`, `selected` highlighted, those that cannot act dim with
/// the reason.
pub fn draw(frame: &mut Frame, bar: Rect, entries: &[Entry], selected: usize) {
    let selected = selected.min(entries.len().saturating_sub(1));
    let top = (selected + 1).saturating_sub(ROWS);
    let width = entries.iter().map(|e| e.word.chars().count()).max().unwrap_or(0).clamp(10, NAME);
    let outer = bar.width.min(100);
    let inner = usize::from(outer.saturating_sub(2));
    let lines: Vec<Line> = entries
        .iter()
        .enumerate()
        .skip(top)
        .take(ROWS)
        .map(|(at, e)| {
            let name = format!("{:<width$}  ", style::fit(&e.word, width));
            let line = match &e.why {
                None => Line::from(vec![
                    Span::styled(name, Style::new().bold()),
                    Span::styled(e.detail.clone(), style::dim()),
                ]),
                Some(why) => Line::from(vec![
                    Span::raw(name),
                    Span::raw(format!("({why}) ")).italic(),
                    Span::raw(e.detail.clone()),
                ])
                .style(style::dim()),
            };
            let line = style::clip(line, inner);
            if at == selected { line.reversed() } else { line }
        })
        .collect();
    let height = (u16::try_from(lines.len()).unwrap_or(u16::MAX) + 2).min(bar.y);
    let area = Rect { x: bar.x, y: bar.y.saturating_sub(height), width: outer, height };
    let title = if entries.len() > ROWS { format!(" {} of {} ", selected + 1, entries.len()) } else { String::new() };
    frame.render_widget(Clear, area);
    frame.render_widget(Paragraph::new(lines).block(style::block(title, true)), area);
}

#[cfg(test)]
mod tests {
    use ants_proto::ThreadId;

    use super::*;
    use crate::test_support::live::{at, message};
    use crate::threads::thread_meta::Marking;
    use crate::tui::exec::Job;
    use crate::tui::model::{Focus, Page};
    use crate::tui::test_support::{model, typed};
    use crate::tui::thread;

    fn on_thread() -> (Model, ThreadId) {
        let thread = ThreadId::generate();
        let mut m = model();
        m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy")], at(1));
        m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
        m.focus = Focus::View;
        (m, thread)
    }

    #[test]
    fn slash_then_a_few_letters_and_enter_runs_the_command_in_a_thread() {
        let (mut m, thread) = on_thread();
        m.key(Key::Char('/'), at(1));
        assert_eq!((m.ui.bar.focused, m.ui.bar.text()), (true, "/".to_owned()), "/ opens the palette");
        typed(&mut m, "arch");
        let words: Vec<String> = m.palette(at(1)).into_iter().map(|e| e.word).collect();
        assert_eq!(words[..2], ["/archive", "e"], "the command, then the key doing it: {words:?}");
        let archive = Job::Mark { thread, marking: Marking::Archive };
        assert_eq!(m.key(Key::Enter, at(1)), Some(Action::Run(archive, "/archive".into())));
        assert!(!m.ui.bar.focused && m.ui.bar.is_empty());
    }

    #[test]
    fn an_unknown_or_malformed_command_opens_the_palette_on_the_commands_it_may_mean() {
        let (mut m, thread) = on_thread();
        m.key(Key::Char('/'), at(1));
        typed(&mut m, "nope");
        assert!(m.palette(at(1)).is_empty());
        assert_eq!(m.key(Key::Enter, at(1)), None);
        assert_eq!(m.ui.bar.status.as_ref().unwrap().text, "no command /nope: the palette lists them");
        let words: Vec<String> = m.palette(at(1)).into_iter().map(|e| e.word).collect();
        assert_eq!(words.len(), COMMANDS.len(), "matching none, all the commands: {words:?}");
        let shown = crate::test_support::render::rendered(100, 20, |f| crate::tui::frame::draw(f, &m, at(1)));
        assert!(shown.contains(&format!("1 of {}", COMMANDS.len())), "{shown}");
        m.key(Key::Backspace, at(1));
        assert!(m.palette(at(1)).is_empty(), "an edit narrows as typed again");

        m.ui.bar.set_text("/done now");
        assert_eq!(m.key(Key::Enter, at(1)), None);
        assert_eq!(m.ui.bar.status.as_ref().unwrap().text, "usage: /done");
        let words: Vec<String> = m.palette(at(1)).into_iter().map(|e| e.word).collect();
        assert_eq!(words, ["/done", "/undone"]);
        let done = Job::Done { thread, done: true };
        assert_eq!(m.key(Key::Enter, at(1)), Some(Action::Run(done, "/done".into())), "Enter runs the one picked");
    }

    #[test]
    fn what_cannot_act_here_is_dim_with_the_reason_and_enter_on_a_key_says_it() {
        let mut m = model();
        m.focus = Focus::View;
        m.key(Key::Ctrl('k'), at(1));
        assert_eq!((m.ui.bar.focused, m.ui.bar.text()), (true, "/".to_owned()), "Ctrl-K opens the palette");
        let all = m.palette(at(1));
        assert!(all.iter().any(|e| e.word == "/goto") && all.iter().any(|e| e.word == "s"), "commands and keys");
        assert!(all.iter().all(|e| e.word != "Ctrl-K /"), "not the palette itself");
        let first_dim = all.iter().position(|e| e.why.is_some()).unwrap();
        assert!(all[first_dim..].iter().all(|e| e.why.is_some()), "what can act comes first");
        typed(&mut m, "archive");
        let archive = &m.palette(at(1))[0];
        assert_eq!((archive.word.as_str(), archive.why.as_deref()), ("/archive", Some("no thread open or selected")));
        let shown = crate::test_support::render::rendered(100, 20, |f| crate::tui::frame::draw(f, &m, at(1)));
        assert!(shown.contains("/archive    (no thread open or selected)"), "{shown}");

        m.ui.bar.set_text("/done");
        let done = m.palette(at(1));
        let key = done.iter().position(|e| e.word == "d").unwrap();
        for _ in 0..key {
            m.key(Key::Down, at(1));
        }
        assert_eq!(m.key(Key::Enter, at(1)), None);
        assert_eq!(m.ui.bar.status.as_ref().unwrap().text, "nothing selected", "a key that cannot act says why");
        assert_eq!(m.ui.bar.text(), "/done", "and the palette stays");
    }

    #[test]
    fn enter_on_a_key_presses_it_and_esc_leaves_the_palette() {
        let (mut m, _) = on_thread();
        m.key(Key::Ctrl('k'), at(1));
        typed(&mut m, "tools");
        let shown = m.palette(at(1));
        assert_eq!(shown[0].act, Act::Key(Key::Char('T')), "{shown:?}");
        assert!(matches!(m.key(Key::Enter, at(1)), Some(Action::KeepTools(_))));
        assert!(!m.ui.bar.focused && m.ui.bar.is_empty());

        m.key(Key::Char('/'), at(2));
        typed(&mut m, "mu");
        assert_eq!(m.key(Key::Tab, at(2)), None);
        assert_eq!(m.ui.bar.text(), "/mute ", "Tab completes a command");
        assert!(m.palette(at(2)).is_empty(), "the arguments typed, the palette goes");
        m.ui.bar.set_text("/titl");
        m.key(Key::Enter, at(2));
        assert_eq!(m.ui.bar.text(), "/title ", "Enter completes a command needing an argument");
        m.ui.bar.set_text("/");
        m.key(Key::Esc, at(2));
        assert!(!m.ui.bar.focused && m.ui.bar.is_empty(), "Esc empties and leaves the bar the palette opened");
        assert_eq!(m.key(Key::Ctrl('f'), at(2)), None);
        assert!(!m.ui.bar.focused, "Ctrl-F finds in a thread");
    }

    fn with_channels() -> Model {
        use ants_proto::{ChannelEntry, Humans};

        use crate::tui::reads::Read;
        let mut m = model();
        let channels = ["dev", "ops"]
            .map(|c| ChannelEntry {
                members: vec![crate::test_support::live::user("alice")],
                ..ChannelEntry::new(c.parse().unwrap())
            })
            .to_vec();
        m.read(Read::Directory(Ok((channels, Humans::default()))), at(0));
        m.focus = Focus::View;
        m
    }

    #[test]
    fn from_home_a_few_letters_of_a_place_and_enter_show_it() {
        let mut m = with_channels();
        let thread = ThreadId::generate();
        m.stored(vec![message(1, &thread, "dev-a", "alice", "the ops deploy is done")], at(1));
        m.key(Key::Ctrl('k'), at(1));
        typed(&mut m, "ops");
        let words: Vec<String> = m.palette(at(1)).into_iter().map(|e| e.word).collect();
        assert_eq!(words[0], "#ops", "{words:?}");
        assert!(words.contains(&"the ops deploy is done".to_owned()), "threads by title: {words:?}");
        m.key(Key::Enter, at(1));
        assert_eq!((m.page().name(), &m.nav), ("channel", &crate::tui::nav::Item::Channel("ops".parse().unwrap())));
        assert!(!m.ui.bar.focused && m.ui.bar.is_empty());

        m.key(Key::Char('/'), at(2));
        typed(&mut m, "deploy");
        assert_eq!(m.palette(at(2))[0].word, "the ops deploy is done", "unread first");
        assert_eq!(m.key(Key::Enter, at(2)), Some(Action::Open(thread)));
    }

    #[test]
    fn ctrl_t_goes_to_the_next_conversation_with_something_unread() {
        let mut m = with_channels();
        let (first, second) = (ThreadId::generate(), ThreadId::generate());
        m.stored(
            vec![message(1, &first, "dev-a", "alice", "one"), message(2, &second, "dev-a", "alice", "two")],
            at(2),
        );
        let Some(Action::Open(to)) = m.key(Key::Ctrl('t'), at(3)) else { panic!("no thread opened") };
        let then = if to == first { second } else { first };
        assert_eq!(m.key(Key::Ctrl('t'), at(3)), Some(Action::Open(then)), "the next one");
        m.data.marker.as_mut().unwrap().mark(2);
        assert_eq!(m.key(Key::Ctrl('t'), at(4)), None);
        assert_eq!(m.ui.notice.as_ref().unwrap().text, "nothing unread elsewhere");
    }
}
