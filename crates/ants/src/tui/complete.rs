//! Completion in the command bar: `@` lists users, `#` channels and `/` at the start the slash commands, narrowed by
//! what follows them up to the cursor.

use std::collections::BTreeSet;

use ants_proto::{Humans, UserId};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Style, Stylize as _};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use super::command::COMMANDS;
use super::frame;
use super::model::{Model, Page};

/// Most completions shown at once.
const ROWS: usize = 8;

/// A completion of the word at the cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// The word it puts in place of the one typed, its sigil included.
    pub word: String,
    /// What it tells besides.
    pub detail: String,
}

/// Which completion is selected, and whether Esc closed the list.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Pick {
    pub selected: usize,
    pub closed: bool,
}

/// What the word typed so far completes: its sigil and the text after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Word<'a> {
    User(&'a str),
    Channel(&'a str),
    Command(&'a str),
}

impl<'a> Word<'a> {
    /// The completable word in `typed`, the text from the start of the word at the cursor to the cursor, `first`
    /// when that word starts the text.
    pub fn of(typed: &'a str, first: bool) -> Option<Self> {
        let mut chars = typed.chars();
        let word = match chars.next()? {
            '@' => Word::User(chars.as_str()),
            '#' => Word::Channel(chars.as_str()),
            '/' if first => Word::Command(chars.as_str()),
            _ => return None,
        };
        Some(word)
    }
}

/// The `names` that `typed` narrows to, case-insensitive: those it starts first, then those holding it, each in the
/// order given.
pub fn narrow<T>(typed: &str, names: impl IntoIterator<Item = (String, T)>) -> Vec<(String, T)> {
    let typed = typed.to_lowercase();
    let (mut starting, mut holding) = (Vec::new(), Vec::new());
    for (name, item) in names {
        let lower = name.to_lowercase();
        if lower.starts_with(&typed) {
            starting.push((name, item));
        } else if lower.contains(&typed) {
            holding.push((name, item));
        }
    }
    starting.extend(holding);
    starting
}

impl Model {
    /// The completions of the word at the bar's cursor, none when the bar is not focused or Esc closed them.
    pub fn completions(&self) -> Vec<Candidate> {
        if !self.bar.focused || self.bar.pick.closed {
            return Vec::new();
        }
        let (start, typed) = self.bar.word();
        let Some(word) = Word::of(&typed, start == 0) else {
            return Vec::new();
        };
        match word {
            Word::User(typed) => narrow(typed, self.users().into_iter().map(|(u, what)| (u.to_string(), what)))
                .into_iter()
                .map(|(name, what)| Candidate { word: format!("@{name}"), detail: what.to_owned() })
                .collect(),
            Word::Channel(typed) => {
                let channels = self.data.channels.iter().map(|c| (c.channel.to_string(), c.members.len()));
                narrow(typed, channels)
                    .into_iter()
                    .map(|(name, members)| Candidate { word: format!("#{name}"), detail: format!("{members} members") })
                    .collect()
            }
            Word::Command(typed) => {
                narrow(typed, COMMANDS.iter().map(|(name, args, what)| ((*name).to_owned(), (args, what))))
                    .into_iter()
                    .map(|(name, (args, what))| Candidate {
                        word: format!("/{name}"),
                        detail: format!("{args:<24}{what}"),
                    })
                    .collect()
            }
        }
    }

    /// The users `@` completes, by name, each an "ant" or a "human": the members of the channel on view, else the
    /// roster's ants and the registered humans; the human using the UI and the default human are left out.
    fn users(&self) -> Vec<(UserId, &'static str)> {
        let data = &self.data;
        let channel = match self.page() {
            Page::Channel(page) => Some(page.channel.clone()),
            Page::Thread(page) => data.live.summary(&page.history.thread).and_then(|s| s.channels.first().cloned()),
            Page::Home(_) | Page::Work(_) | Page::Dm(_) | Page::Search(_) => None,
        };
        let members = channel.and_then(|channel| data.channels.iter().find(|c| c.channel == channel));
        let users: BTreeSet<&UserId> = match members {
            Some(entry) => entry.members.iter().collect(),
            None => data.live.roster.iter().map(|e| &e.ant).chain(data.humans.iter().flat_map(Humans::iter)).collect(),
        };
        users
            .into_iter()
            .filter(|u| *u != data.me() && **u != UserId::human())
            .map(|u| (u.clone(), if data.is_human(u) { "human" } else { "ant" }))
            .collect()
    }

    /// Take `key` for the list of completions when it shows: Up and Down select, Tab or Enter completes (Enter only
    /// when that changes the word, else it sends) and Esc closes it. False when the key is not the list's.
    pub fn complete_key(&mut self, key: super::model::Key) -> bool {
        use super::model::Key;
        let candidates = self.completions();
        if candidates.is_empty() {
            return false;
        }
        let pick = &mut self.bar.pick;
        let selected = pick.selected.min(candidates.len() - 1);
        match key {
            Key::Up => pick.selected = selected.checked_sub(1).unwrap_or(candidates.len() - 1),
            Key::Down => pick.selected = (selected + 1) % candidates.len(),
            Key::Esc => pick.closed = true,
            Key::Tab => self.bar.complete(&candidates[selected].word),
            Key::Enter if self.bar.word().1 != candidates[selected].word => {
                self.bar.complete(&candidates[selected].word);
            }
            _ => return false,
        }
        true
    }
}

/// Draw `candidates` over the view just above the bar at `bar`, `selected` highlighted.
pub fn draw(frame: &mut Frame, bar: Rect, candidates: &[Candidate], selected: usize) {
    let selected = selected.min(candidates.len().saturating_sub(1));
    let top = (selected + 1).saturating_sub(ROWS);
    let width = candidates.iter().map(|c| c.word.chars().count()).max().unwrap_or(0).max(12);
    let lines: Vec<Line> = candidates
        .iter()
        .enumerate()
        .skip(top)
        .take(ROWS)
        .map(|(at, c)| {
            let line = Line::from(vec![
                Span::styled(format!("{:<width$}  ", c.word), Style::new().bold()),
                Span::styled(c.detail.clone(), frame::dim()),
            ]);
            if at == selected { line.reversed() } else { line }
        })
        .collect();
    let height = (u16::try_from(lines.len()).unwrap_or(u16::MAX) + 2).min(bar.y);
    let area = Rect { x: bar.x, y: bar.y.saturating_sub(height), width: bar.width.min(84), height };
    let title =
        if candidates.len() > ROWS { format!(" {} of {} ", selected + 1, candidates.len()) } else { String::new() };
    frame.render_widget(Clear, area);
    frame.render_widget(Paragraph::new(lines).block(frame::block(title, true)), area);
}

#[cfg(test)]
mod tests {
    use ants_proto::{ChannelEntry, HumanEntry};

    use super::*;
    use crate::live::tests::{at, user};
    use crate::roster::tests::entry;
    use crate::tui::channel;
    use crate::tui::model::Key;
    use crate::tui::model::tests::model;
    use crate::tui::reads::Read;

    fn words(m: &Model) -> Vec<String> {
        m.completions().into_iter().map(|c| c.word).collect()
    }

    fn typed(m: &mut Model, text: &str) {
        for c in text.chars() {
            m.key(Key::Char(c), at(0));
        }
    }

    fn team() -> Model {
        let mut m = model();
        let ops = ChannelEntry {
            members: vec![user("alice"), user("dev-b"), user("carol")],
            ..ChannelEntry::new("ops".parse().unwrap())
        };
        let devops = ChannelEntry { members: vec![user("alice")], ..ChannelEntry::new("devops".parse().unwrap()) };
        let humans = Humans::from_entries(["alice", "bob", "carol"].map(|h| (h, HumanEntry::new(user(h)))));
        m.read(Read::Directory(Ok((vec![devops, ops], humans))), at(0));
        m.data.live.roster(
            vec![entry("dev-b", 60, 5, at(0)), entry("dev-a", 60, 5, at(0)), entry("bobcat", 60, 5, at(0))],
            at(0),
        );
        m
    }

    #[test]
    fn the_word_completed_is_a_user_a_channel_or_a_command_at_the_start() {
        assert_eq!(Word::of("@de", false), Some(Word::User("de")));
        assert_eq!(Word::of("#", false), Some(Word::Channel("")));
        assert_eq!(Word::of("/ap", true), Some(Word::Command("ap")));
        assert_eq!(Word::of("/ap", false), None);
        assert_eq!(Word::of("plain", true), None);
        assert_eq!(Word::of("", true), None);
    }

    #[test]
    fn prefixes_come_first_then_substrings_ignoring_case() {
        let names = ["devops", "ops", "Opsec", "random"].map(|n| (n.to_owned(), ()));
        let narrowed: Vec<String> = narrow("OPS", names).into_iter().map(|(n, ())| n).collect();
        assert_eq!(narrowed, ["ops", "Opsec", "devops"]);
    }

    #[test]
    fn at_lists_the_channels_members_on_a_channel_else_the_ants_and_humans() {
        let mut m = team();
        m.key(Key::Char('@'), at(0));
        assert_eq!(words(&m), ["@bob", "@bobcat", "@carol", "@dev-a", "@dev-b"], "neither alice nor the default human");
        typed(&mut m, "B");
        assert_eq!(words(&m), ["@bob", "@bobcat", "@dev-b"]);
        let details: Vec<String> = m.completions().into_iter().map(|c| c.detail).collect();
        assert_eq!(details, ["human", "ant", "ant"]);
        m.bar.set_text("@");
        m.pages.push(Page::Channel(channel::Page::new("ops".parse().unwrap())));
        assert_eq!(words(&m), ["@carol", "@dev-b"]);
        m.bar.set_text("hi #");
        assert_eq!(words(&m), ["#devops", "#ops"]);
        m.bar.set_text("/");
        assert_eq!(words(&m).len(), COMMANDS.len());
        m.bar.set_text("/mu");
        assert_eq!(
            m.completions()[0],
            Candidate {
                word: "/mute".into(),
                detail: format!("{:<24}{}", "[DURATION]", "hold back the thread's notifications")
            }
        );
        m.bar.set_text("say /mu");
        assert!(m.completions().is_empty(), "a command only starts the text");
    }

    #[test]
    fn up_down_select_tab_or_enter_complete_and_esc_closes() {
        let mut m = team();
        m.key(Key::Ctrl('k'), at(0));
        typed(&mut m, "ask @d please");
        for _ in 0.."please".len() + 1 {
            m.key(Key::Left, at(0));
        }
        assert_eq!(words(&m), ["@dev-a", "@dev-b"]);
        m.key(Key::Down, at(0));
        m.key(Key::Down, at(0));
        m.key(Key::Up, at(0));
        assert_eq!(m.key(Key::Tab, at(0)), None);
        assert_eq!(m.bar.text(), "ask @dev-b please", "the space after the word is kept");
        assert!(m.completions().is_empty());
        m.bar.set_text("/don");
        assert_eq!(m.key(Key::Enter, at(0)), None);
        assert_eq!(m.bar.text(), "/done ");
        m.bar.set_text("/done");
        assert_eq!(m.key(Key::Enter, at(0)), None);
        assert_eq!(m.bar.status.as_ref().unwrap().text, "no thread open or selected", "Enter on a whole word sends it");
        m.bar.focused = true;
        m.bar.set_text("#o");
        m.key(Key::Esc, at(0));
        assert!(m.completions().is_empty() && m.bar.focused, "Esc closes the list, not the bar");
        m.key(Key::Enter, at(0));
        assert_eq!(m.bar.status.as_ref().unwrap().text, "nothing to post to #o");
        typed(&mut m, "p");
        assert_eq!(words(&m), ["#ops", "#devops"], "typing opens the list again");
    }
}
