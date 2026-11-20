//! The channel view's members editor, which `M` opens over the header's members pane: a row for each member, which
//! `d` removes, then rows to add members (ants or humans), and to set or clear the messenger (an ant in the colony),
//! the owner (an ant) and the description. Each change runs [`Job::Channel`], which writes the channel's entry as
//! `ants channel set|add|remove` does; the rows follow the entry as read back.

use std::collections::BTreeSet;

use ants_proto::{ChannelEntry, ChannelId, Humans, MAX_CHANNEL_DESCRIPTION, UserId};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Stylize as _;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use crate::channel::{self, Change};
use crate::text::excerpt;
use crate::tui::complete::{self, Candidate, Pick};
use crate::tui::exec::Job;
use crate::tui::input::Input;
use crate::tui::model::{Action, Data, Key, To};
use crate::tui::select::step;
use crate::tui::style;

/// Most completions shown under the row typed in.
const CANDIDATES: usize = 5;

/// A row of the editor besides the members'.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Field {
    Add,
    Messenger,
    Owner,
    Description,
}

const FIELDS: [Field; 4] = [Field::Add, Field::Messenger, Field::Owner, Field::Description];

/// What is typed in a row, and which of its completions is picked.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Typing {
    field: Field,
    text: Input,
    pick: Pick,
}

/// What a key did to the editor.
#[derive(Debug, PartialEq, Eq)]
pub enum Edited {
    Kept,
    Closed,
    To(Box<To>),
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Editor {
    /// The row selected: a member, else a field after them.
    at: usize,
    typing: Option<Typing>,
}

impl Editor {
    pub fn keys(&self) -> &'static str {
        match &self.typing {
            Some(Typing { field: Field::Description, .. }) => "type the description · Enter set · Esc cancel",
            Some(_) => "type @ and a name · ↑↓ Tab complete · Enter set · Esc cancel",
            None => "↑↓ row · Enter edit · a add · d remove or clear · Esc close",
        }
    }

    /// The field of row `at` of `entry`, none on a member's.
    fn field(entry: &ChannelEntry, at: usize) -> Option<Field> {
        at.checked_sub(entry.members.len()).and_then(|at| FIELDS.get(at)).copied()
    }

    /// The row selected, kept within those of `entry`.
    fn at(&self, entry: &ChannelEntry) -> usize {
        self.at.min(entry.members.len() + FIELDS.len() - 1)
    }

    pub fn key(&mut self, key: Key, channel: &ChannelId, data: &Data) -> Edited {
        let Some(entry) = data.channels.iter().find(|c| c.channel == *channel) else { return Edited::Closed };
        if self.typing.is_some() {
            return self.type_key(key, entry, data);
        }
        let at = self.at(entry);
        match key {
            Key::Esc => return Edited::Closed,
            Key::Char('a' | '@') => {
                self.at = entry.members.len();
                self.start(Field::Add, entry);
            }
            Key::Enter => {
                if let Some(field) = Self::field(entry, at) {
                    self.start(field, entry);
                }
            }
            Key::Char('d') | Key::Delete | Key::Backspace => {
                let change = match (entry.members.get(at), Self::field(entry, at)) {
                    (Some(member), _) => Change { remove: vec![member.clone()], ..Change::default() },
                    (None, Some(Field::Messenger)) => Change { messenger: Some(None), ..Change::default() },
                    (None, Some(Field::Owner)) => Change { owner: Some(None), ..Change::default() },
                    (None, Some(Field::Description)) => Change { description: Some(None), ..Change::default() },
                    (None, Some(Field::Add) | None) => return Edited::Kept,
                };
                return Edited::To(Box::new(run(channel, change)));
            }
            _ => {
                if let Some(to) = step(key, at, None) {
                    self.at = to.min(entry.members.len() + FIELDS.len() - 1);
                }
            }
        }
        Edited::Kept
    }

    /// Type `line` pasted in the row typed in, if any; the editor takes it anyway.
    pub fn paste(&mut self, line: &str) -> bool {
        if let Some(typing) = &mut self.typing {
            typing.text.paste(line);
            typing.pick = Pick::default();
        }
        true
    }

    /// Start typing in `field`, from what it holds.
    fn start(&mut self, field: Field, entry: &ChannelEntry) {
        let text = match field {
            Field::Add => "@".to_owned(),
            Field::Messenger => entry.messenger.as_ref().map_or_else(String::new, |m| format!("@{m}")),
            Field::Owner => entry.owner.as_ref().map_or_else(String::new, |o| format!("@{o}")),
            Field::Description => entry.description.clone().unwrap_or_default(),
        };
        self.typing = Some(Typing { field, text: Input::from(text.as_str()), pick: Pick::default() });
    }

    fn type_key(&mut self, key: Key, entry: &ChannelEntry, data: &Data) -> Edited {
        let candidates = self.completions(entry, data);
        let typing = self.typing.as_mut().expect("typing");
        if !candidates.is_empty() {
            let selected = typing.pick.selected.min(candidates.len() - 1);
            let word = &candidates[selected].word;
            match key {
                Key::Up => typing.pick.selected = selected.checked_sub(1).unwrap_or(candidates.len() - 1),
                Key::Down => typing.pick.selected = (selected + 1) % candidates.len(),
                Key::Esc => typing.pick.closed = true,
                Key::Tab => typing.complete(word),
                Key::Enter if typing.text.word().1 != *word => typing.complete(word),
                _ => return self.edit_key(key, &entry.channel, data),
            }
            return Edited::Kept;
        }
        self.edit_key(key, &entry.channel, data)
    }

    fn edit_key(&mut self, key: Key, channel: &ChannelId, data: &Data) -> Edited {
        let typing = self.typing.as_mut().expect("typing");
        match key {
            Key::Esc => self.typing = None,
            Key::Enter => {
                return match change(typing.field, &typing.text.text(), channel, data) {
                    Ok(change) => {
                        self.typing = None;
                        Edited::To(Box::new(run(channel, change)))
                    }
                    Err(why) => Edited::To(Box::new(To::Refused(why))),
                };
            }
            key => {
                if typing.text.key(key) {
                    typing.pick = Pick::default();
                }
            }
        }
        Edited::Kept
    }

    /// The completions of the word before the cursor, by name: for a member to add, the colony's ants and the humans not
    /// in `entry` yet; for the messenger or owner, the colony's ants.
    fn completions(&self, entry: &ChannelEntry, data: &Data) -> Vec<Candidate> {
        let Some(typing) = self.typing.as_ref().filter(|t| !t.pick.closed && t.field != Field::Description) else {
            return Vec::new();
        };
        let (_, word) = typing.text.word();
        if word.is_empty() {
            return Vec::new();
        }
        let ants = data.live.colony.iter().map(|e| &e.ant).filter(|a| !data.is_human(a));
        let users: BTreeSet<&UserId> = match typing.field {
            Field::Add => ants
                .chain(data.humans.iter().flat_map(Humans::iter))
                .filter(|u| !entry.is_member(u) && **u != UserId::human())
                .collect(),
            _ => ants.collect(),
        };
        let names = users.into_iter().map(|u| (u.to_string(), if data.is_human(u) { "human" } else { "ant" }));
        complete::narrow(word.trim_start_matches('@'), names)
            .into_iter()
            .map(|(name, what)| Candidate { word: format!("@{name}"), detail: what.to_owned() })
            .collect()
    }

    /// Draw the editor of `entry` in `area`, over what is there.
    pub fn draw(&self, frame: &mut Frame, area: Rect, entry: &ChannelEntry, data: &Data, now: DateTime<Utc>) {
        let width = usize::from(area.width.saturating_sub(2));
        let at = self.at(entry);
        let (lines, focus) = self.lines(entry, data, now, width, at);
        let rows = usize::from(area.height.saturating_sub(2));
        let scroll = focus.end.saturating_sub(rows).min(focus.start);
        let mut block = style::block(format!(" manage #{} ", entry.channel), true);
        if let Some(Typing { field: Field::Description, text, .. }) = &self.typing {
            let count = text.text().trim().chars().count();
            let style = if count > MAX_CHANNEL_DESCRIPTION {
                ratatui::style::Style::new().fg(style::ERROR)
            } else {
                style::dim()
            };
            block = block.title_bottom(Line::styled(format!(" {count}/{MAX_CHANNEL_DESCRIPTION} "), style));
        }
        frame.render_widget(Clear, area);
        let scroll = u16::try_from(scroll).unwrap_or(u16::MAX);
        frame.render_widget(Paragraph::new(lines).block(block).scroll((scroll, 0)), area);
    }

    /// How many lines the editor of `entry` takes, its borders included.
    pub fn height(&self, entry: &ChannelEntry, data: &Data) -> u16 {
        let candidates = self.completions(entry, data).len().min(CANDIDATES);
        u16::try_from(entry.members.len() + FIELDS.len() + candidates + 2).unwrap_or(u16::MAX)
    }

    /// The editor's lines, and those of the row selected or typed in with its completions.
    fn lines(
        &self,
        entry: &ChannelEntry,
        data: &Data,
        now: DateTime<Utc>,
        width: usize,
        at: usize,
    ) -> (Vec<Line<'static>>, std::ops::Range<usize>) {
        let mut lines: Vec<Line<'static>> = Vec::new();
        let mut focus = 0..1;
        let selected =
            |line: Line<'static>, row: usize| if row == at { line.patch_style(style::highlight(true)) } else { line };
        for (row, member) in entry.members.iter().enumerate() {
            let mut spans = vec![Span::raw("  ")];
            spans.extend(super::header::member(member, entry, data, now));
            if row == at {
                focus = lines.len()..lines.len() + 1;
            }
            lines.push(selected(Line::from(spans), row));
        }
        for (row, field) in FIELDS.iter().enumerate().map(|(n, f)| (entry.members.len() + n, *f)) {
            let label = match field {
                Field::Add => "+ add",
                Field::Messenger => "messenger",
                Field::Owner => "owner",
                Field::Description => "description",
            };
            let mut spans = vec![Span::styled(format!("{label:<12}"), style::dim())];
            let start = lines.len();
            match self.typing.as_ref().filter(|t| t.field == field) {
                Some(typing) => {
                    spans.extend([Span::raw("> "), Span::raw(typing.text.caret(width.saturating_sub(14)))]);
                    lines.push(Line::from(spans));
                    let candidates = self.completions(entry, data);
                    let picked = typing.pick.selected.min(candidates.len().saturating_sub(1));
                    let top = (picked + 1).saturating_sub(CANDIDATES);
                    for (n, c) in candidates.iter().enumerate().skip(top).take(CANDIDATES) {
                        let line = Line::from(vec![
                            Span::raw("    "),
                            Span::raw(c.word.clone()).bold(),
                            Span::styled(format!("  {}", c.detail), style::dim()),
                        ]);
                        lines.push(if n == picked { line.reversed() } else { line });
                    }
                    focus = start..lines.len();
                    continue;
                }
                None => spans.push(match field {
                    Field::Add => Span::styled("ants or humans: a or Enter", style::dim()),
                    Field::Messenger => match &entry.messenger {
                        Some(messenger) => Span::raw(messenger.to_string()),
                        None => Span::styled(format!("{} (default)", entry.messenger()), style::dim()),
                    },
                    Field::Owner => match &entry.owner {
                        Some(owner) => Span::raw(owner.to_string()),
                        None => Span::styled("none", style::dim()),
                    },
                    Field::Description => match &entry.description {
                        Some(text) => Span::raw(excerpt(text, width.saturating_sub(13).max(1))),
                        None => Span::styled("none", style::dim()),
                    },
                }),
            }
            if row == at && self.typing.is_none() {
                focus = start..start + 1;
            }
            lines.push(selected(Line::from(spans), row));
        }
        (lines, focus)
    }
}

impl Typing {
    /// Put `word` in place of the word at the cursor; after it, a member to add may follow.
    fn complete(&mut self, word: &str) {
        let add = self.field == Field::Add;
        self.text.complete(word, add);
        if add {
            self.pick = Pick::default();
        } else {
            self.pick.closed = true;
        }
    }
}

/// Write `change` to `channel`.
fn run(channel: &ChannelId, change: Change) -> To {
    To::Do(Action::Run(Job::Channel { channel: channel.clone(), change }, String::new()))
}

/// The change `text` typed in `field` makes, refused as `ants channel` refuses it; the messenger an ant of the
/// colony too.
fn change(field: Field, text: &str, channel: &ChannelId, data: &Data) -> Result<Change, String> {
    let text = text.trim();
    let user = || if text.trim_start_matches('@').is_empty() { Ok(None) } else { channel::parse_ant(text).map(Some) };
    let change = match field {
        Field::Add => {
            let words = text.split_whitespace().filter(|w| *w != "@");
            let add = words.map(channel::parse_ant).collect::<Result<Vec<_>, _>>()?;
            if add.is_empty() {
                return Err("nothing to add: type @ and a name".to_owned());
            }
            Change { add, ..Change::default() }
        }
        Field::Messenger => {
            let messenger = user()?;
            let unknown = |m: &&UserId| !data.is_human(m) && !data.live.colony.iter().any(|e| e.ant == **m);
            if let Some(m) = messenger.as_ref().filter(unknown) {
                return Err(format!("{m} is no ant in the colony, so not the messenger of #{channel}"));
            }
            Change { messenger: Some(messenger), ..Change::default() }
        }
        Field::Owner => Change { owner: Some(user()?), ..Change::default() },
        Field::Description => {
            Change { description: Some((!text.is_empty()).then(|| text.to_owned())), ..Change::default() }
        }
    };
    // The CLI's human is one before the humans are read.
    let mut humans = data.humans.clone().unwrap_or_default();
    let me = data.me();
    let _ = humans.add(me.as_str(), ants_proto::HumanEntry::new(me.clone()));
    change.check(channel, &humans).map_err(|e| format!("{e:#}"))?;
    Ok(change)
}

#[cfg(test)]
mod tests {
    use ants_proto::HumanEntry;

    use super::*;
    use crate::test_support::colony::entry;
    use crate::test_support::live::{at, user};
    use crate::test_support::render::rendered;
    use crate::tui::command::Tone;
    use crate::tui::exec::{Done, Outcome};
    use crate::tui::input::{Typed, exercise};
    use crate::tui::model::{Model, Page};
    use crate::tui::nav;
    use crate::tui::reads::Read;
    use crate::tui::test_support::model;

    /// Alice's model showing #ops, which dev-a owns, with dev-m its messenger, bob a member and carol not; dev-b is in
    /// the colony too.
    fn ops() -> Model {
        let ops: ChannelId = "ops".parse().unwrap();
        let mut m = model();
        let entry = ChannelEntry {
            members: ["alice", "dev-a", "bob", "dev-m"].map(user).to_vec(),
            messenger: Some(user("dev-m")),
            owner: Some(user("dev-a")),
            description: Some("Deploys and outages.".to_owned()),
            ..ChannelEntry::new(ops.clone())
        };
        let humans = Humans::from_entries(["alice", "bob", "carol"].map(|h| (h, HumanEntry::new(user(h)))));
        m.read(Read::Directory(Ok((vec![entry], humans))), at(0));
        let colony = ["dev-a", "dev-b", "dev-m"].map(|a| self::entry(a, 60, 5, at(0)));
        m.data.live.colony(colony.to_vec(), at(0));
        m.select(nav::Item::Channel(ops));
        m.focus = crate::tui::model::Focus::View;
        m
    }

    fn keys(m: &mut Model, keys: impl IntoIterator<Item = Key>) -> Option<Action> {
        keys.into_iter().fold(None, |_, key| m.key(key, at(1)))
    }

    fn typed(m: &mut Model, text: &str) -> Option<Action> {
        keys(m, text.chars().map(Key::Char))
    }

    fn editor(m: &Model) -> Option<&Editor> {
        let Page::Channel(page) = m.page() else { panic!("not the channel") };
        page.editor.as_ref()
    }

    fn written(change: Change) -> Option<Action> {
        Some(Action::Run(Job::Channel { channel: "ops".parse().unwrap(), change }, String::new()))
    }

    fn status(m: &Model) -> (String, Tone) {
        let status = m.bar.status.as_ref().expect("a status");
        (status.text.clone(), status.tone)
    }

    /// Move to row `row` from the top.
    fn row(m: &mut Model, row: usize) {
        keys(m, std::iter::once(Key::Home).chain(std::iter::repeat_n(Key::Down, row)));
    }

    #[test]
    fn m_opens_the_editor_which_takes_the_keys_until_esc_closes_it() {
        let mut m = ops();
        let Page::Channel(page) = m.page() else { panic!() };
        assert_eq!(page.prompt(), None);
        keys(&mut m, [Key::Up, Key::Char('M')]);
        let Page::Channel(page) = m.page() else { panic!() };
        assert_eq!(page.prompt().unwrap(), "↑↓ row · Enter edit · a add · d remove or clear · Esc close");
        assert_eq!(keys(&mut m, [Key::Char('q'), Key::Char('n'), Key::Char('/')]), None, "taken by the editor");
        assert!(!m.bar.focused && editor(&m).is_some());
        keys(&mut m, [Key::Esc]);
        assert!(editor(&m).is_none(), "closed");
        assert!(matches!(m.page(), Page::Channel(_)), "still on the channel");
    }

    #[test]
    fn m_is_refused_before_the_channel_is_read() {
        let mut m = model();
        m.select(nav::Item::Channel("ops".parse().unwrap()));
        m.focus = crate::tui::model::Focus::View;
        keys(&mut m, [Key::Up, Key::Char('M')]);
        assert!(editor(&m).is_none());
        assert_eq!(status(&m), ("#ops is not read yet".to_owned(), Tone::Error));
    }

    #[test]
    fn a_completes_ants_and_humans_not_members_yet_and_adds_them() {
        let mut m = ops();
        keys(&mut m, [Key::Up, Key::Char('M'), Key::Char('a')]);
        let entry = &m.data.channels[0];
        let words = |m: &Model| -> Vec<String> {
            editor(m).unwrap().completions(&m.data.channels[0], &m.data).into_iter().map(|c| c.word).collect()
        };
        assert_eq!(words(&m), ["@carol", "@dev-b"], "not alice, bob, dev-a or dev-m: members already");
        assert_eq!(editor(&m).unwrap().completions(entry, &m.data)[0].detail, "human");
        typed(&mut m, "de");
        assert_eq!(words(&m), ["@dev-b"]);
        keys(&mut m, [Key::Tab]);
        typed(&mut m, "@c");
        keys(&mut m, [Key::Enter]);
        assert_eq!(editor(&m).unwrap().typing.as_ref().unwrap().text.text(), "@dev-b @carol ", "Enter completes first");
        let change = Change { add: vec![user("dev-b"), user("carol")], ..Change::default() };
        assert_eq!(keys(&mut m, [Key::Enter]), written(change));
        assert_eq!(status(&m), ("running…".to_owned(), Tone::Busy));
        assert!(editor(&m).is_some_and(|e| e.typing.is_none()), "the editor stays open");
    }

    #[test]
    fn a_paste_goes_into_the_row_typed_in_and_never_acts_as_keys() {
        let mut m = ops();
        keys(&mut m, [Key::Up, Key::Char('M')]);
        m.paste("dd", at(1));
        assert!(!m.bar.focused && m.bar.is_empty() && editor(&m).unwrap().typing.is_none(), "dropped");
        keys(&mut m, [Key::Char('a')]);
        m.paste("dev-b\n", at(1));
        assert_eq!(editor(&m).unwrap().typing.as_ref().unwrap().text.text(), "@dev-b ");
    }

    #[test]
    fn d_removes_the_member_selected() {
        let mut m = ops();
        keys(&mut m, [Key::Up, Key::Char('M')]);
        row(&mut m, 2);
        assert_eq!(keys(&mut m, [Key::Char('d')]), written(Change { remove: vec![user("bob")], ..Change::default() }));
        row(&mut m, 4);
        assert_eq!(keys(&mut m, [Key::Char('d')]), None, "nothing to remove on the add row");
    }

    #[test]
    fn the_messenger_is_an_ant_of_the_colony_set_or_cleared() {
        let mut m = ops();
        keys(&mut m, [Key::Up, Key::Char('M')]);
        row(&mut m, 5);
        keys(&mut m, [Key::Enter]);
        assert_eq!(editor(&m).unwrap().typing.as_ref().unwrap().text.text(), "@dev-m", "from what it is");
        keys(&mut m, std::iter::repeat_n(Key::Backspace, 3));
        keys(&mut m, [Key::Down, Key::Enter]);
        assert_eq!(editor(&m).unwrap().typing.as_ref().unwrap().text.text(), "@dev-b", "completed");
        let set = keys(&mut m, [Key::Enter]);
        assert_eq!(set, written(Change { messenger: Some(Some(user("dev-b"))), ..Change::default() }));

        keys(&mut m, [Key::Enter]);
        keys(&mut m, std::iter::repeat_n(Key::Backspace, 6));
        typed(&mut m, "@ghost");
        assert_eq!(keys(&mut m, [Key::Enter]), None);
        assert_eq!(status(&m), ("ghost is no ant in the colony, so not the messenger of #ops".to_owned(), Tone::Error));
        assert!(editor(&m).unwrap().typing.is_some(), "kept to fix");
        keys(&mut m, std::iter::repeat_n(Key::Backspace, 6));
        typed(&mut m, "@bob");
        assert_eq!(keys(&mut m, [Key::Enter]), None);
        assert!(status(&m).0.starts_with("bob is a human, so not the messenger"), "{:?}", status(&m));

        keys(&mut m, std::iter::repeat_n(Key::Backspace, 4));
        assert_eq!(keys(&mut m, [Key::Enter]), written(Change { messenger: Some(None), ..Change::default() }));
        assert_eq!(keys(&mut m, [Key::Char('d')]), written(Change { messenger: Some(None), ..Change::default() }));
    }

    #[test]
    fn the_owner_is_an_ant_set_or_cleared() {
        let mut m = ops();
        keys(&mut m, [Key::Up, Key::Char('M')]);
        row(&mut m, 6);
        keys(&mut m, [Key::Enter]);
        keys(&mut m, std::iter::repeat_n(Key::Backspace, 6));
        typed(&mut m, "alice");
        assert_eq!(keys(&mut m, [Key::Enter]), None);
        assert!(status(&m).0.starts_with("alice is a human, who needs no ownership"), "{:?}", status(&m));
        keys(&mut m, std::iter::repeat_n(Key::Backspace, 5));
        typed(&mut m, "@lead");
        let set = keys(&mut m, [Key::Enter]);
        assert_eq!(set, written(Change { owner: Some(Some(user("lead"))), ..Change::default() }), "any ant");
        assert_eq!(keys(&mut m, [Key::Delete]), written(Change { owner: Some(None), ..Change::default() }));
    }

    #[test]
    fn the_description_is_at_most_1000_characters() {
        let mut m = ops();
        keys(&mut m, [Key::Up, Key::Char('M')]);
        row(&mut m, 7);
        keys(&mut m, [Key::Enter]);
        assert_eq!(editor(&m).unwrap().typing.as_ref().unwrap().text.text(), "Deploys and outages.");
        typed(&mut m, &" x".repeat(500));
        assert_eq!(keys(&mut m, [Key::Enter]), None);
        assert!(status(&m).0.contains("1000"), "{:?}", status(&m));
        assert_eq!(status(&m).1, Tone::Error);
        keys(&mut m, std::iter::repeat_n(Key::Backspace, 1020));
        typed(&mut m, " Releases ");
        let set = keys(&mut m, [Key::Enter]);
        assert_eq!(set, written(Change { description: Some(Some("Releases".to_owned())), ..Change::default() }));
        let cleared = keys(&mut m, [Key::Char('d')]);
        assert_eq!(cleared, written(Change { description: Some(None), ..Change::default() }));
    }

    #[test]
    fn a_row_typed_in_edits_as_the_bar_does() {
        let mut m = ops();
        keys(&mut m, [Key::Up, Key::Char('M')]);
        row(&mut m, 7);
        keys(&mut m, [Key::Enter, Key::Ctrl('u')]);
        let want = exercise(|typed| match typed {
            Typed::Key(key) => _ = m.key(key, at(1)),
            Typed::Paste(text) => m.paste(text, at(1)),
        });
        assert_eq!(editor(&m).unwrap().typing.as_ref().unwrap().text.text(), want);
        let set = keys(&mut m, [Key::Enter]);
        assert_eq!(set, written(Change { description: Some(Some(want.to_owned())), ..Change::default() }));
    }

    #[test]
    fn a_write_refused_shows_on_the_status_line_and_one_done_reads_the_channel_back() {
        let mut m = ops();
        keys(&mut m, [Key::Up, Key::Char('M')]);
        let refused = anyhow::anyhow!("no permission to write ants_channels");
        m.outcome(Outcome { result: Err(refused), text: String::new(), quoting: None });
        assert_eq!(status(&m), ("no permission to write ants_channels".to_owned(), Tone::Error));
        assert!(!m.bar.focused && editor(&m).is_some(), "the editor stays");
        let entry = ChannelEntry { members: vec![user("alice")], ..m.data.channels[0].clone() };
        m.read(Read::Directory(Ok((vec![entry], m.data.humans.clone().unwrap()))), at(2));
        let shown = shown(&m, 130, 30);
        assert!(shown.lines().nth(2).unwrap().contains("+ add"), "the rows follow the entry read back: {shown}");
        let done = Done { line: "#ops: removed bob".to_owned(), ..Done::default() };
        m.outcome(Outcome { result: Ok(done), text: String::new(), quoting: None });
        assert_eq!(status(&m), ("#ops: removed bob".to_owned(), Tone::Done));
    }

    fn shown(m: &Model, width: u16, height: u16) -> String {
        let Page::Channel(page) = m.page() else { panic!("not the channel") };
        rendered(width, height, |f| page.draw(f, f.area(), &m.data, at(61), true))
    }

    #[test]
    fn the_editor_covers_the_members_pane_or_under_a_one_line_header_the_top_of_the_view() {
        let mut m = ops();
        let before = shown(&m, 130, 30);
        assert!(before.lines().nth(4).unwrap().contains("M manage"), "{before}");
        keys(&mut m, [Key::Up, Key::Char('M'), Key::Char('a'), Key::Char('c')]);
        let shown = self::shown(&m, 130, 30);
        let lines: Vec<&str> = shown.lines().collect();
        let left = lines[0].chars().collect::<String>().split("manage #ops").next().unwrap().chars().count();
        assert!(left > 30 && left < 60, "over the members pane: {shown}");
        assert!(lines[1].contains("alice") && lines[4].contains("dev-m ✉"), "{shown}");
        assert!(lines[5].contains("+ add") && lines[5].contains("> @c▏"), "{shown}");
        assert!(lines[6].contains("@carol") && lines[6].contains("human"), "{shown}");
        assert!(lines[7].contains("messenger") && lines[7].contains("dev-m"), "{shown}");
        assert!(lines[9].contains("description") && lines[9].contains("Deploys and outages."), "{shown}");

        for (width, height) in [(80, 30), (130, 12)] {
            let shown = self::shown(&m, width, height);
            let lines: Vec<&str> = shown.lines().collect();
            assert!(lines[0].starts_with("#ops · notify"), "the header in one line: {shown}");
            assert!(lines[1].starts_with("┌ manage #ops"), "at the top of the view: {shown}");
            assert!(shown.contains("> @c▏"), "the row typed in shows: {shown}");
        }
    }
}
