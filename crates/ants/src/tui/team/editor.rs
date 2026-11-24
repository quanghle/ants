//! The team view's editor, which `M` opens over the top of the members tree: a row for each direct member, which `d`
//! removes (the lead too, leaving the team without a lead of its own), then rows to add members (ants or teams on no
//! team), to hand the lead to another direct ant member or clear it, and to set or clear the owner (a human) and the description. Each change runs [`Job::Team`], which
//! writes the directory as `ants team add|remove|set` does; the rows follow the directory as read back.

use std::collections::BTreeSet;

use ants_proto::{Humans, MAX_TEAM_DESCRIPTION, Team, TeamDirectory, TeamRefusal, UserId};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Style, Stylize as _};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use crate::teams::change::Change;
use crate::teams::parse_name;
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
    Lead,
    Owner,
    Description,
}

const FIELDS: [Field; 4] = [Field::Add, Field::Lead, Field::Owner, Field::Description];

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

    /// The field of row `at` of `team`, none on a member's.
    fn field(team: &Team, at: usize) -> Option<Field> {
        at.checked_sub(team.members.len()).and_then(|at| FIELDS.get(at)).copied()
    }

    /// The row selected, kept within those of `team`.
    fn at(&self, team: &Team) -> usize {
        self.at.min(team.members.len() + FIELDS.len() - 1)
    }

    pub fn key(&mut self, key: Key, id: &UserId, data: &Data) -> Edited {
        let Some((directory, team)) = data.teams.as_ref().and_then(|d| Some((d, d.team(id)?))) else {
            return Edited::Closed;
        };
        if self.typing.is_some() {
            return self.type_key(key, directory, team, data);
        }
        let at = self.at(team);
        match key {
            Key::Esc => return Edited::Closed,
            Key::Char('a' | '@') => {
                self.at = team.members.len();
                self.start(Field::Add, team);
            }
            Key::Enter => {
                if let Some(field) = Self::field(team, at) {
                    self.start(field, team);
                }
            }
            Key::Char('d') | Key::Delete | Key::Backspace => {
                let change = match (team.members.get(at), Self::field(team, at)) {
                    (Some(member), _) => Change { remove: vec![member.clone()], ..Change::default() },
                    (None, Some(Field::Lead)) if team.lead.is_none() => return Edited::Kept,
                    (None, Some(Field::Lead)) => Change { lead: Some(None), ..Change::default() },
                    (None, Some(Field::Owner)) => Change { owner: Some(None), ..Change::default() },
                    (None, Some(Field::Description)) => Change { description: Some(None), ..Change::default() },
                    (None, Some(Field::Add) | None) => return Edited::Kept,
                };
                return Edited::To(Box::new(run(id, change)));
            }
            _ => {
                if let Some(to) = step(key, at, None) {
                    self.at = to.min(team.members.len() + FIELDS.len() - 1);
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
    fn start(&mut self, field: Field, team: &Team) {
        let text = match field {
            Field::Add => "@".to_owned(),
            Field::Lead => team.lead.as_ref().map_or_else(|| "@".to_owned(), |l| format!("@{l}")),
            Field::Owner => team.owner.as_ref().map_or_else(String::new, |o| format!("@{o}")),
            Field::Description => team.description.clone().unwrap_or_default(),
        };
        self.typing = Some(Typing { field, text: Input::from(text.as_str()), pick: Pick::default() });
    }

    fn type_key(&mut self, key: Key, directory: &TeamDirectory, team: &Team, data: &Data) -> Edited {
        let candidates = self.completions(directory, team, data);
        let typing = self.typing.as_mut().expect("typing");
        if !candidates.is_empty() {
            let selected = typing.pick.selected.min(candidates.len() - 1);
            let word = &candidates[selected].word;
            match key {
                Key::Up => typing.pick.up(candidates.len()),
                Key::Down => typing.pick.down(candidates.len()),
                Key::Esc => typing.pick.closed = true,
                Key::Tab => typing.complete(word),
                Key::Enter if typing.text.word().1 != *word => typing.complete(word),
                _ => return self.edit_key(key, directory, team, data),
            }
            return Edited::Kept;
        }
        self.edit_key(key, directory, team, data)
    }

    fn edit_key(&mut self, key: Key, directory: &TeamDirectory, team: &Team, data: &Data) -> Edited {
        let typing = self.typing.as_mut().expect("typing");
        match key {
            Key::Esc => self.typing = None,
            Key::Enter => {
                return match change(typing.field, &typing.text.text(), directory, team, data) {
                    Ok(change) => {
                        self.typing = None;
                        Edited::To(Box::new(run(&team.team, change)))
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

    /// The completions of the word before the cursor, by name: for a member to add, the colony's ants and the teams on
    /// no team (but those above `team`); for the lead, the team's direct ant members; for the owner, the humans.
    fn completions(&self, directory: &TeamDirectory, team: &Team, data: &Data) -> Vec<Candidate> {
        let Some(typing) = self.typing.as_ref().filter(|t| !t.pick.closed && t.field != Field::Description) else {
            return Vec::new();
        };
        let (_, word) = typing.text.word();
        if word.is_empty() {
            return Vec::new();
        }
        let free = |u: &&UserId| directory.holder(u).is_none();
        let names: Vec<(String, &str)> = match typing.field {
            Field::Add => {
                let ants: BTreeSet<&UserId> = data
                    .live
                    .colony
                    .iter()
                    .map(|e| &e.ant)
                    .filter(|a| !data.is_human(a) && !directory.is_team(a))
                    .filter(free)
                    .collect();
                let teams = directory.teams.iter().map(|t| &t.team).filter(free);
                let teams = teams.filter(|t| !directory.is_beneath(&team.team, t));
                ants.into_iter().map(|a| (a.to_string(), "ant")).chain(teams.map(|t| (t.to_string(), "team"))).collect()
            }
            Field::Lead => team
                .members
                .iter()
                .filter(|m| !directory.is_team(m) && team.lead.as_ref() != Some(*m))
                .map(|m| (m.to_string(), "ant"))
                .collect(),
            Field::Owner => {
                let humans: BTreeSet<&UserId> = data.humans.iter().flat_map(Humans::iter).collect();
                humans.into_iter().map(|h| (h.to_string(), "human")).collect()
            }
            Field::Description => Vec::new(),
        };
        complete::narrow(word.trim_start_matches('@'), names)
            .into_iter()
            .map(|(name, what)| Candidate { word: format!("@{name}"), detail: what.to_owned() })
            .collect()
    }

    /// Draw the editor of `team` in `area`, over what is there.
    pub fn draw(
        &self,
        frame: &mut Frame,
        area: Rect,
        team: &Team,
        directory: &TeamDirectory,
        data: &Data,
        now: DateTime<Utc>,
    ) {
        let width = usize::from(area.width.saturating_sub(2));
        let at = self.at(team);
        let (lines, focus) = self.lines(team, directory, data, now, width, at);
        let rows = usize::from(area.height.saturating_sub(2));
        let scroll = focus.end.saturating_sub(rows).min(focus.start);
        let mut block = style::block(format!(" manage team {} ", team.team), true);
        if let Some(Typing { field: Field::Description, text, .. }) = &self.typing {
            let count = text.text().trim().chars().count();
            let style = if count > MAX_TEAM_DESCRIPTION { Style::new().fg(style::ERROR) } else { style::dim() };
            block = block.title_bottom(Line::styled(format!(" {count}/{MAX_TEAM_DESCRIPTION} "), style));
        }
        frame.render_widget(Clear, area);
        let scroll = u16::try_from(scroll).unwrap_or(u16::MAX);
        frame.render_widget(Paragraph::new(lines).block(block).scroll((scroll, 0)), area);
    }

    /// How many lines the editor of `team` takes, its borders included.
    pub fn height(&self, team: &Team, directory: &TeamDirectory, data: &Data) -> u16 {
        let candidates = self.completions(directory, team, data).len().min(CANDIDATES);
        u16::try_from(team.members.len() + FIELDS.len() + candidates + 2).unwrap_or(u16::MAX)
    }

    /// The editor's lines, and those of the row selected or typed in with its completions.
    fn lines(
        &self,
        team: &Team,
        directory: &TeamDirectory,
        data: &Data,
        now: DateTime<Utc>,
        width: usize,
        at: usize,
    ) -> (Vec<Line<'static>>, std::ops::Range<usize>) {
        let mut lines: Vec<Line<'static>> = Vec::new();
        let mut focus = 0..1;
        let selected =
            |line: Line<'static>, row: usize| if row == at { line.patch_style(style::highlight(true)) } else { line };
        for (row, member) in team.members.iter().enumerate() {
            let mut spans = vec![Span::raw("  ")];
            if directory.is_team(member) {
                spans.push(Span::raw(format!("team {member}")).bold());
            } else {
                spans.extend(super::ant(member, data, now));
            }
            if team.lead.as_ref() == Some(member) {
                spans.push(Span::styled(format!(" {}", super::LEAD), Style::new().fg(style::WARNING)));
            }
            if row == at {
                focus = lines.len()..lines.len() + 1;
            }
            lines.push(selected(Line::from(spans), row));
        }
        for (row, field) in FIELDS.iter().enumerate().map(|(n, f)| (team.members.len() + n, *f)) {
            let label = match field {
                Field::Add => "+ add",
                Field::Lead => "lead",
                Field::Owner => "owner",
                Field::Description => "description",
            };
            let mut spans = vec![Span::styled(format!("{label:<12}"), style::dim())];
            let start = lines.len();
            match self.typing.as_ref().filter(|t| t.field == field) {
                Some(typing) => {
                    spans.extend([Span::raw("> "), Span::raw(typing.text.caret(width.saturating_sub(14)))]);
                    lines.push(Line::from(spans));
                    let candidates = self.completions(directory, team, data);
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
                    Field::Add => Span::styled("ants or teams: a or Enter", style::dim()),
                    Field::Lead => super::lead_span(directory, team),
                    Field::Owner => match &team.owner {
                        Some(owner) => Span::raw(owner.to_string()),
                        None => Span::styled("none", style::dim()),
                    },
                    Field::Description => match &team.description {
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

/// Write `change` to `team`.
fn run(team: &UserId, change: Change) -> To {
    To::Do(Action::Run(Job::Team { team: team.clone(), change }, String::new()))
}

/// The change `text` typed in `field` makes to `team` of `directory`, refused as `ants team` refuses it: a human as a
/// member, a lead that is no direct ant member, an owner who is no human, a description too long.
fn change(field: Field, text: &str, directory: &TeamDirectory, team: &Team, data: &Data) -> Result<Change, String> {
    let text = text.trim();
    let name = |word: &str| parse_name(word).map_err(|e| format!("{e}"));
    let change = match field {
        Field::Add => {
            let words = text.split_whitespace().filter(|w| *w != "@");
            let add = words.map(name).collect::<Result<Vec<_>, _>>()?;
            if add.is_empty() {
                return Err("nothing to add: type @ and a name".to_owned());
            }
            Change { add, ..Change::default() }
        }
        Field::Lead => {
            if text.trim_start_matches('@').is_empty() {
                return Ok(Change { lead: Some(None), ..Change::default() });
            }
            let lead = name(text)?;
            if !team.is_member(&lead) || directory.is_team(&lead) {
                return Err(TeamRefusal::NotLeadable { ant: lead, team: team.team.clone() }.to_string());
            }
            Change { lead: Some(Some(lead)), ..Change::default() }
        }
        Field::Owner => {
            let owner = if text.trim_start_matches('@').is_empty() { None } else { Some(name(text)?) };
            Change { owner: Some(owner), ..Change::default() }
        }
        Field::Description => {
            Change { description: Some((!text.is_empty()).then(|| text.to_owned())), ..Change::default() }
        }
    };
    // The CLI's human is one before the humans are read.
    let mut humans = data.humans.clone().unwrap_or_default();
    let me = data.me();
    let _ = humans.add(me.as_str(), ants_proto::HumanEntry::new(me.clone()));
    change.check(&team.team, &humans).map_err(|e| format!("{e:#}"))?;
    Ok(change)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::live::{at, user};
    use crate::tui::command::Tone;
    use crate::tui::exec::{Done, Outcome};
    use crate::tui::model::{Model, Page};
    use crate::tui::reads::Read;
    use crate::tui::team::tests::{backend, team_screen};
    use crate::tui::test_support::{keys, typed};

    fn editor(m: &Model) -> Option<&Editor> {
        let Page::Team(page) = m.page() else { panic!("not the team") };
        page.editor.as_ref()
    }

    fn written(change: Change) -> Option<Action> {
        Some(Action::Run(Job::Team { team: user("backend"), change }, String::new()))
    }

    fn status(m: &Model) -> (String, Tone) {
        let status = m.ui.bar.status.as_ref().expect("a status");
        (status.text.clone(), status.tone)
    }

    /// Move to row `row` from the top.
    fn row(m: &mut Model, row: usize) {
        keys(m, std::iter::once(Key::Home).chain(std::iter::repeat_n(Key::Down, row)));
    }

    fn words(m: &Model) -> Vec<String> {
        let directory = m.data.teams.as_ref().unwrap();
        let team = directory.team(&user("backend")).unwrap();
        editor(m).unwrap().completions(directory, team, &m.data).into_iter().map(|c| c.word).collect()
    }

    #[test]
    fn m_opens_the_editor_which_takes_the_keys_until_esc_closes_it() {
        let mut m = backend();
        let Page::Team(page) = m.page() else { panic!() };
        assert_eq!(page.prompt(), None);
        keys(&mut m, [Key::Char('M')]);
        let Page::Team(page) = m.page() else { panic!() };
        assert_eq!(page.prompt().unwrap(), "↑↓ row · Enter edit · a add · d remove or clear · Esc close");
        assert_eq!(keys(&mut m, [Key::Char('q'), Key::Char('/')]), None, "taken by the editor");
        assert!(!m.ui.bar.focused && editor(&m).is_some());
        keys(&mut m, [Key::Esc]);
        assert!(editor(&m).is_none(), "closed");
        assert!(matches!(m.page(), Page::Team(_)), "still on the team");
    }

    #[test]
    fn m_is_refused_before_the_teams_are_read() {
        let mut m = backend();
        m.data.teams = None;
        keys(&mut m, [Key::Char('M')]);
        assert!(editor(&m).is_none());
        assert_eq!(status(&m), ("team backend is not read yet".to_owned(), Tone::Error));
    }

    #[test]
    fn a_completes_the_ants_and_teams_on_no_team_and_adds_them() {
        let mut m = backend();
        keys(&mut m, [Key::Char('M'), Key::Char('a')]);
        assert_eq!(words(&m), ["@dev-d", "@web"], "not dev-c or boss, on teams, nor eng above it");
        typed(&mut m, "dev-");
        keys(&mut m, [Key::Tab]);
        typed(&mut m, "@w");
        keys(&mut m, [Key::Enter]);
        let typing = &editor(&m).unwrap().typing.as_ref().unwrap().text;
        assert_eq!(typing.text(), "@dev-d @web ", "Enter completes first");
        let change = Change { add: vec![user("dev-d"), user("web")], ..Change::default() };
        assert_eq!(keys(&mut m, [Key::Enter]), written(change));
        assert_eq!(status(&m), ("running…".to_owned(), Tone::Busy));
        assert!(editor(&m).is_some_and(|e| e.typing.is_none()), "the editor stays open");

        keys(&mut m, [Key::Char('a')]);
        typed(&mut m, "bob");
        assert_eq!(keys(&mut m, [Key::Enter]), None);
        assert!(status(&m).0.starts_with("bob is a human: a team's members are ants and teams"), "{:?}", status(&m));
    }

    #[test]
    fn d_removes_a_member_the_lead_too() {
        let mut m = backend();
        keys(&mut m, [Key::Char('M')]);
        row(&mut m, 1);
        assert_eq!(
            keys(&mut m, [Key::Char('d')]),
            written(Change { remove: vec![user("dev-b")], ..Change::default() })
        );
        row(&mut m, 0);
        assert_eq!(
            keys(&mut m, [Key::Char('d')]),
            written(Change { remove: vec![user("dev-a")], ..Change::default() })
        );
        row(&mut m, 2);
        assert_eq!(keys(&mut m, [Key::Char('d')]), None, "nothing to remove on the add row");
    }

    #[test]
    fn the_lead_is_handed_to_a_direct_ant_member_or_cleared() {
        let mut m = backend();
        keys(&mut m, [Key::Char('M')]);
        row(&mut m, 3);
        keys(&mut m, [Key::Enter]);
        assert_eq!(editor(&m).unwrap().typing.as_ref().unwrap().text.text(), "@dev-a", "from what it is");
        keys(&mut m, std::iter::repeat_n(Key::Backspace, 5));
        assert_eq!(words(&m), ["@dev-b"], "the other direct ant members");
        keys(&mut m, [Key::Enter]);
        let set = keys(&mut m, [Key::Enter]);
        assert_eq!(set, written(Change { lead: Some(Some(user("dev-b"))), ..Change::default() }));

        keys(&mut m, [Key::Enter, Key::Ctrl('u')]);
        typed(&mut m, "@dev-c");
        assert_eq!(keys(&mut m, [Key::Enter]), None);
        let refused = "dev-c cannot lead team backend: the lead is one of its direct ant members";
        assert_eq!(status(&m), (refused.to_owned(), Tone::Error));
        keys(&mut m, [Key::Esc]);
        assert_eq!(keys(&mut m, [Key::Char('d')]), written(Change { lead: Some(None), ..Change::default() }));
    }

    #[test]
    fn the_owner_is_a_human_set_or_cleared() {
        let mut m = backend();
        keys(&mut m, [Key::Char('M')]);
        row(&mut m, 4);
        keys(&mut m, [Key::Enter]);
        typed(&mut m, "@dev-a");
        assert_eq!(keys(&mut m, [Key::Enter]), None);
        assert!(status(&m).0.starts_with("dev-a is no human of the bus"), "{:?}", status(&m));
        keys(&mut m, [Key::Ctrl('u')]);
        typed(&mut m, "@b");
        assert_eq!(words(&m), ["@bob"]);
        keys(&mut m, [Key::Enter]);
        let set = keys(&mut m, [Key::Enter]);
        assert_eq!(set, written(Change { owner: Some(Some(user("bob"))), ..Change::default() }));
        assert_eq!(keys(&mut m, [Key::Delete]), written(Change { owner: Some(None), ..Change::default() }));
    }

    #[test]
    fn the_description_is_at_most_1000_characters() {
        let mut m = backend();
        keys(&mut m, [Key::Char('M')]);
        row(&mut m, 5);
        keys(&mut m, [Key::Enter]);
        assert_eq!(editor(&m).unwrap().typing.as_ref().unwrap().text.text(), "The API and its storage.");
        typed(&mut m, &" x".repeat(500));
        assert_eq!(keys(&mut m, [Key::Enter]), None);
        assert!(status(&m).0.contains("1000"), "{:?}", status(&m));
        keys(&mut m, [Key::Ctrl('u')]);
        typed(&mut m, " Billing ");
        let set = keys(&mut m, [Key::Enter]);
        assert_eq!(set, written(Change { description: Some(Some("Billing".to_owned())), ..Change::default() }));
        let cleared = keys(&mut m, [Key::Char('d')]);
        assert_eq!(cleared, written(Change { description: Some(None), ..Change::default() }));
    }

    #[test]
    fn a_write_refused_shows_on_the_status_line_and_the_rows_follow_the_directory_read_back() {
        let mut m = backend();
        keys(&mut m, [Key::Char('M')]);
        let refused = anyhow::anyhow!("dev-c is on team web already; each ant and team is on one team at most");
        m.outcome(Outcome { result: Err(refused), text: String::new(), quoting: None });
        assert_eq!(status(&m).1, Tone::Error);
        assert!(status(&m).0.starts_with("dev-c is on team web already"));
        assert!(editor(&m).is_some(), "the editor stays");
        let mut directory = m.data.teams.clone().unwrap();
        directory.add(&user("backend"), &user("dev-d")).unwrap();
        m.read(Read::Teams(Ok(Some(directory))), at(2));
        let shown = team_screen(&m, 100, 30);
        assert!(shown.contains("dev-d") && shown.contains("manage team backend"), "{shown}");
        let done = Done { line: "team backend: added dev-d".to_owned(), teams: true, ..Done::default() };
        m.outcome(Outcome { result: Ok(done), text: String::new(), quoting: None });
        assert_eq!(status(&m), ("team backend: added dev-d".to_owned(), Tone::Done));
    }
}
