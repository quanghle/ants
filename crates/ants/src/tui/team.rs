//! A team's view: a header with the team, the teams above it, its description, lead and owner, over its members as a
//! tree down through its member teams, as `ants team show` prints them: each ant with its state's mark, its face and
//! its state in the colony, the leads marked, each member team with its lead. Enter opens the member selected, `a`
//! writes to the team (its lead) in the command bar, and `M` opens the team's editor ([`editor`]).

use ants_proto::{ColonyEntry, Team, TeamDirectory, UserId};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use super::model::{Data, Key, Model, To};
use super::select::{PAGE, Selection};
use super::{nav, style};
use crate::teams::{self, Member};

mod editor;

/// The mark of a team's lead among its members.
const LEAD: &str = "★ lead";
/// Most lines of the description the header shows.
const DESCRIPTION_LINES: usize = 3;

#[derive(Debug)]
pub struct Page {
    pub team: UserId,
    /// The member selected in the tree, by name.
    selected: Selection<UserId>,
    /// The editor `M` opens.
    editor: Option<editor::Editor>,
}

/// A row of the members tree: how deep it is and the member.
struct Row {
    depth: usize,
    member: Member,
}

impl Row {
    fn name(&self) -> &UserId {
        match &self.member {
            Member::Ant { name, .. } | Member::Team { name, .. } => name,
        }
    }
}

impl Page {
    pub fn new(team: UserId) -> Self {
        Self { team, selected: Selection::default(), editor: None }
    }

    /// The team as read, if it is.
    fn entry<'d>(&self, data: &'d Data) -> Option<(&'d TeamDirectory, &'d Team)> {
        let directory = data.teams.as_ref()?;
        Some((directory, directory.team(&self.team)?))
    }

    /// The members tree, one row each, depth first.
    fn rows(&self, data: &Data, now: DateTime<Utc>) -> Vec<Row> {
        fn flatten(members: Vec<Member>, depth: usize, out: &mut Vec<Row>) {
            for member in members {
                let under = match &member {
                    Member::Team { members, .. } => members.clone(),
                    Member::Ant { .. } => Vec::new(),
                };
                out.push(Row { depth, member });
                flatten(under, depth + 1, out);
            }
        }
        let Some((directory, team)) = self.entry(data) else { return Vec::new() };
        let tree = teams::members(directory, team, &|ant| teams::state_of(&data.live.colony, ant, now));
        let mut rows = Vec::new();
        flatten(tree, 0, &mut rows);
        rows
    }

    fn ids(&self, data: &Data, now: DateTime<Utc>) -> Vec<UserId> {
        self.rows(data, now).iter().map(|r| r.name().clone()).collect()
    }

    /// The keys of the editor, which takes every key, when it is open.
    pub fn prompt(&self) -> Option<&'static str> {
        self.editor.as_ref().map(editor::Editor::keys)
    }

    /// Take `key` for the editor when it is open: what to do, if anything.
    pub fn write(&mut self, key: Key, data: &Data) -> Option<Option<To>> {
        let editor = self.editor.as_mut()?;
        Some(match editor.key(key, &self.team, data) {
            editor::Edited::Kept => None,
            editor::Edited::Closed => {
                self.editor = None;
                None
            }
            editor::Edited::To(to) => Some(*to),
        })
    }

    /// Take `line` pasted for the editor when it is open, into the row typed in if any.
    pub fn paste(&mut self, line: &str) -> bool {
        self.editor.as_mut().is_some_and(|editor| editor.paste(line))
    }

    pub fn key(&mut self, key: Key, data: &Data, now: DateTime<Utc>) -> Option<To> {
        match key {
            Key::Enter => {
                let rows = self.rows(data, now);
                let row = rows.get(self.selected.at(&self.ids(data, now)))?;
                return Some(match &row.member {
                    Member::Ant { name, .. } => To::Ant(name.clone()),
                    Member::Team { name, .. } => To::Team(name.clone()),
                });
            }
            Key::Char('a') => return Some(To::Bar(format!("@{} ", self.team))),
            Key::Char('M') => {
                if self.entry(data).is_none() {
                    return Some(To::Refused(format!("team {} is not read yet", self.team)));
                }
                self.editor = Some(editor::Editor::default());
            }
            _ => _ = self.selected.step(key, &self.ids(data, now), Some(PAGE)),
        }
        None
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, model: &Model, now: DateTime<Utc>, focused: bool) {
        let data = &model.data;
        let Some((directory, team)) = self.entry(data) else {
            let text = format!("no team {}", self.team);
            let block = style::block(format!(" team {} ", self.team), focused);
            frame.render_widget(Paragraph::new(Line::styled(text, style::dim())).block(block), area);
            return;
        };
        let width = usize::from(area.width.saturating_sub(2));
        let about = about(directory, team, width);
        let height = u16::try_from(about.len() + 2).unwrap_or(u16::MAX);
        let [head, list] = Layout::vertical([Constraint::Length(height), Constraint::Min(1)]).areas(area);
        let within: String = directory.ancestors(&self.team).iter().map(|t| format!(" · within {}", t.team)).collect();
        let keys = Line::styled(" M manage · a ask ", style::dim()).right_aligned();
        let block = style::block(format!(" team {}{within} ", self.team), false).title_bottom(keys);
        frame.render_widget(Paragraph::new(about).block(block), head);

        let rows = self.rows(data, now);
        let selected = self.selected.at(&rows.iter().map(|r| r.name().clone()).collect::<Vec<_>>());
        let inner = usize::from(list.width.saturating_sub(2));
        let lines: Vec<Line<'static>> = rows
            .iter()
            .enumerate()
            .map(|(at, row)| {
                let line = style::clip(tree_line(row, data, now), inner);
                if at == selected { line.patch_style(style::highlight(focused)) } else { line }
            })
            .collect();
        let ants = directory.ants_under(&self.team).len();
        let title = format!(" members · {ants} {} ", if ants == 1 { "ant" } else { "ants" });
        let room = usize::from(list.height.saturating_sub(2)).max(1);
        let top = u16::try_from((selected + 1).saturating_sub(room)).unwrap_or(u16::MAX);
        frame.render_widget(Paragraph::new(lines).block(style::block(title, focused)).scroll((top, 0)), list);
        if let Some(editor) = &self.editor {
            let width = list.width.min(64);
            let height = editor.height(team, directory, data).min(area.bottom().saturating_sub(list.y));
            editor.draw(frame, Rect { x: list.x, y: list.y, width, height }, team, directory, data, now);
        }
    }
}

/// The team's description, wrapped to `width` and cut to [`DESCRIPTION_LINES`], then its lead and owner, and a line
/// saying messages to it are refused while it has no lead.
fn about(directory: &TeamDirectory, team: &Team, width: usize) -> Vec<Line<'static>> {
    let mut lines: Vec<Line<'static>> = match &team.description {
        Some(text) => {
            let mut wrapped = style::rows(text, width.max(1));
            if wrapped.len() > DESCRIPTION_LINES {
                wrapped.truncate(DESCRIPTION_LINES);
                let last = wrapped.last_mut().expect("a line");
                if last.chars().count() >= width {
                    last.pop();
                }
                last.push('…');
            }
            wrapped.into_iter().map(Line::raw).collect()
        }
        None => vec![Line::styled("no description", style::dim())],
    };
    let lead = match &team.lead {
        Some(lead) => Span::raw(lead.to_string()),
        None => Span::styled(teams::lead_label(team), Style::new().fg(style::ERROR)),
    };
    let owner = team.owner.as_ref().map_or_else(|| "none".to_owned(), UserId::to_string);
    let subteams = directory.subteams(team).len();
    let mut facts = vec![Span::styled("lead ", style::dim()), lead, Span::styled(" · owner ", style::dim())];
    facts.push(Span::raw(owner));
    if subteams > 0 {
        let teams = if subteams == 1 { "team" } else { "teams" };
        facts.push(Span::styled(format!(" · {subteams} {teams} beneath"), style::dim()));
    }
    lines.push(Line::from(facts));
    if team.leaderless() {
        let refusal = ants_proto::TeamRefusal::Leaderless { team: team.team.clone() };
        lines.push(Line::styled(refusal.to_string(), Style::new().fg(style::ERROR)));
    }
    lines
}

/// A row of the members tree: indented by its depth, an ant with its state's mark, its face, its name, the lead's mark
/// and its state; a team with its lead.
fn tree_line(row: &Row, data: &Data, now: DateTime<Utc>) -> Line<'static> {
    let mut spans = vec![Span::raw("  ".repeat(row.depth))];
    match &row.member {
        Member::Ant { name, lead, state } => {
            spans.extend(ant(name, data, now));
            if *lead {
                spans.push(Span::styled(format!(" {LEAD}"), Style::new().fg(style::WARNING)));
            }
            spans.push(Span::styled(format!("  {state}"), style::dim()));
        }
        Member::Team { name, lead, .. } => {
            spans.push(Span::styled(format!("team {name}"), Style::new().add_modifier(Modifier::BOLD)));
            let lead = lead.as_ref().map_or_else(|| "no lead".to_owned(), |l| format!("lead {l}"));
            spans.push(Span::styled(format!("  {lead}"), style::dim()));
        }
    }
    Line::from(spans)
}

/// An ant as the nav shows it: its state's mark, its face and its name.
fn ant(name: &UserId, data: &Data, now: DateTime<Utc>) -> Vec<Span<'static>> {
    let entries: Vec<&ColonyEntry> = data.live.colony.iter().filter(|e| e.ant == *name).collect();
    let (mark, color) = nav::marker(&entries, now);
    vec![
        Span::styled(mark, Style::new().fg(color)),
        Span::raw(" "),
        data.looks.face(name),
        Span::raw(format!(" {name}")),
    ]
}

#[cfg(test)]
mod tests;
