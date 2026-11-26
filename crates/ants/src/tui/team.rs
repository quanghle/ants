//! A team's view: a header with the team, the teams above it, its description, lead and owner, over its members as a
//! tree down through its member teams, as `ants team show` prints them: each ant with its state's mark, its face and
//! its state in the colony, the leads marked, each member team with its lead. Enter opens the member selected, `a`
//! writes to the team (its lead) in the command bar, `M` opens the team's editor ([`editor`]), `n` the form creating
//! a team ([`create`]), which the nav's `+ new team` shows alone, and `D` deletes the team once `y` confirms.

use ants_proto::{ColonyEntry, Team, TeamDirectory, UserId};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use super::armed::Armed;
use super::exec::Job;
use super::model::{Action, Data, Key, Model, To};
use super::select::{PAGE, Selection};
use super::{nav, style};
use crate::teams::{self, Member};

mod create;
mod editor;

/// The mark of a team's lead among its members.
const LEAD: &str = "★ lead";
/// Most lines of the description the header shows.
const DESCRIPTION_LINES: usize = 3;

#[derive(Debug)]
pub struct Page {
    /// The team shown; none for the nav's `+ new team`, which shows the form creating one alone.
    pub team: Option<UserId>,
    /// The member selected in the tree, by name.
    selected: Selection<UserId>,
    /// The editor `M` opens.
    editor: Option<editor::Editor>,
    /// The form creating a team, which `n` opens.
    creating: Option<create::Form>,
    /// The deletion of the team `D` asked for, which `y` confirms.
    armed: Armed<UserId>,
}

/// A row of the members tree: how deep it is and the member.
struct Row {
    depth: usize,
    member: Member,
}

impl Row {
    fn name(&self) -> &UserId {
        self.member.name()
    }
}

impl Page {
    pub fn new(team: UserId) -> Self {
        Self { team: Some(team), creating: None, ..Self::creating() }
    }

    /// The form creating a team, alone.
    pub fn creating() -> Self {
        let creating = Some(create::Form::default());
        Self { team: None, selected: Selection::default(), editor: None, creating, armed: Armed::default() }
    }

    /// The team as read, if it is.
    fn entry<'d>(&self, data: &'d Data) -> Option<(&'d TeamDirectory, &'d Team)> {
        let directory = data.teams.as_ref()?;
        Some((directory, directory.team(self.team.as_ref()?)?))
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
        let tree = teams::tree(directory, team, &data.live.colony, now);
        let mut rows = Vec::new();
        flatten(tree, 0, &mut rows);
        rows
    }

    fn ids(&self, data: &Data, now: DateTime<Utc>) -> Vec<UserId> {
        self.rows(data, now).iter().map(|r| r.name().clone()).collect()
    }

    const DELETE_KEYS: &str = "y delete the team, its members then on no team · any other key keeps it";

    /// The keys of the deletion `D` asked to confirm, or of the form or the editor, which take every key, when open.
    pub fn prompt(&self) -> Option<&'static str> {
        if self.armed.get().is_some() {
            return Some(Self::DELETE_KEYS);
        }
        match (&self.creating, &self.editor) {
            (Some(form), _) => Some(form.keys()),
            (None, editor) => editor.as_ref().map(editor::Editor::keys),
        }
    }

    /// Whether the view waits for the `y` confirming the deletion `D` asked for.
    pub fn armed(&self) -> bool {
        self.armed.get().is_some()
    }

    /// Drop the deletion asked for, if any: whether there was one.
    pub fn disarm(&mut self) -> bool {
        self.armed.disarm()
    }

    /// Take `key` for the form or the editor when one is open: what to do, if anything. Shown alone, the form leaves
    /// the keys it has no use for, Esc among them, to the view.
    pub fn write(&mut self, key: Key, data: &Data) -> Option<Option<To>> {
        if let Some(form) = self.creating.as_mut() {
            if self.team.is_none() && !form.takes(key) {
                return None;
            }
            return Some(match form.key(key, data) {
                editor::Edited::Kept => None,
                editor::Edited::Closed => {
                    if self.team.is_some() {
                        self.creating = None;
                    }
                    None
                }
                editor::Edited::To(to) => Some(*to),
            });
        }
        let team = self.team.clone()?;
        let editor = self.editor.as_mut()?;
        Some(match editor.key(key, &team, data) {
            editor::Edited::Kept => None,
            editor::Edited::Closed => {
                self.editor = None;
                None
            }
            editor::Edited::To(to) => Some(*to),
        })
    }

    /// Take `line` pasted for the form or the editor when one is open, into the row typed in if any.
    pub fn paste(&mut self, line: &str) -> bool {
        match (self.creating.as_mut(), self.editor.as_mut()) {
            (Some(form), _) => form.paste(line),
            (None, editor) => editor.is_some_and(|editor| editor.paste(line)),
        }
    }

    pub fn key(&mut self, key: Key, data: &Data, now: DateTime<Utc>) -> Option<To> {
        let team = self.team.clone()?;
        match (key, self.armed.take()) {
            (Key::Char('y'), Some(team)) => return Some(To::Do(Action::Run(Job::TeamDelete { team }, String::new()))),
            (Key::Char('D'), _) => {
                if self.entry(data).is_none() {
                    return Some(To::Refused(format!("team {team} is not read yet")));
                }
                self.armed.arm(Some(team));
                return None;
            }
            _ => {}
        }
        match key {
            Key::Enter => {
                let rows = self.rows(data, now);
                let row = rows.get(self.selected.at(&self.ids(data, now)))?;
                return Some(match &row.member {
                    Member::Ant { name, .. } => To::Ant(name.clone()),
                    Member::Team { name, .. } => To::Team(name.clone()),
                });
            }
            Key::Char('a') => return Some(To::Bar(format!("@{team} "))),
            Key::Char('M') => {
                if self.entry(data).is_none() {
                    return Some(To::Refused(format!("team {team} is not read yet")));
                }
                self.editor = Some(editor::Editor::default());
            }
            Key::Char('n') => {
                if data.teams.is_none() {
                    return Some(To::Refused("the teams are not read yet".to_owned()));
                }
                self.creating = Some(create::Form::default());
            }
            _ => _ = self.selected.step(key, &self.ids(data, now), Some(PAGE)),
        }
        None
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, model: &Model, now: DateTime<Utc>, focused: bool) {
        let data = &model.data;
        let form = self.creating.as_ref().filter(|_| data.teams.is_some());
        let Some(id) = &self.team else {
            if let Some(form) = form {
                return form.draw(frame, area, data, focused);
            }
            let block = style::block(" new team ".to_owned(), focused);
            let line = Line::styled("the teams are not read yet", style::dim());
            return frame.render_widget(Paragraph::new(line).block(block), area);
        };
        let Some((directory, team)) = self.entry(data) else {
            let text = format!("no team {id}");
            let block = style::block(format!(" team {id} "), focused);
            frame.render_widget(Paragraph::new(Line::styled(text, style::dim())).block(block), area);
            if let Some(form) = form {
                let inner = Rect { x: area.x, y: area.y + 1, width: area.width.min(72), height: form.height(data) };
                form.draw(frame, inner.intersection(area), data, focused);
            }
            return;
        };
        let width = usize::from(area.width.saturating_sub(2));
        let about = about(directory, team, width);
        let height = u16::try_from(about.len() + 2).unwrap_or(u16::MAX);
        let [head, list] = Layout::vertical([Constraint::Length(height), Constraint::Min(1)]).areas(area);
        let within: String = directory.ancestors(id).iter().map(|t| format!(" · within {}", t.team)).collect();
        let keys = Line::styled(" M manage · a ask · n new · D delete ", style::dim()).right_aligned();
        let block = style::block(format!(" team {id}{within} "), false).title_bottom(keys);
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
        let ants = directory.ants_under(id).len();
        let title = format!(" members · {ants} {} ", if ants == 1 { "ant" } else { "ants" });
        let room = usize::from(list.height.saturating_sub(2)).max(1);
        let top = u16::try_from((selected + 1).saturating_sub(room)).unwrap_or(u16::MAX);
        frame.render_widget(Paragraph::new(lines).block(style::block(title, focused)).scroll((top, 0)), list);
        let room = area.bottom().saturating_sub(list.y);
        if let Some(form) = form {
            let (width, height) = (list.width.min(72), form.height(data).min(room));
            form.draw(frame, Rect { x: list.x, y: list.y, width, height }, data, focused);
        } else if let Some(editor) = &self.editor {
            let width = list.width.min(64);
            let height = editor.height(team, directory, data).min(room);
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
    let lead = lead_span(directory, team);
    let owner = team.owner.as_ref().map_or_else(|| "none".to_owned(), UserId::to_string);
    let subteams = directory.subteams(team).len();
    let mut facts = vec![Span::styled("lead ", style::dim()), lead, Span::styled(" · owner ", style::dim())];
    facts.push(Span::raw(owner));
    if subteams > 0 {
        let teams = if subteams == 1 { "team" } else { "teams" };
        facts.push(Span::styled(format!(" · {subteams} {teams} beneath"), style::dim()));
    }
    lines.push(Line::from(facts));
    if let Err(refusal) = directory.lead_of(&team.team) {
        lines.push(Line::styled(refusal.to_string(), Style::new().fg(style::ERROR)));
    }
    lines
}

/// The lead of `team` as the team view shows it: its own, one taken from a team above it dimmed with that team's
/// name, or none in the colour of an error.
fn lead_span(directory: &TeamDirectory, team: &Team) -> Span<'static> {
    let label = teams::lead_label(directory, team);
    match directory.lead_source(&team.team) {
        Some(source) if source.team == team.team => Span::raw(label),
        Some(_) => Span::styled(label, style::dim()),
        None => Span::styled(label, Style::new().fg(style::ERROR)),
    }
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
        Member::Team { name, lead, lead_from, .. } => {
            spans.push(Span::styled(format!("team {name}"), Style::new().add_modifier(Modifier::BOLD)));
            let lead = match (lead, lead_from) {
                (Some(l), Some(from)) => format!("lead {l}, from team {from}"),
                (Some(l), None) => format!("lead {l}"),
                (None, _) => "no lead, nor above it".to_owned(),
            };
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
