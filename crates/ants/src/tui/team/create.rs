//! The form creating a team, which `n` on a team's view opens over its members tree and the nav's `+ new team` shows
//! alone: a row each for the team's name, its lead (an ant on no team, else none), its other members (ants or teams on
//! no team), its owner (a human) and its description, then the row creating it, which runs [`Job::TeamCreate`],
//! writing the directory as `ants team create` does.

use ants_proto::{MAX_TEAM_DESCRIPTION, TeamDirectory, UserId};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Style, Stylize as _};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use super::editor::{Edited, candidates, free, humans, known_humans};
use crate::teams::change::Create;
use crate::teams::parse_name;
use crate::text::excerpt;
use crate::tui::complete::{Candidate, Pick};
use crate::tui::exec::Job;
use crate::tui::input::Input;
use crate::tui::model::{Action, Data, Key, To};
use crate::tui::select::step;
use crate::tui::style;

/// Most completions shown under the row typed in.
const CANDIDATES: usize = 5;

/// A row of the form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Row {
    Name,
    Lead,
    Members,
    Owner,
    Description,
    Create,
}

const ROWS: [Row; 6] = [Row::Name, Row::Lead, Row::Members, Row::Owner, Row::Description, Row::Create];

/// What is typed in a row, and which of its completions is picked.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Typing {
    row: Row,
    text: Input,
    pick: Pick,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Form {
    /// The row selected.
    at: usize,
    typing: Option<Typing>,
    name: Option<UserId>,
    lead: Option<UserId>,
    members: Vec<UserId>,
    owner: Option<UserId>,
    description: Option<String>,
}

impl Form {
    pub fn keys(&self) -> &'static str {
        match &self.typing {
            Some(Typing { row: Row::Name, .. }) => "type the team's name · Enter set · Esc cancel",
            Some(Typing { row: Row::Description, .. }) => "type the description · Enter set · Esc cancel",
            Some(_) => "type @ and a name · ↑↓ Tab complete · Enter set · Esc cancel",
            None => "↑↓ row · Enter edit, or create on its row · d clear · Esc close",
        }
    }

    /// Whether the form has a use for `key`: every key while a row is typed in, else those moving between its rows,
    /// editing, clearing or creating.
    pub fn takes(&self, key: Key) -> bool {
        self.typing.is_some()
            || matches!(
                key,
                Key::Up
                    | Key::Down
                    | Key::Char('k' | 'j' | 'd')
                    | Key::Home
                    | Key::End
                    | Key::Enter
                    | Key::Delete
                    | Key::Backspace
            )
    }

    fn row(&self) -> Row {
        ROWS[self.at.min(ROWS.len() - 1)]
    }

    pub fn key(&mut self, key: Key, data: &Data) -> Edited {
        let Some(directory) = data.teams.as_ref() else { return Edited::Closed };
        if self.typing.is_some() {
            return self.type_key(key, directory, data);
        }
        match key {
            Key::Esc => return Edited::Closed,
            Key::Enter if self.row() == Row::Create => return Edited::To(Box::new(self.create(data))),
            Key::Enter => self.start(self.row()),
            Key::Char('d') | Key::Delete | Key::Backspace => match self.row() {
                Row::Name => self.name = None,
                Row::Lead => self.lead = None,
                Row::Members => self.members.clear(),
                Row::Owner => self.owner = None,
                Row::Description => self.description = None,
                Row::Create => {}
            },
            _ => {
                if let Some(to) = step(key, self.at, None) {
                    self.at = to.min(ROWS.len() - 1);
                }
            }
        }
        Edited::Kept
    }

    /// Type `line` pasted in the row typed in, if any; the form takes it anyway.
    pub fn paste(&mut self, line: &str) -> bool {
        if let Some(typing) = &mut self.typing {
            typing.text.paste(line);
            typing.pick = Pick::default();
        }
        true
    }

    /// Start typing in `row`, from what it holds.
    fn start(&mut self, row: Row) {
        let at = |user: &Option<UserId>| user.as_ref().map_or_else(|| "@".to_owned(), |u| format!("@{u}"));
        let text = match row {
            Row::Name => self.name.as_ref().map_or_else(String::new, UserId::to_string),
            Row::Lead => at(&self.lead),
            Row::Members => self.members.iter().map(|m| format!("@{m} ")).collect::<String>() + "@",
            Row::Owner => at(&self.owner),
            Row::Description => self.description.clone().unwrap_or_default(),
            Row::Create => return,
        };
        self.typing = Some(Typing { row, text: Input::from(text.as_str()), pick: Pick::default() });
    }

    fn type_key(&mut self, key: Key, directory: &TeamDirectory, data: &Data) -> Edited {
        let candidates = self.completions(directory, data);
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
                _ => return self.edit_key(key, directory, data),
            }
            return Edited::Kept;
        }
        self.edit_key(key, directory, data)
    }

    fn edit_key(&mut self, key: Key, directory: &TeamDirectory, data: &Data) -> Edited {
        let typing = self.typing.as_mut().expect("typing");
        match key {
            Key::Esc => self.typing = None,
            Key::Enter => {
                let (row, text) = (typing.row, typing.text.text());
                if let Err(why) = self.set(row, text.trim(), directory, data) {
                    return Edited::To(Box::new(To::Refused(why)));
                }
                self.typing = None;
            }
            key => {
                if typing.text.key(key) {
                    typing.pick = Pick::default();
                }
            }
        }
        Edited::Kept
    }

    /// Set `row` to `text`, refused as `ants team create` refuses it: a name a team, an ant or a human holds, a human
    /// as the lead or a member, an owner who is no human, a description too long.
    fn set(&mut self, row: Row, text: &str, directory: &TeamDirectory, data: &Data) -> Result<(), String> {
        let name = |word: &str| parse_name(word).map_err(|e| format!("{e}"));
        let one = |text: &str| match text.trim_start_matches('@') {
            "" => Ok(None),
            _ => name(text).map(Some),
        };
        let humans = known_humans(data);
        match row {
            Row::Name => {
                let team = one(text)?;
                if let Some(team) = &team {
                    let ant = data.live.colony.iter().any(|e| e.ant == *team);
                    if directory.is_team(team) || humans.contains(team) || ant {
                        return Err(ants_proto::TeamRefusal::NameTaken { name: team.clone() }.to_string());
                    }
                }
                self.name = team;
            }
            Row::Lead => {
                let lead = one(text)?;
                if let Some(lead) = &lead {
                    crate::teams::change::check_not_human(lead, &humans).map_err(|e| format!("{e:#}"))?;
                }
                self.lead = lead;
            }
            Row::Members => {
                let words = text.split_whitespace().filter(|w| *w != "@");
                let members = words.map(name).collect::<Result<Vec<_>, _>>()?;
                for member in &members {
                    crate::teams::change::check_not_human(member, &humans).map_err(|e| format!("{e:#}"))?;
                }
                self.members = members;
            }
            Row::Owner => {
                let owner = one(text)?;
                if let Some(owner) = &owner {
                    crate::teams::change::check_owner(owner, &humans).map_err(|e| format!("{e:#}"))?;
                }
                self.owner = owner;
            }
            Row::Description => {
                let count = text.chars().count();
                if count > MAX_TEAM_DESCRIPTION {
                    return Err(format!("the description has {count} characters, over {MAX_TEAM_DESCRIPTION}"));
                }
                self.description = (!text.is_empty()).then(|| text.to_owned());
            }
            Row::Create => {}
        }
        Ok(())
    }

    /// Create the team as the form has it, refused as `ants team create` refuses it before writing.
    fn create(&self, data: &Data) -> To {
        let Some(team) = self.name.clone() else { return To::Refused("no name for the team: type one".to_owned()) };
        let create = Create {
            team,
            lead: self.lead.clone(),
            members: self.members.clone(),
            owner: self.owner.clone(),
            description: self.description.clone(),
        };
        if let Err(e) = create.check(data.me(), &known_humans(data)) {
            return To::Refused(format!("{e:#}"));
        }
        To::Do(Action::Run(Job::TeamCreate(create), String::new()))
    }

    /// The completions of the word before the cursor, by name: for the lead, the colony's ants on no team; for the
    /// members, those and the teams on no team; for the owner, the humans.
    fn completions(&self, directory: &TeamDirectory, data: &Data) -> Vec<Candidate> {
        let Some(typing) = self.typing.as_ref().filter(|t| !t.pick.closed) else { return Vec::new() };
        let (_, word) = typing.text.word();
        if word.is_empty() {
            return Vec::new();
        }
        let names = match typing.row {
            Row::Lead => free(directory, data, None).into_iter().filter(|(_, what)| *what == "ant").collect(),
            Row::Members => free(directory, data, None)
                .into_iter()
                .filter(|(name, _)| self.lead.as_ref().is_none_or(|l| l.as_str() != name))
                .collect(),
            Row::Owner => humans(data),
            Row::Name | Row::Description | Row::Create => return Vec::new(),
        };
        candidates(&word, names)
    }

    /// How many lines the form takes, its borders included.
    pub fn height(&self, data: &Data) -> u16 {
        let candidates = data.teams.as_ref().map_or(0, |d| self.completions(d, data).len().min(CANDIDATES));
        u16::try_from(ROWS.len() + candidates + 2).unwrap_or(u16::MAX)
    }

    /// Draw the form in `area`, over what is there.
    pub fn draw(&self, frame: &mut Frame, area: Rect, data: &Data, focused: bool) {
        let width = usize::from(area.width.saturating_sub(2));
        let mut block = style::block(" new team ".to_owned(), focused);
        if let Some(Typing { row: Row::Description, text, .. }) = &self.typing {
            let count = text.text().trim().chars().count();
            let style = if count > MAX_TEAM_DESCRIPTION { Style::new().fg(style::ERROR) } else { style::dim() };
            block = block.title_bottom(Line::styled(format!(" {count}/{MAX_TEAM_DESCRIPTION} "), style));
        }
        let lines = self.lines(data, width, focused);
        frame.render_widget(Clear, area);
        frame.render_widget(Paragraph::new(lines).block(block), area);
    }

    fn lines(&self, data: &Data, width: usize, focused: bool) -> Vec<Line<'static>> {
        let mut lines = Vec::new();
        let none = || Span::styled("none", style::dim());
        let user = |u: &Option<UserId>| u.as_ref().map_or_else(none, |u| Span::raw(u.to_string()));
        for (at, row) in ROWS.iter().enumerate() {
            let label = match row {
                Row::Name => "name",
                Row::Lead => "lead",
                Row::Members => "members",
                Row::Owner => "owner",
                Row::Description => "description",
                Row::Create => "",
            };
            let mut spans = vec![Span::styled(format!("{label:<12}"), style::dim())];
            if let Some(typing) = self.typing.as_ref().filter(|t| t.row == *row) {
                spans.extend([Span::raw("> "), Span::raw(typing.text.caret(width.saturating_sub(14)))]);
                lines.push(Line::from(spans));
                let candidates = data.teams.as_ref().map_or_else(Vec::new, |d| self.completions(d, data));
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
                continue;
            }
            spans.push(match row {
                Row::Name => match &self.name {
                    Some(name) => Span::raw(name.to_string()).bold(),
                    None => Span::styled("type one: Enter", style::dim()),
                },
                Row::Lead => match &self.lead {
                    Some(lead) => Span::raw(lead.to_string()),
                    None => Span::styled("none: the lead of the team above it, once under one", style::dim()),
                },
                Row::Members if self.members.is_empty() => none(),
                Row::Members => Span::raw(self.members.iter().map(UserId::as_str).collect::<Vec<_>>().join(", ")),
                Row::Owner => user(&self.owner),
                Row::Description => match &self.description {
                    Some(text) => Span::raw(excerpt(text, width.saturating_sub(13).max(1))),
                    None => none(),
                },
                Row::Create => Span::raw("▸ create the team").bold(),
            });
            let line = Line::from(spans);
            let selected = at == self.at.min(ROWS.len() - 1) && self.typing.is_none();
            lines.push(if selected { line.patch_style(style::highlight(focused)) } else { line });
        }
        lines
    }
}

impl Typing {
    /// Put `word` in place of the word at the cursor; after it, another member may follow.
    fn complete(&mut self, word: &str) {
        let more = self.row == Row::Members;
        self.text.complete(word, more);
        if more {
            self.pick = Pick::default();
        } else {
            self.pick.closed = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::live::user;
    use crate::tui::command::Tone;
    use crate::tui::model::{Focus, Model, Page};
    use crate::tui::team::tests::{backend, team_screen};
    use crate::tui::test_support::{keys, typed};

    fn form(m: &Model) -> Option<&Form> {
        let Page::Team(page) = m.page() else { panic!("not the team") };
        page.creating.as_ref()
    }

    fn status(m: &Model) -> (String, Tone) {
        let status = m.ui.bar.status.as_ref().expect("a status");
        (status.text.clone(), status.tone)
    }

    /// Type `text` in row `row` of the form and set it with Enter.
    fn fill(m: &mut Model, row: usize, text: &str) {
        let to = std::iter::once(Key::Home).chain(std::iter::repeat_n(Key::Down, row));
        keys(m, to.chain([Key::Enter, Key::Ctrl('u')]));
        typed(m, text);
        keys(m, [Key::Enter]);
    }

    fn words(m: &Model) -> Vec<String> {
        let directory = m.data.teams.as_ref().unwrap();
        form(m).unwrap().completions(directory, &m.data).into_iter().map(|c| c.word).collect()
    }

    #[test]
    fn n_opens_the_form_which_creates_the_team_as_typed() {
        let mut m = backend();
        keys(&mut m, [Key::Char('n')]);
        assert_eq!(form(&m).unwrap().keys(), "↑↓ row · Enter edit, or create on its row · d clear · Esc close");
        assert!(team_screen(&m, 100, 30).contains("new team"), "over the members tree");
        keys(&mut m, [Key::End, Key::Enter]);
        assert_eq!(status(&m), ("no name for the team: type one".to_owned(), Tone::Error));

        fill(&mut m, 0, "dev-a");
        assert!(status(&m).0.contains("dev-a"), "an ant holds the name: {:?}", status(&m));
        assert!(form(&m).unwrap().name.is_none());
        keys(&mut m, [Key::Esc]);
        fill(&mut m, 0, "ops");
        keys(&mut m, [Key::Home, Key::Down, Key::Enter, Key::Ctrl('u')]);
        typed(&mut m, "@dev");
        assert_eq!(words(&m), ["@dev-d"], "the ants on no team");
        keys(&mut m, [Key::Tab, Key::Enter]);
        assert!(form(&m).unwrap().typing.is_none());
        fill(&mut m, 2, "@web @bob");
        assert!(status(&m).0.starts_with("bob is a human"), "{:?}", status(&m));
        keys(&mut m, [Key::Esc]);
        fill(&mut m, 2, "@web");
        fill(&mut m, 3, "@bob");
        fill(&mut m, 4, " Runs the services. ");
        let shown = team_screen(&m, 100, 30);
        assert!(shown.contains("ops") && shown.contains("Runs the services."), "{shown}");

        let create = Create {
            team: user("ops"),
            lead: Some(user("dev-d")),
            members: vec![user("web")],
            owner: Some(user("bob")),
            description: Some("Runs the services.".to_owned()),
        };
        let created = keys(&mut m, [Key::End, Key::Enter]);
        assert_eq!(created, Some(Action::Run(Job::TeamCreate(create), String::new())), "{:?}", m.ui.bar.status);
        keys(&mut m, [Key::Esc]);
        assert!(form(&m).is_none(), "closed");
        assert!(matches!(m.page(), Page::Team(_)), "still on the team");
    }

    #[test]
    fn the_nav_s_new_team_shows_the_form_alone_leaving_esc_to_the_view() {
        let mut m = backend();
        m.focus = Focus::Nav;
        keys(&mut m, [Key::Home]);
        while m.nav != crate::tui::nav::Item::NewTeam {
            keys(&mut m, [Key::Down]);
        }
        keys(&mut m, [Key::Enter]);
        assert!(form(&m).is_some() && m.focus == Focus::View);
        let shown = team_screen(&m, 80, 12);
        assert!(shown.contains("new team") && shown.contains("name"), "{shown}");
        keys(&mut m, [Key::Esc]);
        assert_eq!(m.focus, Focus::Nav, "back to the nav");
        assert!(form(&m).is_some(), "the form stays");
    }
}
