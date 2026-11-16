//! The two-line pane of `ants chat --compact` and bare `ants --compact`, as for a tmux split: what waits on the human
//! by kind, the ants working and on what, and the ants' spend that UTC day; then the top home item with its keys, which
//! act on it as in home.

use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use super::frame::{self, ago};
use super::home::{self, Why};
use super::model::{Action, Data, Focus, Key, Level, Model};
use super::nav;
use super::work::dollars;
use crate::live::Link;

/// The keys of the pane, after the top item.
const KEYS: &str = "Enter open in the full view · q quit";

/// How many home items there are of each kind, in home's order, the kinds with none left out.
pub fn counts(items: &[home::Item]) -> Vec<(Why, usize)> {
    let mut counts: Vec<(Why, usize)> = Vec::new();
    for item in items {
        match counts.last_mut() {
            Some((why, n)) if *why == item.why => *n += 1,
            _ => counts.push((item.why, 1)),
        }
    }
    counts
}

/// The ants working, each with what it does: as their progress says, else as the colony does.
fn working(data: &Data) -> Vec<String> {
    let mut shown: Vec<String> = data
        .live
        .order
        .iter()
        .filter_map(|thread| data.live.progress_of(thread, usize::MAX))
        .flat_map(|(line, _)| line.split(" | ").map(str::to_owned).collect::<Vec<_>>())
        .collect();
    for entry in &data.live.colony {
        let ant = format!("{}:", entry.ant);
        if entry.activity == ants_proto::Activity::Working && !shown.iter().any(|s| s.starts_with(&ant)) {
            shown.push(entry.ant.to_string());
        }
    }
    shown
}

pub fn draw(frame: &mut Frame, model: &Model, now: DateTime<Utc>) {
    let [first, second] = Layout::vertical([Constraint::Length(1), Constraint::Length(1)]).areas(frame.area());
    let data = &model.data;
    let items = home::waiting(data, now);
    let bar = || Span::styled(" │ ", frame::dim());
    let mut spans = vec![Span::styled("ants ▸ ", Style::new().bold())];
    let counts = counts(&items);
    if counts.is_empty() {
        spans.push(Span::styled("nothing waits", frame::dim()));
    }
    for (i, (why, n)) in counts.into_iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled(" · ", frame::dim()));
        }
        spans.push(Span::styled(format!("{n} {}", why.label()), Style::new().fg(why.color())));
    }
    spans.push(bar());
    let working = working(data);
    if working.is_empty() {
        spans.push(Span::styled("no ant working", frame::dim()));
    } else {
        let working: Vec<String> = working.iter().map(|w| format!("◐ {w}")).collect();
        spans.push(Span::raw(working.join(" · ")));
    }
    spans.push(bar());
    let today = data.cards.today().map_or_else(|| "$?".to_owned(), |s| dollars(s.cost));
    spans.push(Span::raw(format!("{today} today")));
    if let Link::Down(since) = data.live.link {
        spans.push(bar());
        spans.push(Span::styled(format!("○ disconnected {}", ago(since, now)), Style::new().fg(Color::Red)));
    }
    frame.render_widget(Paragraph::new(Line::from(spans)), first);
    frame.render_widget(Paragraph::new(second_line(model, &items, now)), second);
}

/// The latest notice, else the top home item with the keys.
fn second_line(model: &Model, items: &[home::Item], now: DateTime<Utc>) -> Line<'static> {
    if let Some(notice) = model.notice_at(now) {
        let color = if notice.level == Level::Error { Color::Red } else { Color::Cyan };
        return Line::styled(format!("      {}", notice.text), Style::new().fg(color));
    }
    if let Some(line) = model.asking.as_ref().and_then(super::approvals::Asking::footer) {
        return line;
    }
    if let Some(confirm) = &model.interrupting {
        return confirm.line();
    }
    if let Some(prompt) = &model.snoozing {
        return prompt.line(&model.data);
    }
    let Some(top) = items.first() else {
        let keys = Span::styled(format!("   {KEYS}"), frame::dim());
        let what =
            if model.data.live.stored_at.is_some() { "nothing waits on a human" } else { "reading the stream…" };
        return Line::from(vec![Span::styled(format!("      {what}"), frame::dim()), keys]);
    };
    let keys = Span::styled(format!("   {} · {KEYS}", top.why.keys()), frame::dim());
    let text = crate::excerpt(&top.text, 120);
    Line::from(vec![
        Span::raw("      ▸ "),
        Span::styled(format!("{} ", top.why.label()), Style::new().fg(top.why.color())),
        Span::raw(format!("{}: {text}", model.data.title(&top.thread))),
        keys,
    ])
}

impl Model {
    /// Take `key` in the pane: on the top home item as home takes it, Enter opening it in the full view, as replying
    /// does.
    pub(super) fn compact_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Action> {
        match key {
            Key::Char('q') => return Some(Action::Quit),
            Key::Char('R') => return Some(Action::Refresh),
            Key::Up | Key::Down | Key::Char('j' | 'k') | Key::PageUp | Key::PageDown | Key::Home | Key::End => {
                return None;
            }
            Key::Tab | Key::Esc => return None,
            _ => {}
        }
        // Home again, its selection on the top item.
        self.select(nav::Item::Home);
        self.focus = Focus::View;
        let action = self.view_key(key, now);
        // Opened, or replying in the bar: the full view shows it.
        if key == Key::Enter || self.bar.focused {
            self.compact = false;
        }
        action
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::{ApprovalRequest, ThreadId, Verdict};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::live::tests::{at, message, request, user};
    use crate::tools::Update;
    use crate::tui::model::Page;
    use crate::tui::model::tests::model;

    fn pane(m: &Model, now: DateTime<Utc>) -> String {
        let mut terminal = Terminal::new(TestBackend::new(140, 2)).unwrap();
        terminal.draw(|f| frame::draw(f, m, now)).unwrap();
        let buffer = terminal.backend().buffer();
        let cells: Vec<&str> = buffer.content.iter().map(ratatui::buffer::Cell::symbol).collect();
        cells.chunks(140).map(|row| row.concat().trim_end().to_owned()).collect::<Vec<_>>().join("\n")
    }

    #[test]
    fn the_pane_counts_what_waits_names_the_working_ants_and_opens_the_top_item() {
        use ants_daemon::caps::Spent;

        let (asked, done) = (ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        m.compact = true;
        let mut opening = message(1, &done, "alice", "dev-a", "deploy");
        opening.owner = Some(user("alice"));
        m.stored(vec![opening, message(2, &done, "dev-a", "alice", "deployed")], at(2));
        let shown = pane(&m, at(3));
        assert!(shown.starts_with("ants ▸ 1 finished │ no ant working │ $? today"), "{shown}");
        let keys = "r follow up · d mark done · z snooze · Enter open in the full view · q quit";
        assert!(shown.contains(&format!("▸ finished deploy: dev-a answered   {keys}")), "{shown}");

        let push =
            ApprovalRequest { thread_id: asked.clone(), ..request("dev-a", "git push", vec![Verdict::AllowOnce]) };
        m.data.live.approval(&Update::Pending(push));
        m.data.cards.today = Some(vec![(user("dev-a"), Spent { tokens: 10, cost: 4.12 })]);
        let mut entry = crate::colony::tests::entry("scout", 60, 5, at(0));
        entry.activity = ants_proto::Activity::Working;
        m.data.live.colony(vec![entry], at(3));
        let shown = pane(&m, at(3));
        assert!(shown.starts_with("ants ▸ 1 approve · 1 finished │ ◐ scout │ $4.12 today"), "{shown}");
        assert!(shown.contains("▸ approve "), "{shown}");

        assert_eq!(m.key(Key::Down, at(3)), None);
        assert_eq!(m.key(Key::Char('r'), at(3)), None);
        assert!(pane(&m, at(3)).contains("with reject? y confirms"), "the top request is answered in the pane");
        m.key(Key::Esc, at(3));
        assert_eq!(m.key(Key::Enter, at(3)), None);
        assert!(!m.compact && m.asking.as_ref().is_some_and(|a| a.full), "the request in full in the full view");
        assert_eq!(m.key(Key::Enter, at(3)), Some(Action::Open(asked.clone())), "its thread");
        assert!(matches!(m.page(), Page::Thread(page) if page.history.thread == asked));
    }
}
