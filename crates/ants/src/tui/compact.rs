//! The two-line pane of `ants chat --compact` and bare `ants --compact`, as for a tmux split: what waits on the human
//! by kind, the ants working and on what, and the ants' spend that UTC day; then the top home item with its keys, which
//! act on it as in home.

use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use super::home::{self, Why};
use super::model::{Action, Data, Focus, Key, Level, Model, Page};
use super::nav;
use super::style::{self, ago};
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
    let bar = || Span::styled(" │ ", style::dim());
    let mut spans = vec![Span::styled("ants ▸ ", Style::new().bold())];
    let counts = counts(&items);
    if counts.is_empty() {
        spans.push(Span::styled("nothing waits", style::dim()));
    }
    for (i, (why, n)) in counts.into_iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled(" · ", style::dim()));
        }
        spans.push(Span::styled(format!("{n} {}", why.label()), Style::new().fg(why.color())));
    }
    spans.push(bar());
    let working = working(data);
    if working.is_empty() {
        spans.push(Span::styled("no ant working", style::dim()));
    } else {
        let working: Vec<String> = working.iter().map(|w| format!("◐ {w}")).collect();
        spans.push(Span::raw(working.join(" · ")));
    }
    spans.push(bar());
    let today = data.cards.today().map_or_else(|| "$?".to_owned(), |s| dollars(s.cost));
    spans.push(Span::raw(format!("{today} today")));
    if let Link::Down(since) = data.live.link {
        spans.push(bar());
        spans.push(Span::styled(format!("○ disconnected {}", ago(since, now)), Style::new().fg(style::ERROR)));
    }
    frame.render_widget(Paragraph::new(Line::from(spans)), first);
    frame.render_widget(Paragraph::new(second_line(model, &items, now, second.width)), second);
}

/// The latest notice, else the top home item with the keys.
fn second_line(model: &Model, items: &[home::Item], now: DateTime<Utc>, width: u16) -> Line<'static> {
    if model.quitting_at(now) {
        return Line::styled(format!("      {}", super::frame::QUIT_AGAIN), Style::new().fg(style::INFO));
    }
    if let Some(notice) = model.notice_at(now) {
        let mut line = style::notice(&notice.text, notice.level == Level::Error, width.saturating_sub(5));
        line.spans.insert(0, Span::raw("     "));
        return line;
    }
    if let Some(line) = model.asking.as_ref().and_then(super::approvals::Asking::status) {
        return line;
    }
    if let Some(confirm) = &model.interrupting {
        return confirm.line();
    }
    if let Some(prompt) = &model.snoozing {
        return prompt.line(&model.data);
    }
    let Some(top) = items.first() else {
        let keys = Span::styled(format!("   {KEYS}"), style::dim());
        let what =
            if model.data.live.stored_at.is_some() { "nothing waits on a human" } else { "reading the stream…" };
        return Line::from(vec![Span::styled(format!("      {what}"), style::dim()), keys]);
    };
    let keys = Span::styled(format!("   {} · {KEYS}", top.why.keys()), style::dim());
    let mut text = top.text.clone();
    if let Some(always) = top
        .request
        .as_ref()
        .filter(|(_, settled)| settled.is_none())
        .and_then(|(request, _)| request.label(ants_proto::Verdict::AllowAlways))
    {
        text = format!("{text} · A always: {always}");
    }
    let text = crate::text::excerpt(&text, 120);
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
            Key::Char('q') => return self.quit(now),
            Key::Char('R') => return Some(Action::Refresh),
            Key::Up | Key::Down | Key::Char('j' | 'k') | Key::PageUp | Key::PageDown | Key::Home | Key::End => {
                return None;
            }
            Key::Tab | Key::Esc => return None,
            _ => {}
        }
        // Home again, its selection on the top item.
        self.select(nav::Item::Home);
        if let Page::Home(home) = self.page() {
            home.compacted();
        }
        self.focus = Focus::View;
        let action = self.view_key(key, now);
        // Opened, replying in the bar, or a request shown in full: the full view shows it.
        if key == Key::Enter || self.bar.focused || self.asking.as_ref().is_some_and(|a| a.full) {
            self.compact = false;
        }
        action
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::{ApprovalRequest, ThreadId, Verdict};

    use super::*;
    use crate::test_support::live::{at, message, request, user};
    use crate::tools::Update;
    use crate::tui::test_support::model;

    fn pane(m: &Model, now: DateTime<Utc>) -> String {
        crate::test_support::render::rendered(140, 2, |f| crate::tui::frame::draw(f, m, now))
    }

    #[test]
    fn the_pane_counts_what_waits_names_the_working_ants_and_opens_the_top_item() {
        use ants_daemon::limits::caps::Spent;

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
        m.data.live.approval(&Update::Pending(push.clone()));
        m.data.cards.today = Some(vec![(user("dev-a"), Spent { tokens: 10, cost: 4.12 })]);
        let mut entry = crate::test_support::colony::entry("scout", 60, 5, at(0));
        entry.activity = ants_proto::Activity::Working;
        m.data.live.colony(vec![entry], at(3));
        let shown = pane(&m, at(3));
        assert!(shown.starts_with("ants ▸ 1 approve · 1 finished │ ◐ scout │ $4.12 today"), "{shown}");
        assert!(shown.contains("▸ approve "), "{shown}");

        assert_eq!(m.key(Key::Down, at(3)), None);
        assert_eq!(m.key(Key::Char('r'), at(3)), Some(Action::Decide(Box::new(push.clone()), Verdict::Reject)));
        let storing = format!("storing the decision on request {}…", crate::tools::qualified(&push));
        assert!(pane(&m, at(3)).contains(&storing), "the top request is answered in the pane");
        m.take(crate::live::Event::Decided(Err(anyhow::anyhow!("request was already answered"))), at(3));
        assert!(m.asking.is_none() && pane(&m, at(3)).contains("request was already answered"));
        assert_eq!(m.key(Key::Enter, at(3)), None);
        assert!(!m.compact && m.asking.as_ref().is_some_and(|a| a.full), "the request in full in the full view");
        assert_eq!(m.key(Key::Enter, at(3)), Some(Action::Open(asked.clone())), "its thread");
        assert!(matches!(m.page(), Page::Thread(page) if page.history.thread == asked));
    }

    #[test]
    fn a_top_tool_request_names_what_allow_always_allows() {
        let mut m = model();
        m.compact = true;
        let push = request("dev-a", "git push", vec![Verdict::AllowAlways])
            .labelled([(Verdict::AllowAlways, "Yes, and don't ask again".to_owned())].into());
        m.data.live.approval(&Update::Pending(push));
        let shown = pane(&m, at(3));
        assert!(shown.contains("dev-a asks to run execute: git push · A always: Yes, and don't ask again"), "{shown}");
    }

    #[test]
    fn a_top_tool_request_too_long_for_the_pane_opens_in_full_on_its_keys() {
        let mut m = model();
        let mut push = request("dev-a", "git push", vec![Verdict::AllowOnce]);
        push.tool.input = Some(serde_json::json!({ "command": "echo long ".repeat(20) }).to_string());
        m.data.live.approval(&Update::Pending(push.clone()));
        let _ = crate::test_support::render::rendered(140, 30, |f| crate::tui::frame::draw(f, &m, at(3)));
        m.compact = true;
        assert_eq!(m.key(Key::Char('a'), at(3)), None);
        assert!(!m.compact && m.asking.as_ref().is_some_and(|a| a.full), "the request in full in the full view");
        assert_eq!(m.key(Key::Char('a'), at(3)), Some(Action::Decide(Box::new(push), Verdict::AllowOnce)));
    }
}
