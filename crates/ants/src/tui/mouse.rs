//! Hit regions recorded while drawing, and line-based list viewports.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::crossterm::event::{KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use ratatui::layout::{Position, Rect};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::Paragraph;

use super::model::{Action, Data, Focus, Key, Model, Page, Prompt};
use super::{ant, channel, nav, style};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    View,
    Overlay,
    Nav(nav::Item),
    Row(usize),
    Message(u64),
    /// A step of a trail run in the thread view, by the step's own thread.
    Step(ants_proto::ThreadId),
    Completion {
        at: usize,
        palette: bool,
    },
    ChannelTab(channel::Tab),
    Copy(usize),
    Question(ants_proto::Uuid, usize),
    AntPane(ant::Pane),
    AntColumn(usize),
}

#[derive(Default)]
struct Viewport {
    top: usize,
    selected: Option<usize>,
    most: usize,
}

#[derive(Default)]
pub struct Regions {
    hits: RefCell<Vec<(Rect, Target)>>,
    scrolls: RefCell<Vec<(Rect, String)>>,
    views: RefCell<HashMap<String, Viewport>>,
}

impl Regions {
    pub fn clear(&self) {
        self.hits.borrow_mut().clear();
        self.scrolls.borrow_mut().clear();
    }

    pub fn add(&self, area: Rect, target: Target) {
        self.hits.borrow_mut().push((area, target));
    }

    pub fn top(&self, id: &str, area: Rect, selected: usize, start: usize, end: usize, total: usize) -> usize {
        self.scrolls.borrow_mut().push((area, id.to_owned()));
        let mut views = self.views.borrow_mut();
        let view = views.entry(id.to_owned()).or_default();
        view.most = total.saturating_sub(usize::from(area.height));
        if view.selected != Some(selected) {
            if start < view.top {
                view.top = start;
            } else if end > view.top + usize::from(area.height) {
                view.top = end.saturating_sub(usize::from(area.height)).min(start);
            }
            view.selected = Some(selected);
        }
        view.top = view.top.min(view.most);
        view.top
    }

    fn wheel(&self, point: Position, by: isize) -> bool {
        let scrolls = self.scrolls.borrow();
        let Some((_, id)) = scrolls.iter().rev().find(|(area, _)| area.contains(point)) else { return false };
        self.scroll(id, by);
        true
    }

    fn scroll(&self, id: &str, by: isize) {
        let mut views = self.views.borrow_mut();
        let Some(view) = views.get_mut(id) else { return };
        view.top = view.top.saturating_add_signed(by).min(view.most);
    }

    fn hit(&self, point: Position) -> Option<Target> {
        self.hits.borrow().iter().rev().find(|(area, _)| area.contains(point)).map(|(_, hit)| hit.clone())
    }

    pub fn message_at(&self, point: Position) -> Option<u64> {
        match self.hit(point)? {
            Target::Message(seq) => Some(seq),
            _ => None,
        }
    }

    pub fn visible_messages(&self) -> HashSet<u64> {
        self.hits
            .borrow()
            .iter()
            .filter_map(|(_, target)| match target {
                Target::Message(seq) => Some(*seq),
                _ => None,
            })
            .collect()
    }
}

/// Draw the same rows as `style::list`, but allow a wheel to expose a partial multiline row.
pub fn list(
    frame: &mut Frame,
    area: Rect,
    data: &Data,
    id: &str,
    rows: Vec<Text<'static>>,
    selected: usize,
    focused: bool,
) -> usize {
    let mut lines = Vec::new();
    let mut ranges = Vec::new();
    for (at, row) in rows.into_iter().enumerate() {
        let start = lines.len();
        for (n, line) in row.lines.into_iter().enumerate() {
            let mut spans = vec![if at == selected { style::bar(focused) } else { Span::raw(" ") }];
            spans.extend(line.spans);
            let line = Line::from(spans).style(if at == selected && n == 0 { line.style.bold() } else { line.style });
            lines.push(line);
        }
        ranges.push((start, lines.len()));
    }
    let (start, end) = ranges.get(selected).copied().unwrap_or_default();
    let top = data.mouse.top(id, area, selected, start, end, lines.len());
    for (at, (start, end)) in ranges.into_iter().enumerate() {
        let from = start.max(top);
        let to = end.min(top + usize::from(area.height));
        if from < to {
            data.mouse
                .add(Rect { y: area.y + (from - top) as u16, height: (to - from) as u16, ..area }, Target::Row(at));
        }
    }
    frame.render_widget(Paragraph::new(lines).scroll((u16::try_from(top).unwrap_or(u16::MAX), 0)), area);
    top
}

pub fn completions(data: &Data, bar: Rect, count: usize, selected: usize, palette: bool) {
    let rows = if palette { 10 } else { 8 };
    let selected = selected.min(count.saturating_sub(1));
    let top = (selected + 1).saturating_sub(rows);
    let height = (u16::try_from(count.min(rows)).unwrap_or(u16::MAX) + 2).min(bar.y);
    let area = Rect {
        x: bar.x,
        y: bar.y.saturating_sub(height),
        width: bar.width.min(if palette { 100 } else { 84 }),
        height,
    };
    for row in 0..height.saturating_sub(2) {
        data.mouse.add(
            Rect { x: area.x + 1, y: area.y + 1 + row, width: area.width.saturating_sub(2), height: 1 },
            Target::Completion { at: top + usize::from(row), palette },
        );
    }
}

pub fn table(data: &Data, id: &str, area: Rect, selected: usize, total: usize) -> usize {
    let inner = Rect {
        x: area.x + 1,
        y: area.y + 2,
        width: area.width.saturating_sub(2),
        height: area.height.saturating_sub(3),
    };
    let top = data.mouse.top(id, inner, selected, selected, selected + 1, total);
    for row in top..total.min(top + usize::from(inner.height)) {
        data.mouse.add(Rect { y: inner.y + (row - top) as u16, height: 1, ..inner }, Target::Row(row));
    }
    top
}

#[derive(Default)]
pub struct Mouse {
    last: Option<(Target, DateTime<Utc>)>,
}

impl Mouse {
    pub fn event(&mut self, model: &mut Model, event: MouseEvent, now: DateTime<Utc>) -> Option<Action> {
        if event.modifiers.intersects(KeyModifiers::SHIFT | KeyModifiers::ALT) || model.compact {
            return None;
        }
        // The box of a run to start takes keys alone: a wheel over it would leave the run unstarted as another key.
        if model.prompt.as_ref().is_some_and(Prompt::covers) && model.overlays.is_empty() {
            return None;
        }
        let point = Position::new(event.column, event.row);
        let target = model.data.mouse.hit(point)?;
        let by = match event.kind {
            MouseEventKind::ScrollUp => -1,
            MouseEventKind::ScrollDown => 1,
            MouseEventKind::Down(MouseButton::Left) => 0,
            _ => return None,
        };
        if by != 0 {
            return self.wheel(model, target, point, by, now);
        }
        let again = self
            .last
            .as_ref()
            .is_some_and(|(last, at)| *last == target && now - *at < chrono::TimeDelta::milliseconds(600));
        self.last = Some((target.clone(), now));
        match target {
            Target::Nav(item) => {
                model.bar.focused = false;
                let action = if model.nav != item { model.select(item) } else { None };
                model.focus = if again { Focus::View } else { Focus::Nav };
                action
            }
            Target::Completion { at, palette } => {
                model.bar.pick.selected = at;
                if again { model.key(if palette { Key::Enter } else { Key::Tab }, now) } else { None }
            }
            Target::ChannelTab(tab) => {
                let Page::Channel(page) = model.page() else { return None };
                let channel = page.channel.clone();
                model.data.channel_tabs.insert(channel.clone(), tab);
                Some(Action::KeepChannelTab(channel, tab))
            }
            Target::Copy(at) => {
                model.copying()?.selected = at;
                if again { model.key(Key::Enter, now) } else { None }
            }
            Target::Message(seq) => {
                model.bar.focused = false;
                model.focus = Focus::View;
                model.selecting = Some(model.conversation());
                if let Some(Page::Thread(page)) = model.pages.last_mut() {
                    page.mouse_select(seq);
                }
                if again { model.view_key(Key::Char('v'), now) } else { None }
            }
            Target::Step(thread) => {
                model.bar.focused = false;
                model.focus = Focus::View;
                model.selecting = Some(model.conversation());
                if let Some(Page::Thread(page)) = model.pages.last_mut() {
                    page.mouse_select_step(thread);
                }
                if again { model.view_key(Key::Enter, now) } else { None }
            }
            Target::Row(at) => {
                model.bar.focused = false;
                model.focus = Focus::View;
                model.selecting = Some(model.conversation());
                model.view_key(Key::Home, now);
                for _ in 0..at {
                    model.view_key(Key::Down, now);
                }
                if again { model.key(Key::Enter, now) } else { None }
            }
            Target::Question(question, at) => {
                model.bar.focused = false;
                model.focus = Focus::View;
                let data = &model.data;
                let selected = if let Some(Page::Thread(page)) = model.pages.last_mut() {
                    page.mouse_question(question, at, data, now)
                } else if let Some(Page::Home(page)) = model.pages.last_mut() {
                    page.mouse_question(question, at, data, now)
                } else {
                    false
                };
                if again && selected { model.key(Key::Enter, now) } else { None }
            }
            Target::View => {
                model.bar.focused = false;
                model.focus = Focus::View;
                None
            }
            Target::Overlay => None,
            Target::AntPane(pane) => {
                if let Some(Page::Ant(page)) = model.pages.last_mut() {
                    page.pane = pane;
                }
                model.bar.focused = false;
                model.focus = Focus::View;
                None
            }
            Target::AntColumn(_) => None,
        }
    }

    fn wheel(
        &mut self,
        model: &mut Model,
        target: Target,
        point: Position,
        by: isize,
        now: DateTime<Utc>,
    ) -> Option<Action> {
        self.last = None;
        if matches!(target, Target::Nav(_)) {
            model.data.mouse.scroll("nav", by);
            return None;
        }
        if let Target::AntColumn(column) = target {
            if let Some(Page::Ant(page)) = model.pages.last_mut() {
                page.mouse_scroll(column, by);
            }
            return None;
        }
        if let Target::Completion { palette, .. } = target {
            let count = if palette { model.palette(now).len() } else { model.completions().len() };
            model.bar.pick.selected = model.bar.pick.selected.saturating_add_signed(by).min(count.saturating_sub(1));
            return None;
        }
        if matches!(target, Target::Overlay | Target::Copy(_)) {
            return model.key(if by < 0 { Key::Up } else { Key::Down }, now);
        }
        if model.data.mouse.wheel(point, by) {
            return None;
        }
        if let Some(Page::Thread(page)) = model.pages.last_mut() {
            page.mouse_scroll(by);
            return None;
        }
        model.view_key(if by < 0 { Key::Up } else { Key::Down }, now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::live::{at, message};
    use crate::tui::{frame, test_support, thread};

    fn draw(model: &Model) {
        crate::test_support::render::rendered(120, 32, |f| frame::draw(f, model, at(1)));
    }

    fn click(model: &Model, target: &Target) -> MouseEvent {
        let hits = model.data.mouse.hits.borrow();
        let (area, _) =
            hits.iter().rev().find(|(_, hit)| hit == target).unwrap_or_else(|| panic!("no hit for {target:?}"));
        MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: area.x,
            row: area.y,
            modifiers: KeyModifiers::NONE,
        }
    }

    #[test]
    fn wheel_over_conversation_moves_one_line_and_modifiers_leave_selection_native() {
        let mut m = test_support::model();
        let id = ants_proto::ThreadId::generate();
        let mut page = thread::Page::new(id.clone());
        let entry = message(1, &id, "alice", "dev-a", &(0..100).map(|n| format!("line {n}\n")).collect::<String>());
        page.history.loaded(&id, Ok(vec![entry])).unwrap();
        m.pages.push(Page::Thread(page));
        let draw = |m: &Model| crate::test_support::render::rendered(100, 25, |f| frame::draw(f, m, at(1)));
        let first = draw(&m);
        let event = MouseEvent { kind: MouseEventKind::ScrollUp, column: 40, row: 8, modifiers: KeyModifiers::NONE };
        let mut mouse = Mouse::default();
        mouse.event(&mut m, event, at(1));
        let moved = draw(&m);
        assert_ne!(first, moved);
        let Page::Thread(page) = m.page() else { panic!("thread") };
        assert_eq!(page.mouse_top() + 1, page.mouse_bottom());
        mouse.event(&mut m, MouseEvent { modifiers: KeyModifiers::SHIFT, ..event }, at(1));
        assert_eq!(draw(&m), moved);
    }

    #[test]
    fn a_folded_nav_border_never_scrolls_the_conversation_behind_it() {
        let mut m = test_support::model();
        let id = ants_proto::ThreadId::generate();
        let mut page = thread::Page::new(id.clone());
        let text = (0..100).map(|n| format!("line {n}\n")).collect::<String>();
        page.history.loaded(&id, Ok(vec![message(1, &id, "alice", "dev-a", &text)])).unwrap();
        m.pages.push(Page::Thread(page));
        m.focus = Focus::Nav;
        crate::test_support::render::rendered(80, 25, |f| frame::draw(f, &m, at(1)));
        let Page::Thread(page) = m.page() else { panic!("thread") };
        let top = page.mouse_top();
        assert!(top > 0);
        let event = MouseEvent { kind: MouseEventKind::ScrollUp, column: 0, row: 8, modifiers: KeyModifiers::NONE };
        assert_eq!(Mouse::default().event(&mut m, event, at(1)), None);
        let Page::Thread(page) = m.page() else { panic!("thread") };
        assert_eq!(page.mouse_top(), top);
    }

    #[test]
    fn an_overlay_wheel_scrolls_the_overlay_not_the_thread_underneath() {
        let mut m = test_support::model();
        let id = ants_proto::ThreadId::generate();
        let mut page = thread::Page::new(id.clone());
        let text = (0..100).map(|n| format!("line {n}\n")).collect::<String>();
        page.history.loaded(&id, Ok(vec![message(1, &id, "alice", "dev-a", &text)])).unwrap();
        m.pages.push(Page::Thread(page));
        m.overlay(crate::tui::model::Overlay::Help(super::super::help::Help::default()));
        let render = |m: &Model| crate::test_support::render::rendered(80, 25, |f| frame::draw(f, m, at(1)));
        let before = render(&m);
        let Page::Thread(page) = m.page() else { panic!("thread") };
        let top = page.mouse_top();
        let event = MouseEvent { kind: MouseEventKind::ScrollDown, column: 40, row: 8, modifiers: KeyModifiers::NONE };
        assert_eq!(Mouse::default().event(&mut m, event, at(1)), None);
        assert_ne!(render(&m), before);
        let Page::Thread(page) = m.page() else { panic!("thread") };
        assert_eq!(page.mouse_top(), top);
        assert!(m.help().is_some());
    }

    #[test]
    fn the_mouse_over_the_box_of_a_run_to_start_leaves_it_waiting_for_its_keys() {
        use crate::cmd::trail::start::Start;
        use ants_proto::{TrailLimits, TrailSource};

        let mut m = test_support::model();
        let start = Start {
            source: TrailSource::Saved("review".parse().unwrap()),
            args: serde_json::Value::Null,
            conductor: crate::test_support::live::user("lead"),
            thread: ants_proto::ThreadId::generate(),
            limits: TrailLimits::DEFAULT,
        };
        m.prompt = Some(crate::tui::model::Prompt::Start(super::super::trail::Planned {
            summary: "trail review\n".to_owned(),
            start: start.clone(),
        }));
        draw(&m);
        let mut mouse = Mouse::default();
        let at_box = |kind| MouseEvent { kind, column: 60, row: 15, modifiers: KeyModifiers::NONE };
        for kind in [MouseEventKind::ScrollDown, MouseEventKind::ScrollUp, MouseEventKind::Down(MouseButton::Left)] {
            assert_eq!(mouse.event(&mut m, at_box(kind), at(1)), None);
            assert!(matches!(m.prompt, Some(crate::tui::model::Prompt::Start(_))), "{kind:?} leaves the run waiting");
        }
        m.key(Key::Char('?'), at(1));
        draw(&m);
        assert_eq!(mouse.event(&mut m, at_box(MouseEventKind::ScrollDown), at(1)), None);
        assert!(
            m.help().is_some() && matches!(m.prompt, Some(crate::tui::model::Prompt::Start(_))),
            "the wheel scrolls the help over the box"
        );
        m.key(Key::Esc, at(1));
        let Some(Action::Run(super::super::exec::Job::TrailStart(asked), _)) = m.key(Key::Char('y'), at(1)) else {
            panic!("y starts the run")
        };
        assert_eq!(*asked, start);
    }

    #[test]
    fn list_wheels_expose_one_line_without_changing_the_selection() {
        let mut m = test_support::model();
        let render = |m: &Model| {
            crate::test_support::render::rendered(20, 3, |f| {
                m.data.mouse.clear();
                list(
                    f,
                    f.area(),
                    &m.data,
                    "test",
                    vec![Text::from("first-a\nfirst-b"), Text::from("second-a\nsecond-b")],
                    0,
                    true,
                );
            })
        };
        assert!(render(&m).lines().next().unwrap().contains("first-a"));
        let event = MouseEvent { kind: MouseEventKind::ScrollDown, column: 5, row: 1, modifiers: KeyModifiers::NONE };
        assert_eq!(Mouse::default().event(&mut m, event, at(1)), None);
        let shown = render(&m);
        assert!(shown.lines().next().unwrap().contains("first-b"), "{shown}");
        assert_eq!(m.data.mouse.views.borrow()["test"].selected, Some(0));
    }

    #[test]
    fn a_row_click_selects_and_a_second_click_opens_and_nav_focus_follows_the_pointer() {
        let mut m = test_support::model();
        let ids = [ants_proto::ThreadId::generate(), ants_proto::ThreadId::generate()];
        m.stored(
            ids.iter().enumerate().map(|(n, id)| message(n as u64 + 1, id, "alice", "dev-a", "work")).collect(),
            at(1),
        );
        draw(&m);
        let nav = click(&m, &Target::Nav(nav::Item::Work(None)));
        let mut mouse = Mouse::default();
        mouse.event(&mut m, nav, at(1));
        assert_eq!(m.focus, Focus::Nav);
        mouse.event(&mut m, nav, at(1));
        assert_eq!(m.focus, Focus::View);
        draw(&m);
        let Page::Work(page) = m.page() else { panic!("work") };
        let chosen = page.rows(&m.data)[1].0.clone();
        let event = click(&m, &Target::Row(1));
        assert_eq!(mouse.event(&mut m, event, at(1)), None);
        let Page::Work(page) = m.page() else { panic!("a first click must not open") };
        assert_eq!(page.thread(&m.data), Some(chosen.clone()));
        assert_eq!(mouse.event(&mut m, event, at(1)), Some(Action::Open(chosen)));
    }

    #[test]
    fn completions_select_before_applying_and_question_options_select_before_sending() {
        let mut m = test_support::model();
        m.bar.focused = true;
        m.bar.set_text("@");
        draw(&m);
        let candidate = m.completions()[0].word.clone();
        let event = click(&m, &Target::Completion { at: 0, palette: false });
        let mut mouse = Mouse::default();
        assert_eq!(mouse.event(&mut m, event, at(1)), None);
        assert_eq!(m.bar.text(), "@");
        assert_eq!(mouse.event(&mut m, event, at(1)), None);
        assert!(m.bar.text().starts_with(&candidate));

        let mut m = test_support::model();
        let id = ants_proto::ThreadId::generate();
        let mut entry = message(1, &id, "dev-a", "alice", "Ship?");
        entry.question = Some(ants_proto::Question::new("Ship?", &["yes".into(), "no".into()], false).unwrap().into());
        let question = entry.id.unwrap();
        m.stored(vec![entry.clone()], at(1));
        let mut page = thread::Page::new(id.clone());
        page.history.loaded(&id, Ok(vec![entry])).unwrap();
        m.pages.push(Page::Thread(page));
        draw(&m);
        let event = click(&m, &Target::Question(question, 1));
        let mut mouse = Mouse::default();
        assert_eq!(mouse.event(&mut m, event, at(1)), None);
        let Some(Action::Run(super::super::exec::Job::Answer { picks, .. }, _)) = mouse.event(&mut m, event, at(1))
        else {
            panic!("a second click must answer")
        };
        assert_eq!(picks, vec![ants_proto::Pick::option(2)]);
    }
}
