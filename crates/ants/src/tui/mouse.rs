//! Hit regions recorded while drawing, and line-based list viewports.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::crossterm::event::{KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use ratatui::layout::{Position, Rect};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::Paragraph;

use super::model::{Action, Focus, Key, Model, Page, Prompt};
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

/// A list the mouse scrolls, its rows kept apart from the other lists' of its page, and from those of the other pages
/// stacked ([`Regions::forget`]).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Viewport {
    Nav,
    Home,
    /// A work view, or an ant's threads.
    Work,
    Channel(ants_proto::ChannelId),
    Dm(ants_proto::UserId),
    Search,
    Tree,
    Credentials,
    Held,
    /// A thread's waiting cards, listed.
    ThreadWaiting,
    AntQueue,
    AntMcp,
    AntSettings,
}

/// Where a list's viewport is.
#[derive(Default)]
struct Scrolled {
    top: usize,
    selected: Option<usize>,
    most: usize,
}

/// A viewport of the page at a depth of the stack, or the nav's, at none.
type Stacked = (Option<usize>, Viewport);

#[derive(Default)]
pub struct Regions {
    hits: RefCell<Vec<(Rect, Target)>>,
    scrolls: RefCell<Vec<(Rect, Stacked)>>,
    views: RefCell<HashMap<Stacked, Scrolled>>,
    /// The depth in the stack of the page drawn.
    depth: Cell<usize>,
}

impl Regions {
    /// Start a frame drawing the page at `depth` in the stack.
    pub fn clear(&self, depth: usize) {
        self.hits.borrow_mut().clear();
        self.scrolls.borrow_mut().clear();
        self.depth.set(depth);
    }

    /// Drop the viewports of the pages at `depth` and above, popped or replaced.
    pub fn forget(&self, depth: usize) {
        self.views.borrow_mut().retain(|(at, _), _| at.is_none_or(|at| at < depth));
    }

    fn stacked(&self, id: Viewport) -> Stacked {
        let depth = (id != Viewport::Nav).then(|| self.depth.get());
        (depth, id)
    }

    pub fn add(&self, area: Rect, target: Target) {
        self.hits.borrow_mut().push((area, target));
    }

    pub fn top(&self, id: Viewport, area: Rect, selected: usize, start: usize, end: usize, total: usize) -> usize {
        let key = self.stacked(id);
        self.scrolls.borrow_mut().push((area, key.clone()));
        let mut views = self.views.borrow_mut();
        let view = views.entry(key).or_default();
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
        let Some((_, key)) = scrolls.iter().rev().find(|(area, _)| area.contains(point)) else { return false };
        self.scroll_key(key, by);
        true
    }

    fn scroll(&self, id: Viewport, by: isize) {
        self.scroll_key(&self.stacked(id), by);
    }

    fn scroll_key(&self, key: &Stacked, by: isize) {
        let mut views = self.views.borrow_mut();
        let Some(view) = views.get_mut(key) else { return };
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
    mouse: &Regions,
    id: Viewport,
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
    let top = mouse.top(id, area, selected, start, end, lines.len());
    for (at, (start, end)) in ranges.into_iter().enumerate() {
        let from = start.max(top);
        let to = end.min(top + usize::from(area.height));
        if from < to {
            mouse.add(Rect { y: area.y + (from - top) as u16, height: (to - from) as u16, ..area }, Target::Row(at));
        }
    }
    frame.render_widget(Paragraph::new(lines).scroll((u16::try_from(top).unwrap_or(u16::MAX), 0)), area);
    top
}

pub fn completions(mouse: &Regions, bar: Rect, count: usize, selected: usize, palette: bool) {
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
        mouse.add(
            Rect { x: area.x + 1, y: area.y + 1 + row, width: area.width.saturating_sub(2), height: 1 },
            Target::Completion { at: top + usize::from(row), palette },
        );
    }
}

pub fn table(mouse: &Regions, id: Viewport, area: Rect, selected: usize, total: usize) -> usize {
    let inner = Rect {
        x: area.x + 1,
        y: area.y + 2,
        width: area.width.saturating_sub(2),
        height: area.height.saturating_sub(3),
    };
    let top = mouse.top(id, inner, selected, selected, selected + 1, total);
    for row in top..total.min(top + usize::from(inner.height)) {
        mouse.add(Rect { y: inner.y + (row - top) as u16, height: 1, ..inner }, Target::Row(row));
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
        let target = model.ui.mouse.hit(point)?;
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
                model.ui.bar.focused = false;
                let action = if model.nav != item { model.select(item) } else { None };
                model.focus = if again { Focus::View } else { Focus::Nav };
                action
            }
            Target::Completion { at, palette } => {
                model.ui.bar.pick.selected = at;
                if again { model.key(if palette { Key::Enter } else { Key::Tab }, now) } else { None }
            }
            Target::ChannelTab(tab) => {
                let Page::Channel(page) = model.page() else { return None };
                let channel = page.channel.clone();
                Some(model.choose_tab(channel, tab))
            }
            Target::Copy(at) => {
                model.copying()?.selected = at;
                if again { model.key(Key::Enter, now) } else { None }
            }
            Target::Message(seq) => {
                model.ui.bar.focused = false;
                model.focus = Focus::View;
                model.ui.selecting = Some(model.conversation());
                if let Some(Page::Thread(page)) = model.pages.last_mut() {
                    page.mouse_select(seq);
                }
                if again { model.view_key(Key::Char('v'), now) } else { None }
            }
            Target::Step(thread) => {
                model.ui.bar.focused = false;
                model.focus = Focus::View;
                model.ui.selecting = Some(model.conversation());
                if let Some(Page::Thread(page)) = model.pages.last_mut() {
                    page.mouse_select_step(thread);
                }
                if again { model.view_key(Key::Enter, now) } else { None }
            }
            Target::Row(at) => {
                model.ui.bar.focused = false;
                model.focus = Focus::View;
                model.ui.selecting = Some(model.conversation());
                model.view_key(Key::Home, now);
                for _ in 0..at {
                    model.view_key(Key::Down, now);
                }
                if again { model.key(Key::Enter, now) } else { None }
            }
            Target::Question(question, at) => {
                model.ui.bar.focused = false;
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
                model.ui.bar.focused = false;
                model.focus = Focus::View;
                None
            }
            Target::Overlay => None,
            Target::AntPane(pane) => {
                if let Some(Page::Ant(page)) = model.pages.last_mut() {
                    page.pane = pane;
                }
                model.ui.bar.focused = false;
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
            model.ui.mouse.scroll(Viewport::Nav, by);
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
            model.ui.bar.pick.selected =
                model.ui.bar.pick.selected.saturating_add_signed(by).min(count.saturating_sub(1));
            return None;
        }
        if matches!(target, Target::Overlay | Target::Copy(_)) {
            return model.key(if by < 0 { Key::Up } else { Key::Down }, now);
        }
        if model.ui.mouse.wheel(point, by) {
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
        let hits = model.ui.mouse.hits.borrow();
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
    fn an_ants_threads_and_the_work_view_stacked_on_it_scroll_apart() {
        let mut m = test_support::model();
        let messages = (1..=40)
            .map(|n| message(n, &ants_proto::ThreadId::generate(), "alice", "dev-a", &format!("task {n:02}")))
            .collect();
        m.stored(messages, at(2));
        m.select(crate::tui::nav::Item::Ant(crate::test_support::live::user("dev-a")));
        m.focus = Focus::View;
        let shown = test_support::screen(&m, 160, 36, at(3));
        assert!(shown.contains("task 40"), "{shown}");
        m.ui.mouse.scroll(Viewport::Work, 10);
        assert!(!test_support::screen(&m, 160, 36, at(3)).contains("task 40"), "the ant's threads scrolled");
        m.key(Key::Char('w'), at(3));
        assert_eq!(m.page().name(), "work");
        let shown = test_support::screen(&m, 160, 36, at(3));
        assert!(shown.contains("task 40"), "the work view keeps its own scroll: {shown}");
    }

    #[test]
    fn a_clicked_tab_is_kept_as_one_chosen_by_key() {
        let ops: ants_proto::ChannelId = "ops".parse().unwrap();
        let mut m = test_support::model();
        let lunch = ants_proto::ChannelPost::new(ops.clone(), crate::test_support::live::user("bob"), "lunch?");
        m.stored(vec![test_support::post(1, &lunch)], at(1));
        m.select(crate::tui::nav::Item::Channel(ops.clone()));
        let mut mouse = Mouse::default();
        draw(&m);
        let event = click(&m, &Target::ChannelTab(channel::Tab::Idle));
        assert_eq!(mouse.event(&mut m, event, at(1)), Some(Action::KeepChannelTab(ops.clone(), channel::Tab::Idle)));
        assert_eq!(m.ui.channel_tabs.get(&ops), Some(&channel::Tab::Idle));
        draw(&m);
        let event = click(&m, &Target::ChannelTab(channel::Tab::All));
        assert_eq!(mouse.event(&mut m, event, at(5)), Some(Action::KeepChannelTab(ops, channel::Tab::All)));
        assert!(m.ui.channel_tabs.is_empty(), "the default is kept as none, as by key");
    }

    fn forty_tasks_of_dev_a() -> Model {
        let mut m = test_support::model();
        let messages = (1..=40)
            .map(|n| message(n, &ants_proto::ThreadId::generate(), "alice", "dev-a", &format!("task {n:02}")))
            .collect();
        m.stored(messages, at(2));
        m.select(crate::tui::nav::Item::Ant(crate::test_support::live::user("dev-a")));
        m.focus = Focus::View;
        m
    }

    fn top_shown(m: &Model) -> bool {
        test_support::screen(m, 160, 36, at(3)).contains("task 40")
    }

    #[test]
    fn stacked_pages_of_one_kind_scroll_apart() {
        let mut m = forty_tasks_of_dev_a();
        assert!(top_shown(&m));
        m.ui.mouse.scroll(Viewport::Work, 10);
        assert!(!top_shown(&m), "the ant's threads scrolled");
        m.key(Key::Char('w'), at(3));
        assert!(top_shown(&m));
        m.ui.mouse.scroll(Viewport::Work, 10);
        assert!(!top_shown(&m), "the work view scrolled");
        let thread = ants_proto::ThreadId::generate();
        m.push(Page::Thread(thread::Page::new(thread.clone())));
        m.tree(thread);
        m.push(Page::Ant(ant::Page::new(crate::test_support::live::user("dev-a"))));
        assert!(top_shown(&m), "the same ant stacked again scrolls on its own");
        m.key(Key::Char('w'), at(3));
        assert_eq!((m.pages.len(), m.page().name()), (6, "work"));
        assert!(top_shown(&m), "the same work view stacked again scrolls on its own");
        m.ui.mouse.scroll(Viewport::Work, 20);
        for _ in 0..4 {
            m.key(Key::Esc, at(3));
        }
        assert_eq!((m.pages.len(), m.page().name()), (2, "work"));
        assert!(!top_shown(&m), "the first work view keeps its scroll");
        m.key(Key::Esc, at(3));
        assert_eq!(m.page().name(), "ant");
        assert!(!top_shown(&m), "the first ant keeps its scroll");
    }

    #[test]
    fn a_page_pushed_where_one_was_popped_starts_unscrolled() {
        let mut m = forty_tasks_of_dev_a();
        m.key(Key::Char('w'), at(3));
        assert!(top_shown(&m));
        m.ui.mouse.scroll(Viewport::Work, 10);
        assert!(!top_shown(&m), "the work view scrolled");
        m.key(Key::Esc, at(3));
        assert!(m.ui.mouse.views.borrow().keys().all(|(depth, _)| depth.is_none_or(|depth| depth < 1)));
        m.key(Key::Char('w'), at(3));
        assert!(top_shown(&m), "the work view opened again starts at its top");
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
                m.ui.mouse.clear(0);
                list(
                    f,
                    f.area(),
                    &m.ui.mouse,
                    Viewport::Search,
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
        assert_eq!(m.ui.mouse.views.borrow()[&(Some(0), Viewport::Search)].selected, Some(0));
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
        m.ui.bar.focused = true;
        m.ui.bar.set_text("@");
        draw(&m);
        let candidate = m.completions()[0].word.clone();
        let event = click(&m, &Target::Completion { at: 0, palette: false });
        let mut mouse = Mouse::default();
        assert_eq!(mouse.event(&mut m, event, at(1)), None);
        assert_eq!(m.ui.bar.text(), "@");
        assert_eq!(mouse.event(&mut m, event, at(1)), None);
        assert!(m.ui.bar.text().starts_with(&candidate));

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
