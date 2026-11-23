//! A thread's delegation tree, which `t` opens on a thread: the thread's owner, then each ant messaged on it, on the
//! threads spun off it ([`ants_proto::Envelope::origin_thread`]) or on the threads the links bucket says the link to it
//! was passed on ([`ants_daemon::records::links::passed`]), each nested under the ant that messaged it first, with the title or opening line
//! it was messaged with, its turns left, what it spent and its state; then the totals. Enter opens a node's thread, and
//! `x` then `y` cancels every prompt running in a node's subtree, as `ants cancel` does for each of its ants.
//!
//! The messages come from what the live feed read ([`Delegations`]); the links and the spend are read every while the
//! tree shows ([`Io`]).

use std::collections::BTreeSet;
use std::fmt::Write as _;

use ants_daemon::records::links::Passed;
use ants_proto::{ThreadId, UserId};
use anyhow::Result;
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use super::armed::Armed;
use super::model::{Action, Data, Key, Level, Model, To};
use super::select::{PAGE, Selection, step};
use super::style;
use crate::spend::ThreadReport;

mod io;
mod shape;

pub use io::{Event, Io};
pub use shape::{Delegations, NodeState, Tree};

impl NodeState {
    fn style(&self) -> Style {
        match self {
            Self::Thread(state) => Style::new().fg(state.color()),
            Self::Working(_) | Self::WaitingOnMe => Style::new().fg(Color::Yellow),
            Self::GaveUp => Style::new().fg(style::ERROR),
            Self::Queued => Style::new().fg(Color::Cyan),
            Self::Answered | Self::Linked => style::dim(),
        }
    }
}

/// The keys of `tree`'s nodes, as a selection keeps them.
fn nodes(tree: &Tree) -> Vec<(UserId, ThreadId)> {
    tree.nodes.iter().map(|n| (n.user.clone(), n.thread.clone())).collect()
}

#[derive(Debug)]
pub struct Page {
    pub root: ThreadId,
    /// The selected node, kept as nodes come.
    selected: Selection<(UserId, ThreadId)>,
    /// `x` was pressed: `y` cancels the selected subtree's prompts.
    armed: Armed<()>,
    /// The links to the root passed on, once read.
    pub links: Vec<Passed>,
    /// What the ants spent on the tree's threads, once read.
    pub spend: Option<Vec<ThreadReport>>,
}

impl Page {
    pub const ARMED_KEYS: &str = "y cancel every prompt in the selected subtree · any other key keeps them";

    pub fn new(root: ThreadId) -> Self {
        Self { root, selected: Selection::default(), armed: Armed::default(), links: Vec::new(), spend: None }
    }

    /// Drop the confirmation asked for, if any: whether there was one.
    pub fn disarm(&mut self) -> bool {
        self.armed.disarm()
    }

    /// The keys of the cancel `x` asked to confirm, while it waits.
    pub fn prompt(&self) -> Option<&'static str> {
        self.armed.get().map(|()| Self::ARMED_KEYS)
    }

    pub fn tree(&self, data: &Data, now: DateTime<Utc>) -> Tree {
        Tree::build(&self.root, data, &self.links, self.spend.as_deref(), now)
    }

    fn at(&self, tree: &Tree) -> usize {
        self.selected.at(&nodes(tree))
    }

    pub fn key(&mut self, key: Key, data: &Data, now: DateTime<Utc>) -> Option<To> {
        let tree = self.tree(data, now);
        let at = self.at(&tree);
        let armed = self.armed.take().is_some();
        let to = match key {
            Key::Enter => return tree.nodes.get(at).map(|n| To::Thread(n.thread.clone())),
            Key::Char('x') => {
                self.armed.arm(Some(()));
                return None;
            }
            Key::Char('y') if armed => {
                let prompts: Vec<(UserId, ThreadId)> = tree.nodes[at..=at + tree.nodes[at].under]
                    .iter()
                    .filter(|n| n.depth > 0)
                    .map(|n| (n.user.clone(), n.thread.clone()))
                    .collect();
                return (!prompts.is_empty()).then_some(To::Do(Action::CancelSubtree(prompts)));
            }
            _ => step(key, at, Some(PAGE))?,
        };
        self.selected.select(&nodes(&tree), to);
        None
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, model: &Model, now: DateTime<Utc>, focused: bool) {
        let (data, mouse) = (&model.data, &model.ui.mouse);
        let tree = self.tree(data, now);
        let at = self.at(&tree);
        let width = usize::from(area.width.saturating_sub(2));
        let name = tree
            .nodes
            .iter()
            .map(|n| n.glyph.chars().count() + n.user.as_str().len())
            .max()
            .unwrap_or(0)
            .min(width / 3);
        let state = 18;
        let line = width.saturating_sub(name + 2 + 2 + 5 + 2 + 7 + 2 + state).max(8);
        let mut lines = vec![Line::styled(
            format!("{:<name$}  {:<line$}  {:>5}  {:>7}  state", "", "", "turns", "spend"),
            style::dim(),
        )];
        let mut selected = 0;
        for (n, node) in tree.nodes.iter().enumerate() {
            let text = if n == 0 {
                let via = if tree.via.is_empty() { String::new() } else { format!(", via {}", tree.via.join(" ")) };
                format!("(owner{via})")
            } else {
                node.line.clone()
            };
            let turns = node.turns.map_or_else(|| "-".to_owned(), |t| t.to_string());
            let spend = node.spend.map_or_else(|| "-".to_owned(), |s| format!("${:.2}", s.cost));
            let who = style::column(&format!("{}{}", node.glyph, node.user), name);
            let mut row = Line::from(vec![
                Span::raw(format!("{who}  ")),
                Span::styled(
                    format!("{}  ", style::column(&text, line)),
                    if n == 0 { style::dim() } else { Style::new() },
                ),
                Span::raw(format!("{turns:>5}  {spend:>7}  ")),
                Span::styled(style::fit(&node.state.label(), state), node.state.style()),
            ]);
            if n == at {
                row = row.style(style::highlight(focused));
                selected = lines.len();
            }
            lines.push(row);
        }
        lines.push(Line::raw(""));
        let mut total = vec![format!("{} ants", tree.ants), format!("{} turns used", tree.turns)];
        total.extend(tree.spend.map(|s| format!("${:.2}", s.cost)));
        total.extend(tree.elapsed.map(crate::colony::short));
        lines.push(Line::from(vec![Span::styled("total  ", style::dim()), Span::raw(total.join(" · "))]));
        let height = usize::from(area.height.saturating_sub(2));
        let inner = style::block(String::new(), focused).inner(area);
        let top = mouse.top(super::mouse::Viewport::Tree, inner, selected, selected, selected + 1, lines.len());
        for n in 0..tree.nodes.len() {
            let row = n + 1;
            if row >= top && row < top + height {
                mouse.add(Rect { y: inner.y + (row - top) as u16, height: 1, ..inner }, super::mouse::Target::Row(n));
            }
        }
        let title = format!(" delegation tree · {} ", data.title(&self.root));
        let paragraph = Paragraph::new(lines)
            .block(style::block(title, focused))
            .scroll((u16::try_from(top).unwrap_or(u16::MAX), 0));
        frame.render_widget(paragraph, area);
    }
}

/// The notice a cancel's `outcomes` show.
pub fn notice(outcomes: &[(UserId, ThreadId, Result<Option<bool>>)]) -> (String, Level) {
    let names = |pick: &dyn Fn(&Result<Option<bool>>) -> bool| {
        let names: BTreeSet<&str> = outcomes.iter().filter(|(.., o)| pick(o)).map(|(u, ..)| u.as_str()).collect();
        names.into_iter().collect::<Vec<_>>().join(", ")
    };
    let cancelled = names(&|o| matches!(o, Ok(Some(true))));
    let mut text = if cancelled.is_empty() {
        "no prompt was running in the subtree".to_owned()
    } else {
        format!("cancelled the prompts of {cancelled}; the partial replies follow on their threads")
    };
    let unanswered = names(&|o| matches!(o, Ok(None)));
    if !unanswered.is_empty() {
        let _ = write!(text, "; no daemon answered for {unanswered}");
    }
    let mut level = Level::Info;
    if let Some((user, thread, Err(e))) = outcomes.iter().find(|(.., o)| o.is_err()) {
        let _ = write!(text, "; asking {user} on thread {thread} failed: {e:#}");
        level = Level::Error;
    }
    (text, level)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ants_daemon::limits::caps::Spent;

    use super::shape::Node;
    use super::*;
    use crate::test_support::live::{at, message, user};
    use crate::tui::model::{Focus, Model, Page as View};
    use crate::tui::test_support::model;

    fn link(from: &str, to: &str, on: Option<&ThreadId>) -> Passed {
        Passed { from: user(from), to: user(to), on: on.cloned() }
    }

    fn spent(ant: &str, thread: &ThreadId, cost: f64) -> ThreadReport {
        let spent = Spent { tokens: 1000, cost };
        ThreadReport { ant: user(ant), thread: thread.clone(), spent, total: spent, caps: None }
    }

    /// alice asks dev-a on `root`; dev-a asks reviewer there, and tester on `spun`, a thread spun off `root`.
    fn delegated(m: &mut Model) -> (ThreadId, ThreadId) {
        let (root, spun) = (ThreadId::generate(), ThreadId::generate());
        let mut asked = message(1, &root, "alice", "dev-a", "fix the login bug");
        asked.turns = Some(10);
        let mut answered = message(3, &root, "reviewer", "dev-a", "looks good");
        answered.turns = Some(7);
        let mut tester = message(5, &spun, "dev-a", "tester", "run the e2e suite");
        tester.origin = Some(root.clone());
        let entries = vec![
            asked,
            message(2, &root, "dev-a", "reviewer", "review the diff\nin worker/retry.rs"),
            answered,
            message(4, &root, "dev-a", "alice", "fixed"),
            tester,
        ];
        m.stored(entries, at(5));
        (root, spun)
    }

    fn rows(tree: &Tree) -> Vec<(String, &str, String, String)> {
        tree.nodes
            .iter()
            .map(|n| (format!("{}{}", n.glyph, n.user), n.thread_name(), n.line.clone(), n.state.label()))
            .collect()
    }

    impl Node {
        fn thread_name(&self) -> &'static str {
            if self.spun_off { "spun" } else { "same" }
        }
    }

    #[test]
    fn ants_nest_under_who_messaged_them_first_across_spin_offs_and_links() {
        let mut m = model();
        let (root, spun) = delegated(&mut m);
        let links = [link("dev-a", "docs", Some(&root)), link("stranger", "ops", None), link("dev-a", "alice", None)];
        let tree = Tree::build(&root, &m.data, &links, None, at(9));
        let title = m.data.title(&root);
        let s = |s: &str| s.to_owned();
        assert_eq!(
            rows(&tree),
            vec![
                (s("alice"), "same", s(""), m.data.state(&root).label().to_owned()),
                (s("├─ dev-a"), "same", title.clone(), s("answered")),
                (s("│  ├─ reviewer"), "same", s("review the diff"), s("answered")),
                (s("│  ├─ tester"), "spun", format!("spin-off: {}", m.data.title(&spun)), s("queued")),
                (s("│  └─ docs"), "same", title.clone(), s("linked")),
                (s("└─ ops"), "same", title, s("linked")),
            ]
        );
        assert_eq!(tree.nodes[1].turns, Some(7), "the turns left on the last message to it");
        assert_eq!(tree.nodes.iter().map(|n| n.under).collect::<Vec<_>>(), [5, 3, 0, 0, 0, 0]);
        assert_eq!((tree.ants, tree.turns, tree.spend), (5, 3, None), "humans are no ants; 3 messages between ants");
        assert_eq!(tree.elapsed, Some(Duration::from_secs(4)), "from the first message to the last");

        let forwarded = ThreadId::generate();
        let mut handed = message(6, &forwarded, "alice", "dev-b", "take this over");
        handed.origin = Some(root.clone());
        m.stored(vec![handed], at(6));
        let tree = Tree::build(&root, &m.data, &links, None, at(9));
        let line = |user: &str| tree.nodes.iter().find(|n| n.user.as_str() == user).map(|n| n.line.clone());
        assert_eq!(line("dev-b"), Some(format!("forwarded: {}", m.data.title(&forwarded))), "a human forwarded it");
        assert_eq!(line("tester"), Some(format!("spin-off: {}", m.data.title(&spun))));

        let spend = [spent("dev-a", &root, 0.25), spent("tester", &spun, 0.5), spent("tester", &root, 9.0)];
        let tree = Tree::build(&root, &m.data, &links, Some(&spend), at(9));
        assert_eq!(tree.nodes[1].spend.map(|s| s.cost), Some(0.25));
        assert_eq!(tree.nodes[3].spend.map(|s| s.cost), Some(0.5), "what it spent on its own thread");
        assert_eq!(tree.spend.map(|s| s.cost), Some(0.75));
    }

    #[test]
    fn a_tree_spans_only_its_own_threads() {
        let mut m = model();
        let (root, spun) = delegated(&mut m);
        let other = ThreadId::generate();
        m.stored(vec![message(6, &other, "alice", "dev-b", "unrelated")], at(6));
        assert_eq!(m.data.delegations.threads(&root), [root.clone(), spun.clone()]);
        let users: Vec<String> =
            Tree::build(&spun, &m.data, &[], None, at(9)).nodes.iter().map(|n| n.user.to_string()).collect();
        assert_eq!(users, ["alice", "tester"], "a spin-off's own tree: the owner, then the ants messaged on it");
    }

    #[test]
    fn enter_opens_a_nodes_thread_and_x_then_y_cancels_its_subtree() {
        let mut m = model();
        let (root, spun) = delegated(&mut m);
        let mut page = Page::new(root.clone());
        let data = &m.data;
        assert_eq!(page.key(Key::Enter, data, at(9)), Some(To::Thread(root.clone())));
        page.key(Key::Down, data, at(9));
        page.key(Key::Char('j'), data, at(9));
        page.key(Key::Char('j'), data, at(9));
        assert_eq!(page.key(Key::Enter, data, at(9)), Some(To::Thread(spun.clone())), "tester's thread");
        assert_eq!(page.key(Key::Char('y'), data, at(9)), None, "y alone cancels nothing");
        assert_eq!(page.key(Key::Char('x'), data, at(9)), None);
        assert_eq!(page.prompt(), Some(Page::ARMED_KEYS));
        let only = vec![(user("tester"), spun.clone())];
        assert_eq!(page.key(Key::Char('y'), data, at(9)), Some(To::Do(Action::CancelSubtree(only))));
        assert_eq!(page.prompt(), None);

        page.key(Key::Char('k'), data, at(9));
        page.key(Key::Char('k'), data, at(9));
        page.key(Key::Char('x'), data, at(9));
        page.key(Key::Char('j'), data, at(9));
        assert_eq!(page.key(Key::Char('y'), data, at(9)), None, "another key disarms");
        page.key(Key::Home, data, at(9));
        page.key(Key::Char('x'), data, at(9));
        let all = vec![(user("dev-a"), root.clone()), (user("reviewer"), root), (user("tester"), spun)];
        assert_eq!(page.key(Key::Char('y'), data, at(9)), Some(To::Do(Action::CancelSubtree(all))), "the owner not");
    }

    #[test]
    fn t_in_a_thread_shows_its_tree_until_esc() {
        let mut m = model();
        let (root, spun) = delegated(&mut m);
        m.pages.push(View::Thread(crate::tui::thread::Page::new(root.clone())));
        m.focus = Focus::View;
        m.key(Key::Up, at(9));
        assert_eq!(m.key(Key::Char('t'), at(9)), None);
        assert_eq!(m.page().name(), "tree");
        assert_eq!(m.tree_shown(), Some((root.clone(), vec![root.clone(), spun])));
        m.tree_event(Event::Read(root, Some(vec![link("dev-a", "docs", None)]), None), at(9));
        let View::Tree(page) = m.page() else { panic!("not a tree") };
        assert_eq!(page.tree(&m.data, at(9)).ants, 4, "the links read show");
        m.key(Key::Esc, at(9));
        assert_eq!((m.page().name(), m.tree_shown()), ("thread", None));
    }

    #[test]
    fn the_tree_draws_each_node_with_its_turns_spend_and_state_then_the_totals() {
        let mut m = model();
        let (root, spun) = delegated(&mut m);
        m.tree(root.clone());
        if let Some(View::Tree(page)) = m.pages.last_mut() {
            page.spend = Some(vec![spent("dev-a", &root, 0.25), spent("tester", &spun, 0.5)]);
        }
        let shown = crate::test_support::render::rendered(120, 20, |f| crate::tui::frame::draw(f, &m, at(9)));
        assert!(shown.contains("delegation tree · fix the login bug"), "{shown}");
        assert!(shown.contains("└─ dev-a        fix the login bug"), "{shown}");
        assert!(shown.contains("$0.25  answered"), "{shown}");
        assert!(shown.contains("   └─ tester    spin-off: run the e2e suite"), "{shown}");
        assert!(shown.contains("total  3 ants · 3 turns used · $0.75 · 4s"), "{shown}");
        assert!(shown.contains("x cancel subtree"), "{shown}");
    }

    #[test]
    fn a_title_in_wide_characters_is_cut_to_its_column_and_leaves_the_tree_s_other_columns() {
        let mut m = model();
        let root = ThreadId::generate();
        let asked = message(1, &root, "alice", "dev-a", &"ログイン".repeat(30));
        m.stored(vec![asked, message(2, &root, "dev-a", "alice", "fixed")], at(2));
        m.tree(root);
        let shown = crate::test_support::render::rendered(100, 12, |f| crate::tui::frame::draw(f, &m, at(9)));
        let row = shown.lines().find(|l| l.contains("─ dev-a")).expect("the ant's row");
        assert!(row.contains('ロ') && row.contains('…') && row.contains("answered"), "{shown}");
    }

    #[test]
    fn a_cancels_notice_tells_who_stopped_and_what_failed() {
        let thread = ThreadId::generate();
        let outcome = |ant: &str, o: Result<Option<bool>>| (user(ant), thread.clone(), o);
        let (text, level) = notice(&[outcome("dev-a", Ok(Some(false)))]);
        assert_eq!((text.as_str(), level), ("no prompt was running in the subtree", Level::Info));
        let (text, level) = notice(&[
            outcome("tester", Ok(Some(true))),
            outcome("dev-a", Ok(Some(true))),
            outcome("ops", Ok(None)),
            outcome("docs", Err(anyhow::anyhow!("no route"))),
        ]);
        assert_eq!(
            text,
            format!(
                "cancelled the prompts of dev-a, tester; the partial replies follow on their threads; no daemon \
                 answered for ops; asking docs on thread {thread} failed: no route"
            )
        );
        assert_eq!(level, Level::Error);
    }
}
