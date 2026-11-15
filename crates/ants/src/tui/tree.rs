//! A thread's delegation tree, which `t` opens on a thread: the thread's owner, then each ant messaged on it, on the
//! threads spun off it ([`ants_proto::Envelope::origin_thread`]) or on the threads the links bucket says the link to it
//! was passed on ([`links::passed`]), each nested under the ant that messaged it first, with the title or opening line
//! it was messaged with, its turns left, what it spent and its state; then the totals. Enter opens a node's thread, and
//! `x` then `y` cancels every prompt running in a node's subtree, as `ants cancel` does for each of its ants.
//!
//! The messages come from what the live feed read ([`Delegations`]); the links and the spend are read every while the
//! tree shows ([`Io`]).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;
use std::time::Duration;

use ants_daemon::caps::Spent;
use ants_daemon::direct_kv::DirectKv;
use ants_daemon::links::{self, Passed};
use ants_proto::{CancelRequest, JsDomain, SESSIONS_BUCKET, ThreadId, UserId};
use anyhow::Result;
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use futures::future::LocalBoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt as _};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use super::frame;
use super::model::{Action, Data, Key, Level, To};
use super::work::ThreadState;
use crate::cmd::cancel::{CANCEL_GRACE, CANCEL_WAIT, cancel_outcome};
use crate::live::maybe;
use crate::spend::{self, ThreadReport};
use crate::thread::{Entry, Kind};

/// How often the shown tree's links and spend are read.
const READ_EVERY: Duration = Duration::from_secs(10);
/// How many characters of a message's first line are kept.
const LINE: usize = 120;

/// A message, post or note on a thread, as the tree takes it.
#[derive(Debug, Clone)]
struct Said {
    at: DateTime<Utc>,
    kind: Kind,
    from: Option<UserId>,
    to: Option<UserId>,
    turns: Option<u32>,
    /// Its first line.
    line: String,
}

#[derive(Debug, Default)]
struct OnThread {
    /// By stream sequence.
    said: BTreeMap<u64, Said>,
    /// The thread's owner, as its first message naming one says.
    owner: Option<UserId>,
}

/// What the messages read tell of who messaged whom on each thread, and which threads were spun off which.
#[derive(Debug, Default)]
pub struct Delegations {
    threads: HashMap<ThreadId, OnThread>,
    spun_off: HashMap<ThreadId, BTreeSet<ThreadId>>,
}

impl Delegations {
    /// Take `entry` in, once however often it is read.
    pub fn add(&mut self, entry: &Entry) {
        if entry.forged
            || !matches!(
                entry.kind,
                Kind::Message | Kind::Escalation | Kind::Mention | Kind::Post | Kind::Note | Kind::Dead
            )
        {
            return;
        }
        let thread = self.threads.entry(entry.thread_id.clone()).or_default();
        if thread.owner.is_none() {
            thread.owner.clone_from(&entry.owner);
        }
        thread.said.entry(entry.seq).or_insert_with(|| Said {
            at: entry.stored_at,
            kind: entry.kind,
            from: entry.from.clone(),
            to: entry.to.clone(),
            turns: entry.turns,
            line: entry.text.lines().next().unwrap_or("").chars().take(LINE).collect(),
        });
        if let Some(origin) = entry.origin.as_ref().filter(|o| **o != entry.thread_id) {
            self.spun_off.entry(origin.clone()).or_default().insert(entry.thread_id.clone());
        }
    }

    /// The threads of `root`'s tree: it, then those spun off it.
    pub fn threads(&self, root: &ThreadId) -> Vec<ThreadId> {
        let spun_off = self.spun_off.get(root).into_iter().flatten().filter(|t| *t != root);
        std::iter::once(root.clone()).chain(spun_off.cloned()).collect()
    }
}

/// What a node of the tree is doing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeState {
    /// The owner's row: the state of the thread.
    Thread(ThreadState),
    /// Busy, as its status updates tell.
    Working(String),
    /// A tool request of it waits on a human.
    WaitingOnMe,
    /// A message to it was given up on.
    GaveUp,
    /// It has a message it did not answer yet.
    Queued,
    /// It answered the last message to it.
    Answered,
    /// Only the links bucket tells of it: its messages are older than those read.
    Linked,
}

impl NodeState {
    fn label(&self) -> String {
        match self {
            Self::Thread(state) => state.label().to_owned(),
            Self::Working(stand) => format!("◐ {stand}"),
            Self::WaitingOnMe => "waiting on me".to_owned(),
            Self::GaveUp => "gave up".to_owned(),
            Self::Queued => "queued".to_owned(),
            Self::Answered => "answered".to_owned(),
            Self::Linked => "linked".to_owned(),
        }
    }

    fn style(&self) -> Style {
        match self {
            Self::Thread(state) => Style::new().fg(state.color()),
            Self::Working(_) | Self::WaitingOnMe => Style::new().fg(Color::Yellow),
            Self::GaveUp => Style::new().fg(Color::Red),
            Self::Queued => Style::new().fg(Color::Cyan),
            Self::Answered | Self::Linked => frame::dim(),
        }
    }
}

/// A user on a thread of the tree: the owner at its root, else an ant.
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    pub user: UserId,
    pub thread: ThreadId,
    pub depth: usize,
    /// What the tree draws before the user.
    pub glyph: String,
    /// The thread's title for the first ant messaged on a thread, else the opening line of the first message to it.
    pub line: String,
    /// On another thread than the node it nests under.
    pub spun_off: bool,
    /// The turns left on the last message to it.
    pub turns: Option<u32>,
    /// What it spent on the thread since a human's last message there, once read.
    pub spend: Option<Spent>,
    pub state: NodeState,
    /// How many nodes nest under it, at any depth, which follow it.
    pub under: usize,
}

/// A thread's delegation tree: its nodes, the root first, each followed by those nesting under it; and the totals.
#[derive(Debug, Clone, PartialEq)]
pub struct Tree {
    pub nodes: Vec<Node>,
    /// The channels the root thread was posted in.
    pub via: Vec<String>,
    /// The ants in it.
    pub ants: usize,
    /// The turns its ants spent: one per message from an ant to another.
    pub turns: usize,
    /// What its ants spent, once read.
    pub spend: Option<Spent>,
    /// From its first message to its last, or to now while an ant works.
    pub elapsed: Option<Duration>,
}

/// A message on a thread of the tree, and its stream sequence.
type On<'a> = (&'a ThreadId, u64, &'a Said);
/// A node as first made: its user, its thread and the index of the node it nests under.
type Made = (UserId, ThreadId, usize);

impl Tree {
    /// The tree of `root` from what `data` read, `links` passed on to it and the `spend` read of its threads, at
    /// `now`.
    pub fn build(
        root: &ThreadId,
        data: &Data,
        links: &[Passed],
        spend: Option<&[ThreadReport]>,
        now: DateTime<Utc>,
    ) -> Self {
        let human = |user: &UserId| data.is_human(user);
        let delegations = &data.delegations;
        let mut threads = delegations.threads(root);
        for on in links.iter().filter_map(|l| l.on.as_ref()) {
            if !threads.contains(on) {
                threads.push(on.clone());
            }
        }
        let mut said: Vec<On> = threads
            .iter()
            .filter_map(|t| delegations.threads.get(t).map(|s| (t, s)))
            .flat_map(|(t, s)| s.said.iter().map(move |(seq, said)| (t, *seq, said)))
            .collect();
        said.sort_by_key(|(_, seq, _)| *seq);
        let owner = delegations
            .threads
            .get(root)
            .and_then(|t| t.owner.clone())
            .or_else(|| data.live.summary(root).and_then(|s| s.owner.clone()))
            .or_else(|| said.iter().find(|(t, ..)| *t == root).and_then(|(.., s)| s.from.clone()).filter(human))
            .unwrap_or_else(|| data.me().clone());
        let made = shape(root, owner, &said, links, &human);

        // The first node made on each thread, but for the root.
        let mut first: HashMap<&ThreadId, usize> = HashMap::new();
        for (at, (_, thread, _)) in made.iter().enumerate().skip(1) {
            first.entry(thread).or_insert(at);
        }
        let mut nodes: Vec<Node> = order(&made)
            .into_iter()
            .map(|(at, depth, glyph)| {
                let (user, thread, parent) = &made[at];
                if at == 0 {
                    let state = NodeState::Thread(data.state(thread));
                    return Node::new(user.clone(), thread.clone(), state);
                }
                let on: Vec<(u64, &Said)> =
                    said.iter().filter(|(t, ..)| *t == thread).map(|(_, seq, s)| (*seq, *s)).collect();
                let mut node = Node::of(user, thread, &on, data, spend);
                node.depth = depth;
                node.glyph = glyph;
                node.spun_off = made[*parent].1 != *thread;
                if first.get(thread) == Some(&at) {
                    let title = data.title(thread);
                    node.line = if node.spun_off { format!("spin-off: {title}") } else { title };
                }
                node
            })
            .collect();
        for at in (0..nodes.len()).rev() {
            let depth = nodes[at].depth;
            nodes[at].under = nodes[at + 1..].iter().take_while(|n| n.depth > depth).count();
        }

        let ants = nodes.iter().skip(1).map(|n| &n.user).collect::<BTreeSet<_>>().len();
        let between_ants = |s: &Said| s.from.as_ref().zip(s.to.as_ref()).is_some_and(|(f, t)| !human(f) && !human(t));
        let turns = said.iter().filter(|(.., s)| s.kind == Kind::Message && between_ants(s)).count();
        let spend = spend.map(|_| sum(nodes.iter().filter_map(|n| n.spend)));
        let working = nodes.iter().any(|n| matches!(n.state, NodeState::Working(_)));
        let first = said.iter().map(|(.., s)| s.at).min();
        let last = if working { Some(now) } else { said.iter().map(|(.., s)| s.at).max() };
        let elapsed = first.zip(last).map(|(first, last)| crate::roster::since(first, last));
        let via = data.live.summary(root).map(|s| s.channels.iter().map(|c| format!("#{c}")).collect());
        Self { nodes, via: via.unwrap_or_default(), ants, turns, spend, elapsed }
    }
}

impl Node {
    fn new(user: UserId, thread: ThreadId, state: NodeState) -> Self {
        let (depth, under, glyph, line) = (0, 0, String::new(), String::new());
        Self { user, thread, depth, glyph, line, spun_off: false, turns: None, spend: None, state, under }
    }

    /// The node of `ant` on `thread`, whose messages are `on`, from what `data` read and the `spend` read.
    fn of(ant: &UserId, thread: &ThreadId, on: &[(u64, &Said)], data: &Data, spend: Option<&[ThreadReport]>) -> Self {
        let to_it =
            |s: &Said| s.to.as_ref() == Some(ant) && matches!(s.kind, Kind::Message | Kind::Escalation | Kind::Mention);
        let last = |pick: &dyn Fn(&Said) -> bool| on.iter().filter(|(_, s)| pick(s)).map(|(seq, _)| *seq).max();
        let last_in = last(&to_it);
        let last_out = last(&|s| s.from.as_ref() == Some(ant) && s.kind != Kind::Dead);
        let dead = last(&|s| s.kind == Kind::Dead && s.to.as_ref() == Some(ant));
        let state = if let Some(stand) = data.live.progress.get(thread).and_then(|p| p.status.of(ant)) {
            NodeState::Working(stand)
        } else if data.live.approvals.iter().any(|r| r.ant == *ant && r.thread_id == *thread) {
            NodeState::WaitingOnMe
        } else if dead.is_some_and(|d| Some(d) > last_out && Some(d) >= last_in) {
            NodeState::GaveUp
        } else if last_in > last_out {
            NodeState::Queued
        } else if last_out.is_some() {
            NodeState::Answered
        } else {
            NodeState::Linked
        };
        let mut node = Self::new(ant.clone(), thread.clone(), state);
        node.line = on.iter().find(|(_, s)| to_it(s)).map_or_else(|| data.title(thread), |(_, s)| s.line.clone());
        node.turns = on.iter().rev().find(|(_, s)| to_it(s)).and_then(|(_, s)| s.turns);
        node.spend =
            spend.map(|reports| sum(reports.iter().filter(|r| r.ant == *ant && r.thread == *thread).map(|r| r.spent)));
        node
    }
}

fn sum(spent: impl Iterator<Item = Spent>) -> Spent {
    spent.fold(Spent::default(), |mut sum, s| {
        sum += s;
        sum
    })
}

/// The nodes of the tree of `root`, owned by `owner`, in the order first messaged in `said` (by sequence), then as
/// `links` passed on tell of the others: each ant on a thread nests under the node of the ant that messaged it first
/// there, that ant's own node on the thread, else its first one; under the root when a human messaged it, or no node
/// of the sender is known.
fn shape(root: &ThreadId, owner: UserId, said: &[On], links: &[Passed], human: &dyn Fn(&UserId) -> bool) -> Vec<Made> {
    let mut made: Vec<Made> = vec![(owner, root.clone(), 0)];
    let find = |made: &[Made], user: &UserId, on: &ThreadId| {
        made.iter().skip(1).position(|(u, t, _)| u == user && t == on).map(|at| at + 1)
    };
    let under = |made: &[Made], from: &UserId, on: &ThreadId| {
        if human(from) {
            return Some(0);
        }
        find(made, from, on).or_else(|| made.iter().skip(1).position(|(u, ..)| u == from).map(|at| at + 1))
    };
    for (thread, _, s) in said {
        let (Some(from), Some(to)) = (&s.from, &s.to) else { continue };
        if matches!(s.kind, Kind::Message | Kind::Escalation | Kind::Mention)
            && !human(to)
            && find(&made, to, thread).is_none()
        {
            let at = under(&made, from, thread).unwrap_or(0);
            made.push((to.clone(), (*thread).clone(), at));
        }
    }
    // A link passed on by an ant not in the tree yet waits for one passed on to it, else nests under the root.
    let on = |link: &Passed| link.on.clone().unwrap_or_else(|| root.clone());
    let mut waiting: Vec<&Passed> = links.iter().filter(|l| !human(&l.to)).collect();
    loop {
        let before = waiting.len();
        waiting.retain(|link| {
            let on = on(link);
            if find(&made, &link.to, &on).is_some() {
                return false;
            }
            let Some(at) = under(&made, &link.from, &on) else { return true };
            made.push((link.to.clone(), on, at));
            false
        });
        if waiting.len() == before {
            break;
        }
    }
    for link in waiting {
        if find(&made, &link.to, &on(link)).is_none() {
            made.push((link.to.clone(), on(link), 0));
        }
    }
    made
}

/// The nodes `made` in the order drawn, each followed by those nesting under it, with its depth and the tree drawn
/// before it.
fn order(made: &[Made]) -> Vec<(usize, usize, String)> {
    let mut under: Vec<Vec<usize>> = vec![Vec::new(); made.len()];
    for (at, (.., parent)) in made.iter().enumerate().skip(1) {
        under[*parent].push(at);
    }
    let mut order = vec![(0, 0, String::new())];
    walk(&under, 0, 0, "", &mut order);
    order
}

fn walk(under: &[Vec<usize>], at: usize, depth: usize, prefix: &str, order: &mut Vec<(usize, usize, String)>) {
    let children = &under[at];
    for (n, child) in children.iter().enumerate() {
        let last = n + 1 == children.len();
        order.push((*child, depth + 1, format!("{prefix}{}", if last { "└─ " } else { "├─ " })));
        walk(under, *child, depth + 1, &format!("{prefix}{}", if last { "   " } else { "│  " }), order);
    }
}

#[derive(Debug)]
pub struct Page {
    pub root: ThreadId,
    /// The selected node, kept as nodes come.
    selected: Option<(UserId, ThreadId)>,
    /// `x` was pressed: `y` cancels the selected subtree's prompts.
    armed: bool,
    /// The links to the root passed on, once read.
    pub links: Vec<Passed>,
    /// What the ants spent on the tree's threads, once read.
    pub spend: Option<Vec<ThreadReport>>,
}

impl Page {
    pub const KEYS: &str = "↑↓ select · Enter open thread · x cancel subtree · Esc back";
    pub const ARMED_KEYS: &str = "y cancel every prompt in the selected subtree · any other key keeps them";

    pub fn new(root: ThreadId) -> Self {
        Self { root, selected: None, armed: false, links: Vec::new(), spend: None }
    }

    pub fn keys(&self) -> &'static str {
        if self.armed { Self::ARMED_KEYS } else { Self::KEYS }
    }

    pub fn tree(&self, data: &Data, now: DateTime<Utc>) -> Tree {
        Tree::build(&self.root, data, &self.links, self.spend.as_deref(), now)
    }

    fn at(&self, tree: &Tree) -> usize {
        self.selected
            .as_ref()
            .and_then(|(user, thread)| tree.nodes.iter().position(|n| n.user == *user && n.thread == *thread))
            .unwrap_or(0)
    }

    pub fn key(&mut self, key: Key, data: &Data, now: DateTime<Utc>) -> Option<To> {
        let tree = self.tree(data, now);
        let at = self.at(&tree);
        let armed = std::mem::take(&mut self.armed);
        let to = match key {
            Key::Enter => return tree.nodes.get(at).map(|n| To::Thread(n.thread.clone())),
            Key::Char('x') => {
                self.armed = true;
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
            Key::Up | Key::Char('k') => at.saturating_sub(1),
            Key::Down | Key::Char('j') => at + 1,
            Key::PageUp => at.saturating_sub(10),
            Key::PageDown => at + 10,
            Key::Home => 0,
            Key::End => usize::MAX,
            _ => return None,
        };
        let node = &tree.nodes[to.min(tree.nodes.len() - 1)];
        self.selected = Some((node.user.clone(), node.thread.clone()));
        None
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, data: &Data, now: DateTime<Utc>, focused: bool) {
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
            frame::dim(),
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
            let who = cut(&format!("{}{}", node.glyph, node.user), name);
            let mut row = Line::from(vec![
                Span::raw(format!("{who:<name$}  ")),
                Span::styled(
                    format!("{:<line$}  ", cut(&text, line)),
                    if n == 0 { frame::dim() } else { Style::new() },
                ),
                Span::raw(format!("{turns:>5}  {spend:>7}  ")),
                Span::styled(cut(&node.state.label(), state), node.state.style()),
            ]);
            if n == at {
                row = row.style(frame::highlight(focused));
                selected = lines.len();
            }
            lines.push(row);
        }
        lines.push(Line::raw(""));
        let mut total = vec![format!("{} ants", tree.ants), format!("{} turns used", tree.turns)];
        total.extend(tree.spend.map(|s| format!("${:.2}", s.cost)));
        total.extend(tree.elapsed.map(crate::roster::short));
        lines.push(Line::from(vec![Span::styled("total  ", frame::dim()), Span::raw(total.join(" · "))]));
        let height = usize::from(area.height.saturating_sub(2));
        let top = (selected + 1).saturating_sub(height);
        let title = format!(" delegation tree · {} ", data.title(&self.root));
        let paragraph = Paragraph::new(lines)
            .block(frame::block(title, focused))
            .scroll((u16::try_from(top).unwrap_or(u16::MAX), 0));
        frame.render_widget(paragraph, area);
    }
}

/// `text` in at most `width` characters, an ellipsis ending it when cut.
fn cut(text: &str, width: usize) -> String {
    if text.chars().count() <= width {
        return text.to_owned();
    }
    let mut cut: String = text.chars().take(width.saturating_sub(1)).collect();
    cut.push('…');
    cut
}

/// What the tree's reads and cancels brought.
pub enum Event {
    /// The links to the root and the spend on the tree's threads; None for what could not be read.
    Read(ThreadId, Option<Vec<Passed>>, Option<Vec<ThreadReport>>),
    /// How each cancel asked went: whether the ant's daemon cancelled a prompt, None when none answered.
    Cancelled(Vec<(UserId, ThreadId, Result<Option<bool>>)>),
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

/// The reads of the shown tree, and the cancels asked.
pub struct Io {
    js: jetstream::Context,
    domain: Option<JsDomain>,
    me: UserId,
    /// The root of the tree shown, and the threads known of it.
    watched: Option<(ThreadId, Vec<ThreadId>)>,
    every: tokio::time::Interval,
    read: Option<LocalBoxFuture<'static, Event>>,
    cancels: FuturesUnordered<LocalBoxFuture<'static, Event>>,
}

impl Io {
    pub fn new(js: jetstream::Context, domain: Option<JsDomain>, me: UserId) -> Self {
        let every = tokio::time::interval(READ_EVERY);
        Self { js, domain, me, watched: None, every, read: None, cancels: FuturesUnordered::new() }
    }

    /// The next thing read or cancelled. Cancel-safe, so it can race the terminal's keys and the live feed.
    pub async fn next(&mut self) -> Event {
        loop {
            tokio::select! {
                _ = self.every.tick() => {
                    if self.read.is_none() {
                        self.start_read();
                    }
                }
                event = maybe(&mut self.read) => {
                    self.read = None;
                    if let Event::Read(root, ..) = &event
                        && self.watched.as_ref().is_some_and(|(watched, _)| watched == root)
                    {
                        return event;
                    }
                }
                Some(event) = self.cancels.next() => return event,
            }
        }
    }

    /// Read the links and spend of the tree of the root of `shown` now and every while, with its threads known; with
    /// None, of none.
    pub fn watch(&mut self, shown: Option<(ThreadId, Vec<ThreadId>)>) {
        let again = self.watched.as_ref().map(|(root, _)| root) != shown.as_ref().map(|(root, _)| root);
        self.watched = shown;
        if again {
            self.start_read();
        }
    }

    /// Read everything again now.
    pub fn refresh(&mut self) {
        self.start_read();
    }

    fn start_read(&mut self) {
        let Some((root, threads)) = self.watched.clone() else {
            self.read = None;
            return;
        };
        let (js, domain) = (self.js.clone(), self.domain.clone());
        self.read = Some(
            async move {
                let links =
                    links::passed(&js, &root).await.inspect_err(|e| tracing::debug!("reading the links: {e:#}")).ok();
                let mut threads = threads;
                for on in links.iter().flatten().filter_map(|l| l.on.as_ref()) {
                    if !threads.contains(on) {
                        threads.push(on.clone());
                    }
                }
                let spend = async {
                    let kv = DirectKv::new(&js, domain.as_ref(), SESSIONS_BUCKET).await?;
                    let mut reports = Vec::new();
                    for thread in &threads {
                        reports.extend(spend::thread(&js, &kv, None, thread, &BTreeMap::new()).await?);
                    }
                    anyhow::Ok(reports)
                };
                let spend = spend.await.inspect_err(|e| tracing::debug!("reading the spend: {e:#}")).ok();
                Event::Read(root, links, spend)
            }
            .boxed_local(),
        );
    }

    /// Cancel the prompt of each ant on its thread, as `ants cancel` does, all at once.
    pub fn cancel(&mut self, prompts: Vec<(UserId, ThreadId)>) {
        let (client, me) = (self.js.client(), self.me.clone());
        let cancels = prompts.into_iter().map(move |(ant, thread)| {
            let (client, me) = (client.clone(), me.clone());
            async move {
                let outcome = cancel(&client, &me, &ant, &thread).await;
                (ant, thread, outcome)
            }
        });
        self.cancels.push(async move { Event::Cancelled(futures::future::join_all(cancels).await) }.boxed_local());
    }
}

/// Ask the daemons of `ant` to cancel its prompt on `thread`: whether one did, None when none answered.
async fn cancel(client: &async_nats::Client, me: &UserId, ant: &UserId, thread: &ThreadId) -> Result<Option<bool>> {
    let request = CancelRequest::new(ant.clone(), thread.clone(), me.clone());
    let answers = client.subscribe(request.reply_subject()).await?;
    client.publish(request.subject(), serde_json::to_vec(&request)?.into()).await?;
    client.flush().await?;
    Ok(cancel_outcome(&request, answers, CANCEL_WAIT, CANCEL_GRACE).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::tests::{at, message, user};
    use crate::tui::model::tests::model;
    use crate::tui::model::{Focus, Model, Page as View};

    fn link(from: &str, to: &str, on: Option<&ThreadId>) -> Passed {
        Passed { from: user(from), to: user(to), on: on.cloned() }
    }

    fn spent(ant: &str, thread: &ThreadId, cost: f64) -> ThreadReport {
        ThreadReport { ant: user(ant), thread: thread.clone(), spent: Spent { tokens: 1000, cost }, caps: None }
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
        assert_eq!(page.keys(), Page::ARMED_KEYS);
        let only = vec![(user("tester"), spun.clone())];
        assert_eq!(page.key(Key::Char('y'), data, at(9)), Some(To::Do(Action::CancelSubtree(only))));
        assert_eq!(page.keys(), Page::KEYS);

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
        let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(120, 20)).unwrap();
        terminal.draw(|f| frame::draw(f, &m, at(9))).unwrap();
        let buffer = terminal.backend().buffer();
        let cells: Vec<&str> = buffer.content.iter().map(ratatui::buffer::Cell::symbol).collect();
        let shown = cells.chunks(120).map(|row| row.concat().trim_end().to_owned()).collect::<Vec<_>>().join("\n");
        assert!(shown.contains("delegation tree · fix the login bug"), "{shown}");
        assert!(shown.contains("└─ dev-a        fix the login bug"), "{shown}");
        assert!(shown.contains("$0.25  answered"), "{shown}");
        assert!(shown.contains("   └─ tester    spin-off: run the e2e suite"), "{shown}");
        assert!(shown.contains("total  3 ants · 3 turns used · $0.75 · 4s"), "{shown}");
        assert!(shown.contains("x cancel subtree"), "{shown}");
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
