//! The shape of a delegation tree: who messaged whom on a thread and those spun off it, as the live feed read it
//! ([`Delegations`]), and the tree built of it, the links passed on and the spend read ([`Tree`]).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use ants_client::entry::{Entry, Kind};
use ants_daemon::limits::caps::Spent;
use ants_daemon::records::links::Passed;
use ants_proto::{ThreadId, UserId};
use chrono::{DateTime, Utc};

use super::super::model::Data;
use super::super::work::ThreadState;
use crate::spend::ThreadReport;

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
    pub(super) fn label(&self) -> String {
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
                    // A human opens a thread off another only by forwarding to it; an ant, by spinning it off.
                    let forwarded = on.first().and_then(|(_, s)| s.from.as_ref()).is_some_and(human);
                    node.line = match (node.spun_off, forwarded) {
                        (false, _) => title,
                        (true, false) => format!("spin-off: {title}"),
                        (true, true) => format!("forwarded: {title}"),
                    };
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
        let elapsed = first.zip(last).map(|(first, last)| crate::text::since(first, last));
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
