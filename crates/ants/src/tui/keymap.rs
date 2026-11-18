//! The one table of keys: for each view, and everywhere, what each key does, how important it is and when it can act.
//! The footer, the help and the command palette all read it.
//!
//! A row is an [`Entry`]: the keys it stands for (checked against what the view's key function takes), the label shown
//! for them, a short description for the footer, a long one for the help, a rank (lower first, [`HELP_ONLY`] never in
//! the footer) and a [`When`] telling whether it can act now, or why not. To add a key, add a row to the view's slice
//! in [`entries`], or to [`GLOBAL`] for a key taken everywhere; [`views`] tells which views' rows hold on screen.

use chrono::{DateTime, Utc};
use ratatui::style::Style;
use ratatui::text::Line;

use super::model::{Focus, Key, Model, Page};
use super::{ant, home, question};

/// Whether an entry can act on `model` at the time given: `Err` says why not, as "no thread selected".
pub type When = fn(&Model, DateTime<Utc>) -> Result<(), &'static str>;

/// The rank of an entry that shows in the help only, never in the footer.
pub const HELP_ONLY: u8 = u8::MAX;

/// What the footer always ends in.
pub const HELP_HINT: &str = "? help";

/// One row of the table.
#[derive(Debug, Clone, Copy)]
pub struct Entry {
    /// The keys the row stands for.
    #[expect(dead_code, reason = "the help reads it")]
    pub keys: &'static [Key],
    /// How the footer and the help write the keys.
    pub label: &'static str,
    /// What the keys do, in the footer.
    pub short: &'static str,
    /// What the keys do, in the help.
    #[expect(dead_code, reason = "the help reads it")]
    pub long: &'static str,
    /// How important the row is: lower comes first in the footer.
    pub rank: u8,
    pub when: When,
}

impl Entry {
    const fn new(
        keys: &'static [Key],
        label: &'static str,
        short: &'static str,
        long: &'static str,
        rank: u8,
        when: When,
    ) -> Self {
        Self { keys, label, short, long, rank, when }
    }

    /// Whether the row's keys act on `model` now, else why not.
    pub fn applies(&self, model: &Model, now: DateTime<Utc>) -> Result<(), &'static str> {
        (self.when)(model, now)
    }

    /// The row as the footer shows it: "label short".
    pub fn hint(&self) -> String {
        format!("{} {}", self.label, self.short)
    }
}

/// A view whose keys the table holds; an ant's view holds both its pane's keys and [`View::Ant`]'s.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum View {
    Nav,
    Home,
    Work,
    Channel,
    Thread,
    Dm,
    Search,
    Tree,
    Ant,
    AntQueue,
    Memories,
    Mcp,
    Settings,
    Credentials,
    Held,
}

/// The views whose keys hold on `model`'s screen, the most particular first.
pub fn views(model: &Model) -> Vec<View> {
    if model.focus == Focus::Nav {
        return vec![View::Nav];
    }
    let view = match model.page() {
        Page::Home(_) => View::Home,
        Page::Work(_) => View::Work,
        Page::Channel(_) => View::Channel,
        Page::Thread(_) => View::Thread,
        Page::Dm(_) => View::Dm,
        Page::Search(_) => View::Search,
        Page::Tree(_) => View::Tree,
        Page::Credentials(_) => View::Credentials,
        Page::Held(_) => View::Held,
        Page::Ant(page) => {
            let pane = match page.pane {
                ant::Pane::Threads => View::Work,
                ant::Pane::Queue => View::AntQueue,
                ant::Pane::Memories => View::Memories,
                ant::Pane::Mcp => View::Mcp,
                ant::Pane::Settings => View::Settings,
            };
            return vec![pane, View::Ant];
        }
    };
    vec![view]
}

/// The footer of `model` at most `width` wide: the hints of the views on screen and of everywhere that can act now,
/// most important first, as long as they fit whole, then "? help".
pub fn footer(model: &Model, now: DateTime<Utc>, width: u16) -> Line<'static> {
    let mut rows: Vec<&Entry> = views(model).into_iter().flat_map(entries).chain(GLOBAL).collect();
    rows.sort_by_key(|e| e.rank);
    let hints = rows.into_iter().filter(|e| e.rank != HELP_ONLY && e.applies(model, now).is_ok()).map(Entry::hint);
    fit(hints, width, true)
}

/// A prompt's keys, "·" between them, as many as fit whole in `width`, then "? help" when `help`.
pub fn prompt(keys: &str, width: u16, help: bool) -> Line<'static> {
    fit(keys.split(" · ").map(str::to_owned), width, help)
}

fn fit(hints: impl IntoIterator<Item = String>, width: u16, help: bool) -> Line<'static> {
    let room = usize::from(width);
    let tail = if help { format!(" · {HELP_HINT}") } else { String::new() };
    let mut text = String::new();
    for hint in hints {
        let next = if text.is_empty() { format!(" {hint}") } else { format!("{text} · {hint}") };
        if Line::raw(format!("{next}{tail}")).width() > room {
            break;
        }
        text = next;
    }
    let text = if text.is_empty() && help { format!(" {HELP_HINT}") } else { text + &tail };
    Line::styled(text, Style::new().dim())
}

/// The rows of `view`.
pub fn entries(view: View) -> &'static [Entry] {
    match view {
        View::Nav => NAV,
        View::Home => HOME,
        View::Work => WORK,
        View::Channel => CHANNEL,
        View::Thread => THREAD,
        View::Dm => DM,
        View::Search => SEARCH,
        View::Tree => TREE,
        View::Ant => ANT,
        View::AntQueue => ANT_QUEUE,
        View::Memories => MEMORIES,
        View::Mcp => MCP,
        View::Settings => SETTINGS,
        View::Credentials => CREDENTIALS,
        View::Held => HELD,
    }
}

const UP_DOWN: &[Key] = &[Key::Up, Key::Down, Key::Char('k'), Key::Char('j')];
const PAGES: &[Key] = &[Key::PageUp, Key::PageDown];
const ENDS: &[Key] = &[Key::Home, Key::End];
const DIGITS: &[Key] = &[
    Key::Char('1'),
    Key::Char('2'),
    Key::Char('3'),
    Key::Char('4'),
    Key::Char('5'),
    Key::Char('6'),
    Key::Char('7'),
    Key::Char('8'),
    Key::Char('9'),
];

/// The rows that hold everywhere, under the views'.
pub const GLOBAL: &[Entry] = &[
    Entry::new(&[Key::Esc], "Esc", "back", "back to the previous view, else to the nav", 9, on_view),
    Entry::new(&[Key::Tab], "Tab", "nav/view", "move between the nav and the view", 40, always),
    Entry::new(&[Key::Ctrl('k')], "Ctrl-K", "commands", "the command bar, listing its /commands", 45, always),
    Entry::new(&[Key::Char('/')], "/", "search", "the bar on /search, as in: /search deploy from:dev-a", 50, always),
    Entry::new(
        &[Key::Char('@'), Key::Char('#')],
        "@ #",
        "ask/post",
        "the command bar: @ant asks an ant, #channel posts to a channel",
        70,
        on_view,
    ),
    Entry::new(&[Key::Char('R')], "R", "read again", "read everything again", 80, always),
    Entry::new(&[Key::Char('q')], "q", "quit", "quit", 90, always),
    Entry::new(&[Key::Char('?')], "?", "help", "this help", HELP_ONLY, always),
    Entry::new(&[Key::Interrupt], "Ctrl-C", "quit", "quit", HELP_ONLY, always),
];

const NAV: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "select", "move the selection", 10, always),
    Entry::new(&[Key::Enter], "Enter", "view", "show the selected view", 5, always),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

const HOME: &[Entry] = &[
    Entry::new(&[Key::Char('a')], "a", "allow", "allow the selected tool request once", 1, home_pending),
    Entry::new(&[Key::Char('A')], "A", "always", "allow the selected tool request always", 2, home_pending),
    Entry::new(&[Key::Char('r')], "r", "reject", "reject the selected tool request at once", 3, home_pending),
    Entry::new(
        &[Key::Char('n')],
        "n",
        "note",
        "reject the selected tool request with a note to the ant",
        4,
        home_pending,
    ),
    Entry::new(DIGITS, "1-9", "answer", "answer the selected question by number", 5, home_question),
    Entry::new(
        &[Key::Enter],
        "Enter",
        "open",
        "open the selection: a tool request in full, a question's card, a group of requests, else its thread",
        6,
        home_selected,
    ),
    Entry::new(UP_DOWN, "↑↓", "select", "move the selection", 10, always),
    Entry::new(&[Key::Char('r')], "r", "reply", "reply on the selected thread", 12, home_reply),
    Entry::new(&[Key::Char('g')], "g", "grant", "grant the stuck thread selected more turns", 13, home_stuck),
    Entry::new(
        &[Key::Char('d')],
        "d",
        "done",
        "mark the selected thread done; on a reminder, clear it",
        14,
        home_selected,
    ),
    Entry::new(&[Key::Char('z')], "z", "snooze", "snooze the selected thread", 15, home_selected),
    Entry::new(&[Key::Char('x')], "x", "cancel", "cancel the prompt running on the selected thread", 16, home_selected),
    Entry::new(&[Key::Char('f')], "f", "forward", "forward the selected thread", 17, home_selected),
    Entry::new(&[Key::Char('b')], "b", "remind", "remind of the selected thread later", 18, home_selected),
    Entry::new(&[Key::Char('u')], "u", "unread", "mark the selected thread unread", 19, home_selected),
    Entry::new(&[Key::Char('t')], "t", "tree", "the delegation tree of the selected thread", 20, home_selected),
    Entry::new(&[Key::Char('s')], "s", "digest", "the digest of what came while away", 21, always),
    Entry::new(&[Key::Char('c')], "c", "creds", "the credentials asked for", 22, always),
    Entry::new(&[Key::Char('o')], "o", "held", "the admin orders held", 23, always),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

const THREAD: &[Entry] = &[
    Entry::new(&[Key::Char('a')], "a", "allow", "allow the oldest tool request waiting once", 1, thread_pending),
    Entry::new(&[Key::Char('A')], "A", "always", "allow the oldest tool request waiting always", 2, thread_pending),
    Entry::new(&[Key::Char('r')], "r", "reject", "reject the oldest tool request waiting at once", 3, thread_pending),
    Entry::new(
        &[Key::Char('n')],
        "n",
        "note",
        "reject the oldest tool request waiting with a note to the ant",
        4,
        thread_pending,
    ),
    Entry::new(&[Key::Enter], "Enter", "details", "the oldest tool request waiting in full", 5, thread_pending),
    Entry::new(DIGITS, "1-9", "answer", "answer the question waiting by number", 6, thread_question),
    Entry::new(&[Key::Enter], "Enter", "answer", "focus the card of the question waiting", 7, thread_question),
    Entry::new(&[Key::Char('i')], "i", "reply", "reply on the thread in the command bar", 8, always),
    Entry::new(UP_DOWN, "↑↓", "select", "select a message, the latest at the bottom", 10, always),
    Entry::new(
        &[Key::Char('>')],
        ">",
        "quote",
        "reply to the selected message, quoting it and @mentioning its author when an ant",
        11,
        thread_message,
    ),
    Entry::new(&[Key::Char('y')], "y", "copy", "copy the selected message", 12, thread_message),
    Entry::new(&[Key::Char('+')], "+", "react", "react to the selected message", 13, thread_message),
    Entry::new(
        &[Key::Char('x'), Key::Ctrl('x')],
        "x",
        "cancel",
        "cancel the prompt running on the thread (Ctrl-X too)",
        14,
        always,
    ),
    Entry::new(&[Key::Char('d')], "d", "done", "mark the thread done, again to undo", 15, always),
    Entry::new(&[Key::Char('z')], "z", "snooze", "snooze the thread", 16, always),
    Entry::new(&[Key::Char('o')], "o", "open file", "open the marked attachment", 17, thread_attachment),
    Entry::new(&[Key::Char(']')], "]", "next file", "mark the next attachment", 18, thread_attachments),
    Entry::new(&[Key::Char('p')], "p", "pin", "pin the thread, again to undo", 20, always),
    Entry::new(&[Key::Char('m')], "m", "mute", "mute the thread, again to undo", 21, always),
    Entry::new(&[Key::Char('e')], "e", "archive", "archive the thread, again to undo", 22, always),
    Entry::new(&[Key::Char('f')], "f", "forward", "forward the thread", 23, always),
    Entry::new(&[Key::Char('b')], "b", "remind", "remind of the thread, at the selected message, later", 24, always),
    Entry::new(&[Key::Char('u')], "u", "unread", "mark the thread unread from the selected message", 25, always),
    Entry::new(&[Key::Char('t')], "t", "tree", "the thread's delegation tree", 26, always),
    Entry::new(&[Key::Char('S')], "S", "TL;DR", "a TL;DR of the thread", 27, always),
    Entry::new(&[Key::Char('T')], "T", "tools", "show or hide the tool calls", 28, always),
    Entry::new(&[Key::Char('W')], "W", "withdraw", "withdraw a message or post still waiting", 29, always),
    Entry::new(&[Key::Char('E')], "E", "edit", "withdraw a message or post still waiting, to edit it", 31, always),
    Entry::new(PAGES, "PgUp PgDn", "page", "move the selection further", 92, always),
    Entry::new(ENDS, "Home End", "first/latest", "select the first message, or follow the latest", 95, always),
];

const WORK: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "thread", "select a thread", 10, always),
    Entry::new(&[Key::Enter], "Enter", "open", "open the selected thread", 5, thread_selected),
    Entry::new(
        &[Key::Char('x'), Key::Ctrl('x')],
        "x",
        "cancel",
        "cancel the prompt running on the selected thread (Ctrl-X too)",
        14,
        thread_selected,
    ),
    Entry::new(&[Key::Char('z')], "z", "snooze", "snooze the selected thread", 15, thread_selected),
    Entry::new(&[Key::Char('S')], "S", "TL;DR", "a TL;DR of the selected thread", 16, thread_selected),
    Entry::new(&[Key::Char('u')], "u", "unread", "mark the selected thread unread", 17, thread_selected),
    Entry::new(&[Key::Char('b')], "b", "remind", "remind of the selected thread later", 18, thread_selected),
    Entry::new(&[Key::Char('f')], "f", "forward", "forward the selected thread", 19, thread_selected),
    Entry::new(PAGES, "PgUp PgDn", "page", "move the selection further", 92, always),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

const CHANNEL: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "post", "select a post", 10, always),
    Entry::new(&[Key::Enter], "Enter", "open", "open the selected post's thread", 5, post_selected),
    Entry::new(&[Key::Left, Key::Right], "←→", "tab", "the channel's state tab", 11, always),
    Entry::new(
        &[Key::Char('n')],
        "n",
        "notify",
        "the next notify level: all posts, posts by humans and @mentions, @mentions only",
        12,
        always,
    ),
    Entry::new(&[Key::Char('M')], "M", "members", "manage the channel's members and description", 13, always),
    Entry::new(&[Key::Char('o')], "o", "open file", "open the marked attachment", 14, post_attachment),
    Entry::new(&[Key::Char(']')], "]", "next file", "mark the next attachment", 15, post_attachments),
    Entry::new(PAGES, "PgUp PgDn", "page", "move the selection further", 92, always),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

const DM: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "thread", "select a thread", 10, always),
    Entry::new(&[Key::Enter], "Enter", "open", "open the selected thread", 5, always),
    Entry::new(&[Key::Char('n')], "n", "new message", "write a new message to the person", 6, always),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

const SEARCH: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "match", "select a match", 10, always),
    Entry::new(&[Key::Enter], "Enter", "open", "open the match's thread at it", 5, always),
    Entry::new(PAGES, "PgUp PgDn", "page", "move the selection further", 92, always),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

const TREE: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "select", "select a thread of the tree", 10, always),
    Entry::new(&[Key::Enter], "Enter", "open", "open the selected thread", 5, always),
    Entry::new(&[Key::Char('x')], "x", "cancel subtree", "cancel every prompt in the selected subtree", 11, always),
    Entry::new(&[Key::Char('y')], "y", "confirm", "confirm the cancel x asked for", 1, tree_armed),
    Entry::new(PAGES, "PgUp PgDn", "page", "move the selection further", 92, always),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

const ANT: &[Entry] = &[
    Entry::new(&[Key::Left, Key::Right], "←→", "pane", "the pane: threads, queue, memories, MCP, settings", 20, always),
    Entry::new(&[Key::Char('a')], "a", "ask", "ask the ant in the command bar", 21, always),
    Entry::new(&[Key::Char('w')], "w", "work", "the ant's threads in work", 22, always),
    Entry::new(&[Key::Char('r')], "r", "restart", "restart the ant's harnesses, once y or ! confirms", 23, always),
    Entry::new(
        &[Key::Char('y'), Key::Char('!')],
        "y !",
        "confirm",
        "confirm the restart r asked for: y after the prompts running, ! at once, cancelling them",
        1,
        ant_armed,
    ),
    Entry::new(&[Key::Char('p')], "p", "(un)pause", "pause the ant, or resume it", 24, always),
    Entry::new(&[Key::Char('X')], "X", "retire", "retire the ant, in the command bar", 25, always),
    Entry::new(&[Key::Char('c')], "c", "creds", "the credentials asked for", 26, always),
    Entry::new(&[Key::Char('['), Key::Char(']')], "[ ]", "scroll", "scroll the about or now column", 27, always),
    Entry::new(&[Key::Char('\\')], "\\", "column", "switch between the about and now columns", 28, always),
];

const ANT_QUEUE: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "message", "select a message waiting for the ant", 10, always),
    Entry::new(&[Key::Enter], "Enter", "open", "open the selected message's thread", 5, always),
    Entry::new(PAGES, "PgUp PgDn", "page", "move the selection further", 92, always),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

const MEMORIES: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "scroll", "scroll the memories", 10, always),
    Entry::new(&[Key::Char('u')], "u", "read again", "read the memories again", 11, always),
    Entry::new(PAGES, "PgUp PgDn", "page", "scroll further", 92, always),
    Entry::new(ENDS, "Home End", "top/bottom", "scroll to the top or the bottom", 95, always),
];

const MCP: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "server", "select a server", 10, always),
    Entry::new(&[Key::Char('s'), Key::Enter], "s", "sign in", "sign in to the selected server (Enter too)", 5, always),
    Entry::new(&[Key::Char('y')], "y", "copy address", "copy the address the sign-in gave", 3, mcp_signing),
    Entry::new(&[Key::Char('v')], "v", "hand back", "hand back the address the browser came back to", 4, mcp_signing),
    Entry::new(&[Key::Char('u')], "u", "read again", "read the servers again", 11, always),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

const SETTINGS: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "field", "select a field", 10, always),
    Entry::new(&[Key::Enter], "Enter", "change", "change the selected field", 5, always),
    Entry::new(&[Key::Char('n')], "n", "new field", "set a field not listed", 6, always),
    Entry::new(&[Key::Char('u')], "u", "read again", "read the settings again", 11, always),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

const CREDENTIALS: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "select", "select a request", 10, always),
    Entry::new(&[Key::Char('a')], "a", "approve", "approve or renew the selected request, once y confirms", 5, always),
    Entry::new(&[Key::Char('r')], "r", "reject", "reject the selected request, once y confirms", 6, always),
    Entry::new(&[Key::Char('y')], "y", "confirm", "confirm the order a or r asked for", 1, page_armed),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

const HELD: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "select", "select an order", 10, always),
    Entry::new(&[Key::Char('a')], "a", "confirm", "confirm the selected order, once y confirms", 5, always),
    Entry::new(&[Key::Char('r')], "r", "deny", "deny the selected order, once y confirms", 6, always),
    Entry::new(&[Key::Char('y')], "y", "confirm", "give the answer a or r asked for", 1, page_armed),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

fn always(_: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    Ok(())
}

fn check(holds: bool, why: &'static str) -> Result<(), &'static str> {
    if holds { Ok(()) } else { Err(why) }
}

fn on_view(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    check(model.focus == Focus::View, "the nav is focused")
}

fn thread_selected(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    check(model.target().is_ok(), "no thread selected")
}

fn home_item(model: &Model, now: DateTime<Utc>) -> Option<home::Item> {
    match model.page() {
        Page::Home(page) => page.selected(&model.data, now),
        _ => None,
    }
}

fn home_selected(model: &Model, now: DateTime<Utc>) -> Result<(), &'static str> {
    check(home_item(model, now).is_some(), "nothing selected")
}

fn home_pending(model: &Model, now: DateTime<Utc>) -> Result<(), &'static str> {
    check(home_item(model, now).is_some_and(|i| i.pending().is_some()), "no tool request waits")
}

fn home_reply(model: &Model, now: DateTime<Utc>) -> Result<(), &'static str> {
    match home_item(model, now) {
        None => Err("nothing selected"),
        Some(item) if item.pending().is_some() => Err("r rejects the tool request selected"),
        Some(_) => Ok(()),
    }
}

fn home_question(model: &Model, now: DateTime<Utc>) -> Result<(), &'static str> {
    check(home_item(model, now).is_some_and(|i| i.question.is_some()), "no question waits")
}

fn home_stuck(model: &Model, now: DateTime<Utc>) -> Result<(), &'static str> {
    check(home_item(model, now).is_some_and(|i| i.why == home::Why::Stuck), "no stuck thread selected")
}

fn thread_pending(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    match model.page() {
        Page::Thread(page) => check(page.pending(&model.data).is_some(), "no tool request waits"),
        _ => Err("no thread open"),
    }
}

fn thread_question(model: &Model, now: DateTime<Utc>) -> Result<(), &'static str> {
    match model.page() {
        Page::Thread(page) => {
            let asked = question::keyed(&model.data, &page.history.entries, now).is_some();
            check(asked && page.pending(&model.data).is_none(), "no question waits")
        }
        _ => Err("no thread open"),
    }
}

fn thread_message(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    match model.page() {
        Page::Thread(page) => check(page.selection().is_some(), "no message selected"),
        _ => Err("no thread open"),
    }
}

fn thread_attachment(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    match model.page() {
        Page::Thread(page) => check(page.attachment().is_some(), "no attachment marked"),
        _ => Err("no thread open"),
    }
}

fn thread_attachments(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    match model.page() {
        Page::Thread(page) => check(page.attached().is_some_and(|e| e.attachments.len() > 1), "no other attachment"),
        _ => Err("no thread open"),
    }
}

fn post_selected(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    match model.page() {
        Page::Channel(page) => check(page.post(&model.data).is_some(), "no post selected"),
        _ => Err("no channel open"),
    }
}

fn post_attachment(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    match model.page() {
        Page::Channel(page) => check(page.attachment(&model.data).is_some(), "no attachment marked"),
        _ => Err("no channel open"),
    }
}

fn post_attachments(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    match model.page() {
        Page::Channel(page) => {
            let several = page.post(&model.data).is_some_and(|p| p.attachments.len() > 1);
            check(several, "no other attachment")
        }
        _ => Err("no channel open"),
    }
}

fn tree_armed(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    check(matches!(model.page(), Page::Tree(page) if page.prompt().is_some()), "no cancel asked for")
}

fn ant_armed(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    check(matches!(model.page(), Page::Ant(page) if page.armed()), "no restart asked for")
}

fn mcp_signing(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    check(matches!(model.page(), Page::Ant(page) if page.mcp.signing()), "no sign-in under way")
}

fn page_armed(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    let armed = match model.page() {
        Page::Credentials(page) => page.prompt().is_some(),
        Page::Held(page) => page.prompt().is_some(),
        _ => false,
    };
    check(armed, "nothing asked for")
}
