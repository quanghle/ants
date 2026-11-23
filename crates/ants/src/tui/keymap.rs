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

use super::ant;
use super::approvals::Asking;
use super::model::{Focus, Key, Model, Page, Prompt};

mod table;
mod when;

pub use table::GLOBAL;
use table::{
    ANT, ANT_QUEUE, BAR, CHANNEL, CREDENTIALS, DM, HELD, HOME, MCP, MEMORIES, NAV, PENDING, REQUEST, SEARCH, SETTINGS,
    START, THREAD, TREE, TYPING, WORK,
};
use when::finding;

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
    pub keys: &'static [Key],
    /// How the footer and the help write the keys.
    pub label: &'static str,
    /// What the keys do, in the footer.
    pub short: &'static str,
    /// What the keys do, in the help.
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
        if self.written(model) {
            return Err("written in the bar: ↑ selects a message first");
        }
        (self.when)(model, now)
    }

    /// Whether the row's key is a printable one written in the bar while typing in a conversation, rather than acting;
    /// n and N step through a find shown in the thread all the same.
    fn written(&self, model: &Model) -> bool {
        let letter = matches!(self.keys.first(), Some(Key::Char(c)) if !matches!(c, '?' | '@' | '#' | '/'));
        let stepping = finding(model) && self.keys.iter().all(|k| matches!(k, Key::Char('n' | 'N')));
        letter && !stepping && !model.ui.bar.focused && model.typing()
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
    /// The command bar, focused.
    Bar,
    /// A tool request shown in full.
    Request,
    /// The waiting requests and questions of a thread.
    Pending,
    /// A thread, a channel's feed or a DM with nothing selected, a printable key written in the bar.
    Typing,
    /// A run of a trail `/trail run` planned, waiting to start.
    Start,
}

impl View {
    /// How the help heads the view's keys.
    pub fn title(self) -> &'static str {
        match self {
            Self::Nav => "the nav",
            Self::Home => "home",
            Self::Work => "work, and an ant's threads",
            Self::Channel => "a channel",
            Self::Thread => "a thread",
            Self::Dm => "a person",
            Self::Search => "a search",
            Self::Tree => "a delegation tree",
            Self::Ant => "an ant",
            Self::AntQueue => "an ant's queue",
            Self::Memories => "an ant's memories",
            Self::Mcp => "an ant's MCP servers",
            Self::Settings => "an ant's settings",
            Self::Credentials => "credentials",
            Self::Held => "held for confirmation",
            Self::Bar => "the command bar",
            Self::Request => "a tool request in full",
            Self::Pending => "a thread's waiting cards",
            Self::Typing => "a conversation, writing",
            Self::Start => "a trail's run to start",
        }
    }
}

/// The views whose keys hold on `model`'s screen, the most particular first.
pub fn views(model: &Model) -> Vec<View> {
    if model.asking().is_some_and(Asking::takes_keys) {
        return vec![View::Request];
    }
    if model.prompt.as_ref().is_some_and(Prompt::covers) {
        return vec![View::Start];
    }
    if model.focus == Focus::Nav {
        return vec![View::Nav];
    }
    if model.page().covers() {
        return vec![View::Pending];
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
    if model.typing() { vec![View::Typing, view] } else { vec![view] }
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
        View::Bar => BAR,
        View::Request => REQUEST,
        View::Pending => PENDING,
        View::Typing => TYPING,
        View::Start => START,
    }
}
