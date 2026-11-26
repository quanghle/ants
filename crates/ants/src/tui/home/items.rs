//! What waits on the human at a time, as ants-client derives it ([`ants_client::home`]) from what the UI read
//! ([`Data`] as its [`Source`]), with how home shows each reason and the keys it offers on it ([`Shown`]) and the
//! line of an ant's tool request with what assistants decided on it ([`asks`]).

use std::fmt::Write as _;

use ants_client::home::Source;
use ants_proto::{ApprovalRequest, Reminders, ThreadId, ThreadSnoozes, UserId, Uuid};
use chrono::{DateTime, Utc};
use ratatui::style::Color;

pub use ants_client::home::{Id, Why, items, snoozed, waiting};

use super::super::approvals;
use super::super::model::Data;
use crate::tools::State;

/// An item of home, a settled tool request's state the UI's.
pub type Item = ants_client::home::Item<State>;

/// How home shows an item of each reason, and what it offers on it.
pub trait Shown {
    fn color(self) -> Color;
    /// The item's one-key actions but Enter, as home and the compact pane show them.
    fn keys(self) -> &'static str;
    /// What Enter does on the item, in home.
    fn enter(self) -> &'static str;
}

impl Shown for Why {
    fn color(self) -> Color {
        match self {
            Self::Approval => Color::Yellow,
            Self::Question => Color::Cyan,
            Self::Stuck => Color::Red,
            Self::Message => Color::Blue,
            Self::Mention => Color::Magenta,
            Self::Reminder => Color::LightCyan,
            Self::Finished => Color::Green,
        }
    }

    fn keys(self) -> &'static str {
        match self {
            Self::Approval => "a A r answer · n reject with a note · z snooze",
            Self::Question => "1-9 answer · r reply · d done · z snooze",
            Self::Stuck => "g grant · x cancel · d done · z snooze",
            Self::Message | Self::Mention => "r reply · d done · z snooze",
            Self::Finished => "r follow up · d mark done · z snooze",
            Self::Reminder => "d done · b remind again",
        }
    }

    fn enter(self) -> &'static str {
        match self {
            Self::Approval => "Enter details",
            Self::Finished => "Enter review",
            Self::Question => "Enter choose",
            Self::Stuck | Self::Message | Self::Mention | Self::Reminder => "Enter open",
        }
    }
}

impl Source for Data {
    type Settled = State;

    fn me(&self) -> &UserId {
        &self.live.me
    }

    fn facts(&self) -> &ants_client::facts::Facts {
        &self.facts
    }

    fn threads(&self) -> impl Iterator<Item = &ThreadId> {
        self.work()
    }

    fn is_human(&self, user: &UserId) -> bool {
        Data::is_human(self, user)
    }

    fn unread(&self, seq: u64) -> bool {
        Data::unread(self, seq)
    }

    fn done(&self, thread: &ThreadId) -> bool {
        Data::done(self, thread).is_some()
    }

    fn owner(&self, thread: &ThreadId) -> Option<&UserId> {
        self.live.summary(thread)?.owner.as_ref()
    }

    fn working(&self, thread: &ThreadId) -> bool {
        self.live.progress.contains_key(thread)
    }

    fn cleared(&self, seq: u64) -> bool {
        self.dead_cleared.contains(&seq)
    }

    fn answering(&self, id: Uuid) -> bool {
        self.answering.contains(&id)
    }

    fn requests(&self, now: DateTime<Utc>) -> Vec<(ApprovalRequest, Option<State>)> {
        let cards = approvals::cards(self, None, now).into_iter();
        cards.map(|card| (card.request.clone(), card.settled.cloned())).collect()
    }

    fn reminders(&self) -> &Reminders {
        &self.cards.reminders
    }

    fn snoozes(&self) -> &ThreadSnoozes {
        &self.cards.snoozed
    }

    fn asks(&self, request: &ApprovalRequest) -> String {
        asks(self, request)
    }

    fn title(&self, thread: &ThreadId) -> String {
        Data::title(self, thread)
    }
}

/// What an ant asks to run with `request`, and what assistants decided on it for their humans, which waits for the
/// humans' consent.
pub(super) fn asks(data: &Data, request: &ApprovalRequest) -> String {
    decided(data, request, ants_client::home::asks(request))
}

/// Who asks on `item`, the card the keys answer showing what `request`, its oldest, asks.
pub(super) fn asker(data: &Data, item: &Item, request: &ApprovalRequest) -> String {
    match item.requests.len() {
        0 | 1 => decided(data, request, format!("{} asks to run:", request.ant)),
        n => format!("{} asks to run {n} tools, the oldest:", request.ant),
    }
}

/// `text`, then what assistants decided on `request` for their humans: the human's own assistant's to confirm on the
/// page `o` opens.
fn decided(data: &Data, request: &ApprovalRequest, mut text: String) -> String {
    for held in data.live.held.get(&(request.ant.clone(), request.id)).into_iter().flatten() {
        let (assistant, human, verdict) = (&held.assistant, &held.human, ants_client::text::spelling(held.verdict));
        let _ = write!(text, " · {assistant} chose {verdict} for {human}");
        if human == data.me() {
            text.push_str(": o lists it");
        }
    }
    text
}
