//! The people in the nav: the team's other humans, each online while any device of theirs keeps a presence
//! ([`presence::online`]), with the unread messages they sent the human; and what the direct-message view
//! ([`super::dm`]) reads besides the live feed.

use std::collections::HashSet;

use ants_proto::{PRESENCE_HEARTBEAT, UserId};
use anyhow::{Result, anyhow};
use async_nats::jetstream;
use futures::FutureExt as _;
use futures::future::LocalBoxFuture;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};

use super::dm::{self, Dms};
use super::model::Data;
use super::style;
use crate::live::maybe;
use crate::people::presence;

/// What is known of the other humans.
#[derive(Debug, Default)]
pub struct People {
    /// The humans with a device following, once read; unknown until then, or while the presences cannot be read.
    online: Option<HashSet<UserId>>,
    /// Whether a failure to read the presences was told: only the first is.
    told: bool,
    /// The threads with messages between the human and each other user.
    pub dms: Dms,
}

impl People {
    /// Whether `human` has a device following; None while unknown.
    pub fn online(&self, human: &UserId) -> Option<bool> {
        self.online.as_ref().map(|online| online.contains(human))
    }

    /// Take the presences read: what to tell of a failure, the first time only, as the UI goes on without them.
    pub fn read(&mut self, read: Result<HashSet<UserId>>) -> Option<String> {
        match read {
            Ok(online) => self.online = Some(online),
            Err(e) if !self.told => {
                self.told = true;
                return Some(format!("not showing who is online: {e:#}"));
            }
            Err(e) => tracing::debug!("reading the presences: {e:#}"),
        }
        None
    }
}

/// How a person shows in the nav: online or not, and how many messages from that person the human has not read.
pub fn nav_line(data: &Data, person: &UserId) -> Line<'static> {
    let (mark, color) = match data.people.online(person) {
        Some(true) => ("●", style::CONNECTED),
        Some(false) => ("○", Color::DarkGray),
        None => ("·", Color::DarkGray),
    };
    let mut spans = vec![Span::styled(mark, Style::new().fg(color)), Span::raw(format!(" {person}"))];
    let unread = dm::unread(data, person);
    if unread > 0 {
        spans.push(Span::styled(format!(" {unread}"), Style::new().fg(Color::Yellow).bold()));
    }
    Line::from(spans)
}

/// What [`Io`] read.
pub enum Event {
    /// The humans online, or why they could not be read.
    Online(Result<HashSet<UserId>>),
}

/// The presences read every [`PRESENCE_HEARTBEAT`].
pub struct Io {
    js: jetstream::Context,
    /// Why the presences are not read, to tell once; None when they are.
    refused: Option<String>,
    may_read: bool,
    ticks: tokio::time::Interval,
    online: Option<LocalBoxFuture<'static, Result<HashSet<UserId>>>>,
}

impl Io {
    /// Reading as `me`; `may_read` tells whether its credentials may read every human's presence
    /// ([`presence::online_right`]).
    pub fn new(js: jetstream::Context, me: &UserId, may_read: bool) -> Self {
        let refused = (!may_read).then(|| format!("the credentials of {me} may not read humans' presence"));
        Self { js, refused, may_read, ticks: tokio::time::interval(PRESENCE_HEARTBEAT), online: None }
    }

    /// The next thing read. Cancel-safe, so it can race the terminal's keys and the live feed.
    pub async fn next(&mut self) -> Event {
        if let Some(refused) = self.refused.take() {
            return Event::Online(Err(anyhow!(refused)));
        }
        loop {
            tokio::select! {
                _ = self.ticks.tick(), if self.may_read => {
                    if self.online.is_none() {
                        let js = self.js.clone();
                        self.online = Some(async move { presence::online(&js).await }.boxed_local());
                    }
                }
                read = maybe(&mut self.online) => {
                    self.online = None;
                    return Event::Online(read);
                }
            }
        }
    }

    /// Read the presences again now.
    pub fn refresh(&mut self) {
        self.ticks.reset_immediately();
        self.online = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::live::user;

    #[test]
    fn presences_are_unknown_until_read_and_a_failure_is_told_once() {
        let mut people = People::default();
        assert_eq!(people.online(&user("bob")), None);
        assert!(people.read(Err(anyhow!("denied"))).is_some_and(|t| t.contains("not showing who is online")));
        assert_eq!(people.read(Err(anyhow!("denied"))), None, "told once");
        people.read(Ok(HashSet::from([user("bob")])));
        assert_eq!((people.online(&user("bob")), people.online(&user("carol"))), (Some(true), Some(false)));
        people.read(Err(anyhow!("timed out")));
        assert_eq!(people.online(&user("bob")), Some(true), "what was read stays");
    }
}
