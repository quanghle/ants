//! The people in the nav: the team's other humans, each online while any device of theirs keeps a presence
//! ([`presence::online`]), with the unread messages they sent the human; and what the direct-message view
//! ([`super::dm`]) reads and sends besides the live feed.

use std::collections::{HashSet, VecDeque};

use ants_daemon::bus;
use ants_proto::{JsDomain, PRESENCE_HEARTBEAT, ThreadId, UserId};
use anyhow::{Result, anyhow};
use async_nats::jetstream;
use futures::FutureExt as _;
use futures::future::LocalBoxFuture;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};

use super::dm::{self, Dms};
use super::model::{Data, Level};
use crate::cmd::send;
use crate::humans::Me;
use crate::live::maybe;
use crate::presence;
use crate::sealed::Sealing;

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
        Some(true) => ("●", Color::Green),
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

/// What [`Io`] read or did.
pub enum Event {
    /// The humans online, or why they could not be read.
    Online(Result<HashSet<UserId>>),
    Sent(Sent),
}

/// The outcome of sending a message ([`dm::Draft`]): its thread, and where it is kept.
pub struct Sent {
    pub to: UserId,
    pub sent: Result<(ThreadId, bus::Stored)>,
}

impl Sent {
    /// What to tell of it.
    pub fn notice(&self) -> (String, Level) {
        match &self.sent {
            Ok((thread, bus::Stored::Hub)) => (format!("sent to {} on thread {thread}", self.to), Level::Info),
            Ok((thread, bus::Stored::Outbox)) => (
                format!(
                    "sent to {} on thread {thread}, kept in this machine's outbox until the hub is reachable",
                    self.to
                ),
                Level::Info,
            ),
            Err(e) => (format!("sending to {}: {e:#}", self.to), Level::Error),
        }
    }
}

/// The presences read every [`PRESENCE_HEARTBEAT`], and the messages sent one at a time, in order.
pub struct Io {
    js: jetstream::Context,
    js_domain: Option<JsDomain>,
    me: Me,
    /// What seals the messages sent.
    sealing: Sealing,
    /// Why the presences are not read, to tell once; None when they are.
    refused: Option<String>,
    may_read: bool,
    ticks: tokio::time::Interval,
    online: Option<LocalBoxFuture<'static, Result<HashSet<UserId>>>>,
    /// The messages to send, after the one in flight.
    to_send: VecDeque<dm::Draft>,
    sending: Option<LocalBoxFuture<'static, Sent>>,
}

impl Io {
    /// Reading and sending as `me`, sealing with `sealing`; `may_read` tells whether its credentials may read every
    /// human's presence ([`presence::online_right`]).
    pub fn new(js: jetstream::Context, js_domain: Option<JsDomain>, me: Me, sealing: Sealing, may_read: bool) -> Self {
        let refused = (!may_read).then(|| {
            format!(
                "the credentials of {} predate reading other humans' presence (permissions version 2); `ants renew` \
                 renews them",
                me.id
            )
        });
        Self {
            js,
            js_domain,
            me,
            sealing,
            refused,
            may_read,
            ticks: tokio::time::interval(PRESENCE_HEARTBEAT),
            online: None,
            to_send: VecDeque::new(),
            sending: None,
        }
    }

    /// The next thing read or done. Cancel-safe, so it can race the terminal's keys and the live feed.
    pub async fn next(&mut self) -> Event {
        if let Some(refused) = self.refused.take() {
            return Event::Online(Err(anyhow!(refused)));
        }
        loop {
            if self.sending.is_none()
                && let Some(draft) = self.to_send.pop_front()
            {
                let (js, js_domain, me, sealing) =
                    (self.js.clone(), self.js_domain.clone(), self.me.clone(), self.sealing.clone());
                self.sending = Some(send_draft(js, js_domain, me, sealing, draft));
            }
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
                sent = maybe(&mut self.sending) => {
                    self.sending = None;
                    return Event::Sent(sent);
                }
            }
        }
    }

    /// Send `draft`, after those asked for before.
    pub fn send(&mut self, draft: dm::Draft) {
        self.to_send.push_back(draft);
    }

    /// Read the presences again now.
    pub fn refresh(&mut self) {
        self.ticks.reset_immediately();
        self.online = None;
    }
}

/// Send `draft` as `me` on a new thread, sealed, as `ants ask @human` does.
fn send_draft(
    js: jetstream::Context,
    js_domain: Option<JsDomain>,
    me: Me,
    sealing: Sealing,
    draft: dm::Draft,
) -> LocalBoxFuture<'static, Sent> {
    async move {
        let mut envelope = send::ask_envelope(&me, None, draft.to.clone(), draft.text);
        let sent = async {
            send::seal_to_human(&js, &me, &sealing, &mut envelope, &mut []).await?;
            send::deliver(&js, js_domain.as_ref(), &envelope).await
        };
        let sent = sent.await.map(|s| (envelope.thread_id, s));
        Sent { to: draft.to, sent }
    }
    .boxed_local()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::tests::user;

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

    #[test]
    fn a_sent_message_tells_its_thread_and_where_it_is_kept() {
        let thread = ThreadId::generate();
        let sent = |sent| Sent { to: user("bob"), sent }.notice();
        assert_eq!(
            sent(Ok((thread.clone(), bus::Stored::Hub))),
            (format!("sent to bob on thread {thread}"), Level::Info)
        );
        assert!(sent(Ok((thread, bus::Stored::Outbox))).0.ends_with("outbox until the hub is reachable"));
        assert_eq!(sent(Err(anyhow!("no stream"))), ("sending to bob: no stream".to_owned(), Level::Error));
    }
}
