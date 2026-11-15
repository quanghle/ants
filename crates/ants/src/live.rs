//! The live view of the ants shared by the full-screen commands such as `ants dash`: what is read and followed with
//! the CLI's own connection ([`Feed`]), as `ants thread`, `ants watch` and `ants tools list --follow` read it, what
//! was read of it ([`State`], [`History`]), the answers to tool requests, and the terminal drawn on ([`Screen`]).

mod screen;
mod state;
#[cfg(test)]
pub(crate) mod tests;

use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use ants_daemon::reading::{self, Reading};
use ants_proto::{ApprovalRequest, RosterEntry, STREAM, StatusEvent, ThreadId, ThreadMarks, UserId, Verdict, subject};
use anyhow::{Context as _, Result};
use async_nats::connection::State as Connection;
use async_nats::jetstream::{self, consumer};
use chrono::Utc;
use futures::future::LocalBoxFuture;
use futures::{FutureExt as _, StreamExt as _};
use tokio::sync::mpsc;

pub use self::screen::{Screen, warning};
pub use self::state::{History, Link, State};
use crate::thread::{self, Entry, Parser};
use crate::tools::{self, Update};
use crate::{roster, thread_meta};

/// How often the stream is read for what was stored without being heard, such as a leaf node's sourced messages, and
/// following the tool requests starts again after it ended.
const REREAD: Duration = Duration::from_secs(5);
/// How often the roster is read.
const ROSTER_EVERY: Duration = Duration::from_secs(10);

/// Connect as `name` for a live view of what `nats`'s human reads in the window `since`: the feed, and the state to
/// take its events into, with the threads the human archived or left idle to be left out.
pub async fn open(nats: &crate::Nats<'_>, name: &str, since: Duration) -> Result<(Feed, State)> {
    let (client, js) = nats.connect(name).await?;
    start(nats, client, js, since).await
}

/// [`open`] on the connection `client` and `js` that `nats` made.
pub async fn start(
    nats: &crate::Nats<'_>,
    client: async_nats::Client,
    js: jetstream::Context,
    since: Duration,
) -> Result<(Feed, State)> {
    let auto_archive = crate::defaults::auto_archive()?;
    let marks = async { thread_meta::open(nats, &js).await?.marks().await };
    let marks = marks.await.unwrap_or_else(|e| {
        tracing::warn!("not leaving archived threads out: {e:#}");
        ThreadMarks::default()
    });
    let feed = Feed::new(client, js, since).await?;
    let mut state = State::new(nats.me.id.clone(), since);
    (state.marks, state.auto_archive) = (marks, auto_archive);
    Ok((feed, state))
}

/// What the [`Feed`] read or heard.
#[derive(Debug)]
pub enum Event {
    /// Messages read from the stream since the last read, or why reading failed.
    Stored(Result<Vec<Entry>>),
    /// An ant's progress on a thread.
    Status(StatusEvent),
    /// Following the tool requests started (again): it reports the pending ones anew.
    Following,
    /// Word from following the tool requests.
    Approval(Update),
    /// Following the tool requests ended, and why if it failed; it starts again after a while.
    FollowEnded(Result<()>),
    /// The roster read, or why reading it failed.
    Roster(Result<Vec<RosterEntry>>),
    /// The history of a thread, as [`Feed::load_history`] asked.
    History(ThreadId, Result<Vec<Entry>>),
    /// The outcome of [`Feed::decide`]: the line saying so, or why the decision was not stored.
    Decided(Result<String>),
}

type Tail = LocalBoxFuture<'static, (Parser, Result<(Vec<Entry>, u64)>)>;

/// Read the messages stored after sequence `after`, or else those of the window `since`, with `parser`, which keeps
/// the threads of the tool requests read for their decisions. Returns them and the sequence read up to.
fn tail(js: jetstream::Context, mut parser: Parser, after: Option<u64>, since: Duration) -> Tail {
    async move {
        let read = async {
            let stream = js.get_stream(STREAM).await.context("opening the ANTS stream")?;
            let mut last = stream.cached_info().state.last_sequence;
            let start = match after {
                Some(after) if after >= last => return Ok((Vec::new(), after)),
                Some(after) => consumer::DeliverPolicy::ByStartSequence { start_sequence: after + 1 },
                None => thread::window(since),
            };
            let mut entries = Vec::new();
            reading::read_back(&js, "", Reading::Gets, subject::ALL, start, |seq, stored, subject, payload| {
                last = last.max(seq);
                entries.extend(parser.entry(seq, stored.into(), subject, payload));
                ControlFlow::Continue(())
            })
            .await
            .context("reading the stream")?;
            Ok((entries, last))
        }
        .await;
        (parser, read)
    }
    .boxed_local()
}

/// Why `verdict` cannot answer `request`, in the state it was last known in; None when it can.
pub fn refusal(request: &ApprovalRequest, state: &tools::State, verdict: Verdict) -> Option<String> {
    let id = tools::qualified(request);
    if *state != tools::State::Pending {
        Some(format!("request {id} is answered already"))
    } else if !request.options.contains(&verdict) {
        Some(format!("{} is not offered for request {id}", tools::spelling(verdict)))
    } else {
        None
    }
}

/// Store `verdict` on `request` as `ants tools approve` or `reject` does, a decision stored first winning; returns the
/// line saying so, with the allow rule an approval suggests.
async fn decide(js: jetstream::Context, request: ApprovalRequest, verdict: Verdict, by: UserId) -> Result<String> {
    let stream = js.get_stream(STREAM).await.context("opening the ANTS stream")?;
    let (seq, request) = tools::find(&stream, &tools::qualified(&request)).await?;
    let decision = tools::answer_request(&js, &stream, seq, &request, verdict, &by, Utc::now()).await?;
    let mut text = format!("answered request {}/{}: {}", decision.ant, decision.request_id, tools::spelling(verdict));
    if verdict != Verdict::Reject
        && let Ok(Some(suggestion)) = tools::suggestion(&stream, seq, &request).await
    {
        text = format!("{text}; {}", tools::suggestion_text(&request, &suggestion));
    }
    Ok(text)
}

/// Await `work` while there is one; it is taken once done.
pub(crate) async fn maybe<T>(work: &mut Option<LocalBoxFuture<'static, T>>) -> T {
    match work {
        Some(work) => work.await,
        None => std::future::pending().await,
    }
}

/// The live view's connection and its reads in flight, at most one of each kind: the ants traffic heard, the stream
/// read for what was stored, the tool requests followed, the roster read every while, and the thread history and
/// decision asked for.
pub struct Feed {
    client: async_nats::Client,
    js: jetstream::Context,
    since: Duration,
    traffic: async_nats::Subscriber,
    updates: mpsc::UnboundedSender<Update>,
    updated: mpsc::UnboundedReceiver<Update>,
    rereads: tokio::time::Interval,
    rosters: tokio::time::Interval,
    /// The parser for the next read of the stream, None while one is in flight.
    parser: Option<Parser>,
    tail: Option<Tail>,
    /// Something was heard that may have been stored.
    tail_wanted: bool,
    /// The stream sequence read up to, once the threads were read.
    stored_seq: Option<u64>,
    history: Option<LocalBoxFuture<'static, (ThreadId, Result<Vec<Entry>>)>>,
    decision: Option<LocalBoxFuture<'static, Result<String>>>,
    follow: Option<LocalBoxFuture<'static, Result<()>>>,
    /// When following the tool requests may start again.
    follow_at: Instant,
    roster: Option<LocalBoxFuture<'static, Result<Vec<RosterEntry>>>>,
}

impl Feed {
    /// Follow the ants traffic on `client`, to read the stream from the window `since` on.
    pub async fn new(client: async_nats::Client, js: jetstream::Context, since: Duration) -> Result<Self> {
        let traffic = client.subscribe(subject::ALL).await.context("following ants traffic")?;
        let (updates, updated) = mpsc::unbounded_channel();
        Ok(Self {
            client,
            js,
            since,
            traffic,
            updates,
            updated,
            rereads: tokio::time::interval(REREAD),
            rosters: tokio::time::interval(ROSTER_EVERY),
            parser: Some(Parser::default()),
            tail: None,
            tail_wanted: true,
            stored_seq: None,
            history: None,
            decision: None,
            follow: None,
            follow_at: Instant::now(),
            roster: None,
        })
    }

    pub fn js(&self) -> &jetstream::Context {
        &self.js
    }

    pub fn connected(&self) -> bool {
        self.client.connection_state() == Connection::Connected
    }

    /// The next thing read or heard. Cancel-safe, so it can race the terminal's keys; awaiting it again picks up
    /// where it left off.
    pub async fn next(&mut self) -> Event {
        loop {
            let connected = self.connected();
            if connected
                && self.tail_wanted
                && let Some(parser) = self.parser.take()
            {
                self.tail_wanted = false;
                self.tail = Some(tail(self.js.clone(), parser, self.stored_seq, self.since));
            }
            if connected && self.follow.is_none() && Instant::now() >= self.follow_at {
                self.follow_tools();
                return Event::Following;
            }
            tokio::select! {
                Some(msg) = self.traffic.next() => {
                    if let Some(event) = self.heard(&msg) {
                        return Event::Status(event);
                    }
                }
                Some(update) = self.updated.recv() => return Event::Approval(update),
                _ = self.rereads.tick() => self.tail_wanted = true,
                _ = self.rosters.tick() => self.read_roster(),
                (parser, read) = maybe(&mut self.tail) => {
                    self.tail = None;
                    self.parser = Some(parser);
                    let read = read.map(|(entries, last)| {
                        self.stored_seq = Some(self.stored_seq.unwrap_or_default().max(last));
                        entries
                    });
                    return Event::Stored(read);
                }
                (thread, read) = maybe(&mut self.history) => {
                    self.history = None;
                    return Event::History(thread, read);
                }
                outcome = maybe(&mut self.decision) => {
                    self.decision = None;
                    return Event::Decided(outcome);
                }
                followed = maybe(&mut self.follow) => {
                    self.follow = None;
                    self.follow_at = Instant::now() + REREAD;
                    return Event::FollowEnded(followed);
                }
                read = maybe(&mut self.roster) => {
                    self.roster = None;
                    return Event::Roster(read);
                }
            }
        }
    }

    /// Take in `msg`, heard on the ants traffic: a status event, or else a hint that the stream has more to read.
    fn heard(&mut self, msg: &async_nats::Message) -> Option<StatusEvent> {
        if msg.subject.starts_with("ants.status.") {
            serde_json::from_slice::<StatusEvent>(&msg.payload).ok().filter(|e| e.check_subject(&msg.subject).is_ok())
        } else {
            self.tail_wanted = true;
            None
        }
    }

    /// Follow the tool requests, sending their updates to `updated`.
    fn follow_tools(&mut self) {
        let (client, js, updates) = (self.client.clone(), self.js.clone(), self.updates.clone());
        let follow = async move {
            tools::follow(&client, &js, |update| {
                let _ = updates.send(update.clone());
                Ok(())
            })
            .await
        };
        self.follow = Some(follow.boxed_local());
    }

    fn read_roster(&mut self) {
        if self.roster.is_none() {
            let js = self.js.clone();
            self.roster = Some(async move { roster::list(&js).await }.boxed_local());
        }
    }

    /// Read the history of `thread`, dropping any read of another one.
    pub fn load_history(&mut self, thread: ThreadId) {
        let js = self.js.clone();
        self.history = Some(
            async move {
                let read = thread::history(&js, &thread, None, false, None).await;
                (thread, read)
            }
            .boxed_local(),
        );
    }

    /// Store `verdict` on `request` by `by`, the first decision winning, as `ants tools approve` and `reject` do.
    pub fn decide(&mut self, request: ApprovalRequest, verdict: Verdict, by: UserId) {
        self.decision = Some(decide(self.js.clone(), request, verdict, by).boxed_local());
    }

    /// Read again what may have missed changes: the stream, the tool requests and the roster.
    pub fn refresh(&mut self) {
        self.follow = None;
        self.follow_at = Instant::now();
        self.tail_wanted = true;
        self.roster = None;
        self.read_roster();
    }
}
