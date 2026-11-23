//! Following the tool requests ([`follow`]): the pending ones, then each new one and how each reported one ends, as
//! `ants tools list --follow` and `ants inbox --follow` show them ([`render`]).

use ants_client::text::spelling;
use ants_daemon::bus::relayed::{self, Heard, Read};
use std::collections::HashSet;
use std::fmt::Write as _;

use ants_proto::{ApprovalDecision, ApprovalRequest, Envelope, STREAM, UserId, Uuid, subject};
use anyhow::{Context as _, Result};
use async_nats::jetstream;
use async_nats::jetstream::stream::{LastRawMessageErrorKind, Stream};
use chrono::{DateTime, TimeDelta, Utc};
use futures::StreamExt as _;
use futures::stream::SelectAll;

use super::requests::{pending_at, qualified, standing};
use super::text::{commands, excerpt};
use super::{HeldAct, State, deciders};
use crate::text::left;

/// What following the tool requests reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Update {
    /// A request waiting for a decision: pending when following started, or new since.
    Pending(ApprovalRequest),
    /// The decisions assistants made on a request reported pending for their humans, which wait for the humans'
    /// consent: all of them, in place of those reported before.
    Held(ApprovalRequest, Vec<HeldAct>),
    /// How a request reported pending ended: decided, or expired.
    Resolved(ApprovalRequest, State),
}

/// A request reported pending and not resolved yet.
struct Open {
    /// Its stream sequence, once read: a request heard live is looked up in the stream when its state is needed.
    seq: Option<u64>,
    request: ApprovalRequest,
    /// When to look again at a request that was still pending right after word of a decision on it came, as that
    /// word may come before the stream stores the decision, or that holds decisions waiting for consent.
    recheck: Option<DateTime<Utc>>,
    /// The decisions held on it last reported.
    held: Vec<HeldAct>,
}

/// How long after word of a decision a request still pending is looked at again.
const RECHECK: TimeDelta = TimeDelta::seconds(1);

/// The request `msg` published live, unless unreadable, for another ant or expired.
fn live_request(msg: &async_nats::Message) -> Option<ApprovalRequest> {
    match serde_json::from_slice::<ApprovalRequest>(&msg.payload) {
        Ok(request) => match request.check_subject(&msg.subject) {
            Err(e) => {
                tracing::warn!(error = %e, "skipping a request for another ant");
                None
            }
            Ok(()) => (!request.is_expired(Utc::now())).then_some(request),
        },
        Err(e) => {
            tracing::warn!(error = %e, "skipping unreadable tool request");
            None
        }
    }
}

/// How often a request holding decisions waiting for consent is looked at again: the human's consent is not heard.
const HELD_RECHECK: TimeDelta = TimeDelta::seconds(5);

/// Report the pending requests, then each new one and how each reported one ends, until the connection closes. Word
/// of a decision comes on the decision subjects (a human's) and in the inbox of each ant with a request reported (an
/// ant's of its escalate_to, or an assistant's for its human); a request is looked at again when one names it, when it
/// expires, and every [`HELD_RECHECK`] while it holds decisions waiting for consent.
pub async fn follow(
    client: &async_nats::Client,
    js: &jetstream::Context,
    mut report: impl FnMut(&Update) -> Result<()>,
) -> Result<()> {
    let stream = js.get_stream(STREAM).await.context("opening the ANTS stream")?;
    // A request heard live is stored after this.
    let stored = stream.cached_info().state.last_sequence;
    // Subscribe before listing, so a request or decision published in between is not missed.
    let mut asked =
        relayed::subscribe(client, subject::ALL_APPROVAL_REQUESTS).await.context("following tool requests")?;
    let mut decided =
        relayed::subscribe(client, subject::ALL_APPROVAL_DECISIONS).await.context("following decisions")?;
    let fresh = Box::pin(pending_at(js, &stream, Utc::now()))
        .await?
        .into_iter()
        .map(|(seq, r, held)| (Some(seq), r, held))
        .collect();
    let mut watch = Watch {
        client,
        js,
        stream,
        stored,
        inboxes: SelectAll::new(),
        heard: HashSet::new(),
        reported: HashSet::new(),
        open: Vec::new(),
        fresh,
    };
    loop {
        watch.take(&mut report).await?;
        let wake = sleep_until(watch.wake());
        let mut named = None;
        tokio::select! {
            msg = asked.next() => {
                let Some(msg) = msg else { return Ok(()) };
                watch.fresh.extend(live_request(&msg).map(|request| (None, request, Vec::new())));
            }
            msg = decided.next() => {
                let Some(msg) = msg else { return Ok(()) };
                if let Ok(d) = serde_json::from_slice::<ApprovalDecision>(&msg.payload) {
                    named = Some((d.ant, d.request_id));
                }
            }
            Some(msg) = watch.inboxes.next(), if !watch.inboxes.is_empty() => {
                if let Ok(envelope) = serde_json::from_slice::<Envelope>(&msg.payload)
                    && let Some(d) = envelope.approval
                {
                    named = Some((d.ant, d.request_id));
                }
            }
            () = wake => {}
        }
        watch.look(named.as_ref(), &mut report).await?;
    }
}

/// A request to report: its stream sequence once read, the request, and the decisions held on it.
type Fresh = (Option<u64>, ApprovalRequest, Vec<HeldAct>);

/// What [`follow`] keeps between the words it hears.
struct Watch<'a> {
    client: &'a async_nats::Client,
    js: &'a jetstream::Context,
    stream: Stream,
    /// The stream's last sequence when following started: a request heard live is stored after it.
    stored: u64,
    /// The inboxes of the ants with a request reported, where their chains' and assistants' decisions come.
    inboxes: SelectAll<Heard>,
    /// The ants whose inbox is followed.
    heard: HashSet<UserId>,
    /// The requests reported, by ant and id.
    reported: HashSet<(UserId, Uuid)>,
    open: Vec<Open>,
    /// The requests to report next.
    fresh: Vec<Fresh>,
}

impl Watch<'_> {
    /// Report the fresh requests not reported yet as pending, with their held decisions, following their ants'
    /// inboxes.
    async fn take(&mut self, report: &mut impl FnMut(&Update) -> Result<()>) -> Result<()> {
        for (seq, request, held) in std::mem::take(&mut self.fresh) {
            if !self.reported.insert((request.ant.clone(), request.id)) {
                continue;
            }
            if self.heard.insert(request.ant.clone()) {
                let inbox = subject::inbox_of(&request.ant);
                self.inboxes
                    .push(relayed::subscribe(self.client, inbox).await.context("following decisions of chain ants")?);
            }
            report(&Update::Pending(request.clone()))?;
            let recheck = (!held.is_empty()).then(|| Utc::now() + HELD_RECHECK);
            if !held.is_empty() {
                report(&Update::Held(request.clone(), held.clone()))?;
            }
            self.open.push(Open { seq, request, recheck, held });
        }
        Ok(())
    }

    /// When a request open is next due a look: when rechecked, else when it expires.
    fn wake(&self) -> Option<DateTime<Utc>> {
        self.open.iter().map(|o| o.recheck.unwrap_or(o.request.expires_at)).min()
    }

    /// Look at the requests open that are due, the one `named` by word of a decision among them, reporting those
    /// resolved and the held decisions changed.
    async fn look(
        &mut self,
        named: Option<&(UserId, Uuid)>,
        report: &mut impl FnMut(&Update) -> Result<()>,
    ) -> Result<()> {
        let now = Utc::now();
        let mut i = 0;
        while i < self.open.len() {
            let o = &mut self.open[i];
            let is_named = named.is_some_and(|(ant, id)| *ant == o.request.ant && *id == o.request.id);
            let due = is_named || o.request.is_expired(now) || o.recheck.is_some_and(|at| at <= now);
            if !due {
                i += 1;
                continue;
            }
            o.recheck = None;
            match Box::pin(resolution(self.js, &self.stream, self.stored, o, now)).await {
                Ok((State::Pending, _)) if o.request.is_expired(now) => {
                    // Only a request no longer in the stream: an expired one is pending no more.
                    tracing::debug!(request = %qualified(&o.request), "no longer following an expired request");
                    self.open.remove(i);
                }
                Ok((State::Pending, held)) => {
                    if held != o.held {
                        report(&Update::Held(o.request.clone(), held.clone()))?;
                        o.held = held;
                    }
                    if is_named {
                        o.recheck = Some(now + RECHECK);
                    } else if !o.held.is_empty() {
                        o.recheck = Some(now + HELD_RECHECK);
                    }
                    i += 1;
                }
                Ok((state, _)) => report(&Update::Resolved(self.open.remove(i).request, state))?,
                Err(e) if o.request.is_expired(now) => {
                    tracing::warn!(request = %qualified(&o.request), "not telling how the request ended: {e:#}");
                    self.open.remove(i);
                }
                Err(e) => {
                    tracing::warn!(request = %qualified(&o.request), "reading the state of the request: {e:#}");
                    i += 1;
                }
            }
        }
        Ok(())
    }
}

/// Sleep until `at`, forever without one.
async fn sleep_until(at: Option<DateTime<Utc>>) {
    match at {
        Some(at) => tokio::time::sleep((at - Utc::now()).to_std().unwrap_or_default()).await,
        None => std::future::pending().await,
    }
}

/// Where `open` stands at `now`, with the decisions held on it while pending; a request heard live is looked up among
/// those stored after sequence `stored`.
async fn resolution(
    js: &jetstream::Context,
    stream: &Stream,
    stored: u64,
    open: &mut Open,
    now: DateTime<Utc>,
) -> Result<(State, Vec<HeldAct>)> {
    let seq = match open.seq {
        Some(seq) => seq,
        None => {
            let Some(seq) = seq_of(stream, &open.request, stored + 1).await? else {
                return Ok((State::Pending, Vec::new()));
            };
            *open.seq.insert(seq)
        }
    };
    let (state, _, held) = standing(js, stream, seq, &open.request, now).await?;
    Ok((state, held))
}

/// The stream sequence of `request`, looked for from sequence `from` on, if stored.
async fn seq_of(stream: &Stream, request: &ApprovalRequest, mut from: u64) -> Result<Option<u64>> {
    loop {
        let msg = match stream.get_first_raw_message_by_subject(&request.subject(), from).await {
            Ok(msg) => msg,
            Err(e) if e.kind() == LastRawMessageErrorKind::NoMessageFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("reading request {}", qualified(request))),
        };
        let relayed = relayed::stored_relayed(Some(&msg.headers), &msg.payload, Read::Consumer);
        if !relayed && serde_json::from_slice::<ApprovalRequest>(&msg.payload).is_ok_and(|r| r.id == request.id) {
            return Ok(Some(msg.sequence));
        }
        from = msg.sequence + 1;
    }
}

/// An update as `ants inbox --follow` shows it among replies: when, what, and how to answer a pending request.
pub fn render(update: &Update, now: DateTime<Utc>) -> String {
    let local = |at: DateTime<Utc>| at.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M").to_string();
    match update {
        Update::Pending(r) => {
            let mut out = format!(
                "{}  {} asks to run a tool on thread {}: request {}\n  {}: {}\n",
                local(r.requested_at),
                r.ant,
                r.thread_id,
                qualified(r),
                r.tool.kind,
                excerpt(&r.tool.title, 200)
            );
            if !r.tool.locations.is_empty() {
                let _ = writeln!(out, "  files: {}", r.tool.locations.join(", "));
            }
            let _ = writeln!(out, "  expires in {}; answer with {}", left(r.expires_at, now), commands(r));
            out
        }
        Update::Held(r, held) => held.iter().fold(String::new(), |mut out, h| {
            let (assistant, human, verdict) = (&h.assistant, &h.human, spelling(h.verdict));
            let _ = writeln!(
                out,
                "{}  {assistant} chose {verdict} on request {} for {human}, held until {human} confirms it: ants \
                 assistant confirm {}",
                local(h.sent_at),
                qualified(r),
                qualified(r)
            );
            out
        }),
        Update::Resolved(r, state) => {
            let (at, how) = match state {
                State::Pending => (now, "is pending".to_owned()),
                State::Decided(d) => (d.decided_at, format!("was answered: {}", spelling(d.verdict))),
                State::DecidedByAnt(by, d) => {
                    (d.decided_at, format!("was answered by {}: {}", deciders(by, d), spelling(d.verdict)))
                }
                State::Expired => (r.expires_at, format!("expired unanswered: {} rejected it", r.ant)),
            };
            format!("{}  request {} of {} {how}\n", local(at), qualified(r), r.ant)
        }
    }
}
