//! Holding the messages while the ant's own harness cannot answer, signed out, limited or failing to start: the
//! humans told, the fallbacks that answer meanwhile, and the checks that let the messages go on; and while the hub
//! cannot take what they need, its JetStream store full or out of reach.

use std::time::Duration;

use ants_proto::State;
use async_nats::jetstream;

use crate::bus;
use crate::harness::{self, SignedOut};
use crate::limits::breaker::{self, Breaker, StartFailed};
use crate::limits::fallback::{Failover, OnFallback, OutageNotice};
use crate::limits::ratelimit::{self, Limited, Throttle};
use crate::limits::signin::{self, SignIn};
use crate::monitoring::status;
use crate::sync::MutexExt as _;

use super::{Inbox, Incoming, mcp_sign_in};

/// What holds the messages while the ant's own harness cannot answer, and the sign-ins to MCP servers to tell.
#[derive(Default)]
pub(super) struct Holds {
    /// Whether the harness is signed out, holding the messages.
    pub(super) sign_in: SignIn,
    /// Whether the harness keeps failing to start, holding the messages.
    pub(super) breaker: Breaker,
    /// Whether messages wait out a provider limit.
    pub(super) throttle: Throttle,
    /// Which fallbacks answer while the harness is signed out, out of quota or failing to start.
    pub(super) failover: Failover,
    /// The sign-ins to MCP servers the harness's sessions made as they opened, until told.
    pub(super) mcp_sign_ins: mcp_sign_in::Untold,
    /// Whether messages wait in hand for the hub to store what they need, holding the messages.
    pub(super) stall: Stall,
    /// The messages never to be answered whose dead letter waits in hand for the hub.
    pub(super) junk: Junk,
    /// The deliveries of messages in hand that were no attempt.
    pub(super) uncounted: Uncounted,
}

/// Messages never to be answered ([`Inbox::bury_unanswerable`]) whose dead letter the hub could not take, its store
/// full or out of reach, each with why it is buried: held in hand, spending no delivery, until the hub is tried again
/// ([`Inbox::hub_back`]).
#[derive(Default)]
pub(super) struct Junk(std::sync::Mutex<Vec<(jetstream::Message, String)>>);

impl Junk {
    /// Hold `msg`, buried for `error`, in place of an earlier delivery of it held already.
    pub(super) fn hold(&self, msg: &jetstream::Message, error: &str) {
        let mut held = self.0.locked();
        held.retain(|(kept, _)| super::stream_seq(kept) != super::stream_seq(msg));
        held.push((msg.clone(), error.to_owned()));
    }

    pub(super) fn is_empty(&self) -> bool {
        self.0.locked().is_empty()
    }

    /// The messages held, to tell the server they are still being worked on.
    pub(super) fn messages(&self) -> Vec<jetstream::Message> {
        self.0.locked().iter().map(|(msg, _)| msg.clone()).collect()
    }

    /// Take the messages held, to bury them again.
    fn take(&self) -> Vec<(jetstream::Message, String)> {
        std::mem::take(&mut *self.0.locked())
    }
}

/// The deliveries the server made of messages while a copy was in hand, their progress acks lost, as while the hub was
/// out of reach for longer than the ack wait: nats-server counts each, but none is an attempt. By stream sequence, the
/// delivery counts the server gave them, until the message is done with.
#[derive(Default)]
pub(super) struct Uncounted(std::sync::Mutex<std::collections::HashMap<u64, Vec<i64>>>);

impl Uncounted {
    /// Note delivery `delivered` of the message at stream sequence `seq`, which came while a copy of it was in hand.
    pub(super) fn add(&self, seq: u64, delivered: i64) {
        self.0.locked().entry(seq).or_default().push(delivered);
    }

    /// Of delivery `delivered` of the message at stream sequence `seq`: the attempt it is, the deliveries before it
    /// that were no attempt left out, and the most deliveries the server counted of the message.
    pub(super) fn standing(&self, seq: u64, delivered: i64) -> (i64, i64) {
        let uncounted = self.0.locked();
        let made = uncounted.get(&seq).map_or(&[][..], Vec::as_slice);
        let before = made.iter().filter(|&&made| made < delivered).count() as i64;
        (delivered - before, made.iter().copied().fold(delivered, i64::max))
    }

    /// Forget the message at stream sequence `seq`, done with.
    pub(super) fn forget(&self, seq: u64) {
        self.0.locked().remove(&seq);
    }
}

/// What of the hub a message waits for in hand, its handling having failed only for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Stalled {
    /// JetStream refused to store for want of room ([`bus::store_refused`]).
    StoreFull,
    /// JetStream did not answer ([`bus::OutOfReach`]).
    OutOfReach,
}

impl std::fmt::Display for Stalled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::StoreFull => "the hub's JetStream store is full",
            Self::OutOfReach => "the hub is out of reach",
        })
    }
}

/// Whether messages wait in hand for the hub, and for what; and what of it the log told last.
#[derive(Default)]
pub(super) struct Stall(std::sync::Mutex<(Option<Stalled>, Option<Stalled>)>);

impl Stall {
    pub(super) fn is_on(&self) -> bool {
        self.0.locked().0.is_some()
    }

    /// Note that messages wait for `why`: true when the log has not told so since a message was last handled.
    fn on(&self, why: Stalled) -> bool {
        let mut stall = self.0.locked();
        stall.0 = Some(why);
        stall.1.replace(why) != Some(why)
    }

    fn off(&self) {
        self.0.locked().0 = None;
    }

    /// Note how a message's handling ended: once one is done, the next stall is told again.
    pub(super) fn settled(&self, handled: &super::handling::Handled) {
        if matches!(handled, super::handling::Handled::Done) {
            self.0.locked().1 = None;
        }
    }
}

impl Inbox<'_> {
    /// What of the hub handling a message failed for with `e`, if only for it: JetStream refusing to store for want of
    /// room, or not answering, which a request every user's credentials allow confirms ([`bus::in_reach`]); a stream
    /// missing though JetStream answers is an ordinary failure.
    pub(super) async fn stalled_by(&self, e: &anyhow::Error) -> Option<Stalled> {
        if bus::store_refused(e) {
            Some(Stalled::StoreFull)
        } else if bus::hub_out_of_reach(e) || bus::unanswered(e) && !bus::in_reach(self.js).await {
            Some(Stalled::OutOfReach)
        } else {
            None
        }
    }

    /// Hold the messages, `incoming` having failed only for `why`, with `e`: none is prompted until the hub is tried
    /// again ([`Self::hub_back`]), and none spends an attempt meanwhile.
    pub(super) fn wait_for_hub(&self, incoming: &Incoming, why: Stalled, e: &anyhow::Error) {
        let thread = &incoming.envelope.thread_id;
        status::state(self.client, self.ant, thread, State::Idle);
        let error = format!("{e:#}");
        if self.holds.stall.on(why) {
            tracing::warn!(%thread, %error, "{why}; messages wait in hand, spending no attempt, until it takes them");
        } else {
            tracing::info!(%thread, %error, "{why}; the message waits in hand");
        }
    }

    /// Wait `store_every`, then let the messages held for the hub go on: the first to fail for it again holds them
    /// again.
    pub(super) async fn hub_back(&self) {
        tokio::time::sleep(self.pacing.store_every).await;
        tracing::info!("trying the messages waiting for the hub again");
        self.holds.stall.off();
    }

    /// Hold `msg`, never to be answered, whose dead letter for `error` the hub could not take for `why`, with `e`.
    pub(super) fn hold_junk(&self, msg: &jetstream::Message, error: &str, why: Stalled, e: &anyhow::Error) {
        self.holds.junk.hold(msg, error);
        let error = format!("{e:#}");
        if self.holds.stall.on(why) {
            tracing::warn!(%error, "{why}; messages wait in hand, spending no attempt, until it takes them");
        } else {
            tracing::info!(%error, "{why}; the dead letter of a message never to be answered waits in hand");
        }
    }

    /// Bury again the messages never to be answered held for the hub ([`Self::hold_junk`]).
    pub(super) async fn bury_held_junk(&self) {
        for (msg, error) in self.holds.junk.take() {
            self.bury_unanswerable(&msg, &error).await;
        }
    }

    /// Why the ant's own harness cannot answer now, if it cannot: messages go to a fallback meanwhile, if one is up.
    pub(super) fn outage(&self) -> Option<&'static str> {
        if self.holds.sign_in.is_out() {
            Some("signed out")
        } else if self.holds.breaker.is_open() {
            Some("failing to start")
        } else if self.holds.throttle.is_quota_held() {
            Some("out of quota")
        } else {
            None
        }
    }

    /// The fallback messages go to at `now`: the first not passed over, while the ant's own harness cannot answer.
    pub(super) fn fallback(&self, now: tokio::time::Instant) -> Option<usize> {
        self.outage()?;
        self.holds.failover.pick(self.pool.fallback_count(), now)
    }

    /// How long the fallback `e` came from is passed over, when it turned the prompt away: signed out, limited or
    /// failing to start.
    fn turned_away(&self, e: &anyhow::Error) -> Option<Duration> {
        if let Some(limited) = e.downcast_ref::<Limited>() {
            return Some(limited.retry_after.unwrap_or(self.pacing.fallback_retry));
        }
        (e.is::<SignedOut>() || e.is::<StartFailed>()).then_some(self.pacing.fallback_retry)
    }

    /// Where a notice of an outage `incoming` met goes, naming `fallbacks`.
    pub(super) fn outage_notice<'a>(&'a self, incoming: &'a Incoming, fallbacks: &'a [String]) -> OutageNotice<'a> {
        OutageNotice {
            ant: self.ant,
            envelope: &incoming.envelope,
            post: incoming.post.is_some(),
            machine: self.machine,
            fallbacks,
            humans: self.humans,
        }
    }

    /// The ant's own harness answers again: no fallback serves.
    fn back_on_own(&self) {
        if self.holds.failover.clear() {
            tracing::info!("the harness answers again; fallbacks stand down");
        }
        self.workload.set_fallback(None);
    }

    /// Hold the messages, `incoming` having found the harness signed out, show that in the colony entry, and tell its
    /// thread's owner, else the default human, how to sign it in, unless told already since the harness last worked.
    /// The notice goes to the human directly: no ant up the chain can sign the harness in.
    pub(super) async fn wait_for_sign_in(&self, incoming: &Incoming, signed_out: &SignedOut) {
        let thread = &incoming.envelope.thread_id;
        status::state(self.client, self.ant, thread, State::SignedOut);
        let tell = self.holds.sign_in.out();
        if let Some(since) = self.holds.sign_in.out_since() {
            let sign_in = signed_out.sign_in.clone();
            self.workload.set_signed_out(Some(ants_proto::SignInNeeded { since, sign_in }));
        }
        let Some(since) = tell else {
            tracing::info!(%thread, error = %signed_out, "harness signed out; the message waits");
            return;
        };
        tracing::warn!(%thread, error = %signed_out, "harness signed out; messages wait until it is signed in");
        let fallbacks = self.pool.fallbacks();
        let notice = signin::notice(&self.outage_notice(incoming, &fallbacks), signed_out, self.pacing.sign_in_every);
        let id = format!("sign-in-{}-{since}", self.ant);
        if let Err(e) = bus::publish_json(self.js, notice.subject(), &notice, Some(&id)).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "telling a human to sign the harness in");
            self.holds.sign_in.untold();
        }
    }

    /// Check the harness every `sign_in_every` until a handshake with it succeeds, then clear the colony entry's
    /// signed-out state and let the messages go on.
    pub(super) async fn signed_in(&self) {
        loop {
            tokio::time::sleep(self.pacing.sign_in_every).await;
            let checked = harness::handshake(self.pool.launch()).await;
            match checked.readiness {
                harness::Readiness::Ready { agent } => {
                    tracing::info!(%agent, "harness signed in; answering the waiting messages");
                    self.holds.sign_in.back();
                    self.workload.set_signed_out(None);
                    if self.outage().is_none() {
                        self.back_on_own();
                    }
                    return;
                }
                harness::Readiness::SignedOut { detail } => {
                    tracing::debug!(%detail, "harness still signed out");
                }
                harness::Readiness::Missing => tracing::warn!("harness not installed; messages wait"),
                harness::Readiness::Failed { detail } => {
                    tracing::warn!(%detail, "checking the signed-out harness failed; messages wait");
                }
            }
        }
    }

    /// Hold the messages, `incoming` having met a provider limit, for the backoff, and show that in the colony entry.
    /// A rate limit passes quietly; an exhausted quota is told to the thread's owner, else the default human, once
    /// until a message gets past a limit.
    pub(super) async fn wait_out_limit(&self, incoming: &Incoming, limited: &Limited) {
        let thread = &incoming.envelope.thread_id;
        status::state(self.client, self.ant, thread, State::Idle);
        let hold = self.holds.throttle.hit(limited, &self.pacing.backoffs);
        self.workload.set_limited(Some(hold.shown.clone()));
        let until = hold.shown.until;
        if limited.kind == ants_proto::LimitKind::OpenFiles {
            tracing::warn!(%thread, error = %limited, %until, "the daemon is out of open files; messages wait");
            return;
        }
        if !hold.tell {
            tracing::info!(%thread, error = %limited, %until, "provider limit; messages wait");
            return;
        }
        tracing::warn!(%thread, error = %limited, %until, "provider quota exhausted; messages wait");
        let fallbacks = self.pool.fallbacks();
        let max = self.pacing.backoffs.quota.max;
        let notice = ratelimit::notice(&self.outage_notice(incoming, &fallbacks), limited, &hold.shown, max);
        let id = format!("quota-{}-{}", self.ant, hold.shown.since.timestamp_millis());
        if let Err(e) = bus::publish_json(self.js, notice.subject(), &notice, Some(&id)).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "telling a human of the exhausted quota");
            self.holds.throttle.untold();
        }
    }

    /// Hold the messages, `incoming` having found the harness failing to start once too often ([`Breaker::failed`]),
    /// show that in the colony entry, and tell its thread's owner, else the default human, how to check the harness,
    /// unless told already since a message last got past its start. The notice goes to the human directly, carrying a
    /// level-0 escalation so it is pushed: no ant up the chain can mend the harness.
    pub(super) async fn wait_for_repair(&self, incoming: &Incoming, failed: &StartFailed, hold: breaker::Hold) {
        let thread = &incoming.envelope.thread_id;
        status::state(self.client, self.ant, thread, State::Idle);
        self.workload.set_failing(Some(hold.shown.clone()));
        if !hold.tell {
            tracing::info!(%thread, error = %failed, "harness failing to start; the message waits");
            return;
        }
        tracing::warn!(%thread, error = %failed, "harness keeps failing to start; messages wait until a check starts it");
        let fallbacks = self.pool.fallbacks();
        let mut notice = breaker::notice(&self.outage_notice(incoming, &fallbacks), failed, self.pacing.sign_in_every);
        self.redactor.redact_string(&mut notice.text);
        let id = format!("harness-{}-{}", self.ant, hold.shown.since.timestamp_millis());
        if let Err(e) = bus::publish_json(self.js, notice.subject(), &notice, Some(&id)).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "telling a human the harness fails to start");
            self.holds.breaker.untold();
        }
    }

    /// Check the harness every `sign_in_every`, starting it as for a thread ([`harness::probe`]), until it starts or
    /// says it is signed out or limited, then clear the colony entry's failing state and let the messages go on.
    pub(super) async fn repaired(&self) {
        loop {
            tokio::time::sleep(self.pacing.sign_in_every).await;
            match harness::probe(self.pool.launch()).await {
                Ok(()) => tracing::info!("harness started; answering the waiting messages"),
                Err(e) if e.is::<SignedOut>() || e.is::<Limited>() => {
                    tracing::info!(error = %e, "harness started; trying the waiting messages again");
                }
                Err(e) => {
                    let error = format!("{e:#}");
                    tracing::warn!(error, "harness still fails to start; messages wait");
                    self.workload.set_failing(self.holds.breaker.still_failing(&self.redactor.redact(&error)));
                    continue;
                }
            }
            self.holds.breaker.closed();
            self.workload.set_failing(None);
            if self.outage().is_none() {
                self.back_on_own();
            }
            return;
        }
    }

    /// Wait until the backoff of the provider limit passes, then clear the colony entry's limit and let the messages
    /// go on.
    pub(super) async fn limit_passed(&self) {
        while let Some(until) = self.holds.throttle.until() {
            tokio::time::sleep_until(until).await;
            if self.holds.throttle.lift() {
                break;
            }
        }
        tracing::info!("provider limit waited out; trying the waiting messages again");
        self.workload.set_limited(None);
    }

    /// Whether a fallback turned the message away with `e`, which then goes to the next one up.
    pub(super) async fn passed_over(&self, e: &anyhow::Error, arrived: &Incoming) -> bool {
        let (Some(on), Some(retry)) = (e.downcast_ref::<OnFallback>(), self.turned_away(e)) else {
            return false;
        };
        let thread = &arrived.envelope.thread_id;
        tracing::warn!(%thread, error = format!("{e:#}"), "fallback turned the message away; passing it over");
        let now = tokio::time::Instant::now();
        self.holds.failover.down(on.index, now + retry);
        if e.is::<StartFailed>() {
            self.attempts.release(&arrived.envelope).await;
        }
        if let Some(outage) = self.outage()
            && self.fallback(now).is_none()
            && self.holds.failover.exhausted()
        {
            self.tell_fallbacks_passed_over(e, on, arrived, outage, now).await;
        }
        true
    }

    /// Tell the owner of the thread `arrived` came on, else the default human, that every fallback is passed over
    /// while the ant's own harness is `outage`, the last, `on`, having turned the message away with `e`.
    async fn tell_fallbacks_passed_over(
        &self,
        e: &anyhow::Error,
        on: &OnFallback,
        arrived: &Incoming,
        outage: &str,
        now: tokio::time::Instant,
    ) {
        let thread = &arrived.envelope.thread_id;
        tracing::warn!(%thread, "every fallback is turned away; messages wait");
        let why = if e.is::<SignedOut>() {
            "signed out"
        } else if e.is::<StartFailed>() {
            "failing to start"
        } else {
            "limited"
        };
        let detail: Vec<String> = e.chain().map(ToString::to_string).filter(|c| *c != on.to_string()).collect();
        let detail = self.redactor.redact(&detail.join(": ")).into_owned();
        let next = self.holds.failover.next_up(now).map(|at| {
            chrono::Utc::now() + chrono::Duration::from_std(at.saturating_duration_since(now)).unwrap_or_default()
        });
        let own = self.pool.launch().harness_name();
        let fallbacks = self.pool.fallbacks();
        let to = self.outage_notice(arrived, &fallbacks);
        let notice = crate::limits::fallback::exhausted_notice(&to, (own, outage), (&on.name, why, &detail), next);
        let id = format!("fallbacks-{}-{}", self.ant, chrono::Utc::now().timestamp_millis());
        if let Err(e) = bus::publish_json(self.js, notice.subject(), &notice, Some(&id)).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "telling a human every fallback is turned away");
            self.holds.failover.untold();
        }
    }

    /// Record which backend answered: the colony names a fallback, and the reply's notice says why it answered.
    pub(super) fn served_by(&self, fallback: Option<usize>) -> Option<String> {
        let Some(name) = fallback.and_then(|i| self.pool.fallbacks().into_iter().nth(i)) else {
            self.back_on_own();
            return None;
        };
        let since = self.holds.failover.served();
        self.workload.set_fallback(Some(ants_proto::ServingFallback { name: name.clone(), since }));
        let why = self.outage().unwrap_or("unavailable");
        let own = self.pool.launch().harness_name();
        Some(format!("[ants] Answered by fallback {name}, harness {own} being {why}."))
    }
}
