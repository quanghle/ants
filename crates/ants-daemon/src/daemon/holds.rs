//! Holding the messages while the ant's own harness cannot answer, signed out, limited or failing to start: the
//! humans told, the fallbacks that answer meanwhile, and the checks that let the messages go on.

use std::time::Duration;

use ants_proto::State;

use crate::breaker::{self, StartFailed};
use crate::bus;
use crate::fallback::{OnFallback, OutageNotice};
use crate::harness::{self, SignedOut};
use crate::ratelimit::{self, Limited};
use crate::signin;
use crate::status;

use super::{Inbox, Incoming};

impl Inbox<'_> {
    /// Why the ant's own harness cannot answer now, if it cannot: messages go to a fallback meanwhile, if one is up.
    pub(super) fn outage(&self) -> Option<&'static str> {
        if self.sign_in.is_out() {
            Some("signed out")
        } else if self.breaker.is_open() {
            Some("failing to start")
        } else if self.throttle.is_quota_held() {
            Some("out of quota")
        } else {
            None
        }
    }

    /// The fallback messages go to at `now`: the first not passed over, while the ant's own harness cannot answer.
    pub(super) fn fallback(&self, now: tokio::time::Instant) -> Option<usize> {
        self.outage()?;
        self.failover.pick(self.pool.fallback_count(), now)
    }

    /// How long the fallback `e` came from is passed over, when it turned the prompt away: signed out, limited or
    /// failing to start.
    fn turned_away(&self, e: &anyhow::Error) -> Option<Duration> {
        if let Some(limited) = e.downcast_ref::<Limited>() {
            return Some(limited.retry_after.unwrap_or(self.fallback_retry));
        }
        (e.is::<SignedOut>() || e.is::<StartFailed>()).then_some(self.fallback_retry)
    }

    /// Where a notice of an outage `incoming` met goes, naming `fallbacks`.
    fn outage_notice<'a>(&'a self, incoming: &'a Incoming, fallbacks: &'a [String]) -> OutageNotice<'a> {
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
        if self.failover.clear() {
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
        let tell = self.sign_in.out();
        if let Some(since) = self.sign_in.out_since() {
            let sign_in = signed_out.sign_in.clone();
            self.workload.set_signed_out(Some(ants_proto::SignInNeeded { since, sign_in }));
        }
        let Some(since) = tell else {
            tracing::info!(%thread, error = %signed_out, "harness signed out; the message waits");
            return;
        };
        tracing::warn!(%thread, error = %signed_out, "harness signed out; messages wait until it is signed in");
        let fallbacks = self.pool.fallbacks();
        let notice = signin::notice(&self.outage_notice(incoming, &fallbacks), signed_out, self.sign_in_every);
        let id = format!("sign-in-{}-{since}", self.ant);
        if let Err(e) = bus::publish_json(self.js, notice.subject(), &notice, Some(&id)).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "telling a human to sign the harness in");
            self.sign_in.untold();
        }
    }

    /// Check the harness every `sign_in_every` until a handshake with it succeeds, then clear the colony entry's
    /// signed-out state and let the messages go on.
    pub(super) async fn signed_in(&self) {
        loop {
            tokio::time::sleep(self.sign_in_every).await;
            let checked = harness::handshake(self.pool.launch()).await;
            match checked.readiness {
                harness::Readiness::Ready { agent } => {
                    tracing::info!(%agent, "harness signed in; answering the waiting messages");
                    self.sign_in.back();
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
        let hold = self.throttle.hit(limited, &self.backoffs);
        self.workload.set_limited(Some(hold.shown.clone()));
        let until = hold.shown.until;
        if !hold.tell {
            tracing::info!(%thread, error = %limited, %until, "provider limit; messages wait");
            return;
        }
        tracing::warn!(%thread, error = %limited, %until, "provider quota exhausted; messages wait");
        let fallbacks = self.pool.fallbacks();
        let max = self.backoffs.quota.max;
        let notice = ratelimit::notice(&self.outage_notice(incoming, &fallbacks), limited, &hold.shown, max);
        let id = format!("quota-{}-{}", self.ant, hold.shown.since.timestamp_millis());
        if let Err(e) = bus::publish_json(self.js, notice.subject(), &notice, Some(&id)).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "telling a human of the exhausted quota");
            self.throttle.untold();
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
        let mut notice = breaker::notice(&self.outage_notice(incoming, &fallbacks), failed, self.sign_in_every);
        self.redactor.redact_string(&mut notice.text);
        let id = format!("harness-{}-{}", self.ant, hold.shown.since.timestamp_millis());
        if let Err(e) = bus::publish_json(self.js, notice.subject(), &notice, Some(&id)).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "telling a human the harness fails to start");
            self.breaker.untold();
        }
    }

    /// Check the harness every `sign_in_every`, starting it as for a thread ([`harness::probe`]), until it starts or
    /// says it is signed out or limited, then clear the colony entry's failing state and let the messages go on.
    pub(super) async fn repaired(&self) {
        loop {
            tokio::time::sleep(self.sign_in_every).await;
            match harness::probe(self.pool.launch()).await {
                Ok(()) => tracing::info!("harness started; answering the waiting messages"),
                Err(e) if e.is::<SignedOut>() || e.is::<Limited>() => {
                    tracing::info!(error = %e, "harness started; trying the waiting messages again");
                }
                Err(e) => {
                    let error = format!("{e:#}");
                    tracing::warn!(error, "harness still fails to start; messages wait");
                    self.workload.set_failing(self.breaker.still_failing(&self.redactor.redact(&error)));
                    continue;
                }
            }
            self.breaker.closed();
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
        while let Some(until) = self.throttle.until() {
            tokio::time::sleep_until(until).await;
            if self.throttle.lift() {
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
        self.failover.down(on.index, now + retry);
        if e.is::<StartFailed>() {
            self.attempts.release(&arrived.envelope).await;
        }
        if let Some(outage) = self.outage()
            && self.fallback(now).is_none()
            && self.failover.exhausted()
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
        let next = self.failover.next_up(now).map(|at| {
            chrono::Utc::now() + chrono::Duration::from_std(at.saturating_duration_since(now)).unwrap_or_default()
        });
        let own = self.pool.launch().harness_name();
        let fallbacks = self.pool.fallbacks();
        let to = self.outage_notice(arrived, &fallbacks);
        let notice = crate::fallback::exhausted_notice(&to, (own, outage), (&on.name, why, &detail), next);
        let id = format!("fallbacks-{}-{}", self.ant, chrono::Utc::now().timestamp_millis());
        if let Err(e) = bus::publish_json(self.js, notice.subject(), &notice, Some(&id)).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "telling a human every fallback is turned away");
            self.failover.untold();
        }
    }

    /// Record which backend answered: the colony names a fallback, and the reply's notice says why it answered.
    pub(super) fn served_by(&self, fallback: Option<usize>) -> Option<String> {
        let Some(name) = fallback.and_then(|i| self.pool.fallbacks().into_iter().nth(i)) else {
            self.back_on_own();
            return None;
        };
        let since = self.failover.served();
        self.workload.set_fallback(Some(ants_proto::ServingFallback { name: name.clone(), since }));
        let why = self.outage().unwrap_or("unavailable");
        let own = self.pool.launch().harness_name();
        Some(format!("[ants] Answered by fallback {name}, harness {own} being {why}."))
    }
}
