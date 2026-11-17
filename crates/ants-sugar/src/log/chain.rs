//! Verifying a sugar identity's log as it is read ([`Chain`]).

use ants_proto::{SugarId, UserId};
use serde::{Deserialize, Serialize};

use super::{Event, Stored, Verifier, VerifyError};

/// An event of a log, by its position and the SHA-256 of its message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mark {
    pub index: u64,
    pub sha256: String,
}

/// Where a log's chain breaks, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Break {
    /// The position in the log where the break is: of the event that does not verify, or of the first missing.
    pub index: u64,
    /// The stream sequence of the message that does not verify; none when events are missing at the end.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    pub reason: String,
}

/// The verified part of a sugar identity's log, read in order ([`Chain::check`]): each message must be an event of the
/// identity and of its ant, at the next position, holding the hash of the message before and signed by the ant's
/// credentials. The first break is kept; reading goes on from the message that broke the chain, as the log has no
/// other. A log that ends before, or differs at, the furthest event seen before ([`Chain::seen`]), as recorded in the
/// ant's home, is broken too: events were removed or rewritten.
#[derive(Debug, Clone)]
pub struct Chain {
    sugar: SugarId,
    ant: UserId,
    /// The last message read: its sequence and mark.
    tip: Option<(u64, Mark)>,
    seen: Option<Mark>,
    broken: Option<Break>,
    events: u64,
}

impl Chain {
    /// The chain of `sugar`, the identity of `ant`, of which `seen`, the furthest event verified before, is expected.
    pub fn new(sugar: SugarId, ant: UserId, seen: Option<Mark>) -> Self {
        Self { sugar, ant, tip: None, seen, broken: None, events: 0 }
    }

    pub fn sugar(&self) -> &SugarId {
        &self.sugar
    }

    pub fn ant(&self) -> &UserId {
        &self.ant
    }

    /// The sequence of the last message read; 0 before any.
    pub fn last_sequence(&self) -> u64 {
        self.tip.as_ref().map_or(0, |(sequence, _)| *sequence)
    }

    /// The last message read, which the next event chains to.
    pub fn tip(&self) -> Option<&Mark> {
        self.tip.as_ref().map(|(_, mark)| mark)
    }

    /// The furthest event verified, now or before.
    pub fn seen(&self) -> Option<&Mark> {
        self.seen.as_ref()
    }

    /// The first break found.
    pub fn broken(&self) -> Option<&Break> {
        self.broken.as_ref()
    }

    /// The events read that verified.
    pub fn events(&self) -> u64 {
        self.events
    }

    /// Check `stored`, the message after the last read, with `verifier`, and move past it: its event if it verified,
    /// none if it breaks the chain. Fails, without moving, only when the verifier cannot tell now.
    pub async fn check(&mut self, stored: &Stored, verifier: &impl Verifier) -> Result<Option<Event>, VerifyError> {
        let mark = Mark { index: self.tip().map_or(1, |tip| tip.index + 1), sha256: stored.sha256() };
        let event = match stored.event() {
            Ok(event) => event,
            Err(e) => {
                let reason = format!("not an event: {e}");
                self.broken.get_or_insert(Break { index: mark.index, sequence: Some(stored.sequence), reason });
                self.tip = Some((stored.sequence, mark));
                return Ok(None);
            }
        };
        let problem = match self.problem(&event, &mark) {
            Some(problem) => Some(problem),
            None => match verifier.verify(&event, stored.stored_at).await {
                Ok(()) => None,
                Err(VerifyError::Invalid(e)) => Some(e),
                Err(e @ VerifyError::Unavailable(_)) => return Err(e),
            },
        };
        let expected = mark.index;
        let mark = Mark { index: event.index, ..mark };
        let verified = match problem {
            Some(reason) => {
                self.broken.get_or_insert(Break { index: expected, sequence: Some(stored.sequence), reason });
                None
            }
            None => {
                self.events += 1;
                if self.seen.as_ref().is_none_or(|seen| seen.index <= mark.index) {
                    self.seen = Some(mark.clone());
                }
                Some(event)
            }
        };
        self.tip = Some((stored.sequence, mark));
        Ok(verified)
    }

    /// What is wrong with `event`, stored as `mark` says, past its signature.
    fn problem(&self, event: &Event, mark: &Mark) -> Option<String> {
        if event.sugar != self.sugar {
            return Some(format!("an event of sugar identity {}", event.sugar));
        }
        if event.index != mark.index {
            return Some(format!("event {} where event {} follows", event.index, mark.index));
        }
        let prev = self.tip().map(|tip| tip.sha256.as_str());
        if event.prev.as_deref() != prev {
            return Some(format!("chained to {}, not to the event before", event.prev.as_deref().unwrap_or("nothing")));
        }
        if event.ant != self.ant {
            return Some(format!("an event of {}, not of {}", event.ant, self.ant));
        }
        match &self.seen {
            Some(seen) if seen.index == mark.index && seen.sha256 != mark.sha256 => {
                Some(format!("differs from event {} as verified before", seen.index))
            }
            _ => None,
        }
    }

    /// Mark the end of the log: broken if it ends before the furthest event seen.
    pub fn end(&mut self) {
        let last = self.tip().map_or(0, |tip| tip.index);
        if let Some(seen) = &self.seen
            && seen.index > last
        {
            let reason = format!("the log ends at event {last}, event {} was verified before", seen.index);
            let index = last + 1;
            self.broken.get_or_insert(Break { index, sequence: None, reason });
        }
    }

    /// The chain past the message `stored` this daemon just appended, holding `event`, which it signed.
    pub(super) fn appended(&mut self, stored: &Stored, event: &Event) {
        let mark = Mark { index: event.index, sha256: stored.sha256() };
        self.events += 1;
        if self.seen.as_ref().is_none_or(|seen| seen.index <= mark.index) {
            self.seen = Some(mark.clone());
        }
        self.tip = Some((stored.sequence, mark));
    }
}
