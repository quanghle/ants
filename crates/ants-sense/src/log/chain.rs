//! Verifying a sense identity's log as it is read ([`Chain`]).

use ants_proto::{SenseId, UserId};
use serde::{Deserialize, Serialize};

use super::{Event, RENAMED, Stored, Verifier, VerifyError};

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

/// How far a [`Chain`] was read: the last message read, the events that verified, the bytes read and the first break,
/// kept with what was built from them (the cache) to go on reading from there later ([`Chain::resume`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    /// The stream sequence of the last message read.
    pub sequence: u64,
    pub tip: Mark,
    pub events: u64,
    /// The bytes of the messages read ([`Chain::bytes`]).
    pub bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broken: Option<Break>,
    /// The name of the ant whose events the log holds by then: of the last event that verified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ant: Option<UserId>,
}

/// The verified part of a sense identity's log, read in order ([`Chain::check`]): each message must be an event of the
/// identity and of its ant, at the next position, holding the hash of the message before and signed by the ant's
/// credentials. The first break is kept; reading goes on from the message that broke the chain, as the log has no
/// other. A log that ends before, or differs at, the furthest event seen before ([`Chain::seen`]), as recorded in the
/// ant's home, is broken too: events were removed or rewritten.
///
/// An ant renamed keeps its log: its events are of the ant under its current name or one of its earlier names, as its
/// home tells ([`Chain::with_earlier_names`]). The log's events are of one name at a time ([`Chain::named`]): the
/// first event's, until a [`RENAMED`] event of the ant's new name, naming the one before as `from`.
#[derive(Debug, Clone)]
pub struct Chain {
    sense: SenseId,
    ant: UserId,
    earlier: Vec<UserId>,
    /// The name the log's events are of: of the last event that verified.
    named: Option<UserId>,
    /// The last message read: its sequence and mark.
    tip: Option<(u64, Mark)>,
    seen: Option<Mark>,
    broken: Option<Break>,
    events: u64,
    bytes: u64,
}

impl Chain {
    /// The chain of `sense`, the identity of `ant`, of which `seen`, the furthest event verified before, is expected.
    pub fn new(sense: SenseId, ant: UserId, seen: Option<Mark>) -> Self {
        Self { sense, ant, earlier: Vec::new(), named: None, tip: None, seen, broken: None, events: 0, bytes: 0 }
    }

    /// The chain of an ant once named `earlier` too, as well as by its current name.
    #[must_use]
    pub fn with_earlier_names(self, earlier: Vec<UserId>) -> Self {
        Self { earlier, ..self }
    }

    pub fn sense(&self) -> &SenseId {
        &self.sense
    }

    /// The ant reading and writing the log, by its current name.
    pub fn ant(&self) -> &UserId {
        &self.ant
    }

    /// The ant's earlier names, whose events are its own too.
    pub fn earlier(&self) -> &[UserId] {
        &self.earlier
    }

    /// The name of the ant the log's events are of by now: of the last event that verified; none before any.
    pub fn named(&self) -> Option<&UserId> {
        self.named.as_ref()
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

    /// The bytes of the messages read, those breaking the chain too: the size of the log's payloads up to the last
    /// message read, as the store holds them.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// How far the chain was read; none before any message.
    pub fn position(&self) -> Option<Position> {
        let (sequence, tip) = self.tip.clone()?;
        let (events, bytes, broken, ant) = (self.events, self.bytes, self.broken.clone(), self.named.clone());
        Some(Position { sequence, tip, events, bytes, broken, ant })
    }

    /// Go on reading from `position`, as read before, or with none from the log's start, as if nothing was read. The
    /// furthest event seen stays expected.
    pub fn resume(&mut self, position: Option<Position>) {
        let Some(Position { sequence, tip, events, bytes, broken, ant }) = position else {
            (self.tip, self.events, self.bytes, self.broken, self.named) = (None, 0, 0, None, None);
            return;
        };
        if self.seen.as_ref().is_none_or(|seen| seen.index <= tip.index) {
            self.seen = Some(tip.clone());
        }
        (self.tip, self.events, self.bytes, self.broken, self.named) =
            (Some((sequence, tip)), events, bytes, broken, ant);
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
                self.bytes += stored.payload.len() as u64;
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
                self.named = Some(event.ant.clone());
                Some(event)
            }
        };
        self.bytes += stored.payload.len() as u64;
        self.tip = Some((stored.sequence, mark));
        Ok(verified)
    }

    /// What is wrong with `event`, stored as `mark` says, past its signature.
    fn problem(&self, event: &Event, mark: &Mark) -> Option<String> {
        if event.sense != self.sense {
            return Some(format!("an event of sense identity {}", event.sense));
        }
        if event.index != mark.index {
            return Some(format!("event {} where event {} follows", event.index, mark.index));
        }
        let prev = self.tip().map(|tip| tip.sha256.as_str());
        if event.prev.as_deref() != prev {
            return Some(format!("chained to {}, not to the event before", event.prev.as_deref().unwrap_or("nothing")));
        }
        if let Some(problem) = self.name_problem(event) {
            return Some(problem);
        }
        match &self.seen {
            Some(seen) if seen.index == mark.index && seen.sha256 != mark.sha256 => {
                Some(format!("differs from event {} as verified before", seen.index))
            }
            _ => None,
        }
    }

    /// What is wrong with the name of `event`'s ant: none when it is the name the log's events are of by then, or, for
    /// a [`RENAMED`] event, the ant's next name, renamed from that one.
    fn name_problem(&self, event: &Event) -> Option<String> {
        let known = |name: &UserId| *name == self.ant || self.earlier.contains(name);
        if !known(&event.ant) {
            return Some(format!("an event of {}, not of {} nor of an earlier name of it", event.ant, self.ant));
        }
        if event.kind == RENAMED {
            let from = event.data.get("from").and_then(|from| from.as_str()?.parse::<UserId>().ok());
            return match (from, &self.named) {
                (None, _) => Some("a rename from no name".to_owned()),
                (Some(from), _) if from == event.ant => Some(format!("a rename of {from} to the same name")),
                (Some(from), Some(named)) if from != *named => {
                    Some(format!("a rename from {from}, where the events before are of {named}"))
                }
                (Some(from), None) if !known(&from) => Some(format!("a rename from {from}, no earlier name of it")),
                _ => None,
            };
        }
        match &self.named {
            Some(named) if event.ant != *named => Some(format!("an event of {}, not of {named}", event.ant)),
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
        self.bytes += stored.payload.len() as u64;
        if self.seen.as_ref().is_none_or(|seen| seen.index <= mark.index) {
            self.seen = Some(mark.clone());
        }
        self.named = Some(event.ant.clone());
        self.tip = Some((stored.sequence, mark));
    }
}
