use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{Envelope, UserId, subject};

/// A message an ant gave up on: it failed on every allowed delivery, or was not an envelope. Kept on
/// [`subject::dead`] until the human replays or drops it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetter {
    /// The ant whose inbox held the message.
    pub ant: UserId,
    /// Stream sequence of the original message.
    pub stream_seq: u64,
    /// Deliveries the message got.
    pub deliveries: u64,
    /// Why it was given up on.
    pub error: String,
    pub died_at: DateTime<Utc>,
    /// The original message, ready to replay; `None` when the payload was not an envelope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envelope: Option<Envelope>,
    /// The start of a payload that was not an envelope, for inspection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
}

impl DeadLetter {
    /// Most bytes of an unparsable payload kept in [`DeadLetter::payload`].
    pub const PAYLOAD_EXCERPT: usize = 4096;

    /// The dead letter of an envelope that failed `deliveries` times.
    pub fn of_envelope(
        ant: UserId,
        stream_seq: u64,
        deliveries: u64,
        error: impl Into<String>,
        envelope: Envelope,
    ) -> Self {
        Self {
            ant,
            stream_seq,
            deliveries,
            error: error.into(),
            died_at: Utc::now(),
            envelope: Some(envelope),
            payload: None,
        }
    }

    /// The dead letter of a payload that is not an envelope; keeps at most [`Self::PAYLOAD_EXCERPT`] bytes of it.
    pub fn of_payload(ant: UserId, stream_seq: u64, deliveries: u64, error: impl Into<String>, payload: &[u8]) -> Self {
        // A cut through a multi-byte character leaves a replacement character, which is fine for inspection.
        let payload = String::from_utf8_lossy(&payload[..payload.len().min(Self::PAYLOAD_EXCERPT)]).into_owned();
        Self {
            ant,
            stream_seq,
            deliveries,
            error: error.into(),
            died_at: Utc::now(),
            envelope: None,
            payload: Some(payload),
        }
    }

    /// `Nats-Msg-Id` of the dead letter, so a daemon that crashes after storing it does not store it twice.
    pub fn msg_id(&self) -> String {
        format!("dead-{}", self.stream_seq)
    }

    pub fn subject(&self) -> String {
        subject::dead(&self.ant)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p;

    #[test]
    fn dead_letters_keep_the_envelope_or_an_excerpt_of_the_payload() {
        let msg = Envelope::new(UserId::human(), p("dev-a"), "build it");
        let dead = DeadLetter::of_envelope(p("dev-a"), 7, 5, "harness crashed", msg.clone());
        assert_eq!((dead.subject().as_str(), dead.msg_id().as_str()), ("ants.dead.dev-a", "dead-7"));
        let json = serde_json::to_string(&dead).unwrap();
        assert!(!json.contains("payload"));
        assert_eq!(serde_json::from_str::<DeadLetter>(&json).unwrap().envelope, Some(msg));

        let junk = vec![b'x'; DeadLetter::PAYLOAD_EXCERPT + 10];
        let dead = DeadLetter::of_payload(p("dev-a"), 8, 1, "not an envelope", &junk);
        assert_eq!(dead.envelope, None);
        assert_eq!(dead.payload.unwrap().len(), DeadLetter::PAYLOAD_EXCERPT);
    }
}
