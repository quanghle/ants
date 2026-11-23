//! Dead letters: the [`DeadLetter`] of a message an ant's daemon gave up on, published on [`crate::subject::dead`]
//! (`ants.dead.<ant>`), where the `ANTS` stream keeps it until a human replays or drops it (`ants dead`).

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
    /// Most bytes of [`DeadLetter::error`] a letter cut to fit keeps ([`DeadLetter::fit`]).
    pub const ERROR_MAX: usize = 4096;

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

    /// The dead letter, cut to serialize in at most `max` bytes: its error to [`Self::ERROR_MAX`] bytes, then the text of
    /// its envelope, ending with an `[ants]` line telling of the cut, so the envelope's other fields, and its replay to
    /// its thread, stay. One still too large, by the envelope's other fields, keeps an excerpt of the envelope as its
    /// payload instead ([`Self::of_payload`]).
    #[must_use]
    pub fn fit(mut self, max: usize) -> Self {
        self.error = cut(&self.error, Self::ERROR_MAX).to_owned();
        let size = |dead: &Self| serde_json::to_vec(dead).map_or(usize::MAX, |json| json.len());
        let mut over = size(&self).saturating_sub(max);
        if over == 0 {
            return self;
        }
        if let Some(text) = self.envelope.as_mut().map(|envelope| std::mem::take(&mut envelope.text)) {
            let mut keep = text.len();
            // Each byte cut shrinks the JSON by one or more, its escape included: a few rounds at most.
            for _ in 0..4 {
                let kept = cut(&text, keep.saturating_sub(over));
                let note =
                    format!("\n\n[ants] Cut to fit the dead letter: {} of {} bytes kept.", kept.len(), text.len());
                if let Some(envelope) = &mut self.envelope {
                    envelope.text = format!("{kept}{note}");
                }
                over = size(&self).saturating_sub(max);
                if over == 0 {
                    return self;
                }
                keep = kept.len();
            }
        }
        let payload = serde_json::to_vec(&self.envelope).unwrap_or_default();
        let excerpt = Self::of_payload(self.ant, self.stream_seq, self.deliveries, self.error, &payload);
        Self { died_at: self.died_at, ..excerpt }
    }

    /// `Nats-Msg-Id` of the dead letter, so a daemon that crashes after storing it does not store it twice.
    pub fn msg_id(&self) -> String {
        format!("dead-{}", self.stream_seq)
    }

    pub fn subject(&self) -> String {
        subject::dead(&self.ant)
    }
}

/// `text` cut to at most `max` bytes, on a character boundary.
fn cut(text: &str, max: usize) -> &str {
    &text[..text.floor_char_boundary(max)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p;

    #[test]
    fn a_dead_letter_of_a_max_size_envelope_fits_the_limit_its_envelope_cut_and_replayable() {
        const MAX: usize = 1 << 20;
        // Quotes and control characters take more bytes as JSON than as text.
        let text = "\"é\u{1}x".repeat(MAX / 4);
        let msg = Envelope::new(UserId::human(), p("dev-a"), text);
        let error = "e".repeat(2 * MAX);
        let dead = DeadLetter::of_envelope(p("dev-a"), 7, 5, error, msg.clone()).fit(MAX - 1024);
        let json = serde_json::to_vec(&dead).unwrap();
        assert!(json.len() <= MAX - 1024, "{} bytes", json.len());
        assert_eq!(dead.error.len(), DeadLetter::ERROR_MAX);
        let kept = dead.envelope.expect("still an envelope");
        assert_eq!((kept.id, &kept.thread_id), (msg.id, &msg.thread_id));
        assert!(
            kept.text.ends_with(&format!(" of {} bytes kept.", msg.text.len())),
            "{}",
            &kept.text[kept.text.len() - 80..]
        );
        assert!(msg.text.starts_with(kept.text.split("\n\n[ants] Cut").next().unwrap()));

        let small = DeadLetter::of_envelope(
            p("dev-a"),
            8,
            5,
            "harness crashed",
            Envelope::new(UserId::human(), p("dev-a"), "hi"),
        );
        assert_eq!(small.clone().fit(MAX), small, "a letter that fits is kept whole");
        let excerpt = small.fit(100);
        assert_eq!(
            (excerpt.envelope, excerpt.payload.is_some()),
            (None, true),
            "an envelope too large without its text"
        );
    }

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
