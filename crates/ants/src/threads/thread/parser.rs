//! Reading stored messages into [`Entry`]s as the CLI's human reads them ([`Parser`]): the entries are ants-client's
//! ([`ants_client::Parser`]), with the lookups the CLI makes for them, opening sealed messages with the keyring of its
//! device and checking who acts on whose behalf.

use std::sync::Arc;

use ants_client::Lookups;
use ants_proto::{Acting, Envelope, UserId, subject};
use chrono::{DateTime, Utc};

use super::Entry;
use crate::people::sealed;

/// Turns stored messages into [`Entry`]s, in stream order ([`ants_client::Parser`]). A sealed message between two
/// humans is opened with the keyring given ([`Parser::opening`]); without one, it shows as `[sealed]`.
#[derive(Default)]
pub struct Parser {
    entries: ants_client::Parser,
    looked_up: LookedUp,
}

/// What the CLI looked up for the entries it reads.
#[derive(Default)]
struct LookedUp {
    keyring: Option<Arc<sealed::Keyring>>,
    behalf: Option<Arc<crate::people::behalf::Checker>>,
}

impl Lookups for LookedUp {
    fn read(&self, envelope: Envelope) -> (Envelope, Option<sealed::State>) {
        sealed::read(envelope, self.keyring.as_deref())
    }

    /// What `from`'s act claiming `claim` comes to, as checked by [`Parser::learn`]: without a checker, any claim is
    /// forged.
    fn acting(&self, from: &UserId, claim: Option<&UserId>) -> Acting {
        match (&self.behalf, claim) {
            (_, None) => Acting::Own,
            (Some(behalf), claim) => behalf.acting(from, claim),
            (None, Some(human)) => Acting::Forged(human.clone()),
        }
    }
}

impl Parser {
    /// A parser opening sealed messages with `keyring`.
    pub fn opening(keyring: Option<Arc<sealed::Keyring>>) -> Self {
        Self { looked_up: LookedUp { keyring, behalf: None }, ..Self::default() }
    }

    /// The parser, checking the `on_behalf_of` of inbox messages with `behalf`; without it, every such claim is forged.
    pub fn checking(mut self, behalf: Option<Arc<crate::people::behalf::Checker>>) -> Self {
        self.looked_up.behalf = behalf;
        self
    }

    /// What checks the `on_behalf_of` of the messages read, if anything does.
    pub(super) fn behalf(&self) -> Option<&Arc<crate::people::behalf::Checker>> {
        self.looked_up.behalf.as_ref()
    }

    /// Look the sending device of a sealed message stored on `subject` up unless known ([`sealed::Keyring::learn`]),
    /// and check the claim of an inbox message, channel post or notes post made on behalf of a human
    /// ([`crate::people::behalf::Checker::check`]), before [`Parser::entry`] takes it, so one from a device added since the
    /// keyring was loaded opens.
    pub async fn learn(&self, subject: &str, payload: &[u8]) {
        /// The part of any of the three that names a claim.
        #[derive(serde::Deserialize)]
        struct Claim {
            from: UserId,
            on_behalf_of: Option<UserId>,
        }
        let claimed = || payload.windows(14).any(|w| w == b"\"on_behalf_of\"");
        let kept = || {
            subject::parse_inbox(subject).is_some()
                || subject::parse_chan(subject).is_some()
                || subject::parse_notes(subject).is_some()
        };
        if let Some(behalf) = &self.looked_up.behalf
            && claimed()
            && kept()
            && let Ok(claim) = serde_json::from_slice::<Claim>(payload)
        {
            Box::pin(behalf.check(&claim.from, claim.on_behalf_of.as_ref())).await;
        }
        let Some(keyring) = &self.looked_up.keyring else { return };
        if subject::parse_inbox(subject).is_none() || !payload.windows(8).any(|w| w == b"\"sealed\"") {
            return;
        }
        if let Ok(envelope) = serde_json::from_slice::<Envelope>(payload) {
            // Boxed: the directory lookup would otherwise swell every read of the stream.
            Box::pin(keyring.learn(&envelope)).await;
        }
    }

    /// [`Parser::entry`], the sending device of a sealed message looked up first ([`Parser::learn`]).
    pub async fn read(&mut self, seq: u64, stored_at: DateTime<Utc>, subject: &str, payload: &[u8]) -> Option<Entry> {
        self.learn(subject, payload).await;
        self.entry(seq, stored_at, subject, payload)
    }

    /// The entry of the message stored at `seq` on `subject`, None for one that is not ants' or does not parse.
    pub fn entry(&mut self, seq: u64, stored_at: DateTime<Utc>, subject: &str, payload: &[u8]) -> Option<Entry> {
        self.entries.entry_with(&self.looked_up, seq, stored_at, subject, payload)
    }
}
