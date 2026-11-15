//! Acts an assistant publishes for its human: an [`Envelope`], an [`ApprovalDecision`], a [`ReactionEntry`], a
//! [`ThreadTitle`] or a [`ThreadDone`] carrying `on_behalf_of`, published under the assistant's own name.
//!
//! - A reader honours the claim only while the human's grant ([`crate::AssistantGrant`]) names the publisher and the
//!   publisher's own user JWT, which the team's account signed, names the human in its assistant_of tag
//!   ([`crate::VerifiedGrant::honours`]). Readers never see a publisher's JWT on NATS, so the assistant's daemon puts
//!   it in [`DEVICES_BUCKET`] under [`assistant_record_key`] ([`AssistantRecord`]); a reader verifies it for its own
//!   account and takes no tag from a JWT whose key has a revocation record ([`crate::revoked_device_key`]).
//! - A claim not honoured is forged ([`Acting::Forged`]): the act counts as the publisher's own, and readers show it
//!   so. Acts never travel under the human's name: an honoured one shows as `<human> via <assistant>`.

use serde::{Deserialize, Serialize};

#[cfg(doc)]
use crate::{ApprovalDecision, DEVICES_BUCKET, Envelope, ReactionEntry, ThreadDone, ThreadTitle};
use crate::{JwtError, UserId, VerifiedUser, verify_user_jwt};

/// Last token of the key of an assistant's record in [`DEVICES_BUCKET`] ([`assistant_record_key`]).
pub const ASSISTANT_RECORD: &str = "assistant";

/// Key in [`DEVICES_BUCKET`] of the record of `assistant`'s credentials: `<assistant>.assistant`, which only the
/// assistant's credentials write. No device id is `assistant`, so readers of the devices' entries skip it.
pub fn assistant_record_key(assistant: &UserId) -> String {
    format!("{assistant}.{ASSISTANT_RECORD}")
}

/// The user JWT of an assistant's credentials, which carries its assistant_of tag, put by its daemon on start under
/// [`assistant_record_key`] so readers of its acts can check the tag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantRecord {
    pub jwt: String,
}

impl AssistantRecord {
    /// Verify the record of `assistant` for the team of `accounts` at `now` (seconds since the epoch): the JWT is a
    /// member's ([`verify_user_jwt`]) named `assistant`. Whether its key is revoked the caller checks.
    pub fn verify<'a>(
        &self,
        assistant: &UserId,
        accounts: impl IntoIterator<Item = &'a str>,
        now: u64,
    ) -> Result<VerifiedUser, RecordError> {
        let user = verify_user_jwt(&self.jwt, accounts, now)?;
        if &user.id != assistant {
            return Err(RecordError::Foreign { found: user.id, key: assistant.clone() });
        }
        Ok(user)
    }
}

/// Why an [`AssistantRecord`] is not taken.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RecordError {
    #[error(transparent)]
    Jwt(#[from] JwtError),
    #[error("the record of {key} holds the credentials of {found}")]
    Foreign { found: UserId, key: UserId },
}

/// For whom a publisher acted, as a reader verified the `on_behalf_of` its act claims: in JSON `"own"`,
/// `{"for":"alice"}` or `{"forged":"alice"}`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Acting {
    /// For itself: no claim.
    #[default]
    Own,
    /// For this human, whose assistant it is now.
    For(UserId),
    /// A claim to act for this human the reader does not honour: the act is the publisher's own.
    Forged(UserId),
}

impl Acting {
    /// What `claim`, an act's `on_behalf_of`, comes to, `honoured` telling whether the publisher acts for a human.
    pub fn of(claim: Option<&UserId>, honoured: impl FnOnce(&UserId) -> bool) -> Self {
        match claim {
            None => Self::Own,
            Some(human) if honoured(human) => Self::For(human.clone()),
            Some(human) => Self::Forged(human.clone()),
        }
    }

    pub fn is_own(&self) -> bool {
        *self == Self::Own
    }

    /// The human the act counts for, if honoured.
    pub fn human(&self) -> Option<&UserId> {
        match self {
            Self::For(human) => Some(human),
            Self::Own | Self::Forged(_) => None,
        }
    }

    /// The claim the act keeps: that of an honoured one, none of a forged one, which counts as the publisher's own.
    pub fn kept(&self) -> Option<UserId> {
        self.human().cloned()
    }

    /// How a reader names the publisher `by` of the act: `alice via asst` for an honoured one, `asst (forged: for
    /// alice)` for a forged one.
    pub fn label(&self, by: &str) -> String {
        match self {
            Self::Own => by.to_owned(),
            Self::For(human) => format!("{human} via {by}"),
            Self::Forged(human) => format!("{by} (forged: for {human})"),
        }
    }
}

#[cfg(test)]
mod tests {
    use nkeys::KeyPair;

    use super::*;
    use crate::{AssistantOf, roles::tests::user_jwt};

    const NOW: u64 = 1_800_000_000;

    #[test]
    fn a_record_is_taken_only_for_the_assistant_it_names() {
        let account = KeyPair::new_account();
        let asst: UserId = "asst".parse().unwrap();
        let alice: UserId = "alice".parse().unwrap();
        let tag = AssistantOf::tag(&alice);
        let record = AssistantRecord { jwt: user_jwt(&account, &KeyPair::new_user(), "asst", &[&tag], None) };
        let accounts = [account.public_key()];
        let accounts = || accounts.iter().map(String::as_str);
        let user = record.verify(&asst, accounts(), NOW).unwrap();
        assert_eq!(user.assistant_of(), Some(alice));

        let other: UserId = "other".parse().unwrap();
        assert!(matches!(record.verify(&other, accounts(), NOW), Err(RecordError::Foreign { .. })));
        let foreign = KeyPair::new_account().public_key();
        assert!(matches!(record.verify(&asst, [foreign.as_str()], NOW), Err(RecordError::Jwt(_))));
        assert_eq!(assistant_record_key(&asst), "asst.assistant");
        assert_eq!(crate::device_entry_of(&assistant_record_key(&asst)), None, "no device's entry");
    }

    #[test]
    fn an_unhonoured_claim_is_forged_and_counts_as_the_publishers_own() {
        let alice: UserId = "alice".parse().unwrap();
        assert_eq!(Acting::of(None, |_| unreachable!()), Acting::Own);
        let honoured = Acting::of(Some(&alice), |h| h == &alice);
        assert_eq!((honoured.label("asst"), honoured.kept()), ("alice via asst".to_owned(), Some(alice.clone())));
        let forged = Acting::of(Some(&alice), |_| false);
        assert_eq!((forged.label("asst"), forged.kept()), ("asst (forged: for alice)".to_owned(), None));
        assert_eq!(Acting::Own.label("asst"), "asst");
    }
}
