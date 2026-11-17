//! Signing events with the ant's credentials ([`Signer`]) and checking their signatures ([`Verifier`]), without holding
//! keys here.

use std::future::Future;

use ants_proto::verify_user_jwt;
use chrono::{DateTime, Utc};

use super::Event;

/// What signs an ant's events: the key of its credentials, which the daemon holds.
pub trait Signer {
    /// The signature of `bytes` ([`Event::signed`]) with the JWT of the signing credentials; none on a server without
    /// authentication.
    fn sign(&self, bytes: &[u8]) -> Result<Option<Signature>, String>;
}

/// A signature of an event, with the credentials whose key made it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signature {
    /// The user JWT of the signing credentials.
    pub credentials: String,
    /// The signature, base64url without padding.
    pub signature: String,
}

/// A [`Signer`] signing nothing, for a server without authentication.
pub struct Unsigned;

impl Signer for Unsigned {
    fn sign(&self, _: &[u8]) -> Result<Option<Signature>, String> {
        Ok(None)
    }
}

/// What checks that an event was signed by credentials of its ant.
pub trait Verifier {
    /// Check `event`, which the store stored at `stored_at`.
    fn verify(&self, event: &Event, stored_at: DateTime<Utc>) -> impl Future<Output = Result<(), VerifyError>> + Send;
}

/// Why an event did not verify.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum VerifyError {
    /// The event is not signed by valid credentials of its ant: a break in the chain.
    #[error("{0}")]
    Invalid(String),
    /// Whether it is could not be told now, such as a revocation lookup failing: try again later.
    #[error("{0}")]
    Unavailable(String),
}

/// Where a [`AccountVerifier`] looks up revocations of credentials.
pub trait Revocations {
    /// When the credentials of user key `key` (`U...`) were revoked, in seconds since the epoch; none when they are
    /// not.
    fn revoked_at(&self, key: &str) -> impl Future<Output = Result<Option<u64>, String>> + Send;
}

/// No revocations at all.
pub struct NoRevocations;

impl Revocations for NoRevocations {
    fn revoked_at(&self, _: &str) -> impl Future<Output = Result<Option<u64>, String>> + Send {
        std::future::ready(Ok(None))
    }
}

/// A [`Verifier`] taking events signed by the key of credentials of the event's ant that one of the team account's
/// keys signed, unexpired when the event was stored and not revoked before then. Without accounts, a server without
/// authentication, it takes every event.
pub struct AccountVerifier<R> {
    accounts: Vec<String>,
    revocations: R,
}

impl<R: Revocations> AccountVerifier<R> {
    /// Checking against `accounts`, the team account's public keys (`A...`), and `revocations`.
    pub fn new(accounts: Vec<String>, revocations: R) -> Self {
        Self { accounts, revocations }
    }
}

impl<R: Revocations + Sync> Verifier for AccountVerifier<R> {
    async fn verify(&self, event: &Event, stored_at: DateTime<Utc>) -> Result<(), VerifyError> {
        if self.accounts.is_empty() {
            return Ok(());
        }
        let (Some(jwt), Some(signature)) = (&event.credentials, &event.signature) else {
            return Err(VerifyError::Invalid("signed by no credentials of the ant".into()));
        };
        let at = u64::try_from(stored_at.timestamp()).unwrap_or_default();
        let invalid = |e: ants_proto::JwtError| VerifyError::Invalid(format!("the signing credentials: {e}"));
        let user = verify_user_jwt(jwt, self.accounts.iter().map(String::as_str), at).map_err(invalid)?;
        if user.id != event.ant {
            return Err(VerifyError::Invalid(format!(
                "signed by the credentials of {}, not of {}",
                user.id, event.ant
            )));
        }
        user.check_signature(&event.signed(), signature).map_err(invalid)?;
        match self.revocations.revoked_at(&user.public_key).await.map_err(VerifyError::Unavailable)? {
            Some(revoked) if revoked <= at => {
                Err(VerifyError::Invalid(format!("signed by credentials revoked before ({})", user.public_key)))
            }
            _ => Ok(()),
        }
    }
}
