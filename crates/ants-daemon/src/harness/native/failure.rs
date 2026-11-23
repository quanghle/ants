//! The errors a native protocol answers with, worded so that the daemon reads of them what it reads of an ACP
//! harness's: not signed in, a provider limit, a context overflow, or another failure.

use std::fmt;
use std::time::Duration;

use agent_client_protocol::{Error, ErrorCode};

/// The CLI says it is not signed in: ACP's `auth_required`, which makes the daemon hold messages and name the
/// definition's `sign_in`.
pub(crate) fn signed_out(detail: impl fmt::Display) -> Error {
    Error::new(ErrorCode::AUTH_REQUIRED, format!("not signed in: {detail}"))
}

/// The provider rate limited or is overloaded, with the HTTP status it answered and how long it says to wait, if
/// known: a message [`crate::limits::ratelimit::classify`] reads as a rate limit and its retry hint.
pub(crate) fn rate_limited(status: Option<u16>, retry_after: Option<Duration>, detail: impl fmt::Display) -> Error {
    let status = status.map_or_default(|status| format!(" (status {status})"));
    failed(format!("rate limit{status}: {detail}{}", hint(retry_after)))
}

/// The provider's quota or usage allowance is exhausted, until `retry_after` if known: a message
/// [`crate::limits::ratelimit::classify`] reads as an exhausted quota (or, back within 5 minutes, as a rate limit).
pub(crate) fn quota_exhausted(retry_after: Option<Duration>, detail: impl fmt::Display) -> Error {
    failed(format!("quota exceeded: {detail}{}", hint(retry_after)))
}

/// The session outgrew the model's context window: a message [`crate::limits::overflow`] recognizes, so the daemon retries
/// the prompt once in a new session seeded with the thread's history.
pub(crate) fn context_overflow(detail: impl fmt::Display) -> Error {
    failed(format!("context window exceeded: {detail}"))
}

/// Any other failure, `detail` naming it as the CLI did; the daemon still reads the common phrases of sign-in, limits
/// and context overflow in it.
pub(crate) fn failed(detail: impl fmt::Display) -> Error {
    Error::new(ErrorCode::INTERNAL_ERROR, detail.to_string())
}

/// A request the protocol does not support.
pub(crate) fn unsupported(what: &str) -> Error {
    Error::new(ErrorCode::METHOD_NOT_FOUND, format!("{what} is not supported"))
}

fn hint(retry_after: Option<Duration>) -> String {
    retry_after.map_or_default(|wait| format!("; retry after {}s", wait.as_secs().max(1)))
}
