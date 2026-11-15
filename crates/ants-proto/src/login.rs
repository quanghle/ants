//! Self-service registration of a human's device: `ants login` asks `ants signer` on [`subject::login`] with the
//! team's public bootstrap credentials.

#[cfg(doc)]
use crate::subject;

/// The `name` of the user JWT of the team's bootstrap credentials (`ants auth bootstrap`).
pub const BOOTSTRAP_USER: &str = "bootstrap";

/// The largest message, in bytes, bootstrap credentials may publish.
pub const BOOTSTRAP_MAX_PAYLOAD: u32 = 4096;
