//! An ant's sugar data as an append-only event log, one per sugar identity ([`SugarId`]), kept on the bus by the
//! daemon serving the ant ([`ants_proto::SUGAR_STREAM`]). Each [`Event`] carries the SHA-256 of the message before it
//! and its position in the log, and is signed with the ant's own credentials; the log's store ([`EventLog`]) appends
//! one only where the writer expects the identity's last message, so a stale writer is refused and catches up first,
//! and the log has no gaps. [`AntLog`] writes and reads an ant's log, verifying each event as it reads it ([`Chain`]),
//! and records what it verified in the ant's home ([`ChainStatus`]), for `ants doctor` to report a break.
//!
//! Nothing here holds keys or talks to NATS: a [`Signer`] signs, a [`Verifier`] checks signatures, an [`EventLog`]
//! stores, the daemon providing each; [`MemoryLog`] stores in memory, for tests.

mod ant;
mod chain;
mod event;
mod sign;
mod status;
mod store;

pub use ant::{AntLog, Checked};
pub use chain::{Break, Chain, Mark};
pub use event::{EVENT_SIGNING_CONTEXT, Event, Stored, sha256};
pub use sign::{AccountVerifier, NoRevocations, Revocations, Signature, Signer, Unsigned, Verifier, VerifyError};
pub use status::{CHAIN_FILE, ChainStatus, SUGAR_DIR, read_status, status_file, write_status};
pub use store::{AppendError, EventLog, LogError, MemoryLog};

#[cfg(doc)]
use ants_proto::SugarId;

#[cfg(test)]
mod tests;
