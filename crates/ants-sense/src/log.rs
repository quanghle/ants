//! An ant's sense data as an append-only event log, one per sense identity ([`SenseId`]), kept on the bus by the
//! daemon serving the ant ([`ants_proto::SENSE_STREAM`]). Each [`Event`] carries the SHA-256 of the message before it
//! and its position in the log, and is signed with the ant's own credentials; the log's store ([`EventLog`]) appends
//! one only where the writer expects the identity's last message, so a stale writer is refused and catches up first,
//! and the log has no gaps. [`AntLog`] writes and reads an ant's log, verifying each event as it reads it ([`Chain`]),
//! and records what it verified in the ant's home ([`ChainStatus`]), for `ants doctor` to report a break or a log over
//! [`SIZE_LIMIT`]. A renamed ant goes on with its log under its new name: the log holds its rename ([`RENAMED`]), from
//! a name its home lists among its earlier ones. A log imported from another bus ([`crate::export`]) holds an
//! [`Anchor`] ([`IMPORTED`]) after the events it brought, signed on this bus, against which they verify.
//!
//! Nothing here holds keys or talks to NATS: a [`Signer`] signs, a [`Verifier`] checks signatures, an [`EventLog`]
//! stores, the daemon providing each; [`MemoryLog`] stores in memory, for tests.

mod ant;
mod chain;
mod event;
mod sign;
mod status;
mod store;

pub use ant::{APPEND_TRIES, AntLog, Checked};
pub use chain::{Anchor, Break, Chain, Mark, Position, Verdict};
pub use event::{EVENT_SIGNING_CONTEXT, Event, IMPORTED, RENAMED, Stored, sha256};
pub use sign::{AccountVerifier, NoRevocations, Revocations, Signature, Signer, Unsigned, Verifier, VerifyError};
pub use status::{CHAIN_FILE, ChainStatus, SENSE_DIR, SIZE_LIMIT, mib, read_status, status_file, write_status};
pub use store::{AppendError, EventLog, LogError, MemoryLog};

#[cfg(doc)]
use ants_proto::SenseId;

#[cfg(test)]
pub(crate) mod tests;
