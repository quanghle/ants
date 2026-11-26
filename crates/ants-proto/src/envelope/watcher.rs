//! The typed part of a watcher's wake ([`WatchWake`]): what a reader shows of the wake, and which earlier wake of the
//! watcher it hides as withdrawn, without reading the wake's text.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(doc)]
use super::Envelope;
#[cfg(doc)]
use crate::provider::WakeRequest;

/// The most bytes of a watcher check's output a wake carries ([`WatchWake::output`]).
pub const MAX_WATCH_OUTPUT: usize = 4096;

/// A watcher's wake, as its provider asks for it ([`WakeRequest::watcher`]) and the wake message carries it
/// ([`Envelope::watcher`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchWake {
    /// The watcher's id, the same on each of its wakes.
    pub id: String,
    /// The watcher's title.
    pub title: String,
    /// Whether the watcher reported itself done: the wake is its last.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub done: bool,
    /// The check's output, redacted and cut to its first [`MAX_WATCH_OUTPUT`] bytes; none when the provider no longer
    /// held it as the wake was sent, as after a restart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    /// Whether the output was cut.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub cut: bool,
    /// The message ([`Envelope::id`]) of the watcher's earlier wake that this one withdrew from the ant's queue before
    /// the ant took it up: set by the ant's daemon alone, and only when its withdrawal won, never for a wake the ant
    /// took up already. A reader hides that wake. The daemon ignores what a wake request carries here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replaces: Option<Uuid>,
}
