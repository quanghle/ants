//! What a reader makes of a message sealed between two humans ([`State`]), and how one it does not open shows
//! ([`unopened`]). Opening one needs the reader's device key and the directory of devices, which stay with the client
//! that holds them.

use ants_proto::{Envelope, Sealed};
use serde::{Deserialize, Serialize};

/// What a reader makes of a sealed message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    Opened,
    /// Not sealed for the reader's device, or not openable by this reader.
    Unreadable,
    /// Its key or body does not open with the verified key of the sending device it names, which has none, or its
    /// body disagrees with its envelope.
    Forged,
}

impl State {
    /// What a reader shows of a message it does not open.
    pub fn placeholder(self) -> &'static str {
        match self {
            Self::Opened => "",
            Self::Unreadable => "[sealed]",
            Self::Forged => "[forged]",
        }
    }
}

/// How the ciphertext of a message shows where nothing is opened, as on a dead letter.
pub fn size_note(sealed: &Sealed) -> String {
    format!("[sealed, {} bytes]", sealed.size())
}

/// `envelope`, sealed, as a reader that made `state` of it without opening it shows it: its text the placeholder of
/// the state ([`State::placeholder`]), with no title, quote or attachment, and no longer sealed.
#[must_use]
pub fn unopened(mut envelope: Envelope, state: State) -> Envelope {
    envelope.sealed = None;
    envelope.title = None;
    envelope.quote = None;
    envelope.attachments = Vec::new();
    envelope.text = state.placeholder().to_owned();
    envelope
}
