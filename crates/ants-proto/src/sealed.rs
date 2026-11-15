//! Messages between two humans sealed so only their devices read them ([`Sealed`]): the text, and the suggested title
//! and attachments if any, travel encrypted in [`SealedBody`], under a content key sealed for each device.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Attachment, DeviceId, ThreadId, UserId};
#[cfg(doc)]
use crate::{DEVICES_BUCKET, DeviceKey, Envelope};

/// The version of the sealing [`Sealed`] describes.
pub const SEALED_VERSION: u32 = 1;

/// The sealed part of a message between two humans ([`Envelope::sealed`]), whose own text is then empty.
///
/// `body` is the XSalsa20-Poly1305 encryption (NaCl's `crypto_secretbox`), under a random 32-byte content key and a
/// random 24-byte nonce, of a JSON [`SealedBody`]: the nonce then the ciphertext, in standard
/// padded base64. `keys` holds the content key for each device that may open it, sealed with `crypto_box` (nkeys
/// `XKey::seal`) from the encryption key of the sending device `device` to the key of that device, as
/// [`DEVICES_BUCKET`] holds them verified ([`DeviceKey`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sealed {
    /// [`SEALED_VERSION`]; a reader opens no other.
    pub v: u32,
    /// The sending device, one of the sender's.
    pub device: DeviceId,
    pub keys: Vec<SealedKey>,
    pub body: String,
}

/// The content key of a [`Sealed`] for one device.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedKey {
    pub device: DeviceId,
    /// What nkeys `XKey::seal` makes of the 32-byte key, in standard padded base64.
    pub key: String,
}

impl Sealed {
    /// The entry of `device` in [`Sealed::keys`], if the message was sealed for it.
    pub fn key_for(&self, device: &DeviceId) -> Option<&SealedKey> {
        self.keys.iter().find(|k| k.device == *device)
    }

    /// The size of the sealed body in bytes, nonce and ciphertext, as its base64 tells without decoding it.
    pub fn size(&self) -> usize {
        let padding = self.body.bytes().rev().take_while(|b| *b == b'=').count().min(2);
        (self.body.len() / 4 * 3).saturating_sub(padding)
    }
}

/// What the [`Sealed::body`] of a message holds: the envelope's text, and its suggested title and attachments if any,
/// with what names the message, which a reader checks against the envelope it came in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedBody {
    pub from: UserId,
    pub to: UserId,
    pub thread_id: ThreadId,
    /// The message's id.
    pub id: Uuid,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<Attachment>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_size_is_that_of_the_bytes_the_base64_holds() {
        let sealed = |body: &str| Sealed { v: 1, device: DeviceId::generate(), keys: Vec::new(), body: body.into() };
        assert_eq!(sealed("").size(), 0);
        assert_eq!(sealed("AAAA").size(), 3);
        assert_eq!(sealed("AAA=").size(), 2);
        assert_eq!(sealed("AA==").size(), 1);
        assert_eq!(sealed("AAAAAAA=").size(), 5);
    }
}
