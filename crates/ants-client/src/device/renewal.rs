//! The renewal of a device's credentials for a new key, as `ants renew` asks for it: a [`RenewRequest`] carrying the
//! current user JWT, signed by the current key, repeated every [`ants_proto::ENROLL_REPEAT`] on [`subject::renew`]
//! until [`subject::renew_reply`] answers it with the JWT of the new key, or `ants signer` refuses it
//! ([`Renewal`]). The current credentials connect until then.

use ants_proto::{RenewReply, RenewRequest, subject};
use serde::{Deserialize, Serialize};

use super::{Credentials, Error};
use crate::relayed;
use crate::transport::Message;

/// The credentials a renewal got: the user JWT of the new key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Renewed {
    pub jwt: String,
    pub credentials: Credentials,
}

/// A renewal asked for: the request, signed once [`Renewal::sign`] has the current key's signature, and what checks
/// the answer.
pub struct Renewal {
    request: RenewRequest,
    current: Credentials,
}

impl Renewal {
    /// A renewal of `current`, the credentials of the user JWT `jwt`, for the new NKey `public_key`, asked for from
    /// `host`, what the device says it is.
    pub fn new(current: &Credentials, jwt: &str, public_key: String, host: String) -> Self {
        let mut request = RenewRequest::new(current.human.as_str(), public_key, host);
        request.current = jwt.to_owned();
        Self { request, current: current.clone() }
    }

    /// What the current key signs.
    pub fn signed(&self) -> Vec<u8> {
        self.request.signed()
    }

    /// Complete the request with `signature`, of [`Renewal::signed`] by the current key, base64url without padding.
    pub fn sign(&mut self, signature: String) {
        self.request.signature = signature;
    }

    /// The request, to publish every [`ants_proto::ENROLL_REPEAT`] until answered.
    pub fn request(&self) -> Message {
        Message::new(self.request.subject(), serde_json::to_vec(&self.request).unwrap_or_default())
    }

    /// Where the answer comes.
    pub fn replies(&self) -> String {
        subject::renew_reply(&self.request.name)
    }

    /// What `message`, heard on [`Renewal::replies`], tells: none for an answer to another key, one a stream relayed or
    /// a JWT that is not the new key's, for the same human, by the same account.
    pub fn answer(&self, message: &Message) -> Option<Result<Renewed, Error>> {
        if relayed::heard_relayed(message.reply.as_deref(), message.header_names()) {
            return None;
        }
        let reply: RenewReply = serde_json::from_slice(&message.payload).ok()?;
        if reply.name != self.request.name || reply.public_key != self.request.public_key {
            return None;
        }
        if let Some(reason) = reply.refused {
            return Some(Err(Error::Refused(reason)));
        }
        let credentials = Credentials::read(&reply.jwt, &self.request.public_key, Some(&self.current.account)).ok()?;
        (credentials.human == self.current.human).then_some(Ok(Renewed { jwt: reply.jwt, credentials }))
    }
}
