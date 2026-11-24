//! What the credentials that come before a user's own allow: an ant's enrollment credentials
//! ([`Permissions::for_enrollment`]), and the bus's public bootstrap credentials, for a human's new device
//! ([`Permissions::for_bootstrap`]).

use super::Permissions;
use crate::{UserId, subject};

impl Permissions {
    /// What the enrollment credentials of `ant` allow: asking for its credentials on [`subject::enroll`] and
    /// hearing the answer on [`subject::enroll_reply`], nothing else, so they cannot ask for another name or hear
    /// another's answer.
    pub fn for_enrollment(ant: &UserId) -> Self {
        Self { publish: vec![subject::enroll(ant)], subscribe: vec![subject::enroll_reply(ant)] }
    }

    /// What the bus's public bootstrap credentials allow (`ants auth bootstrap`): asking for the credentials of a
    /// human's new device on [`subject::login`] and hearing the answers, nothing else, JetStream included.
    pub fn for_bootstrap() -> Self {
        Self {
            publish: vec![subject::ALL_LOGIN_REQUESTS.to_owned()],
            subscribe: vec![subject::ALL_LOGIN_REPLIES.to_owned()],
        }
    }
}
