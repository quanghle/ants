use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, LINKS_BUCKET, OUTBOX_STREAM, OWNERS_BUCKET, inbox_prefix, subject};
#[cfg(doc)]
use crate::{HUMANS_BUCKET, Humans, INTAKE_STREAM, STREAM, link_outbox, owner_outbox};

/// Whether `s` is safe as one NATS subject token and as a JetStream consumer name.
fn valid_token(s: &str) -> bool {
    (1..=64).contains(&s.len())
        && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !s.starts_with('-')
}

macro_rules! token_id {
    ($name:ident, $kind:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl FromStr for $name {
            type Err = Error;
            fn from_str(s: &str) -> Result<Self, Error> {
                if valid_token(s) {
                    Ok(Self(s.to_owned()))
                } else {
                    Err(Error::InvalidId { kind: $kind, value: s.to_owned() })
                }
            }
        }

        impl TryFrom<String> for $name {
            type Error = Error;
            fn try_from(s: String) -> Result<Self, Error> {
                s.parse()
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> String {
                id.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

token_id!(UserId, "user id");
token_id!(ThreadId, "thread id");
// A channel: a named group of ants, with the same syntax as a user id.
token_id!(ChannelId, "channel id");
// A reaction to a message, such as `eyes` or `check`: one subject token, with the same syntax as a user id.
token_id!(Reaction, "reaction");
// The JetStream domain of the hub (`jetstream { domain: ... }`). Clients on a leaf node reach the hub's JetStream only
// through it, as `$JS.<domain>.API`: the plain `$JS.API` stays on the leaf.
token_id!(JsDomain, "JetStream domain");

impl UserId {
    /// The default human: a human whether or not [`HUMANS_BUCKET`] lists it, and the one escalations go to when a
    /// thread has no owner.
    pub fn human() -> Self {
        Self("human".to_owned())
    }

    /// The router ant that takes requests without an explicit recipient.
    pub fn messenger() -> Self {
        Self("messenger".to_owned())
    }

    /// The channel manager (`ants channel serve`), which posts in a channel what it changed there. A reserved name:
    /// no ant may take it.
    pub fn channel_manager() -> Self {
        Self("channel-manager".to_owned())
    }

    /// Whether this is the default human, [`UserId::human`]. Other humans are known only from [`HUMANS_BUCKET`]
    /// ([`Humans`]).
    pub fn is_human(&self) -> bool {
        self.0 == "human"
    }
}

/// What a set of credentials, or the sender of a message, is: a human, with every human right, or an ant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    Human,
    Ant,
}

impl Role {
    /// The role of `user` without the [`HUMANS_BUCKET`]: human for the default human, [`UserId::human`], else
    /// ant.
    pub fn by_default(user: &UserId) -> Self {
        if user.is_human() { Self::Human } else { Self::Ant }
    }

    pub fn is_human(self) -> bool {
        self == Self::Human
    }
}

/// A machine's name: the `server_name` and JetStream domain of its leaf node, which keeps the machine's outbox
/// ([`OUTBOX_STREAM`]), and the name of its uplink credentials. One subject token, so it can name a domain.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Machine(String);

impl Machine {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The JetStream API prefix of the machine's leaf node, as the hub reaches it.
    pub fn api_prefix(&self) -> String {
        format!("$JS.{self}.API")
    }

    /// Prefix of the subjects the leaf node delivers its outbox to the hub on. Each machine has its own, so its uplink
    /// credentials can feed only its own source.
    pub fn deliver_prefix(&self) -> String {
        format!("$JS.S.{self}")
    }

    /// Reply prefix of the hub's own requests for the machine's outbox, with nats-server's `js_api_reply_v2` feature
    /// flag on the hub: `$JSC.R.<domain>.<account hash>.<stream>.`, then the consumer and a unique token.
    pub fn source_replies(&self) -> String {
        format!("$JSC.R.{self}.*.{OUTBOX_STREAM}")
    }

    /// Inbox prefix of the connection of `human` that sets up the machine's outbox (`ants leaf outbox`): the leaf node
    /// answers on it, so it is the machine's own, apart from the replies of any other request of that human.
    pub fn setup_inbox(&self, human: &UserId) -> String {
        format!("{}.leaf.{self}", inbox_prefix(human))
    }

    /// What the hub's [`STREAM`] admits from the machine's outbox for `user` in `role`, as pairs of an
    /// [`INTAKE_STREAM`] subject filter and the subject it is stored under: exactly the inbox, notes and channel
    /// subjects the user may publish itself ([`subject::sendable`]), and for an ant its own dead letters.
    /// With `inbox` false, no inbox subjects: a human's admission takes every inbox subject of a machine it is on, and
    /// the filters of one stream's sources may not overlap. A human's dead letters, of any ant, are written from
    /// what only the hub holds, so none are buffered.
    pub fn admitted(&self, user: &UserId, role: Role, inbox: bool) -> Vec<(String, String)> {
        let [own_inbox, own_notes, own_chan] = subject::sendable(user, role);
        let mut patterns = if inbox { vec![own_inbox, own_notes, own_chan] } else { vec![own_notes, own_chan] };
        if !role.is_human() {
            patterns.push(subject::dead(user));
        }
        patterns
            .into_iter()
            .map(|pattern| {
                let filter = format!("ants.in.{self}.{}", subject::outbox(&pattern).expect("an outbox subject"));
                let mut n = 0;
                let destination: Vec<String> = pattern
                    .split('.')
                    .map(|token| match token {
                        "*" => {
                            n += 1;
                            format!("{{{{wildcard({n})}}}}")
                        }
                        token => token.to_owned(),
                    })
                    .collect();
                (filter, destination.join("."))
            })
            .collect()
    }

    /// What the hub's [`LINKS_BUCKET`] admits from the machine's outbox for ant `user`, as an [`INTAKE_STREAM`] subject
    /// filter and the subject it is stored under: exactly the links `user` passed on ([`link_outbox`]), under its own
    /// keys.
    pub fn admitted_links(&self, user: &UserId) -> (String, String) {
        let filter = format!("ants.in.{self}.ants.out.links.{user}.*.*");
        (filter, format!("$KV.{LINKS_BUCKET}.{user}.{{{{wildcard(1)}}}}.{{{{wildcard(2)}}}}"))
    }

    /// What the hub's [`OWNERS_BUCKET`] admits from the machine's outbox when a human runs on it, as an
    /// [`INTAKE_STREAM`] subject filter and the subject it is stored under: the owner records its humans kept there
    /// ([`owner_outbox`]), under their keys.
    pub fn admitted_owners(&self) -> (String, String) {
        let filter = format!("ants.in.{self}.ants.out.owners.*.*");
        (filter, format!("$KV.{OWNERS_BUCKET}.{{{{wildcard(1)}}}}.{{{{wildcard(2)}}}}"))
    }
}

impl FromStr for Machine {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        let valid = (1..=64).contains(&s.len())
            && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            && s.bytes().next().is_some_and(|b| b.is_ascii_alphanumeric());
        if valid { Ok(Self(s.to_owned())) } else { Err(Error::InvalidMachine(s.to_owned())) }
    }
}

impl TryFrom<String> for Machine {
    type Error = Error;
    fn try_from(s: String) -> Result<Self, Error> {
        s.parse()
    }
}

impl From<Machine> for String {
    fn from(machine: Machine) -> String {
        machine.0
    }
}

impl fmt::Display for Machine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl ThreadId {
    /// A new, time-ordered thread id.
    pub fn generate() -> Self {
        Self(Uuid::now_v7().to_string())
    }

    /// The id of the thread a top-level channel post with id `post` starts.
    pub(crate) fn of_post(post: Uuid) -> Self {
        Self(post.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p;

    #[test]
    fn ids_reject_subject_metacharacters() {
        for bad in ["", "a.b", "*", ">", "dev a", "Dev", "-dev", &"x".repeat(65)] {
            assert!(bad.parse::<UserId>().is_err(), "{bad:?} should be rejected");
        }
        assert_eq!(p("dev-b2").as_str(), "dev-b2");
        assert!(serde_json::from_str::<UserId>(r#""a.b""#).is_err());
    }

    #[test]
    fn generated_thread_ids_are_valid_tokens() {
        let t = ThreadId::generate();
        assert_eq!(t.as_str().parse::<ThreadId>().unwrap(), t);
    }

    #[test]
    fn machine_names_are_one_token() {
        for bad in ["", "a.b", "m*", ">", "dev a", "-m", "_m", &"x".repeat(65)] {
            assert!(bad.parse::<Machine>().is_err(), "{bad:?} should be rejected");
        }
        assert_eq!("Mac_mini-2".parse::<Machine>().unwrap().as_str(), "Mac_mini-2");
    }
}
