//! Identifiers that travel as subject tokens: [`UserId`], [`ThreadId`], [`ChannelId`], [`Reaction`] and [`JsDomain`],
//! each 1-64 of `a-z`, `0-9` and `-`; a machine's name ([`Machine`]), with the subjects and consumers of its leaf node;
//! and the [`Role`] of a user, human or ant.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(doc)]
use crate::{AttachmentHandle, HUMANS_BUCKET, Humans, INTAKE_STREAM, STREAM, link_outbox, owner_outbox};
use crate::{Error, FILES_BUCKET, LINKS_BUCKET, OUTBOX_CONSUMER, OUTBOX_STREAM, OWNERS_BUCKET, inbox_prefix, subject};

/// Whether `s` is safe as one NATS subject token and as a JetStream consumer name.
fn valid_token(s: &str) -> bool {
    (1..=64).contains(&s.len())
        && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !s.starts_with('-')
}

/// Whether `s` is a machine's name: 1-64 of ASCII letters, digits, `-` and `_`, starting with a letter or digit.
fn valid_machine(s: &str) -> bool {
    (1..=64).contains(&s.len())
        && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        && s.bytes().next().is_some_and(|b| b.is_ascii_alphanumeric())
}

/// A string id type: `$valid` tells the strings it takes, `$invalid` makes the error for any other.
macro_rules! token_id {
    ($name:ident, $kind:literal) => {
        token_id!($name, valid_token, |value| Error::InvalidId { kind: $kind, value });
    };
    ($(#[$meta:meta])* $name:ident, $valid:expr, $invalid:expr) => {
        $(#[$meta])*
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
                if $valid(s) { Ok(Self(s.to_owned())) } else { Err($invalid(s.to_owned())) }
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

    /// The reviewing ant every ant names as its sage unless its config names another or none.
    pub fn sage() -> Self {
        Self("sage".to_owned())
    }

    /// The channel manager (`ants channel serve`), which posts in a channel what it changed there. A reserved name:
    /// no ant may take it.
    pub fn channel_manager() -> Self {
        Self("channel-manager".to_owned())
    }

    /// `ants signer`, whose approval requests ask the humans about registrations outside the team's policy. A
    /// reserved name: no ant or human may take it.
    pub fn signer() -> Self {
        Self("signer".to_owned())
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

token_id!(
    /// A machine's name: the `server_name` and JetStream domain of its leaf node, which keeps the machine's outbox
    /// ([`OUTBOX_STREAM`]), and the name of its uplink credentials. One subject token, so it can name a domain.
    Machine,
    valid_machine,
    Error::InvalidMachine
);

impl Machine {
    /// The JetStream API prefix of the machine's leaf node, as the hub reaches it.
    pub fn api_prefix(&self) -> String {
        format!("$JS.{self}.API")
    }

    /// Prefix of the subjects the leaf node delivers its outbox to the hub on. Each machine has its own, so its uplink
    /// credentials can feed only its own source.
    pub fn deliver_prefix(&self) -> String {
        format!("$JS.S.{self}")
    }

    /// Subject the machine's [`OUTBOX_CONSUMER`] delivers the outbox to the hub on, under [`Self::deliver_prefix`].
    pub fn outbox_delivery(&self) -> String {
        format!("{}.{OUTBOX_CONSUMER}", self.deliver_prefix())
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
    /// subjects the user may publish itself ([`subject::sendable`]), and for an ant its own dead letters and timeline.
    /// With `inbox` false, no inbox subjects: a human's admission takes every inbox subject of a machine it is on, and
    /// the filters of one stream's sources may not overlap. A human's dead letters, of any ant, are written from
    /// what only the hub holds, so none are buffered.
    pub fn admitted(&self, user: &UserId, role: Role, inbox: bool) -> Vec<(String, String)> {
        let [own_inbox, own_notes, own_chan] = subject::sendable(user, role);
        let mut patterns = if inbox { vec![own_inbox, own_notes, own_chan] } else { vec![own_notes, own_chan] };
        if !role.is_human() {
            patterns.extend([subject::dead(user), subject::timeline_from(user)]);
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

    /// What the hub's attachments' store ([`FILES_BUCKET`]) admits from the machine's outbox for `user` in `role`, as
    /// an [`INTAKE_STREAM`] subject filter and the subject it is stored under: the chunks of attachments `user` may
    /// write itself ([`AttachmentHandle::outbox_chunk_subject`]), under their handles; for a human, those of any
    /// sender, as it sends under any to replay dead letters as they were.
    pub fn admitted_files(&self, user: &UserId, role: Role) -> (String, String) {
        let sender = if role.is_human() { String::new() } else { format!("{user}.") };
        (format!("ants.in.{self}.ants.out.files.{sender}>"), format!("$O.{FILES_BUCKET}.C.{sender}>"))
    }
}

impl Machine {
    /// The machine name a host name gives: its first label (`laptop` of `laptop.local`), each character a machine name
    /// cannot hold made `-`, cut to 64 characters; `machine` when nothing of it is left.
    pub fn from_hostname(hostname: &str) -> Self {
        let label = hostname.trim().split('.').next().unwrap_or_default();
        let name: String = label
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '-' })
            .skip_while(|c| !c.is_ascii_alphanumeric())
            .take(64)
            .collect();
        name.parse().unwrap_or_else(|_| Self("machine".to_owned()))
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

    /// The thread that gathers the reports of what `human`'s assistant did for it on `day` (UTC), in its inbox:
    /// `acts-YYYYMMDD-<human>`, the name cut and a hash of it added where it would not fit.
    pub fn activity(human: &UserId, day: chrono::NaiveDate) -> Self {
        let prefix = format!("{ACTIVITY_PREFIX}{}-", day.format("%Y%m%d"));
        let id = format!("{prefix}{human}");
        if id.len() <= 64 {
            return Self(id);
        }
        // FNV-1a: stable across builds, unlike the std hasher.
        let hash = human
            .as_str()
            .bytes()
            .fold(0xcbf2_9ce4_8422_2325_u64, |h, b| (h ^ u64::from(b)).wrapping_mul(0x100_0000_01b3));
        let keep = 64 - prefix.len() - 17;
        Self(format!("{prefix}{}-{hash:016x}", &human.as_str()[..keep]))
    }

    /// Whether the thread gathers an assistant's reports to its human ([`ThreadId::activity`]).
    pub fn is_activity(&self) -> bool {
        self.0.strip_prefix(ACTIVITY_PREFIX).is_some_and(|rest| {
            rest.len() > 9 && rest.as_bytes()[8] == b'-' && rest[..8].bytes().all(|b| b.is_ascii_digit())
        })
    }
}

const ACTIVITY_PREFIX: &str = "acts-";

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
    fn activity_threads_are_one_per_human_and_day_and_valid_tokens() {
        let day = chrono::NaiveDate::from_ymd_opt(2026, 11, 17).unwrap();
        let alice = ThreadId::activity(&p("alice"), day);
        assert_eq!(alice.as_str(), "acts-20261117-alice");
        assert!(alice.is_activity());
        assert_ne!(alice, ThreadId::activity(&p("alice"), day.succ_opt().unwrap()));
        let (long, longer) = (p(&"h".repeat(64)), p(&format!("{}x", "h".repeat(63))));
        let (a, b) = (ThreadId::activity(&long, day), ThreadId::activity(&longer, day));
        assert_ne!(a, b);
        assert!(a.is_activity() && b.is_activity());
        assert_eq!(a.as_str().parse::<ThreadId>().unwrap(), a);
        assert!(!ThreadId::generate().is_activity());
        assert!(!"acts-x".parse::<ThreadId>().unwrap().is_activity());
    }

    #[test]
    fn machine_names_are_one_token() {
        for bad in ["", "a.b", "m*", ">", "dev a", "-m", "_m", &"x".repeat(65)] {
            assert!(bad.parse::<Machine>().is_err(), "{bad:?} should be rejected");
        }
        assert_eq!("Mac_mini-2".parse::<Machine>().unwrap().as_str(), "Mac_mini-2");
    }

    #[test]
    fn a_host_name_gives_a_machine_name() {
        for (hostname, machine) in [
            ("Quangs-MacBook.local\n", "Quangs-MacBook"),
            ("build box", "build-box"),
            ("_x.y", "x"),
            ("", "machine"),
            ("--", "machine"),
        ] {
            assert_eq!(Machine::from_hostname(hostname).as_str(), machine, "{hostname:?}");
        }
        assert_eq!(Machine::from_hostname(&"h".repeat(80)).as_str().len(), 64);
    }
}
