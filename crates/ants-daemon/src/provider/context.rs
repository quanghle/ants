//! The [`CallContext`] of a provider call, as the daemon serving the ant knows it: never taken from the ant's text.
//! Tool calls and brief requests alike take who calls from [`Callers`] and where the thread came from from [`origin`],
//! kept per thread by [`settled`].

use ants_proto::provider::{CallContext, Origin, Starter};
use ants_proto::{ChannelId, Envelope, Humans, ThreadId, UserId};
use chrono::Utc;

use crate::profile::identity::Identity;

/// Where the daemon finds who the provider calls of its ant are for: the ant, the machine serving it, and its identity
/// record, whose id is read when a call needs it ([`Identity::id`] keeps it once read).
#[derive(Clone)]
pub struct Callers {
    ant: UserId,
    machine: String,
    record: Record,
}

#[derive(Clone)]
#[cfg_attr(test, expect(clippy::large_enum_variant, reason = "the small variant serves tests only"))]
enum Record {
    Stored(Identity),
    #[cfg(test)]
    Known(String),
}

impl Callers {
    pub fn new(ant: UserId, machine: String, identity: Identity) -> Self {
        Self { ant, machine, record: Record::Stored(identity) }
    }

    /// Callers whose identity record has the id `identity`, without reading it.
    #[cfg(test)]
    pub(crate) fn known(ant: UserId, machine: &str, identity: &str) -> Self {
        Self { ant, machine: machine.to_owned(), record: Record::Known(identity.to_owned()) }
    }

    /// Who a call now is for: an error saying why when the identity record is absent or cannot be read.
    pub async fn caller(&self) -> Result<Caller, String> {
        let identity = match &self.record {
            Record::Stored(identity) => match identity.id().await {
                Ok(Some(id)) => id,
                Ok(None) => return Err("the ant has no identity record".to_owned()),
                Err(e) => {
                    tracing::warn!(error = format!("{e:#}"), "reading the ant's identity for a provider call");
                    return Err(CANNOT_READ.to_owned());
                }
            },
            #[cfg(test)]
            Record::Known(id) => id.clone(),
        };
        Ok(Caller { ant: self.ant.clone(), identity, machine: self.machine.clone() })
    }
}

/// Why a provider call names no caller when the ant's identity record cannot be read.
pub const CANNOT_READ: &str = "the ant's identity record cannot be read";

/// Who a provider call is for: the ant, the id of its identity record ([`crate::profile::identity::Identity::id`])
/// and the machine serving it, as its colony entry names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    pub ant: UserId,
    pub identity: String,
    pub machine: String,
}

impl Caller {
    /// The context of a call now on `thread`, which `origin` started, from a session of `harness` running `model`
    /// (None for the harness's default).
    pub fn context(&self, thread: ThreadId, origin: Origin, harness: &str, model: Option<&str>) -> CallContext {
        CallContext {
            ant: self.ant.clone(),
            identity: self.identity.clone(),
            thread,
            origin,
            harness: harness.to_owned(),
            model: model.map(str::to_owned),
            machine: self.machine.clone(),
            at: Utc::now(),
        }
    }
}

/// Where the thread of `envelope`, a message as the daemon took it on arrival, came from, on `channel` when a post on
/// it is the message, in this order:
/// - a webhook's or schedule's message, or one on a thread narrowed to tool rules ([`Envelope::allow`]), which only
///   those start and only ants carry on: the webhook's, the sender standing for a webhook it passes on;
/// - the ant that started the thread with a channel post (its creator);
/// - the human who started it (its owner), or the human an assistant acts for;
/// - else the message's sender, a human or an ant.
pub fn origin(envelope: &Envelope, channel: Option<ChannelId>, humans: &Humans) -> Origin {
    let by = if envelope.webhook || envelope.allow.is_some() {
        Starter::Webhook { user: envelope.from.clone() }
    } else if let Some(creator) = &envelope.creator {
        Starter::Ant { user: creator.clone() }
    } else if let Some(owner) = envelope.owner.as_ref().or(envelope.on_behalf_of.as_ref()) {
        Starter::Human { user: owner.clone() }
    } else if envelope.sender_role(humans).is_human() {
        Starter::Human { user: envelope.from.clone() }
    } else {
        Starter::Ant { user: envelope.from.clone() }
    };
    Origin { by, channel }
}

/// The origin of a thread `earlier` gave, now that a message on it gives `now`: the first stays, its channel set when
/// it had none, unless `now` is from outside the bus and `earlier` is not, so a thread a webhook ever reached stays
/// untrusted.
pub fn settled(earlier: Option<Origin>, now: Origin) -> Origin {
    match earlier {
        None => now,
        Some(earlier) if earlier.trusted() && !now.trusted() => {
            Origin { channel: earlier.channel.or(now.channel), ..now }
        }
        Some(Origin { by, channel }) => Origin { by, channel: channel.or(now.channel) },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(from: &str) -> Envelope {
        Envelope::on_thread("t1".parse().unwrap(), from.parse().unwrap(), "dev-a".parse().unwrap(), "hi")
    }

    fn by(envelope: &Envelope) -> Starter {
        origin(envelope, None, &Humans::default()).by
    }

    fn user(name: &str) -> UserId {
        name.parse().unwrap()
    }

    #[test]
    fn the_origin_is_the_webhook_then_the_creator_then_the_owner_then_the_sender() {
        let webhook = Envelope { owner: Some(UserId::human()), ..message("hook").from_webhook() };
        assert_eq!(by(&webhook), Starter::Webhook { user: user("hook") });
        assert!(!origin(&webhook, None, &Humans::default()).trusted());
        let created = Envelope { creator: Some(user("dev-c")), owner: Some(UserId::human()), ..message("dev-b") };
        assert_eq!(by(&created), Starter::Ant { user: user("dev-c") });
        let owned = Envelope { owner: Some(UserId::human()), ..message("dev-b") };
        assert_eq!(by(&owned), Starter::Human { user: UserId::human() });
        let assisted = message("assistant").on_behalf(Some(UserId::human()));
        assert_eq!(by(&assisted), Starter::Human { user: UserId::human() });
        assert_eq!(by(&message("human")), Starter::Human { user: UserId::human() });
        assert_eq!(by(&message("dev-b")), Starter::Ant { user: user("dev-b") });
        let narrowed = message("dev-b").with_allow(Some(vec!["read:*".to_owned()]));
        assert_eq!(by(&narrowed), Starter::Webhook { user: user("dev-b") }, "carrying on a webhook's thread");
        let channel: ChannelId = "ops".parse().unwrap();
        assert_eq!(origin(&message("dev-b"), Some(channel.clone()), &Humans::default()).channel, Some(channel));
    }

    #[test]
    fn the_first_origin_stays_until_one_from_outside_the_bus() {
        let ops: ChannelId = "ops".parse().unwrap();
        let human = Origin { by: Starter::Human { user: UserId::human() }, channel: None };
        let ant = Origin { by: Starter::Ant { user: user("dev-b") }, channel: Some(ops.clone()) };
        let hook = Origin { by: Starter::Webhook { user: user("hook") }, channel: None };
        assert_eq!(settled(None, ant.clone()), ant);
        assert_eq!(settled(Some(human.clone()), ant.clone()), Origin { channel: Some(ops.clone()), ..human.clone() });
        let untrusted = settled(Some(ant), hook.clone());
        assert_eq!(untrusted, Origin { channel: Some(ops), ..hook.clone() });
        assert_eq!(settled(Some(hook.clone()), human), hook, "a human's message keeps it untrusted");
    }
}
