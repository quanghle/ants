//! The [`CallContext`] of a provider call, as the daemon serving the ant knows it: never taken from the ant's text.

use ants_proto::provider::{CallContext, Origin, Starter};
use ants_proto::{ChannelId, Envelope, Humans, ThreadId, UserId};
use chrono::Utc;

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
/// it is the message: a webhook's message, the ant that started the thread with a channel post (its creator), the
/// human who started it (its owner), else the message's sender, a human or an ant. Read from the message at hand, so
/// a thread a webhook started counts as a human's once its owner messages on it.
pub fn origin(envelope: &Envelope, channel: Option<ChannelId>, humans: &Humans) -> Origin {
    let by = if envelope.webhook {
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
        let channel: ChannelId = "ops".parse().unwrap();
        assert_eq!(origin(&message("dev-b"), Some(channel.clone()), &Humans::default()).channel, Some(channel));
    }
}
