//! What the uplink credentials of a machine's leaf node allow ([`Permissions::for_leaf`]): what every member of the
//! leaf may do in its role ([`LeafRole`]) and what the machine's host connection may; and what those of its store
//! uplink allow: the hub's sourcing of the leaf's outbox ([`Permissions::for_leaf_store`]).

use super::{Permissions, flow_control_subjects};
#[cfg(doc)]
use crate::{AssistantOf, EnrollRequest, RenewRequest};
use crate::{Machine, OUTBOX_CONSUMER, OUTBOX_STREAM, Role, UserId, subject};

/// The reply subjects nats-server gives a request that crosses a service import, in the exporting account.
const SERVICE_REPLIES: &str = "_R_.>";

/// The role of a member of a machine's leaf node ([`Permissions::for_leaf`]): an ant or a human in its [`Role`], or an
/// ant that is the assistant of a human ([`AssistantOf`]), with the rights [`Permissions::for_assistant`] gives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeafRole<'a> {
    Ant,
    Human,
    AssistantOf(&'a UserId),
}

impl From<Role> for LeafRole<'_> {
    fn from(role: Role) -> Self {
        match role {
            Role::Ant => Self::Ant,
            Role::Human => Self::Human,
        }
    }
}

impl Permissions {
    /// What the uplink credentials of `machine`'s leaf node allow: everything any of `users` may do in its role,
    /// since every message of a user on the machine crosses that one connection, except the outbox subjects, which
    /// stay on the leaf. Include each human that runs the CLI on the machine, as [`Role::Human`], and an assistant as
    /// [`LeafRole::AssistantOf`] its human, which carries what [`Self::for_assistant`] allows it.
    ///
    /// The machine's own JetStream domain, where its outbox is, is left to the store uplink
    /// ([`Self::for_leaf_store`]): these credentials carry none of it.
    ///
    /// For each pattern of a user's inbox, notes and channel subjects, which has a wildcard before the sender, and of a
    /// human's admin orders, the uplink also gets one literal subject of it (`ants.inbox.dev-a.dev-a`,
    /// `ants.admin.alice.alice`): nats-server lets the hub stream's interest cross the uplink only when it finds such a
    /// subject, and the pattern allows it anyway.
    ///
    /// The uplink also carries enrollment requests from the machine ([`subject::ALL_ENROLL_REQUESTS`]) and their
    /// answers to it ([`subject::ALL_ENROLL_REPLIES`]), for an ant new to the machine: never other machines' requests,
    /// nor answers from the machine, which only the approver gives. A request proves its enrollment credentials by
    /// signature ([`EnrollRequest::signed`]), so the uplink cannot ask for an ant itself; an answer holds no secret, the
    /// seed of the key it completes staying on the asking machine. Likewise renewal requests from the machine
    /// ([`subject::ALL_RENEW_REQUESTS`]), its own among them, and their answers ([`subject::ALL_RENEW_REPLIES`]): each
    /// request proves the credentials it renews by signature ([`RenewRequest::signed`]).
    pub fn for_leaf<'a, R: Into<LeafRole<'a>>>(
        machine: &Machine,
        users: impl IntoIterator<Item = (&'a UserId, R)>,
    ) -> Self {
        let mut leaf = Self { publish: Vec::new(), subscribe: Vec::new() };
        for (user, role) in users {
            let (role, Self { publish, subscribe }) = match role.into() {
                LeafRole::Ant => (Role::Ant, Self::for_user(user, Role::Ant)),
                LeafRole::Human => (Role::Human, Self::for_user(user, Role::Human)),
                LeafRole::AssistantOf(human) => (Role::Ant, Self::for_assistant(user, human)),
            };
            // nats-server lets the hub stream's interest (`ants.inbox.>`, `ants.notes.>`, ...) cross the uplink only if
            // `ReverseMatch` finds a publish allowance under it, and that skips allowances with `*` before the last
            // token, such as `ants.inbox.*.dev-a`. One literal subject each of them already allows is found.
            let mut own = if role.is_human() {
                vec![subject::ALL_ADMIN_ORDERS_SENT.to_owned()]
            } else {
                vec![subject::chanadm_from(user), subject::timeline_from(user)]
            };
            if !role.is_human() && publish.iter().any(|s| s == subject::ALL_ADMIN_ORDERS_SENT) {
                own.push(subject::ALL_ADMIN_ORDERS_SENT.to_owned());
            }
            let found = subject::sendable(user, role).into_iter().chain(own);
            let found = found.map(|s| s.replace('*', user.as_str()));
            for (all, new) in
                [(&mut leaf.publish, publish.into_iter().chain(found).collect()), (&mut leaf.subscribe, subscribe)]
            {
                for s in new {
                    if !all.contains(&s) && !s.starts_with("ants.out.") {
                        all.push(s);
                    }
                }
            }
        }
        // The machine's host connection, which its daemon makes through the leaf node.
        let host = Self::for_host(machine);
        leaf.publish.extend(host.publish);
        leaf.subscribe.extend(host.subscribe);
        for (all, new) in [
            (&mut leaf.publish, [subject::ALL_ENROLL_REQUESTS, subject::ALL_RENEW_REQUESTS]),
            (&mut leaf.subscribe, [subject::ALL_ENROLL_REPLIES, subject::ALL_RENEW_REPLIES]),
        ] {
            for s in new {
                if !all.iter().any(|known| known == s) {
                    all.push(s.to_owned());
                }
            }
        }
        leaf
    }

    /// What the store uplink credentials of `machine`'s leaf node allow, which bind the leaf's store account, holding
    /// its JetStream, to the hub's: the hub's intake may source the leaf's outbox through the machine's own JetStream
    /// domain. The hub sends the leaf requests to create, update and look up the outbox, to create and delete the
    /// consumers that read it, to look up and reset its durable [`OUTBOX_CONSUMER`], and flow control. The leaf
    /// delivers the outbox under [`Machine::deliver_prefix`] and answers under [`Machine::source_replies`] and, for a
    /// request a client of the hub made through the team account's import, under the reply subject the import gave it
    /// (`_R_.…`, `SERVICE_REPLIES`). nats-server carries a reply across a leaf connection only if the leaf may
    /// publish it: the interest behind it reaches the leaf only then.
    ///
    /// Nothing else crosses: the hub's JetStream domain stays on the leaf's other uplink ([`Self::for_leaf`]), so a
    /// client's request to the hub takes one path, not two.
    pub fn for_leaf_store(machine: &Machine) -> Self {
        let api = machine.api_prefix();
        let publish = vec![
            format!("{}.>", machine.deliver_prefix()),
            format!("{}.>", machine.source_replies()),
            SERVICE_REPLIES.to_owned(),
        ];
        let mut subscribe = vec![
            format!("{api}.STREAM.CREATE.{OUTBOX_STREAM}"),
            format!("{api}.STREAM.UPDATE.{OUTBOX_STREAM}"),
            format!("{api}.STREAM.INFO.{OUTBOX_STREAM}"),
            // One filter puts the consumer's name and filter in the subject, several leave them out.
            format!("{api}.CONSUMER.CREATE.{OUTBOX_STREAM}"),
            format!("{api}.CONSUMER.CREATE.{OUTBOX_STREAM}.>"),
            format!("{api}.CONSUMER.DELETE.{OUTBOX_STREAM}.>"),
            // The durable consumer: a human looks it up, and the hub resets it to resume sourcing.
            format!("{api}.CONSUMER.INFO.{OUTBOX_STREAM}.{OUTBOX_CONSUMER}"),
            format!("{api}.CONSUMER.RESET.{OUTBOX_STREAM}.{OUTBOX_CONSUMER}"),
        ];
        // The outbox is in the machine's own domain.
        subscribe.extend(flow_control_subjects(machine.as_str(), OUTBOX_STREAM));
        Self { publish, subscribe }
    }
}
