//! Subject names. Each takes typed ids, so a subject never carries a wildcard or a stray dot.

use crate::{ChannelId, Machine, Role, ThreadId, UserId};

/// Durable inbox of `to`, for messages from `from`. The sender is part of the subject because credentials can pin
/// it there ([`inbox_from`]), while anything in the payload is whatever the publisher wrote.
pub fn inbox(to: &UserId, from: &UserId) -> String {
    format!("ants.inbox.{to}.{from}")
}

/// Every message to `user`, whoever sent it (the human's is `ants.inbox.human.*`).
pub fn inbox_of(user: &UserId) -> String {
    format!("ants.inbox.{user}.*")
}

/// Every message to `user` as the hub's stream stored it, republished there ([`super::STORED_INBOX_SOURCE`]).
pub fn stored_inbox(user: &UserId) -> String {
    format!("{}.stored.*", super::inbox_prefix(user))
}

/// The sender a subject of [`stored_inbox`] of `user` names, or `None` when it is none of them.
pub fn parse_stored_inbox(user: &UserId, subject: &str) -> Option<UserId> {
    subject.strip_prefix(&super::inbox_prefix(user))?.strip_prefix(".stored.")?.parse().ok()
}

/// Every inbox subject `user` may send on.
pub fn inbox_from(user: &UserId) -> String {
    format!("ants.inbox.*.{user}")
}

/// The recipient and the sender an inbox subject names, or `None` when it is no inbox subject.
pub fn parse_inbox(subject: &str) -> Option<(UserId, UserId)> {
    let (to, from) = subject.strip_prefix("ants.inbox.")?.split_once('.')?;
    Some((to.parse().ok()?, from.parse().ok()?))
}

/// A post by `from` to the notes of `thread`. Like an inbox subject it names the sender, so credentials can
/// pin it there ([`notes_from`]).
pub fn notes(thread: &ThreadId, from: &UserId) -> String {
    format!("ants.notes.{thread}.{from}")
}

/// Every post to the notes of `thread`, whoever wrote it.
pub fn notes_of(thread: &ThreadId) -> String {
    format!("ants.notes.{thread}.*")
}

/// Every notes subject `user` may post on.
pub fn notes_from(user: &UserId) -> String {
    format!("ants.notes.*.{user}")
}

/// The thread and the sender a notes subject names, or `None` when it is no notes subject.
pub fn parse_notes(subject: &str) -> Option<(ThreadId, UserId)> {
    let (thread, from) = subject.strip_prefix("ants.notes.")?.split_once('.')?;
    Some((thread.parse().ok()?, from.parse().ok()?))
}

/// The notes of every thread.
pub const ALL_NOTES: &str = "ants.notes.>";

/// A post by `from` to `channel`. Like an inbox subject it names the sender, so credentials can pin it there
/// ([`chan_from`]).
pub fn chan(channel: &ChannelId, from: &UserId) -> String {
    format!("ants.chan.{channel}.{from}")
}

/// Every post to `channel`, whoever wrote it.
pub fn chan_room(channel: &ChannelId) -> String {
    format!("ants.chan.{channel}.*")
}

/// Every channel subject `user` may post on.
pub fn chan_from(user: &UserId) -> String {
    format!("ants.chan.*.{user}")
}

/// The channel and the sender a channel subject names, or `None` when it is no channel subject.
pub fn parse_chan(subject: &str) -> Option<(ChannelId, UserId)> {
    let (channel, from) = subject.strip_prefix("ants.chan.")?.split_once('.')?;
    Some((channel.parse().ok()?, from.parse().ok()?))
}

/// Every channel.
pub const ALL_CHANNELS: &str = "ants.chan.>";

/// Where `ant` asks the channel manager for changes to `channel` ([`ChannelAdminRequest`](super::ChannelAdminRequest);
/// core NATS, not stored). Like an inbox subject it names the sender, so credentials can pin it there.
pub fn chanadm(channel: &ChannelId, ant: &UserId) -> String {
    format!("ants.chanadm.{channel}.{ant}")
}

/// Every channel management subject `ant` may ask on.
pub fn chanadm_from(ant: &UserId) -> String {
    format!("ants.chanadm.*.{ant}")
}

/// The channel and the ant a channel management subject names, or `None` when it is none.
pub fn parse_chanadm(subject: &str) -> Option<(ChannelId, UserId)> {
    let (channel, ant) = subject.strip_prefix("ants.chanadm.")?.split_once('.')?;
    Some((channel.parse().ok()?, ant.parse().ok()?))
}

/// Every channel management request.
pub const ALL_CHANADM: &str = "ants.chanadm.*.*";

/// Where `ant` asks for the reactions to a message ([`ReactionsRequest`](super::ReactionsRequest); core NATS, not
/// stored). Only the ant publishes here.
pub fn reactions(ant: &UserId) -> String {
    format!("ants.reactions.{ant}")
}

/// Where `ant` asks to add or remove its own reaction to a message ([`ReactionsRequest`](super::ReactionsRequest) with
/// a change; core NATS, not stored). Only the ant publishes here, and only `ants signer` answers: no ant writes the
/// reactions bucket itself.
pub fn react(ant: &UserId) -> String {
    format!("ants.reactions.{ant}.react")
}

/// Where the answer to a reactions request of `ant` goes ([`ReactionsReply`](super::ReactionsReply); core NATS): under
/// the ant's reply inboxes ([`inbox_prefix`](super::inbox_prefix)), which only it reads, so a leaf uplink carries it
/// down with no subject of its own. Only humans and `ants signer` answer.
pub fn reactions_reply(ant: &UserId) -> String {
    format!("{}.reactions", super::inbox_prefix(ant))
}

/// Every reactions request, of all ants.
pub const ALL_REACTIONS_REQUESTS: &str = "ants.reactions.*";
/// Every request of an ant to change its reactions.
pub const ALL_REACT_REQUESTS: &str = "ants.reactions.*.react";
/// Every answer to a reactions request.
pub const ALL_REACTIONS_REPLIES: &str = "_INBOX.*.reactions";

/// Whether `subject` is that of a message, a notes post or a channel post the credentials of the ant `ant` read: its
/// own inbox, the messages it sent, the notes of every thread and every channel
/// ([`Permissions::for_user`](super::Permissions::for_user)). Its dead letters and approval requests are no messages.
pub fn readable_by(ant: &UserId, subject: &str) -> bool {
    if let Some((to, from)) = parse_inbox(subject) {
        return &to == ant || &from == ant;
    }
    parse_notes(subject).is_some() || parse_chan(subject).is_some()
}

/// Permission requests an ant escalates to the human ([`ApprovalRequest`](super::ApprovalRequest)).
pub fn approvals(ant: &UserId) -> String {
    format!("ants.approvals.{ant}")
}

/// Messages an ant gave up on, as [`DeadLetter`](super::DeadLetter)s.
pub fn dead(ant: &UserId) -> String {
    format!("ants.dead.{ant}")
}

/// Every ant's dead letters.
pub const DEAD_ALL: &str = "ants.dead.>";

/// The tool-call timeline `ant` stores of its work on `thread` ([`Timeline`](super::Timeline)).
pub fn timeline(thread: &ThreadId, ant: &UserId) -> String {
    format!("ants.timeline.{thread}.{ant}")
}

/// Every timeline subject `ant` may store on.
pub fn timeline_from(ant: &UserId) -> String {
    format!("ants.timeline.*.{ant}")
}

/// The thread and the ant a timeline subject names, or `None` when it is no timeline subject.
pub fn parse_timeline(subject: &str) -> Option<(ThreadId, UserId)> {
    let (thread, ant) = subject.strip_prefix("ants.timeline.")?.split_once('.')?;
    Some((thread.parse().ok()?, ant.parse().ok()?))
}

/// The timelines of every thread.
pub const ALL_TIMELINE: &str = "ants.timeline.>";

/// JetStream's advisories that a consumer of the `ANTS` stream stopped delivering a message after its last allowed
/// delivery went unacked (core NATS, not stored).
pub fn max_deliveries_advisories() -> String {
    format!("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.{}.*", super::STREAM)
}

/// The inbox, notes and channel subjects `user` in `role` may publish on: every inbox for a human (to replay dead
/// letters as they were), else [`inbox_from`]; [`notes_from`]; and [`chan_from`].
pub fn sendable(user: &UserId, role: Role) -> [String; 3] {
    let inbox = if role.is_human() { "ants.inbox.>".to_owned() } else { inbox_from(user) };
    [inbox, notes_from(user), chan_from(user)]
}

/// Where a client on a leaf node puts an inbox, notes, channel, dead letter or timeline subject (or pattern) while the hub's
/// stream is out of reach: `ants.out.` and the rest of it, kept by the leaf's outbox ([`super::OUTBOX_STREAM`]).
/// `None` for other subjects, which are not buffered: approvals wait for a human's answer through the hub.
pub fn outbox(subject: &str) -> Option<String> {
    let rest = subject.strip_prefix("ants.")?;
    ["inbox.", "notes.", "chan.", "dead.", "timeline."]
        .iter()
        .any(|kind| rest.starts_with(kind))
        .then(|| format!("ants.out.{rest}"))
}

/// Everything the hub's intake ([`super::INTAKE_STREAM`]) took from the outbox of `machine`: the leaf's subjects,
/// whatever they are, under the machine's prefix.
pub fn intake(machine: &super::Machine) -> String {
    format!("ants.in.{machine}.>")
}

/// The human's answer to one approval request ([`ApprovalDecision`](super::ApprovalDecision)). Only the human may
/// publish here; the ant that asked subscribes before it publishes the request.
pub fn approval_decision(ant: &UserId, request: &super::Uuid) -> String {
    format!("ants.approvals.{ant}.{request}")
}

/// Every approval request, of all ants.
pub const ALL_APPROVAL_REQUESTS: &str = "ants.approvals.*";
/// Every approval decision, of all ants.
pub const ALL_APPROVAL_DECISIONS: &str = "ants.approvals.*.*";

/// Where `ants add --enroll` asks for the credentials of `ant`
/// ([`EnrollRequest`](super::EnrollRequest); core NATS, not stored). Only the enrollment credentials of `ant`
/// publish here.
pub fn enroll(ant: &UserId) -> String {
    format!("ants.enroll.{ant}")
}

/// Where the answer to an enrollment request of `ant` goes ([`EnrollReply`](super::EnrollReply); core NATS).
pub fn enroll_reply(ant: &UserId) -> String {
    format!("ants.enroll.{ant}.reply")
}

/// Every enrollment request, of all ants.
pub const ALL_ENROLL_REQUESTS: &str = "ants.enroll.*";
/// Every answer to an enrollment request.
pub const ALL_ENROLL_REPLIES: &str = "ants.enroll.*.reply";

/// Where `ants renew` asks for the credentials named `name` renewed for a new key
/// ([`RenewRequest`](super::RenewRequest); core NATS, not stored): an ant, human, webhook or the channel manager
/// for its own, a human also for the uplink of its machine's leaf node (`leaf-<machine>`), which connects as no
/// client. `name` is a [`renewable_name`].
pub fn renew(name: &str) -> String {
    format!("ants.renew.{name}")
}

/// Where the answer to a renewal request of `name` goes ([`RenewReply`](super::RenewReply); core NATS).
pub fn renew_reply(name: &str) -> String {
    format!("ants.renew.{name}.reply")
}

/// Whether `name` is one credentials renewed over NATS may carry: a user id, or `leaf-<machine>` of a leaf node's
/// uplink. Either is one subject token.
pub fn renewable_name(name: &str) -> bool {
    name.parse::<UserId>().is_ok() || name.strip_prefix("leaf-").is_some_and(|m| m.parse::<Machine>().is_ok())
}

/// Every renewal request.
pub const ALL_RENEW_REQUESTS: &str = "ants.renew.*";
/// Every answer to a renewal request.
pub const ALL_RENEW_REPLIES: &str = "ants.renew.*.reply";

/// Where `ants login` asks for the credentials of a new device of the human `name`
/// ([`LoginRequest`](super::LoginRequest); core NATS, not stored). Anyone holding the team's public bootstrap
/// credentials publishes here.
pub fn login(name: &UserId) -> String {
    format!("ants.login.{name}")
}

/// Where the answers to a registration request of `name` go ([`LoginReply`](super::LoginReply); core NATS), each
/// naming the request's key in the clear and sealed to it.
pub fn login_reply(name: &UserId) -> String {
    format!("ants.login.{name}.reply")
}

/// Every registration request.
pub const ALL_LOGIN_REQUESTS: &str = "ants.login.*";
/// Every answer to a registration request.
pub const ALL_LOGIN_REPLIES: &str = "ants.login.*.reply";

/// Where a manager ant asks for an ant added from a template (`request_ant`;
/// [`AntRequest`](super::AntRequest); core NATS, not stored). Only the manager publishes here.
pub fn antreq(manager: &UserId) -> String {
    format!("ants.antreq.{manager}")
}

/// Where the answer to an ant request of `manager` goes ([`AntReply`](super::AntReply); core NATS).
pub fn antreq_reply(manager: &UserId) -> String {
    format!("ants.antreq.{manager}.reply")
}

/// Every ant request, of all managers.
pub const ALL_ANT_REQUESTS: &str = "ants.antreq.*";
/// Every answer to an ant request.
pub const ALL_ANT_REPLIES: &str = "ants.antreq.*.reply";

/// Live progress of an ant on a thread ([`StatusEvent`](super::StatusEvent)s; core NATS, not stored).
pub fn status(ant: &UserId, thread: &ThreadId) -> String {
    format!("ants.status.{ant}.{thread}")
}

/// Live progress of an ant on every thread.
pub fn status_of(ant: &UserId) -> String {
    format!("ants.status.{ant}.*")
}

/// Live progress of every ant on every thread.
pub const ALL_STATUS: &str = "ants.status.*.*";

/// Live progress of every ant on a thread.
pub fn status_on(thread: &ThreadId) -> String {
    format!("ants.status.*.{thread}")
}

/// Where a human asks the daemon of `ant` to cancel the prompt running on `thread`
/// ([`CancelRequest`](super::CancelRequest); core NATS, not stored). Only humans publish here.
pub fn cancel(ant: &UserId, thread: &ThreadId) -> String {
    format!("ants.cancel.{ant}.{thread}")
}

/// The cancel requests for `ant`, on every thread: what its daemon listens to.
pub fn cancel_of(ant: &UserId) -> String {
    format!("ants.cancel.{ant}.*")
}

/// Every cancel request, of all ants.
pub const ALL_CANCELS: &str = "ants.cancel.*.*";

/// Where a daemon of `ant` answers a cancel request on `thread` ([`CancelReply`](super::CancelReply); core NATS).
pub fn cancelled(ant: &UserId, thread: &ThreadId) -> String {
    format!("ants.cancelled.{ant}.{thread}")
}

/// The answers of the daemons of `ant` to cancel requests, on every thread.
pub fn cancelled_of(ant: &UserId) -> String {
    format!("ants.cancelled.{ant}.*")
}

/// Where a human asks the daemons of `ant` which messages they hold ([`QueueRequest`](super::QueueRequest); core
/// NATS, not stored). Only humans publish here, and only that ant's daemons listen.
pub fn queue(ant: &UserId) -> String {
    format!("ants.queue.{ant}")
}

/// Every queue request, of all ants.
pub const ALL_QUEUE_REQUESTS: &str = "ants.queue.*";

/// Where a daemon of `ant` answers a queue request ([`QueueReply`](super::QueueReply); core NATS).
pub fn queued(ant: &UserId) -> String {
    format!("ants.queued.{ant}")
}

/// The roster entry of an ant: the subject its daemon puts [`super::RosterEntry`] on.
pub fn roster(ant: &UserId) -> String {
    format!("$KV.{}.{ant}", super::ROSTER_BUCKET)
}

/// The record of `machine` in [`super::MACHINES_BUCKET`]: the subject its daemon puts [`super::MachineRecord`] on.
pub fn machine_record(machine: &Machine) -> String {
    format!("$KV.{}.{machine}", super::MACHINES_BUCKET)
}

/// Every admin order to the ants of `machine`, `ants.admin.<machine>.<ant>`, which the machine's daemon takes from the
/// stream with its host credentials ([`super::admin_consumer`]).
pub fn admin_orders(machine: &Machine) -> String {
    format!("ants.admin.{machine}.>")
}

/// Where an admin sends its orders about `ant` to the daemon of `machine` ([`super::AdminOrder`]): only humans publish
/// here, the `ANTS` stream keeps them, and only the machine's host credentials take them from it.
pub fn admin_order(machine: &Machine, ant: &UserId) -> String {
    format!("ants.admin.{machine}.{ant}")
}

/// Every admin order about `ant`, on whatever machine.
pub fn admin_orders_to(ant: &UserId) -> String {
    format!("ants.admin.*.{ant}")
}

/// Every admin order a human may send.
pub const ALL_ADMIN_ORDERS_SENT: &str = "ants.admin.*.*";

/// Every admin order, as the `ANTS` stream keeps them.
pub const ALL_ADMIN_ORDERS: &str = "ants.admin.>";

/// The machine and the ant an admin order subject names ([`admin_order`]), or `None` when it is none.
pub fn parse_admin_order(subject: &str) -> Option<(Machine, UserId)> {
    let (machine, ant) = subject.strip_prefix("ants.admin.")?.split_once('.')?;
    Some((machine.parse().ok()?, ant.parse().ok()?))
}

/// The records of every machine in [`super::MACHINES_BUCKET`], whose keys are the machines' names alone, without the
/// outcomes of admin orders kept beside them ([`super::admin_outcome_key`]).
pub fn machine_records() -> String {
    format!("$KV.{}.*", super::MACHINES_BUCKET)
}

/// The outcomes of the admin orders to the ants of `machine` in [`super::MACHINES_BUCKET`], which only its host
/// credentials write.
pub fn admin_outcomes(machine: &Machine) -> String {
    format!("$KV.{}.{machine}.>", super::MACHINES_BUCKET)
}

/// The outcomes of the admin orders about `ant`, on whatever machine ([`super::admin_outcome_key`]).
pub fn admin_outcomes_of(ant: &UserId) -> String {
    format!("$KV.{}.*.{ant}.*", super::MACHINES_BUCKET)
}

/// The chain of an ant in [`super::CHAINS_BUCKET`]: the subject its daemon puts [`super::ChainEntry`] on.
pub fn chain(ant: &UserId) -> String {
    format!("$KV.{}.{ant}", super::CHAINS_BUCKET)
}

/// The entry of a human in [`super::HUMANS_BUCKET`]: the subject a human puts [`super::HumanEntry`] on.
pub fn human_entry(human: &UserId) -> String {
    format!("$KV.{}.{human}", super::HUMANS_BUCKET)
}

/// The [`super::ReadMarker`] of a human in [`super::READ_BUCKET`]: the subject only that human puts it on.
pub fn read_marker(human: &UserId) -> String {
    format!("$KV.{}.{human}", super::READ_BUCKET)
}

/// The [`super::ChannelReadMarkers`] of a human in [`super::READ_BUCKET`]: the subject only that human puts them on.
pub fn channel_reads(human: &UserId) -> String {
    format!("$KV.{}.{}", super::READ_BUCKET, super::channel_reads_key(human))
}

/// The [`super::NotifyPrefs`] of a human in [`super::NOTIFY_BUCKET`]: the subject only that human puts them on.
pub fn notify_prefs(human: &UserId) -> String {
    format!("$KV.{}.{human}", super::NOTIFY_BUCKET)
}

/// The [`super::HeldNotices`] of a human in [`super::NOTIFY_BUCKET`]: the subject only that human's credentials put
/// them on.
pub fn notify_held(human: &UserId) -> String {
    format!("$KV.{}.{}", super::NOTIFY_BUCKET, super::held_key(human))
}

/// Every [`super::Presence`] of a human's devices in [`super::PRESENCE_BUCKET`]: the subjects only that human puts
/// them on.
pub fn presence_of(human: &UserId) -> String {
    format!("$KV.{}.{human}.>", super::PRESENCE_BUCKET)
}

/// Every entry of a human's devices in [`super::DEVICES_BUCKET`]: the subjects only that human puts its device user
/// JWTs on.
pub fn devices_of(human: &UserId) -> String {
    format!("$KV.{}.{human}.>", super::DEVICES_BUCKET)
}

/// The record of `assistant`'s credentials in [`super::DEVICES_BUCKET`] ([`super::AssistantRecord`]): the subject
/// only the assistant's credentials put it on.
pub fn assistant_record(assistant: &UserId) -> String {
    format!("$KV.{}.{}", super::DEVICES_BUCKET, super::assistant_record_key(assistant))
}

/// Every [`super::AssistantRecord`] in [`super::DEVICES_BUCKET`].
pub fn assistant_records() -> String {
    format!("$KV.{}.*.{}", super::DEVICES_BUCKET, super::ASSISTANT_RECORD)
}

/// Every [`super::DeviceRevocation`] in [`super::DEVICES_BUCKET`]: the subjects only the credentials `ants auth revoke`
/// and `ants auth push` issue put records on.
pub fn revoked_devices() -> String {
    format!("$KV.{}.{}.*", super::DEVICES_BUCKET, super::REVOKED_DEVICES)
}

/// The entry of a channel in [`super::CHANNELS_BUCKET`]: the subject a human, or the channel manager, puts
/// [`super::ChannelEntry`] on.
pub fn channel_entry(channel: &ChannelId) -> String {
    format!("$KV.{}.{channel}", super::CHANNELS_BUCKET)
}

/// Everything, for `ants watch`.
pub const ALL: &str = "ants.>";

#[cfg(test)]
mod tests {

    use uuid::Uuid;

    use super::*;
    use crate::{
        Envelope, NotesPost, STREAM_SUBJECTS, chan_consumer, chan_reader, inbox_consumer, p, session_key, subject,
    };

    #[test]
    fn subjects_are_scoped_under_ants() {
        let t: ThreadId = "t1".parse().unwrap();
        assert_eq!(subject::inbox(&p("dev-a"), &UserId::human()), "ants.inbox.dev-a.human");
        assert_eq!(subject::inbox_of(&p("dev-a")), "ants.inbox.dev-a.*");
        assert_eq!(subject::inbox_from(&p("dev-a")), "ants.inbox.*.dev-a");
        assert_eq!(subject::notes(&t, &p("dev-a")), "ants.notes.t1.dev-a");
        assert_eq!(subject::notes_of(&t), "ants.notes.t1.*");
        assert_eq!(subject::notes_from(&p("dev-a")), "ants.notes.*.dev-a");
        assert!(STREAM_SUBJECTS.contains(&subject::ALL_NOTES), "thread notes are durable");
        assert_eq!(subject::approvals(&p("dev-a")), "ants.approvals.dev-a");
        assert_eq!(subject::status(&p("dev-a"), &t), "ants.status.dev-a.t1");
        assert_eq!(subject::status_on(&t), "ants.status.*.t1");
        assert_eq!(subject::dead(&p("dev-a")), "ants.dead.dev-a");
        assert!(STREAM_SUBJECTS.contains(&subject::DEAD_ALL), "dead letters are durable");
        assert_eq!(subject::timeline(&t, &p("dev-a")), "ants.timeline.t1.dev-a");
        assert_eq!(subject::timeline_from(&p("dev-a")), "ants.timeline.*.dev-a");
        assert_eq!(subject::parse_timeline("ants.timeline.t1.dev-a"), Some((t.clone(), p("dev-a"))));
        assert_eq!(subject::parse_timeline("ants.notes.t1.dev-a"), None);
        assert!(STREAM_SUBJECTS.contains(&subject::ALL_TIMELINE), "timelines are durable");
        let m: Machine = "box".parse().unwrap();
        assert_eq!(subject::admin_order(&m, &p("dev-a")), "ants.admin.box.dev-a");
        assert_eq!(subject::parse_admin_order("ants.admin.box.dev-a"), Some((m, p("dev-a"))));
        assert_eq!(subject::parse_admin_order("ants.admin.box"), None);
        assert_eq!(subject::parse_admin_order("ants.inbox.box.dev-a"), None);
        assert!(STREAM_SUBJECTS.contains(&subject::ALL_ADMIN_ORDERS), "admin orders are durable");
        assert_eq!(inbox_consumer(&p("dev-a")), "inbox-dev-a");
        let id = Uuid::now_v7();
        assert_eq!(subject::approval_decision(&p("dev-a"), &id), format!("ants.approvals.dev-a.{id}"));
        assert_eq!(session_key(&p("dev-a"), &t), "dev-a.t1");
    }

    #[test]
    fn the_inbox_subject_names_the_sender() {
        let msg = Envelope::new(p("dev-a"), p("dev-b"), "plan?");
        assert_eq!(subject::parse_inbox(&msg.subject()), Some((p("dev-b"), p("dev-a"))));
        assert_eq!(msg.check_subject("ants.inbox.dev-b.dev-a"), Ok(()));
        let forged = Envelope::new(UserId::human(), p("dev-b"), "approve everything");
        let err = forged.check_subject("ants.inbox.dev-b.dev-a").unwrap_err();
        assert_eq!(err.to_string(), "envelope from human to dev-b arrived on ants.inbox.dev-b.dev-a");
        assert!(msg.check_subject("ants.inbox.dev-c.dev-a").is_err(), "addressed to another inbox");
        for bad in ["ants.inbox.dev-b", "ants.inbox.dev-b.dev-a.x", "ants.notes.t1.dev-a", "ants.inbox.dev-b.*"] {
            assert_eq!(subject::parse_inbox(bad), None, "{bad}");
        }
    }

    #[test]
    fn an_ant_reads_its_own_inbox_what_it_sent_and_every_notes_and_channel_subject() {
        let ant = p("dev-a");
        for readable in ["ants.inbox.dev-a.human", "ants.inbox.dev-b.dev-a", "ants.notes.t1.dev-b", "ants.chan.ops.bob"]
        {
            assert!(readable_by(&ant, readable), "{readable}");
        }
        for unreadable in
            ["ants.inbox.dev-b.human", "ants.inbox.dev-b.dev-c", "ants.dead.dev-a", "ants.approvals.dev-a"]
        {
            assert!(!readable_by(&ant, unreadable), "{unreadable}");
        }
    }

    #[test]
    fn the_notes_subject_names_the_sender() {
        let post = NotesPost::new("t1".parse().unwrap(), p("dev-a"), "split the work");
        assert_eq!(subject::parse_notes(&post.subject()), Some(("t1".parse().unwrap(), p("dev-a"))));
        assert_eq!(post.check_subject("ants.notes.t1.dev-a"), Ok(()));
        let err = post.check_subject("ants.notes.t1.dev-b").unwrap_err();
        assert_eq!(err.to_string(), "notes post from dev-a on thread t1 arrived on ants.notes.t1.dev-b");
        assert!(post.check_subject("ants.notes.t2.dev-a").is_err(), "posted to the notes of another thread");
        for bad in ["ants.notes.t1", "ants.notes.t1.dev-a.x", "ants.inbox.t1.dev-a", "ants.notes.t1.*"] {
            assert_eq!(subject::parse_notes(bad), None, "{bad}");
        }
    }

    #[test]
    fn channel_subjects_name_the_channel_and_the_sender() {
        let c: ChannelId = "ops".parse().unwrap();
        assert_eq!(subject::chan(&c, &p("dev-a")), "ants.chan.ops.dev-a");
        assert_eq!(subject::chan_room(&c), "ants.chan.ops.*");
        assert_eq!(subject::chan_from(&p("dev-a")), "ants.chan.*.dev-a");
        assert_eq!(subject::parse_chan("ants.chan.ops.dev-a"), Some((c.clone(), p("dev-a"))));
        for bad in
            ["ants.chan.ops", "ants.chan.ops.dev-a.x", "ants.notes.ops.dev-a", "ants.chan.ops.*", "ants.chan.Ops.a"]
        {
            assert_eq!(subject::parse_chan(bad), None, "{bad}");
        }
        assert!(STREAM_SUBJECTS.contains(&subject::ALL_CHANNELS), "channel posts are durable");
        assert_eq!(subject::channel_entry(&c), "$KV.ants_channels.ops");
        assert_eq!(
            (chan_consumer(&p("dev-a")), chan_reader(&p("dev-a"))),
            ("chan-dev-a".into(), "chanread-dev-a".into())
        );
        for bad in ["", "a.b", "*", ">", "Ops", "-ops", &"x".repeat(65)] {
            assert!(bad.parse::<ChannelId>().is_err(), "{bad:?} should be rejected");
        }
        assert!(serde_json::from_str::<ChannelId>(r#""a.b""#).is_err());
    }
}
