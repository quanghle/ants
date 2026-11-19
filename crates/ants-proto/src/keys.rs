//! Names of the JetStream consumers, reply prefixes and KV keys of ants: each user's readers of the `ANTS` stream, and
//! the keys of [`crate::SESSIONS_BUCKET`], [`crate::ATTEMPTS_BUCKET`], [`crate::THREADS_BUCKET`],
//! [`crate::WEBHOOKS_BUCKET`] and other buckets, most starting with the user they belong to, so credentials can be
//! confined to their own.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(doc)]
use crate::{
    ATTEMPTS_BUCKET, ApprovalDecision, ApprovalRequest, ChannelPost, ChannelReadMarkers, ColonyEntry, DeadLetter,
    Envelope, HeldNotices, LINKS_BUCKET, Machine, MessageMark, NOTIFY_BUCKET, NotifyPrefs, OUTBOX_STREAM,
    OWNERS_BUCKET, Permissions, REACTIONS_BUCKET, READ_BUCKET, ReadMarker, SESSIONS_BUCKET, THREADS_BUCKET,
};
#[cfg(doc)]
use crate::{
    JournalEntry, TRAIL_RATINGS_BUCKET, TRAIL_TIPS_BUCKET, TipVote, TrailRating, TrailRun, TrailRunIndex, TrailSession,
    TrailStepThread, TrailTip,
};
use crate::{
    PERMISSIONS_VERSION, PermissionsVersion, Reaction, RunId, TRAIL_RUNS_BUCKET, ThreadId, TipId, TrailName, UserId,
    WEBHOOKS_BUCKET, subject,
};

/// Durable JetStream consumer name of a user's inbox. The CLI with an ant's credentials reads the ant's inbox back
/// through one of its [`spare_readers`], with no ack, while the ant's daemon takes the inbox from it.
pub fn inbox_consumer(user: &UserId) -> String {
    format!("inbox-{user}")
}

/// Durable JetStream consumer `ants webhook serve` takes a human's notifications from: the tool requests of every ant
/// and the human's own inbox, each delivered to one process listing the human, which pushes it to the human's
/// notification sinks. Only the human's credentials create it.
pub fn notify_consumer(human: &UserId) -> String {
    format!("notify-{human}")
}

/// Prefix of the reply subjects a user's connections use, so its credentials need not let it read others' replies.
pub fn inbox_prefix(user: &UserId) -> String {
    format!("_INBOX.{user}")
}

/// Key of a thread's harness session in [`SESSIONS_BUCKET`]. Keys start with the ant, so its credentials can be
/// confined to its own.
pub fn session_key(ant: &UserId, thread: &ThreadId) -> String {
    format!("{ant}.{thread}")
}

/// Key of what a thread has spent since the human's last message on it, of its escalation and of the tool rules it is
/// narrowed to ([`Envelope::allow`]), in [`SESSIONS_BUCKET`] beside its session.
pub fn thread_spend_key(ant: &UserId, thread: &ThreadId) -> String {
    format!("{ant}.{thread}.spend")
}

/// Key of the stream sequence of the last post on a channel thread the ant was shown, in [`SESSIONS_BUCKET`] beside
/// the thread's session: its next prompt on the thread shows the posts after it.
pub fn channel_seen_key(ant: &UserId, thread: &ThreadId) -> String {
    format!("{ant}.{thread}.seen")
}

/// Key of where a thread came from as the ant's daemon settled it ([`crate::provider::Origin`]), in
/// [`SESSIONS_BUCKET`] beside the thread's session, so a restarted daemon carries on with it.
pub fn thread_origin_key(ant: &UserId, thread: &ThreadId) -> String {
    format!("{ant}.{thread}.origin")
}

/// Key of the marker that the mention notice of the ant's own channel post at stream sequence `seq` reached `human`,
/// in [`SESSIONS_BUCKET`] ([`ChannelPost::mention_notice`]).
pub fn mention_key(ant: &UserId, seq: u64, human: &UserId) -> String {
    format!("{ant}.mention.{seq}.{human}")
}

/// Key of the ant's escalation deadlines, the threads whose escalation awaits its leader until a time, in
/// [`SESSIONS_BUCKET`]. Each thread's escalation itself is kept with its spend ([`thread_spend_key`]).
pub fn escalation_deadlines_key(ant: &UserId) -> String {
    format!("{ant}.escalations.deadlines")
}

/// Key of what an ant has spent on `day` (`YYYY-MM-DD`, UTC), in [`SESSIONS_BUCKET`].
pub fn daily_spend_key(ant: &UserId, day: &str) -> String {
    format!("{ant}.day.{day}")
}

/// Key in [`SESSIONS_BUCKET`] noting that the humans were warned of JetStream `store` (`file` or `memory`) of `domain`
/// (`_` for none) filling up, shared by every daemon so that one warning goes out per crossing. A user id never starts
/// with `_`, so it is no ant's key, and its four tokens match none of the spend keys.
pub fn storage_warning_key(domain: Option<&str>, store: &str) -> String {
    format!("{STORAGE_WARNING_PREFIX}.{}.{store}.warned", domain.unwrap_or("_"))
}

pub(crate) const STORAGE_WARNING_PREFIX: &str = "_storage";

/// Key of the attempt marker of the inbox message at stream sequence `seq` in [`ATTEMPTS_BUCKET`]. Keys start with the
/// ant, so its credentials can be confined to its own.
pub fn attempt_key(ant: &UserId, seq: u64) -> String {
    format!("{ant}.{seq}")
}

/// Key in [`ATTEMPTS_BUCKET`] of the [`MessageMark`] of message `message` ([`Envelope::id`]) to `ant`: the daemon
/// creates it as it starts the message, a human as it withdraws it, so exactly one of them does; a message awaiting a
/// retry has it `retrying`, which one of them swaps by compare-and-set. Keys start with the
/// ant, so its credentials can be confined to its own; a human writes and reads only these keys of the bucket.
pub fn message_mark_key(ant: &UserId, message: &Uuid) -> String {
    format!("{ant}.msg.{message}")
}

/// Key in [`ATTEMPTS_BUCKET`] of the [`MessageMark::Withdrawn`] that `assistant` records for its human of message
/// `message` to `ant` while no daemon of the ant answers its withdrawal: [`message_mark_key`] with the assistant after
/// it. The daemon checks it before it starts the message, and honours it only if the human's grant names the
/// assistant and it was stored before the message's own mark.
pub fn assisted_withdrawal_key(ant: &UserId, message: &Uuid, assistant: &UserId) -> String {
    format!("{ant}.msg.{message}.{assistant}")
}

/// Key in [`ATTEMPTS_BUCKET`] of the marker that `sender` was told that its message `message` keeps waiting for an
/// ant no daemon serves: the first of the sender's watchers to create it tells, once across watchers and restarts.
/// Keys start with the sender, so its credentials can be confined to its own.
pub fn unserved_key(sender: &UserId, message: &Uuid) -> String {
    format!("{sender}.unserved.{message}")
}

/// Key in [`LINKS_BUCKET`] of the record that `from` passed the link to channel thread `thread` on to `to`. Keys start
/// with the ant that passed it on, so its credentials can be confined to its own.
pub fn link_key(from: &UserId, thread: &ThreadId, to: &UserId) -> String {
    format!("{from}.{thread}.{to}")
}

/// Where a client on a leaf node records a link passed on ([`link_key`]) while the hub is out of reach: the leaf's
/// outbox ([`OUTBOX_STREAM`]) keeps it, and the hub's [`LINKS_BUCKET`] sources it from the hub's intake under the key,
/// for the ants of the machine only ([`Machine::admitted_links`]).
pub fn link_outbox(from: &UserId, thread: &ThreadId, to: &UserId) -> String {
    format!("ants.out.links.{}", link_key(from, thread, to))
}

/// Key in [`OWNERS_BUCKET`] of the owner of `thread` that a message to `to` on it names.
pub fn owner_key(thread: &ThreadId, to: &UserId) -> String {
    format!("{thread}.{to}")
}

/// Key in [`OWNERS_BUCKET`] of `human`, the owner of `thread` that a message to `to` on it names, recorded by
/// `assistant` for it: only the credentials of `assistant` as the assistant of `human` write the keys ending in both,
/// and a reader takes the record only while the human's grant names that assistant.
pub fn assisted_owner_key(thread: &ThreadId, to: &UserId, human: &UserId, assistant: &UserId) -> String {
    format!("{thread}.{to}.{human}.{assistant}")
}

/// The message id (`Nats-Msg-Id`) of every record of `key` in [`OWNERS_BUCKET`], whoever writes it, so the bucket
/// drops a second one, as from an outbox, rather than refuse it.
pub fn owner_record_id(key: &str) -> String {
    format!("owner.{key}")
}

/// Where a human's client on a leaf node records the owner of `thread` that its message to `to` names
/// ([`owner_key`]) while the hub is out of reach: the leaf's outbox ([`OUTBOX_STREAM`]) keeps it, and the hub's
/// [`OWNERS_BUCKET`] sources it from the hub's intake under the key, from machines a human runs on only
/// ([`Machine::admitted_owners`]).
pub fn owner_outbox(thread: &ThreadId, to: &UserId) -> String {
    format!("ants.out.owners.{}", owner_key(thread, to))
}

/// Key in [`REACTIONS_BUCKET`] of `by`'s `reaction` to message `message` ([`Envelope::id`]). Keys start with the
/// reactor, so its credentials can be confined to its own.
pub fn reaction_key(by: &UserId, message: &Uuid, reaction: &Reaction) -> String {
    format!("{by}.{message}.{reaction}")
}

/// Every key in [`REACTIONS_BUCKET`] of a reaction to `message`, whoever reacted.
pub fn reactions_to(message: &Uuid) -> String {
    format!("*.{message}.*")
}

/// Who reacted how to which message, as a [`reaction_key`] names them, or `None` when the key is no reaction.
pub fn parse_reaction_key(key: &str) -> Option<(UserId, Uuid, Reaction)> {
    let mut tokens = key.split('.');
    let (by, message, reaction) = (tokens.next()?, tokens.next()?, tokens.next()?);
    if tokens.next().is_some() {
        return None;
    }
    Some((by.parse().ok()?, message.parse().ok()?, reaction.parse().ok()?))
}

/// Key in [`THREADS_BUCKET`] of the title of `thread`.
pub fn title_key(thread: &ThreadId) -> String {
    format!("title.{thread}")
}

/// Key in [`THREADS_BUCKET`] of the TL;DR of `thread` ([`ThreadSummary`]).
pub fn summary_key(thread: &ThreadId) -> String {
    format!("summary.{thread}")
}

/// Key in [`THREADS_BUCKET`] of the done state of `thread` ([`crate::ThreadDone`]).
pub fn done_key(thread: &ThreadId) -> String {
    format!("done.{thread}")
}

/// Key in [`THREADS_BUCKET`] of the pinned and archived threads of `human`: starting with `marks`, so no thread id
/// or human name collides with a title's key.
pub fn marks_key(human: &UserId) -> String {
    format!("marks.{human}")
}

/// Key in [`THREADS_BUCKET`] of the reminders `human` set and the threads it snoozed on home ([`crate::Reminders`]):
/// starting with `reminders`, so no thread id or human name collides with a title's key.
pub fn reminders_key(human: &UserId) -> String {
    format!("reminders.{human}")
}

/// Key in [`THREADS_BUCKET`] of the threads `assistant` pinned and archived for `human` ([`crate::ThreadMarks`]):
/// [`marks_key`] of the human with the assistant after it, so the assistant writes only its own entries and never the
/// human's.
pub fn assisted_marks_key(human: &UserId, assistant: &UserId) -> String {
    format!("marks.{human}.{assistant}")
}

/// Key in [`THREADS_BUCKET`] of the threads `assistant` snoozed for `human` ([`crate::Reminders::snoozed`]), as
/// [`assisted_marks_key`] is of its marks: [`reminders_key`] of the human with the assistant after it.
pub fn assisted_reminders_key(human: &UserId, assistant: &UserId) -> String {
    format!("reminders.{human}.{assistant}")
}

/// Key in [`READ_BUCKET`] of the posts `human` read of the channels ([`ChannelReadMarkers`]): its user id with
/// `.chan` after it, so it collides with no human's inbox [`ReadMarker`], whose key is the bare user id.
pub fn channel_reads_key(human: &UserId) -> String {
    format!("{human}.chan")
}

/// Key in [`NOTIFY_BUCKET`] of the notices held back from `human` ([`HeldNotices`]): starting with `held`, so it
/// collides with no human's [`NotifyPrefs`], whose key is the bare user id.
pub fn held_key(human: &UserId) -> String {
    format!("held.{human}")
}

/// Every key in [`LINKS_BUCKET`] of a link to `thread` passed on, whoever passed it on to whom.
pub fn links_to(thread: &ThreadId) -> String {
    format!("*.{thread}.*")
}

/// Who passed a link on to whom, as a [`link_key`] names them, or `None` when the key is no link of `thread`.
pub fn parse_link_key(key: &str, thread: &ThreadId) -> Option<(UserId, UserId)> {
    let (from, rest) = key.split_once('.')?;
    let to = rest.strip_prefix(thread.as_str())?.strip_prefix('.')?;
    Some((from.parse().ok()?, to.parse().ok()?))
}

/// JetStream consumer an ant's daemon reads thread notes through, one thread at a time. Its name is fixed, so its
/// credentials need not let it pull from other consumers. The CLI with the ant's credentials reads through one of its
/// [`spare_readers`] instead, and a human gets the posts by message gets, with no consumer.
pub fn notes_consumer(user: &UserId) -> String {
    format!("notes-{user}")
}

/// How many [`spare_readers`] each of an ant's reader consumers has.
pub const SPARE_READERS: usize = 2;

/// The spares of an ant's reader consumer `reader` ([`inbox_consumer`], [`notes_consumer`], [`chan_reader`]),
/// `<reader>_<n>`, with the same filter pinned: the CLI with the ant's credentials takes a free one for each read, so it
/// never deletes the consumer its daemon, or another CLI read, is reading through. A user id has no `_`, so no spare is
/// named like any user's own consumer.
pub fn spare_readers(reader: &str) -> impl Iterator<Item = String> + '_ {
    (1..=SPARE_READERS).map(move |n| format!("{reader}_{n}"))
}

/// Durable JetStream consumer an ant takes channel posts from, to be woken by those meant for it. Its filter is
/// pinned under [`subject::ALL_CHANNELS`], so it cannot be pointed at an inbox.
pub fn chan_consumer(ant: &UserId) -> String {
    format!("chan-{ant}")
}

/// Durable JetStream consumer of [`THREADS_BUCKET`]'s stream an ant's daemon hears of the threads marked done through,
/// to tell the ant's provider. Its filter is pinned to the done states ([`done_key`]), so it reads no other key.
pub fn done_reader(ant: &UserId) -> String {
    format!("done-{ant}")
}

/// JetStream consumer an ant's daemon reads a channel (or a thread of it) back through, one read at a time, like
/// [`notes_consumer`]. Its filter is pinned under [`subject::ALL_CHANNELS`] too. The CLI with the ant's credentials
/// reads through one of its [`spare_readers`], and a human gets the posts by message gets, so no read
/// gets in another's way.
pub fn chan_reader(user: &UserId) -> String {
    format!("chanread-{user}")
}

/// JetStream consumer an ant reads its own dead letters back through, one read at a time, like [`notes_consumer`]. Its
/// filter is pinned to [`subject::dead`] of the ant.
pub fn dead_reader(ant: &UserId) -> String {
    format!("deadread-{ant}")
}

/// JetStream consumer an ant's daemon reads back what the ant stored through, one read at a time, like
/// [`notes_consumer`]: the messages it sent, to rebuild the context of a thread and to find those that keep waiting for
/// an ant no daemon serves ([`unserved_key`]), and its own tool-call timeline, to show its threads in its dreams' review
/// packets. Its filter is pinned to [`subject::inbox_from`] or [`subject::timeline_from`] of the ant.
pub fn sent_reader(ant: &UserId) -> String {
    format!("sentread-{ant}")
}

/// What an observer ([`Permissions::for_observer`]) reads from the `ANTS` stream, each through a consumer of its own
/// ([`Feed::consumer`]) whose filter is pinned in the subject that creates it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Feed {
    /// Every [`ApprovalRequest`], on [`subject::ALL_APPROVAL_REQUESTS`].
    ApprovalRequests,
    /// Every [`ApprovalDecision`], on [`subject::ALL_APPROVAL_DECISIONS`].
    ApprovalDecisions,
    /// Every [`DeadLetter`], on [`subject::DEAD_ALL`].
    DeadLetters,
}

impl Feed {
    pub const ALL: [Feed; 3] = [Feed::ApprovalRequests, Feed::ApprovalDecisions, Feed::DeadLetters];

    /// The subjects of the feed, its consumer's filter.
    pub fn filter(self) -> &'static str {
        match self {
            Feed::ApprovalRequests => subject::ALL_APPROVAL_REQUESTS,
            Feed::ApprovalDecisions => subject::ALL_APPROVAL_DECISIONS,
            Feed::DeadLetters => subject::DEAD_ALL,
        }
    }

    /// The JetStream consumer `observer` reads the feed through: `observe-<feed>-<observer>`, durable or not, as the
    /// observer creates it.
    pub fn consumer(self, observer: &UserId) -> String {
        let feed = match self {
            Feed::ApprovalRequests => "requests",
            Feed::ApprovalDecisions => "decisions",
            Feed::DeadLetters => "dead",
        };
        format!("observe-{feed}-{observer}")
    }
}

/// Key of the event of `webhook` whose id hashes to `hash` in [`WEBHOOKS_BUCKET`]. Keys start with the webhook, so its
/// credentials can be confined to its own.
pub fn webhook_seen_key(webhook: &UserId, hash: &str) -> String {
    format!("{webhook}.{hash}")
}

/// Key of the marker of `webhook` in [`WEBHOOKS_BUCKET`], which `ants webhook serve` (or the daemon running the
/// schedule of that name) puts on connecting, before it delivers, and every [`SERVING_EVERY`] while it runs. Only the
/// webhook's credentials write it, so a daemon honours [`Envelope::webhook`] only from a sender whose marker is there.
/// Its value is the [`Serving`] of the process that put it.
pub fn webhook_marker_key(webhook: &UserId) -> String {
    format!("{webhook}.{WEBHOOK_MARKER}")
}

/// Last token of [`webhook_marker_key`]; event keys end in hex digits.
pub(crate) const WEBHOOK_MARKER: &str = "webhook";

/// Key of the [`Serving`] of `ants channel serve` in [`WEBHOOKS_BUCKET`], put on connecting and every
/// [`SERVING_EVERY`] while it runs. `channel-manager` is a reserved name, so no webhook writes it, and it ends in
/// no [`WEBHOOK_MARKER`], so it makes no webhook of the channel manager.
pub fn channel_manager_serving_key() -> String {
    format!("{}.serve", UserId::channel_manager())
}

/// The subject of [`channel_manager_serving_key`] in the stream of [`WEBHOOKS_BUCKET`].
pub fn channel_manager_serving_subject() -> String {
    format!("$KV.{WEBHOOKS_BUCKET}.{}", channel_manager_serving_key())
}

/// The version of ants, the same for every crate of the workspace.
pub const ANTS_VERSION: &str = env!("CARGO_PKG_VERSION");

/// How often `ants webhook serve`, each schedule and `ants channel serve` put their [`Serving`] again.
pub const SERVING_EVERY: Duration = Duration::from_hours(1);

/// What a process that is no daemon, yet uses credentials `ants auth` issues, publishes of the ants it runs: `ants
/// webhook serve` and each schedule as the value of their marker ([`webhook_marker_key`]), `ants channel serve` under
/// [`channel_manager_serving_key`]. Daemons publish the same in their [`ColonyEntry`]. Readers should treat unknown
/// fields as additions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Serving {
    /// Host name of the machine the process runs on.
    pub machine: String,
    /// [`ANTS_VERSION`] of the process.
    pub version: String,
    /// [`PERMISSIONS_VERSION`] of the process.
    pub perms: PermissionsVersion,
}

impl Serving {
    /// That of this ants, on `machine`.
    pub fn here(machine: impl Into<String>) -> Self {
        Self { machine: machine.into(), version: ANTS_VERSION.to_owned(), perms: PERMISSIONS_VERSION }
    }
}

/// The subject of the marker of `webhook` ([`webhook_marker_key`]) in the stream of [`WEBHOOKS_BUCKET`].
pub fn webhook_marker_subject(webhook: &UserId) -> String {
    format!("$KV.{WEBHOOKS_BUCKET}.{}", webhook_marker_key(webhook))
}

/// The key token of a conductor's [`TrailRunIndex`] ([`trail_runs_index_key`]), which no [`RunId`] is.
pub const TRAIL_RUNS_INDEX: &str = "runs";
/// The key token under which a conductor keeps its [`TrailStepThread`]s ([`trail_step_key`]), which no [`RunId`] is.
pub const TRAIL_STEPS: &str = "steps";

/// Key of a trail run's [`TrailRun`] in [`TRAIL_RUNS_BUCKET`]. Every key of a run starts with its conductor, so its
/// credentials can be confined to its own runs.
pub fn trail_run_key(conductor: &UserId, run: &RunId) -> String {
    format!("{conductor}.{run}")
}

/// Key of a trail run's [`TrailSession`] in [`TRAIL_RUNS_BUCKET`].
pub fn trail_session_key(conductor: &UserId, run: &RunId) -> String {
    format!("{conductor}.{run}.session")
}

/// Key of the [`JournalEntry`] of a trail run's answered call `call` in [`TRAIL_RUNS_BUCKET`].
pub fn trail_call_key(conductor: &UserId, run: &RunId, call: u32) -> String {
    format!("{conductor}.{run}.call.{call}")
}

/// Key of a conductor's [`TrailRunIndex`] in [`TRAIL_RUNS_BUCKET`].
pub fn trail_runs_index_key(conductor: &UserId) -> String {
    format!("{conductor}.{TRAIL_RUNS_INDEX}")
}

/// Key of the [`TrailStepThread`] of the step thread `thread` of one of a conductor's runs in [`TRAIL_RUNS_BUCKET`].
pub fn trail_step_key(conductor: &UserId, thread: &ThreadId) -> String {
    format!("{conductor}.{TRAIL_STEPS}.{thread}")
}

/// The key token under which a conductor indexes its [`TrailStepThread`]s by the thread of their run
/// ([`trail_thread_step_key`]), which no [`RunId`] is.
pub const TRAIL_STEPS_ON: &str = "on";

/// Key of the [`TrailStepThread`] of the step thread `step` of one of a conductor's runs reporting on `thread`, in
/// [`TRAIL_RUNS_BUCKET`]: the index by which a reader takes the steps on one thread alone, the keys starting with
/// [`trail_thread_steps_prefix`].
pub fn trail_thread_step_key(conductor: &UserId, thread: &ThreadId, step: &ThreadId) -> String {
    format!("{}{step}", trail_thread_steps_prefix(conductor, thread))
}

/// The start of every [`trail_thread_step_key`] of a conductor's runs reporting on `thread`.
pub fn trail_thread_steps_prefix(conductor: &UserId, thread: &ThreadId) -> String {
    format!("{conductor}.{TRAIL_STEPS_ON}.{thread}.")
}

/// The subject of every key of a conductor's runs in the stream of [`TRAIL_RUNS_BUCKET`]: those its credentials write.
pub fn trail_runs_subject(conductor: &UserId) -> String {
    format!("$KV.{TRAIL_RUNS_BUCKET}.{conductor}.>")
}

/// Key of a [`TrailTip`] in [`TRAIL_TIPS_BUCKET`]: its trail, then its writer, so credentials can confine an ant to
/// its own tips, then its id.
pub fn trail_tip_key(trail: &TrailName, writer: &UserId, id: &TipId) -> String {
    format!("{trail}.{writer}.{id}")
}

/// Key of `voter`'s [`TipVote`] on the tip [`trail_tip_key`] names in [`TRAIL_TIPS_BUCKET`]: the trail, then the
/// voter in the writer's place, so one subject confines an ant to its own tips and votes, then the tip's writer and id.
pub fn trail_tip_vote_key(trail: &TrailName, writer: &UserId, id: &TipId, voter: &UserId) -> String {
    format!("{trail}.{voter}.{writer}.{id}")
}

/// Key of a [`TrailRating`] in [`TRAIL_RATINGS_BUCKET`]: its trail, then its conductor, so credentials can confine an
/// ant to its own ratings, then the run rated.
pub fn trail_rating_key(trail: &TrailName, conductor: &UserId, run: &RunId) -> String {
    format!("{trail}.{conductor}.{run}")
}

/// The conductor and run a key of [`trail_run_key`] names, or `None` for any other key of [`TRAIL_RUNS_BUCKET`], as
/// a reader listing the headers of every run takes them from the keys `*.*`.
pub fn parse_trail_run_key(key: &str) -> Option<(UserId, RunId)> {
    let (conductor, run) = key.split_once('.')?;
    Some((conductor.parse().ok()?, run.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ReactionEntry, p};

    #[test]
    fn every_key_of_a_run_starts_with_its_conductor() {
        let (dev_a, run) = (p("dev-a"), RunId::generate());
        let keys = [
            trail_run_key(&dev_a, &run),
            trail_session_key(&dev_a, &run),
            trail_call_key(&dev_a, &run, 7),
            trail_runs_index_key(&dev_a),
            trail_step_key(&dev_a, &ThreadId::generate()),
            trail_thread_step_key(&dev_a, &ThreadId::generate(), &ThreadId::generate()),
        ];
        let own = trail_runs_subject(&dev_a);
        let own = own.strip_suffix('>').unwrap();
        for key in &keys {
            assert!(format!("$KV.{TRAIL_RUNS_BUCKET}.{key}").starts_with(own), "{key}");
        }
        assert_eq!(keys[2], format!("dev-a.{run}.call.7"));
        assert_eq!(parse_trail_run_key(&keys[0]), Some((dev_a, run)));
        for other in &keys[1..] {
            assert_eq!(parse_trail_run_key(other), None, "{other}");
        }
    }

    #[test]
    fn a_link_key_names_who_passed_the_link_to_its_thread_on_to_whom() {
        let (thread, other) = (ThreadId::generate(), ThreadId::generate());
        let key = link_key(&p("dev-a"), &thread, &p("dev-b"));
        assert!(key.starts_with("dev-a."), "credentials confine an ant to its own keys");
        assert_eq!(parse_link_key(&key, &thread), Some((p("dev-a"), p("dev-b"))));
        assert_eq!(parse_link_key(&key, &other), None);
        for malformed in [format!("dev-a.{thread}"), format!("dev-a.{thread}.dev-b.x"), format!("{thread}.dev-b")] {
            assert_eq!(parse_link_key(&malformed, &thread), None, "{malformed}");
        }
    }

    #[test]
    fn a_reaction_key_names_the_reactor_first_then_the_message_and_reaction() {
        let message = Uuid::now_v7();
        let eyes: Reaction = "eyes".parse().unwrap();
        let key = reaction_key(&p("dev-a"), &message, &eyes);
        assert_eq!(key, format!("dev-a.{message}.eyes"), "credentials confine a reactor to its own keys");
        assert_eq!(ReactionEntry::new(p("dev-a"), message, eyes.clone()).key(), key);
        assert_eq!(parse_reaction_key(&key), Some((p("dev-a"), message, eyes)));
        for malformed in [format!("dev-a.{message}"), format!("dev-a.{message}.eyes.x"), "dev-a.t1.eyes".to_owned()] {
            assert_eq!(parse_reaction_key(&malformed), None, "{malformed}");
        }
        assert!("Eyes".parse::<Reaction>().is_err() && "a.b".parse::<Reaction>().is_err());
    }
}
