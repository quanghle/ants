//! What the credentials of an ant that is the assistant of a human allow ([`Permissions::for_assistant`]): an ant's
//! rights and a human's reads, with the acts for its human on subjects and keys naming who acts.

use super::human::{notify_prefs_rights, read_marker_rights, thread_rights};
use super::{Permissions, covers, kv_stream};
use crate::{
    ATTEMPTS_BUCKET, CHANNELS_BUCKET, FILES_BUCKET, FILES_STREAM, HUMANS_BUCKET, IDENTITIES_BUCKET, INTAKE_STREAM,
    OUTBOX_STREAM, OWNERS_BUCKET, Role, THREADS_BUCKET, TRAILS_BUCKET, UserId, assisted_marks_key,
    assisted_reminders_key, chan_reader, inbox_consumer, inbox_prefix, notes_consumer, notify_consumer, sent_reader,
    spare_readers, subject,
};
#[cfg(doc)]
use crate::{AssistantOf, assisted_owner_key, assisted_withdrawal_key, unserved_key};

impl Permissions {
    /// What the credentials of `assistant`, the assistant of `human` ([`AssistantOf`]), allow: everything an ant may do,
    /// as it is one run by a daemon, and what a human may read and keep of its own besides ([`Self::for_user`]): it
    /// watches all ants traffic, so its human's home queue, reads the stream and the buckets a human reads, sets
    /// titles, TL;DRs and done states, puts its own reactions and read markers, and reads every attachment and every
    /// device's encryption key. Of its human's own keys it reads, by direct gets, never writes, the read markers
    /// ([`read_marker_rights`]) and the markers of messages kept waiting ([`unserved_key`]), so it sees what its human
    /// has read and set aside. It keeps no pins, archives or snoozed threads of its own: it reads its human's
    /// ([`thread_rights`]) and writes only those it sets for the human, under keys naming it ([`assisted_marks_key`],
    /// [`assisted_reminders_key`]), never the human's own, which readers take in only while the human's grant names it.
    ///
    /// It reads the stream back by direct gets, as a human does, so it holds none of an ant's reader consumers but its
    /// dead letters' (its inbox and channel consumers still deliver), no spare of one, and no consumer of a human's
    /// notifications: their rights would not fit a token within `max_control_line` once doubled for a JetStream
    /// domain.
    ///
    /// It sends only as itself, never under any sender as a human may, nor writes attachments under another's name.
    /// It cancels an ant's prompt, withdraws a message waiting for one, and asks for an ant's queue only on the subjects
    /// naming it ([`subject::cancels_by`], [`subject::queues_by`]), whose daemons honour them only for its human, and
    /// records a thread's owner only under keys naming its human ([`assisted_owner_key`]) and a withdrawal no daemon
    /// answered only under keys naming it ([`assisted_withdrawal_key`]). It gets none of a human's
    /// rights whose subjects do not name who acts, which readers could not tell from its human's: cancelling or asking
    /// for a queue on [`subject::ALL_CANCELS`] and [`subject::ALL_QUEUE_REQUESTS`] (whose request's `by` no reader can
    /// check), creating message marks in [`ATTEMPTS_BUCKET`], recording an owner under any key, putting dead letters,
    /// answering an ant's reactions requests or another's renewal, setting another ant's avatar
    /// ([`IDENTITIES_BUCKET`]), saving or deleting a trail ([`TRAILS_BUCKET`], whose entries name the human who saved
    /// them). Nor does it
    /// decide approval requests on [`subject::ALL_APPROVAL_DECISIONS`]: it decides one for its human in a message to
    /// the ant (`on_behalf_of`), which waits for its human's confirmation, as readers ignore a stored decision carrying
    /// `on_behalf_of` and one stored there would keep its human's own from being stored. It sends admin orders for its
    /// human (`on_behalf_of`), which daemons hold for the human's confirmation. Nor does it change the team: no entry of [`HUMANS_BUCKET`] (where its human's grant stands) or [`CHANNELS_BUCKET`],
    /// whose description of a channel it asks the channel manager to change for its human instead, no stream created, updated or purged of a message. It puts no device encryption key, no presence and no
    /// notification holds, which only a human's devices have, and it gets no power of the account key: it signs no
    /// credentials and records no revocation.
    pub fn for_assistant(assistant: &UserId, human: &UserId) -> Self {
        let mut publish = Self::shared_publish(assistant, Role::Ant);
        let mut subscribe = vec![format!("{}.>", inbox_prefix(assistant))];
        Self::ant_rights(assistant, &mut publish, &mut subscribe);
        let mut as_human = Vec::new();
        Self::human_rights(assistant, &mut as_human, &mut subscribe);
        // It reads the stream back by direct gets, as a human does: no reader consumer of an ant's but its dead
        // letters' (the inbox and channel consumers deliver), no spare of one, and no consumer of a human's
        // notifications, which it has no preferences for.
        let (inbox, notes, chan_read) = (inbox_consumer(assistant), notes_consumer(assistant), chan_reader(assistant));
        let mut readers: Vec<String> =
            [&inbox, &notes, &chan_read].into_iter().flat_map(|r| spare_readers(r)).collect();
        readers.extend([notes, chan_read, sent_reader(assistant), notify_consumer(assistant)]);
        let reads_through = |s: &str| s.split('.').any(|token| readers.iter().any(|r| r == token));
        publish.retain(|s| !reads_through(s));
        as_human.retain(|s| !reads_through(s));
        let devices_only = [subject::devices_of(assistant), subject::presence_of(assistant)];
        let own_marks =
            [format!("$KV.{THREADS_BUCKET}.*.{assistant}"), format!("$KV.{THREADS_BUCKET}.*.{assistant}.*")];
        let notify = notify_prefs_rights(assistant);
        let unattributed = [
            // Acts for its human that wait for the human's confirmation, which these subjects cannot carry.
            subject::ALL_APPROVAL_DECISIONS.to_owned(),
            // Subjects naming no sender, whose readers could not tell its acts from its human's.
            subject::ALL_CANCELS.to_owned(),
            subject::ALL_QUEUE_REQUESTS.to_owned(),
            subject::ALL_REACTIONS_REPLIES.to_owned(),
            subject::ALL_RENEW_REQUESTS.to_owned(),
            subject::DEAD_ALL.to_owned(),
            format!("$KV.{ATTEMPTS_BUCKET}.*.msg.*"),
            format!("$KV.{OWNERS_BUCKET}.*.*"),
            "ants.out.owners.*.*".to_owned(),
            format!("$KV.{CHANNELS_BUCKET}.>"),
            format!("$KV.{IDENTITIES_BUCKET}.*"),
            format!("$KV.{TRAILS_BUCKET}.*"),
            format!("$JS.API.STREAM.INFO.{OUTBOX_STREAM}"),
            format!("$JS.API.STREAM.INFO.{INTAKE_STREAM}"),
            format!("$JS.API.STREAM.MSG.GET.{INTAKE_STREAM}"),
        ];
        let humans = format!("$KV.{HUMANS_BUCKET}.");
        let withheld = |s: &str| {
            s.starts_with(&humans)
                || ["$JS.API.STREAM.CREATE.", "$JS.API.STREAM.UPDATE.", "$JS.API.STREAM.MSG.DELETE.", "$JS.*.API."]
                    .iter()
                    .any(|prefix| s.starts_with(prefix))
        };
        let marks_of_its_own = |s: &str| own_marks.iter().any(|k| s.ends_with(k.as_str()));
        as_human.retain(|s| {
            !devices_only.contains(s)
                && !notify.contains(s)
                && !unattributed.contains(s)
                && !withheld(s)
                && !marks_of_its_own(s)
        });
        publish.extend(as_human);
        let chunks = format!("$O.{FILES_BUCKET}.C");
        publish.push(format!("$JS.API.DIRECT.GET.{FILES_STREAM}.{chunks}.>"));
        let gets_only = |rights: &[String]| -> Vec<String> {
            rights.iter().filter(|s| s.starts_with("$JS.API.DIRECT.GET.")).cloned().collect()
        };
        // The record of its credentials, which readers check the claims of its acts for its human against.
        publish.push(subject::assistant_record(assistant));
        // Acts for its human on subjects and keys naming who acts.
        publish.extend([
            subject::cancels_by(assistant),
            subject::queues_by(assistant),
            format!("$KV.{OWNERS_BUCKET}.*.*.{human}.{assistant}"),
        ]);
        publish.extend(gets_only(&read_marker_rights(human)));
        publish.extend(gets_only(&thread_rights(human)));
        // The pins, archives and snoozes it sets for its human, under keys naming it.
        publish.extend(
            [assisted_marks_key(human, assistant), assisted_reminders_key(human, assistant)]
                .map(|k| format!("$KV.{THREADS_BUCKET}.{k}")),
        );
        let attempts = kv_stream(ATTEMPTS_BUCKET);
        publish.push(format!("$JS.API.DIRECT.GET.{attempts}.$KV.{ATTEMPTS_BUCKET}.{human}.unserved.*"));
        // The withdrawals it records for its human while no daemon of the ant answers, under keys naming it.
        let withdrawals = format!("$KV.{ATTEMPTS_BUCKET}.*.msg.*.{assistant}");
        publish.extend([format!("$JS.API.DIRECT.GET.{attempts}.{withdrawals}"), withdrawals]);
        let mut seen = std::collections::HashSet::new();
        publish.retain(|s| seen.insert(s.clone()));
        // An ant's narrower reads (attachments, the assistants' records) a human's wider ones cover take no room.
        let wider = publish.clone();
        publish.retain(|s| !wider.iter().any(|other| other != s && covers(other, s)));
        let mut seen = std::collections::HashSet::new();
        subscribe.retain(|s| seen.insert(s.clone()));
        let wider = subscribe.clone();
        subscribe.retain(|s| !wider.iter().any(|other| other != s && covers(other, s)));
        Self { publish, subscribe }
    }
}
