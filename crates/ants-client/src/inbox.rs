//! A human's inbox as its client reads it: the stored messages that are an [`Envelope`] its subject names ([`parse`]),
//! which of the entries read are the human's to read ([`inboxed`]), and which of those its [`ReadMarker`] tells
//! unread ([`unread`], [`unread_by_thread`]). The marker is shared by the human's devices, each changing it by
//! compare-and-set ([`mark_read`], [`mark_unread`]).

use std::collections::BTreeMap;

use ants_proto::{Envelope, ReadMarker, ThreadId, UserId};

use crate::entry::{Entry, Kind};

/// How many times a change of the marker is tried again after another device moved it first.
pub const CONFLICTS: usize = 16;

/// Why a stored message is no message of an inbox.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    /// It is no [`Envelope`].
    #[error("{0}")]
    Malformed(serde_json::Error),
    /// Its subject names another sender, or another recipient, than it does.
    #[error("forged sender: {0}")]
    Forged(ants_proto::Error),
}

/// The envelope of a message stored on `subject`, unless malformed or its sender forged.
pub fn parse(subject: &str, payload: &[u8]) -> Result<Envelope, ParseError> {
    let envelope = serde_json::from_slice::<Envelope>(payload).map_err(ParseError::Malformed)?;
    envelope.check_subject(subject).map_err(ParseError::Forged)?;
    Ok(envelope)
}

/// The marker stored as `value`; why not for a malformed one, which a reader starts over from
/// ([`ReadMarker::default`]).
pub fn marker_of(value: &[u8]) -> Result<ReadMarker, serde_json::Error> {
    serde_json::from_slice(value)
}

/// Mark the messages at `seqs` read in `marker`. `inbox` holds the sequences, in order, of every inbox message of the
/// human stored from `start` on, as the reader read them: when they follow the marker with none left out, it moves
/// past the read ones ([`ReadMarker::compact`]).
pub fn mark_read(marker: &mut ReadMarker, seqs: &[u64], start: Option<u64>, inbox: &[u64]) {
    seqs.iter().for_each(|seq| marker.mark(*seq));
    // The sequences read cover the inbox from `start` on only.
    if start.is_some_and(|start| marker.below + 1 >= start) {
        marker.compact(inbox.iter().copied());
    }
}

/// Mark the messages at `seqs` unread again in `marker`.
pub fn mark_unread(marker: &mut ReadMarker, seqs: &[u64]) {
    seqs.iter().for_each(|seq| marker.unmark(*seq));
}

/// Whether the human's inbox message at `seq` is unread; not while the read marker is unknown.
pub fn unread(marker: Option<&ReadMarker>, seq: u64) -> bool {
    marker.is_some_and(|marker| !marker.is_read(seq))
}

/// Whether `entry` is in the inbox of `me`.
pub fn holds(entry: &Entry, me: &UserId) -> bool {
    matches!(entry.kind, Kind::Message | Kind::Escalation | Kind::Mention) && entry.to.as_ref() == Some(me)
}

/// Whether `entry` is a message for `me` to read: in its inbox ([`holds`]), its sender not forged, and, for a daemon's
/// notice, one a human has to act on.
pub fn inboxed(entry: &Entry, me: &UserId) -> bool {
    holds(entry, me)
        && !entry.forged
        && entry.from.is_some()
        && entry.notice.is_none_or(ants_proto::NoticeKind::needs_action)
}

/// The sequences of the messages among `entries` for `me` to read ([`inboxed`]) that `marker` tells unread, by thread;
/// none while the marker is unknown.
pub fn unread_by_thread<'a>(
    marker: Option<&ReadMarker>,
    me: &UserId,
    entries: impl IntoIterator<Item = &'a Entry>,
) -> BTreeMap<ThreadId, Vec<u64>> {
    let mut found: BTreeMap<ThreadId, Vec<u64>> = BTreeMap::new();
    for entry in entries {
        if inboxed(entry, me) && unread(marker, entry.seq) {
            found.entry(entry.thread_id.clone()).or_default().push(entry.seq);
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use ants_proto::{NoticeKind, Role};
    use chrono::DateTime;

    use super::*;
    use crate::parser::Parser;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    fn entries(envelopes: &[(String, Envelope)]) -> Vec<Entry> {
        let mut parser = Parser::default();
        let stored = envelopes.iter().enumerate();
        stored
            .filter_map(|(i, (subject, e))| {
                let at = DateTime::from_timestamp(1_700_000_000 + i as i64, 0).unwrap();
                parser.entry(i as u64 + 1, at, subject, &serde_json::to_vec(e).unwrap())
            })
            .collect()
    }

    #[test]
    fn a_stored_message_is_an_envelope_its_subject_names() {
        let ask = Envelope::new(p("alice"), p("dev-a"), "fix it");
        let payload = serde_json::to_vec(&ask).unwrap();
        assert_eq!(parse(&ask.subject(), &payload).unwrap(), ask);
        assert!(matches!(parse("ants.inbox.dev-a.bob", &payload), Err(ParseError::Forged(_))));
        assert!(matches!(parse(&ask.subject(), b"not json"), Err(ParseError::Malformed(_))));
        assert!(parse(&ask.subject(), br#"{"stream":"ANTS","seq":2}"#).is_err(), "no envelope");
    }

    #[test]
    fn the_marker_tells_which_of_a_human_s_messages_are_unread_by_thread() {
        let (alice, dev_a) = (p("alice"), p("dev-a"));
        let ask = Envelope::new(alice.clone(), dev_a.clone(), "fix it");
        let reply = ask.reply(dev_a.clone(), "done", Role::Human).unwrap();
        let other = Envelope::new(dev_a.clone(), alice.clone(), "by the way");
        let mut notice = Envelope::on_thread(ask.thread_id.clone(), dev_a, alice.clone(), "queued");
        notice.notice = Some(NoticeKind::Steered);
        let forged = Envelope::on_thread(ask.thread_id.clone(), p("dev-b"), alice.clone(), "psst");
        let read = entries(&[
            (ask.subject(), ask.clone()),
            (reply.subject(), reply),
            (other.subject(), other.clone()),
            (notice.subject(), notice),
            ("ants.inbox.alice.dev-a".to_owned(), forged),
        ]);
        assert_eq!(read.len(), 5);
        assert!(!holds(&read[0], &alice) && holds(&read[1], &alice) && holds(&read[4], &alice));
        assert!(inboxed(&read[1], &alice) && !inboxed(&read[3], &alice) && !inboxed(&read[4], &alice));

        assert!(unread_by_thread(None, &alice, &read).is_empty(), "the marker is unknown");
        let mut marker = ReadMarker::default();
        let by_thread = unread_by_thread(Some(&marker), &alice, &read);
        assert_eq!(by_thread.get(&ask.thread_id), Some(&vec![2]));
        assert_eq!(by_thread.get(&other.thread_id), Some(&vec![3]));

        // Reading the second thread alone keeps an entry above the marker; reading the first moves it past both.
        mark_read(&mut marker, &[3], Some(1), &[2, 3, 4, 5]);
        assert_eq!((marker.below, marker.read.iter().copied().collect::<Vec<_>>()), (0, vec![3]));
        assert_eq!(unread_by_thread(Some(&marker), &alice, &read).len(), 1);
        mark_read(&mut marker, &[2], Some(1), &[2, 3, 4, 5]);
        assert_eq!((marker.below, marker.read.len()), (3, 0));
        assert!(unread_by_thread(Some(&marker), &alice, &read).is_empty());
        assert!(!unread(Some(&marker), 2));

        mark_unread(&mut marker, &[2]);
        assert!(unread(Some(&marker), 2) && !unread(Some(&marker), 3));
        // A read that started past the marker moves nothing.
        let mut late = ReadMarker::default();
        mark_read(&mut late, &[5], Some(4), &[5]);
        assert_eq!((late.below, late.read.len()), (0, 1));
    }
}
