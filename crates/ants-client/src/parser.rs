//! Reading stored messages into [`Entry`]s: matching answers to their questions and decisions to their requests
//! ([`Parser`]). Opening a sealed message and checking who acts on whose behalf need lookups the client reading makes
//! ([`Lookups`]); without them a sealed message is an entry marked sealed, and a claim to act for a human forged.

use std::collections::HashMap;

use ants_proto::{
    Acting, Answer, ApprovalDecision, ApprovalRequest, ChannelPost, DeadLetter, Envelope, NotesPost, Questions,
    ThreadId, UserId, Uuid, subject,
};
use chrono::{DateTime, Utc};

use crate::attachments;
use crate::entry::{Entry, Kind};
use crate::sealed;
use crate::text::{escalation_note, spelling};

/// What a [`Parser`] asks of the client reading, which looked up what it takes beforehand: the devices whose keys open
/// a sealed message, and the grants that let an assistant act for its human.
pub trait Lookups {
    /// `envelope` as the reader reads it, with what it made of it if sealed: opened, its text, suggested title, quote
    /// and attachments put back; else its text the placeholder its state shows ([`sealed::State::placeholder`]).
    /// Either way no longer sealed. By default every sealed message is unreadable.
    fn read(&self, envelope: Envelope) -> (Envelope, Option<sealed::State>) {
        if envelope.sealed.is_none() {
            return (envelope, None);
        }
        (sealed::unopened(envelope, sealed::State::Unreadable), Some(sealed::State::Unreadable))
    }

    /// What `from`'s act claiming to be for `claim` comes to. By default any claim is forged.
    fn acting(&self, from: &UserId, claim: Option<&UserId>) -> Acting {
        let _ = from;
        claim.map_or(Acting::Own, |human| Acting::Forged(human.clone()))
    }
}

/// No lookups: every sealed message unreadable, every claim forged.
pub struct NoLookups;

impl Lookups for NoLookups {}

/// Turns stored messages into [`Entry`]s, in stream order. A human's decision does not name its request's thread, so
/// the threads of the requests read so far are kept to place it, and an answer names its question by id only, so the
/// questions read so far are kept to check it.
#[derive(Default)]
pub struct Parser {
    requests: HashMap<(UserId, Uuid), ThreadId>,
    questions: HashMap<Uuid, Envelope>,
}

impl Parser {
    /// The valid question `e` asks, kept to check later answers against, and its valid answer to a question read
    /// earlier, given by its sender or by the human `acting` says it acts for; none of either for a forged message.
    fn question_and_answer(
        &mut self,
        e: &Envelope,
        forged: bool,
        acting: &Acting,
    ) -> (Option<Questions>, Option<Answer>) {
        if forged {
            return (None, None);
        }
        let question = e.asks().cloned();
        if question.is_some() {
            self.questions.insert(e.id, Envelope { text: String::new(), ..e.clone() });
        }
        let answerer = match acting {
            Acting::For(human) => human,
            Acting::Own | Acting::Forged(_) => &e.from,
        };
        let asked = e.answer.as_ref().and_then(|a| self.questions.get(&a.question));
        (question, asked.filter(|q| e.answers_as(q, answerer).is_some()).and(e.answer.clone()))
    }

    /// The entry of the envelope stored at `seq` on inbox `subject`, None for one that does not parse.
    fn envelope_entry(
        &mut self,
        lookups: &dyn Lookups,
        seq: u64,
        stored_at: DateTime<Utc>,
        subject: &str,
        payload: &[u8],
    ) -> Option<Entry> {
        let e: Envelope = serde_json::from_slice(payload).ok()?;
        Some(self.envelope_with(lookups, seq, stored_at, subject, e))
    }

    /// The entry of `e`, an envelope stored at `seq` on inbox `subject` and read already, as [`Parser::entry_with`]
    /// reads it.
    pub fn envelope_with(
        &mut self,
        lookups: &dyn Lookups,
        seq: u64,
        stored_at: DateTime<Utc>,
        subject: &str,
        e: Envelope,
    ) -> Entry {
        let entry = |kind, thread| Entry::new(seq, stored_at, kind, thread);
        let forged = e.check_subject(subject).is_err();
        let (e, sealed) = lookups.read(e);
        let acting = acting(lookups, &e.from, e.on_behalf_of.as_ref());
        let (question, answer) = self.question_and_answer(&e, forged || sealed == Some(sealed::State::Forged), &acting);
        let provider = e.wake_provider().filter(|_| !forged).map(str::to_owned);
        let watcher = e.wake_watcher().filter(|_| !forged).cloned().map(Box::new);
        let (kind, note, text) = if let Some(d) = &e.approval {
            let text = format!("{} on {}/{}", spelling(d.verdict), d.ant, d.request_id);
            (Kind::Decision, None, text)
        } else if let Some(x) = &e.escalation {
            (Kind::Escalation, Some(escalation_note(x)), e.text)
        } else if e.mention.is_some() {
            (Kind::Mention, None, e.text)
        } else if let Some(notice) = e.notice.filter(|_| !forged) {
            (Kind::Message, Some(format!("notice: {}", notice.as_str())), e.text)
        } else if let Some(provider) = &provider {
            (Kind::Message, Some(format!("wake from {provider}")), e.text)
        } else {
            (Kind::Message, None, e.text)
        };
        let notice = e.notice.filter(|_| kind == Kind::Message && !forged);
        let channel = e.mention.map(|m| m.channel);
        Entry {
            id: Some(e.id),
            from: Some(e.from),
            to: Some(e.to),
            channel,
            team: e.team,
            owner: e.owner,
            note,
            text,
            quote: e.quote,
            // Under the handles of the subject it was stored on, a forged one's too: its sender stored them.
            attachments: attachments::stored_on(subject, &e.attachments, stored_at),
            forged,
            acting,
            stuck: e.escalation.as_ref().map(|x| x.stuck.clone()),
            sealed,
            reason: e.escalation.map(|x| x.reason),
            // Only an ant suggests a title, and no ant seals.
            suggested_title: e.title.filter(|_| !forged && sealed.is_none()),
            summary: e.summary.filter(|_| !forged),
            question,
            answer,
            turns: Some(e.turn_budget),
            webhook: e.webhook,
            provider,
            watcher,
            origin: e.origin_thread,
            in_reply_to: e.in_reply_to,
            notice,
            ..entry(kind, e.thread_id)
        }
    }

    /// The entry of the message stored at `seq` on `subject`, None for one that is not ants' or does not parse, read
    /// with no lookups ([`NoLookups`]).
    pub fn entry(&mut self, seq: u64, stored_at: DateTime<Utc>, subject: &str, payload: &[u8]) -> Option<Entry> {
        self.entry_with(&NoLookups, seq, stored_at, subject, payload)
    }

    /// [`Parser::entry`], a sealed message read and a claim to act for a human checked as `lookups` tells.
    pub fn entry_with(
        &mut self,
        lookups: &dyn Lookups,
        seq: u64,
        stored_at: DateTime<Utc>,
        subject: &str,
        payload: &[u8],
    ) -> Option<Entry> {
        let entry = |kind, thread| Entry::new(seq, stored_at, kind, thread);
        if subject::parse_inbox(subject).is_some() {
            return self.envelope_entry(lookups, seq, stored_at, subject, payload);
        }
        if subject::parse_chan(subject).is_some() {
            let post: ChannelPost = serde_json::from_slice(payload).ok()?;
            let forged = post.check_subject(subject).is_err();
            let note = Some(format!("{} turns left", post.turn_budget));
            let acting = acting(lookups, &post.from, post.on_behalf_of.as_ref());
            return Some(Entry {
                attachments: attachments::stored_on(subject, &post.attachments, stored_at),
                id: Some(post.id),
                from: Some(post.from),
                acting,
                channel: Some(post.channel),
                mentions: post.mentions,
                owner: post.owner,
                note,
                text: post.text,
                quote: post.quote,
                forged,
                suggested_title: post.title.filter(|_| !forged),
                turns: Some(post.turn_budget),
                in_reply_to: post.in_reply_to,
                ..entry(Kind::Post, post.thread_id)
            });
        }
        if subject::parse_notes(subject).is_some() {
            let post: NotesPost = serde_json::from_slice(payload).ok()?;
            let forged = post.check_subject(subject).is_err();
            let note = Some(format!("{} turns left", post.turn_budget));
            let acting = acting(lookups, &post.from, post.on_behalf_of.as_ref());
            return Some(Entry {
                attachments: attachments::stored_on(subject, &post.attachments, stored_at),
                id: Some(post.id),
                from: Some(post.from),
                acting,
                note,
                text: post.text,
                forged,
                suggested_title: post.title.filter(|_| !forged),
                turns: Some(post.turn_budget),
                ..entry(Kind::Note, post.thread_id)
            });
        }
        if let Some((_, request)) = subject::parse_approvals(subject) {
            if request.is_some() {
                let d: ApprovalDecision = serde_json::from_slice(payload).ok()?;
                let thread = self.requests.get(&(d.ant.clone(), d.request_id))?.clone();
                let forged = d.subject() != subject;
                let text = format!("{} on {}/{}", spelling(d.verdict), d.ant, d.request_id);
                return Some(Entry { to: Some(d.ant), text, forged, ..entry(Kind::Decision, thread) });
            }
            let r: ApprovalRequest = serde_json::from_slice(payload).ok()?;
            let forged = r.check_subject(subject).is_err();
            if !forged {
                self.requests.insert((r.ant.clone(), r.id), r.thread_id.clone());
            }
            let options = r.options.iter().map(|v| spelling(*v)).collect::<Vec<_>>().join("|");
            let (note, text) = match r.cancelled_at {
                Some(_) => {
                    (format!("{}/{} (gone)", r.ant, r.id), format!("cancelled with its prompt: {}", r.tool.title))
                }
                None => (format!("{}/{} ({options})", r.ant, r.id), format!("{}: {}", r.tool.kind, r.tool.title)),
            };
            let note = Some(note);
            return Some(Entry { from: Some(r.ant), note, text, forged, ..entry(Kind::Approval, r.thread_id) });
        }
        if let Some(ant) = subject::parse_dead(subject) {
            let d: DeadLetter = serde_json::from_slice(payload).ok()?;
            let forged = d.ant.as_str() != ant;
            let note = Some(format!("after {} deliveries: {}", d.deliveries, d.error));
            let e = d.envelope?;
            let text = match &e.sealed {
                Some(sealed) => sealed::size_note(sealed),
                None => e.text.clone(),
            };
            return Some(Entry {
                // Stored under the handles of the message as it was sent.
                attachments: attachments::listed(&e.attachments, |id| e.attachment_handle(id)),
                from: Some(e.from),
                to: Some(d.ant),
                owner: e.owner,
                note,
                text,
                forged,
                ..entry(Kind::Dead, e.thread_id)
            });
        }
        None
    }
}

/// What `from`'s act claiming `claim` comes to, as `lookups` tells of a claim.
fn acting(lookups: &dyn Lookups, from: &UserId, claim: Option<&UserId>) -> Acting {
    if claim.is_none() { Acting::Own } else { lookups.acting(from, claim) }
}

#[cfg(test)]
mod tests;
