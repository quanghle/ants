//! Reading stored messages into [`Entry`]s: opening sealed ones, matching answers to their questions, and checking who
//! acts on whose behalf ([`Parser`]).

use std::collections::HashMap;
use std::sync::Arc;

use ants_proto::{
    Acting, Answer, ApprovalDecision, ApprovalRequest, ChannelPost, DeadLetter, Envelope, NotesPost, Questions,
    ThreadId, UserId, Uuid, subject,
};
use chrono::{DateTime, Utc};

use super::entry::{Entry, Kind};
use crate::messages::attachments;
use crate::people::sealed;
use crate::tools;

/// Turns stored messages into [`Entry`]s, in stream order. A human's decision does not name its request's thread, so
/// the threads of the requests read so far are kept to place it, and an answer names its question by id only, so the
/// questions read so far are kept to check it. A sealed message between two humans is opened with the keyring given
/// ([`Parser::opening`]); without one, it shows as `[sealed]`.
#[derive(Default)]
pub struct Parser {
    requests: HashMap<(UserId, Uuid), ThreadId>,
    questions: HashMap<Uuid, Envelope>,
    keyring: Option<Arc<sealed::Keyring>>,
    pub(super) behalf: Option<Arc<crate::people::behalf::Checker>>,
}

impl Parser {
    /// A parser opening sealed messages with `keyring`.
    pub fn opening(keyring: Option<Arc<sealed::Keyring>>) -> Self {
        Self { keyring, ..Self::default() }
    }

    /// The parser, checking the `on_behalf_of` of inbox messages with `behalf`; without it, every such claim is forged.
    pub fn checking(self, behalf: Option<Arc<crate::people::behalf::Checker>>) -> Self {
        Self { behalf, ..self }
    }

    /// Look the sending device of a sealed message stored on `subject` up unless known ([`sealed::Keyring::learn`]),
    /// and check the claim of an inbox message, channel post or notes post made on behalf of a human
    /// ([`crate::people::behalf::Checker::check`]), before [`Parser::entry`] takes it, so one from a device added since the
    /// keyring was loaded opens.
    pub async fn learn(&self, subject: &str, payload: &[u8]) {
        /// The part of any of the three that names a claim.
        #[derive(serde::Deserialize)]
        struct Claim {
            from: UserId,
            on_behalf_of: Option<UserId>,
        }
        let claimed = || payload.windows(14).any(|w| w == b"\"on_behalf_of\"");
        let kept = || {
            subject::parse_inbox(subject).is_some()
                || subject::parse_chan(subject).is_some()
                || subject::parse_notes(subject).is_some()
        };
        if let Some(behalf) = &self.behalf
            && claimed()
            && kept()
            && let Ok(claim) = serde_json::from_slice::<Claim>(payload)
        {
            Box::pin(behalf.check(&claim.from, claim.on_behalf_of.as_ref())).await;
        }
        let Some(keyring) = &self.keyring else { return };
        if subject::parse_inbox(subject).is_none() || !payload.windows(8).any(|w| w == b"\"sealed\"") {
            return;
        }
        if let Ok(envelope) = serde_json::from_slice::<Envelope>(payload) {
            // Boxed: the directory lookup would otherwise swell every read of the stream.
            Box::pin(keyring.learn(&envelope)).await;
        }
    }

    /// [`Parser::entry`], the sending device of a sealed message looked up first ([`Parser::learn`]).
    pub async fn read(&mut self, seq: u64, stored_at: DateTime<Utc>, subject: &str, payload: &[u8]) -> Option<Entry> {
        self.learn(subject, payload).await;
        self.entry(seq, stored_at, subject, payload)
    }

    /// The valid question `e` asks, kept to check later answers against, and its valid answer to a question read
    /// earlier; none of either for a forged message.
    fn question_and_answer(&mut self, e: &Envelope, forged: bool) -> (Option<Questions>, Option<Answer>) {
        if forged {
            return (None, None);
        }
        let question = e.asks().cloned();
        if question.is_some() {
            self.questions.insert(e.id, Envelope { text: String::new(), ..e.clone() });
        }
        let asked = e.answer.as_ref().and_then(|a| self.questions.get(&a.question));
        (question, asked.filter(|q| e.answers(q).is_some()).and(e.answer.clone()))
    }

    /// What `from`'s act claiming `claim` comes to, as checked by [`Parser::learn`]: without a checker, any claim is
    /// forged.
    fn acting(&self, from: &UserId, claim: Option<&UserId>) -> Acting {
        match (&self.behalf, claim) {
            (_, None) => Acting::Own,
            (Some(behalf), claim) => behalf.acting(from, claim),
            (None, Some(human)) => Acting::Forged(human.clone()),
        }
    }

    /// The entry of the envelope stored at `seq` on inbox `subject`, None for one that does not parse.
    fn envelope_entry(&mut self, seq: u64, stored_at: DateTime<Utc>, subject: &str, payload: &[u8]) -> Option<Entry> {
        let entry = |kind, thread| Entry::new(seq, stored_at, kind, thread);
        let e: Envelope = serde_json::from_slice(payload).ok()?;
        let forged = e.check_subject(subject).is_err();
        let (e, sealed) = sealed::read(e, self.keyring.as_deref());
        let (question, answer) = self.question_and_answer(&e, forged || sealed == Some(sealed::State::Forged));
        let acting = self.acting(&e.from, e.on_behalf_of.as_ref());
        let (kind, note, text) = if let Some(d) = &e.approval {
            let text = format!("{} on {}/{}", tools::spelling(d.verdict), d.ant, d.request_id);
            (Kind::Decision, None, text)
        } else if let Some(x) = &e.escalation {
            (Kind::Escalation, Some(crate::text::escalation_note(x)), e.text)
        } else if e.mention.is_some() {
            (Kind::Mention, None, e.text)
        } else if let Some(notice) = e.notice.filter(|_| !forged) {
            (Kind::Message, Some(format!("notice: {}", notice.as_str())), e.text)
        } else {
            (Kind::Message, None, e.text)
        };
        let notice = e.notice.filter(|_| kind == Kind::Message && !forged);
        let channel = e.mention.map(|m| m.channel);
        Some(Entry {
            id: Some(e.id),
            from: Some(e.from),
            to: Some(e.to),
            channel,
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
            origin: e.origin_thread,
            in_reply_to: e.in_reply_to,
            notice,
            ..entry(kind, e.thread_id)
        })
    }

    /// The entry of the message stored at `seq` on `subject`, None for one that is not ants' or does not parse.
    pub fn entry(&mut self, seq: u64, stored_at: DateTime<Utc>, subject: &str, payload: &[u8]) -> Option<Entry> {
        let entry = |kind, thread| Entry::new(seq, stored_at, kind, thread);
        if subject::parse_inbox(subject).is_some() {
            return self.envelope_entry(seq, stored_at, subject, payload);
        }
        if subject::parse_chan(subject).is_some() {
            let post: ChannelPost = serde_json::from_slice(payload).ok()?;
            let forged = post.check_subject(subject).is_err();
            let note = Some(format!("{} turns left", post.turn_budget));
            let acting = self.acting(&post.from, post.on_behalf_of.as_ref());
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
            let acting = self.acting(&post.from, post.on_behalf_of.as_ref());
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
        if let Some(rest) = subject.strip_prefix("ants.approvals.") {
            if rest.contains('.') {
                let d: ApprovalDecision = serde_json::from_slice(payload).ok()?;
                let thread = self.requests.get(&(d.ant.clone(), d.request_id))?.clone();
                let forged = d.subject() != subject;
                let text = format!("{} on {}/{}", tools::spelling(d.verdict), d.ant, d.request_id);
                return Some(Entry { to: Some(d.ant), text, forged, ..entry(Kind::Decision, thread) });
            }
            let r: ApprovalRequest = serde_json::from_slice(payload).ok()?;
            let forged = r.check_subject(subject).is_err();
            if !forged {
                self.requests.insert((r.ant.clone(), r.id), r.thread_id.clone());
            }
            let options = r.options.iter().map(|v| tools::spelling(*v)).collect::<Vec<_>>().join("|");
            let note = Some(format!("{}/{} ({options})", r.ant, r.id));
            let text = format!("{}: {}", r.tool.kind, r.tool.title);
            return Some(Entry { from: Some(r.ant), note, text, forged, ..entry(Kind::Approval, r.thread_id) });
        }
        if let Some(ant) = subject.strip_prefix("ants.dead.") {
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
