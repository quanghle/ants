//! What a human is notified of: the [`Notice`] of a tool request, escalation, completion, question, unserved
//! address, digest or reminder, whether a snooze or quiet hours hold it back, and its JSON form for exec and http sinks.

use std::fmt::Write as _;
use std::sync::OnceLock;

use ants_daemon::delivery::unserved::Unserved;
use ants_daemon::secrets::redact::Redactor;
use ants_proto::{
    ApprovalRequest, Away, Envelope, Escalation, EscalationReason, HeldNotices, NoticeKind, NotifyPrefs, Question,
    Questions, Reminder, ThreadId, UserId, Uuid, Verdict,
};
use chrono::{DateTime, Utc};
use jiff::tz::TimeZone;
use serde_json::{Value, json};

use super::config::{Bypass, Human};
use super::links::{Action, Links};
use super::sinks::cut;
use super::{DIGEST_PENDING, MAX_TEXT, NOTICE_VERSION};
use crate::tools;

/// What a sink sends, whatever its kind: each kind's form of it is [`Sink::request`]'s.
///
/// [`Sink::request`]: super::config::Sink::request
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Notice {
    pub(super) about: About,
    /// The human notified.
    pub(super) human: UserId,
    pub(super) title: String,
    pub(super) text: String,
    pub(super) buttons: Vec<Button>,
    pub(super) urgent: bool,
}

/// What a notice is about.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum About {
    /// A pending tool request, at stream sequence `seq`.
    Approval { request: Box<ApprovalRequest>, seq: u64 },
    /// An escalation reaching the human's inbox, in the message `id` from `from`.
    Escalation { id: Uuid, from: UserId, escalation: Box<Escalation> },
    /// An ant's reply `id` from `from` on `thread`, reaching the human's inbox: the latest of `replies` on it folded
    /// into one notice.
    Completion { id: Uuid, from: UserId, thread: ThreadId, replies: u32 },
    /// A daemon's notice `kind` that a human has to act on ([`NoticeKind::needs_action`]), in the message `id` from
    /// `from` on `thread`.
    Notice { id: Uuid, from: UserId, thread: ThreadId, kind: NoticeKind },
    /// The questions `questions` that `from` asks the human on `thread`, in the message `id`.
    Question { id: Uuid, from: UserId, thread: ThreadId, questions: Box<Questions> },
    /// A test notice of `ants webhook notify-test`.
    Test { id: Uuid },
    /// The messages `messages` the human sent, waiting for `ant`, which no daemon serves.
    Unserved { ant: UserId, messages: Vec<Uuid> },
    /// The digest `id` of the notices `held` back, with the tool requests `pending` when it was sent.
    Digest { id: Uuid, held: HeldNotices, pending: Vec<ApprovalRequest> },
    /// The reminder `id` the human set of `thread`, due at `due`, of the message `message` on it if any.
    Reminder { id: Uuid, thread: ThreadId, message: Option<Uuid>, due: DateTime<Utc> },
}

impl About {
    /// The thread the notice is about, which muting it holds back; none for a test.
    fn thread(&self) -> Option<&ThreadId> {
        match self {
            Self::Approval { request, .. } => Some(&request.thread_id),
            Self::Escalation { escalation, .. } => Some(&escalation.thread_id),
            Self::Completion { thread, .. } | Self::Question { thread, .. } | Self::Notice { thread, .. } => {
                Some(thread)
            }
            Self::Test { .. } | Self::Unserved { .. } | Self::Digest { .. } | Self::Reminder { .. } => None,
        }
    }

    /// Its `kind` in the JSON notice.
    pub(super) fn kind(&self) -> &'static str {
        match self {
            Self::Approval { .. } => "approval",
            Self::Escalation { .. } => "escalation",
            Self::Completion { .. } => "completion",
            Self::Notice { .. } => "notice",
            Self::Question { .. } => "question",
            Self::Test { .. } => "test",
            Self::Unserved { .. } => "unserved",
            Self::Digest { .. } => "digest",
            Self::Reminder { .. } => "reminder",
        }
    }

    /// The `bypass` entry letting it through quiet hours and a snooze, if one may.
    fn bypass(&self) -> Option<Bypass> {
        match self {
            Self::Approval { .. } => Some(Bypass::Approvals),
            Self::Escalation { .. } | Self::Notice { .. } => Some(Bypass::Escalations),
            Self::Question { .. } => Some(Bypass::Questions),
            Self::Completion { .. }
            | Self::Test { .. }
            | Self::Unserved { .. }
            | Self::Digest { .. }
            | Self::Reminder { .. } => None,
        }
    }
}

/// Why `about` is held back from `human` at `now`, given the holds `prefs` the human set, if it is: a muted thread
/// holds back every notice of it; quiet hours and a snooze each one the human's `bypass` does not let through.
pub(super) fn held(human: &Human, about: &About, prefs: &NotifyPrefs, now: DateTime<Utc>) -> Option<&'static str> {
    if about.thread().is_some_and(|thread| prefs.is_muted(thread, now)) {
        return Some("muted thread");
    }
    if about.bypass().is_some_and(|kind| human.bypass.contains(&kind)) {
        return None;
    }
    if prefs.snoozed(now) {
        return Some("snoozed");
    }
    human.quiet.as_ref().is_some_and(|quiet| quiet.contains(now)).then_some("quiet hours")
}

/// Whether a snooze or quiet hours hold back notices of `human` at `now`, whatever its `bypass` lets through: while
/// they do, no digest is due.
pub(super) fn holding(human: &Human, prefs: &NotifyPrefs, now: DateTime<Utc>) -> bool {
    prefs.snoozed(now) || human.quiet.as_ref().is_some_and(|quiet| quiet.contains(now))
}

/// `notice` as exec and http sinks get it, at `now`: version [`NOTICE_VERSION`] of the schema docs/webhooks.md documents.
pub(super) fn notice_json(notice: &Notice, now: DateTime<Utc>) -> Value {
    let links: Vec<Value> = notice
        .buttons
        .iter()
        .map(|b| json!({ "verdict": b.verdict.as_str(), "label": b.label, "url": b.url }))
        .collect();
    let mut json = json!({
        "version": NOTICE_VERSION,
        "human": notice.human,
        "title": notice.title,
        "text": notice.text,
        "urgent": notice.urgent,
        "sent_at": now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "links": links,
    });
    let facts = match &notice.about {
        About::Approval { request, seq } => json!({
            "kind": "approval",
            "id": request.id,
            "ant": request.ant,
            "thread": request.thread_id,
            "request": tools::qualified(request),
            "seq": seq,
            "tool": request.tool,
            "options": request.options.iter().map(|v| v.as_str()).collect::<Vec<_>>(),
            "labels": request
                .options
                .iter()
                .filter_map(|&v| Some((v.as_str(), request.label(v)?)))
                .collect::<std::collections::BTreeMap<_, _>>(),
            "expires_at": request.expires_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        }),
        About::Escalation { id, from, escalation } => {
            let away = away_of(from, escalation);
            let mut facts = json!({
                "kind": "escalation",
                "id": id,
                "ant": away.map_or(from, |away| &away.ant),
                "thread": escalation.thread_id,
                "reason": escalation.reason.as_str(),
                "stuck": escalation.stuck,
                "level": escalation.level,
            });
            if let Some(away) = away {
                facts["stand_in_for"] = json!(away.human);
            }
            facts
        }
        About::Completion { id, from, thread, replies } => json!({
            "kind": "completion",
            "id": id,
            "ant": from,
            "thread": thread,
            "replies": replies,
        }),
        About::Notice { id, from, thread, kind } => json!({
            "kind": "notice",
            "id": id,
            "ant": from,
            "thread": thread,
            "notice": kind.as_str(),
        }),
        About::Question { id, from, thread, questions } => json!({
            "kind": "question",
            "id": id,
            "ant": from,
            "thread": thread,
            "questions": questions.questions.iter().map(question_facts).collect::<Vec<_>>(),
        }),
        About::Test { id } => json!({ "kind": "test", "id": id }),
        About::Unserved { ant, messages } => {
            json!({ "kind": "unserved", "id": messages.first(), "ant": ant, "messages": messages })
        }
        About::Digest { id, held, pending } => digest_facts(*id, held, pending),
        About::Reminder { id, thread, message, due } => json!({
            "kind": "reminder",
            "id": id,
            "thread": thread,
            "message": message,
            "due": due.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        }),
    };
    if let (Value::Object(json), Value::Object(facts)) = (&mut json, facts) {
        json.extend(facts);
    }
    json
}

/// The facts of the digest `id` of the notices `held` back, with the tool requests `pending`, in its JSON notice.
fn digest_facts(id: Uuid, held: &HeldNotices, pending: &[ApprovalRequest]) -> Value {
    let pending: Vec<Value> = pending
        .iter()
        .map(|request| {
            json!({
                "request": tools::qualified(request),
                "ant": request.ant,
                "thread": request.thread_id,
                "title": request.tool.title,
                "expires_at": request.expires_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            })
        })
        .collect();
    json!({
        "kind": "digest",
        "id": id,
        "since": held.since.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "counts": held.counts,
        "pending": pending,
    })
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Button {
    pub(super) label: &'static str,
    pub(super) url: String,
    pub(super) verdict: Verdict,
}

pub(super) fn label(verdict: Verdict) -> &'static str {
    match verdict {
        Verdict::AllowOnce => "Approve",
        Verdict::AllowAlways => "Always allow",
        Verdict::Reject => "Reject",
    }
}

/// Redacts what notices carry, whatever the ant's `redact` setting: ntfy topics, chats and hooks are seen by more than
/// the bus is.
fn redactor() -> &'static Redactor {
    static REDACTOR: OnceLock<Redactor> = OnceLock::new();
    REDACTOR.get_or_init(|| Redactor::default().with_process_env())
}

/// `request` with its tool call redacted.
pub(super) fn redacted(request: &ApprovalRequest) -> ApprovalRequest {
    let mut request = request.clone();
    redactor().redact_tool(&mut request.tool);
    request.labels.values_mut().for_each(|label| redactor().redact_string(label));
    request
}

/// The notice of `request`, stored at stream sequence `seq`, for `human`, with a button per verdict it offers when
/// there are `links`, and the allow rule `suggested` for it ([`tools::suggestion`]).
pub(super) fn approval_notice(
    request: &ApprovalRequest,
    seq: u64,
    human: &UserId,
    links: Option<&Links>,
    suggested: Option<&tools::Suggestion>,
    now: DateTime<Utc>,
) -> Notice {
    let request = &redacted(request);
    let tool = &request.tool;
    let mut text = format!("{} ({})", tool.title, tool.kind);
    if !tool.locations.is_empty() {
        let _ = write!(text, "\nfiles: {}", tool.locations.join(", "));
    }
    if let Some(input) = &tool.input {
        let _ = write!(text, "\ninput: {}", cut(input, MAX_TEXT));
    }
    if let Some(always) = request.label(Verdict::AllowAlways) {
        let _ = write!(text, "\nallow-always: {always}");
    }
    let id = tools::qualified(request);
    let _ = write!(
        text,
        "\nthread {}, request {id}, rejected in {} unless answered\nants tools approve {id} | ants tools reject {id}",
        request.thread_id,
        crate::text::left(request.expires_at, now),
    );
    if let Some(suggested) = suggested {
        let _ = write!(
            text,
            "\nallowed {} times before: `ants tools approve {id} --rule` also adds the allow rule {:?} to {}",
            suggested.approved_before, suggested.rule, request.ant
        );
    }
    let buttons = links.map_or_default(|links| {
        request
            .options
            .iter()
            .map(|&verdict| {
                let action = Action {
                    ant: request.ant.clone(),
                    id: request.id,
                    seq,
                    verdict,
                    by: human.clone(),
                    expires: request.expires_at.timestamp(),
                };
                Button { label: label(verdict), url: links.url(&action), verdict }
            })
            .collect()
    });
    Notice {
        about: About::Approval { request: Box::new(request.clone()), seq },
        human: human.clone(),
        title: format!("{} asks to run a tool", request.ant),
        text,
        buttons,
        urgent: true,
    }
}

/// The notice of an escalation `envelope` brings, if it brings one. The thread to answer on is the escalation's: an
/// expired tool request's notice comes on a thread of its own. One relayed by a human away ([`relayed`]) names that
/// human and the ant to answer, whose answer its text gives.
pub(super) fn escalation_notice(envelope: &Envelope) -> Option<Notice> {
    let escalation = envelope.escalation.as_ref()?;
    if let Some(away) = away_of(&envelope.from, escalation) {
        let what = match escalation.reason {
            EscalationReason::Approval => format!("{} asks to run a tool", away.ant),
            reason => format!("{} escalates: {} on {}", away.ant, reason.as_str(), escalation.stuck),
        };
        return Some(Notice {
            about: About::Escalation { id: envelope.id, from: envelope.from.clone(), escalation: escalation.clone() },
            human: envelope.to.clone(),
            title: format!("for {} (away): {what}", away.human),
            text: notice_text(envelope, 2 * MAX_TEXT),
            buttons: Vec::new(),
            urgent: escalation.reason == EscalationReason::Approval,
        });
    }
    let thread = &escalation.thread_id;
    let text = format!(
        "{}\n\nthread {thread}; answer with ants ask --thread {thread} @{} TEXT",
        notice_text(envelope, MAX_TEXT),
        envelope.from,
    );
    let title = match escalation.reason {
        EscalationReason::Approval => format!("{}'s tool request expired unanswered", escalation.stuck),
        EscalationReason::Storage => "JetStream storage filling up".to_owned(),
        reason => format!("{} escalates: {} on {}", envelope.from, reason.as_str(), escalation.stuck),
    };
    Some(Notice {
        about: About::Escalation { id: envelope.id, from: envelope.from.clone(), escalation: escalation.clone() },
        human: envelope.to.clone(),
        title,
        text,
        buttons: Vec::new(),
        urgent: false,
    })
}

/// What a notice shows of the text of `envelope`, cut at `max`: of a sealed message, only that it arrived.
fn notice_text(envelope: &Envelope, max: usize) -> String {
    match envelope.sealed {
        Some(_) => "a sealed message arrived".to_owned(),
        None => cut(&redactor().redact(&envelope.text), max),
    }
}

/// Whose escalation or tool request `escalation`, sent by `from`, relays to a stand-in: the human away, who sent it.
pub(super) fn away_of<'a>(from: &UserId, escalation: &'a Escalation) -> Option<&'a Away> {
    escalation.away.as_ref().filter(|away| away.human == *from)
}

/// The message relaying to `human`'s stand-in the notice `notice`, held back from `human` by `why`, if it is to be
/// relayed: a pending tool request that waits for `human`, or an escalation an ant sent it, but not the notice of an
/// expired request nor one relayed already, and not what a muted thread holds back. The stand-in is the one the ant
/// named on it, else `human`'s own; it must be a human (`is_human`), as another ant would take the relay for a
/// human's message, and not `human`. `human`'s assistant gets none: what reaches `human` went to it first, or while
/// it could not act. The relay goes on the escalation's or request's thread, from `human` to the stand-in, carrying
/// the escalation (a request's as [`Escalation::of_request`] makes it) marked [`Escalation::away`].
pub(super) fn relayed(
    human: &Human,
    notice: &Notice,
    why: &str,
    is_human: impl Fn(&UserId) -> bool,
) -> Option<Envelope> {
    if why == "muted thread" {
        return None;
    }
    let (ant, named, escalation) = match &notice.about {
        About::Approval { request, .. } if request.human.as_ref() == Some(&human.name) => {
            (&request.ant, request.stand_in.as_ref(), Escalation::of_request(request))
        }
        About::Escalation { from, escalation, .. }
            if escalation.away.is_none() && escalation.reason != EscalationReason::Approval =>
        {
            (from, escalation.stand_in.as_ref(), (**escalation).clone())
        }
        _ => return None,
    };
    let owner = &human.name;
    let valid = |user: &&UserId| *user != owner && is_human(user);
    let stand_in = named.filter(valid).or(human.stand_in.as_ref().filter(valid))?;
    let text = format!(
        "[ants] Relayed to {stand_in}, {owner}'s stand-in, while {owner} is away ({why}); an answer acts for {owner}, \
         who is told of it."
    );
    let text = format!("{text}\n\n{}\n\n{}", notice.title, notice.text);
    let escalation = Escalation { away: Some(Away { human: owner.clone(), ant: ant.clone() }), ..escalation };
    let thread = escalation.thread_id.clone();
    Some(Envelope::on_thread(thread, owner.clone(), stand_in.clone(), text).with_escalation(Some(escalation)))
}

/// The notice of an ant's reply `envelope`, if it is one: a message answering another (`in_reply_to`) that carries no
/// escalation, decision, mention, summary or daemon's notice and comes from no webhook. The CLI that asked for a summary takes the
/// reply itself.
pub(super) fn completion_notice(envelope: &Envelope) -> Option<Notice> {
    let reply = envelope.in_reply_to.is_some()
        && envelope.escalation.is_none()
        && envelope.approval.is_none()
        && envelope.mention.is_none()
        && envelope.summary.is_none()
        && envelope.notice.is_none()
        && !envelope.webhook
        && envelope.from != envelope.to;
    if !reply {
        return None;
    }
    let thread = &envelope.thread_id;
    let text = format!(
        "{}\n\nthread {thread}; answer with ants ask --thread {thread} @{} TEXT",
        notice_text(envelope, MAX_TEXT),
        envelope.from,
    );
    Some(Notice {
        about: About::Completion { id: envelope.id, from: envelope.from.clone(), thread: thread.clone(), replies: 1 },
        human: envelope.to.clone(),
        title: format!("{} replied", envelope.from),
        text,
        buttons: Vec::new(),
        urgent: false,
    })
}

/// The notice of the daemon's notice `envelope`, if a human has to act on it ([`NoticeKind::needs_action`]): an ant
/// signed out or out of quota, say. Other daemon's notices tell of what needs nothing done, and push nothing.
pub(super) fn action_notice(envelope: &Envelope) -> Option<Notice> {
    let kind = envelope.notice.filter(|kind| kind.needs_action())?;
    let thread = &envelope.thread_id;
    let what = match kind {
        NoticeKind::SignedOut => "signed out",
        NoticeKind::Quota => "out of quota",
        NoticeKind::Failing => "failing",
        NoticeKind::NoFallback => "no fallback left",
        NoticeKind::McpSignIn => "MCP sign-in needed",
        _ => "gave up",
    };
    Some(Notice {
        about: About::Notice { id: envelope.id, from: envelope.from.clone(), thread: thread.clone(), kind },
        human: envelope.to.clone(),
        title: format!("{}: {what}", envelope.from),
        text: format!("{}\n\nthread {thread}", notice_text(envelope, MAX_TEXT)),
        buttons: Vec::new(),
        urgent: false,
    })
}

/// A question of a question notice's JSON, redacted.
fn question_facts(question: &Question) -> serde_json::Value {
    let redact = |text: &str| redactor().redact(text).into_owned();
    let options: Vec<_> = question
        .options
        .iter()
        .map(|o| json!({ "label": redact(&o.label), "description": o.description.as_deref().map(redact) }))
        .collect();
    json!({
        "question": redact(&question.text),
        "header": question.header.as_deref().map(redact),
        "options": options,
        "multi_select": question.multi_select,
        "free_text": question.free_text,
    })
}

/// The notice of the questions an ant asks (ask_human) in `envelope`, if it carries any: its text numbers their
/// options, and says how `ants answer` picks them or types answers.
pub(super) fn question_notice(envelope: &Envelope) -> Option<Notice> {
    let questions = envelope.question.as_ref()?;
    let (id, thread) = (envelope.id, &envelope.thread_id);
    let how = match questions.questions.as_slice() {
        [one] => {
            let numbers = if one.multi_select { "options' numbers joined by commas" } else { "an option's number" };
            let note = match (one.multi_select, one.free_text) {
                (false, false) => String::new(),
                (_, true) => format!(" ({numbers}, or a typed answer)"),
                (true, false) => format!(" ({numbers})"),
            };
            format!("ants answer {id} NUMBER{note}")
        }
        several => format!(
            "ants answer {id} and an answer for each of the {} questions, in order: an option's number, numbers \
             joined by commas where several may be picked, or a quoted typed answer where one is taken",
            several.len()
        ),
    };
    let text = format!("{}\n\nthread {thread}; answer with {how}", notice_text(envelope, MAX_TEXT));
    let first = &questions.questions[0].text;
    let asked = cut(&redactor().redact(&first.split_whitespace().collect::<Vec<_>>().join(" ")), 100);
    let asks = match questions.questions.len() {
        1 => "asks".to_owned(),
        n => format!("asks {n} questions"),
    };
    Some(Notice {
        about: About::Question {
            id,
            from: envelope.from.clone(),
            thread: thread.clone(),
            questions: Box::new(questions.clone()),
        },
        human: envelope.to.clone(),
        title: format!("{} {asks}: {asked}", envelope.from),
        text,
        buttons: Vec::new(),
        urgent: false,
    })
}

/// The notice to `human` of its messages `found` waiting for an ant no daemon serves, at `now`.
pub(super) fn unserved_notice(found: &Unserved, human: &UserId, now: DateTime<Utc>) -> Notice {
    let text = found.text(human, now);
    let text = redactor().redact(&text);
    Notice {
        about: About::Unserved { ant: found.ant.clone(), messages: found.messages.iter().map(|m| m.id).collect() },
        human: human.clone(),
        title: format!("messages wait for {}, which no daemon serves", found.ant),
        text: text.strip_prefix("[ants] ").unwrap_or(&text).to_owned(),
        buttons: Vec::new(),
        urgent: false,
    }
}

/// How `n` notices of `kind` read.
pub(super) fn counted(kind: &str, n: u32) -> String {
    let (one, many) = match kind {
        "approval" => ("tool request", "tool requests"),
        "escalation" => ("escalation", "escalations"),
        "completion" => ("reply", "replies"),
        "notice" => ("notice", "notices"),
        "question" => ("question", "questions"),
        _ => return format!("{n} {kind} notices"),
    };
    format!("{n} {}", if n == 1 { one } else { many })
}

/// The digest to `human` of the notices `held` back, with the tool requests `pending` at `now` (redacted, at most
/// [`DIGEST_PENDING`] in its text), its times on the wall clock of the human's quiet hours, else UTC.
pub(super) fn digest_notice(
    human: &Human,
    held: HeldNotices,
    pending: &[ApprovalRequest],
    now: DateTime<Utc>,
) -> Notice {
    let zone = human.quiet.as_ref().map_or(TimeZone::UTC, |quiet| quiet.zone.clone());
    let since = jiff::Timestamp::from_second(held.since.timestamp())
        .map_or_default(|at| at.to_zoned(zone).strftime("%Y-%m-%d %H:%M %Z").to_string());
    let counts: Vec<String> = held.counts.iter().map(|(kind, n)| counted(kind, *n)).collect();
    let mut text = format!(
        "{} held back by quiet hours or a snooze since {since}; nothing was marked read: ants inbox shows the \
         messages.",
        counts.join(", ")
    );
    let pending: Vec<ApprovalRequest> = pending.iter().map(redacted).collect();
    if !pending.is_empty() {
        let _ = write!(
            text,
            "\n\n{} still pending:",
            counted("approval", u32::try_from(pending.len()).unwrap_or(u32::MAX))
        );
        for request in pending.iter().take(DIGEST_PENDING) {
            let _ = write!(
                text,
                "\n{} {} ({}), rejected in {} unless answered",
                tools::qualified(request),
                request.tool.title,
                request.tool.kind,
                crate::text::left(request.expires_at, now)
            );
        }
        if pending.len() > DIGEST_PENDING {
            let _ = write!(text, "\n…and {} more: ants tools list", pending.len() - DIGEST_PENDING);
        }
    }
    let total = held.total();
    Notice {
        title: format!("{total} {} held back", if total == 1 { "notice" } else { "notices" }),
        human: human.name.clone(),
        text,
        buttons: Vec::new(),
        urgent: !pending.is_empty(),
        about: About::Digest { id: Uuid::now_v7(), held, pending },
    }
}

/// The notice to `human` of its `reminder` of `thread`, now due.
pub(super) fn reminder_notice(human: &UserId, thread: &ThreadId, reminder: &Reminder) -> Notice {
    let note = cut(&redactor().redact(&reminder.note), MAX_TEXT);
    let title = if note.is_empty() { format!("reminder of thread {thread}") } else { format!("reminder: {note}") };
    let about = match reminder.message {
        Some(message) => format!("message {message} on thread {thread}"),
        None => format!("thread {thread}"),
    };
    Notice {
        about: About::Reminder {
            id: Uuid::now_v7(),
            thread: thread.clone(),
            message: reminder.message,
            due: reminder.due,
        },
        human: human.clone(),
        title,
        text: format!(
            "Reminder of {about}: `ants thread show {thread}` shows it; ants chat lists it on home until done."
        ),
        buttons: Vec::new(),
        urgent: false,
    }
}
