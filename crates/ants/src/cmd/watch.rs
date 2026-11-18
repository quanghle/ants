//! `ants watch`: follow what ants do, as it happens.

use ants_daemon::records::reactions;
use ants_proto::{
    Acting, ApprovalDecision, ApprovalRequest, CancelReply, CancelRequest, ChannelPost, DeadLetter, DecidedBy,
    Envelope, NotesPost, StatusEvent, StatusUpdate, ThreadId, UserId, subject,
};
use anyhow::{Result, bail};
use futures::StreamExt as _;

use crate::avatars::Faces;
use crate::connection::Nats;
use crate::text::escalation_note;
use crate::threads::thread;
use crate::tools;

/// What `ants watch <subject> [thread]` subscribes to: a subject filter as given, or, for an ant (one token), its
/// status subjects.
pub(crate) fn watch_filter(target: &str, thread: Option<&ThreadId>) -> Result<String> {
    match watched_ant(target, thread.is_some())? {
        Some(ant) => Ok(thread.map_or_else(|| subject::status_of(&ant), |t| subject::status(&ant, t))),
        None => Ok(target.to_owned()),
    }
}

/// The ant `target` names for `ants watch`, None for a subject filter, which follows no `thread`.
pub(crate) fn watched_ant(target: &str, thread: bool) -> Result<Option<UserId>> {
    match target.parse::<UserId>().ok() {
        None if thread => bail!("a thread can only follow an ant, not the subject filter {target:?}"),
        ant => Ok(ant),
    }
}

pub(crate) async fn run(nats: &Nats<'_>, target: String, thread: Option<thread::Pick>) -> Result<()> {
    use std::io::Write as _;

    let target = target.as_str();
    watched_ant(target, thread.is_some())?;
    let (client, js) = nats.connect("ants-watch").await?;
    let filter = watch_filter(target, thread::resolve(thread, nats, &js).await?.as_ref())?;
    let mut sub = client.subscribe(filter).await?;
    let faces = crate::avatars::Faces::stdout(crate::avatars::read_or_none(&js).await);
    let mut printer = Printer { faces: Some(faces), ..Printer::default() };
    let behalf = std::sync::Arc::new(crate::people::behalf::Checker::new(nats, &js));
    let _following = behalf.follow();
    let mut reacted = watched_reactions(&js, target, nats.me.role.is_human(), &behalf).await;
    loop {
        let msg = tokio::select! {
            msg = sub.next() => match msg {
                Some(msg) => msg,
                None => break,
            },
            Some(change) = reacted.next() => {
                let mut stdout = std::io::stdout().lock();
                stdout.write_all(printer.line(&reaction_line(&change)).as_bytes())?;
                stdout.flush()?;
                continue;
            }
        };
        let forged = |ok: bool| if ok { String::new() } else { format!(" (forged: sent on {})", msg.subject) };
        let out = if let Ok(event) = serde_json::from_slice::<StatusEvent>(&msg.payload) {
            printer.status(&event, &forged(event.check_subject(&msg.subject).is_ok()))
        } else if let Ok(mut e) = serde_json::from_slice::<Envelope>(&msg.payload) {
            let genuine = e.check_subject(&msg.subject).is_ok();
            let acting = behalf.check(&e.from, e.on_behalf_of.as_ref()).await;
            e.on_behalf_of = None;
            let line = envelope_line(&e, &acting, &forged(genuine), printer.faces.as_ref());
            printer.line(&line)
        } else if let Ok(post) = serde_json::from_slice::<ChannelPost>(&msg.payload) {
            // Before notes posts: a channel post has every field of one.
            let acting = behalf.check(&post.from, post.on_behalf_of.as_ref()).await;
            printer.line(&channel_line(&post, &acting, &forged(post.check_subject(&msg.subject).is_ok())))
        } else if let Ok(post) = serde_json::from_slice::<NotesPost>(&msg.payload) {
            let forged = forged(post.check_subject(&msg.subject).is_ok());
            let acting = behalf.check(&post.from, post.on_behalf_of.as_ref()).await;
            let (thread, from, left) = (&post.thread_id, acting.label(post.from.as_str()), post.turn_budget);
            printer.line(&format!("[{thread}] {from} -> thread notes ({left} turns left){forged}: {}", post.text))
        } else if let Ok(d) = serde_json::from_slice::<DeadLetter>(&msg.payload) {
            printer.line(&format!("dead letter of {} after {} deliveries: {}", d.ant, d.deliveries, d.error))
        } else if let Ok(r) = serde_json::from_slice::<ApprovalRequest>(&msg.payload) {
            printer.line(&request_line(&r, &forged(r.check_subject(&msg.subject).is_ok())))
        } else if let Ok(d) = serde_json::from_slice::<ApprovalDecision>(&msg.payload) {
            let forged = forged(d.subject() == msg.subject.as_str());
            printer.line(&format!("{}/{}{forged}: answered {}", d.ant, d.request_id, tools::spelling(d.verdict)))
        } else if let Ok(r) = serde_json::from_slice::<CancelRequest>(&msg.payload) {
            let forged = forged(r.check_subject(&msg.subject).is_ok());
            let by = behalf.label(&r.by, r.on_behalf_of.as_ref()).await;
            let asks = match r.withdraw {
                Some(message) => format!("to withdraw message {message} from the queue of"),
                None => "to cancel the prompt of".to_owned(),
            };
            printer.line(&format!("[{}] {by} asks {asks} {}{forged}", r.thread_id, r.ant))
        } else if let Ok(r) = serde_json::from_slice::<CancelReply>(&msg.payload) {
            let forged = forged(r.check_subject(&msg.subject).is_ok());
            let outcome = if r.cancelled { "prompt cancelled" } else { "no prompt to cancel" };
            printer.line(&format!("[{}] {}{forged}: {outcome}", r.thread_id, r.ant))
        } else if let Ok(r) = serde_json::from_slice::<ants_proto::QueueRequest>(&msg.payload) {
            // After cancel requests: a cancel request has every field of one.
            let forged = forged(r.check_subject(&msg.subject).is_ok());
            let by = behalf.label(&r.by, r.on_behalf_of.as_ref()).await;
            printer.line(&format!("{by} asks for the queue of {}{forged}", r.ant))
        } else if let Ok(r) = serde_json::from_slice::<ants_proto::QueueReply>(&msg.payload) {
            let forged = forged(r.check_subject(&msg.subject).is_ok());
            printer.line(&format!("{}{forged}: {} messages in hand", r.ant, r.messages.len()))
        } else if let Ok(r) = serde_json::from_slice::<ants_proto::ReactionsRequest>(&msg.payload) {
            let forged = forged(r.check_subject(&msg.subject).is_ok());
            printer.line(&format!("{}{forged} asks for the reactions to message {}", r.ant, r.message))
        } else {
            printer.line(&format!("{} {}", msg.subject, String::from_utf8_lossy(&msg.payload)))
        };
        let mut stdout = std::io::stdout().lock();
        stdout.write_all(out.as_bytes())?;
        stdout.flush()?;
    }
    Ok(())
}

/// The reactions `ants watch` follows besides `target`: all of them for a human taking every message (`ants.>` or
/// `>`), else none, as when the bucket cannot be read. `behalf` checks an assistant's for its human.
pub(crate) async fn watched_reactions(
    js: &async_nats::jetstream::Context,
    target: &str,
    human: bool,
    behalf: &std::sync::Arc<crate::people::behalf::Checker>,
) -> futures::stream::BoxStream<'static, reactions::Change> {
    let none = || futures::stream::pending().boxed();
    if !human || ![subject::ALL, ">"].contains(&target) {
        return none();
    }
    match reactions::watch(js).await {
        Ok(changes) => crate::messages::reacted::checked(changes, behalf).chain(futures::stream::pending()).boxed(),
        Err(e) => {
            tracing::warn!("not showing reactions: {e:#}");
            none()
        }
    }
}

/// A reaction added or removed as `ants watch` prints it, an assistant's for its human naming both ([`Acting::label`]).
pub(crate) fn reaction_line(change: &reactions::Change) -> String {
    let verb = if change.removed { "removed the reaction" } else { "reacted" };
    let by = change.acting.label(change.by.as_str());
    format!("{by} {verb} {} to message {}", change.reaction, change.message)
}

/// An envelope as `ants watch` prints it: an escalation it carries with its level, reason, stuck ant and thread,
/// leader and the ants visited; an ant's approval decision in place of its text; the size of a sealed message's
/// ciphertext in place of its text. An assistant's message for its human names both ([`Acting::label`]).
pub(crate) fn envelope_line(e: &Envelope, acting: &Acting, forged: &str, faces: Option<&Faces>) -> String {
    let from = faces.and_then(|f| f.known(&e.from)).unwrap_or_else(|| e.from.to_string());
    let head = format!("[{}] {} -> {}{forged}", e.thread_id, acting.label(&from), e.to);
    if let Some(d) = &e.approval {
        return format!("{head} decides {} on approval request {}/{}", tools::spelling(d.verdict), d.ant, d.request_id);
    }
    if let Some(sealed) = &e.sealed {
        return format!("{head}: {}", crate::people::sealed::size_note(sealed));
    }
    match &e.escalation {
        Some(x) => format!("{head} [{}]: {}", escalation_note(x), e.text),
        None => format!("{head}: {}", e.text),
    }
}

/// A tool request as `ants watch` prints it.
pub(crate) fn request_line(r: &ApprovalRequest, forged: &str) -> String {
    let options = r.options.iter().map(|v| tools::spelling(*v)).collect::<Vec<_>>().join("|");
    let (thread, ant, id) = (&r.thread_id, &r.ant, r.id);
    format!("[{thread}] {ant}{forged} asks approval {ant}/{id} ({options}): {}: {}", r.tool.kind, r.tool.title)
}

/// A channel post as `ants watch` prints it, an assistant's for its human naming both ([`Acting::label`]).
pub(crate) fn channel_line(post: &ChannelPost, acting: &Acting, forged: &str) -> String {
    let (thread, from, channel, left) =
        (&post.thread_id, acting.label(post.from.as_str()), &post.channel, post.turn_budget);
    let created = post.creator.as_ref().map_or_default(|c| format!(", created by {c}"));
    format!("[{thread}] {from} -> #{channel} ({left} turns left{created}){forged}: {}", post.text)
}

/// Renders watched traffic, continuing an ant's streamed text on one line until something else arrives.
#[derive(Default)]
pub(crate) struct Printer {
    /// Whose text the output ends with, unterminated.
    streaming: Option<(UserId, ThreadId, &'static str)>,
    /// The ants' faces, put before their names.
    faces: Option<Faces>,
}

impl Printer {
    /// A whole line, after ending any streamed text.
    fn line(&mut self, line: &str) -> String {
        let end = if self.streaming.take().is_some() { "\n" } else { "" };
        format!("{end}{line}\n")
    }

    fn status(&mut self, event: &StatusEvent, forged: &str) -> String {
        let (who, thread) = (&event.ant, &event.thread_id);
        let named = self.faces.as_ref().map_or_else(|| who.to_string(), |f| f.named(who));
        let (verb, text) = match &event.update {
            StatusUpdate::Message { text } => ("says", text),
            StatusUpdate::Thought { text } => ("thinks", text),
            StatusUpdate::State { state } => {
                return self.line(&format!("[{thread}] {named}{forged}: {}", state.as_str()));
            }
            StatusUpdate::Tool { id, title, tool_kind, status, .. } => {
                let kind = tool_kind.as_deref().map_or_default(|k| format!(" ({k})"));
                let status = status.as_deref().map_or_default(|s| format!(": {s}"));
                let title = title.as_deref().unwrap_or(id);
                return self.line(&format!("[{thread}] {named}{forged} tool {title}{kind}{status}"));
            }
            StatusUpdate::Plan { steps } => {
                let steps: Vec<String> = steps
                    .iter()
                    .map(|s| {
                        let mark = match s.status.as_str() {
                            "completed" => "x",
                            "in_progress" => ">",
                            _ => " ",
                        };
                        format!("[{mark}] {}", s.content)
                    })
                    .collect();
                return self.line(&format!("[{thread}] {named}{forged} plan: {}", steps.join("; ")));
            }
            StatusUpdate::Queued { message, ahead: 0 } => {
                return self.line(&format!("[{thread}] {named}{forged}: message {message} starts"));
            }
            StatusUpdate::Queued { message, ahead } => {
                return self.line(&format!("[{thread}] {named}{forged}: message {message} queued behind {ahead}"));
            }
            StatusUpdate::Approval { request_id } => {
                return self.line(&format!("[{thread}] {named}{forged} waits for approval {who}/{request_id}"));
            }
            StatusUpdate::Escalated { to, escalation } => {
                let (stuck, level, reason) = (&escalation.stuck, escalation.level, escalation.reason.as_str());
                return self
                    .line(&format!("[{thread}] {named}{forged} escalated {stuck}'s {reason} to {to} (level {level})"));
            }
            StatusUpdate::Asked { to, message, question } => {
                let first = question
                    .questions
                    .first()
                    .map_or_default(|q| q.text.split_whitespace().collect::<Vec<_>>().join(" "));
                let more = match question.questions.len() {
                    0 | 1 => String::new(),
                    n => format!(" and {} more", n - 1),
                };
                return self
                    .line(&format!("[{thread}] {named}{forged} asked {to} {first:?}{more} (message {message})"));
            }
            StatusUpdate::Decided { request_id, verdict, by, decider, via } => {
                let verdict = tools::spelling(*verdict);
                let outcome = match (by, decider, via) {
                    (DecidedBy::Expiry, ..) => format!("expired: {verdict}"),
                    (DecidedBy::Human, Some(decider), Some(via)) => format!("decided {verdict} by {decider} via {via}"),
                    (_, Some(decider), _) => format!("decided {verdict} by {decider}"),
                    (DecidedBy::Human, None, _) => format!("decided {verdict} by a human"),
                    (DecidedBy::Chain, None, _) => format!("decided {verdict} by its chain"),
                };
                return self.line(&format!("[{thread}] {named}{forged}: approval {who}/{request_id} {outcome}"));
            }
            StatusUpdate::Step { run, call, label, to, step_thread, stage } => {
                let label = label.as_deref().map_or_default(|label| format!(" ({label})"));
                let stage = stage.as_str();
                return self.line(&format!(
                    "[{thread}] {named}{forged}: trail run {run} step {call}{label} to {to} {stage} on {step_thread}"
                ));
            }
            StatusUpdate::Leg { run, leg, trail, waypoint } => {
                let entry = ants_proto::leg_entry(trail.as_ref(), waypoint.as_deref());
                return self.line(&format!("[{thread}] {named}{forged}: trail run {run} leg {leg} {entry} started"));
            }
        };
        let key = (who.clone(), thread.clone(), verb);
        if !forged.is_empty() {
            return self.line(&format!("[{thread}] {named} {verb}{forged}: {text}"));
        }
        if self.streaming.as_ref() == Some(&key) {
            return text.clone();
        }
        let start = self.line(&format!("[{thread}] {named} {verb}: "));
        self.streaming = Some(key);
        format!("{}{text}", start.trim_end_matches('\n'))
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::Escalation;

    use super::*;

    #[test]
    fn watch_follows_an_ant_or_a_subject_filter() {
        let t1: ThreadId = "t1".parse().unwrap();
        assert_eq!(watch_filter("dev-a", None).unwrap(), "ants.status.dev-a.*");
        assert_eq!(watch_filter("dev-a", Some(&t1)).unwrap(), "ants.status.dev-a.t1");
        assert_eq!(watch_filter("ants.notes.>", None).unwrap(), "ants.notes.>");
        assert_eq!(watch_filter(">", None).unwrap(), ">");
        assert!(watch_filter("ants.>", Some(&t1)).is_err());
        assert!(watched_ant("ants.>", true).is_err());
    }

    #[test]
    fn streamed_text_continues_on_one_line() {
        let (a, t1): (UserId, ThreadId) = ("dev-a".parse().unwrap(), "t1".parse().unwrap());
        let event = |update| StatusEvent::new(a.clone(), t1.clone(), update);
        let says = |text: &str| event(StatusUpdate::Message { text: text.to_owned() });
        let mut printer = Printer::default();
        let out: String = [
            printer.status(&says("Hel"), ""),
            printer.status(&says("lo"), ""),
            printer.status(&event(StatusUpdate::Thought { text: "hmm".to_owned() }), ""),
            printer.status(
                &event(StatusUpdate::Tool {
                    id: "c1".to_owned(),
                    title: Some("cargo test".to_owned()),
                    tool_kind: Some("execute".to_owned()),
                    status: Some("in_progress".to_owned()),
                    paths: None,
                }),
                "",
            ),
            printer.status(&says("done"), ""),
            printer.status(&event(StatusUpdate::State { state: ants_proto::State::Idle }), ""),
        ]
        .concat();
        assert_eq!(
            out,
            "[t1] dev-a says: Hello\n[t1] dev-a thinks: hmm\n[t1] dev-a tool cargo test (execute): in_progress\n\
             [t1] dev-a says: done\n[t1] dev-a: idle\n"
        );
    }

    #[test]
    fn watched_channel_posts_name_the_channel_and_turns_left() {
        let mut post = ChannelPost::new("ops".parse().unwrap(), "dev-a".parse().unwrap(), "@dev-b ready?");
        (post.thread_id, post.turn_budget) = ("t1".parse().unwrap(), 3);
        let own = &Acting::Own;
        assert_eq!(channel_line(&post, own, ""), "[t1] dev-a -> #ops (3 turns left): @dev-b ready?");
        let forged = " (forged: sent on ants.chan.ops.dev-b)";
        let line = channel_line(&post, own, forged);
        assert_eq!(line, format!("[t1] dev-a -> #ops (3 turns left){forged}: @dev-b ready?"));
        let created = post.with_creator(Some("lead".parse().unwrap()));
        let line = channel_line(&created, own, "");
        assert_eq!(line, "[t1] dev-a -> #ops (3 turns left, created by lead): @dev-b ready?");
        let alice: UserId = "alice".parse().unwrap();
        let line = channel_line(&created, &Acting::For(alice.clone()), "");
        assert!(line.starts_with("[t1] alice via dev-a -> #ops"), "{line}");
        let line = channel_line(&created, &Acting::Forged(alice), "");
        assert!(line.starts_with("[t1] dev-a (forged: for alice) -> #ops"), "{line}");
    }

    #[test]
    fn watched_escalations_show_level_reason_leader_and_visited() {
        let p = |s: &str| -> UserId { s.parse().unwrap() };
        let t1: ThreadId = "t1".parse().unwrap();
        let start = Escalation::new(p("dev-a"), t1.clone(), ants_proto::EscalationReason::TurnBudget);
        let ants_proto::Hop::Chain { escalation, .. } = start.up(&[p("lead"), p("dev-b")], 3) else { panic!("up") };
        let notice =
            Envelope::on_thread(t1.clone(), p("dev-a"), p("lead"), "[ants] stuck").with_escalation(Some(*escalation));
        assert_eq!(
            envelope_line(&notice, &Acting::Own, "", None),
            "[t1] dev-a -> lead [escalation level 1, turn_budget: dev-a stuck on t1, leader lead, \
             visited dev-a > lead > dev-b]: [ants] stuck"
        );

        let tool = ants_proto::ToolCall { kind: "execute".into(), title: "git push".into(), ..Default::default() };
        let options = vec![ants_proto::Verdict::AllowOnce];
        let request = ApprovalRequest::new(p("dev-a"), t1.clone(), tool, options, std::time::Duration::from_secs(60));
        let id = request.id;
        let ants_proto::Hop::Chain { escalation, .. } = Escalation::of_request(&request).up(&[p("lead")], 3) else {
            panic!("up")
        };
        assert_eq!(
            escalation_note(&escalation),
            format!("escalation level 1, approval dev-a/{id}: dev-a stuck on t1, leader lead, visited dev-a > lead")
        );
        assert_eq!(
            request_line(&request, ""),
            format!("[t1] dev-a asks approval dev-a/{id} (allow-once|reject): execute: git push")
        );
        let decision = ApprovalDecision::new(id, p("dev-a"), ants_proto::Verdict::AllowOnce);
        let decided = Envelope::decision(t1, p("lead"), decision);
        assert_eq!(
            envelope_line(&decided, &Acting::Own, "", None),
            format!("[t1] lead -> dev-a decides allow-once on approval request dev-a/{id}")
        );

        let mut printer = Printer::default();
        let mut told = |by, decider, via| {
            let verdict = ants_proto::Verdict::AllowOnce;
            let update = StatusUpdate::Decided { request_id: id, verdict, by, decider, via };
            printer.status(&StatusEvent::new(p("dev-a"), "t1".parse().unwrap(), update), "")
        };
        assert_eq!(
            told(DecidedBy::Chain, Some(p("lead")), None),
            format!("[t1] dev-a: approval dev-a/{id} decided allow-once by lead\n")
        );
        assert_eq!(
            told(DecidedBy::Human, None, None),
            format!("[t1] dev-a: approval dev-a/{id} decided allow-once by a human\n")
        );
        assert_eq!(
            told(DecidedBy::Human, Some(p("alice")), Some(p("pa"))),
            format!("[t1] dev-a: approval dev-a/{id} decided allow-once by alice via pa\n")
        );
    }
}
