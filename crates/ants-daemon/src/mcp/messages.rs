//! The tools messaging other ants and humans: `send_message`, `ask_ant`, which waits for the reply (see
//! [`super::asks`]), `check_inbox` for the replies that came too late, and `ask_human`; and `handoff`, which hands work
//! to the ant itself on another thread. A message follows on from the one its thread is handling ([`Hub::outgoing`]).

use std::time::Duration;

use ants_proto::{Envelope, Question, QuestionOption, Questions, Role, ThreadId, UserId};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, schemars, tool, tool_router};
use serde::Deserialize;

use super::tools::{Tools, text};
use super::{Hub, attach};
use crate::escalation;
use crate::sync::MutexExt as _;

const DEFAULT_ASK_WAIT: Duration = Duration::from_secs(300);
const MAX_ASK_WAIT: Duration = Duration::from_mins(30);

#[derive(Deserialize, schemars::JsonSchema)]
struct SendMessage {
    /// Recipient ant, such as `dev-b`, or `human`.
    to: String,
    /// The message.
    text: String,
    /// Thread to send on: omitted for the current one, `new` for a fresh one, or a thread id.
    #[serde(default)]
    thread: Option<String>,
    /// Files to attach: paths in the working directory, at most 8 files of up to 8 MiB each; secrets in text files are
    /// redacted.
    #[serde(default)]
    attach: Vec<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct Handoff {
    /// The work to hand off: the message that starts it, saying all it needs, as the new thread does not see this one.
    text: String,
    /// Thread to hand off to: omitted or `new` for a fresh one, or the id of another thread.
    #[serde(default)]
    thread: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct AskAnt {
    /// Ant to ask, such as `dev-b`.
    to: String,
    /// The question.
    text: String,
    /// How long to wait for the reply, such as `90s` or `10m` (default 5m, at most 30m).
    #[serde(default)]
    timeout: Option<String>,
    /// Files to attach: paths in the working directory, at most 8 files of up to 8 MiB each; secrets in text files are
    /// redacted.
    #[serde(default)]
    attach: Vec<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct CheckInbox {
    /// Take the messages of every thread of the ant, not only this one's.
    #[serde(default)]
    all_threads: bool,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct AskHuman {
    /// 1-4 questions, answered together in one message.
    questions: Vec<AskedQuestion>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct AskedQuestion {
    /// The question: 1-500 characters.
    question: String,
    /// A short label naming the question on its tab, such as `Database`: one line of at most 12 characters.
    #[serde(default)]
    header: Option<String>,
    /// 2-9 options to pick from by number: each a label, one line of 1-200 characters, or an object with the label
    /// and a description saying what picking it means, one line of at most 300 characters.
    options: Vec<AskedOption>,
    /// Whether the human may pick several options (default false).
    #[serde(default)]
    multi_select: bool,
    /// Whether the human may type an answer instead of picking options (default true).
    #[serde(default)]
    free_text: Option<bool>,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
enum AskedOption {
    Label(String),
    Described {
        label: String,
        #[serde(default)]
        description: Option<String>,
    },
}

impl AskHuman {
    /// The questions asked, checked as [`Questions::new`] does.
    fn questions(self) -> Result<Questions, String> {
        let questions = self
            .questions
            .into_iter()
            .map(|q| Question {
                text: q.question,
                header: q.header,
                options: q
                    .options
                    .into_iter()
                    .map(|o| match o {
                        AskedOption::Label(label) => QuestionOption::new(&label),
                        AskedOption::Described { label, description } => {
                            QuestionOption::described(&label, description.as_deref().unwrap_or_default())
                        }
                    })
                    .collect(),
                multi_select: q.multi_select,
                free_text: q.free_text.unwrap_or(true),
            })
            .collect();
        Questions::new(questions).map_err(|e| e.to_string())
    }
}

/// The questions of `asked`, ask_human's arguments (`{"questions": [...]}`), checked as ask_human checks them.
pub(crate) fn asked_questions(asked: serde_json::Value) -> Result<Questions, String> {
    serde_json::from_value::<AskHuman>(asked).map_err(|e| e.to_string())?.questions()
}

impl Hub {
    /// A message from the ant, following on from the one `thread` is handling and keeping its owner: on the same
    /// thread unless `on` names another (`new` for a fresh one), and with one turn less unless it answers a human or
    /// goes to one.
    fn outgoing(&self, thread: &ThreadId, to: &str, text: String, on: Option<&str>) -> Result<Envelope, String> {
        let to: UserId = to.trim().trim_start_matches('@').parse().map_err(|e| format!("{e}"))?;
        if to == self.ant {
            return Err(format!(
                "{to} is this ant; message another one, or call handoff to start work on another thread"
            ));
        }
        self.follow_on(thread, to, text, on)
    }

    /// A message from the ant to `to` as [`Hub::outgoing`] makes it, `to` being any user, the ant itself included.
    fn follow_on(&self, thread: &ThreadId, to: UserId, text: String, on: Option<&str>) -> Result<Envelope, String> {
        let current = self.sessions.locked().current.get(thread).cloned();
        let current = current.ok_or("no message is being handled on this thread; messages go out only while one is")?;
        let role = if self.humans.is_human(&to) { Role::Human } else { self.humans.sender_role(&current) };
        let mut envelope = current.reply(self.ant.clone(), text, role).map_err(|_| no_turns(thread, &current.from))?;
        envelope.to = to;
        envelope.in_reply_to = None;
        match on.map(str::trim) {
            None | Some("") => {}
            Some("new") => envelope = envelope.spun_off(ThreadId::generate()),
            Some(other) => envelope = envelope.spun_off(other.parse().map_err(|e| format!("{e}"))?),
        }
        // On the escalation's thread, a message carries it, so its leader's answer is known as one.
        let escalation = current.escalation.as_deref().filter(|e| e.thread_id == envelope.thread_id).cloned();
        if let Some(e) = escalation::group(&self.ant, &current)
            && e.thread_id == envelope.thread_id
            && envelope.to == e.stuck
            && !e.is_leader(&self.ant)
        {
            let (leader, stuck) = (&e.leader, &e.stuck);
            return Err(format!(
                "only {leader} answers {stuck} on thread {}; post_notes reaches {leader} and the group",
                e.thread_id
            ));
        }
        Ok(envelope.with_escalation(escalation))
    }

    /// Ask the owner of the thread `thread` is handling a message on, else the default human, `args`'s questions on
    /// the thread, without waiting: the answer to all of them comes later as one message. Spends no turn.
    async fn ask_human(&self, thread: &ThreadId, args: AskHuman) -> Result<String, String> {
        let question = args.questions()?;
        let human = {
            let sessions = self.sessions.locked();
            let current = sessions.current.get(thread);
            current
                .map(|current| self.humans.owner_of(current))
                .ok_or("no message is being handled on this thread; questions go out only while one is".to_owned())?
        };
        let rendered = question.render();
        let mut envelope = self.outgoing(thread, human.as_str(), rendered, None)?;
        envelope.question = Some(question);
        envelope.escalation = None;
        if let Some(escalator) = &self.escalator {
            let first = escalator.ask_first(&envelope).await.map_err(|e| format!("not asked: {e:#}"))?;
            if let Some(assistant) = first {
                return Ok(format!(
                    "Asked {assistant}, the assistant of {human}, first, on thread {}. The answer comes later as a \
                     message from {assistant}, or {assistant} passes the question on to {human}, as happens without \
                     its answer after escalate_after; {human}'s answer then names each question and the options \
                     picked, or the answer typed.",
                    envelope.thread_id
                ));
            }
        }
        self.send(&envelope).await?;
        Ok(format!(
            "Asked {human} on thread {} as message {}. The answer comes later as one message from {human} naming \
             each question and the options picked, or the answer typed.",
            envelope.thread_id, envelope.id
        ))
    }
}

impl Tools {
    /// Hand `text` to the ant itself on thread `on`, a fresh one unless it names another than `thread`, as `handoff`
    /// does.
    async fn hand_off(&self, thread: &ThreadId, text: String, on: Option<&str>) -> Result<String, String> {
        let on = on.map(str::trim).filter(|on| !on.is_empty()).unwrap_or("new");
        if on != "new" && on.parse::<ThreadId>().is_ok_and(|on| on == *thread) {
            return Err(format!("thread {thread} is the current one; hand off to `new` or another thread"));
        }
        let envelope = self.hub.follow_on(thread, self.hub.ant.clone(), text, Some(on))?;
        self.hub.send(&envelope).await?;
        Ok(format!(
            "Handed off on thread {} as message {} ({} turns left). The work runs there as its own prompt, its answer \
             going to the thread's owner; this prompt goes on.",
            envelope.thread_id, envelope.id, envelope.turn_budget
        ))
    }

    /// Send `text` to `to` as `send_message` does, noting when no daemon serves `to` now: the message waits for one.
    pub(super) async fn send_to(
        &self,
        thread: &ThreadId,
        to: &str,
        text: String,
        on: Option<&str>,
        attach: &[String],
    ) -> Result<String, String> {
        let mut envelope = self.hub.outgoing(thread, to, text, on)?;
        let loaded = self.hub.load(thread, attach).await?;
        let unserved = self.hub.unserved(&envelope.to, Duration::ZERO).await;
        let attachments = self.hub.store(loaded, |id| envelope.attachment_handle(id)).await?;
        envelope.attachments = attachments;
        self.hub.send(&envelope).await?;
        let sent = format!(
            "Sent to {} on thread {} as message {} ({} turns left).",
            envelope.to, envelope.thread_id, envelope.id, envelope.turn_budget
        );
        Ok(match unserved {
            Some(why) => format!("{sent} Warning: the message waits in the inbox of {}, as {why}", envelope.to),
            None => sent,
        })
    }

    /// Ask `to` as `ask_ant` does, and return the reply. Refused at once, with nothing sent, when no daemon serves
    /// `to` now, rather than waiting out the reply that cannot come.
    pub(super) async fn ask(
        &self,
        thread: &ThreadId,
        to: &str,
        text: String,
        attach: &[String],
        wait: Duration,
        cx: &RequestContext<RoleServer>,
    ) -> Result<Envelope, String> {
        let mut envelope = self.hub.outgoing(thread, to, text, None)?;
        if self.hub.humans.is_human(&envelope.to) {
            return Err(format!("ask_ant is for ants; send_message reaches {}", envelope.to));
        }
        if envelope.turn_budget == 0 {
            return Err(format!("thread {thread} has no turns left for a reply; ask the human instead"));
        }
        if let Some(why) = self.hub.unserved(&envelope.to, wait).await {
            return Err(format!(
                "Not sent, as {why} send_message leaves a message waiting in the inbox of {} instead.",
                envelope.to
            ));
        }
        let loaded = self.hub.load(thread, attach).await?;
        let attachments = self.hub.store(loaded, |id| envelope.attachment_handle(id)).await?;
        envelope.attachments = attachments;
        self.hub.ask(envelope, wait, cx.ct.cancelled()).await
    }
}

#[tool_router(router = messages_router, vis = "pub(super)")]
impl Tools {
    #[tool(description = "Send a message to another ant, or to `human`, and return at once. It goes on the \
        current thread unless `thread` says otherwise. A message between ants spends one turn of the thread's \
        budget; with none left it is refused and the thread is its owner's to decide. A reply comes later as a new \
        message. When no daemon serves the ant now (no colony entry, a stale one, or a harness signed out) the \
        message still goes and waits, and the result warns so, naming ants from the colony to message instead. \
        `attach` lists files in the working directory to attach (at most 8 of up to 8 MiB each), secrets in text \
        files replaced with [redacted]; one that cannot be attached fails the call with nothing sent.")]
    async fn send_message(
        &self,
        Parameters(args): Parameters<SendMessage>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.send_to(&thread, &args.to, args.text, args.thread.as_deref(), &args.attach).await))
    }

    #[tool(description = "Hand work off to this ant itself on another thread, to run there as its own prompt \
        while this one goes on, as for a separate task that came up. `thread` is `new` (the default) for a fresh \
        thread, or the id of another thread of this ant; never the current one. The message keeps the thread's owner, \
        spends one turn of the current thread's budget unless it answers a human, and carries the turns left; the \
        answer goes to the thread's owner. The new thread does not see this one: the text says all the work needs.")]
    async fn handoff(
        &self,
        Parameters(args): Parameters<Handoff>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hand_off(&thread, args.text, args.thread.as_deref()).await))
    }

    #[tool(description = "Ask the thread's owner, else the default human, 1-4 questions in one message, each \
        with 2-9 options to pick from by number, and return the message id at once without waiting. Ask related \
        decisions together rather than one call each: the human answers them all, then sends one answer. Each \
        question may have a short `header` naming its tab, options with a `description`, `multi_select` (default \
        false) to let the human pick several options, and `free_text` (default true) to let the human type an answer \
        instead. One question: {\"questions\": [{\"question\": \"Ship it?\", \"options\": [\"yes\", \"no\"]}]}. The \
        answer arrives later as one new message on this thread, with a line for each question naming the options \
        picked or the answer typed. A human with an assistant that can act gets the questions through it: the \
        assistant answers, or passes them on to the human. Spends no turn.")]
    async fn ask_human(
        &self,
        Parameters(args): Parameters<AskHuman>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hub.ask_human(&thread, args).await))
    }

    #[tool(description = "Ask another ant and wait for its reply, which is returned. The question goes on the \
        current thread and spends one turn of its budget. A reply that arrives after the wait gives up is kept for \
        check_inbox, and once the turn ends is prompted on this thread. When no daemon serves the ant now (no colony entry, a stale one, or a harness signed out) the \
        call is refused at once with nothing sent, saying why and naming ants from the colony to ask instead. \
        `attach` lists files in the working directory to attach, as for send_message.")]
    async fn ask_ant(
        &self,
        Parameters(args): Parameters<AskAnt>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        let result = match ask_wait(args.timeout.as_deref()) {
            Ok(wait) => self.ask(&thread, &args.to, args.text, &args.attach, wait, &cx).await,
            Err(e) => Err(e),
        };
        Ok(text(result.map(|reply| format!("{} replied (message {}):\n{}", reply.from, reply.id, shown(&reply)))))
    }

    #[tool(description = "Take the messages kept for this ant outside of any prompt: replies to ask_ant calls \
        that arrived after the call gave up. Each is returned once. Only this thread's unless `all_threads` is set.")]
    async fn check_inbox(
        &self,
        Parameters(args): Parameters<CheckInbox>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        let late = match self.hub.take_late((!args.all_threads).then_some(&thread)).await {
            Ok(late) => late,
            Err(e) => return Ok(text(Err(format!("{e:#}")))),
        };
        if late.is_empty() {
            return Ok(text(Ok("No messages.".to_owned())));
        }
        let shown: Vec<String> = late
            .iter()
            .map(|e| {
                let at = e.sent_at.to_rfc3339();
                format!("From {} on thread {} at {at} (message {}):\n{}", e.from, e.thread_id, e.id, shown(e))
            })
            .collect();
        Ok(text(Ok(shown.join("\n\n"))))
    }
}

/// Why a message or post cannot go out on `thread`, which has no turns left, while answering `from`.
pub(super) fn no_turns(thread: &ThreadId, from: &UserId) -> String {
    format!(
        "thread {thread} has no turns left; answer {from} in the reply instead, or call escalate if the prompt says \
         the answer is not sent back"
    )
}

/// The wait an `ask_ant` or `route` call asked for, from its `timeout` argument.
pub(super) fn ask_wait(timeout: Option<&str>) -> Result<Duration, String> {
    let Some(timeout) = timeout else { return Ok(DEFAULT_ASK_WAIT) };
    let wait = ants_proto::duration::parse(timeout.trim()).map_err(|e| format!("timeout: {e}"))?;
    if wait.is_zero() || wait > MAX_ASK_WAIT {
        return Err(format!(
            "timeout {timeout:?} is out of range: more than 0 and at most {}",
            ants_proto::duration::format(MAX_ASK_WAIT)
        ));
    }
    Ok(wait)
}

/// A message's text, with its attachments listed for `read_attachment`.
pub(super) fn shown(envelope: &Envelope) -> String {
    format!("{}{}", envelope.text, attach::listing(&envelope.attachments, |id| envelope.attachment_handle(id)))
}
