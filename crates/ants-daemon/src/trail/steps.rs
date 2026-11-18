//! A call's step: the message carrying it to its recipient, and the answer a reply to it gives the call.

use ants_proto::{Envelope, PendingCall, TrailFunction, TrailRun, TrailStep, WEBHOOK_ALLOW};
use ants_trail::{Answer, Function};
use serde::Deserialize;
use serde_json::Value;

use super::schema;

/// How many times a reply not matching its schema is asked for again before the call raises in the script.
pub(super) const MAX_RETRIES: u32 = 2;

/// The arguments of an agent() call, as the engine hands them out.
#[derive(Deserialize)]
pub(super) struct AgentArgs {
    pub(super) prompt: String,
    #[serde(default)]
    pub(super) ant: Option<String>,
    #[serde(default)]
    pub(super) capability: Option<String>,
    #[serde(default)]
    pub(super) schema: Option<Value>,
    #[serde(default)]
    pub(super) label: Option<String>,
}

impl AgentArgs {
    pub(super) fn of(args: &Value) -> Result<Self, String> {
        serde_json::from_value(args.clone()).map_err(|e| format!("agent() takes a prompt: {e}"))
    }
}

/// The arguments of a trail() call, as the engine hands them out.
#[derive(Deserialize)]
pub(super) struct TrailArgs {
    pub(super) name: String,
    #[serde(default)]
    pub(super) args: Value,
}

impl TrailArgs {
    pub(super) fn of(args: &Value) -> Result<Self, String> {
        serde_json::from_value(args.clone()).map_err(|e| format!("trail() takes a name: {e}"))
    }
}

/// The arguments of a goto() call, as the engine hands them out.
#[derive(Deserialize)]
pub(super) struct GotoArgs {
    #[serde(default)]
    pub(super) trail: Option<String>,
    #[serde(default)]
    pub(super) at: Option<String>,
    #[serde(default)]
    pub(super) state: Value,
}

impl GotoArgs {
    pub(super) fn of(args: &Value) -> Result<Self, String> {
        serde_json::from_value(args.clone()).map_err(|e| format!("goto() takes a trail and a waypoint by name: {e}"))
    }
}

pub(super) fn function(function: Function) -> TrailFunction {
    match function {
        Function::Agent => TrailFunction::Agent,
        Function::Human => TrailFunction::Human,
        Function::Trail => TrailFunction::Trail,
        Function::Goto => TrailFunction::Goto,
    }
}

pub(super) fn engine_function(function: TrailFunction) -> Function {
    match function {
        TrailFunction::Agent => Function::Agent,
        TrailFunction::Human => Function::Human,
        TrailFunction::Trail => Function::Trail,
        TrailFunction::Goto => Function::Goto,
    }
}

/// The text of an agent() step: its prompt, and with a schema, how to reply.
pub(super) fn prompt(args: &AgentArgs) -> String {
    match &args.schema {
        Some(schema) => format!("{}\n\n{}", args.prompt, reply_as(schema)),
        None => args.prompt.clone(),
    }
}

fn reply_as(schema: &Value) -> String {
    let schema = serde_json::to_string_pretty(schema).unwrap_or_else(|_| schema.to_string());
    format!("Reply with JSON matching this JSON Schema, alone or in a ```json block:\n```json\n{schema}\n```")
}

/// The text asking again for a reply that does not match `schema`, for `why`.
pub(super) fn again(why: &str, schema: &Value) -> String {
    format!("The reply does not match the schema: {why}. {}", reply_as(schema))
}

/// The step's [`TrailStep`] for `pending`, a call of `run`.
pub(super) fn step(run: &TrailRun, pending: &PendingCall) -> TrailStep {
    let args = (pending.function == TrailFunction::Agent).then(|| AgentArgs::of(&pending.args).ok()).flatten();
    let (label, schema) = args.map_or_default(|args| (args.label, args.schema));
    TrailStep { run: run.run.clone(), call: pending.call, label, schema }
}

/// The message of `pending`, a call of `run`, as first sent: the same each time it is made, so a repeat of it is a
/// duplicate the stream drops. Its thread keeps the run's owner, links to the run's thread (a child run's to its root
/// run's) and, on a run an untrusted thread started, is narrowed as a webhook's thread is, so it is as trusted as the
/// run. A trail() call has none: a child run answers it.
pub(super) fn envelope(run: &TrailRun, pending: &PendingCall) -> Result<Envelope, String> {
    let (thread, from, to) = (pending.thread.clone(), run.conductor.clone(), pending.to.clone());
    let message = match pending.function {
        TrailFunction::Agent => Envelope::on_thread(thread, from, to, prompt(&AgentArgs::of(&pending.args)?)),
        TrailFunction::Human => Envelope::asking(thread, from, to, crate::mcp::asked_questions(pending.args.clone())?),
        TrailFunction::Trail => return Err("a trail() call sends no message".to_owned()),
        TrailFunction::Goto => return Err("a goto() call sends no message".to_owned()),
    };
    let allow = (!run.origin.trusted()).then(|| WEBHOOK_ALLOW.map(str::to_owned).to_vec());
    let origin_thread = run.parent.as_ref().map_or(&run.thread, |parent| &parent.origin_thread);
    let message =
        Envelope { id: pending.message, sent_at: pending.sent_at, turn_budget: pending.turn_budget, ..message }
            .with_owner(run.owner.clone())
            .with_origin_thread(Some(origin_thread.clone()))
            .with_allow(allow);
    Ok(message.as_step(step(run, pending)))
}

/// What a reply gives a call.
pub(super) enum Taken {
    Answer(Answer),
    /// The reply does not match the call's schema, for this reason: ask again.
    Again(String),
}

/// What `reply`, answering `pending`, gives the call: for human(), the answer in words; for agent(), the reply's
/// text, or with a schema its JSON, asked for again at most [`MAX_RETRIES`] times while it does not match.
pub(super) fn taken(pending: &PendingCall, reply: &Envelope) -> Taken {
    if pending.function == TrailFunction::Human {
        return Taken::Answer(Answer::Ok(Value::String(human_answer(pending, reply))));
    }
    let Some(schema) = AgentArgs::of(&pending.args).ok().and_then(|args| args.schema) else {
        return Taken::Answer(Answer::Ok(Value::String(reply.text.clone())));
    };
    match schema::reply_json(&reply.text).and_then(|value| schema::check(&schema, &value).map(|()| value)) {
        Ok(value) => Taken::Answer(Answer::Ok(value)),
        Err(why) if pending.retries < MAX_RETRIES => Taken::Again(why),
        Err(why) => {
            let asked = pending.retries + 1;
            Taken::Answer(Answer::Error(format!("the reply did not match the schema, asked {asked} times: {why}")))
        }
    }
}

/// The answer of a human to the questions of `pending`, as ask_human gives it: for one question the option picked
/// or the text typed, for several one `title: answer` line each; the reply's text when it answers otherwise.
fn human_answer(pending: &PendingCall, reply: &Envelope) -> String {
    let questions = crate::mcp::asked_questions(pending.args.clone()).ok();
    let answer = reply.answer.as_ref().filter(|answer| answer.question == pending.message);
    let words = questions
        .as_ref()
        .zip(answer)
        .and_then(|(questions, answer)| questions.answered(answer).map(|answered| questions.answer_text(&answered)));
    words.unwrap_or_else(|| reply.text.clone())
}
