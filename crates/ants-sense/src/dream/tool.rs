//! `sense_dream`: the ant's verdicts on the findings of its dream, on the dream thread, and finishing the dream.

use std::fmt::Write as _;

use ants_proto::provider::{BoxFuture, ToolCall};
use chrono::Utc;
use serde_json::{Value, json};

use super::{
    BY, Dream, Judgement, MAX_DREAM_WRITES, MAX_FINDINGS, MAX_REASON_CHARS, State, TRIES, Verdict, close, get,
};
use crate::Sense;
use crate::items::{ItemId, Values};
use crate::sanitize::{Limit, sanitize_title};
use crate::store::{Items, StoreError, Write};
use crate::tools::Answer;

/// The JSON Schema of `sense_dream`'s arguments.
pub fn schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "verdicts": {
                "type": "array",
                "maxItems": MAX_FINDINGS,
                "items": {
                    "type": "object",
                    "properties": {
                        "finding": {"type": "string", "description": "The finding's id, such as 3.1."},
                        "verdict": {"enum": ["applied", "rejected", "deferred"]},
                        "reason": {"type": "string", "maxLength": MAX_REASON_CHARS},
                        "items": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "With applied: the ids of the memories written or voted on.",
                        },
                    },
                    "required": ["finding", "verdict", "reason"],
                },
            },
            "done": {"type": "boolean", "description": "Finish the dream: its diary is written."},
        },
    })
}

/// Answers a `sense_dream` call.
pub fn dream<'a>(sense: &'a Sense, call: &'a ToolCall) -> BoxFuture<'a, Answer> {
    Box::pin(async move { dream_in(sense, call).await.unwrap_or_else(|refused| refused) })
}

async fn dream_in(sense: &Sense, call: &ToolCall) -> Result<Answer, Answer> {
    let context = &call.context;
    let items = sense.items(&context.ant).await.map_err(unavailable)?;
    let (dream, _) = super::last(&*items).await.map_err(unavailable)?.ok_or_else(|| {
        Answer::error("no_dream", "No dream yet: sense begins one once the ant has been idle a while.".into())
    })?;
    if dream.thread.as_ref() != Some(&context.thread) {
        let message = match &dream.thread {
            Some(thread) => format!("Dream {} is answered on its own thread, {thread}.", dream.number),
            None => format!("Dream {} has not woken the ant yet.", dream.number),
        };
        return Err(Answer::error("not_dream_thread", message));
    }
    if dream.state == State::Done {
        return Err(finished(&dream));
    }
    let verdicts = verdicts(&*items, &dream, &call.arguments).await?;
    let done = match call.arguments.get("done") {
        None | Some(Value::Null) => false,
        Some(Value::Bool(done)) => *done,
        Some(_) => return Err(invalid("done must be true or false.".into())),
    };
    if verdicts.is_empty() && !done {
        return Ok(show(&dream));
    }
    for _ in 0..TRIES {
        let gone = || unavailable(StoreError::NotFound(Dream::id(dream.number)));
        let (mut current, revision) = get(&*items, dream.number).await.map_err(unavailable)?.ok_or_else(gone)?;
        if current.state == State::Done {
            return Err(finished(&current));
        }
        for verdict in &verdicts {
            current.verdicts.retain(|earlier| earlier.finding != verdict.finding);
            current.verdicts.push(verdict.clone());
        }
        let writes = if done {
            close(current.clone(), revision, context, Utc::now()).to_vec()
        } else {
            let mut values = Values::new();
            values.insert("verdicts".into(), json!(current.verdicts));
            vec![Write::Change { id: Dream::id(current.number), values, expected: Some(revision) }]
        };
        match items.write(&writes, BY).await {
            Ok(written) => {
                if done {
                    current.state = State::Done;
                    current.diary = written.first().map(|diary| diary.id.clone());
                }
                return Ok(answered(&current, verdicts.len()));
            }
            Err(StoreError::Conflict { .. }) => {}
            Err(e) => return Err(unavailable(e)),
        }
    }
    Err(unavailable(StoreError::Unavailable(format!("dream {} kept changing meanwhile", dream.number))))
}

/// The verdicts `arguments` give on findings of `dream`, checked.
async fn verdicts(
    items: &dyn Items,
    dream: &Dream,
    arguments: &serde_json::Map<String, Value>,
) -> Result<Vec<Verdict>, Answer> {
    let given = match arguments.get("verdicts") {
        None | Some(Value::Null) => return Ok(Vec::new()),
        Some(Value::Array(given)) if given.len() <= MAX_FINDINGS => given,
        Some(Value::Array(_)) => return Err(invalid(format!("At most {MAX_FINDINGS} verdicts at a time."))),
        Some(_) => return Err(invalid("verdicts must be a list.".into())),
    };
    let at = Utc::now();
    let mut verdicts: Vec<Verdict> = Vec::new();
    for given in given {
        let text = |name: &str| given.get(name).and_then(Value::as_str).map(str::trim).filter(|text| !text.is_empty());
        let finding = text("finding").ok_or_else(|| invalid("Each verdict names its finding.".into()))?;
        if !dream.findings.iter().any(|known| known.id == finding) {
            return Err(Answer::error("not_found", format!("Dream {} has no finding {finding:?}.", dream.number)));
        }
        let verdict = match text("verdict") {
            Some("applied") => Judgement::Applied,
            Some("rejected") => Judgement::Rejected,
            Some("deferred") => Judgement::Deferred,
            _ => return Err(invalid(format!("Finding {finding}: the verdict is applied, rejected or deferred."))),
        };
        let reason = text("reason").ok_or_else(|| invalid(format!("Finding {finding}: the reason is missing.")))?;
        let reason = sanitize_title(reason, Limit::Chars(MAX_REASON_CHARS))
            .map_err(|refused| Answer::error("too_long", format!("Finding {finding}: the reason is {refused}.")))?
            .text;
        let mut written = Vec::new();
        for id in given.get("items").and_then(Value::as_array).into_iter().flatten() {
            let id = id.as_str().and_then(|id| id.trim().parse::<ItemId>().ok());
            let id = id.ok_or_else(|| invalid(format!("Finding {finding}: items are item ids.")))?;
            if items.get(&id).await.map_err(unavailable)?.is_none() {
                return Err(Answer::error("not_found", format!("Finding {finding}: no item {id}.")));
            }
            written.push(id);
        }
        if verdict == Judgement::Applied && written.is_empty() {
            return Err(invalid(format!("Finding {finding}: applied names the items written or voted on.")));
        }
        verdicts.retain(|earlier| earlier.finding != finding);
        verdicts.push(Verdict { finding: finding.to_owned(), verdict, reason, items: written, at });
    }
    Ok(verdicts)
}

/// `dream`'s findings and the verdicts on them.
fn show(dream: &Dream) -> Answer {
    let mut text =
        format!("Dream {} ({}), {} of {MAX_DREAM_WRITES} writes made.", dream.number, dream.state, dream.writes.len());
    if dream.findings.is_empty() {
        text.push_str("\nNo findings: remember what the threads teach, then finish with done.");
    }
    for finding in &dream.findings {
        let _ = write!(text, "\n{}", super::packet::finding_line(finding, dream.number));
        if let Some(verdict) = dream.verdict(&finding.id) {
            let _ = write!(text, "\n  {}: {}", verdict.verdict, verdict.reason);
        }
    }
    Answer { text, json: json(dream), is_error: false }
}

/// The answer after recording `recorded` verdicts on `dream`.
fn answered(dream: &Dream, recorded: usize) -> Answer {
    let open: Vec<_> = dream.findings.iter().filter(|finding| dream.verdict(&finding.id).is_none()).collect();
    let mut text = format!("Dream {}: {recorded} verdicts recorded", dream.number);
    if dream.state == State::Done {
        let diary = dream.diary.as_ref().map_or_else(String::new, |diary| format!(" in diary {diary}"));
        let _ = write!(text, "; done{diary}.");
        if !open.is_empty() {
            let _ = write!(text, " {} unanswered findings come again in the next dream.", open.len());
        }
    } else if open.is_empty() {
        text.push_str("; every finding is answered: finish with done.");
    } else {
        let ids: Vec<_> = open.iter().map(|finding| finding.id.as_str()).collect();
        let _ = write!(text, "; open: {}.", ids.join(", "));
    }
    Answer { text, json: json(dream), is_error: false }
}

fn json(dream: &Dream) -> Value {
    json!({
        "dream": dream.number,
        "state": dream.state,
        "findings": dream.findings,
        "verdicts": dream.verdicts,
        "writes": dream.writes.len(),
        "diary": dream.diary,
    })
}

fn finished(dream: &Dream) -> Answer {
    Answer::error("done", format!("Dream {} is done: its diary is written.", dream.number))
}

fn invalid(message: String) -> Answer {
    Answer::error("invalid", message)
}

fn unavailable(e: StoreError) -> Answer {
    Answer::error("unavailable", format!("Dreams are not available: {e}."))
}
