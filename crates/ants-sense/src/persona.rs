//! The ant's persona (requirement ants-x39hk.6.5): its values, voice, boundaries and working style, one item of kind
//! [`PERSONA`]. Only `sense_persona` changes it, and only through a proposal ([`crate::proposal`]) the ant confirms in a
//! dream [`crate::proposal::CONFIRM_AFTER_HOURS`] or more later, at most once in [`PERSONA_EVERY_HOURS`]; each
//! revision keeps the reason of its proposal. The brief shows it after the identity.

use std::fmt::Write as _;

use ants_proto::provider::{BoxFuture, ToolCall};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::Sense;
use crate::items::{Item, ItemId, Values, stamp};
use crate::memory::{holds_recalled_marker, redacted, sanitized};
use crate::proposal::{self, Confirmation, Proposal, ProposalItem, Status, time};
use crate::sanitize::{Limit, Redaction, sanitize_body};
use crate::store::{Items, StoreError};
use crate::tools::Answer;

/// The kind of the persona's item; an ant has one.
pub const PERSONA: &str = "persona";
/// The hours from one change of the persona to the next, at least.
pub const PERSONA_EVERY_HOURS: i64 = 24;
/// The most bytes of each field of the persona: all of them show in every brief.
pub const MAX_FIELD_BYTES: usize = 500;
/// The persona's fields, in the order shown, with their headings.
pub const FIELDS: &[(&str, &str)] =
    &[("values", "Values"), ("voice", "Voice"), ("boundaries", "Boundaries"), ("working_style", "Working style")];

/// The id of the persona's item.
pub fn persona_id() -> ItemId {
    ItemId::named(PERSONA, "self")
}

/// The persona in `items`; none before the ant confirmed one.
pub async fn current(items: &dyn Items) -> Result<Option<Item>, StoreError> {
    items.get(&persona_id()).await
}

/// The fields of the persona `values` holds, one paragraph each under its heading.
pub fn text(values: &Values) -> String {
    let mut text = String::new();
    for (field, heading) in FIELDS {
        if let Some(value) = values.get(*field).and_then(Value::as_str) {
            if !text.is_empty() {
                text.push('\n');
            }
            let _ = write!(text, "{heading}: {}", value.replace('\n', "\n  "));
        }
    }
    text
}

/// Answers a `sense_persona` call: without fields, show the persona and its pending proposal, or with `history`
/// every version; with fields and a reason, propose the persona they make with the current one.
pub fn persona<'a>(sense: &'a Sense, call: &'a ToolCall) -> BoxFuture<'a, Answer> {
    Box::pin(async move { answer(sense, call).await.unwrap_or_else(|refused| refused) })
}

/// The arguments of a `sense_persona` call.
#[derive(Deserialize)]
struct Request {
    values: Option<String>,
    voice: Option<String>,
    boundaries: Option<String>,
    working_style: Option<String>,
    reason: Option<String>,
    finding: Option<String>,
    #[serde(default)]
    history: bool,
}

async fn answer(sense: &Sense, call: &ToolCall) -> Result<Answer, Answer> {
    let request: Request = crate::tools::arguments(call)?;
    let context = &call.context;
    let items = sense.items(&context.ant).await?;
    let mut redactions = Vec::new();
    let fields = fields(
        [
            request.values.as_deref(),
            request.voice.as_deref(),
            request.boundaries.as_deref(),
            request.working_style.as_deref(),
        ],
        &mut redactions,
    )?;
    if fields.is_empty() {
        if request.reason.is_some() {
            return Err(Answer::invalid("Name the fields to change: values, voice, boundaries or working_style."));
        }
        return if request.history { show_history(&*items).await } else { show(&*items).await };
    }
    if request.history {
        return Err(Answer::invalid("history shows the versions: pass it without fields."));
    }
    let reason = proposal::reason(request.reason.as_deref())?;
    let finding = proposal::finding(request.finding.as_deref())?;
    proposal::refuse_untrusted(&*items, context, finding.as_deref()).await?;

    let current = current(&*items).await?;
    let mut version = current.as_ref().map_or_else(Values::new, |current| proposal::content(&current.values));
    for (field, value) in fields {
        match value {
            Some(value) => version.insert(field.to_owned(), json!(value)),
            None => version.remove(field),
        };
    }
    if version.is_empty() {
        return Err(Answer::invalid("A persona holds at least one of values, voice, boundaries and working_style."));
    }
    if current.as_ref().is_some_and(|current| proposal::content(&current.values) == version) {
        return Err(Answer::error("unchanged", "The persona is that already: nothing to propose.".into()));
    }
    let changed: Vec<_> = FIELDS
        .iter()
        .filter(|(field, _)| current.as_ref().and_then(|current| current.values.get(*field)) != version.get(*field))
        .map(|(field, _)| *field)
        .collect();
    let summary = format!("Persona change ({}), proposed in full:\n{}\n", changed.join(", "), text(&version));
    for (field, _) in FIELDS {
        version.entry((*field).to_owned()).or_insert(Value::Null);
    }
    let made = Proposal {
        target: persona_id(),
        kind: PERSONA.into(),
        summary,
        version,
        base: current.as_ref().map_or(0, |current| current.revision),
        reason,
        status: Status::Pending,
        writer: context.clone(),
        finding,
        superseded_by: None,
        decided: None,
    };
    let (made, superseded) = proposal::propose(&*items, made).await?;
    let mut text = format!(
        "Proposed persona change {}: a dream from {} offers it, applied confirming it.",
        made.id(),
        time(made.proposal.ripe())
    );
    if let Some(superseded) = &superseded {
        let _ = write!(text, " It supersedes proposal {superseded}.");
    }
    let (redacted_text, redacted_json) = redacted(&redactions);
    text.push_str(&redacted_text);
    let json = json!({"proposal": made.json(), "superseded": superseded, "redactions": redacted_json});
    Ok(Answer { text, json, is_error: false })
}

/// The fields `given`, in the order of [`FIELDS`], sanitized: an empty text clearing its field.
fn fields(
    given: [Option<&str>; FIELDS.len()],
    redactions: &mut Vec<Redaction>,
) -> Result<Vec<(&'static str, Option<String>)>, Answer> {
    let mut fields = Vec::new();
    for ((field, _), given) in FIELDS.iter().zip(given) {
        let Some(given) = given.map(str::trim) else { continue };
        if given.is_empty() {
            fields.push((*field, None));
            continue;
        }
        let text = sanitized(field, Some(given), sanitize_body(given, Limit::Bytes(MAX_FIELD_BYTES)))?;
        if holds_recalled_marker(&text.text) {
            let message = format!("The {field} quotes recalled memories: write the persona in new words.");
            return Err(Answer::error("recalled_block", message));
        }
        redactions.extend(text.redactions.iter().copied());
        fields.push((*field, Some(text.text)));
    }
    Ok(fields)
}

/// The persona, its revision and the reason of its last change, then its pending proposal.
async fn show(items: &dyn Items) -> Result<Answer, Answer> {
    let current = current(items).await?;
    let pending = proposal::pending_for(items, &persona_id()).await?;
    let mut text = match &current {
        Some(current) => {
            let confirmation = Confirmation::of(&current.values);
            let confirmed = confirmation.confirmed.map_or_default(stamp);
            let reason = confirmation.reason;
            format!(
                "Persona, revision {}, confirmed {confirmed}: {reason}\n{}",
                current.revision,
                self::text(&current.values)
            )
        }
        None => "No persona yet: propose one with sense_persona, giving values, voice, boundaries or working_style \
                 and a reason."
            .to_owned(),
    };
    if let Some(pending) = &pending {
        let _ = write!(text, "\n{}\n{}", pending.line(), self::text(&pending.proposal.version));
    }
    let json = json!({
        "persona": current.as_ref().map(item_json),
        "pending": pending.as_ref().map(ProposalItem::json),
    });
    Ok(Answer { text, json, is_error: false })
}

/// Every version of the persona, the newest first, each with the reason it was confirmed for.
async fn show_history(items: &dyn Items) -> Result<Answer, Answer> {
    let versions = proposal::versions(items, &persona_id()).await?;
    if versions.is_empty() {
        return Ok(Answer { text: "No persona yet.".into(), json: json!({"versions": []}), is_error: false });
    }
    let mut text = format!("{} versions of the persona, the newest first:", versions.len());
    let mut json = Vec::new();
    for (revision, at, values) in versions.iter().rev() {
        let reason = Confirmation::of(values).reason;
        let _ = write!(
            text,
            "\n- Revision {revision}, {}: {reason}\n  {}",
            time(*at),
            self::text(values).replace('\n', "\n  ")
        );
        let mut version = Value::Object(values.clone());
        version["revision"] = json!(revision);
        json.push(version);
    }
    Ok(Answer { text, json: json!({"versions": json}), is_error: false })
}

fn item_json(item: &Item) -> Value {
    let mut json = Value::Object(item.values.clone());
    json["revision"] = json!(item.revision);
    json
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::memory::tests::{context, human, sense, tool};
    use crate::proposal::tests::error;

    #[tokio::test]
    async fn a_proposal_is_checked_and_shown_with_the_persona() {
        let (sense, _) = sense().await;
        let call = |arguments| tool(&sense, persona, arguments, context(human()));
        let shown = call(json!({})).await;
        assert!(shown.text.starts_with("No persona yet"), "{}", shown.text);
        let refusals = [
            (json!({"voice": "Plain."}), "invalid_arguments"),
            (json!({"reason": "Why."}), "invalid_arguments"),
            (json!({"voice": 1, "reason": "Why."}), "invalid_arguments"),
            (json!({"voice": "x".repeat(MAX_FIELD_BYTES + 1), "reason": "Why."}), "too_long"),
            (json!({"voice": "", "reason": "Why."}), "invalid_arguments"),
            (json!({"voice": "Plain.", "reason": "Why.", "history": true}), "invalid_arguments"),
        ];
        for (arguments, code) in refusals {
            assert_eq!(error(&call(arguments.clone()).await), code, "{arguments}");
        }
        let made = call(json!({"voice": "Plain.", "boundaries": "No force pushes.", "reason": "Why."})).await;
        assert!(!made.is_error, "{made:?}");
        let shown = call(json!({})).await;
        assert!(shown.text.contains("(pending, made 2025-01-01T12:00:00Z"), "{}", shown.text);
        assert!(shown.text.contains("Voice: Plain.\nBoundaries: No force pushes."), "{}", shown.text);
    }
}
