//! The ant's persona (requirement ants-x39hk.6.5): its values, voice, boundaries and working style, and the profile
//! in which it tells the colony what it is good for, one item of kind [`PERSONA`]. Only `sense_identity` changes it
//! (`propose`), and only through a proposal (`crate::proposal`) the ant confirms in a dream
//! `crate::proposal::CONFIRM_AFTER_HOURS` or more later, at most once in [`PERSONA_EVERY_HOURS`]; each revision keeps
//! the reason of its proposal. The brief shows it after the identity, and so does `sense_identity` without arguments;
//! the daemon serving the ant publishes the confirmed profile in its colony entry ([`profile`]).

use std::fmt::Write as _;

use ants_proto::provider::CallContext;
use serde_json::{Value, json};

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
pub const FIELDS: &[(&str, &str)] = &[
    ("values", "Values"),
    ("voice", "Voice"),
    ("boundaries", "Boundaries"),
    ("working_style", "Working style"),
    (PROFILE, "Profile"),
];
/// The field in which the ant tells the colony what it is good for: other ants and the messenger read it, as confirmed,
/// in the ant's colony entry.
pub const PROFILE: &str = "profile";
/// The most bytes of the profile, which every listing of the colony shows.
pub const MAX_PROFILE_BYTES: usize = 300;

/// The id of the persona's item.
pub fn persona_id() -> ItemId {
    ItemId::named(PERSONA, "self")
}

/// The persona in `items`; none before the ant confirmed one.
pub async fn current(items: &dyn Items) -> Result<Option<Item>, StoreError> {
    items.get(&persona_id()).await
}

/// The profile of the confirmed persona in `items`: what the ant published to the colony; none without one.
pub async fn profile(items: &dyn Items) -> Result<Option<String>, StoreError> {
    Ok(current(items).await?.and_then(|persona| Some(persona.values.get(PROFILE)?.as_str()?.to_owned())))
}

/// The headings of the fields the persona `values` holds, in the order shown.
pub fn headings(values: &Values) -> Vec<&'static str> {
    let held = FIELDS.iter().filter(|(field, _)| values.get(*field).is_some_and(|value| !value.is_null()));
    held.map(|(_, heading)| *heading).collect()
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

/// Propose the persona the fields `given`, in the order of [`FIELDS`], make with the current one, for `reason`, in
/// answer to the dream finding `finding` when there is one: a `sense_identity` call of the ant of `context`.
pub(crate) async fn propose(
    items: &dyn Items,
    context: &CallContext,
    given: [Option<&str>; FIELDS.len()],
    reason: Option<&str>,
    finding: Option<&str>,
) -> Result<Answer, Answer> {
    let mut redactions = Vec::new();
    let fields = fields(given, &mut redactions)?;
    if fields.is_empty() {
        return Err(Answer::invalid("Name the fields to change: values, voice, boundaries, working_style or profile."));
    }
    let reason = proposal::reason(reason)?;
    let finding = proposal::finding(finding)?;
    proposal::refuse_untrusted(items, context, finding.as_deref()).await?;

    let current = current(items).await?;
    let mut version = current.as_ref().map_or_else(Values::new, |current| proposal::content(&current.values));
    for (field, value) in fields {
        match value {
            Some(value) => version.insert(field.to_owned(), json!(value)),
            None => version.remove(field),
        };
    }
    if version.is_empty() {
        return Err(Answer::invalid(
            "A persona holds at least one of values, voice, boundaries, working_style and profile.",
        ));
    }
    if current.as_ref().is_some_and(|current| proposal::content(&current.values) == version) {
        return Err(Answer::error("unchanged", "The persona is that already: nothing to propose.".into()));
    }
    let changed: Vec<_> = FIELDS
        .iter()
        .filter(|(field, _)| current.as_ref().and_then(|current| current.values.get(*field)) != version.get(*field))
        .map(|(field, _)| *field)
        .collect();
    let mut summary = format!("Persona change ({}), proposed in full:\n{}\n", changed.join(", "), text(&version));
    if changed.contains(&PROFILE) {
        summary.push_str(if version.contains_key(PROFILE) {
            "Once confirmed, the profile shows in the ant's colony entry, to other ants and the messenger.\n"
        } else {
            "Once confirmed, the profile leaves the ant's colony entry.\n"
        });
    }
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
    let (made, superseded) = proposal::propose(items, made).await?;
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
        let most = if *field == PROFILE { MAX_PROFILE_BYTES } else { MAX_FIELD_BYTES };
        let text = sanitized(field, Some(given), sanitize_body(given, Limit::Bytes(most)))?;
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
pub(crate) async fn show(items: &dyn Items) -> Result<Answer, Answer> {
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
        None => "No persona yet: propose one with sense_identity, giving values, voice, boundaries, working_style or \
                 profile and a reason."
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
pub(crate) async fn history(items: &dyn Items) -> Result<Answer, Answer> {
    let versions = proposal::versions(items, &persona_id()).await?;
    if versions.is_empty() {
        return Ok(Answer { text: "No persona yet.".into(), json: json!({"versions": []}), is_error: false });
    }
    let count = versions.len();
    let noun = if count == 1 { "version" } else { "versions" };
    let mut text = format!("{count} {noun} of the persona, the newest first:");
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
    use crate::identity::identity;
    use crate::proposal::tests::error;
    use crate::testing::{context, human, sense, tool};

    #[tokio::test]
    async fn a_proposal_is_checked_and_shown_with_the_persona() {
        let (sense, _) = sense().await;
        let call = |arguments| tool(&sense, identity, arguments, context(human()));
        let shown = call(json!({})).await;
        assert!(shown.text.contains("## Persona\nNo persona yet: propose one with sense_identity"), "{}", shown.text);
        let refusals = [
            (json!({"voice": "Plain."}), "invalid_arguments"),
            (json!({"reason": "Why."}), "invalid_arguments"),
            (json!({"voice": 1, "reason": "Why."}), "invalid_arguments"),
            (json!({"voice": "x".repeat(MAX_FIELD_BYTES + 1), "reason": "Why."}), "too_long"),
            (json!({"profile": "x".repeat(MAX_PROFILE_BYTES + 1), "reason": "Why."}), "too_long"),
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
