//! `sense_identity` (requirements ants-x39hk.6.5 and ants-x39hk.6.6): who the ant is, shown whole, and the one way
//! to change it. Without arguments it shows the card with the experience, the persona with its pending proposal, the
//! self-model, the relationship records by title and the skills by name ([`show`]). With `self_model`, or
//! `relationship` and `who`, it replaces that record from the revision read, at once ([`records::replace`]); with
//! persona fields and a reason, it proposes a persona change a later dream confirms ([`persona::propose`]); with
//! `history`, it shows every version of the persona ([`persona::history`]).

use std::fmt::Write as _;

use ants_proto::provider::{BoxFuture, CallContext, ToolCall};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::brief::{TOP_EXPERIENCE, identity_text};
use crate::memory::{MAX_TITLE_CHARS, Quotes, Scope, framed};
use crate::persona::{self, MAX_FIELD_BYTES};
use crate::records::{self, Change, MAX_RECORD_BYTES, RELATIONSHIP, RecordItem};
use crate::store::{Items, StoreError};
use crate::tools::{Answer, MAX_REASON_CHARS};
use crate::{Sense, skill, thread};

/// The JSON Schema of `sense_identity`'s arguments.
pub fn schema() -> Value {
    let field = |what: &str| {
        let description = format!(
            "Persona: {what}, at most {MAX_FIELD_BYTES} bytes; empty clears it, none keeps it. Needs a reason."
        );
        json!({"type": "string", "description": description})
    };
    let record = |what: &str| {
        let description = format!("{what}, at most {MAX_RECORD_BYTES} bytes, replacing the record whole.");
        json!({"type": "string", "description": description})
    };
    json!({
        "type": "object",
        "properties": {
            "values": field("what the ant holds important"),
            "voice": field("how it speaks"),
            "boundaries": field("what it will not do"),
            "working_style": field("how it works"),
            "reason": {
                "type": "string",
                "maxLength": MAX_REASON_CHARS,
                "description": "Why the persona changes: kept with the version.",
            },
            "finding": {"type": "string", "description": "The dream finding a persona change answers, such as 3.1."},
            "history": {"type": "boolean", "description": "Show every version of the persona and its reason."},
            "self_model": record("The self-model: what the ant holds true of itself"),
            "relationship": record("The relationship record of who: working with them"),
            "who": {"type": "string", "description": "With relationship: human:<name> or ant:<name>."},
            "title": {
                "type": "string",
                "maxLength": MAX_TITLE_CHARS,
                "description": "With self_model or relationship: a line summing the record up.",
            },
            "revision": {
                "type": "integer",
                "minimum": 0,
                "description": "With self_model or relationship: the revision replaced, none for the first.",
            },
            "quotes": {
                "enum": ["recalled", "external"],
                "description": "With self_model or relationship: set when the text quotes recalled memories or \
                                outside content, stored as untrusted.",
            },
        },
    })
}

/// The arguments of a `sense_identity` call.
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
    self_model: Option<String>,
    relationship: Option<String>,
    who: Option<Scope>,
    title: Option<String>,
    revision: Option<u64>,
    quotes: Option<Quotes>,
}

/// Answers a `sense_identity` call.
pub fn identity<'a>(sense: &'a Sense, call: &'a ToolCall) -> BoxFuture<'a, Answer> {
    Box::pin(async move { answer(sense, call).await.unwrap_or_else(|refused| refused) })
}

async fn answer(sense: &Sense, call: &ToolCall) -> Result<Answer, Answer> {
    let request: Request = crate::tools::arguments(call)?;
    let context = &call.context;
    let items = sense.items(&context.ant).await?;
    let fields = [
        request.values.as_deref(),
        request.voice.as_deref(),
        request.boundaries.as_deref(),
        request.working_style.as_deref(),
    ];
    let proposing = fields.iter().any(Option::is_some) || request.reason.is_some() || request.finding.is_some();
    let body = match (request.self_model.as_deref(), request.relationship.as_deref()) {
        (Some(_), Some(_)) => {
            return Err(Answer::invalid("One record a call: give self_model or relationship, not both."));
        }
        (Some(body), None) | (None, Some(body)) => body,
        (None, None) => {
            if request.who.is_some()
                || request.title.is_some()
                || request.revision.is_some()
                || request.quotes.is_some()
            {
                return Err(Answer::invalid("who, title, revision and quotes go with self_model or relationship."));
            }
            if proposing && request.history {
                return Err(Answer::invalid("history shows the persona's versions: pass it without fields."));
            }
            if proposing {
                let (reason, finding) = (request.reason.as_deref(), request.finding.as_deref());
                return persona::propose(&*items, context, fields, reason, finding).await;
            }
            return if request.history { persona::history(&*items).await } else { show(&*items, context).await };
        }
    };
    if proposing || request.history {
        return Err(Answer::invalid("A record is replaced alone: propose a persona change in another call."));
    }
    let scope = match (request.self_model.is_some(), request.who) {
        (true, None | Some(Scope::Own)) => Scope::Own,
        (true, Some(_)) => return Err(Answer::invalid("The self-model is the ant's own: give it without who.")),
        (false, Some(who @ (Scope::Human(_) | Scope::Ant(_)))) => who,
        (false, _) => return Err(Answer::invalid("A relationship record needs who: human:<name> or ant:<name>.")),
    };
    let change =
        Change { scope, title: request.title.as_deref(), body, revision: request.revision, quotes: request.quotes };
    records::replace(&*items, context, change).await
}

/// Who the ant of `context` is: its card and experience, its persona and pending proposal, its self-model, its
/// relationship records by title and its skills by name.
async fn show(items: &dyn Items, context: &CallContext) -> Result<Answer, Answer> {
    let card = super::card(items).await?;
    let experience = thread::experience::experience(items, &context.ant, TOP_EXPERIENCE).await?;
    let persona = persona::show(items).await?;
    let self_model = records::self_model(items).await?;
    let relationships = relationships(items).await?;
    let skills = skill::skills(items).await?;

    let mut text = format!("## Identity\n{}", identity_text(card.as_ref(), &context.ant, experience.as_ref()));
    let _ = write!(text, "\n\n## Persona\n{}", persona.text);
    text.push_str("\n\n## Self-model\n");
    match &self_model {
        Some(found) => text.push_str(&framed(&found.text())),
        None => text.push_str(
            "No self-model yet: write one with sense_identity, giving self_model and a title, without a revision.",
        ),
    }
    text.push_str("\n\n## Relationships (sense_recall id=<id> reads one)\n");
    if relationships.is_empty() {
        text.push_str("None yet: write one with sense_identity, giving relationship, who and a title.");
    } else {
        let heads: Vec<_> = relationships.iter().map(RecordItem::head).collect();
        text.push_str(&framed(&heads.join("\n")));
    }
    text.push_str("\n\n## Skills (sense_skill name=<name> reads one)\n");
    if skills.is_empty() {
        text.push_str("None yet: propose one with sense_skill.");
    } else {
        let names: Vec<_> = skills.iter().map(|skill| format!("- {}", skill.name)).collect();
        text.push_str(&names.join("\n"));
    }
    let json = json!({
        "card": card,
        "experience": experience,
        "persona": persona.json["persona"],
        "pending": persona.json["pending"],
        "self_model": self_model.as_ref().map(RecordItem::json),
        "relationships": relationships.iter().map(|found| json!({
            "id": found.id(),
            "scope": found.record.scope,
            "title": found.record.title,
            "revision": found.item.revision,
            "trust": found.record.trust,
        })).collect::<Vec<_>>(),
        "skills": skills.iter().map(|skill| &skill.name).collect::<Vec<_>>(),
    });
    Ok(Answer { text, json, is_error: false })
}

/// Every relationship record, by whom it is of.
async fn relationships(items: &dyn Items) -> Result<Vec<RecordItem>, StoreError> {
    let mut found: Vec<_> =
        items.list(RELATIONSHIP, usize::MAX).await?.into_iter().filter_map(RecordItem::of).collect();
    found.sort_by_key(|found| found.record.scope.to_string());
    Ok(found)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;
    use crate::items::Values;
    use crate::memory::RECALLED_BEGIN;
    use crate::memory::tests::{context, human, sense, tool};
    use crate::persona::{PERSONA, persona_id};
    use crate::proposal::tests::error;
    use crate::records::SELF_MODEL;
    use crate::skill::{SKILL, skill_id};
    use crate::store::Write;

    fn position(text: &str, part: &str) -> usize {
        text.find(part).unwrap_or_else(|| panic!("{part:?} not in:\n{text}"))
    }

    #[tokio::test]
    async fn without_arguments_it_shows_the_whole_identity() {
        let (sense, kept) = sense().await;
        let items: Arc<dyn Items> = kept.clone();
        let call = |arguments| tool(&sense, identity, arguments, context(human()));
        let empty = [
            "## Identity\ndev-a, with no sense card yet.",
            "## Persona\nNo persona yet: propose one with sense_identity",
            "## Self-model\nNo self-model yet: write one with sense_identity",
            "## Relationships (sense_recall id=<id> reads one)\nNone yet",
            "## Skills (sense_skill name=<name> reads one)\nNone yet",
        ];
        let shown = call(json!({})).await.text;
        assert!(empty.iter().map(|part| position(&shown, part)).is_sorted(), "{shown}");

        let mut persona = Values::new();
        persona.insert("voice".into(), json!("Plain."));
        let mut skill = Values::new();
        skill.insert("name".into(), json!("release"));
        skill.insert("description".into(), json!("Cut a release."));
        skill.insert("files".into(), json!({"SKILL.md": "---\nname: release\ndescription: Cut a release.\n---\n"}));
        let writes = [
            Write::Create { id: persona_id(), kind: PERSONA.into(), values: persona },
            Write::Create { id: skill_id("release"), kind: SKILL.into(), values: skill },
        ];
        items.write(&writes, "test").await.unwrap();
        for arguments in [
            json!({"self_model": "Careful.", "title": "A careful reviewer"}),
            json!({"relationship": "Wants tests first.", "who": "human:alice", "title": "Alice: tests first"}),
            json!({"relationship": "Reviews fast.", "who": "ant:dev-b", "title": "dev-b: fast reviews"}),
        ] {
            assert!(!call(arguments).await.is_error);
        }
        let shown = call(json!({})).await;
        let text = &shown.text;
        position(text, "## Persona\nPersona, revision 1");
        position(text, "Voice: Plain.");
        let model = position(text, "(self, revision 1): A careful reviewer\n  Careful.");
        assert!(position(text, RECALLED_BEGIN) < model, "{text}");
        let bob = position(text, "(ant:dev-b, revision 1): dev-b: fast reviews\n");
        assert!(bob < position(text, "(human:alice, revision 1): Alice: tests first\n"), "{text}");
        assert!(!text.contains("Wants tests first."), "relationships by title: {text}");
        position(text, "## Skills (sense_skill name=<name> reads one)\n- release");
        assert!(!text.contains("Cut a release."), "skills by name: {text}");
        assert_eq!(shown.json["persona"]["voice"], "Plain.");
        assert_eq!(shown.json["self_model"]["body"], "Careful.");
        assert_eq!(shown.json["relationships"][1]["title"], "Alice: tests first");
        assert_eq!(shown.json["skills"], json!(["release"]));
    }

    #[tokio::test]
    async fn a_call_shows_or_changes_one_part() {
        let (sense, kept) = sense().await;
        let items: Arc<dyn Items> = kept.clone();
        let call = |arguments| tool(&sense, identity, arguments, context(human()));
        let refusals = [
            json!({"self_model": "Me.", "relationship": "You.", "who": "human:bob", "title": "Both"}),
            json!({"who": "human:bob"}),
            json!({"title": "Me"}),
            json!({"revision": 1}),
            json!({"quotes": "external"}),
            json!({"self_model": "Me.", "title": "Me", "voice": "Plain.", "reason": "Why."}),
            json!({"self_model": "Me.", "title": "Me", "history": true}),
            json!({"relationship": "You.", "title": "You"}),
            json!({"who": 1}),
        ];
        for arguments in refusals {
            assert_eq!(error(&call(arguments.clone()).await), "invalid_arguments", "{arguments}");
        }
        assert!(items.list(SELF_MODEL, usize::MAX).await.unwrap().is_empty());
        assert!(items.list(crate::proposal::PROPOSAL, usize::MAX).await.unwrap().is_empty());
        let history = call(json!({"history": true})).await;
        assert_eq!((history.text.as_str(), &history.json), ("No persona yet.", &json!({"versions": []})));
    }
}
