//! `sense_remember`: validate, sanitize and check a new memory, then store it, retiring the one it supersedes in the
//! same write.

use std::fmt::Write as _;

use ants_proto::provider::{BoxFuture, ToolCall};
use serde_json::{Map, Value, json};

use super::{
    CORE_BUDGET, Citation, DEFAULT_IMPORTANCE, DIGEST, MAX_BODY_BYTES, MAX_CITATIONS, MAX_TITLE_CHARS, MEMORY, Memory,
    MemoryItem, MemoryKind, NEAR_DUPLICATE, Scope, Status, Trust, core, digest, holds_recalled_block,
    holds_recalled_marker, normalize_remote, similarity,
};
use crate::Sense;
use crate::items::{ItemId, Values};
use crate::sanitize::{Limit, Redaction, Refused, Sanitized, sanitize_body, sanitize_title};
use crate::store::{Items, StoreError, Write};
use crate::tools::Answer;

/// The best full-text matches compared for near duplicates.
const CANDIDATES: usize = 20;
/// The near duplicates named, at most.
const NAMED: usize = 3;
/// The most characters of a citation's path.
const MAX_PATH_CHARS: usize = 1_024;

/// Answers a `sense_remember` call.
pub fn remember<'a>(sense: &'a Sense, call: &'a ToolCall) -> BoxFuture<'a, Answer> {
    Box::pin(async move { remember_in(sense, call).await.unwrap_or_else(|refused| refused) })
}

async fn remember_in(sense: &Sense, call: &ToolCall) -> Result<Answer, Answer> {
    let request = Request::parse(&call.arguments)?;
    if holds_recalled_block(&request.title.text) || holds_recalled_block(&request.body.text) {
        let message = "Recalled memories are not stored again: write what was learned in new words.";
        return Err(Answer::error("recalled_block", message.into()));
    }
    let items = sense.items(&call.context.ant).await.map_err(unavailable)?;
    let items = items.as_ref();
    let old = match &request.supersedes {
        Some(id) => Some(superseded(items, id).await?),
        None => None,
    };
    let scope = match (&request.scope, &old) {
        (Some(scope), _) => scope.clone(),
        (None, Some(old)) => old.memory.scope.clone(),
        (None, None) => Scope::Own,
    };
    let digest = digest(&request.body.text);
    check(items, &request, &scope, old.as_ref().map(MemoryItem::id), &digest).await?;

    let context = &call.context;
    let untrusted = if !context.trusted() {
        Some("the thread came from a webhook")
    } else if request.quotes.is_some() {
        request.quotes
    } else if holds_recalled_marker(&request.title.text) || holds_recalled_marker(&request.body.text) {
        Some("it quotes recalled memories")
    } else {
        None
    };
    let mut citations = request.citations;
    let citation_warnings = match sense.workdir(context) {
        Some(workdir) => super::citations::record(&mut citations, &workdir).await,
        None => Vec::new(),
    };
    let citations = citations
        .into_iter()
        .map(|citation| match (&scope, &citation.remote) {
            (Scope::Repo(remote), None) => Citation { remote: Some(remote.clone()), ..citation },
            _ => citation,
        })
        .collect();
    let memory = Memory {
        importance: request.importance,
        citations,
        status: if citation_warnings.is_empty() { Status::Active } else { Status::NeedsReview },
        citation_warnings,
        trust: if untrusted.is_some() { Trust::Untrusted } else { Trust::Own },
        core: request.core,
        supersedes: old.as_ref().map(|old| old.id().clone()),
        ..Memory::new(request.kind, scope, request.title.text, request.body.text, context)
    };
    let id = ItemId::new();
    let mut writes = vec![memory.create(id.clone())];
    if let Some(old) = &old {
        let mut values = Values::new();
        values.insert("status".into(), json!(Status::Superseded));
        values.insert("superseded_by".into(), json!(id));
        writes.push(Write::Change { id: old.id().clone(), values, expected: Some(old.item.revision) });
    }
    let written = items.write(&writes, context.thread.as_str()).await.map_err(|e| match e {
        StoreError::Conflict { current, .. } => {
            Answer::error("conflict", format!("Memory {} changed meanwhile: try again.", current.id))
        }
        e => unavailable(e),
    })?;
    Ok(remembered(&written[0].id, written[0].revision, &memory, untrusted, &request.redactions))
}

/// Refuses `request`, of `scope`, when a live memory of its scope other than `old`, the one it supersedes, has the
/// same `digest` of its body or, unless forced, is a near duplicate; or when it is core and takes the core memories
/// past their budget.
async fn check(
    items: &dyn Items,
    request: &Request,
    scope: &Scope,
    old: Option<&ItemId>,
    digest: &str,
) -> Result<(), Answer> {
    let rival =
        |found: &MemoryItem| found.memory.status.live() && found.memory.scope == *scope && Some(found.id()) != old;
    let same = items.list_by(MEMORY, DIGEST, Some(&json!(digest)), usize::MAX).await.map_err(unavailable)?;
    if let Some(same) = same.into_iter().filter_map(MemoryItem::of).find(rival) {
        return Err(duplicate(&same));
    }
    if !request.force {
        let text = format!("{}\n{}", request.title.text, request.body.text);
        let hits = items.search(&text, Some(MEMORY), CANDIDATES).await.map_err(unavailable)?;
        let near: Vec<_> = hits
            .into_iter()
            .filter_map(|hit| MemoryItem::of(hit.item).filter(rival))
            .map(|found| {
                let alike = similarity(&text, &format!("{}\n{}", found.memory.title, found.memory.body));
                (found, alike)
            })
            .filter(|(_, alike)| *alike >= NEAR_DUPLICATE)
            .take(NAMED)
            .collect();
        if !near.is_empty() {
            return Err(near_duplicate(&near));
        }
    }
    if request.core {
        let core = core(items).await.map_err(unavailable)?;
        let core: Vec<_> = core.into_iter().filter(|found| Some(found.id()) != old).collect();
        let total = core.iter().map(|found| found.memory.core_bytes()).sum::<usize>()
            + request.title.text.len()
            + request.body.text.len();
        if total > CORE_BUDGET {
            return Err(core_cap(&core, total));
        }
    }
    Ok(())
}

/// What a call asks to remember, sanitized.
struct Request {
    kind: MemoryKind,
    scope: Option<Scope>,
    title: Sanitized,
    body: Sanitized,
    importance: u8,
    citations: Vec<Citation>,
    core: bool,
    supersedes: Option<ItemId>,
    force: bool,
    /// Why the text is not the ant's own, when the call says it quotes something.
    quotes: Option<&'static str>,
    redactions: Vec<Redaction>,
}

impl Request {
    /// The request of `arguments`; arguments it does not know, such as a writer or a trust, are ignored.
    fn parse(arguments: &Map<String, Value>) -> Result<Self, Answer> {
        let text = |name: &str| -> Result<Option<&str>, Answer> {
            match arguments.get(name) {
                None | Some(Value::Null) => Ok(None),
                Some(Value::String(text)) => Ok(Some(text)),
                Some(_) => Err(invalid(format!("{name} must be a string."))),
            }
        };
        let flag = |name: &str| -> Result<bool, Answer> {
            match arguments.get(name) {
                None | Some(Value::Null) => Ok(false),
                Some(Value::Bool(flag)) => Ok(*flag),
                Some(_) => Err(invalid(format!("{name} must be true or false."))),
            }
        };
        let kind = text("kind")?.ok_or_else(|| invalid(kinds("kind is missing")))?;
        let kind = kind.parse().map_err(|_| invalid(kinds(&format!("No kind of memory {kind:?}"))))?;
        let scope = text("scope")?.map(str::parse).transpose().map_err(invalid)?;
        let title = text("title")?;
        let title =
            sanitized("title", title, sanitize_title(title.unwrap_or_default().trim(), Limit::Chars(MAX_TITLE_CHARS)))?;
        let body = text("body")?;
        let body =
            sanitized("body", body, sanitize_body(body.unwrap_or_default().trim(), Limit::Bytes(MAX_BODY_BYTES)))?;
        let importance = match arguments.get("importance") {
            None | Some(Value::Null) => DEFAULT_IMPORTANCE,
            Some(value) => value
                .as_u64()
                .filter(|importance| (1..=5).contains(importance))
                .map(|importance| importance as u8)
                .ok_or_else(|| invalid("importance must be from 1 to 5.".into()))?,
        };
        let supersedes = text("supersedes")?
            .map(|id| id.trim().parse::<ItemId>().map_err(|_| invalid(format!("No memory id: {id:?}."))))
            .transpose()?;
        let quotes = match text("quotes")? {
            None => None,
            Some("recalled") => Some("it quotes recalled memories"),
            Some("external") => Some("it quotes outside content"),
            Some(other) => return Err(invalid(format!("quotes must be recalled or external, not {other:?}."))),
        };
        let mut redactions = title.redactions.clone();
        redactions.extend(body.redactions.iter().copied());
        let citations = match arguments.get("citations") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Array(given)) if given.len() <= MAX_CITATIONS => {
                given.iter().map(|citation| parse_citation(citation, &mut redactions)).collect::<Result<_, _>>()?
            }
            Some(Value::Array(_)) => return Err(invalid(format!("At most {MAX_CITATIONS} citations."))),
            Some(_) => return Err(invalid("citations must be a list.".into())),
        };
        Ok(Self {
            kind,
            scope,
            title,
            body,
            importance,
            citations,
            core: flag("core")?,
            supersedes,
            force: flag("force")?,
            quotes,
            redactions,
        })
    }
}

fn kinds(start: &str) -> String {
    let all: Vec<_> = MemoryKind::ALL.iter().map(|kind| kind.name()).collect();
    format!("{start}: one of {}.", all.join(", "))
}

/// The sanitized value `name` given as `given`, which must hold text.
fn sanitized(name: &str, given: Option<&str>, sanitized: Result<Sanitized, Refused>) -> Result<Sanitized, Answer> {
    match sanitized {
        Ok(sanitized) if given.is_some() && !sanitized.text.trim().is_empty() => Ok(sanitized),
        Ok(_) => Err(invalid(format!("{name} is missing."))),
        Err(refused) => {
            let unit = match refused.limit {
                Limit::Chars(_) => "characters",
                Limit::Bytes(_) => "bytes",
            };
            let message = format!("The {name} has {} {unit}, over the limit of {}.", refused.size, refused.limit);
            Err(Answer::error("too_long", message))
        }
    }
}

/// A citation as given: `path`, `path:start` or `path:start-end`, or `{path, start, end, remote, hash}`.
fn parse_citation(given: &Value, redactions: &mut Vec<Redaction>) -> Result<Citation, Answer> {
    let bad = |why: &str| invalid(format!("Citation {given}: {why}."));
    let mut citation = match given {
        Value::String(text) => {
            let (path, lines) = match text.rsplit_once(':') {
                Some((path, lines)) if !lines.is_empty() && lines.bytes().all(|b| b.is_ascii_digit() || b == b'-') => {
                    (path, Some(lines))
                }
                _ => (text.as_str(), None),
            };
            let line = |line: &str| line.parse().map_err(|_| bad("lines must read start-end"));
            let (start, end) = match lines.map(|lines| lines.split_once('-').unwrap_or((lines, lines))) {
                Some((start, end)) => (Some(line(start)?), Some(line(end)?)),
                None => (None, None),
            };
            Citation { path: path.to_owned(), start, end, remote: None, hash: None }
        }
        Value::Object(_) => {
            serde_json::from_value(given.clone()).map_err(|_| bad("not {path, start, end, remote, hash}"))?
        }
        _ => return Err(bad("not a path or an object")),
    };
    let path = super::citations::safe_path(citation.path.trim())
        .ok_or_else(|| bad("path must be a safe repository-relative path"))?;
    let path = sanitize_title(&path, Limit::Chars(MAX_PATH_CHARS)).map_err(|_| bad("path too long"))?;
    if path.text.is_empty() {
        return Err(bad("no path"));
    }
    redactions.extend(path.redactions.iter().copied());
    citation.path = path.text;
    citation.end = citation.end.or(citation.start);
    match (citation.start, citation.end) {
        (Some(start), Some(end)) if start == 0 || end < start => return Err(bad("lines run from 1, start to end")),
        (None, Some(_)) => return Err(bad("an end needs a start")),
        _ => {}
    }
    if let Some(remote) = &citation.remote {
        citation.remote = Some(normalize_remote(remote).ok_or_else(|| bad("remote is no git remote"))?);
    }
    if let Some(hash) = &citation.hash {
        let hash = hash.trim().to_ascii_lowercase();
        if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(bad("hash must be the SHA-256 of the cited lines, in hex"));
        }
        citation.hash = Some(hash);
    }
    Ok(citation)
}

/// Memory `id`, which a new one is to supersede: live.
async fn superseded(items: &dyn Items, id: &ItemId) -> Result<MemoryItem, Answer> {
    let found = items.get(id).await.map_err(unavailable)?.and_then(MemoryItem::of);
    let found = found.ok_or_else(|| Answer::error("not_found", format!("No memory {id}.")))?;
    if !found.memory.status.live() {
        let by = found.memory.superseded_by.as_ref().map_or_else(String::new, |by| format!(" by {by}"));
        let status = json!(found.memory.status);
        let message = format!("Memory {id} is {}{by}: supersede the live one.", status.as_str().unwrap_or_default());
        return Err(Answer::error("conflict", message));
    }
    Ok(found)
}

pub(super) fn invalid(message: String) -> Answer {
    Answer::error("invalid", message)
}

pub(super) fn unavailable(e: StoreError) -> Answer {
    Answer::error("unavailable", format!("Memories are not available: {e}."))
}

fn duplicate(same: &MemoryItem) -> Answer {
    let (id, title) = (same.id(), &same.memory.title);
    let message = format!("Already remembered as {id} \"{title}\": vote on it with sense_vote, or supersede it.");
    let mut answer = Answer::error("duplicate", message);
    answer.json["id"] = json!(id);
    answer
}

fn near_duplicate(near: &[(MemoryItem, f64)]) -> Answer {
    let mut message = String::from("Near duplicates exist:");
    for (found, alike) in near {
        let _ = write!(message, "\n- {} \"{}\" ({:.0}% alike)", found.id(), found.memory.title, alike * 100.0);
    }
    message.push_str(
        "\nSupersede one (supersedes: <id>), vote on it with sense_vote, or pass force: true to store this anyway.",
    );
    let mut answer = Answer::error("near_duplicate", message);
    answer.json["memories"] = near
        .iter()
        .map(|(found, alike)| json!({"id": found.id(), "title": found.memory.title, "similarity": alike}))
        .collect();
    answer
}

fn core_cap(core: &[MemoryItem], total: usize) -> Answer {
    let mut message = format!(
        "Core memories would take {total} bytes, over the cap of {CORE_BUDGET}: trim one first, superseding it with a \
         shorter one or one not core:"
    );
    for found in core {
        let memory = &found.memory;
        let _ = write!(message, "\n- {} ({} bytes) \"{}\"", found.id(), memory.core_bytes(), memory.title);
    }
    let mut answer = Answer::error("core_cap", message);
    answer.json["total"] = json!(total);
    answer.json["cap"] = json!(CORE_BUDGET);
    answer.json["memories"] = core
        .iter()
        .map(|found| json!({"id": found.id(), "title": found.memory.title, "bytes": found.memory.core_bytes()}))
        .collect();
    answer
}

fn remembered(
    id: &ItemId,
    revision: u64,
    memory: &Memory,
    untrusted: Option<&str>,
    redactions: &[Redaction],
) -> Answer {
    let core = if memory.core { ", core" } else { "" };
    let mut text = format!(
        "Remembered {id} ({}, {}, importance {}{core}): {}",
        memory.kind, memory.scope, memory.importance, memory.title
    );
    if let Some(old) = &memory.supersedes {
        let _ = write!(text, "\nSupersedes {old}.");
    }
    if let Some(why) = untrusted {
        let _ = write!(text, "\nStored as untrusted: {why}.");
    }
    for warning in &memory.citation_warnings {
        let _ =
            write!(text, "\nWarning: {}: {}; marked needs_review.", memory.citations[warning.citation], warning.reason);
    }
    let mut redacted = Values::new();
    for redaction in redactions {
        let count = redacted.get(redaction.kind.name()).and_then(Value::as_u64).unwrap_or_default();
        redacted.insert(redaction.kind.name().into(), json!(count + redaction.count as u64));
    }
    if !redacted.is_empty() {
        let list: Vec<_> = redacted.iter().map(|(kind, count)| format!("{count} {kind}")).collect();
        let _ = write!(text, "\nRedacted: {}.", list.join(", "));
    }
    let json = json!({
        "id": id,
        "revision": revision,
        "type": memory.kind,
        "scope": memory.scope,
        "importance": memory.importance,
        "core": memory.core,
        "trust": memory.trust,
        "status": memory.status,
        "citations": memory.citations,
        "citation_warnings": memory.citation_warnings,
        "supersedes": memory.supersedes,
        "redactions": redacted,
    });
    Answer { text, json, is_error: false }
}
