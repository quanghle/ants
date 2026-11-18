//! The ant's skills (requirement ants-x39hk.6.7), in the Agent Skills layout: a skill is a directory named after it
//! holding `SKILL.md`, whose front matter gives its `name` and `description`, and the files it refers to. Each is one
//! item of kind [`SKILL`] named after it, its files in its values. Only `sense_skill` creates, changes or removes one,
//! through a proposal ([`crate::proposal`]) the ant confirms in a later dream; each revision keeps the reason of its
//! proposal.
//!
//! The confirmed skills are also files, `<home>/.sense/skills/<name>/`, which the ant's export carries
//! ([`crate::export`]): [`materialize`] makes them match the skills in the log on confirmation, at each brief and
//! before an export. Harnesses see the skills through the brief, which lists them by name and description, and read
//! one with `sense_skill`; the daemon also offers the directory to harnesses with an Agent Skills mechanism of their
//! own.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io;
use std::path::{Component, Path, PathBuf};

use ants_proto::UserId;
use ants_proto::provider::{BoxFuture, ToolCall};
use serde_json::{Map, Value, json};

use crate::Sense;
use crate::export::skills_dir;
use crate::items::{Item, ItemId, Values};
use crate::memory::{holds_recalled_marker, invalid, redacted};
use crate::proposal::{self, Proposal, ProposalItem, Status, time};
use crate::sanitize::{Limit, Redaction, sanitize_body};
use crate::store::{Items, StoreError};
use crate::tools::Answer;

/// The kind of a skill's item.
pub const SKILL: &str = "skill";
/// The file of a skill giving its name and description.
pub const SKILL_FILE: &str = "SKILL.md";
/// The most skills an ant keeps: the brief lists each.
pub const MAX_SKILLS: usize = 50;
/// The most characters of a skill's name.
pub const MAX_NAME_CHARS: usize = 64;
/// The most characters of a skill's description.
pub const MAX_DESCRIPTION_CHARS: usize = 1024;
/// The most bytes of `SKILL.md`, and of each other file of a skill.
pub const MAX_FILE_BYTES: usize = 16 * 1024;
/// The most files of a skill besides `SKILL.md`.
pub const MAX_FILES: usize = 8;
/// The most bytes of all the files of a skill: each version is kept in the log for good.
pub const MAX_SKILL_BYTES: usize = 32 * 1024;
/// The most characters of a file's path within its skill.
pub const MAX_PATH_CHARS: usize = 200;

/// The id of the item of skill `name`.
pub fn skill_id(name: &str) -> ItemId {
    ItemId::named(SKILL, name)
}

/// A confirmed skill, as its item holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skill {
    pub name: String,
    pub description: String,
    /// Its files by their path within the skill, `SKILL.md` among them.
    pub files: BTreeMap<String, String>,
    pub revision: u64,
}

impl Skill {
    /// The skill `item` holds; none for a skill removed.
    pub fn of(item: &Item) -> Option<Self> {
        if item.kind != SKILL || item.values.get("removed").and_then(Value::as_bool) == Some(true) {
            return None;
        }
        let files = files_of(&item.values);
        files.contains_key(SKILL_FILE).then(|| Self {
            name: item.text("name").to_owned(),
            description: item.text("description").to_owned(),
            files,
            revision: item.revision,
        })
    }
}

fn files_of(values: &Values) -> BTreeMap<String, String> {
    let files = values.get("files").and_then(Value::as_object).into_iter().flatten();
    files.filter_map(|(path, text)| Some((path.clone(), text.as_str()?.to_owned()))).collect()
}

/// The confirmed skills in `items`, by name.
pub async fn skills(items: &dyn Items) -> Result<Vec<Skill>, StoreError> {
    let mut skills: Vec<_> = items.list(SKILL, usize::MAX).await?.iter().filter_map(Skill::of).collect();
    skills.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(skills)
}

/// Whether `name` is a skill's name as Agent Skills has it: 1 to [`MAX_NAME_CHARS`] lowercase letters, digits and
/// hyphens, neither starting nor ending with a hyphen nor holding two in a row.
pub fn valid_name(name: &str) -> bool {
    (1..=MAX_NAME_CHARS).contains(&name.len())
        && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !name.starts_with('-')
        && !name.ends_with('-')
        && !name.contains("--")
}

/// Whether `path` names a file within a skill: relative, `/`-separated, of plain names, other than `SKILL.md`.
pub fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && path.chars().count() <= MAX_PATH_CHARS
        && path != SKILL_FILE
        && !path.contains(['\\', '\0'])
        && path.split('/').all(|part| !part.is_empty())
        && Path::new(path).components().all(|component| matches!(component, Component::Normal(_)))
}

/// The `name` and `description` of the front matter `content` opens with: lines `key: value` between two lines `---`,
/// a value quoted or folded over the indented lines after it (`>` or `|`). Why not, when it gives none.
pub fn front_matter(content: &str) -> Result<(String, String), String> {
    let mut lines = content.lines();
    if lines.next().map(str::trim_end) != Some("---") {
        return Err("SKILL.md opens with front matter: a line ---, name: and description: lines, then ---".into());
    }
    let mut block = Vec::new();
    loop {
        match lines.next() {
            Some(line) if line.trim_end() == "---" => break,
            Some(line) => block.push(line),
            None => return Err("the front matter of SKILL.md does not end with a line ---".into()),
        }
    }
    let mut values: BTreeMap<&str, String> = BTreeMap::new();
    let mut at = 0;
    while at < block.len() {
        let line = block[at];
        at += 1;
        if line.starts_with([' ', '\t']) || line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once(':') else { continue };
        let value = value.trim();
        let value = if value.is_empty() || matches!(value, ">" | "|" | ">-" | "|-" | ">+" | "|+") {
            let mut folded = Vec::new();
            while at < block.len() && (block[at].starts_with([' ', '\t']) || block[at].trim().is_empty()) {
                folded.push(block[at].trim());
                at += 1;
            }
            folded.into_iter().filter(|part| !part.is_empty()).collect::<Vec<_>>().join(" ")
        } else {
            unquote(value).to_owned()
        };
        values.insert(key.trim(), value);
    }
    let name = values.remove("name").filter(|name| !name.is_empty());
    let name = name.ok_or("the front matter of SKILL.md gives no name")?;
    let description = values.remove("description").filter(|description| !description.is_empty());
    let description = description.ok_or("the front matter of SKILL.md gives no description")?;
    Ok((name, description))
}

fn unquote(value: &str) -> &str {
    for quote in ['"', '\''] {
        if value.len() >= 2 && value.starts_with(quote) && value.ends_with(quote) {
            return &value[1..value.len() - 1];
        }
    }
    value
}

/// Make the skill files of `home` match `skills`: each in `<home>/.sense/skills/<name>/`, files changed written
/// again, anything else there removed.
pub fn materialize(skills: &[Skill], home: &Path) -> io::Result<()> {
    let root = skills_dir(home);
    let wanted: BTreeMap<PathBuf, &str> = skills
        .iter()
        .flat_map(|skill| skill.files.iter().map(|(path, text)| (Path::new(&skill.name).join(path), text.as_str())))
        .collect();
    prune(&root, Path::new(""), &wanted)?;
    for (path, text) in &wanted {
        let file = root.join(path);
        if std::fs::read(&file).ok().as_deref() != Some(text.as_bytes()) {
            std::fs::create_dir_all(file.parent().expect("a file in a directory"))?;
            std::fs::write(&file, text)?;
        }
    }
    Ok(())
}

/// Remove from directory `relative` of `root` what `wanted` does not hold, symbolic links not followed.
fn prune(root: &Path, relative: &Path, wanted: &BTreeMap<PathBuf, &str>) -> io::Result<()> {
    let entries = match std::fs::read_dir(root.join(relative)) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        let path = relative.join(entry.file_name());
        let kind = entry.file_type()?;
        if kind.is_dir() {
            if wanted.keys().any(|file| file != &path && file.starts_with(&path)) {
                prune(root, &path, wanted)?;
            } else {
                std::fs::remove_dir_all(entry.path())?;
            }
        } else if !kind.is_file() || !wanted.contains_key(&path) {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

/// [`materialize`] the confirmed skills of `ant` in its home, when sense knows it.
pub async fn materialize_for(sense: &Sense, ant: &UserId) -> io::Result<()> {
    let Some(home) = sense.home(ant) else { return Ok(()) };
    let items = sense.items(ant).await.map_err(io::Error::other)?;
    let skills = skills(&*items).await.map_err(io::Error::other)?;
    materialize(&skills, &home)
}

/// Answers a `sense_skill` call: without a name, list the skills and the pending proposals; with `proposal`, show one
/// in full; with a name alone, show the skill, one of its files (`path`) or every version (`history`); with `content`,
/// `files` or `remove` and a reason, propose the skill they make with the current one.
pub fn skill<'a>(sense: &'a Sense, call: &'a ToolCall) -> BoxFuture<'a, Answer> {
    Box::pin(async move { answer(sense, call).await.unwrap_or_else(|refused| refused) })
}

async fn answer(sense: &Sense, call: &ToolCall) -> Result<Answer, Answer> {
    let arguments = &call.arguments;
    let items = sense.items(&call.context.ant).await.map_err(proposal::unavailable)?;
    let path = text(arguments, "path")?;
    if let Some(id) = text(arguments, "proposal")? {
        let id = id.parse::<ItemId>().map_err(|_| invalid("proposal must be a proposal's id.".into()))?;
        return show_proposal(&*items, &id, path).await;
    }
    let Some(name) = text(arguments, "name")? else {
        if ["content", "files", "remove", "path", "history"].iter().any(|key| given(arguments, key)) {
            return Err(invalid("Name the skill.".into()));
        }
        return list(&*items).await;
    };
    if !valid_name(name) {
        return Err(invalid(format!(
            "{name:?} is no skill name: 1 to {MAX_NAME_CHARS} lowercase letters, digits and single hyphens inside."
        )));
    }
    if !["content", "files", "remove"].iter().any(|key| given(arguments, key)) {
        if flag(arguments, "history")? {
            return show_history(&*items, name).await;
        }
        return show(&*items, name, path).await;
    }
    if path.is_some() || given(arguments, "history") {
        return Err(invalid("path and history show a skill: pass them without content, files or remove.".into()));
    }
    propose(&*items, call, name).await
}

/// Propose the skill `name` as `call` gives it.
async fn propose(items: &dyn Items, call: &ToolCall, name: &str) -> Result<Answer, Answer> {
    let (arguments, context) = (&call.arguments, &call.context);
    let reason = proposal::reason(arguments)?;
    let finding = proposal::finding(arguments)?;
    proposal::refuse_untrusted(items, context, finding.as_deref()).await?;
    let id = skill_id(name);
    let current = items.get(&id).await.map_err(proposal::unavailable)?;
    let live = current.as_ref().and_then(Skill::of);
    let remove = flag(arguments, "remove")?;
    let mut redactions = Vec::new();
    let (mut version, summary) = if remove {
        if given(arguments, "content") || given(arguments, "files") {
            return Err(invalid("remove takes no content or files.".into()));
        }
        let Some(live) = &live else {
            return Err(Answer::error("not_found", format!("No skill {name} to remove.")));
        };
        let mut version = Values::new();
        version.insert("name".into(), json!(name));
        version.insert("removed".into(), json!(true));
        version.insert("description".into(), Value::Null);
        version.insert("files".into(), Value::Null);
        (version, format!("Remove skill {name}: {}", live.description))
    } else {
        edited(items, arguments, name, live.as_ref(), &mut redactions).await?
    };
    version.insert("reason".into(), json!(reason));
    let made = Proposal {
        target: id,
        kind: SKILL.into(),
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
    let (made, superseded) = proposal::propose(items, made).await.map_err(proposal::unavailable)?;
    let mut text = format!(
        "Proposed skill change {}: a dream from {} offers it, applied confirming it.",
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

/// The version of skill `name`, now `live`, and its summary, that `arguments` make with their content and files.
async fn edited(
    items: &dyn Items,
    arguments: &Map<String, Value>,
    name: &str,
    live: Option<&Skill>,
    redactions: &mut Vec<Redaction>,
) -> Result<(Values, String), Answer> {
    let id = skill_id(name);
    let mut files = live.map_or_else(BTreeMap::new, |live| live.files.clone());
    let mut changes = BTreeMap::new();
    if let Some(content) = text(arguments, "content")? {
        let content = file(SKILL_FILE, content, redactions)?;
        changes.insert(SKILL_FILE.to_owned(), Some(content));
    }
    match arguments.get("files") {
        None | Some(Value::Null) => {}
        Some(Value::Object(given)) => {
            for (path, text) in given {
                if !valid_path(path) {
                    return Err(invalid(format!(
                        "File {path:?}: a relative path of plain names, at most {MAX_PATH_CHARS} characters; \
                         SKILL.md comes as content."
                    )));
                }
                let text = match text {
                    Value::Null => None,
                    Value::String(text) => Some(file(path, text, redactions)?),
                    _ => return Err(invalid(format!("File {path}: its text, or null to remove it."))),
                };
                changes.insert(path.clone(), text);
            }
        }
        Some(_) => return Err(invalid("files maps paths to their text, or to null to remove one.".into())),
    }
    for (path, text) in &changes {
        match text {
            Some(text) => files.insert(path.clone(), text.clone()),
            None => files.remove(path),
        };
    }
    let content = files
        .get(SKILL_FILE)
        .ok_or_else(|| invalid(format!("A new skill comes with its content, its {SKILL_FILE}.")))?;
    let (named, description) = front_matter(content).map_err(|why| invalid(format!("{why}.")))?;
    if named != name {
        return Err(invalid(format!("The front matter names the skill {named:?}, not {name:?}.")));
    }
    if description.contains('\n') || !(1..=MAX_DESCRIPTION_CHARS).contains(&description.chars().count()) {
        return Err(invalid(format!("The description is one line of at most {MAX_DESCRIPTION_CHARS} characters.")));
    }
    if files.len() > MAX_FILES + 1 {
        return Err(Answer::error(
            "too_long",
            format!("A skill holds at most {MAX_FILES} files besides {SKILL_FILE}."),
        ));
    }
    let bytes: usize = files.values().map(String::len).sum();
    if bytes > MAX_SKILL_BYTES {
        let message = format!("The skill's files hold {bytes} bytes, over the limit of {MAX_SKILL_BYTES}.");
        return Err(Answer::error("too_long", message));
    }
    if live.is_some_and(|live| live.files == files) {
        return Err(Answer::error("unchanged", format!("Skill {name} is that already: nothing to propose.")));
    }
    if live.is_none() {
        let count = skills(items).await.map_err(proposal::unavailable)?.len();
        let new = proposal::pending(items).await.map_err(proposal::unavailable)?;
        let new = new.iter().filter(|found| found.proposal.kind == SKILL && found.proposal.target != id).count();
        if count + new >= MAX_SKILLS {
            let message =
                format!("{count} skills and {new} proposed: an ant keeps at most {MAX_SKILLS}; remove one first.");
            return Err(Answer::error("too_many", message));
        }
    }
    let summary = summary(name, &description, live, &files);
    let mut version = Values::new();
    version.insert("name".into(), json!(name));
    version.insert("description".into(), json!(description));
    version.insert("files".into(), json!(files));
    version.insert("removed".into(), Value::Null);
    Ok((version, summary))
}

/// What a proposal of skill `name` changes, from `live`, to make it `files`.
fn summary(name: &str, description: &str, live: Option<&Skill>, files: &BTreeMap<String, String>) -> String {
    let before = live.map(|live| &live.files);
    let mut changed: Vec<String> = files
        .iter()
        .filter_map(|(path, text)| match before.and_then(|before| before.get(path)) {
            None => Some(format!("{path} (new)")),
            Some(earlier) if earlier != text => Some(format!("{path} (changed)")),
            Some(_) => None,
        })
        .collect();
    changed.extend(
        before
            .into_iter()
            .flatten()
            .filter(|(path, _)| !files.contains_key(*path))
            .map(|(path, _)| format!("{path} (removed)")),
    );
    let what = if live.is_some() { "Change" } else { "New" };
    format!("{what} skill {name}: {description}\nFiles: {}.", changed.join(", "))
}

/// The text of file `path` given as `text`, sanitized.
fn file(path: &str, text: &str, redactions: &mut Vec<Redaction>) -> Result<String, Answer> {
    let sanitized = sanitize_body(text, Limit::Bytes(MAX_FILE_BYTES)).map_err(|refused| {
        Answer::error(
            "too_long",
            format!("File {path} has {} bytes, over the limit of {MAX_FILE_BYTES}.", refused.size),
        )
    })?;
    if holds_recalled_marker(&sanitized.text) {
        let message = format!("File {path} quotes recalled memories: write the skill in new words.");
        return Err(Answer::error("recalled_block", message));
    }
    redactions.extend(sanitized.redactions.iter().copied());
    Ok(sanitized.text)
}

/// The skills, by name and description, then the pending proposals of skills.
async fn list(items: &dyn Items) -> Result<Answer, Answer> {
    let skills = skills(items).await.map_err(proposal::unavailable)?;
    let pending = proposal::pending(items).await.map_err(proposal::unavailable)?;
    let pending: Vec<_> = pending.into_iter().filter(|found| found.proposal.kind == SKILL).collect();
    let mut text = if skills.is_empty() {
        "No skills yet: propose one with sense_skill, giving its name, its content (SKILL.md) and a reason.".to_owned()
    } else {
        format!("{} skills; sense_skill name=<name> reads one:", skills.len())
    };
    for skill in &skills {
        let _ = write!(text, "\n- {}: {}", skill.name, skill.description);
    }
    if !pending.is_empty() {
        text.push_str("\nPending proposals:");
        for found in &pending {
            let _ = write!(
                text,
                "\n- {} {}",
                found.proposal.version.get("name").and_then(Value::as_str).unwrap_or_default(),
                found.line()
            );
        }
    }
    let json = json!({
        "skills": skills.iter().map(|skill| json!({"name": skill.name, "description": skill.description, "revision": skill.revision})).collect::<Vec<_>>(),
        "pending": pending.iter().map(ProposalItem::json).collect::<Vec<_>>(),
    });
    Ok(Answer { text, json, is_error: false })
}

/// Skill `name`: its `SKILL.md` and the paths of its other files, or file `path` alone; then its pending proposal.
async fn show(items: &dyn Items, name: &str, path: Option<&str>) -> Result<Answer, Answer> {
    let id = skill_id(name);
    let item = items.get(&id).await.map_err(proposal::unavailable)?;
    let skill = item.as_ref().and_then(Skill::of);
    let pending = proposal::pending_for(items, &id).await.map_err(proposal::unavailable)?;
    let Some(skill) = skill else {
        let mut message = format!("No skill {name}.");
        if let Some(pending) = &pending {
            let _ = write!(message, " {}; sense_skill proposal={} shows it.", pending.line(), pending.id());
        }
        return Err(Answer::error("not_found", message));
    };
    let item = item.expect("a skill's item");
    if let Some(path) = path {
        let text = skill
            .files
            .get(path)
            .ok_or_else(|| Answer::error("not_found", format!("Skill {name} has no file {path}.")))?;
        return Ok(Answer {
            text: text.clone(),
            json: json!({"name": name, "path": path, "text": text}),
            is_error: false,
        });
    }
    let others: Vec<_> = skill.files.keys().filter(|path| *path != SKILL_FILE).map(String::as_str).collect();
    let mut text = format!(
        "Skill {name}, revision {}, confirmed {}: {}",
        skill.revision,
        item.text("confirmed"),
        item.text("reason")
    );
    if !others.is_empty() {
        let _ = write!(text, "\nOther files (sense_skill name={name} path=<path> reads one): {}", others.join(", "));
    }
    let _ = write!(text, "\n\n{}", skill.files[SKILL_FILE]);
    if let Some(pending) = &pending {
        let _ = write!(text, "\n\n{}; sense_skill proposal={} shows it.", pending.line(), pending.id());
    }
    let mut json = Value::Object(item.values.clone());
    json["revision"] = json!(item.revision);
    json["pending"] = json!(pending.as_ref().map(ProposalItem::json));
    Ok(Answer { text, json, is_error: false })
}

/// Proposal `id` of a skill in full: what it changes, its reason and its `SKILL.md`, or its file `path`.
async fn show_proposal(items: &dyn Items, id: &ItemId, path: Option<&str>) -> Result<Answer, Answer> {
    let found = items.get(id).await.map_err(proposal::unavailable)?.and_then(ProposalItem::of);
    let found = found
        .filter(|found| found.proposal.kind == SKILL)
        .ok_or_else(|| Answer::error("not_found", format!("No skill proposal {id}.")))?;
    let files = files_of(&found.proposal.version);
    if let Some(path) = path {
        let text =
            files.get(path).ok_or_else(|| Answer::error("not_found", format!("Proposal {id} has no file {path}.")))?;
        return Ok(Answer {
            text: text.clone(),
            json: json!({"proposal": id, "path": path, "text": text}),
            is_error: false,
        });
    }
    let mut text = format!("{}\n{}", found.line(), found.proposal.summary);
    if let Some(content) = files.get(SKILL_FILE) {
        let _ = write!(text, "\n\n{content}");
    }
    Ok(Answer { text, json: found.json(), is_error: false })
}

/// Every version of skill `name`, the newest first, each with the reason it was confirmed for.
async fn show_history(items: &dyn Items, name: &str) -> Result<Answer, Answer> {
    let versions = proposal::versions(items, &skill_id(name)).await.map_err(proposal::unavailable)?;
    if versions.is_empty() {
        return Err(Answer::error("not_found", format!("No skill {name}.")));
    }
    let mut text = format!("{} versions of skill {name}, the newest first:", versions.len());
    let mut json = Vec::new();
    for (revision, at, values) in versions.iter().rev() {
        let reason = values.get("reason").and_then(Value::as_str).unwrap_or_default();
        let what = if values.get("removed").and_then(Value::as_bool) == Some(true) {
            "removed".to_owned()
        } else {
            let files: Vec<_> = files_of(values).into_keys().collect();
            format!(
                "{}; files {}",
                values.get("description").and_then(Value::as_str).unwrap_or_default(),
                files.join(", ")
            )
        };
        let _ = write!(text, "\n- Revision {revision}, {}: {reason}\n  {what}", time(*at));
        let mut version = Value::Object(values.clone());
        version["revision"] = json!(revision);
        json.push(version);
    }
    Ok(Answer { text, json: json!({"versions": json}), is_error: false })
}

fn given(arguments: &Map<String, Value>, key: &str) -> bool {
    arguments.get(key).is_some_and(|value| !value.is_null())
}

fn text<'a>(arguments: &'a Map<String, Value>, name: &str) -> Result<Option<&'a str>, Answer> {
    match arguments.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.trim()).filter(|text| !text.is_empty())),
        Some(_) => Err(invalid(format!("{name} must be a string."))),
    }
}

fn flag(arguments: &Map<String, Value>, name: &str) -> Result<bool, Answer> {
    match arguments.get(name) {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(flag)) => Ok(*flag),
        Some(_) => Err(invalid(format!("{name} must be true or false."))),
    }
}

#[cfg(test)]
mod tests;
