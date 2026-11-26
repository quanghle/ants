//! [`AdminOp::ReadMemories`]: read the ant's memories ([`Memories`]) for the outcome's `result`.
//!
//! - Core memories ([`core_memories`]): the `whoami` and `memory` of the ant's shared settings ([`crate::profile::shared`]),
//!   else of its `[[ant]]` table in the config file the daemon serves while it has none, and the `tasks` of that
//!   table, which a set order edits.
//! - Harness memories: the files matching the `memory_files` globs of the ant's harness, relative to its workdir or
//!   under `~`, and the bd memories of the database `BD_DB` names in its environment (its `env`, its harness's, the
//!   daemon's), as `bd memories --json` lists them.
//! - Provider memories: the memories view of the ant's provider ([`crate::provider::Providers::of`]), when it serves
//!   one and the order names the ant's identity, read after the core memories, so they come before the harness's in the
//!   caps, each entry with its typed fields ([`ProviderMemory`]), which count within them. A provider failing or slower
//!   than [`PROVIDER_TIMEOUT`] is noted in [`ProviderMemories::error`], not a refusal.
//!
//! A file is read only if, its symbolic links resolved, it still matches one of the globs, their own links resolved,
//! and lies under the ant's workdir, its home or the home directory: a link out of those paths is named in
//! [`Memories::skipped`], not followed. `**` walks no linked directory. Each text is cut at [`MEMORY_FILE_CAP`] bytes,
//! and all of them at [`MEMORIES_CAP`] bytes of JSON, as the outcome stays in a KV bucket.
//!
//! The daemon reads memories only while online: an order that waited longer than [`MEMORIES_FRESH`] in the stream,
//! while the machine's daemon was away, is refused as stale rather than answered when no one waits for it any more.

use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::io::Read as _;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

#[cfg(doc)]
use ants_proto::AdminOp;
use ants_proto::provider::{AntRef, Capability, Connected, MemoryEntry, MemoryFields};
use ants_proto::{
    AdminOrder, AdminOutcome, BdMemories, BdMemory, CoreMemories, MEMORIES_CAP, MEMORIES_FRESH, MEMORY_FILE_CAP,
    Memories, MemoryFile, ProviderMemories, ProviderMemory, Skipped,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use super::{NO_CONFIG, Orders, applied, refused};
use crate::config::{self, Ant};
use crate::harness::Harness;

/// The most memory files read.
const MAX_FILES: usize = 64;
/// The most directory entries the globs walk.
const MAX_ENTRIES: usize = 10_000;
/// How long `bd memories` may take.
const BD_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the ant's provider may take to show its memories.
pub(super) const PROVIDER_TIMEOUT: Duration = Duration::from_secs(10);
/// The most characters of a provider's error shown.
const MAX_ERROR: usize = 300;

const OVER_CAP: &str = "over the size cap of a memories read";
const OUTSIDE: &str = "links outside the memory files configured and the ant's workdir and home";

pub(super) async fn apply(
    orders: &Orders,
    order: &AdminOrder,
    identity: Option<&str>,
    stored_at: DateTime<Utc>,
) -> AdminOutcome {
    if let Some(stale) = stale(stored_at, "reads memories") {
        return refused(order, &stale);
    }
    let Some(file) = &orders.config else { return refused(order, NO_CONFIG) };
    let loaded = match config::load(&file.path) {
        Ok(loaded) => loaded,
        Err(e) => return refused(order, &format!("{e:#}")),
    };
    let Some(ant) = loaded.ants.iter().find(|ant| ant.name == order.ant) else {
        return refused(order, &format!("{} lists no ant {}", file.path.display(), order.ant));
    };
    let ant = file.shared.overlaid(ant).map_or_else(|| ant.clone(), |(overlaid, _)| overlaid);
    let harness = ant.harness.as_deref().and_then(|name| loaded.harnesses.get(name).ok());
    let Some(env) = Env::current() else { return refused(order, "no home directory") };
    let provider = orders.providers.of(&order.ant).await;
    let provider = provider.as_ref().map(|connected| Of { connected, identity });
    let memories = read(&ant, harness.as_deref(), &env, provider).await;
    match serde_json::to_value(&memories) {
        Ok(result) => applied(order, Some(result)),
        Err(e) => refused(order, &format!("writing the memories read: {e}")),
    }
}

/// Why an order the stream stored at `stored_at` is refused as stale, its daemon doing `what` only while online.
pub(super) fn stale(stored_at: DateTime<Utc>, what: &str) -> Option<String> {
    let waited = Utc::now() - stored_at;
    (waited > MEMORIES_FRESH).then(|| {
        format!(
            "stale: it waited {}s for the machine's daemon, which {what} only while online; ask again",
            waited.num_seconds()
        )
    })
}

/// The provider of an ant, and the id of the ant's identity record, which the order names.
#[derive(Clone, Copy)]
pub(crate) struct Of<'a> {
    pub connected: &'a Connected,
    pub identity: Option<&'a str>,
}

/// A provider's error, or a text of its memories view, cut short for an outcome.
pub(super) fn cut_error(e: impl std::fmt::Display) -> String {
    e.to_string().chars().take(MAX_ERROR).collect()
}

/// The core memories of `ant`, as served: overlaid with its shared settings ([`crate::profile::shared::Shared::overlaid`]).
pub fn core_memories(ant: &Ant) -> CoreMemories {
    CoreMemories { whoami: ant.whoami.clone(), memory: ant.memory.clone(), tasks: ant.tasks.clone() }
}

/// What a read of memories takes from the daemon's environment.
#[derive(Debug, Clone)]
pub(crate) struct Env {
    /// The home directory, which `~` stands for.
    pub home: PathBuf,
    /// The bd command.
    pub bd: OsString,
    /// The daemon's own `BD_DB`, which the ant's harness inherits.
    pub bd_db: Option<String>,
}

impl Env {
    pub(crate) fn current() -> Option<Self> {
        let bd_db = std::env::var("BD_DB").ok();
        Some(Self { home: std::env::home_dir()?, bd: "bd".into(), bd_db })
    }
}

/// The memories of `ant`, whose harness is `harness` and whose provider `provider`.
pub(crate) async fn read(ant: &Ant, harness: Option<&Harness>, env: &Env, provider: Option<Of<'_>>) -> Memories {
    let mut budget = Budget(MEMORIES_CAP);
    let mut skipped = Vec::new();
    let core = core_memories(ant);
    let mut cap = |what: &str, text: Option<String>| {
        let (text, cut) = budget.take(&text?, MEMORY_FILE_CAP, false)?;
        if cut {
            skipped.push(Skipped { what: what.to_owned(), why: "cut short: over the size cap".to_owned() });
        }
        Some(text)
    };
    let core = CoreMemories {
        whoami: cap("whoami", core.whoami),
        memory: cap("memory", core.memory),
        tasks: cap("tasks", core.tasks),
    };
    let provider = match provider {
        Some(of) => provider_memories(&ant.name, of, &mut budget, &mut skipped).await,
        None => None,
    };
    let ant_home = ant.home.clone().unwrap_or_else(|| crate::config::default_home(&env.home, ant.name.as_str()));
    let globs = harness.map_or_default(Harness::memory_files);
    let files = files(globs, &ant.workdir, &ant_home, &env.home, &mut budget, &mut skipped);
    let db =
        ant.env.get("BD_DB").or_else(|| harness?.env.get("BD_DB")).or(env.bd_db.as_ref()).filter(|db| !db.is_empty());
    let bd = match db {
        Some(db) => Some(bd_memories(&env.bd, db, &ant.workdir, &mut budget, &mut skipped).await),
        None => None,
    };
    Memories {
        core,
        harness: harness.map(|h| h.name().to_owned()),
        workdir: Some(ant.workdir.display().to_string()),
        files,
        bd,
        provider,
        skipped,
    }
}

/// The harness memories of an ant whose workdir is `workdir` and home `ant_home`, within the caps of a memories read:
/// the files `globs` match, as [`read`] reads them, and the bd memories of the database `bd_db`, when named; with what
/// was left out, as sense's import of them takes them ([`crate::sense`]).
pub(crate) async fn harness(
    globs: &[String],
    workdir: &Path,
    ant_home: &Path,
    env: &Env,
    bd_db: Option<&str>,
) -> (Vec<MemoryFile>, Option<BdMemories>, Vec<Skipped>) {
    let mut budget = Budget(MEMORIES_CAP);
    let mut skipped = Vec::new();
    let files = files(globs, workdir, ant_home, &env.home, &mut budget, &mut skipped);
    let bd = match bd_db.filter(|db| !db.is_empty()) {
        Some(db) => Some(bd_memories(&env.bd, db, workdir, &mut budget, &mut skipped).await),
        None => None,
    };
    (files, bd, skipped)
}

/// The memories view of the provider `of`, of `ant`: none when it shows none.
async fn provider_memories(
    ant: &ants_proto::UserId,
    of: Of<'_>,
    budget: &mut Budget,
    skipped: &mut Vec<Skipped>,
) -> Option<ProviderMemories> {
    let Of { connected, identity } = of;
    if !connected.has(Capability::Memories) {
        return None;
    }
    let mut memories = ProviderMemories { provider: connected.name().to_owned(), ..ProviderMemories::default() };
    let Some(identity) = identity else {
        memories.error = Some("the order names no identity of the ant, which its provider knows it by".to_owned());
        return Some(memories);
    };
    let read = connected.memories(AntRef { ant: ant.clone(), identity: identity.to_owned() });
    let view = match tokio::time::timeout(PROVIDER_TIMEOUT, read).await {
        Err(_) => Err(format!("{} took over {}s", connected.name(), PROVIDER_TIMEOUT.as_secs())),
        Ok(Err(e)) => Err(cut_error(e)),
        Ok(Ok(view)) => Ok(view),
    };
    match view {
        Ok(view) => {
            for entry in view.memories {
                let key = cut_error(&entry.key);
                match provider_memory(entry, budget) {
                    Some(memory) => memories.memories.push(memory),
                    None => skipped.push(Skipped { what: format!("provider memory {key}"), why: OVER_CAP.to_owned() }),
                }
            }
        }
        Err(e) => memories.error = Some(e),
    }
    Some(memories)
}

/// `entry` of a provider's memories view as `budget` lets it be shown: its key and typed texts cut to [`MAX_ERROR`]
/// characters, its first trigger rows that fit, then the start of its content within [`MEMORY_FILE_CAP`] bytes, its
/// typed fields as JSON among them; none when nothing of it fits.
fn provider_memory(entry: MemoryEntry, budget: &mut Budget) -> Option<ProviderMemory> {
    let MemoryEntry { key, content, fields } = entry;
    let cut = |text: Option<String>| text.as_deref().map(cut_error);
    let mut fields = MemoryFields {
        section: cut(fields.section),
        id: cut(fields.id),
        kind: cut(fields.kind),
        scope: cut(fields.scope),
        trust: cut(fields.trust),
        title: cut(fields.title),
        ..fields
    };
    let mut truncated = false;
    while !fields.triggers.is_empty() && fields.size() > budget.0.min(MEMORY_FILE_CAP) {
        fields.triggers.pop();
        truncated = true;
    }
    let size = fields.size();
    if size > budget.0.min(MEMORY_FILE_CAP) {
        return None;
    }
    let before = budget.0;
    budget.0 -= size;
    let Some((content, cut_short)) = budget.take(&content, MEMORY_FILE_CAP - size, truncated) else {
        budget.0 = before;
        return None;
    };
    Some(ProviderMemory { key: cut_error(&key), content, fields, truncated: cut_short })
}

/// What is left of [`MEMORIES_CAP`]: bytes of the texts as JSON strings, and of a provider memory's typed fields as
/// JSON.
struct Budget(usize);

impl Budget {
    /// The start of `text` that fits in `cap` bytes, at most [`MEMORY_FILE_CAP`], and in what is left, which it takes,
    /// with whether it was cut short, or already `cut`; none when nothing is left.
    fn take(&mut self, text: &str, cap: usize, cut: bool) -> Option<(String, bool)> {
        if self.0 == 0 && !text.is_empty() {
            return None;
        }
        let (mut bytes, mut json) = (0, 2);
        let mut end = text.len();
        for (at, c) in text.char_indices() {
            let escaped = match c {
                '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
                c if c < ' ' => 6,
                c => c.len_utf8(),
            };
            if bytes + c.len_utf8() > cap.min(MEMORY_FILE_CAP) || json + escaped > self.0 {
                end = at;
                break;
            }
            bytes += c.len_utf8();
            json += escaped;
        }
        if end == 0 && !text.is_empty() {
            return None;
        }
        self.0 = self.0.saturating_sub(json);
        Some((text[..end].to_owned(), cut || end < text.len()))
    }
}

/// One `memory_files` glob: the directory it starts from, its links resolved, and the components after it, the first
/// holding a wildcard or naming the file.
struct Glob {
    base: PathBuf,
    rest: Vec<String>,
}

impl Glob {
    /// `pattern` relative to `workdir`, `~` standing for `home`; none when its base does not exist.
    fn of(pattern: &str, workdir: &Path, home: &Path) -> Option<Self> {
        let path = match pattern.strip_prefix('~') {
            Some(rest) if rest.is_empty() || rest.starts_with('/') => home.join(rest.trim_start_matches('/')),
            _ => workdir.join(pattern),
        };
        let mut base = PathBuf::new();
        let mut rest = Vec::new();
        for component in path.components() {
            match component {
                Component::CurDir => {}
                Component::ParentDir => return None,
                Component::Normal(name) if rest.is_empty() && !wild(&name.to_string_lossy()) => base.push(name),
                Component::Normal(name) => rest.push(name.to_str()?.to_owned()),
                other => base.push(other),
            }
        }
        // The file a glob names in full may be a link, which its match must not take as its own path.
        if rest.is_empty() {
            rest.push(base.file_name()?.to_str()?.to_owned());
            base.pop();
        }
        Some(Self { base: base.canonicalize().ok()?, rest })
    }

    /// Whether `path`, its links resolved, matches.
    fn matches(&self, path: &Path) -> bool {
        let Ok(rel) = path.strip_prefix(&self.base) else { return false };
        let rel: Option<Vec<&str>> = rel.components().map(|c| c.as_os_str().to_str()).collect();
        rel.is_some_and(|rel| components_match(&self.rest, &rel))
    }
}

fn wild(component: &str) -> bool {
    component.contains(['*', '?'])
}

/// Whether the path components `path` match the glob components `glob`, where `**` stands for any directories.
fn components_match(glob: &[String], path: &[&str]) -> bool {
    match glob.split_first() {
        None => path.is_empty(),
        Some((first, rest)) if first == "**" => (0..=path.len()).any(|skip| components_match(rest, &path[skip..])),
        Some((first, rest)) => {
            path.split_first().is_some_and(|(name, tail)| name_matches(first, name) && components_match(rest, tail))
        }
    }
}

/// Whether `name` matches `pattern`, where `*` stands for any characters and `?` for one.
fn name_matches(pattern: &str, name: &str) -> bool {
    let (p, t): (Vec<char>, Vec<char>) = (pattern.chars().collect(), name.chars().collect());
    let (mut pi, mut ti) = (0, 0);
    // Where the last `*` was, and the position in `name` it swallows up to.
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        if pi < p.len() && p[pi] == '*' {
            star = Some((pi, ti));
            pi += 1;
        } else if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if let Some((sp, st)) = star {
            (pi, ti) = (sp + 1, st + 1);
            star = Some((sp, st + 1));
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|c| *c == '*')
}

/// The paths under `dir` that `rest` matches, walking at most `entries` more directory entries.
fn expand(dir: &Path, rest: &[String], out: &mut Vec<PathBuf>, entries: &mut usize) {
    let Some((first, tail)) = rest.split_first() else {
        out.push(dir.to_owned());
        return;
    };
    if !wild(first) {
        expand(&dir.join(first), tail, out, entries);
        return;
    }
    let Ok(read) = std::fs::read_dir(dir) else { return };
    let mut children: Vec<(String, bool)> = Vec::new();
    for entry in read.flatten() {
        if *entries == 0 {
            break;
        }
        *entries -= 1;
        let Ok(name) = entry.file_name().into_string() else { continue };
        let is_dir = entry.file_type().is_ok_and(|t| t.is_dir());
        children.push((name, is_dir));
    }
    children.sort();
    if first == "**" {
        expand(dir, tail, out, entries);
        for (name, is_dir) in children {
            if is_dir {
                expand(&dir.join(name), rest, out, entries);
            }
        }
        return;
    }
    for (name, _) in children {
        if name_matches(first, &name) {
            expand(&dir.join(name), tail, out, entries);
        }
    }
}

/// The files under `root` that `globs` match, `root` standing for the workdir they are relative to, as paths relative to
/// `root` in name order: regular files that, their links resolved, still match a glob and lie under `root`. Globs under
/// `~` are left out, and so is what lies under `skip`, relative to `root`.
pub fn files_under(globs: &[String], root: &Path, skip: &[&str]) -> Vec<PathBuf> {
    let Ok(root) = root.canonicalize() else { return Vec::new() };
    let globs: Vec<Glob> = globs
        .iter()
        .filter(|pattern| !pattern.starts_with('~'))
        .filter_map(|pattern| Glob::of(pattern, &root, &root))
        .collect();
    let mut found = Vec::new();
    let mut entries = MAX_ENTRIES;
    for glob in &globs {
        expand(&glob.base, &glob.rest, &mut found, &mut entries);
    }
    let mut files = BTreeSet::new();
    for path in found {
        let Ok(real) = path.canonicalize() else { continue };
        let Ok(rel) = real.strip_prefix(&root) else { continue };
        if real.is_file() && globs.iter().any(|glob| glob.matches(&real)) && !skip.iter().any(|s| rel.starts_with(s)) {
            files.insert(rel.to_owned());
        }
    }
    files.into_iter().collect()
}

/// The memory files `globs` match, read: confined to the globs and to `workdir`, `ant_home` and `home`.
fn files(
    globs: &[String],
    workdir: &Path,
    ant_home: &Path,
    home: &Path,
    budget: &mut Budget,
    skipped: &mut Vec<Skipped>,
) -> Vec<MemoryFile> {
    let globs: Vec<Glob> = globs.iter().filter_map(|pattern| Glob::of(pattern, workdir, home)).collect();
    let roots: Vec<PathBuf> = [workdir, ant_home, home].iter().filter_map(|root| root.canonicalize().ok()).collect();
    let mut found = Vec::new();
    let mut entries = MAX_ENTRIES;
    for glob in &globs {
        expand(&glob.base, &glob.rest, &mut found, &mut entries);
    }
    let mut seen = BTreeSet::new();
    let mut files = Vec::new();
    for path in found {
        let Ok(real) = path.canonicalize() else { continue };
        if !real.is_file() || !seen.insert(real.clone()) {
            continue;
        }
        let what = path.display().to_string();
        if !roots.iter().any(|root| real.starts_with(root)) || !globs.iter().any(|glob| glob.matches(&real)) {
            skipped.push(Skipped { what, why: OUTSIDE.to_owned() });
            continue;
        }
        if files.len() == MAX_FILES {
            skipped.push(Skipped { what, why: format!("over the {MAX_FILES} files a memories read shows") });
            continue;
        }
        match read_file(&real, budget) {
            Ok(Some(file)) => files.push(file),
            Ok(None) => skipped.push(Skipped { what, why: OVER_CAP.to_owned() }),
            Err(e) => skipped.push(Skipped { what, why: format!("reading it: {e}") }),
        }
    }
    files
}

/// The start of the file at `path`, at most [`MEMORY_FILE_CAP`] bytes, as `budget` lets it be shown.
fn read_file(path: &Path, budget: &mut Budget) -> std::io::Result<Option<MemoryFile>> {
    let file = std::fs::File::open(path)?;
    let size = file.metadata()?.len();
    let mut bytes = Vec::new();
    file.take(MEMORY_FILE_CAP as u64).read_to_end(&mut bytes)?;
    let text = match String::from_utf8(bytes) {
        Ok(text) => text,
        // Cut inside a character: drop its start.
        Err(e) if e.utf8_error().error_len().is_none() => {
            let valid = e.utf8_error().valid_up_to();
            String::from_utf8_lossy(&e.into_bytes()[..valid]).into_owned()
        }
        Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
    };
    let cut = (text.len() as u64) < size;
    Ok(budget.take(&text, MEMORY_FILE_CAP, cut).map(|(text, truncated)| MemoryFile {
        path: path.display().to_string(),
        size,
        text,
        truncated,
    }))
}

/// The bd memories of the database `db`, as `bd memories --json` run with the command `bd` in `workdir` lists them.
async fn bd_memories(
    bd: &OsStr,
    db: &str,
    workdir: &Path,
    budget: &mut Budget,
    skipped: &mut Vec<Skipped>,
) -> BdMemories {
    #[derive(Deserialize)]
    struct Listed {
        key: String,
        content: String,
    }
    let mut memories = BdMemories { db: db.to_owned(), ..BdMemories::default() };
    let mut command = tokio::process::Command::new(bd);
    command.args(["memories", "--json"]).env("BD_DB", db).current_dir(workdir).stdin(Stdio::null()).kill_on_drop(true);
    let run = crate::process::output(command);
    let listed = match tokio::time::timeout(BD_TIMEOUT, run).await {
        Err(_) => Err(format!("bd memories took over {}s", BD_TIMEOUT.as_secs())),
        Ok(Err(e)) => Err(format!("running bd: {e}")),
        Ok(Ok(out)) if !out.status.success() => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            let why = stderr.lines().find(|l| !l.trim().is_empty()).unwrap_or("no message");
            let why: String = why.chars().take(300).collect();
            Err(format!("bd memories failed ({}): {why}", out.status))
        }
        Ok(Ok(out)) => serde_json::from_slice::<Vec<Listed>>(&out.stdout)
            .map_err(|e| format!("reading what bd memories listed: {e}")),
    };
    match listed {
        Ok(listed) => {
            for Listed { key, content } in listed {
                match budget.take(&content, MEMORY_FILE_CAP, false) {
                    Some((content, truncated)) => memories.memories.push(BdMemory { key, content, truncated }),
                    None => skipped.push(Skipped { what: format!("bd memory {key}"), why: OVER_CAP.to_owned() }),
                }
            }
        }
        Err(e) => memories.error = Some(e),
    }
    memories
}

#[cfg(test)]
pub(super) mod tests;
