//! [`AdminOp::Clone`]: add an ant under a new name, a copy of the order's ant with its home, sessions and thread
//! history, to the config file the daemon serves. The ant cloned is paused (unless it was already), and the daemon
//! serving its inbox reads a [`Snapshot`] once its last prompt ended; then its home is copied (no worktrees, no thread
//! history, no answered or buried messages), each thread's history written into the clone's home
//! ([`crate::history::cloned`]), each worktree added again on the branch `ants/<new>/<thread>` from the same commit,
//! with its uncommitted changes and untracked files, the clone's key signed by `ants signer` on the order, each session
//! forked when the harness can (else marked to start over, [`crate::history::Rebuild::Cloned`]) and stored under the
//! clone's keys, and its identity record written: a new id and birth, the prompts of the ant cloned and where it came
//! from ([`ants_proto::ClonedFrom`]). Where the hub keeps shared settings ([`crate::shared`]), the clone gets its own
//! record of them: those of the ant cloned, with the order's settings over them. Its `[[ant]]` table, a copy of the
//! cloned ant's with its shared settings and the order's settings over it, goes last, which starts it on the reload,
//! and the ant cloned resumes. A failure undoes what was done, the signed
//! key aside, which no one holds once the credentials are deleted. A comment above the table names the order, so that
//! the order taken again after the table was written is applied without cloning twice.
//!
//! Not cloned: the messages queued for the ant, its channel memberships, references and spend counters.

use std::path::{Path, PathBuf};
use std::time::Duration;

#[cfg(doc)]
use ants_proto::AdminOp;
use ants_proto::{
    AdminOrder, AdminOutcome, AntIdentity, ClonedFrom, SharedSettings, ThreadId, UserId, is_shared_field,
};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream;
use chrono::Utc;
use serde_json::{Map, Value, json};

use nkeys::KeyPair;

use super::add::{NOT_GIVEN, Renewal, entry, entry_text, key_for, leaf_uplink, renew, sign, write_secret};
use super::ask::Uplink;
use super::{ConfigFile, LeafNode, NO_CONFIG, Orders, applied, refused};
use crate::clone::{Snapshot, ThreadCopy};
use crate::config::{self, edit::Edit};
use crate::direct_kv::DirectKv;
use crate::identity::{self, Identity};
use crate::pool::{SessionStore, Stored};
use crate::shared::{self, Bucket, Found};
use crate::{bus, creds, harness};

/// How long the daemon serving the ant waits for its prompts to end and reads its snapshot: past the default
/// `prompt_timeout`.
const SNAPSHOT_WAIT: Duration = Duration::from_mins(35);
/// Longest a git command of a worktree copy may take.
const GIT_WAIT: Duration = Duration::from_mins(2);
/// What of a home a clone does not copy: its worktrees and history are made again, its answered and buried messages
/// and its MCP endpoint are its own.
const NOT_COPIED: [&str; 6] = ["worktrees", "history", "answered", "buried", "mcp.json", "mcp.sock"];
/// Keys of the ant's table a clone does not copy: those naming it or set for it alone.
const NOT_CLONED: [&str; 8] = ["name", "creds", "paused", "added_by", "template", "builtin", "home", "allow_sets"];

pub(super) async fn apply(
    js: &jetstream::Context,
    bucket: Bucket<'_>,
    orders: &Orders,
    order: &AdminOrder,
    new: &UserId,
    settings: &Map<String, Value>,
) -> AdminOutcome {
    let Some(config) = &orders.config else { return refused(order, NO_CONFIG) };
    let Some(host_creds) = &orders.creds else {
        return refused(order, "the machine has no host credentials to ask `ants signer` with");
    };
    let mut done = Done::default();
    let cloning = Cloning { js, bucket, orders, config, host_creds, order, new, settings };
    let result = cloning.run(&mut done).await;
    let (paused, key) = (done.paused, done.key.take());
    if result.is_err() {
        done.undo(order).await;
    }
    if paused {
        resume(config, &order.ant);
    }
    match result {
        Ok(None) => applied(order, None),
        Ok(Some(result)) => {
            config.edited.notify_one();
            applied(order, Some(result))
        }
        Err(e) => match key {
            Some(key) => refused(
                order,
                &format!(
                    "{e:#}; all undone, but the clone's key {key} that `ants signer` signed stays valid, held by no \
                     one, until `ants auth revoke --key {key}` where the account key is"
                ),
            ),
            None => refused(order, &format!("{e:#}; all undone")),
        },
    }
}

/// What a clone did so far, to undo on a failure.
#[derive(Default)]
struct Done {
    /// The ant cloned was paused by the clone, to resume.
    paused: bool,
    home: Option<PathBuf>,
    /// The worktrees added: the repository's git directory, the worktree and its branch.
    worktrees: Vec<(PathBuf, PathBuf, String)>,
    /// The credentials and key files written beside the host credentials.
    files: Vec<PathBuf>,
    /// The key `ants signer` signed.
    key: Option<String>,
    sessions: Option<(SessionStore, Vec<ThreadId>)>,
    identity: Option<Identity>,
    /// The bucket the clone's shared settings were written to.
    shared: Option<(DirectKv, String)>,
}

impl Done {
    async fn undo(self, order: &AdminOrder) {
        let warn = |what: &str, e: anyhow::Error| {
            tracing::warn!(order = %order.id, error = format!("{e:#}"), "undoing a clone: {what}");
        };
        if let Some((kv, key)) = self.shared
            && let Err(e) = kv.delete(&key).await
        {
            warn("deleting the shared settings", e);
        }
        if let Some(identity) = self.identity
            && let Err(e) = identity.delete().await
        {
            warn("deleting the identity record", e);
        }
        if let Some((store, threads)) = self.sessions {
            for thread in threads {
                if let Err(e) = store.delete(&thread).await {
                    warn("deleting a session", e);
                }
            }
        }
        for file in self.files {
            if let Err(e) = std::fs::remove_file(&file)
                && e.kind() != std::io::ErrorKind::NotFound
            {
                warn("removing a file", e.into());
            }
        }
        for (repo, tree, branch) in self.worktrees.into_iter().rev() {
            let tree = tree.to_string_lossy().into_owned();
            if let Err(e) = git(&repo, &["worktree", "remove", "--force", &tree]).await {
                warn("removing a worktree", e);
            }
            if let Err(e) = git(&repo, &["branch", "-D", &branch]).await {
                warn("deleting a branch", e);
            }
        }
        if let Some(home) = self.home
            && let Err(e) = std::fs::remove_dir_all(&home)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            warn("removing the home", e.into());
        }
    }
}

/// The comment above the table of the clone `order` makes.
fn mark(order: &AdminOrder) -> String {
    format!("# cloned from {} by {}'s order {}", order.ant, order.admin, order.id)
}

/// A clone checked before anything changes.
struct Plan {
    /// The `[[ant]]` table of the clone.
    table: String,
    workdir: PathBuf,
    src_home: PathBuf,
    new_home: PathBuf,
    /// The directory of the host credentials, where the clone's go.
    dir: PathBuf,
    creds_path: PathBuf,
    /// The clone's shared settings, where the hub keeps them.
    shared: Option<Map<String, Value>>,
}

/// Check the clone of the order's ant as `new` with the config at `path`: none when the config shows it done already.
fn plan(
    path: &Path,
    host_creds: &Path,
    order: &AdminOrder,
    new: &UserId,
    settings: &Map<String, Value>,
    src_shared: Option<Option<Map<String, Value>>>,
) -> Result<Option<Plan>> {
    let src = &order.ant;
    let mark = mark(order);
    let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
    if text.lines().any(|line| line.trim() == mark) {
        return Ok(None);
    }
    if new.is_human() {
        bail!("{new} is a human's name, not an ant's");
    }
    if new == src {
        bail!("a clone needs another name than {src}");
    }
    if let Some(key) = NOT_GIVEN.iter().find(|key| settings.contains_key(**key)) {
        bail!("the daemon sets {key} itself: an order does not give it");
    }
    if settings.get("name").is_some_and(|name| name.as_str() != Some(new.as_str())) {
        bail!("the settings name another ant than {new}");
    }
    let loaded = config::load(path)?;
    if loaded.ants.iter().any(|a| a.name == *new) {
        bail!("config {} lists {new} already", path.display());
    }
    let ant = loaded
        .ants
        .iter()
        .find(|a| a.name == *src)
        .with_context(|| format!("config {} lists no ant {src}", path.display()))?;
    if ant.builtin {
        bail!("{src} is built in: it is not cloned");
    }
    let (base, user_home) = (path.parent().unwrap_or(Path::new(".")), config::home_dir()?);
    let src_home = ant.home.clone().unwrap_or_else(|| user_home.join(".ants").join(src.as_str()));
    let new_home = match settings.get("home") {
        Some(home) => config::resolve(Path::new(home.as_str().context("home is no string")?), base, &user_home),
        None => user_home.join(".ants").join(new.as_str()),
    };
    if new_home.starts_with(&src_home) || src_home.starts_with(&new_home) {
        bail!("the clone's home {} and {src}'s {} must not hold one another", new_home.display(), src_home.display());
    }
    if std::fs::read_dir(&new_home).is_ok_and(|mut dir| dir.next().is_some()) {
        bail!("{new}'s home {} holds files already", new_home.display());
    }
    let dir = host_creds.parent().context("the host credentials are in no directory")?;
    let creds_path = dir.join(format!("{new}.creds"));
    let shared_over = src_shared.as_ref().and_then(Option::as_ref);
    let source = Source { path, ant, home: &src_home, shared: shared_over };
    let target = Target { new, home: &new_home, creds: &creds_path, settings, mark: &mark };
    let (table, probe, workdir, shared) = table(source, target)?;
    config::parse(&entry_text(&text, &probe), &std::path::absolute(path)?, &user_home)
        .context("the config with the clone added")?;
    let shared = src_shared.map(|_| shared);
    Ok(Some(Plan { table, workdir, src_home, new_home, dir: dir.to_owned(), creds_path, shared }))
}

/// A clone order being carried out, with what it is carried out with.
struct Cloning<'a> {
    js: &'a jetstream::Context,
    bucket: Bucket<'a>,
    orders: &'a Orders,
    config: &'a ConfigFile,
    /// The machine's host credentials, which `ants signer` is asked with.
    host_creds: &'a Path,
    order: &'a AdminOrder,
    new: &'a UserId,
    settings: &'a Map<String, Value>,
}

/// The clone's key, signed by `ants signer`.
struct Signed<'a> {
    /// Where the key is kept until the clone is listed.
    seed_path: PathBuf,
    key: KeyPair,
    public_key: String,
    jwt: String,
    renewal: Option<Renewal<'a>>,
}

impl Cloning<'_> {
    /// Carry out the clone of the order's ant as `new`, noting in `done` what to undo: what it did, none when the
    /// config shows it done already.
    async fn run(&self, done: &mut Done) -> Result<Option<Value>> {
        let (src, path, new) = (&self.order.ant, &self.config.path, self.new);
        let src_shared = match shared::read(self.bucket, src).await? {
            Found::Record(record, _) => Some(Some(record.fields)),
            Found::None(_) | Found::Malformed(..) | Found::Unverified(..) => Some(None),
            Found::NoBucket => None,
        };
        let Some(plan) = plan(path, self.host_creds, self.order, new, self.settings, src_shared)? else {
            return Ok(None);
        };
        if !self.orders.restarts.hears_clones(src) {
            bail!("{} serves no inbox of {src} to clone", self.orders.machine);
        }
        let leaf = leaf_uplink(self.orders.leaf.as_ref())?;
        let snapshot = self.snapshot(done).await?;
        let (trees, branches) = copy_files(&plan, &snapshot, new, done).await?;
        let signed = self.sign(leaf, &plan, done).await?;
        let (forked, restart, record) = self.store(&plan, &snapshot, &trees, done).await?;

        let Plan { table, workdir, new_home, creds_path, shared, .. } = plan;
        if workdir.starts_with(&new_home) {
            std::fs::create_dir_all(&workdir).with_context(|| format!("creating {}", workdir.display()))?;
        }
        // Before listing the ant, so its first connection finds the uplink carrying it.
        let uplink = match signed.renewal {
            Some((leaf, machine, renewed)) => Some(renew(leaf, &machine, new, renewed).await),
            None => None,
        };
        if let Some(fields) = shared {
            let mut record = SharedSettings::new(new.clone(), fields, self.orders.machine.clone(), None);
            done.shared = Some((self.bucket.identities.clone(), record.key()));
            let signer = shared::Signer::new(&signed.jwt, signed.key.clone());
            shared::write(self.bucket.identities, &mut record, &signer, None).await?;
            self.config.shared.store(record);
        }
        config::append(path, &table)?;
        if let Err(e) = std::fs::remove_file(&signed.seed_path)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(path = %signed.seed_path.display(), error = %e, "removing the key of a cloned ant");
        }
        let mut result = json!({
            "creds": creds_path,
            "key": signed.public_key,
            "home": new_home,
            "threads": snapshot.threads.len(),
            "forked": forked,
            "restarted": restart,
            "worktrees": branches,
            "identity": record_summary(&record),
        });
        if let Some(uplink) = uplink {
            result["uplink"] = Value::String(uplink);
        }
        Ok(Some(result))
    }

    /// Pause the ant cloned, unless it was already, and have the daemon serving its inbox read its snapshot once its
    /// last prompt ended.
    async fn snapshot(&self, done: &mut Done) -> Result<Snapshot> {
        let (src, new, machine) = (&self.order.ant, self.new, &self.orders.machine);
        if super::pause::edit(&self.config.path, src, true)? {
            done.paused = true;
            self.config.pausers.paused(src, &self.order.admin);
            self.config.edited.notify_one();
        }
        let snapshot = tokio::time::timeout(SNAPSHOT_WAIT, self.orders.restarts.snapshot(src, new.clone()))
            .await
            .map_err(|_| anyhow!("{src}'s prompts did not end within {}", ants_proto::duration::format(SNAPSHOT_WAIT)))?
            .with_context(|| format!("{machine} stopped serving the inbox of {src}"))?
            .with_context(|| format!("reading {src}'s sessions and threads"))?;
        if let Some(moved) = snapshot.identity.as_ref().and_then(|identity| identity.moved.as_ref()) {
            bail!(
                "{src} was moved to another bus by {} at {}: it is cloned where it is served now",
                moved.by,
                moved.at
            );
        }
        if snapshot.new_identity.is_some() {
            bail!("{new} has an identity record already: a name once used is not cloned into");
        }
        Ok(snapshot)
    }

    /// Have `ants signer` sign the clone's key, kept beside the host credentials, and write its credentials there,
    /// renewing the uplink of `leaf`, if given, to carry it.
    async fn sign<'l>(&self, leaf: Option<(&'l LeafNode, Uplink)>, plan: &Plan, done: &mut Done) -> Result<Signed<'l>> {
        let new = self.new;
        let seed_path = plan.dir.join(format!("{new}.nk"));
        done.files.push(seed_path.clone());
        let key = key_for(&plan.creds_path, &seed_path, new)?;
        let public_key = key.public_key();
        let (jwt, renewal) =
            sign(self.js, self.host_creds, leaf, &self.orders.accounts, self.order, new, &public_key).await?;
        let seed = key.seed().map_err(|e| anyhow!("the seed of {public_key}: {e}"))?;
        done.files.push(plan.creds_path.clone());
        write_secret(&plan.creds_path, &creds::text(&jwt, &seed))?;
        Ok(Signed { seed_path, key, public_key, jwt, renewal })
    }

    /// Store the clone's sessions ([`store_sessions`]) and its identity record, with its credentials: the threads
    /// forked, those that start over, and the record.
    async fn store(
        &self,
        plan: &Plan,
        snapshot: &Snapshot,
        trees: &[Tree],
        done: &mut Done,
    ) -> Result<(Vec<String>, Vec<String>, AntIdentity)> {
        let (new, domain) = (self.new, self.orders.js_domain.as_ref());
        let (_client, new_js) = bus::connect(&self.orders.nats, Some(&plan.creds_path), domain, new, "clone").await?;
        let store = SessionStore::kv(bus::ensure_sessions(&new_js, domain, new).await?, new.clone());
        let (forked, restart) = store_sessions(&store, snapshot, trees, &plan.new_home, &plan.workdir, done).await?;

        let identity = Identity::new(&new_js, domain, new).await?;
        let record = born(new, &self.order.ant, snapshot.identity.as_ref())?;
        done.identity = Some(identity.clone());
        identity.replace(&record).await?;
        Ok((forked, restart, record))
    }
}

/// Copy the home of the ant cloned to the clone's, with the history of each thread of `snapshot`, and add its
/// worktrees again for `new`: the worktrees, and the branches added.
async fn copy_files(
    plan: &Plan,
    snapshot: &Snapshot,
    new: &UserId,
    done: &mut Done,
) -> Result<(Vec<Tree>, Vec<String>)> {
    let (src_home, new_home) = (&plan.src_home, &plan.new_home);
    let trees = worktrees(src_home, &snapshot.threads).await?;
    done.home = Some(new_home.clone());
    copy_home(src_home, new_home)?;
    write_histories(new_home, &snapshot.threads)?;
    let mut branches = Vec::new();
    for tree in &trees {
        branches.push(tree.add(src_home, new_home, new, done).await?);
    }
    Ok((trees, branches))
}

/// Store in `store`, the clone's, a session for each thread of `snapshot`, forked in the thread's worktree among
/// `trees`, added again in `new_home`, else in `workdir`, or marked to start over: the threads forked, and those that
/// start over.
async fn store_sessions(
    store: &SessionStore,
    snapshot: &Snapshot,
    trees: &[Tree],
    new_home: &Path,
    workdir: &Path,
    done: &mut Done,
) -> Result<(Vec<String>, Vec<String>)> {
    let (mut forked, mut restart) = (Vec::new(), Vec::new());
    for copy in &snapshot.threads {
        let tree = new_home.join("worktrees").join(copy.thread.as_str());
        let cwd = if trees.iter().any(|t| t.thread == copy.thread) { tree } else { workdir.to_owned() };
        let stored = session(snapshot.launch.as_ref(), copy, cwd).await;
        if stored.cloned {
            restart.push(copy.thread.to_string());
        } else {
            forked.push(copy.thread.to_string());
        }
        done.sessions.get_or_insert_with(|| (store.clone(), Vec::new())).1.push(copy.thread.clone());
        store.put(&copy.thread, stored).await?;
    }
    Ok((forked, restart))
}

/// The identity record of `new`, a clone of `src`, whose record is `src_identity`.
fn born(new: &UserId, src: &UserId, src_identity: Option<&AntIdentity>) -> Result<AntIdentity> {
    let mut record = identity::born(new)?;
    if let Some(src_identity) = src_identity {
        record.prompts = src_identity.prompts;
        record.cloned_from = Some(ClonedFrom { ant: src.clone(), id: src_identity.id.clone(), at: Utc::now() });
    }
    Ok(record)
}

fn record_summary(record: &AntIdentity) -> Value {
    json!({ "id": record.id, "prompts": record.prompts, "cloned_from": record.cloned_from })
}

/// Write the history of each thread of `copies` in `home`, the clone's.
fn write_histories(home: &Path, copies: &[ThreadCopy]) -> Result<()> {
    for copy in copies {
        let file = crate::history::cloned::path(home, &copy.thread);
        if let Some(dir) = file.parent() {
            std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        }
        std::fs::write(&file, &copy.history).with_context(|| format!("writing {}", file.display()))?;
    }
    Ok(())
}

/// Resume `ant`, paused by the clone.
fn resume(config: &ConfigFile, ant: &UserId) {
    match super::pause::edit(&config.path, ant, false) {
        Ok(written) => {
            config.pausers.take(ant);
            if written {
                config.edited.notify_one();
            }
        }
        Err(e) => tracing::error!(ant = %ant, error = format!("{e:#}"), "resuming an ant after cloning it"),
    }
}

/// The session of the thread of `copy` for the clone: forked with `launch` in `cwd` if its harness can, else marked to
/// start over.
async fn session(launch: Option<&harness::Launch>, copy: &ThreadCopy, cwd: PathBuf) -> Stored {
    let restart = Stored { cloned: true, ..copy.stored.clone() };
    let Some(launch) = launch else { return restart };
    if launch.harness_name() != copy.stored.harness {
        return restart;
    }
    match harness::fork(&launch.clone().in_dir(cwd), &copy.stored.session).await {
        Ok(Some(session)) => Stored { session, harness: copy.stored.harness.clone(), cloned: false },
        Ok(None) => restart,
        Err(e) => {
            tracing::warn!(thread = %copy.thread, error = format!("{e:#}"), "forking a session for a clone");
            restart
        }
    }
}

/// The ant cloned, as the clone's table is made from it.
#[derive(Clone, Copy)]
struct Source<'a> {
    /// The config file listing it.
    path: &'a Path,
    ant: &'a config::Ant,
    home: &'a Path,
    /// Its shared settings, when it has a record of them.
    shared: Option<&'a Map<String, Value>>,
}

/// The clone, as its table names it.
#[derive(Clone, Copy)]
struct Target<'a> {
    new: &'a UserId,
    home: &'a Path,
    creds: &'a Path,
    /// The order's settings, over those of the ant cloned.
    settings: &'a Map<String, Value>,
    /// The comment above the table ([`mark`]).
    mark: &'a str,
}

/// The `[[ant]]` table of the clone `new` of `ant`, one to check the file with before its workdir exists, and its
/// workdir: the table of `ant` in the file at `path` (or, for an ant added from a template, the settings the template
/// gave it) with `shared`, its shared settings when it has a record of them, and `settings` over it, its workdir moved
/// into `new_home` if it was in `src_home`; and the clone's shared settings, as the table gives them.
fn table(source: Source<'_>, target: Target<'_>) -> Result<(String, String, PathBuf, Map<String, Value>)> {
    let Source { path, ant, home: src_home, shared } = source;
    let Target { new, home: new_home, creds, settings, mark } = target;
    let raw = Edit::open(path)?.table(&ant.name)?;
    let mut table = if ant.template.is_some() {
        let mut plain = resolved(ant);
        plain.extend(raw.into_iter().filter(|(key, _)| key == "nats" || key == "js_domain"));
        plain
    } else {
        raw
    };
    table.retain(|key, _| !NOT_CLONED.contains(&key.as_str()));
    if let Some(shared) = shared {
        table.retain(|key, _| !shared::shares(ant, key));
        table.extend(shared.iter().filter(|(key, _)| shared::shares(ant, key)).map(|(k, v)| (k.clone(), v.clone())));
    }
    if let Ok(inside) = ant.workdir.strip_prefix(src_home) {
        let workdir = new_home.join(inside);
        table.insert("workdir".to_owned(), Value::from(workdir.to_str().context("the workdir is not UTF-8")?));
    }
    table.extend(settings.iter().filter(|(key, _)| *key != "name").map(|(k, v)| (k.clone(), v.clone())));
    let text = entry(new, creds, None, &table, mark)?;
    let (base, home) = (path.parent().unwrap_or(Path::new(".")), config::home_dir()?);
    let workdir = table.get("workdir").and_then(Value::as_str).context("the ant has no workdir")?;
    let workdir = config::resolve(Path::new(workdir), base, &home);
    if !workdir.exists() {
        table.insert("workdir".to_owned(), Value::from(base.to_str().context("the config's directory is not UTF-8")?));
    }
    let probe = entry(new, creds, None, &table, mark)?;
    let shared = table.iter().filter(|(key, _)| is_shared_field(key)).map(|(k, v)| (k.clone(), v.clone())).collect();
    Ok((text, probe, workdir, shared))
}

/// The settings of `ant`, added from a template, as a table of its own.
fn resolved(ant: &config::Ant) -> Map<String, Value> {
    let duration = |d: Duration| Value::from(ants_proto::duration::format(d));
    let escalate_to: Vec<&str> = ant.escalate_to.iter().map(UserId::as_str).collect();
    let table = json!({
        "workdir": ant.workdir,
        "whoami": ant.whoami,
        "memory": ant.memory,
        "tasks": ant.tasks,
        "env": ant.env,
        "harness": ant.harness,
        "model": ant.model,
        "reasoning_effort": ant.reasoning_effort,
        "context": ant.context,
        "allow": ant.allow,
        "approval_timeout": duration(ant.approval_timeout),
        "capabilities": ant.capabilities,
        "max_harnesses": ant.max_harnesses,
        "worktrees": ant.worktrees,
        "idle_timeout": duration(ant.idle_timeout),
        "prompt_timeout": duration(ant.prompt_timeout),
        "max_tool_calls": ant.max_tool_calls,
        "max_thread_tokens": ant.max_thread_tokens,
        "max_thread_cost": ant.max_thread_cost,
        "max_daily_tokens": ant.max_daily_tokens,
        "max_daily_cost": ant.max_daily_cost,
        "spend_warn_percent": ant.spend_warn_percent,
        "escalate_to": escalate_to,
        "worktree_base": ant.worktree_base.as_str(),
        "publish": ant.publish.as_str(),
    });
    let Value::Object(mut table) = table else { unreachable!("an object") };
    table.retain(|_, value| match value {
        Value::Null => false,
        Value::Array(items) => !items.is_empty(),
        Value::Object(items) => !items.is_empty(),
        _ => true,
    });
    table
}

/// Copy the home `from` to `to`, all but [`NOT_COPIED`]: directories, regular files and symbolic links.
fn copy_home(from: &Path, to: &Path) -> Result<()> {
    std::fs::create_dir_all(to).with_context(|| format!("creating {}", to.display()))?;
    let entries = match std::fs::read_dir(from) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", from.display())),
    };
    for entry in entries {
        let entry = entry.with_context(|| format!("reading {}", from.display()))?;
        if NOT_COPIED.iter().any(|name| entry.file_name() == *name) {
            continue;
        }
        copy(&entry.path(), &to.join(entry.file_name()))?;
    }
    Ok(())
}

/// Copy the directory, regular file or symbolic link `from` to `to`; anything else is skipped.
fn copy(from: &Path, to: &Path) -> Result<()> {
    let kind = std::fs::symlink_metadata(from).with_context(|| format!("reading {}", from.display()))?.file_type();
    if kind.is_dir() {
        std::fs::create_dir_all(to).with_context(|| format!("creating {}", to.display()))?;
        for entry in std::fs::read_dir(from).with_context(|| format!("reading {}", from.display()))? {
            let entry = entry.with_context(|| format!("reading {}", from.display()))?;
            copy(&entry.path(), &to.join(entry.file_name()))?;
        }
    } else if kind.is_file() {
        std::fs::copy(from, to).with_context(|| format!("copying {} to {}", from.display(), to.display()))?;
    } else if kind.is_symlink() {
        #[cfg(unix)]
        {
            let target = std::fs::read_link(from).with_context(|| format!("reading {}", from.display()))?;
            std::os::unix::fs::symlink(&target, to).with_context(|| format!("linking {}", to.display()))?;
        }
    }
    Ok(())
}

/// A worktree of the ant cloned, as it was before its home was copied.
struct Tree {
    thread: ThreadId,
    from: PathBuf,
    /// The repository's git directory.
    common: PathBuf,
    head: String,
    /// A commit of its uncommitted changes (`git stash create`), if it has any.
    changes: Option<String>,
    untracked: Vec<PathBuf>,
}

/// The worktrees under `home` of the threads of `copies`.
async fn worktrees(home: &Path, copies: &[ThreadCopy]) -> Result<Vec<Tree>> {
    let mut trees = Vec::new();
    for copy in copies {
        let from = home.join("worktrees").join(copy.thread.as_str());
        if !from.join(".git").is_file() {
            continue;
        }
        let head = git(&from, &["rev-parse", "HEAD"]).await?.trim().to_owned();
        let changes = git(&from, &["stash", "create"]).await?.trim().to_owned();
        let untracked = git(&from, &["ls-files", "--others", "--exclude-standard", "-z"]).await?;
        let common = git(&from, &["rev-parse", "--path-format=absolute", "--git-common-dir"]).await?;
        trees.push(Tree {
            thread: copy.thread.clone(),
            common: PathBuf::from(common.trim()),
            head,
            changes: Some(changes).filter(|c| !c.is_empty()),
            untracked: untracked.split('\0').filter(|f| !f.is_empty()).map(PathBuf::from).collect(),
            from,
        });
    }
    Ok(trees)
}

impl Tree {
    /// Add the worktree again in `new_home`, the home of `new` copied from `src_home`, on its own branch: the branch.
    async fn add(&self, src_home: &Path, new_home: &Path, new: &UserId, done: &mut Done) -> Result<String> {
        let repo = match self.common.strip_prefix(src_home) {
            Ok(inside) => new_home.join(inside),
            Err(_) => self.common.clone(),
        };
        let to = new_home.join("worktrees").join(self.thread.as_str());
        let branch = format!("ants/{new}/{}", self.thread);
        let path = to.to_str().context("the worktree path is not UTF-8")?;
        git(&repo, &["worktree", "add", "--quiet", "--no-track", "-b", &branch, path, &self.head]).await?;
        done.worktrees.push((repo, to.clone(), branch.clone()));
        if let Some(changes) = &self.changes {
            git(&to, &["stash", "apply", "--quiet", changes]).await?;
        }
        for file in &self.untracked {
            let target = to.join(file);
            if let Some(dir) = target.parent() {
                std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
            }
            copy(&self.from.join(file), &target)?;
        }
        Ok(branch)
    }
}

async fn git(dir: &Path, args: &[&str]) -> Result<String> {
    crate::worktrees::run("git", dir, args, &[("GIT_TERMINAL_PROMPT", "0")], Some(GIT_WAIT)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_clone_of_an_ant_added_from_a_template_keeps_what_the_template_fixed() {
        let dir = ants_test_support::scratch_dir("clone-templated").canonicalize().unwrap();
        let path = dir.join("config.toml");
        let text = r#"
            messenger = false

            [[ant_template]]
            name = "worker"
            managers = ["lead"]
            max_ants = 1
            workdirs = ["."]
            harnesses = ["codex"]
            models = { "gpt-5" = "high" }
            allow = ["read"]

            [[ant]]
            name = "lead"
            workdir = "."
            harness = "codex"

            [[ant]]
            name = "lead-a"
            template = "worker"
            added_by = "lead"
            model = "gpt-5"
        "#;
        std::fs::write(&path, text).unwrap();
        let loaded = config::parse(text, &path, &dir).unwrap();
        let src = loaded.ants.iter().find(|a| a.name.as_str() == "lead-a").unwrap();
        let record = json!({ "model": "gpt-5", "whoami": "a tester" });
        let (src_home, new_home, creds) = (dir.join("lead-a"), dir.join("lead-b"), dir.join("lead-b.creds"));
        let source = Source { path: &path, ant: src, home: &src_home, shared: record.as_object() };
        let new = "lead-b".parse().unwrap();
        let settings = Map::new();
        let target = Target { new: &new, home: &new_home, creds: &creds, settings: &settings, mark: "# cloned" };
        let (.., shared) = table(source, target).unwrap();
        assert_eq!(shared.get("whoami"), Some(&json!("a tester")), "the shared settings of the ant cloned");
        assert_eq!(shared.get("allow"), Some(&json!(["read"])), "the template's");
        assert_eq!(shared.get("escalate_to"), Some(&json!(["lead"])));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
