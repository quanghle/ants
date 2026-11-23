//! `ants ant export`, `ants ant import` and `ants ant unseal`: an ant carried from one bus to another, as to a new job.
//! It keeps who it is and what it learnt (its identity record, the settings that make it, its home's memories) and
//! leaves behind what it was doing and the old job's role.
//!
//! An export is one plain JSON file ([`Export`]), not signed or encrypted: whoever carries it is trusted with it. Its
//! settings come from the ant's shared settings on the bus, which hold its role settings and core memories, over its
//! `[[ant]]` table ([`settings_of`]), and its files from its home ([`core_files_of`], [`harness_files_of`]); an import
//! writes them back through [`with_settings`] and [`restore`]. It lists the settings in the ant's `[[ant]]` table, from
//! which the first daemon serving the ant creates its shared settings on the new bus.
//!
//! When the ant's provider exports what it holds of the ant, the export carries that too ([`Export::provider`]), which
//! only the daemon serving the ant asks the provider for, and an import hands it to the ant's provider on the new bus
//! through the daemon there ([`provider`]).
//!
//! No bus holds two copies of one ant: an import creates the ant's identity record by a KV create, refused when a record
//! holds its name, and refused first when one holds its id. The id is looked up by reading the whole identities bucket,
//! one record per ant, which suits a team's bus; two imports of one ant under different names at the same moment could
//! both pass that check.
//!
//! An import queues, in the ant's new home, the lifecycle events telling its provider it moved, and was renamed when
//! imported under another name ([`ants_daemon::provider::events::arrived`]), its old name then listed among its earlier
//! names in the home, whose events in its sense log stay its own ([`ants_sense::identity::note_renamed`]).
//!
//! sense's part ([`ants_sense::export`]) carries the ant's sense log and skill files, which the home's files leave out.
//! An import carrying it marks the new home as awaiting it ([`ants_sense::export::expect_import`]), so the daemon
//! serving the ant writes nothing to the ant's log on the new bus before the log arrives.

use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use ants_daemon::config::{self, Ant};
use ants_daemon::harness::Harness;
use ants_daemon::records::direct_kv::{DirectKv, Lookup};
use ants_daemon::secrets::keystore::{self, Store};
use ants_proto::provider::EventKind;
use ants_proto::{AntIdentity, IDENTITIES_BUCKET, MovedMark, SharedSettings, UserId};
use ants_sense::home::Home;
use anyhow::{Context as _, Result, bail};
use chrono::{DateTime, Utc};
use data_encoding::BASE64;
use serde::{Deserialize, Serialize};

use crate::add::{self, AddArgs};
use crate::admin::settings::SharedRecord;
use crate::serve::AntOptions;

mod provider;

/// The version of [`Export`] this build writes and reads.
pub const VERSION: u32 = 1;
/// The most bytes of files one export carries.
const MAX_BYTES: u64 = 64 << 20;
/// How long an export waits for the ant's prompts in flight to end before packing its files anyway.
const PACK_WAIT: Duration = Duration::from_mins(2);
const PACK_POLL: Duration = Duration::from_secs(1);
/// What of an ant's home is not its memory, relative to the home: its worktrees, the daemon's delivery notes, MCP state
/// and lifecycle events, a git repository's history and a bd task database. sense's files but its id and the ant's
/// earlier names are left out too ([`Home::left_out`]).
const LEFT_OUT: [&str; 10] = [
    "worktrees",
    "answered",
    "unsent",
    "buried",
    "mcp.json",
    "mcp.sock",
    ".git",
    ".beads",
    ".bd",
    ants_daemon::provider::events::DIR,
];

/// An exported ant, as `NAME.ant` holds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Export {
    /// [`VERSION`].
    pub ants_ant: u32,
    pub created_at: DateTime<Utc>,
    /// The public key of the account of the bus the ant was exported from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bus: Option<String>,
    /// Its identity record, unmarked: its id, birth, avatar, colour and prompts.
    pub identity: AntIdentity,
    pub settings: Settings,
    #[serde(default)]
    pub files: Vec<Packed>,
    /// What the ant's provider holds of it, as the provider exported it, for a provider of the same name only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<ants_proto::provider::Export>,
}

/// What makes the ant, of its `[[ant]]` table. Its role on a bus (capabilities, escalate_to, allow, memory, tasks) and
/// its machine's (workdir, env) stay behind.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Settings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub whoami: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fallback: Vec<String>,
}

/// One file of the ant's home.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Packed {
    /// Its path relative to the home, `/` between its components.
    pub path: String,
    pub kind: Kind,
    /// Its bytes in base64.
    pub data: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// The ant's own: whatever harness it runs reads it.
    Core,
    /// A file of its harness's `memory_files`, which only that harness reads.
    Harness,
}

/// The settings of `ant` an export carries: those of its shared settings `shared` where the bus holds them, as its
/// daemon serves them, else of its `[[ant]]` table.
pub fn settings_of(ant: &Ant, shared: Option<&SharedSettings>) -> Result<Settings> {
    let overlaid;
    let ant = match shared {
        Some(record) => {
            let roles = ants_daemon::profile::shared::Roles::of(&record.fields)
                .with_context(|| format!("reading the shared settings of {}", ant.name))?;
            overlaid = roles.overlay(ant.clone());
            &overlaid
        }
        None => ant,
    };
    Ok(Settings {
        whoami: ant.whoami.clone(),
        harness: ant.harness.clone(),
        model: ant.model.clone(),
        reasoning_effort: ant.reasoning_effort.clone(),
        context: ant.context.clone(),
        fallback: ant.fallback.clone(),
    })
}

/// `ant` with the exported `settings` in place of its own. Listed in a config, they become the ant's shared settings on
/// the bus when the first daemon serving it creates them; shared settings the bus holds already serve instead.
pub fn with_settings(settings: &Settings, ant: Ant) -> Ant {
    Ant {
        whoami: settings.whoami.clone(),
        harness: settings.harness.clone(),
        model: settings.model.clone(),
        reasoning_effort: settings.reasoning_effort.clone(),
        context: settings.context.clone(),
        fallback: settings.fallback.clone(),
        ..ant
    }
}

/// The files of the home `home` that the `memory_files` globs of `harness` match, relative to it.
pub fn harness_files_of(harness: Option<&Harness>, home: &Path) -> Vec<PathBuf> {
    let globs = harness.map_or(&[][..], Harness::memory_files);
    ants_daemon::admin::files_under(globs, home, &LEFT_OUT)
}

/// The core memory files of `ant` in its home `home`, relative to it: the regular files there that are not `harness`
/// files, not [`LEFT_OUT`] and not the task database its `BD_DB` names; links are not followed.
pub fn core_files_of(ant: &Ant, home: &Path, harness: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let task_db: Vec<PathBuf> = ant
        .env
        .get("BD_DB")
        .and_then(|db| Path::new(db).strip_prefix(home).ok())
        .into_iter()
        .flat_map(|db| ["", "-wal", "-shm", "-journal"].map(|end| PathBuf::from(format!("{}{end}", db.display()))))
        .collect();
    let mut files = Vec::new();
    let mut dirs = vec![PathBuf::new()];
    while let Some(dir) = dirs.pop() {
        let entries = match std::fs::read_dir(home.join(&dir)) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && dir.as_os_str().is_empty() => break,
            Err(e) => return Err(e).with_context(|| format!("reading {}", home.join(&dir).display())),
        };
        for entry in entries {
            let entry = entry?;
            let rel = dir.join(entry.file_name());
            let left_out = LEFT_OUT.iter().any(|left| rel == Path::new(left)) || Home::left_out(&rel);
            if left_out || task_db.contains(&rel) {
                continue;
            }
            let kind = entry.file_type()?;
            if kind.is_dir() {
                dirs.push(rel);
            } else if kind.is_file() && !harness.contains(&rel) {
                files.push(rel);
            }
        }
    }
    files.sort();
    Ok(files)
}

/// The files `paths` of the home `home`, read, as `kind`.
fn pack(home: &Path, paths: &[PathBuf], kind: Kind, total: &mut u64) -> Result<Vec<Packed>> {
    let mut packed = Vec::new();
    for rel in paths {
        let path = home.join(rel);
        let bytes = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        *total += bytes.len() as u64;
        if *total > MAX_BYTES {
            bail!("the files of {} come to over the {} MiB one export carries", home.display(), MAX_BYTES >> 20);
        }
        let parts: Option<Vec<&str>> = rel.components().map(|c| c.as_os_str().to_str()).collect();
        let path = parts.with_context(|| format!("{} is no UTF-8", path.display()))?.join("/");
        packed.push(Packed { path, kind, data: BASE64.encode(&bytes) });
    }
    Ok(packed)
}

/// Write the exported `files` into the home `home`, leaving out the harness files unless `harness_kept`: the paths left
/// out.
pub fn restore(home: &Path, files: &[Packed], harness_kept: bool) -> Result<Vec<String>> {
    let mut dropped = Vec::new();
    std::fs::create_dir_all(home).with_context(|| format!("creating {}", home.display()))?;
    for file in files {
        if file.kind == Kind::Harness && !harness_kept {
            dropped.push(file.path.clone());
            continue;
        }
        let rel = Path::new(&file.path);
        if file.path.is_empty() || !rel.components().all(|c| matches!(c, Component::Normal(_))) {
            bail!("the export holds a file outside the home: {}", file.path);
        }
        let bytes = BASE64.decode(file.data.as_bytes()).with_context(|| format!("decoding {}", file.path))?;
        let path = home.join(rel);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        }
        std::fs::write(&path, bytes).with_context(|| format!("writing {}", path.display()))?;
    }
    Ok(dropped)
}

/// `ants ant export`; `with_provider` asks the ant's provider for its part through the daemon of this machine.
pub async fn export(
    nats: &crate::connection::Nats<'_>,
    ant: &UserId,
    out: Option<PathBuf>,
    seal: bool,
    config: Option<PathBuf>,
    with_provider: bool,
) -> Result<()> {
    if !nats.me.role.is_human() {
        bail!("the credentials of {} are an ant's; an export runs as a human", nats.me.id);
    }
    let path = match config {
        Some(path) => path,
        None => config::default_path()?,
    };
    let loaded = config::load(&path)?;
    let listed = loaded.ants.iter().find(|a| a.name == *ant).with_context(|| {
        format!("{} lists no ant {ant}: export it on a machine whose config file lists it", path.display())
    })?;
    let out = out.unwrap_or_else(|| PathBuf::from(format!("{ant}.ant")));
    let (_, js) = nats.connect("ants-export").await?;
    let kv = DirectKv::whole(&js, nats.domain, IDENTITIES_BUCKET).await?;
    let (identity, _) = read(&kv, ant).await?;
    let identity =
        identity.with_context(|| format!("{ant} has no identity record: its daemon creates one when it starts"))?;
    wait_between_prompts(&js, ant).await;
    let shared = match crate::admin::settings::shared_of(nats, &js, ant).await? {
        Some(SharedRecord::Verified(record)) => Some(record),
        Some(SharedRecord::Unverified(_, why)) => {
            eprintln!(
                "warning: the shared settings of {ant} are not verified ({why}) and daemons ignore them: exporting \
                 the settings of {} instead",
                path.display()
            );
            None
        }
        None => None,
    };
    let harness = listed.harness.as_deref().and_then(|name| loaded.harnesses.get(name).ok());
    let part = if with_provider {
        let machine = provider::machine(&path).await?;
        Box::pin(provider::export(nats, &js, machine, ant, &identity.id)).await?
    } else {
        None
    };

    let mut export = pack_ant(listed, harness.as_deref(), shared.as_ref(), identity, nats.account.clone())?;
    export.provider = part;
    write_new(&out, &serde_json::to_vec_pretty(&export)?)?;
    let count = |kind| export.files.iter().filter(|f| f.kind == kind).count();
    let part = export.provider.as_ref().map_or_else(String::new, |p| format!(", the part of provider {}", p.provider));
    println!(
        "{ant} exported to {}: its identity, settings, {} core and {} harness memory files{part}; the file is not \
         encrypted and may hold sensitive memory: keep it safe",
        out.display(),
        count(Kind::Core),
        count(Kind::Harness)
    );
    if seal {
        let mark = MovedMark { at: Utc::now(), by: nats.me.id.clone() };
        mark_moved(&kv, ant, Some(mark)).await?;
        println!(
            "{ant} marked moved: its daemon drains it and leaves the colony, and no daemon starts it; \
             `ants ant unseal {ant}` undoes the move, `ants auth revoke {ant}` ends it on this bus once the import \
             succeeded"
        );
    }
    Ok(())
}

/// The export of `ant`, whose harness is `harness`, shared settings `shared` and identity record `identity`, from the bus
/// of `bus`.
fn pack_ant(
    ant: &Ant,
    harness: Option<&Harness>,
    shared: Option<&SharedSettings>,
    identity: AntIdentity,
    bus: Option<String>,
) -> Result<Export> {
    let home = home_of(ant)?;
    let harness_files = harness_files_of(harness, &home);
    let core_files = core_files_of(ant, &home, &harness_files)?;
    let mut total = 0;
    let mut files = pack(&home, &core_files, Kind::Core, &mut total)?;
    files.extend(pack(&home, &harness_files, Kind::Harness, &mut total)?);
    Ok(Export {
        ants_ant: VERSION,
        created_at: Utc::now(),
        bus,
        identity: AntIdentity { moved: None, ..identity },
        settings: settings_of(ant, shared)?,
        files,
        provider: None,
    })
}

/// The home of `ant`, as its daemon resolves it.
pub(crate) fn home_of(ant: &Ant) -> Result<PathBuf> {
    match &ant.home {
        Some(home) => Ok(home.clone()),
        None => Ok(ants_daemon::config::default_home(
            &std::env::home_dir().context("no home directory")?,
            ant.name.as_str(),
        )),
    }
}

/// Warn when another machine serves `ant`, as its home here may be behind, and wait up to [`PACK_WAIT`] for its prompts
/// in flight to end when this one does, so its files are packed between prompts.
async fn wait_between_prompts(js: &async_nats::jetstream::Context, ant: &UserId) {
    let lookup = ants_daemon::mcp::jetstream_lookup(js.clone());
    let here = ants_daemon::colony::machine().await;
    let deadline = tokio::time::Instant::now() + PACK_WAIT;
    let mut told = false;
    loop {
        let entry = match lookup(ant.clone()).await {
            Ok(entry) => entry.filter(|e| ants_daemon::colony::is_live(e, Utc::now())),
            Err(e) => {
                eprintln!("warning: reading the colony: {e:#}; packing the files of {ant} as they are");
                return;
            }
        };
        let Some(entry) = entry else { return };
        if entry.machine != here {
            eprintln!(
                "warning: {ant} is served on {}, not here: its home here may be behind; export it there for its \
                 latest memories",
                entry.machine
            );
            return;
        }
        if entry.activity != ants_proto::Activity::Working {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            eprintln!("warning: {ant} still works after {}m; packing its files as they are", PACK_WAIT.as_secs() / 60);
            return;
        }
        if !told {
            eprintln!("waiting for the prompts of {ant} in flight to end, to pack its files between prompts");
            told = true;
        }
        tokio::time::sleep(PACK_POLL).await;
    }
}

/// Write `bytes` to a new file at `path` only its owner can read, refusing to overwrite one.
fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write as _;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options.open(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            anyhow::anyhow!("{} exists: not overwriting it (-o names another path)", path.display())
        } else {
            anyhow::Error::new(e).context(format!("creating {}", path.display()))
        }
    })?;
    if let Err(e) = file.write_all(bytes).and_then(|()| file.sync_all()) {
        drop(file);
        let _ = std::fs::remove_file(path);
        return Err(e).with_context(|| format!("writing {}", path.display()));
    }
    Ok(())
}

/// The identity record of `ant` and its sequence: None when absent.
pub(crate) async fn read(kv: &DirectKv, ant: &UserId) -> Result<(Option<AntIdentity>, u64)> {
    let (found, last) = kv.lookup_last(ant.as_str()).await?;
    match found {
        Lookup::NoBucket => bail!("no {IDENTITIES_BUCKET} bucket: `ants init` creates it"),
        Lookup::NoValue => Ok((None, last)),
        Lookup::Value(value) => {
            let record: AntIdentity =
                serde_json::from_slice(&value).with_context(|| format!("reading the identity of {ant}"))?;
            record.check_key(ant.as_str())?;
            Ok((Some(record), last))
        }
    }
}

/// Set or clear the moved mark of `ant`'s identity record by compare-and-set: whether it changed.
async fn mark_moved(kv: &DirectKv, ant: &UserId, mark: Option<MovedMark>) -> Result<bool> {
    let absent = format!("{ant} has no identity record");
    let marked = crate::avatars::change_in(kv, ant, &absent, |record| {
        if record.moved.is_some() == mark.is_some() {
            return Ok(false);
        }
        record.moved.clone_from(&mark);
        Ok(true)
    });
    Ok(marked.await?.1)
}

/// `ants ant unseal`.
pub async fn unseal(nats: &crate::connection::Nats<'_>, ant: &UserId) -> Result<()> {
    let (_, js) = nats.connect("ants-unseal").await?;
    let kv = DirectKv::whole(&js, nats.domain, IDENTITIES_BUCKET).await?;
    if mark_moved(&kv, ant, None).await? {
        println!("{ant} unsealed: a daemon listing it serves it again");
    } else {
        println!("{ant} is not marked moved");
    }
    Ok(())
}

/// What holds a moved ant back, for the colony, `ants ant show` and the ant view.
pub fn moved_line(ant: &UserId, mark: &MovedMark, now: DateTime<Utc>) -> String {
    format!(
        "{ant} was moved to another bus by {} {} ago: no daemon serves it; `ants ant unseal {ant}` undoes the move, \
         `ants auth revoke {ant}` ends it on this bus",
        mark.by,
        crate::text::short(crate::text::since(mark.at, now))
    )
}

/// `ants ant import`'s arguments: `ants add`'s but the name.
#[derive(clap::Args, Debug)]
pub struct ImportArgs {
    /// The file `ants ant export` wrote.
    pub file: PathBuf,
    /// Import the ant under this name, keeping its id, avatar and growth.
    #[arg(long = "as", value_name = "NEW")]
    pub rename: Option<UserId>,
    /// Config file to list the ant in (default: ~/.ants/config.toml).
    #[arg(long, env = "ANTS_CONFIG")]
    pub config: Option<PathBuf>,
    /// Where to write the ant's credentials (default: ~/.config/ants/auth/<ant>.creds). The seed waits beside them,
    /// as <name>.nk, until they are complete.
    #[arg(long)]
    pub out: Option<PathBuf>,
    /// Keep the ant's NKey seed in the OS credential store, never in a file, as `ants add --store-key` does.
    #[arg(long)]
    pub store_key: bool,
    /// Complete the credentials with the user JWT `ants auth user NAME --public-key ...` printed: the JWT, a file
    /// holding it, or `-` to read it from stdin.
    #[arg(long, value_name = "JWT", conflicts_with = "enroll")]
    pub jwt: Option<String>,
    /// Ask for the credentials over NATS with these enrollment credentials (`ants auth enroll NAME`), and wait for
    /// `ants enroll approve NAME` on the machine holding the account key.
    #[arg(long, value_name = "CREDS")]
    pub enroll: Option<PathBuf>,
    /// Server to send the enrollment request to (default: --nats, the machine's leaf node or the hub).
    #[arg(long, value_name = "URL", requires = "enroll")]
    pub hub: Option<String>,
    /// Configuration of the machine's leaf node (default: ~/.config/ants/leaf.conf, when it exists).
    #[arg(long, value_name = "FILE", requires = "enroll")]
    pub leaf_config: Option<PathBuf>,
    /// Leave out the part of the ant's provider the export carries.
    #[arg(long, conflicts_with = "provider_only")]
    pub no_provider: bool,
    /// Only hand the part of the ant's provider to the provider of the ant imported already, through the daemon of
    /// this machine, as when the import could not.
    #[arg(long)]
    pub provider_only: bool,
    #[command(flatten)]
    pub options: AntOptions,
}

/// `ants ant import`.
pub async fn import(nats: &crate::connection::Nats<'_>, args: ImportArgs) -> Result<()> {
    Box::pin(import_with(nats, args, keystore::os())).await
}

/// [`import`], keeping a seed of the OS credential store in `store`.
async fn import_with(nats: &crate::connection::Nats<'_>, args: ImportArgs, store: &dyn Store) -> Result<()> {
    if !nats.me.role.is_human() {
        bail!("the credentials of {} are an ant's; an import runs as a human", nats.me.id);
    }
    let file = &args.file;
    let text = std::fs::read(file).with_context(|| format!("reading {}", file.display()))?;
    let export: Export =
        serde_json::from_slice(&text).with_context(|| format!("reading {}: not an ants export", file.display()))?;
    if export.ants_ant != VERSION {
        bail!("{} is an export of version {}, and this ants reads version {VERSION}", file.display(), export.ants_ant);
    }
    let name = args.rename.unwrap_or_else(|| export.identity.ant.clone());
    let (no_provider, provider_only) = (args.no_provider, args.provider_only);
    let add = AddArgs {
        name: name.clone(),
        config: args.config,
        out: args.out,
        store_key: args.store_key,
        jwt: args.jwt,
        enroll: args.enroll,
        hub: args.hub,
        leaf_config: args.leaf_config,
        options: args.options,
    };
    let path = match &add.config {
        Some(path) => path.clone(),
        None => config::default_path()?,
    };
    if provider_only {
        if !config::load(&path)?.ants.iter().any(|ant| ant.name == name) {
            bail!("{} lists no ant {name}: --provider-only is for an ant imported already", path.display());
        }
        let (_, js) = nats.connect("ants-import").await?;
        return import_provider(nats, &js, &export, &name, &path, true).await;
    }
    if config::Summary::load(&path)?.creds_of(&name).is_some() {
        bail!("{} lists {name} already: --as NEW imports it under another name", path.display());
    }
    let base = with_settings(&export.settings, Ant::new(name.clone(), std::env::current_dir()?));
    let listing = add::listing(&add, base, nats.url, nats.domain)?;
    let Some((ant, _)) = &listing.entry else {
        bail!("{} lists {name} already: --as NEW imports it under another name", listing.path.display());
    };
    let home = home_of(ant)?;
    let home_existed = home.exists();
    let mut held = if home_existed { Some(std::fs::read_dir(&home)?) } else { None };
    if held.as_mut().and_then(Iterator::next).is_some() {
        bail!("the home {} of {name} is not empty: --home names an empty one", home.display());
    }
    let (_, js) = nats.connect("ants-import").await?;
    let kv = DirectKv::whole(&js, nats.domain, IDENTITIES_BUCKET).await?;
    let identity = AntIdentity { ant: name.clone(), moved: None, ..export.identity.clone() };
    create(&js, &kv, &identity).await?;
    warn_shared_drift(nats, &js, &name, ant).await;
    let done = async {
        let harness_kept = listing.harness == export.settings.harness;
        let dropped = restore(&home, &export.files, harness_kept)?;
        if !no_provider && export.provider.as_ref().is_some_and(|part| part.provider == ants_daemon::sense::NAME) {
            ants_sense::export::expect_import(&home).context("marking the home as awaiting the sense data")?;
        }
        let mut arrived = vec![EventKind::AntMoved];
        if name != export.identity.ant {
            ants_sense::identity::note_renamed(&home, &export.identity.ant)
                .with_context(|| format!("listing {} among the earlier names of {name}", export.identity.ant))?;
            arrived.push(EventKind::AntRenamed { from: export.identity.ant.clone() });
        }
        ants_daemon::provider::events::arrived(&home, &name, &identity.id, arrived)?;
        if !dropped.is_empty() {
            eprintln!(
                "warning: {name} runs {} here, not {}: its harness memory files are left out: {}",
                listing.harness.as_deref().unwrap_or("no harness"),
                export.settings.harness.as_deref().unwrap_or("no harness"),
                dropped.join(", ")
            );
        }
        add::credentials(&add, &listing, nats.url, store).await
    };
    if let Err(e) = done.await {
        undo(&kv, &identity, &home, home_existed).await;
        return Err(e);
    }
    println!(
        "{name} imported from {}: its identity (born {}, {} prompts), settings and memories in {}, with no tasks",
        file.display(),
        identity.born.format("%Y-%m-%d"),
        identity.prompts,
        home.display()
    );
    match &export.provider {
        Some(part) if no_provider => eprintln!("note: the part of provider {} is left out", part.provider),
        _ => import_provider(nats, &js, &export, &name, &listing.path, false).await?,
    }
    Ok(())
}

/// Warn when the bus holds shared settings of `name` that differ from the imported `ant`'s, as they serve in its place.
async fn warn_shared_drift(
    nats: &crate::connection::Nats<'_>,
    js: &async_nats::jetstream::Context,
    name: &UserId,
    ant: &Ant,
) {
    let shared = crate::admin::settings::shared_of(nats, js, name)
        .await
        .inspect_err(|e| tracing::debug!("not comparing the shared settings: {e:#}"));
    if let Some(SharedRecord::Verified(shared)) = shared.ok().flatten() {
        let drift = shared.drift(&ants_daemon::profile::shared::local_fields(ant));
        if !drift.is_empty() {
            eprintln!(
                "warning: the bus holds shared settings of {name} already, which serve in place of the imported {}: \
                 `ants ant set {name} FIELD=VALUE` changes them",
                drift.join(", ")
            );
        }
    }
}

/// Hand the provider of `name`, which the config file at `path` lists, its part of `export`, if any, telling how it
/// went. Unless `strict`, a failure is a warning, the rest of the ant imported already.
async fn import_provider(
    nats: &crate::connection::Nats<'_>,
    js: &async_nats::jetstream::Context,
    export: &Export,
    name: &UserId,
    path: &Path,
    strict: bool,
) -> Result<()> {
    let Some(part) = &export.provider else {
        if strict {
            bail!("the export carries no part of a provider");
        }
        return Ok(());
    };
    let imported = async {
        let machine = provider::machine(path).await?;
        Box::pin(provider::import(nats, js, machine, name, &export.identity.id, part)).await
    };
    let why = match imported.await {
        Ok(provider::Imported::Done) => {
            println!("provider {} took its part of {name}", part.provider);
            return Ok(());
        }
        Ok(provider::Imported::Pending(note)) => {
            eprintln!("note: {note}; it hands provider {} its part of {name} once it takes it", part.provider);
            return Ok(());
        }
        Ok(provider::Imported::Refused(why)) => why,
        Err(e) => format!("the part of provider {} of {name} is not imported: {e:#}", part.provider),
    };
    if strict {
        bail!(why);
    }
    let closed =
        if part.provider == ants_daemon::sense::NAME { ", sense keeping the ant's data closed until then" } else { "" };
    eprintln!("warning: {why}; `ants ant import FILE --provider-only` tries again{closed}");
    Ok(())
}

/// Create `identity` on the bus, refused when a record holds its name or its id.
async fn create(js: &async_nats::jetstream::Context, kv: &DirectKv, identity: &AntIdentity) -> Result<()> {
    let name = &identity.ant;
    if let (Some(_), _) = read(kv, name).await? {
        bail!("the bus holds an ant named {name} already: --as NEW imports it under another name");
    }
    let held = crate::avatars::read(js).await?;
    if let Some(same) = held.values().find(|record| record.id == identity.id) {
        bail!(
            "the bus holds this ant already, as {}: one bus never holds two copies of an ant; after a move, \
             `ants auth revoke {}` ends the copy here",
            same.ant,
            same.ant
        );
    }
    if !kv.create(name.as_str(), serde_json::to_vec(identity)?).await? {
        bail!("the bus holds an ant named {name} already: --as NEW imports it under another name");
    }
    Ok(())
}

/// Take back a failed import: the identity record it created, unless changed since, and the files it restored.
async fn undo(kv: &DirectKv, identity: &AntIdentity, home: &Path, home_existed: bool) {
    let undone = async {
        let (record, last) = read(kv, &identity.ant).await?;
        if record.as_ref() == Some(identity) {
            kv.delete_expecting(identity.ant.as_str(), last, None).await?;
        }
        if home.exists() {
            std::fs::remove_dir_all(home).with_context(|| format!("removing {}", home.display()))?;
        }
        if home_existed {
            std::fs::create_dir_all(home)?;
        }
        anyhow::Ok(())
    };
    if let Err(e) = undone.await {
        eprintln!("warning: taking back the failed import of {}: {e:#}", identity.ant);
    }
}

#[cfg(test)]
mod tests;
