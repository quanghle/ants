//! Saved trails: the colony's, in [`TRAILS_BUCKET`], and this machine's, the `.py` files of `~/.ants/trails`. A name
//! both have runs the colony's.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use ants_proto::{SavedTrail, TRAILS_BUCKET, TrailName, UserId, Uuid};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::colony::{short, since};
use crate::machines::latest_in;
use crate::text::excerpt;

/// What a trail's name is made of.
const NAMES: &str = "a trail's name is 1-64 of a-z, 0-9 and '-'";

/// A saved trail, the colony's or a local one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(super) struct Trail {
    pub(super) name: TrailName,
    pub(super) description: String,
    pub(super) phases: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(super) waypoints: Vec<String>,
    #[serde(flatten)]
    pub(super) source: Source,
    #[serde(skip)]
    pub(super) script: String,
}

/// Where a saved trail is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub(super) enum Source {
    /// In [`TRAILS_BUCKET`], saved by a human.
    Colony { saved_by: UserId, saved_at: DateTime<Utc> },
    /// A file of this machine's `~/.ants/trails`.
    Local { path: PathBuf },
}

impl From<SavedTrail> for Trail {
    fn from(saved: SavedTrail) -> Self {
        Self {
            name: saved.name,
            description: saved.description,
            phases: saved.phases,
            waypoints: saved.waypoints,
            source: Source::Colony { saved_by: saved.saved_by, saved_at: saved.saved_at },
            script: saved.script,
        }
    }
}

/// The directory of this machine's trails: `~/.ants/trails`.
pub(super) fn local_dir() -> Result<PathBuf> {
    Ok(std::env::home_dir().context("no home directory")?.join(".ants").join("trails"))
}

/// The trail of the file `path`, saved by `by` at `now` under `name`, else under its meta's name: refused when the file
/// does not compile, lacks its literal meta first, or breaks the bounds of [`SavedTrail::check`].
pub(super) fn read(path: &Path, name: Option<TrailName>, by: &UserId, now: DateTime<Utc>) -> Result<SavedTrail> {
    let script = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let meta = checked(path, &script)?;
    let name = match name {
        Some(name) => name,
        None => meta.name.parse().map_err(|_| {
            anyhow!("{}: meta's name {:?} is no trail name: {NAMES}; or give --name", path.display(), meta.name)
        })?,
    };
    usable(&name)?;
    let trail = SavedTrail {
        name,
        description: meta.description,
        phases: meta.phases,
        waypoints: meta.waypoints,
        script,
        saved_by: by.clone(),
        saved_at: now,
    };
    trail.check()?;
    Ok(trail)
}

/// The meta of `script`, the file `path`: refused when the file does not compile or lacks its literal meta first.
pub(super) fn checked(path: &Path, script: &str) -> Result<ants_trail::Meta> {
    ants_trail::check(script).map_err(|e| match e {
        ants_trail::TrailError::Syntax(traceback) => anyhow!("{} does not compile:\n{traceback}", path.display()),
        ants_trail::TrailError::Meta(why) => anyhow!("{}: {why}", path.display()),
    })
}

/// Refuse a name reading as a run's id, which `ants trail show` takes for a run.
fn usable(name: &TrailName) -> Result<()> {
    if Uuid::try_parse(name.as_str()).is_ok() {
        bail!("trail name {name} reads as a run's id, which `ants trail show` would look for instead: pick another");
    }
    Ok(())
}

/// Save `trail` for the colony, replacing the one of its name; returns the one replaced.
pub(super) async fn put(js: &jetstream::Context, trail: &SavedTrail) -> Result<Option<SavedTrail>> {
    let before = get(js, &trail.name).await?;
    let kv = js.get_key_value(TRAILS_BUCKET).await.context("opening ants_trails (`ants init` creates it)")?;
    let payload = serde_json::to_vec(trail)?;
    kv.put(trail.key(), payload.into()).await.with_context(|| format!("saving trail {}", trail.name))?;
    Ok(before)
}

/// The colony's trail `name`.
pub(super) async fn get(js: &jetstream::Context, name: &TrailName) -> Result<Option<SavedTrail>> {
    let found = latest_in(js, TRAILS_BUCKET, &format!("$KV.{TRAILS_BUCKET}.{name}")).await?;
    Ok(found.iter().find_map(|(key, payload)| parse(key, payload)))
}

/// The colony's trails, by name.
pub(super) async fn colony(js: &jetstream::Context) -> Result<Vec<SavedTrail>> {
    let found = latest_in(js, TRAILS_BUCKET, &format!("$KV.{TRAILS_BUCKET}.*")).await?;
    let mut trails: Vec<SavedTrail> = found.iter().filter_map(|(key, payload)| parse(key, payload)).collect();
    trails.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(trails)
}

fn parse(key: &str, payload: &[u8]) -> Option<SavedTrail> {
    let checked = serde_json::from_slice::<SavedTrail>(payload)
        .map_err(anyhow::Error::from)
        .and_then(|trail| trail.check_key(key).map(|()| trail).map_err(anyhow::Error::from));
    checked.inspect_err(|e| tracing::warn!(key, "skipping a saved trail: {e:#}")).ok()
}

/// Remove the colony's trail `name`, refused when the colony has none of that name; `local` is the directory of this
/// machine's trails, whose file of that name the refusal names.
pub(super) async fn remove(js: &jetstream::Context, local: &Path, name: &TrailName) -> Result<()> {
    if latest_in(js, TRAILS_BUCKET, &format!("$KV.{TRAILS_BUCKET}.{name}")).await?.is_empty() {
        let path = self::local(local).into_iter().find_map(|t| match t.source {
            Source::Local { path } if t.name == *name => Some(path),
            _ => None,
        });
        match path {
            Some(path) => {
                bail!("the colony saved no trail {name}; the local one goes with its file, {}", path.display())
            }
            None => bail!("the colony saved no trail {name}"),
        }
    }
    let kv = js.get_key_value(TRAILS_BUCKET).await.context("opening ants_trails (`ants init` creates it)")?;
    kv.delete(name.as_str()).await.with_context(|| format!("removing trail {name}"))?;
    Ok(())
}

/// This machine's trails, the `.py` files of `dir` that pass [`ants_trail::check`], each named by its file's stem as a
/// conductor's daemon finds it; the others, and those whose stem is no trail name, are skipped with a warning.
pub(super) fn local(dir: &Path) -> Vec<Trail> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
        Err(e) => {
            tracing::warn!("not reading the local trails of {}: {e}", dir.display());
            return Vec::new();
        }
    };
    let mut paths: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "py") && path.is_file())
        .collect();
    paths.sort();
    let mut trails: Vec<Trail> = Vec::new();
    for path in paths {
        match read_local(&path) {
            Ok(trail) => trails.push(trail),
            Err(e) => tracing::warn!("skipping the local trail {}: {e:#}", path.display()),
        }
    }
    trails.sort_by(|a, b| a.name.cmp(&b.name));
    trails
}

fn read_local(path: &Path) -> Result<Trail> {
    let stem = path.file_stem().map_or_default(|s| s.to_string_lossy());
    let name: TrailName =
        stem.parse().map_err(|_| anyhow!("the file's name {stem:?} is no trail name: {NAMES}, then `.py`"))?;
    usable(&name)?;
    let script = std::fs::read_to_string(path)?;
    let meta = ants_trail::check(&script)?;
    Ok(Trail {
        name,
        description: meta.description,
        phases: meta.phases,
        waypoints: meta.waypoints,
        source: Source::Local { path: path.to_owned() },
        script,
    })
}

/// A trail as `ants trail list` lists it.
#[derive(Debug, Serialize)]
pub(crate) struct Listed {
    #[serde(flatten)]
    pub(super) trail: Trail,
    /// A local trail of a name the colony has too, whose trail runs instead.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub(super) shadowed: bool,
}

/// The colony's trails, then the local ones, each by name.
pub(super) fn listing(colony: Vec<SavedTrail>, local: Vec<Trail>) -> Vec<Listed> {
    let names: Vec<TrailName> = colony.iter().map(|t| t.name.clone()).collect();
    let colony = colony.into_iter().map(|t| Listed { trail: t.into(), shadowed: false });
    let local = local.into_iter().map(|trail| Listed { shadowed: names.contains(&trail.name), trail });
    colony.chain(local).collect()
}

/// The listed trails in columns, their waypoints in a column of their own when one has any.
pub(crate) fn table(listed: &[Listed], now: DateTime<Utc>) -> String {
    let header = ["NAME", "SOURCE", "SAVED BY", "SAVED", "PHASES", "WAYPOINTS", "DESCRIPTION"].map(str::to_owned);
    let rows: Vec<[String; 7]> = std::iter::once(header)
        .chain(listed.iter().map(|Listed { trail, shadowed }| {
            let (source, by, at) = match &trail.source {
                Source::Colony { saved_by, saved_at } => {
                    ("colony".to_owned(), saved_by.to_string(), format!("{} ago", short(since(*saved_at, now))))
                }
                Source::Local { path } => {
                    let shadowed = if *shadowed { " (the colony's runs)" } else { "" };
                    (format!("{}{shadowed}", path.display()), "-".to_owned(), "-".to_owned())
                }
            };
            let list =
                |items: &[String]| if items.is_empty() { "-".to_owned() } else { excerpt(&items.join(", "), 40) };
            let description =
                if trail.description.is_empty() { "-".to_owned() } else { excerpt(&trail.description, 60) };
            [trail.name.to_string(), source, by, at, list(&trail.phases), list(&trail.waypoints), description]
        }))
        .collect();
    if listed.iter().any(|l| !l.trail.waypoints.is_empty()) {
        return super::columns(&rows);
    }
    let rows: Vec<[String; 6]> = rows
        .into_iter()
        .map(|[name, source, by, at, phases, _, description]| [name, source, by, at, phases, description])
        .collect();
    super::columns(&rows)
}

/// The trail `ants trail show NAME` shows, which a run of NAME runs.
#[derive(Debug)]
pub(crate) struct Found {
    pub(super) trail: Trail,
    /// The local file of the same name the colony's trail runs instead of.
    pub(super) shadows: Option<PathBuf>,
}

/// The trail `name`: the colony's, else the one of `local`, the directory of this machine's trails.
pub(super) async fn find(js: &jetstream::Context, local: &Path, name: &TrailName) -> Result<Found> {
    let here = self::local(local).into_iter().find(|t| t.name == *name);
    match (get(js, name).await?, here) {
        (Some(saved), here) => {
            let shadows = here.and_then(|t| match t.source {
                Source::Local { path } => Some(path),
                Source::Colony { .. } => None,
            });
            Ok(Found { trail: saved.into(), shadows })
        }
        (None, Some(trail)) => Ok(Found { trail, shadows: None }),
        (None, None) => {
            bail!("no trail {name}, neither in the colony nor in {}: `ants trail list` lists them", local.display())
        }
    }
}

/// A trail as `ants trail show --json` shows it, with its script.
#[derive(Serialize)]
pub(super) struct Shown<'a> {
    #[serde(flatten)]
    trail: &'a Trail,
    script: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    shadows: Option<&'a Path>,
}

impl<'a> From<&'a Found> for Shown<'a> {
    fn from(found: &'a Found) -> Self {
        Self { trail: &found.trail, script: &found.trail.script, shadows: found.shadows.as_deref() }
    }
}

/// The trail's meta and where it is, then its script.
pub(crate) fn render(found: &Found, now: DateTime<Utc>) -> String {
    let Found { trail, shadows } = found;
    let mut out = format!("trail {}\n", trail.name);
    match &trail.source {
        Source::Colony { saved_by, saved_at } => {
            let at = saved_at.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M");
            let _ = writeln!(out, "saved for the colony by {saved_by} {} ago ({at})", short(since(*saved_at, now)));
        }
        Source::Local { path } => {
            let _ = writeln!(out, "local: {}", path.display());
        }
    }
    if let Some(path) = shadows {
        let _ = writeln!(out, "runs instead of the local trail of the same name, {}", path.display());
    }
    if !trail.description.is_empty() {
        let _ = writeln!(out, "description: {}", trail.description);
    }
    if !trail.phases.is_empty() {
        let _ = writeln!(out, "phases: {}", trail.phases.join(", "));
    }
    if !trail.waypoints.is_empty() {
        let _ = writeln!(out, "waypoints: {}", trail.waypoints.join(", "));
    }
    out.push('\n');
    out.push_str(&trail.script);
    if !trail.script.ends_with('\n') {
        out.push('\n');
    }
    out
}
