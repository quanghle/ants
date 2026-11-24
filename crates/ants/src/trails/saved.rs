//! Saved trails: the colony's, in [`TRAILS_BUCKET`], and this machine's, the `.py` files of `~/.ants/trails`. A name
//! both have runs the colony's.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use ants_proto::{RatingSummary, RunId, SavedTrail, Saver, TRAILS_BUCKET, TrailName, UserId, Uuid, subject};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use serde::Serialize;

use crate::buckets::latest_in;
use crate::text::one_line;
use crate::text::{ago, short, since};

/// What a trail's name is made of.
const NAMES: &str = "a trail's name is 1-64 of a-z, 0-9 and '-'";

/// A saved trail, the colony's or a local one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Trail {
    pub(crate) name: TrailName,
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
    /// In [`TRAILS_BUCKET`], saved by a human or an ant, and by an ant maybe from a run it conducted.
    Colony {
        saved_by: Saver,
        #[serde(skip_serializing_if = "Option::is_none")]
        saved_from: Option<RunId>,
        saved_at: DateTime<Utc>,
    },
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
            source: Source::Colony { saved_by: saved.saved_by, saved_from: saved.saved_from, saved_at: saved.saved_at },
            script: saved.script,
        }
    }
}

/// The directory of this machine's trails: `~/.ants/trails`.
pub(crate) fn local_dir() -> Result<PathBuf> {
    Ok(std::env::home_dir().context("no home directory")?.join(".ants").join("trails"))
}

/// The trail of the file `path`, saved by the human `by` at `now` under `name`, else under its meta's name: refused when
/// the file does not compile, lacks its literal meta first, or breaks the bounds of [`SavedTrail::check`].
pub(crate) fn read(path: &Path, name: Option<TrailName>, by: &UserId, now: DateTime<Utc>) -> Result<SavedTrail> {
    let script = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let meta = checked(path, &script)?;
    let name = match name {
        Some(name) => name,
        None => meta.name.parse().map_err(|_| {
            anyhow!("{}: meta's name {:?} is no trail name: {NAMES}; or give --name", path.display(), meta.name)
        })?,
    };
    let trail = SavedTrail {
        name,
        description: meta.description,
        phases: meta.phases,
        waypoints: meta.waypoints,
        script,
        saved_by: Saver::Human { user: by.clone() },
        saved_from: None,
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

/// Refuse a local trail's name reading as a run's id, which `ants trail show` takes for a run, as
/// [`SavedTrail::check`] refuses a saved one's.
fn usable(name: &TrailName) -> Result<()> {
    if Uuid::try_parse(name.as_str()).is_ok() {
        bail!("trail name {name} reads as a run's id, which `ants trail show` would look for instead: pick another");
    }
    Ok(())
}

/// Save `trail` for the colony, replacing the one of its name; returns the one replaced.
pub(crate) async fn put(js: &jetstream::Context, trail: &SavedTrail) -> Result<Option<SavedTrail>> {
    let before = get(js, &trail.name).await?;
    let kv = js.get_key_value(TRAILS_BUCKET).await.context("opening ants_trails (`ants init` creates it)")?;
    let payload = serde_json::to_vec(trail)?;
    kv.put(trail.key(), payload.into()).await.with_context(|| format!("saving trail {}", trail.name))?;
    Ok(before)
}

/// The colony's trail `name`.
pub(crate) async fn get(js: &jetstream::Context, name: &TrailName) -> Result<Option<SavedTrail>> {
    let found = latest_in(js, TRAILS_BUCKET, &subject::kv(TRAILS_BUCKET, name)).await?;
    Ok(found.iter().find_map(|(key, payload)| parse(key, payload)))
}

/// The colony's trails, by name.
pub(super) async fn colony(js: &jetstream::Context) -> Result<Vec<SavedTrail>> {
    let found = latest_in(js, TRAILS_BUCKET, &subject::kv(TRAILS_BUCKET, "*")).await?;
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

/// What `ants trail rm` purged with a trail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Purged {
    /// Whether the trail was still saved, rather than removed already with tips or ratings left.
    pub(crate) trail: bool,
    /// Entries of tips and votes.
    pub(crate) tips: u64,
    pub(crate) ratings: u64,
}

/// Remove the colony's trail `name` and purge its tips and ratings, refused when the colony has none of that name and
/// neither tips nor ratings are left of it; `local` is the directory of this machine's trails, whose file of that name
/// the refusal names. The trail goes first, so that no tip left or rating put while it is removed outlives the purge,
/// as tips are left and ratings put only of saved trails; removing it again purges what a failed purge left.
pub(crate) async fn remove(js: &jetstream::Context, local: &Path, name: &TrailName) -> Result<Purged> {
    let saved = !latest_in(js, TRAILS_BUCKET, &subject::kv(TRAILS_BUCKET, name)).await?.is_empty();
    if saved {
        let kv = js.get_key_value(TRAILS_BUCKET).await.context("opening ants_trails (`ants init` creates it)")?;
        kv.delete(name.as_str()).await.with_context(|| format!("removing trail {name}"))?;
    }
    let tips = super::tips::purge(js, name)
        .await
        .with_context(|| format!("purging the tips of trail {name}, removed: `ants trail rm {name}` purges them"))?;
    let ratings = super::ratings::purge(js, name)
        .await
        .with_context(|| format!("purging the ratings of trail {name}, removed: `ants trail rm {name}` purges them"))?;
    if !saved && tips == 0 && ratings == 0 {
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
    Ok(Purged { trail: saved, tips, ratings })
}

/// Purge the keys of `stream` matching `filter`: how many keys went, each counted once however many of its messages
/// the bucket kept.
pub(super) async fn purge_keys(stream: &jetstream::stream::Stream, filter: &str) -> Result<u64> {
    let mut keys = stream.info_with_subjects(filter).await?;
    let mut count = 0;
    while let Some(key) = keys.next().await {
        key?;
        count += 1;
    }
    stream.purge().filter(filter).await?;
    Ok(count)
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
    /// The ratings of the trail that runs under its name, the colony's of a shadowed one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) ratings: Option<RatingSummary>,
}

/// The colony's trails, then the local ones, each by name.
pub(super) fn listing(colony: Vec<SavedTrail>, local: Vec<Trail>) -> Vec<Listed> {
    let names: Vec<TrailName> = colony.iter().map(|t| t.name.clone()).collect();
    let colony = colony.into_iter().map(|t| Listed { trail: t.into(), shadowed: false, ratings: None });
    let local = local.into_iter().map(|trail| Listed { shadowed: names.contains(&trail.name), trail, ratings: None });
    colony.chain(local).collect()
}

/// `listed` with the ratings in `summaries` of the trails that run under their names.
pub(super) fn rated(mut listed: Vec<Listed>, summaries: &BTreeMap<TrailName, RatingSummary>) -> Vec<Listed> {
    for entry in listed.iter_mut().filter(|entry| !entry.shadowed) {
        entry.ratings = summaries.get(&entry.trail.name).cloned();
    }
    listed
}

/// The listed trails in columns, the run an ant saved one from, their waypoints and their ratings (worked/partly/
/// failed, and the latest reason) in columns of their own when a trail has any.
pub(crate) fn table(listed: &[Listed], now: DateTime<Utc>) -> String {
    let header =
        ["NAME", "SOURCE", "SAVED BY", "SAVED", "FROM RUN", "PHASES", "WAYPOINTS", "DESCRIPTION", "RATINGS", "LATEST"];
    let rows: Vec<Vec<String>> = listed
        .iter()
        .map(|Listed { trail, shadowed, ratings }| {
            let (source, by, at, from) = match &trail.source {
                Source::Colony { saved_by, saved_from, saved_at } => (
                    "colony".to_owned(),
                    saved_by.to_string(),
                    ago(*saved_at, now),
                    saved_from.as_ref().map_or_else(|| "-".to_owned(), RunId::to_string),
                ),
                Source::Local { path } => {
                    let shadowed = if *shadowed { " (the colony's runs)" } else { "" };
                    (format!("{}{shadowed}", path.display()), "-".to_owned(), "-".to_owned(), "-".to_owned())
                }
            };
            let list =
                |items: &[String]| if items.is_empty() { "-".to_owned() } else { one_line(&items.join(", "), 40) };
            let description =
                if trail.description.is_empty() { "-".to_owned() } else { one_line(&trail.description, 60) };
            let [counts, latest] = super::ratings::cells(ratings.as_ref());
            let (phases, waypoints) = (list(&trail.phases), list(&trail.waypoints));
            vec![trail.name.to_string(), source, by, at, from, phases, waypoints, description, counts, latest]
        })
        .collect();
    crate::text::columns(&filled(&header, &["FROM RUN", "WAYPOINTS", "RATINGS", "LATEST"], rows))
}

/// `rows` under `header`, without the `optional` columns that no row fills, all of whose cells are `-`.
fn filled(header: &[&str], optional: &[&str], rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    let shown: Vec<bool> = (header.iter().enumerate())
        .map(|(i, name)| !optional.contains(name) || rows.iter().any(|row| row[i] != "-"))
        .collect();
    std::iter::once(header.iter().map(|name| (*name).to_owned()).collect())
        .chain(rows)
        .map(|row: Vec<String>| {
            row.into_iter().zip(&shown).filter_map(|(cell, &shown)| shown.then_some(cell)).collect()
        })
        .collect()
}

/// The trail `ants trail show NAME` shows, which a run of NAME runs.
#[derive(Debug)]
pub(crate) struct Found {
    pub(crate) trail: Trail,
    /// The local file of the same name the colony's trail runs instead of.
    pub(super) shadows: Option<PathBuf>,
    pub(super) ratings: Option<RatingSummary>,
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
            Ok(Found { trail: saved.into(), shadows, ratings: None })
        }
        (None, Some(trail)) => Ok(Found { trail, shadows: None, ratings: None }),
        (None, None) => {
            bail!("no trail {name}, either in the colony or in {}: `ants trail list` lists them", local.display())
        }
    }
}

/// A trail as `ants trail show --json` shows it, with its script.
#[derive(Serialize)]
pub(crate) struct Shown<'a> {
    #[serde(flatten)]
    trail: &'a Trail,
    script: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    shadows: Option<&'a Path>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ratings: Option<&'a RatingSummary>,
}

impl<'a> From<&'a Found> for Shown<'a> {
    fn from(found: &'a Found) -> Self {
        let (trail, shadows, ratings) = (&found.trail, found.shadows.as_deref(), found.ratings.as_ref());
        Self { trail, script: &trail.script, shadows, ratings }
    }
}

/// The trail's meta and where it is, its ratings, then its script.
pub(crate) fn render(found: &Found, now: DateTime<Utc>) -> String {
    let Found { trail, shadows, ratings } = found;
    let mut out = format!("trail {}\n", trail.name);
    match &trail.source {
        Source::Colony { saved_by, saved_from, saved_at } => {
            let at = saved_at.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M");
            let _ = write!(out, "saved for the colony by {saved_by} {} ago ({at})", short(since(*saved_at, now)));
            if let Some(run) = saved_from {
                let _ = write!(out, ", from run {run}");
            }
            out.push('\n');
        }
        Source::Local { path } => {
            let _ = writeln!(out, "local: {}", path.display());
        }
    }
    if let Some(path) = shadows {
        let _ = writeln!(out, "runs instead of the local trail of the same name, {}", path.display());
    }
    if !trail.description.is_empty() {
        let _ = writeln!(out, "description: {}", one_line(&trail.description, usize::MAX));
    }
    if !trail.phases.is_empty() {
        let _ = writeln!(out, "phases: {}", one_line(&trail.phases.join(", "), usize::MAX));
    }
    if !trail.waypoints.is_empty() {
        let _ = writeln!(out, "waypoints: {}", trail.waypoints.join(", "));
    }
    if let Some(ratings) = ratings.as_ref().filter(|ratings| ratings.counts.total() > 0) {
        out.push_str(&super::ratings::section(ratings, now));
    }
    out.push('\n');
    out.push_str(&trail.script);
    if !trail.script.ends_with('\n') {
        out.push('\n');
    }
    out
}
