//! The saved trails a run names: colony-wide in [`TRAILS_BUCKET`], which humans put and `ants signer` puts for ants,
//! else in the machine's `~/.ants/trails/<name>.py`. The bucket wins a name both have, so every conductor of the colony runs the same
//! trail by that name.
//!
//! An ant saves a trail as its own ([`Trails::save`]): under a new name, or replacing one it saved, and removes only
//! its own ([`Trails::remove_saved`]). Each write expects the entry it checked, so a trail another saved meanwhile is
//! never replaced nor removed; the daemon asks `ants signer` to make it ([`keeper`]), which checks the same again.

use std::path::PathBuf;
use std::time::Duration;

use ants_proto::{
    JsDomain, RunId, RunState, SaveTrailOutcome, SaveTrailRequest, SavedTrail, Saver, TRAILS_BUCKET, TrailChange,
    TrailName, UserId, script_sha256,
};
use async_nats::jetstream;
use chrono::{DateTime, Utc};

use super::{SaveThrough, Trails, keeper};
use crate::records::direct_kv::{DirectKv, Lookup};

/// Why a save or a removal reaches no saved trail of the colony.
const UNREACHABLE: &str = "the colony's saved trails are unreachable from this ant's daemon";
/// Why a save or a removal went unanswered.
const NO_KEEPER: &str = "`ants signer` did not answer, and only it saves an ant's trails: run it where the account key \
                         is (scripts/install-units.sh --signer), then ask again";

/// Where the script of a trail an ant saves comes from.
#[derive(Debug, Clone)]
pub enum SaveFrom {
    /// The script as given.
    Script(String),
    /// The script of a finished run of an inline script the ant conducted, recorded as the trail's source.
    Run(RunId),
}

/// A trail an ant saves for the colony as its own ([`Trails::save`]).
#[derive(Debug, Clone)]
pub struct SaveTrail {
    pub name: TrailName,
    pub from: SaveFrom,
    /// Replace the trail the ant saved under `name` before; without it, that name is refused.
    pub replace: bool,
}

/// A save checked ([`Trails::prepare_save`]), for [`Trails::commit_save`] to store.
#[derive(Debug, Clone)]
pub struct PreparedSave {
    trail: SavedTrail,
    /// The stream sequence of the entry it replaces; none for a name the colony has no trail of.
    last: Option<u64>,
    before: Option<SavedTrail>,
    run_state: Option<RunState>,
}

impl PreparedSave {
    /// The trail to store.
    pub fn trail(&self) -> &SavedTrail {
        &self.trail
    }
}

/// A trail an ant saved ([`Trails::commit_save`]).
#[derive(Debug, Clone)]
pub struct SaveOutcome {
    pub trail: SavedTrail,
    /// The trail of the ant's it replaced.
    pub replaced: Option<SavedTrail>,
    /// How the run it was saved from ended.
    pub run_state: Option<RunState>,
}

/// What the colony holds under a trail's name.
enum Entry {
    Absent,
    Saved(SavedTrail),
    /// A value that is no trail saved under that name, for why.
    Unreadable(String),
}

pub(super) struct Saved {
    kv: Option<DirectKv>,
    dir: Option<PathBuf>,
    /// The ant saving.
    ant: UserId,
    /// The connection asking `ants signer` to save; none to save as it would, on a server without credentials.
    signer: Option<async_nats::Client>,
}

/// A saved trail, as a search for trails reads it.
#[derive(Debug, Clone)]
pub struct SavedEntry {
    pub name: TrailName,
    pub description: String,
    /// The [`script_sha256`] of its script.
    pub script: String,
    /// Who saved it; none for a trail local to this machine.
    pub saver: Option<Saver>,
    /// The run whose script it was saved from.
    pub saved_from: Option<RunId>,
    /// Whether it is this machine's, in `~/.ants/trails`, the colony having none of its name.
    pub local: bool,
}

impl Saved {
    pub(super) async fn new(
        js: &jetstream::Context,
        js_domain: Option<&JsDomain>,
        dir: Option<PathBuf>,
        ant: UserId,
        through: SaveThrough,
    ) -> Self {
        let kv = match DirectKv::whole(js, js_domain, TRAILS_BUCKET).await {
            Ok(kv) => Some(kv.past_relays()),
            Err(e) => {
                tracing::warn!(error = format!("{e:#}"), "reading no saved trails of the colony");
                None
            }
        };
        let signer = match through {
            SaveThrough::Signer => Some(js.client()),
            SaveThrough::Bucket => None,
        };
        Self { kv, dir, ant, signer }
    }

    /// The script of the trail saved as `name`: the colony's, else this machine's when the colony has none of that name
    /// or the bus keeps no saved trails. Refused when the colony's are unread, as its trail would win.
    pub(super) async fn script(&self, name: &TrailName) -> Result<String, String> {
        let unread = |e: &str| format!("trail {name} is not run: the saved trails of the colony are unread ({e})");
        let kv = self.kv.as_ref().ok_or_else(|| unread(UNREACHABLE))?;
        match kv.lookup(name.as_str()).await.map_err(|e| unread(&format!("{e:#}")))? {
            Lookup::Value(value) => {
                let saved: SavedTrail =
                    serde_json::from_slice(&value).map_err(|e| format!("reading saved trail {name}: {e}"))?;
                saved.check_key(name.as_str()).map_err(|e| e.to_string())?;
                return Ok(saved.script);
            }
            Lookup::NoValue | Lookup::NoBucket => {}
        }
        match self.local(name).await {
            Some(path) => {
                tokio::fs::read_to_string(&path).await.map_err(|e| format!("reading {}: {e}", path.display()))
            }
            None => Err(format!("no trail is saved as {name}")),
        }
    }

    /// The file of this machine's trail `name`, when it has one.
    async fn local(&self, name: &TrailName) -> Option<PathBuf> {
        let path = self.dir.as_ref()?.join(format!("{name}.py"));
        tokio::fs::metadata(&path).await.is_ok_and(|meta| meta.is_file()).then_some(path)
    }

    fn kv(&self) -> Result<&DirectKv, String> {
        self.kv.as_ref().ok_or_else(|| UNREACHABLE.to_owned())
    }

    /// The colony's entry under `name`, and the stream sequence of the key's last message (0 for none), which
    /// [`Saved::write`] and [`Saved::delete`] expect.
    async fn lookup(&self, name: &TrailName) -> Result<(Entry, u64), String> {
        let (found, last) =
            self.kv()?.lookup_last(name.as_str()).await.map_err(|e| format!("reading saved trail {name}: {e:#}"))?;
        let entry = match found {
            Lookup::NoBucket => return Err(format!("{UNREACHABLE}: the bus keeps none (`ants init` creates them)")),
            Lookup::NoValue => Entry::Absent,
            Lookup::Value(value) => match serde_json::from_slice::<SavedTrail>(&value) {
                Ok(trail) => match trail.check_key(name.as_str()) {
                    Ok(()) => Entry::Saved(trail),
                    Err(e) => Entry::Unreadable(e.to_string()),
                },
                Err(e) => Entry::Unreadable(e.to_string()),
            },
        };
        Ok((entry, last))
    }

    /// Store `trail` under its name: as a new trail with no `last`, else replacing the key's last message, at stream
    /// sequence `last`. Whether it stored the trail: false when the key changed meanwhile.
    async fn write(&self, trail: &SavedTrail, last: Option<u64>) -> Result<bool, String> {
        let change = TrailChange::Save { trail: trail.clone(), last };
        self.change(change).await.map_err(|e| format!("saving trail {}: {e}", trail.name))
    }

    /// Remove the trail `name`, if the key's last message is still the one at stream sequence `last`. Whether it
    /// removed it: false when the key changed meanwhile.
    async fn delete(&self, name: &TrailName, last: u64) -> Result<bool, String> {
        let change = TrailChange::Remove { name: name.clone(), last };
        self.change(change).await.map_err(|e| format!("removing trail {name}: {e}"))
    }

    /// Make `change` as the ant, through `ants signer` or, with no signer, as it would: whether it was made.
    async fn change(&self, change: TrailChange) -> Result<bool, String> {
        let request = SaveTrailRequest::new(self.ant.clone(), change);
        let outcome = match &self.signer {
            Some(client) => {
                keeper::request(client, &request).await.map_err(|e| format!("{e:#}"))?.ok_or(NO_KEEPER.to_owned())?
            }
            None => keeper::answer(self.kv()?, &request).await.map_err(|e| format!("{e:#}"))?,
        };
        match outcome {
            SaveTrailOutcome::Done => Ok(true),
            SaveTrailOutcome::Changed => Ok(false),
            SaveTrailOutcome::Refused { why } => Err(why),
        }
    }

    /// Every saved trail, the colony's then this machine's whose names the colony has not, each by name. Fails when the
    /// colony's are unread; a local trail that is unread or no trail is left out.
    pub(super) async fn all(&self) -> Result<Vec<SavedEntry>, String> {
        let mut all = Vec::new();
        if let Some(kv) = &self.kv {
            kv.scan(">", |key, value| match serde_json::from_slice::<SavedTrail>(value) {
                Ok(trail) if trail.check_key(key).is_ok() => all.push(SavedEntry {
                    saver: Some(trail.saved_by.clone()),
                    saved_from: trail.saved_from.clone(),
                    script: script_sha256(&trail.script),
                    name: trail.name,
                    description: trail.description,
                    local: false,
                }),
                _ => tracing::debug!(key, "skipping a saved trail that does not read"),
            })
            .await
            .map_err(|e| format!("the saved trails of the colony are unread: {e:#}"))?;
        }
        if let Some(dir) = &self.dir {
            all.extend(listed_locally(dir, &all).await);
        }
        all.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(all)
    }
}

impl Trails {
    /// Check a save of a trail as the ant's own, the checks of `ants trail save` and these: a run's script only from a
    /// finished run of an inline script the ant conducted, no name of this machine's trails, and a name the colony has
    /// no trail of, or one the ant saved, with `replace`.
    pub async fn prepare_save(&self, ask: SaveTrail) -> Result<PreparedSave, String> {
        let saved = &self.inner.saved;
        saved.kv()?;
        let SaveTrail { name, from, replace } = ask;
        let (script, saved_from, run_state) = match from {
            SaveFrom::Script(script) => (script, None, None),
            SaveFrom::Run(run) => {
                let header = self.run(&run).await?.ok_or_else(|| format!("no run {run} conducted by this ant"))?;
                if !header.finished() {
                    let state = unfinished(&header.state);
                    return Err(format!(
                        "run {run} has not finished ({state}): save it once it ends, or pass `script`"
                    ));
                }
                if let Some(trail) = header.first_trail() {
                    return Err(format!(
                        "run {run} ran saved trail {trail}, saved already; to keep a changed copy under a name of this \
                         ant's, pass its `script`"
                    ));
                }
                let leg = header.route.first().ok_or_else(|| format!("run {run} records no script"))?;
                (leg.script.clone(), Some(run), Some(header.state))
            }
        };
        let mine = Saver::Ant { user: self.inner.ant.clone() };
        let mut trail = SavedTrail {
            name,
            description: String::new(),
            phases: Vec::new(),
            waypoints: Vec::new(),
            script,
            saved_by: mine.clone(),
            saved_from,
            saved_at: Utc::now(),
        };
        // The name and the script's size first, so that a script too long is not compiled.
        trail.check().map_err(|e| e.to_string())?;
        let meta = ants_trail::check(&trail.script).map_err(|e| match e {
            ants_trail::TrailError::Syntax(traceback) => format!("the script does not compile:\n{traceback}"),
            ants_trail::TrailError::Meta(why) => why,
        })?;
        (trail.description, trail.phases, trail.waypoints) = (meta.description, meta.phases, meta.waypoints);
        trail.check().map_err(|e| e.to_string())?;
        let name = &trail.name;
        if let Some(path) = saved.local(name).await {
            return Err(format!(
                "this machine's trails hold one named {name}, {}, which a colony trail of that name would replace in \
                 runs: pick another name",
                path.display()
            ));
        }
        let (entry, last) = saved.lookup(name).await?;
        let before = match entry {
            Entry::Absent => None,
            Entry::Saved(before) if before.saved_by == mine => {
                if !replace {
                    let ago = age(before.saved_at, trail.saved_at);
                    return Err(format!(
                        "this ant saved trail {name} {ago} ago{}: pass `replace: true` to replace it",
                        from_run(&before)
                    ));
                }
                Some(before)
            }
            Entry::Saved(before) => {
                return Err(format!(
                    "trail {name} was saved by {}; an ant replaces only the trails it saved: save under another name",
                    before.saved_by
                ));
            }
            Entry::Unreadable(e) => return Err(unreadable(name, &e)),
        };
        let last = before.is_some().then_some(last);
        Ok(PreparedSave { trail, last, before, run_state })
    }

    /// Store a save [`Trails::prepare_save`] checked, refused when the colony's entry of its name changed since.
    pub async fn commit_save(&self, prepared: PreparedSave) -> Result<SaveOutcome, String> {
        let PreparedSave { trail, last, before, run_state } = prepared;
        if !self.inner.saved.write(&trail, last).await? {
            return Err(format!(
                "trail {} changed while saving and nothing was saved: call save_trail again",
                trail.name
            ));
        }
        Ok(SaveOutcome { trail, replaced: before, run_state })
    }

    /// Save a trail as the ant's own: [`Trails::prepare_save`], then [`Trails::commit_save`].
    pub async fn save(&self, ask: SaveTrail) -> Result<SaveOutcome, String> {
        let prepared = self.prepare_save(ask).await?;
        self.commit_save(prepared).await
    }

    /// Remove the colony's trail `name`, which the ant saved, leaving its tips: refused for a name the colony saved no
    /// trail of, an entry unreadable, a trail a human or another ant saved, and an entry changed since it was read.
    /// Returns the trail removed.
    pub async fn remove_saved(&self, name: &TrailName) -> Result<SavedTrail, String> {
        let saved = &self.inner.saved;
        let (entry, last) = saved.lookup(name).await?;
        let trail = match entry {
            Entry::Saved(trail) if trail.saved_by == (Saver::Ant { user: self.inner.ant.clone() }) => trail,
            Entry::Saved(trail) => {
                return Err(format!(
                    "trail {name} was saved by {}; an ant removes only the trails it saved: ask a human",
                    trail.saved_by
                ));
            }
            Entry::Unreadable(e) => return Err(unreadable(name, &e)),
            Entry::Absent => {
                return Err(match saved.local(name).await {
                    Some(path) => format!(
                        "the colony saved no trail {name}; {} is this machine's, no colony trail, and goes with its file",
                        path.display()
                    ),
                    None => format!("the colony saved no trail {name}"),
                });
            }
        };
        if !saved.delete(name, last).await? {
            return Err(format!(
                "trail {name} changed while removing it and nothing was removed: call remove_trail again"
            ));
        }
        Ok(trail)
    }
}

/// The trails of `dir`, `<name>.py` files that are trails, but for those of a name in `colony`.
async fn listed_locally(dir: &std::path::Path, colony: &[SavedEntry]) -> Vec<SavedEntry> {
    let mut found = Vec::new();
    let Ok(mut entries) = tokio::fs::read_dir(dir).await else { return found };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().is_none_or(|ext| ext != "py") {
            continue;
        }
        let Some(name) = path.file_stem().and_then(|stem| stem.to_str()?.parse::<TrailName>().ok()) else { continue };
        if colony.iter().any(|trail| trail.name == name) {
            continue;
        }
        let Ok(script) = tokio::fs::read_to_string(&path).await else { continue };
        let Some(description) = super::script_description(&script) else { continue };
        found.push(SavedEntry {
            name,
            description,
            script: script_sha256(&script),
            saver: None,
            saved_from: None,
            local: true,
        });
    }
    found
}

/// How long ago `then` was at `now`, in its largest whole unit: `3d`, `2h`, `5m`.
pub(crate) fn age(then: DateTime<Utc>, now: DateTime<Utc>) -> String {
    let secs = (now - then).to_std().unwrap_or_default().as_secs().max(60);
    let unit = [86_400, 3_600, 60].into_iter().find(|unit| secs >= *unit).unwrap_or(60);
    ants_proto::duration::format(Duration::from_secs(secs / unit * unit))
}

/// The run `trail` was saved from, as `, from run R`; nothing for none.
pub(crate) fn from_run(trail: &SavedTrail) -> String {
    trail.saved_from.as_ref().map_or_default(|run| format!(", from run {run}"))
}

fn unreadable(name: &TrailName, why: &str) -> String {
    format!("trail {name} is saved but unreadable ({why}); only a human replaces or removes it")
}

/// How a run not finished stands.
fn unfinished(state: &RunState) -> String {
    match state {
        RunState::Waiting => "waiting on humans".to_owned(),
        RunState::Stopped { by } => format!("stopped by {by}"),
        _ => "running".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::UserId;
    use ants_test_support::NatsServer;

    use super::*;
    use crate::bus;

    #[tokio::test]
    async fn a_save_expecting_an_older_revision_saves_nothing() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
        js.create_stream(crate::hub::trails_config()).await.unwrap();
        let dev_a: UserId = "dev-a".parse().unwrap();
        let saved = Saved::new(&js, None, None, dev_a.clone(), SaveThrough::Bucket).await;
        let name: TrailName = "deploy".parse().unwrap();
        let trail = SavedTrail {
            name: name.clone(),
            description: String::new(),
            phases: Vec::new(),
            waypoints: Vec::new(),
            script: "agent('build')".to_owned(),
            saved_by: Saver::Ant { user: dev_a },
            saved_from: None,
            saved_at: Utc::now(),
        };
        assert!(matches!(saved.lookup(&name).await.unwrap(), (Entry::Absent, 0)));
        assert!(saved.write(&trail, None).await.unwrap());
        assert!(saved.write(&trail, None).await.unwrap(), "a repeat of the save saved it");
        let later = SavedTrail { saved_at: Utc::now() + chrono::Duration::seconds(1), ..trail.clone() };
        assert!(!saved.write(&later, None).await.unwrap(), "a new trail over a saved one saves nothing");
        let (Entry::Saved(found), last) = saved.lookup(&name).await.unwrap() else { panic!("saved") };
        assert_eq!(found, trail);

        let elsewhere = SavedTrail { saved_by: Saver::Human { user: UserId::human() }, ..trail.clone() };
        let kv = DirectKv::whole(&js, None, TRAILS_BUCKET).await.unwrap();
        kv.put(name.as_str(), serde_json::to_vec(&elsewhere).unwrap()).await.unwrap();
        assert!(!saved.write(&later, Some(last)).await.unwrap(), "replacing a revision another replaced");
        assert!(!saved.delete(&name, last).await.unwrap(), "removing a revision another replaced");
        let (Entry::Saved(found), now) = saved.lookup(&name).await.unwrap() else { panic!("saved") };
        assert_eq!(found, elsewhere);
        let refused = saved.delete(&name, now).await.unwrap_err();
        assert!(refused.contains("saved by"), "{refused}");
        kv.delete(name.as_str()).await.unwrap();
        assert!(matches!(saved.lookup(&name).await.unwrap().0, Entry::Absent));
        assert!(saved.write(&trail, None).await.unwrap(), "a new trail over a removed one");
        let (_, now) = saved.lookup(&name).await.unwrap();
        assert!(saved.delete(&name, now).await.unwrap());
        assert!(saved.delete(&name, now).await.unwrap(), "a repeat of the removal removed it");

        kv.put("other", serde_json::to_vec(&trail).unwrap()).await.unwrap();
        let (entry, _) = saved.lookup(&"other".parse().unwrap()).await.unwrap();
        assert!(matches!(entry, Entry::Unreadable(why) if why.contains("found at other")));
    }

    #[tokio::test]
    async fn a_failed_read_of_the_colony_trails_runs_no_local_trail() {
        let Some(server) = NatsServer::start() else { return };
        let (client, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
        let dir = ants_test_support::scratch_dir("saved-trail-unread");
        std::fs::write(dir.join("deploy.py"), "agent('local')").unwrap();
        let saved = Saved::new(&js, None, Some(dir), "dev-a".parse().unwrap(), SaveThrough::Bucket).await;
        let name: TrailName = "deploy".parse().unwrap();
        assert_eq!(saved.script(&name).await.unwrap(), "agent('local')", "a bus keeping no saved trails");

        let mut gets = client.subscribe(format!("$JS.API.DIRECT.GET.KV_{TRAILS_BUCKET}")).await.unwrap();
        let answering = client.clone();
        tokio::spawn(async move {
            use futures::StreamExt as _;
            while let Some(get) = gets.next().await {
                answering.publish(get.reply.unwrap(), "no stream message".into()).await.unwrap();
            }
        });
        client.flush().await.unwrap();
        let refused = saved.script(&name).await.unwrap_err();
        assert!(refused.contains("the saved trails of the colony are unread"), "{refused}");
        assert!(refused.contains("reading deploy"), "names the error: {refused}");
    }
}
