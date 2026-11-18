//! `ants assistant set|clear|show`: the human of the CLI's credentials designates its assistant ant, or none, by a
//! grant its device signs ([`AssistantGrant`]), kept in [`HUMANS_BUCKET`] under the human's own name. See
//! [`ants_proto::Grants`] for how readers honour it. `ants assistant seal on|off` enrolls the assistant as a device of
//! the human by the same grant, or ends it, so the assistant opens and seals the human's sealed messages
//! ([`ants_daemon::secrets::sealing`]), with a key whose seed waits beside its credentials (`ants assistant key`).
//! `enable` and `disable` ([`dedicated`]) create and retire a dedicated assistant ant around the grant. `confirm` and
//! `allow` ([`consent`]) sign the human's consent to what the assistant decides for it.

use std::path::{Path, PathBuf};

use ants_daemon::config::Summary;
use ants_daemon::secrets::creds::{Finding, Kind};
use ants_daemon::secrets::sealing;
use ants_proto::{
    AssistantGrant, ColonyEntry, DeviceId, DeviceKey, HUMANS_BUCKET, SealingDevice, UserId, VerifiedGrant,
    assistant_grant_key,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::{self, kv};
use chrono::{DateTime, TimeDelta, Utc};
use nkeys::XKey;
use serde::Deserialize;

use crate::connection::Nats;
use crate::credentials::revoke;
use crate::people::{devices, humans};
use crate::{auth, colony};

pub(crate) mod consent;
pub(crate) mod dedicated;

pub(crate) use consent::{answer_order, orders_held_for};
pub use dedicated::{DisableArgs, EnableArgs};

/// How many times storing a record is tried again when another device of the human stored one meanwhile.
const CONFLICTS: usize = 5;

/// What the machine the CLI runs on knows of an ant: its `[[ant]]` in the config file, and its credentials in the
/// team's registry, where the account key is.
#[derive(Debug, Default)]
struct Local {
    entry: Option<AntEntry>,
    /// The template the registry records the ant's credentials were added from, if any.
    registry_template: Option<String>,
    /// Whether the registry records credentials of the ant.
    in_registry: bool,
}

/// What `set` reads of an `[[ant]]` entry; the rest of the file is the daemon's to check.
#[derive(Debug, Deserialize)]
struct AntEntry {
    name: String,
    template: Option<String>,
    workdir: Option<PathBuf>,
    home: Option<PathBuf>,
    #[serde(default)]
    allow: Vec<String>,
    #[serde(default)]
    allow_tools: bool,
}

impl Local {
    /// What the config file at `config` and the team's registry in `team` say of `ant`, either missing or unreadable
    /// telling nothing.
    fn read(ant: &UserId, config: &Path, team: Option<&Path>) -> Self {
        #[derive(Deserialize)]
        struct File {
            #[serde(default, rename = "ant")]
            ants: Vec<AntEntry>,
        }
        let entry = std::fs::read_to_string(config).ok().and_then(|text| match toml::from_str::<File>(&text) {
            Ok(file) => file.ants.into_iter().find(|a| a.name == ant.as_str()),
            Err(e) => {
                tracing::warn!("reading the ants of {}: {e}", config.display());
                None
            }
        });
        let issued = team.map(revoke::issued).transpose().unwrap_or_else(|e| {
            tracing::warn!("reading the team's registry: {e:#}");
            None
        });
        let issued = issued.unwrap_or_default().into_iter().filter(|i| i.name == ant.as_str()).collect::<Vec<_>>();
        let registry_template = issued.iter().find_map(|i| i.template.clone());
        Self { entry, registry_template, in_registry: !issued.is_empty() }
    }

    /// The template the ant was added from, if either source says so.
    fn template(&self) -> Option<&str> {
        self.entry.as_ref().and_then(|e| e.template.as_deref()).or(self.registry_template.as_deref())
    }
}

/// What `set` has to say of designating `ant`, as `local` and its colony entries `colony` know it: an error for a
/// templated ant, else the warnings.
fn check(ant: &UserId, local: &Local, colony: &[ColonyEntry]) -> Result<Vec<String>> {
    if let Some(template) = local.template() {
        bail!(
            "{ant} was added from template {template}: a templated ant cannot be an assistant (`ants assistant enable` \
             creates a dedicated one)"
        );
    }
    let mut warnings = Vec::new();
    let home = local.entry.as_ref().and_then(|e| e.home.as_deref());
    let workdirs = colony.iter().filter_map(|e| e.workdir.clone());
    let local_workdir = local.entry.as_ref().and_then(|e| e.workdir.as_ref()).filter(|w| !w.as_os_str().is_empty());
    let mut workdirs: Vec<String> = workdirs
        .chain(local_workdir.map(|w| w.display().to_string()))
        .filter(|w| !is_home(ant, Path::new(w), home))
        .collect();
    workdirs.sort();
    workdirs.dedup();
    if !workdirs.is_empty() {
        warnings.push(format!(
            "{ant} works in {}: an assistant holds its human's privileges, and the repositories it reads widen what \
             can steer it",
            workdirs.join(", ")
        ));
    }
    if let Some(entry) = &local.entry {
        let mut rules: Vec<&str> = entry.allow.iter().map(String::as_str).filter(|r| risky(r)).collect();
        if entry.allow_tools {
            rules.insert(0, "allow_tools");
        }
        if !rules.is_empty() {
            warnings.push(format!(
                "{ant} runs tools without asking ({}): fetched pages and command output can steer an assistant",
                rules.join(", ")
            ));
        }
    }
    if local.entry.is_none() && !local.in_registry && colony.is_empty() {
        warnings.push(format!(
            "{ant} is neither in the colony nor known on this machine: whether it was added from a template is not \
             checked"
        ));
    } else if local.entry.is_none() && !local.in_registry {
        warnings.push(format!(
            "neither the config file nor the team's registry on this machine lists {ant}: whether it was added from a \
             template is not checked"
        ));
    }
    Ok(warnings)
}

/// Whether the allow rule `rule` lets an ant run what can bring in untrusted content or act beyond reading: `*`,
/// `fetch` or `execute` requests.
fn risky(rule: &str) -> bool {
    let kind = rule.trim().split(':').next().unwrap_or_default();
    ["*", "fetch", "execute"].contains(&kind)
}

/// Whether `workdir` is the home of `ant`, `home` as its entry sets one, or as `~/.ants/agents/<ant>` by default on any
/// machine: the workdir of a dedicated assistant, holding no repository.
fn is_home(ant: &UserId, workdir: &Path, home: Option<&Path>) -> bool {
    home.is_some_and(|home| home == workdir) || workdir.ends_with(Path::new(".ants/agents").join(ant.as_str()))
}

/// The credentials of the human the CLI acts as, which sign its grant and its consent records: a human device's.
pub(crate) fn signing_creds<'a>(nats: &Nats<'a>) -> Result<&'a Path> {
    match nats.creds {
        Some(creds) if nats.me.role.is_human() && nats.device.is_some() => Ok(creds),
        _ => bail!("signing a grant needs a human device's credentials (`ants auth user NAME --human`)"),
    }
}

/// `ants assistant set ANT`.
pub async fn set(nats: &Nats<'_>, ant: &UserId) -> Result<()> {
    let creds = signing_creds(nats)?;
    let human = &nats.me.id;
    if ant == human {
        bail!("{human} cannot be its own assistant");
    }
    let (_, js) = nats.connect("ants-assistant").await?;
    if humans::load(&js, &nats.me).await.contains(ant) {
        bail!("{ant} is a human: an assistant is an ant");
    }
    let colony: Vec<ColonyEntry> = match colony::list(&js).await {
        Ok(entries) => entries.into_iter().filter(|e| e.ant == *ant).collect(),
        Err(e) => {
            tracing::warn!("reading the colony: {e:#}");
            Vec::new()
        }
    };
    let team = auth::default_dir().ok().filter(|dir| auth::holds_team(dir));
    let local = Local::read(ant, &crate::defaults::path()?, team.as_deref());
    for warning in check(ant, &local, &colony)? {
        println!("warning: {warning}");
    }
    let (before, _) = put(&js, creds, human, Some(ant.clone()), Sealing::Keep).await?;
    replaced(ant, human, before, None);
    println!(
        "It acts for {human} only while its credentials name {human} as the human it assists, as the team's account \
         signs them."
    );
    Ok(())
}

/// Say that `ant` is the assistant of `human` now, the grant replacing `before`, and that the enrollment of another
/// assistant `before` held ended, as [`ended_sealing`] does with the config file `config`.
fn replaced(ant: &UserId, human: &UserId, before: Option<VerifiedGrant>, config: Option<&Path>) {
    match before.as_ref().and_then(|g| g.assistant.as_ref()) {
        Some(before) if before == ant => println!("{ant} stays the assistant of {human}"),
        Some(before) => println!("{ant} is now the assistant of {human}, in place of {before}"),
        None => println!("{ant} is now the assistant of {human}"),
    }
    if let Some(before) = before.filter(|g| g.assistant.as_ref() != Some(ant)) {
        ended_sealing(&before, config);
    }
}

/// `ants assistant clear`.
pub async fn clear(nats: &Nats<'_>) -> Result<()> {
    let creds = signing_creds(nats)?;
    let human = &nats.me.id;
    let (_, js) = nats.connect("ants-assistant").await?;
    let (before, _) = put(&js, creds, human, None, Sealing::Off).await?;
    match before.as_ref().and_then(|g| g.assistant.as_ref()) {
        Some(assistant) => println!("{assistant} is no longer the assistant of {human}"),
        None => println!("{human} has no assistant"),
    }
    if let Some(before) = &before {
        ended_sealing(before, None);
    }
    Ok(())
}

/// `ants assistant seal on [--key KEY]`: enroll the human's assistant as one of its devices, with `key`, the public key
/// `ants assistant key` printed on the assistant's machine, else the key of the assistant on this machine, made if it
/// has none.
pub async fn seal_on(nats: &Nats<'_>, key: Option<DeviceKey>) -> Result<()> {
    let creds = signing_creds(nats)?;
    let human = &nats.me.id;
    let (_, js) = nats.connect("ants-assistant").await?;
    let account = devices::account_of(Some(creds))?.context("credentials without an issuer")?;
    let standing =
        read(&js, human).await?.and_then(|g| g.verify(&assistant_grant_key(human), &account, Utc::now()).ok());
    let Some(ant) = standing.and_then(|g| g.assistant) else {
        bail!("{human} has no assistant to enroll: `ants assistant set ANT` designates one");
    };
    let key = match key {
        Some(key) => key,
        None => {
            let creds = local_creds(&ant).with_context(|| {
                format!("`ants assistant key {ant}` on the machine running {ant} prints the key `--key` takes")
            })?;
            let (xkey, _) = key_of(&creds)?;
            auth::device_key(&xkey)?
        }
    };
    let (_, grant) = put(&js, creds, human, Some(ant.clone()), Sealing::Set(key.clone())).await?;
    let device = grant.sealing.map_or_default(|s| s.device.to_string());
    println!("{ant} is enrolled as device {device} of {human}, with encryption key {key}");
    println!(
        "Messages sealed for {human} from now on are sealed for {ant} too, which opens them and seals those it sends \
         for {human}, until `ants assistant seal off` or `ants assistant clear`."
    );
    Ok(())
}

/// `ants assistant seal off`: end the enrollment of the human's assistant as one of its devices, keeping it the
/// assistant.
pub async fn seal_off(nats: &Nats<'_>) -> Result<()> {
    let creds = signing_creds(nats)?;
    let human = &nats.me.id;
    let (_, js) = nats.connect("ants-assistant").await?;
    let account = devices::account_of(Some(creds))?.context("credentials without an issuer")?;
    let standing =
        read(&js, human).await?.and_then(|g| g.verify(&assistant_grant_key(human), &account, Utc::now()).ok());
    let Some(standing) = standing.filter(|g| g.sealing.is_some()) else {
        println!("{human} has no assistant enrolled as a device");
        return Ok(());
    };
    let (before, _) = put(&js, creds, human, standing.assistant.clone(), Sealing::Off).await?;
    if let Some(before) = &before {
        ended_sealing(before, None);
    }
    Ok(())
}

/// `ants assistant key ANT`: print the public key `ANT`, an ant on this machine, is enrolled with, making it if it has
/// none.
pub fn key(ant: &UserId) -> Result<()> {
    let creds = local_creds(ant)?;
    let (xkey, made) = key_of(&creds)?;
    let path = sealing::key_path(&creds);
    if made {
        eprintln!("made the encryption key of {ant}, its seed in {}", path.display());
    }
    println!("{}", xkey.public_key());
    Ok(())
}

/// The credentials of `ant` on this machine, as the config file names them.
fn local_creds(ant: &UserId) -> Result<PathBuf> {
    creds_in(ant, &crate::defaults::path()?)
}

/// The credentials of `ant`, as the config file `config` names them.
fn creds_in(ant: &UserId, config: &Path) -> Result<PathBuf> {
    match Summary::load(config)?.creds_of(ant) {
        Some(Some(creds)) => Ok(creds.to_path_buf()),
        Some(None) => bail!("{ant} runs without credentials in {}, so it holds no key", config.display()),
        None => bail!("{ant} is not an ant of {}", config.display()),
    }
}

/// The encryption key of the ant whose credentials are `creds` ([`sealing::load_key`]), and whether it was just made,
/// its seed written beside them.
fn key_of(creds: &Path) -> Result<(XKey, bool)> {
    if let Some(xkey) = sealing::load_key(creds)? {
        return Ok((xkey, false));
    }
    let xkey = XKey::new();
    let seed = xkey.seed().map_err(|e| anyhow::anyhow!("encoding an encryption key: {e}"))?;
    auth::write_secret(&sealing::key_path(creds), &format!("{seed}\n"))?;
    Ok((xkey, true))
}

/// Say that the enrollment `before` held, if any, ended, and what of the key's seed, deleted where the config file
/// `config` (default: the CLI's) names the assistant's credentials.
fn ended_sealing(before: &VerifiedGrant, config: Option<&Path>) {
    let (Some(assistant), Some(sealing)) = (&before.assistant, &before.sealing) else { return };
    println!(
        "{assistant} is no longer device {} of {}: nothing is sealed for it from now on, and it opens nothing",
        sealing.device, before.human
    );
    let creds = match config {
        Some(config) => creds_in(assistant, config),
        None => local_creds(assistant),
    };
    let path = creds.ok().map(|creds| sealing::key_path(&creds)).filter(|p| p.exists());
    match path.map(|path| std::fs::remove_file(&path).map(|()| path)) {
        Some(Ok(path)) => println!("Deleted the seed of its encryption key, {}.", path.display()),
        Some(Err(e)) => println!("warning: the seed of its encryption key stays: {e}"),
        None => println!(
            "The seed of its encryption key stays beside its credentials, as {assistant}.xk, on the machine running \
             it: deleting it there leaves the messages sealed for it before unopened by anyone holding it."
        ),
    }
}

/// `ants assistant show [HUMAN]`.
pub async fn show(nats: &Nats<'_>, human: Option<UserId>) -> Result<()> {
    let human = human.unwrap_or_else(|| nats.me.id.clone());
    let (_, js) = nats.connect("ants-assistant").await?;
    let Some(grant) = read(&js, &human).await? else {
        println!("{human} has no assistant");
        return Ok(());
    };
    let Some(account) = &nats.account else {
        let named = grant.assistant.as_ref().map_or_else(|| "no assistant".to_owned(), ToString::to_string);
        println!("{human}: {named}, not verified without credentials");
        return Ok(());
    };
    match grant.verify(&assistant_grant_key(&human), account, Utc::now()) {
        Ok(verified) => println!("{}", describe(&verified)),
        Err(e) => println!("{human} has no assistant: its grant is not honoured ({e})"),
    }
    Ok(())
}

/// The ant the grant of `human` designates, read and verified now for the team of `account` (the issuer of the CLI's
/// credentials): none without an account or a grant that verifies, and none after a withdrawal. Whether the ant's own
/// credentials name `human` is for the readers of what the ant does for `human` to check, not for whoever writes to it.
pub(crate) async fn designated(js: &jetstream::Context, account: Option<&str>, human: &UserId) -> Option<UserId> {
    let account = account?;
    let grant = match read(js, human).await {
        Ok(grant) => grant?,
        Err(e) => {
            tracing::warn!("reading the assistant of {human}: {e:#}");
            return None;
        }
    };
    match grant.verify(&assistant_grant_key(human), account, Utc::now()) {
        Ok(verified) => verified.assistant,
        Err(e) => {
            tracing::warn!("not honouring the assistant grant of {human}: {e}");
            None
        }
    }
}

/// A verified record in a line.
fn describe(grant: &VerifiedGrant) -> String {
    let when = grant.issued_at.format("%Y-%m-%d %H:%M UTC");
    match &grant.assistant {
        Some(assistant) => {
            let enrolled = grant.sealing.as_ref().map_or_default(|s| {
                format!(", enrolled as device {} with encryption key {} for sealed messages", s.device, s.xkey)
            });
            format!("{} has assistant {assistant}, granted {when} by device {}{enrolled}", grant.human, grant.device)
        }
        None => format!("{} has no assistant, cleared {when} by device {}", grant.human, grant.device),
    }
}

/// The record of `human` stored, unverified; none without one, or without the bucket.
async fn read(js: &jetstream::Context, human: &UserId) -> Result<Option<AssistantGrant>> {
    let Ok(kv) = js.get_key_value(HUMANS_BUCKET).await else { return Ok(None) };
    let Some(value) = kv.get(assistant_grant_key(human)).await.context("reading the assistant grant")? else {
        return Ok(None);
    };
    serde_json::from_slice(&value).map(Some).context("a malformed assistant grant")
}

/// What a record [`put`] stores does with the enrollment of the assistant as a device of its human.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Sealing {
    /// Keep the one standing, if the assistant stays.
    Keep,
    /// Enroll it with this key, as the same device if it already is with it.
    Set(DeviceKey),
    Off,
}

/// Sign the record of `assistant` (none: a withdrawal) by `human` with the device credentials `creds`, enrolling it
/// as `sealing` says, and store it in place of the one stored, issued after it. Returns the record it replaced, if one
/// was stored and verifies, and the one it stored.
async fn put(
    js: &jetstream::Context,
    creds: &Path,
    human: &UserId,
    assistant: Option<UserId>,
    sealing: Sealing,
) -> Result<(Option<VerifiedGrant>, AssistantGrant)> {
    let (jwt, key, account) = device_creds(creds, human)?;
    let kv = humans::ensure(js).await?;
    let grant_key = assistant_grant_key(human);
    for _ in 0..CONFLICTS {
        let stored = kv.entry(&grant_key).await.context("reading the assistant grant")?;
        let (revision, before) = match &stored {
            Some(entry) if entry.operation == kv::Operation::Put => {
                (Some(entry.revision), serde_json::from_slice::<AssistantGrant>(&entry.value).ok())
            }
            Some(entry) => (Some(entry.revision), None),
            None => (None, None),
        };
        let now = Utc::now();
        let issued_at = after(now, before.as_ref().map(|g| g.issued_at));
        let verified = before.and_then(|g| g.verify(&grant_key, &account, now).ok());
        let kept = verified.as_ref().filter(|g| g.assistant.is_some() && g.assistant == assistant);
        let kept = kept.and_then(|g| g.sealing.clone());
        let enrolled = match (&sealing, assistant.as_ref()) {
            (_, None) | (Sealing::Off, _) => None,
            (Sealing::Keep, _) => kept,
            (Sealing::Set(xkey), _) => {
                let device = kept.filter(|s| s.xkey == *xkey).map_or_else(DeviceId::generate, |s| s.device);
                Some(SealingDevice { device, xkey: xkey.clone() })
            }
        };
        let mut grant =
            AssistantGrant::new(human.clone(), assistant.clone(), issued_at, jwt.clone()).with_sealing(enrolled);
        grant.signature = auth::sign(&key, &grant.signed())?;
        let value = serde_json::to_vec(&grant)?.into();
        let stored = match revision {
            Some(revision) => kv.update(&grant_key, value, revision).await.map_err(|e| e.kind().to_string()),
            None => kv.create(&grant_key, value).await.map_err(|e| e.kind().to_string()),
        };
        match stored {
            Ok(_) => return Ok((verified, grant)),
            Err(e) if e.contains("wrong last") || e.contains("already exists") => {}
            Err(e) => bail!("storing the assistant grant in {HUMANS_BUCKET}: {e}"),
        }
    }
    bail!("storing the assistant grant: another device of {human} kept storing one")
}

/// The user JWT, key and account of the device credentials `creds` of `human`, which sign its records.
fn device_creds(creds: &Path, human: &UserId) -> Result<(String, nkeys::KeyPair, String)> {
    let (jwt, key) = auth::read_creds(creds)?;
    let finding = Finding::from_jwt(&jwt).with_context(|| format!("credentials {}", creds.display()))?;
    if finding.kind != Kind::Human || finding.name != human.as_str() {
        bail!("credentials {} are not a device of {human}'s", creds.display());
    }
    let account = devices::account_of(Some(creds))?.context("credentials without an issuer")?;
    Ok((jwt, key, account))
}

/// When a record replacing one issued at `before` is issued: `now`, or just after `before` when this clock is behind
/// the one that issued it, so that readers take the new one for the newer.
fn after(now: DateTime<Utc>, before: Option<DateTime<Utc>>) -> DateTime<Utc> {
    match before {
        Some(before) if before >= now && before <= now + ants_proto::GRANT_CLOCK_SKEW => {
            before + TimeDelta::microseconds(1)
        }
        _ => now,
    }
}

#[cfg(test)]
mod tests;
