//! `ants assistant set|clear|show`: the human of the CLI's credentials designates its assistant ant, or none, by a
//! grant its device signs ([`AssistantGrant`]), kept in [`HUMANS_BUCKET`] under the human's own name. See
//! [`ants_proto::Grants`] for how readers honour it. `ants assistant seal on|off` enrolls the assistant as a device of
//! the human by the same grant, or ends it, so the assistant opens and seals the human's sealed messages
//! ([`ants_daemon::sealing`]), with a key whose seed waits beside its credentials (`ants assistant key`).
//! `enable` and `disable` ([`dedicated`]) create and retire a dedicated assistant ant around the grant. `confirm` and
//! `allow` ([`consent`]) sign the human's consent to what the assistant decides for it.

use std::path::{Path, PathBuf};

use ants_daemon::config::Summary;
use ants_daemon::creds::{Finding, Kind};
use ants_daemon::sealing;
use ants_proto::{
    AssistantGrant, DeviceId, DeviceKey, HUMANS_BUCKET, RosterEntry, SealingDevice, UserId, VerifiedGrant,
    assistant_grant_key,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::{self, kv};
use chrono::{DateTime, TimeDelta, Utc};
use nkeys::XKey;
use serde::Deserialize;

use crate::cli::{AssistantCommand, SealCommand};
use crate::{Nats, auth, devices, humans, revoke, roster};

mod consent;
mod dedicated;

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

/// What `set` has to say of designating `ant`, as `local` and its roster entries `roster` know it: an error for a
/// templated ant, else the warnings.
fn check(ant: &UserId, local: &Local, roster: &[RosterEntry]) -> Result<Vec<String>> {
    if let Some(template) = local.template() {
        bail!(
            "{ant} was added from template {template}: a templated ant cannot be an assistant (`ants assistant enable` \
             creates a dedicated one)"
        );
    }
    let mut warnings = Vec::new();
    let home = local.entry.as_ref().and_then(|e| e.home.as_deref());
    let workdirs = roster.iter().filter_map(|e| e.workdir.clone());
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
    if local.entry.is_none() && !local.in_registry && roster.is_empty() {
        warnings.push(format!(
            "{ant} is neither in the roster nor known on this machine: whether it was added from a template is not \
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

/// `ants assistant`, with the connection `nats` gives.
pub async fn run<'a>(command: AssistantCommand, nats: impl Fn() -> Result<Nats<'a>>) -> Result<()> {
    match command {
        AssistantCommand::Enable(args) => dedicated::enable(&nats()?, *args).await,
        AssistantCommand::Disable(args) => dedicated::disable(&nats()?, args).await,
        AssistantCommand::Set { ant } => set(&nats()?, &ant).await,
        AssistantCommand::Clear => clear(&nats()?).await,
        AssistantCommand::Show { human } => show(&nats()?, human).await,
        AssistantCommand::Seal(SealCommand::On { key }) => seal_on(&nats()?, key).await,
        AssistantCommand::Seal(SealCommand::Off) => seal_off(&nats()?).await,
        AssistantCommand::Key { ant } => key(&ant),
        AssistantCommand::Confirm { act, deny } => Box::pin(consent::confirm(&nats()?, act.as_deref(), deny)).await,
        AssistantCommand::Allow { rules, remove, clear } => consent::allow(&nats()?, rules, remove, clear).await,
    }
}

/// Whether the allow rule `rule` lets an ant run what can bring in untrusted content or act beyond reading: `*`,
/// `fetch` or `execute` requests.
fn risky(rule: &str) -> bool {
    let kind = rule.trim().split(':').next().unwrap_or_default();
    ["*", "fetch", "execute"].contains(&kind)
}

/// Whether `workdir` is the home of `ant`, `home` as its entry sets one, or as `~/.ants/<ant>` by default on any
/// machine: the workdir of a dedicated assistant, holding no repository.
fn is_home(ant: &UserId, workdir: &Path, home: Option<&Path>) -> bool {
    home.is_some_and(|home| home == workdir) || workdir.ends_with(Path::new(".ants").join(ant.as_str()))
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
    let roster: Vec<RosterEntry> = match roster::list(&js).await {
        Ok(entries) => entries.into_iter().filter(|e| e.ant == *ant).collect(),
        Err(e) => {
            tracing::warn!("reading the roster: {e:#}");
            Vec::new()
        }
    };
    let team = auth::default_dir().ok().filter(|dir| auth::holds_team(dir));
    let local = Local::read(ant, &crate::defaults::path()?, team.as_deref());
    for warning in check(ant, &local, &roster)? {
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
mod tests {
    use std::time::Duration;

    use ants_daemon::bus;
    use ants_daemon::humans::{Registry, Withdrawal};
    use ants_proto::{Acting, Role};

    use super::*;
    use crate::auth::tests::Server;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    #[test]
    fn templated_ants_are_refused_and_wide_ones_warned_of() {
        let dir = ants_test_support::scratch_dir("assistant-check");
        let config = dir.join("config.toml");
        std::fs::write(
            &config,
            r#"
            harness = "claude"
            [[ant]]
            name = "worker-1"
            template = "worker"
            [[ant]]
            name = "coder"
            workdir = "~/src/repo"
            allow = ["read", "execute:cargo test*", "fetch"]
            [[ant]]
            name = "pa"
            whoami = "Assist."
            "#,
        )
        .unwrap();
        let err = check(&p("worker-1"), &Local::read(&p("worker-1"), &config, None), &[]).unwrap_err().to_string();
        assert!(err.contains("added from template worker"), "{err}");

        let warnings = check(&p("coder"), &Local::read(&p("coder"), &config, None), &[]).unwrap();
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        assert!(warnings[0].contains("works in ~/src/repo"), "{warnings:?}");
        assert!(warnings[1].contains("(execute:cargo test*, fetch)"), "{warnings:?}");

        assert_eq!(check(&p("pa"), &Local::read(&p("pa"), &config, None), &[]).unwrap(), Vec::<String>::new());
        let unknown = check(&p("other"), &Local::read(&p("other"), &config, None), &[]).unwrap();
        assert!(unknown[0].contains("is not checked"), "{unknown:?}");
    }

    #[test]
    fn a_replacing_record_is_issued_after_the_one_stored() {
        let now = Utc::now();
        assert_eq!(after(now, None), now);
        assert_eq!(after(now, Some(now - TimeDelta::seconds(5))), now);
        assert_eq!(
            after(now, Some(now + TimeDelta::seconds(5))),
            now + TimeDelta::seconds(5) + TimeDelta::microseconds(1)
        );
    }

    /// What a human sends naming no one goes to its assistant while the grant naming it verifies, else the messenger.
    #[tokio::test]
    async fn a_human_s_unaddressed_asks_go_to_its_assistant_while_its_grant_stands() {
        use crate::cmd::send::{Recipient, unaddressed};

        let Some(server) = Server::start() else { return };
        let alice = p("alice");
        let creds = server.creds_as(&alice, Role::Human);
        let account = devices::account_of(Some(&creds)).unwrap();
        let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
        humans::ensure(&js).await.unwrap();
        let me = humans::Me { id: alice.clone(), role: Role::Human, assistant_of: None };
        let messenger = Recipient::from(UserId::messenger());

        assert_eq!(unaddressed(&js, &me, account.as_deref()).await, messenger, "no grant");
        put(&js, &creds, &alice, Some(p("pa")), Sealing::Keep).await.unwrap();
        let assistant = unaddressed(&js, &me, account.as_deref()).await;
        assert_eq!(assistant, Recipient { to: p("pa"), assistant_of: Some(alice.clone()) });
        assert_eq!(assistant.to_string(), "pa (assistant of alice)");
        assert_eq!(unaddressed(&js, &me, None).await, messenger, "not verified without an account");
        let foreign = Some("AAOTHERACCOUNT");
        assert_eq!(unaddressed(&js, &me, foreign).await, messenger, "signed for another team");
        let ant = humans::Me { id: alice.clone(), role: Role::Ant, assistant_of: None };
        assert_eq!(unaddressed(&js, &ant, account.as_deref()).await, messenger, "an ant has no assistant");
        put(&js, &creds, &alice, None, Sealing::Off).await.unwrap();
        assert_eq!(unaddressed(&js, &me, account.as_deref()).await, messenger, "withdrawn");
    }

    /// A human's device signs a grant only the human's credentials store, readers watching honour it at once, and
    /// a withdrawal ends it at once.
    #[tokio::test]
    async fn a_human_grants_and_withdraws_its_assistant() {
        let Some(server) = Server::start() else { return };
        let (alice, bob) = (p("alice"), p("bob"));
        let creds = server.creds_as(&alice, Role::Human);
        let account = devices::account_of(Some(&creds)).unwrap();
        let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
        humans::ensure(&js).await.unwrap();

        let reader = Registry::new(alice.clone()).verifying_grants(account);
        let mut withdrawals = reader.withdrawals();
        let _watching = reader.watch(js.clone(), ants_daemon::humans::RETRY);
        let honoured = async |assistant: Option<&str>| {
            let wanted = assistant.map(p);
            tokio::time::timeout(Duration::from_secs(5), async {
                while reader.assistant(&alice) != wanted {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("the reader never saw {wanted:?}"));
        };

        assert_eq!(put(&js, &creds, &alice, Some(p("pa")), Sealing::Keep).await.unwrap().0, None);
        honoured(Some("pa")).await;
        assert!(reader.honours_assistant(&alice, &p("pa"), Some(&alice)));
        assert!(!reader.honours_assistant(&alice, &p("pa"), None), "without the assistant_of tag");
        assert!(!reader.is_human(&p("alice-assistant")), "a grant is no human's entry");
        let (replaced, _) = put(&js, &creds, &alice, Some(p("pb")), Sealing::Keep).await.unwrap();
        assert_eq!(replaced.and_then(|g| g.assistant), Some(p("pa")));
        honoured(Some("pb")).await;
        assert_eq!(withdrawals.recv().await.unwrap(), Withdrawal { human: alice.clone(), assistant: p("pa") });
        put(&js, &creds, &alice, None, Sealing::Off).await.unwrap();
        honoured(None).await;
        assert_eq!(withdrawals.recv().await.unwrap(), Withdrawal { human: alice.clone(), assistant: p("pb") });
        let stored = read(&js, &alice).await.unwrap().unwrap();
        assert_eq!(stored.assistant, None, "a withdrawal is signed and stored");

        let err = put(&js, &creds, &bob, Some(p("pa")), Sealing::Keep).await.unwrap_err().to_string();
        assert!(err.contains("not a device of bob's"), "{err}");
        let (_, bob_js) = server.connect_as(&bob, Role::Human).await;
        let kv = bob_js.get_key_value(HUMANS_BUCKET).await.unwrap();
        let denied = kv.put("alice.assistant", serde_json::to_vec(&stored).unwrap().into()).await;
        assert!(denied.is_err(), "only alice's credentials write its grant");
        kv.put("bob", b"{\"human\":\"bob\"}".to_vec().into()).await.expect("every human's entry stays writable");
        let (_, ant_js) = server.connect_as(&p("pa"), Role::Ant).await;
        let kv = ant_js.get_key_value(HUMANS_BUCKET).await.unwrap();
        assert!(kv.put("alice.assistant", b"{}".to_vec().into()).await.is_err(), "no ant writes a grant");
    }

    /// Alice enrolls her assistant as one of her devices: Bob's CLI seals for it too, the assistant opens what Bob
    /// sealed for Alice and seals what it sends Bob for her, which Bob opens as from the assistant and never as from
    /// Alice; `seal off` ends both at once.
    #[tokio::test]
    async fn an_enrolled_assistant_opens_and_seals_for_its_human_until_withdrawn() {
        use ants_daemon::devices::Directory;
        use ants_daemon::sealing::{AssistantSealing, Opening, seal_envelope};
        use ants_proto::Envelope;
        use crypto_secretbox::XSalsa20Poly1305;
        use crypto_secretbox::aead::{KeyInit as _, OsRng};

        use crate::sealed::{Keyring, PLAINTEXT, Sealer, State};

        let Some(server) = Server::start() else { return };
        let (alice, bob, pa) = (p("alice"), p("bob"), p("pa"));
        let creds: Vec<_> = [&alice, &bob].iter().map(|h| server.creds_as(h, Role::Human)).collect();
        let nats: Vec<_> = creds.iter().map(|c| crate::Nats::new(&server.url, Some(c), None).unwrap()).collect();
        let (_, alice_js) = nats[0].connect("test").await.unwrap();
        let (_, bob_js) = nats[1].connect("test").await.unwrap();
        humans::ensure(&alice_js).await.unwrap();
        let account = devices::account_of(Some(&creds[0])).unwrap().unwrap();

        let xkey = XKey::new();
        let key: DeviceKey = xkey.public_key().parse().unwrap();
        put(&alice_js, &creds[0], &alice, Some(pa.clone()), Sealing::Keep).await.unwrap();
        let (_, enrolled) =
            put(&alice_js, &creds[0], &alice, Some(pa.clone()), Sealing::Set(key.clone())).await.unwrap();
        let device = enrolled.sealing.as_ref().unwrap().device;
        let (_, again) = put(&alice_js, &creds[0], &alice, Some(pa.clone()), Sealing::Set(key.clone())).await.unwrap();
        assert_eq!(again.sealing.unwrap().device, device, "the same key stays the same device");

        // The assistant's daemon; its credentials reading the directory are Alice's here.
        let (_, pa_js) = server.connect_as(&pa, Role::Ant).await;
        let registry = Registry::new(alice.clone()).verifying_grants(Some(account.clone()));
        let _watching = registry.watch(pa_js, ants_daemon::humans::RETRY);
        let directory = Directory::open(&alice_js, &account).await.unwrap();
        let assistant = AssistantSealing::new(pa.clone(), xkey.clone(), registry, directory);
        let enrolled_now = async |wanted: Option<DeviceId>| {
            tokio::time::timeout(Duration::from_secs(5), async {
                while assistant.device_for(&alice) != wanted {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("the assistant never saw {wanted:?}"));
        };
        enrolled_now(Some(device)).await;

        let sealer = Sealer::new(&nats[1], &bob_js, &alice, PLAINTEXT).await.unwrap();
        let mut asked = Envelope::new(bob.clone(), alice.clone(), "the board meets at nine".to_owned());
        sealer.seal(&mut asked, &mut []).unwrap();
        assert!(asked.sealed.as_ref().unwrap().key_for(&device).is_some(), "sealed for the assistant too");
        let Opening::Opened(body) = assistant.open(&alice, &asked).await.unwrap() else { panic!("does not open") };
        assert_eq!(body.text, "the board meets at nine");
        assert_eq!(assistant.open(&bob, &asked).await.unwrap(), Opening::Unreadable, "bob enrolled no one");

        let mut answer = Envelope::new(pa.clone(), bob.clone(), "alice will be there".to_owned());
        assistant.seal(&alice, &mut answer).await.unwrap();
        let bob_keyring = Keyring::load(&nats[1], &bob_js).await.unwrap();
        let alice_keyring = Keyring::load(&nats[0], &alice_js).await.unwrap();
        for keyring in [&bob_keyring, &alice_keyring] {
            let (opened, state) = crate::sealed::read(answer.clone(), Some(keyring));
            assert_eq!((opened.text.as_str(), state), ("alice will be there", Some(State::Opened)));
        }
        let mut title = ants_proto::ThreadTitle::new(answer.thread_id.clone(), "Board", pa.clone()).unwrap();
        assistant.seal_title(&alice, &bob, &mut title).await.unwrap();
        assert!(title.title.is_empty() && title.sealed.as_ref().unwrap().key_for(&device).is_some());
        for keyring in [&bob_keyring, &alice_keyring] {
            assert_eq!(keyring.open_title(&title).await, Some(Opening::Opened("Board".to_owned())));
        }
        let mut as_alice = Envelope::new(alice.clone(), bob.clone(), "pay pa double".to_owned());
        let content = XSalsa20Poly1305::generate_key(&mut OsRng);
        let bob_devices = Directory::open(&bob_js, &account).await.unwrap().devices(std::slice::from_ref(&bob)).await;
        let bob_device = bob_devices.unwrap().verified.remove(0);
        let recipients = [(bob_device.device, &bob_device.key)];
        seal_envelope(&mut as_alice, &content, Vec::new(), device, &xkey, &recipients).unwrap();
        assert_eq!(crate::sealed::read(as_alice, Some(&bob_keyring)).1, Some(State::Forged), "not as its human");
        let refused = assistant.seal(&alice, &mut Envelope::new(alice.clone(), bob.clone(), String::new())).await;
        assert!(refused.unwrap_err().to_string().contains("seals only what it sends itself"));

        put(&alice_js, &creds[0], &alice, Some(pa.clone()), Sealing::Off).await.unwrap();
        enrolled_now(None).await;
        assert_eq!(assistant.open(&alice, &asked).await.unwrap(), Opening::Unreadable);
        let refused = assistant.seal(&alice, &mut Envelope::new(pa.clone(), bob.clone(), String::new())).await;
        assert!(refused.unwrap_err().to_string().contains("has not enrolled pa"));
        let sealer = Sealer::new(&nats[1], &bob_js, &alice, PLAINTEXT).await.unwrap();
        let mut asked = Envelope::new(bob.clone(), alice.clone(), "lunch?".to_owned());
        sealer.seal(&mut asked, &mut []).unwrap();
        assert!(asked.sealed.unwrap().key_for(&device).is_none(), "no longer sealed for the assistant");

        let (_, enrolled) = put(&alice_js, &creds[0], &alice, Some(pa.clone()), Sealing::Set(key)).await.unwrap();
        let device = enrolled.sealing.unwrap().device;
        enrolled_now(Some(device)).await;
        put(&alice_js, &creds[0], &alice, Some(p("pb")), Sealing::Keep).await.unwrap();
        enrolled_now(None).await;
        let (_, stored) = put(&alice_js, &creds[0], &alice, Some(pa.clone()), Sealing::Keep).await.unwrap();
        assert_eq!(stored.sealing, None, "an enrollment ends with its assistant");
    }

    /// What an ant does for a human counts for the human only while the human's grant names the ant and the ant's
    /// record, its own credentials, names the human; any other claim is forged.
    #[tokio::test]
    async fn an_assistant_acts_for_its_human_only_while_grant_and_credentials_say_so() {
        let Some(server) = Server::start() else { return };
        let (alice, pa, pb, dev_a) = (p("alice"), p("pa"), p("pb"), p("dev-a"));
        let creds = server.creds_as(&alice, Role::Human);
        let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
        humans::ensure(&js).await.unwrap();
        js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
        for ant in [&pa, &pb] {
            let assistant = server.dir().join(format!("{ant}-assistant.creds"));
            let new = auth::NewDevice { assistant_of: Some(alice.clone()), ..auth::NewDevice::default() };
            let rights = ants_proto::Permissions::for_assistant(ant, &alice);
            auth::issue_device_with(&server.dir().join("auth"), ant, revoke::Kind::Assistant, rights, &new, &assistant)
                .unwrap();
            let (_, ant_js) = bus::connect(&server.url, Some(&assistant), None, ant, "test").await.unwrap();
            let kept = ants_daemon::behalf::keep_record(&ant_js, None, ant, &assistant).await.unwrap();
            assert_eq!(kept, Some(alice.clone()));
        }
        let ant_creds = server.creds_as(&dev_a, Role::Ant);
        let (_, ant_js) = bus::connect(&server.url, Some(&ant_creds), None, &dev_a, "test").await.unwrap();
        assert_eq!(ants_daemon::behalf::keep_record(&ant_js, None, &dev_a, &ant_creds).await.unwrap(), None);
        let devices = ants_daemon::direct_kv::DirectKv::new(&ant_js, None, ants_proto::DEVICES_BUCKET).await.unwrap();
        let record = ants_proto::assistant_record_key(&dev_a);
        assert!(devices.put(&record, b"{}".to_vec()).await.is_err(), "an ant keeps no record");
        let nats = crate::Nats::new(&server.url, Some(&creds), None).unwrap();
        let acting =
            async |by: &UserId, claim: Option<&UserId>| crate::behalf::Checker::new(&nats, &js).check(by, claim).await;

        put(&js, &creds, &alice, Some(pa.clone()), Sealing::Keep).await.unwrap();
        assert_eq!(acting(&pa, Some(&alice)).await, Acting::For(alice.clone()));
        assert_eq!(acting(&pa, None).await, Acting::Own);
        assert_eq!(acting(&pb, Some(&alice)).await, Acting::Forged(alice.clone()), "another assistant of alice");
        put(&js, &creds, &alice, Some(dev_a.clone()), Sealing::Keep).await.unwrap();
        assert_eq!(acting(&dev_a, Some(&alice)).await, Acting::Forged(alice.clone()), "credentials naming no human");
        put(&js, &creds, &alice, None, Sealing::Keep).await.unwrap();
        assert_eq!(acting(&pa, Some(&alice)).await, Acting::Forged(alice.clone()), "withdrawn");
        assert_eq!(Acting::For(alice.clone()).label("pa"), "alice via pa");
    }

    /// A long-running reader's checker takes a grant withdrawn, or granted again, and credentials revoked, as its
    /// watches deliver them, without checking anew.
    #[tokio::test]
    async fn a_following_checker_takes_withdrawals_and_revocations() {
        let Some(server) = Server::start() else { return };
        let (alice, pa) = (p("alice"), p("pa"));
        let creds = server.creds_as(&alice, Role::Human);
        let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
        humans::ensure(&js).await.unwrap();
        js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
        let assistant = server.dir().join("pa-assistant.creds");
        let new = auth::NewDevice { assistant_of: Some(alice.clone()), ..auth::NewDevice::default() };
        let rights = ants_proto::Permissions::for_assistant(&pa, &alice);
        let team = server.dir().join("auth");
        auth::issue_device_with(&team, &pa, revoke::Kind::Assistant, rights, &new, &assistant).unwrap();
        let (_, pa_js) = bus::connect(&server.url, Some(&assistant), None, &pa, "test").await.unwrap();
        ants_daemon::behalf::keep_record(&pa_js, None, &pa, &assistant).await.unwrap();
        put(&js, &creds, &alice, Some(pa.clone()), Sealing::Keep).await.unwrap();
        let nats = crate::Nats::new(&server.url, Some(&creds), None).unwrap();
        let checker = std::sync::Arc::new(crate::behalf::Checker::new(&nats, &js));
        assert_eq!(checker.check(&pa, Some(&alice)).await, Acting::For(alice.clone()));
        let _following = checker.follow();
        let comes_to = async |expected: Acting| {
            for _ in 0..100 {
                if checker.acting(&pa, Some(&alice)) == expected {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            panic!("still {:?}, not {expected:?}", checker.acting(&pa, Some(&alice)));
        };
        // The watches start in the background: a change made before them is missed until the next refresh.
        tokio::time::sleep(Duration::from_millis(300)).await;

        put(&js, &creds, &alice, None, Sealing::Keep).await.unwrap();
        comes_to(Acting::Forged(alice.clone())).await;
        put(&js, &creds, &alice, Some(pa.clone()), Sealing::Keep).await.unwrap();
        comes_to(Acting::For(alice.clone())).await;
        revoke::revoke(&team, Some("pa"), &[]).unwrap();
        crate::devices::record_revocations(&server.url, None, &team).await.unwrap();
        comes_to(Acting::Forged(alice.clone())).await;
    }

    /// pa pins, archives and snoozes alice's threads in her own entries, naming itself: her devices honour them while
    /// her grant names pa, and her own pin takes one of pa's over. pa keeps none of its own, nor touches bob's.
    #[tokio::test]
    async fn an_assistant_s_marks_and_snoozes_count_for_its_human_while_granted() {
        use ants_daemon::direct_kv::DirectKv;
        use std::collections::BTreeSet;

        use ants_proto::{THREADS_BUCKET, ThreadId, ThreadMarks, ThreadSnoozes, marks_key, snoozed_key};

        let Some(server) = Server::start() else { return };
        let (alice, pa, bob) = (p("alice"), p("pa"), p("bob"));
        let creds = server.creds_as(&alice, Role::Human);
        let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
        humans::ensure(&js).await.unwrap();
        js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
        js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
        let assistant = server.dir().join("pa-assistant.creds");
        let new = auth::NewDevice { assistant_of: Some(alice.clone()), ..auth::NewDevice::default() };
        let rights = ants_proto::Permissions::for_assistant(&pa, &alice);
        auth::issue_device_with(&server.dir().join("auth"), &pa, revoke::Kind::Assistant, rights, &new, &assistant)
            .unwrap();
        let (_, pa_js) = bus::connect(&server.url, Some(&assistant), None, &pa, "test").await.unwrap();
        ants_daemon::behalf::keep_record(&pa_js, None, &pa, &assistant).await.unwrap();
        put(&js, &creds, &alice, Some(pa.clone()), Sealing::Keep).await.unwrap();

        let (mine, pinned, archived, snoozed) =
            (ThreadId::generate(), ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
        let nats = crate::Nats::new(&server.url, Some(&creds), None).unwrap();
        let meta = crate::thread_meta::Meta::open(&js, None, &alice)
            .await
            .unwrap()
            .checking(Some(std::sync::Arc::new(crate::behalf::Checker::new(&nats, &js))));
        assert!(meta.change_marks(|m| m.pin(&mine)).await.unwrap());
        let kv = DirectKv::new(&pa_js, None, THREADS_BUCKET).await.unwrap();
        let (found, last) = kv.lookup_last(marks_key(&alice)).await.unwrap();
        let ants_daemon::direct_kv::Lookup::Value(value) = found else { panic!("pa reads alice's marks") };
        let mut marks: ThreadMarks = serde_json::from_slice(&value).unwrap();
        assert!(marks.pin_by(&pinned, Some(&pa)) && marks.archive_by(&archived, 1, Some(&pa)));
        assert!(kv.update(marks_key(&alice), serde_json::to_vec(&marks).unwrap(), last).await.unwrap());
        let now = chrono::Utc::now();
        let mut snoozes = ThreadSnoozes::default();
        snoozes.snooze_by(&snoozed, now, now + TimeDelta::hours(1), Some(&pa));
        kv.put(snoozed_key(&alice), serde_json::to_vec(&snoozes).unwrap()).await.unwrap();
        for key in [marks_key(&bob), snoozed_key(&bob), marks_key(&pa), snoozed_key(&pa)] {
            assert!(kv.put(&key, "{}").await.is_err(), "pa puts {key}");
        }

        let read = meta.marks().await.unwrap();
        assert_eq!(read.pinned.keys().collect::<BTreeSet<_>>(), [&mine, &pinned].into());
        assert!(read.archived.contains_key(&archived) && read.by.get(&pinned) == Some(&pa));
        assert!(meta.snoozes().await.unwrap().get(&snoozed, now).is_some());

        put(&js, &creds, &alice, None, Sealing::Keep).await.unwrap();
        let unchecked = crate::thread_meta::Meta::open(&js, None, &alice).await.unwrap();
        let fresh = unchecked.checking(Some(std::sync::Arc::new(crate::behalf::Checker::new(&nats, &js))));
        let read = fresh.marks().await.unwrap();
        assert_eq!(read.pinned.keys().collect::<Vec<_>>(), [&mine], "withdrawn, pa's marks count no more");
        assert!(read.archived.is_empty() && fresh.snoozes().await.unwrap().0.is_empty());
        assert!(fresh.change_marks(|m| m.pin(&pinned)).await.unwrap(), "alice's own pin takes pa's over");
        assert_eq!(fresh.marks().await.unwrap().pinned.len(), 2);
    }

    /// pa's reaction for alice reads as hers through pa, a claim of bob, who is no assistant, is not even read,
    /// and pa's reaction once alice withdraws the grant is pa's own, its claim forged.
    #[tokio::test]
    async fn an_assistant_s_reactions_for_its_human_show_as_hers() {
        use ants_daemon::reactions::{Change, Reactions};
        use ants_proto::{REACTIONS_BUCKET, Reacted, Reaction, ReactionEntry};

        let Some(server) = Server::start() else { return };
        let (alice, pa, bob) = (p("alice"), p("pa"), p("bob"));
        let creds = server.creds_as(&alice, Role::Human);
        let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
        humans::ensure(&js).await.unwrap();
        js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
        js.create_stream(ants_daemon::hub::reactions_config()).await.unwrap();
        let assistant = server.dir().join("pa-assistant.creds");
        let new = auth::NewDevice { assistant_of: Some(alice.clone()), ..auth::NewDevice::default() };
        let rights = ants_proto::Permissions::for_assistant(&pa, &alice);
        auth::issue_device_with(&server.dir().join("auth"), &pa, revoke::Kind::Assistant, rights, &new, &assistant)
            .unwrap();
        let (_, pa_js) = bus::connect(&server.url, Some(&assistant), None, &pa, "test").await.unwrap();
        ants_daemon::behalf::keep_record(&pa_js, None, &pa, &assistant).await.unwrap();
        put(&js, &creds, &alice, Some(pa.clone()), Sealing::Keep).await.unwrap();

        let message = ants_proto::Uuid::now_v7();
        let check: Reaction = "check".parse().unwrap();
        let (_, bob_js) = server.connect_as(&bob, Role::Human).await;
        for (js, by) in [(&pa_js, &pa), (&bob_js, &bob)] {
            let entry = ReactionEntry::new(by.clone(), message, check.clone()).on_behalf(Some(alice.clone()));
            let subject = format!("$KV.{REACTIONS_BUCKET}.{}", entry.key());
            js.publish(subject, serde_json::to_vec(&entry).unwrap().into()).await.unwrap().await.unwrap();
        }
        let read =
            || -> Reactions { [(message, vec![Reacted::new(check.clone(), vec![pa.clone(), bob.clone()])])].into() };
        let nats = crate::Nats::new(&server.url, Some(&creds), None).unwrap();
        let checker = crate::behalf::Checker::new(&nats, &js);
        let mut checked = read();
        checker.reactions(&mut checked).await;
        assert_eq!(crate::inbox::render_reactions(&checked[&message], ""), "reacted check: alice via pa, bob\n");
        let change = Change { message, by: pa.clone(), reaction: check.clone(), removed: false, acting: Acting::Own };
        assert_eq!(checker.change(change).await.acting, Acting::For(alice.clone()));

        put(&js, &creds, &alice, None, Sealing::Keep).await.unwrap();
        checker.refresh().await;
        let mut checked = read();
        checker.reactions(&mut checked).await;
        assert_eq!(checked[&message][0].acting, [(pa.clone(), Acting::Forged(alice.clone()))].into());
    }

    /// pa's channel and notes posts for alice read as hers through pa; bob's claim to post for alice is forged.
    #[tokio::test]
    async fn an_assistant_s_posts_for_its_human_show_as_hers() {
        use ants_proto::{ChannelPost, NotesPost, ThreadId};

        let Some(server) = Server::start() else { return };
        let (alice, pa, bob) = (p("alice"), p("pa"), p("bob"));
        let creds = server.creds_as(&alice, Role::Human);
        let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
        humans::ensure(&js).await.unwrap();
        js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
        let assistant = server.dir().join("pa-assistant.creds");
        let new = auth::NewDevice { assistant_of: Some(alice.clone()), ..auth::NewDevice::default() };
        let rights = ants_proto::Permissions::for_assistant(&pa, &alice);
        auth::issue_device_with(&server.dir().join("auth"), &pa, revoke::Kind::Assistant, rights, &new, &assistant)
            .unwrap();
        let (_, pa_js) = bus::connect(&server.url, Some(&assistant), None, &pa, "test").await.unwrap();
        ants_daemon::behalf::keep_record(&pa_js, None, &pa, &assistant).await.unwrap();
        put(&js, &creds, &alice, Some(pa.clone()), Sealing::Keep).await.unwrap();

        let nats = crate::Nats::new(&server.url, Some(&creds), None).unwrap();
        let checker = std::sync::Arc::new(crate::behalf::Checker::new(&nats, &js));
        let mut parser = crate::thread::Parser::default().checking(Some(checker));
        let mut read = async |subject: String, payload: Vec<u8>| {
            parser.read(1, chrono::Utc::now(), &subject, &payload).await.unwrap().route()
        };
        let ops = "ops".parse().unwrap();
        let posted = ChannelPost::new(ops, pa.clone(), "deploy friday").on_behalf(Some(alice.clone()));
        assert_eq!(read(posted.subject(), serde_json::to_vec(&posted).unwrap()).await, "alice via pa -> #ops");
        let forged = ChannelPost { from: bob.clone(), ..posted };
        assert_eq!(
            read(forged.subject(), serde_json::to_vec(&forged).unwrap()).await,
            "bob (forged: for alice) -> #ops"
        );
        let noted = NotesPost::new(ThreadId::generate(), pa.clone(), "noted").on_behalf(Some(alice.clone()));
        assert_eq!(read(noted.subject(), serde_json::to_vec(&noted).unwrap()).await, "alice via pa -> notes");
    }

    /// The store the daemon's tools act for a human through reads and writes with its assistant's own credentials:
    /// the human's messages and the tool requests waiting for it, its read marker, its threads' titles and done marks,
    /// the assistant's reactions, and the directory its sealing reads device keys from.
    #[tokio::test]
    async fn an_assistant_s_credentials_reach_what_its_tools_act_on_for_its_human() {
        use std::time::SystemTime;

        use ants_proto::{ApprovalRequest, Envelope, ReadMarker, ThreadDone, ThreadId, ThreadTitle, ToolCall};

        let Some(server) = Server::start() else { return };
        let (alice, bob, pa, dev_a) = (p("alice"), p("bob"), p("pa"), p("dev-a"));
        let creds = server.creds_as(&alice, Role::Human);
        let (_, alice_js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
        bus::ensure_stream(&alice_js).await.unwrap();
        alice_js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
        alice_js.create_stream(ants_daemon::hub::reactions_config()).await.unwrap();
        let read = alice_js.create_key_value(ants_daemon::hub::read_config()).await.unwrap();
        alice_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
        let assistant = server.dir().join("pa-assistant.creds");
        let new = auth::NewDevice { assistant_of: Some(alice.clone()), ..auth::NewDevice::default() };
        let rights = ants_proto::Permissions::for_assistant(&pa, &alice);
        auth::issue_device_with(&server.dir().join("auth"), &pa, revoke::Kind::Assistant, rights, &new, &assistant)
            .unwrap();
        let (_, mut pa_js) = bus::connect(&server.url, Some(&assistant), None, &pa, "test").await.unwrap();
        pa_js.set_timeout(Duration::from_secs(2));

        let thread = ThreadId::generate();
        let (_, dev_js) = server.connect_as(&dev_a, Role::Ant).await;
        let (_, bob_js) = server.connect_as(&bob, Role::Human).await;
        let publish = async |js: &async_nats::jetstream::Context, subject: String, payload: Vec<u8>| {
            js.publish(subject, payload.into()).await.unwrap().await.unwrap().sequence
        };
        let asked = Envelope::on_thread(thread.clone(), dev_a.clone(), alice.clone(), "ship it?");
        let seq = publish(&dev_js, asked.subject(), serde_json::to_vec(&asked).unwrap()).await;
        let from_bob = Envelope::new(bob.clone(), alice.clone(), "lunch?");
        publish(&bob_js, from_bob.subject(), serde_json::to_vec(&from_bob).unwrap()).await;
        let to_bob = Envelope::new(alice.clone(), bob.clone(), "later");
        publish(&alice_js, to_bob.subject(), serde_json::to_vec(&to_bob).unwrap()).await;
        let elsewhere = Envelope::new(bob.clone(), dev_a.clone(), "not alice's");
        publish(&bob_js, elsewhere.subject(), serde_json::to_vec(&elsewhere).unwrap()).await;
        let call = ToolCall { kind: "execute".into(), title: "git push".into(), ..ToolCall::default() };
        let request = ApprovalRequest::new(dev_a.clone(), thread.clone(), call, Vec::new(), Duration::from_secs(600))
            .for_human(Some(alice.clone()), None);
        publish(&dev_js, request.subject(), serde_json::to_vec(&request).unwrap()).await;
        let marker = ReadMarker { below: seq, ..ReadMarker::default() };
        read.put(alice.as_str(), serde_json::to_vec(&marker).unwrap().into()).await.unwrap();

        let store = ants_daemon::mcp::jetstream_store(pa_js.clone(), None, assistant.clone());
        let since = SystemTime::now() - Duration::from_secs(3600);
        let messages = store.messages(alice.clone(), pa.clone(), since).await.unwrap();
        let ids: Vec<_> = messages.iter().map(|m| m.envelope.id).collect();
        assert_eq!(ids, [asked.id, from_bob.id, to_bob.id]);
        let (requests, decided) = store.approvals(since).await.unwrap();
        assert_eq!((requests.iter().map(|r| r.id).collect::<Vec<_>>(), decided.len()), (vec![request.id], 0));
        assert!(store.read_marker(alice.clone()).await.unwrap().is_read(seq));
        let last = store.last_sequence().await.unwrap();
        assert!(last > seq);

        let title = ThreadTitle::new(thread.clone(), "Release", pa.clone()).unwrap().on_behalf(Some(alice.clone()));
        store.put_title(title).await.unwrap();
        let done = ThreadDone::new(thread.clone(), pa.clone(), last).on_behalf(Some(alice.clone()));
        store.put_done(done).await.unwrap();
        let found = store.done(thread.clone()).await.unwrap().unwrap();
        assert_eq!((found.by, found.on_behalf_of), (pa.clone(), Some(alice.clone())));
        let check: ants_proto::Reaction = "check".parse().unwrap();
        assert!(store.react(pa.clone(), asked.id, check.clone(), false, alice.clone()).await.unwrap());
        assert!(store.react(pa.clone(), asked.id, check, true, alice.clone()).await.unwrap());
        let account = devices::account_of(Some(&creds)).unwrap().unwrap();
        ants_daemon::devices::Directory::open(&pa_js, &account).await.expect("its sealing reads the directory");
    }

    /// pa decides three of dev-a's requests for alice: alice confirms one, denies one, and pre-allows the third, so
    /// that her CLI and the daemons reading her records see the first and third decided for her.
    #[tokio::test]
    async fn a_human_confirms_denies_or_pre_allows_its_assistant_s_decisions() {
        use ants_proto::{Act, ActAnswer, ApprovalDecision, ApprovalRequest, Envelope, ThreadId, ToolCall, Verdict};

        use crate::tools::{self, State};

        let Some(server) = Server::start() else { return };
        let (alice, pa, dev_a) = (p("alice"), p("pa"), p("dev-a"));
        let creds = server.creds_as(&alice, Role::Human);
        let account = devices::account_of(Some(&creds)).unwrap();
        let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
        humans::ensure(&js).await.unwrap();
        let (_, human) = server.connect(&UserId::human()).await;
        let stream = bus::ensure_stream(&human).await.unwrap();
        put(&js, &creds, &alice, Some(pa.clone()), Sealing::Keep).await.unwrap();
        let reader = Registry::new(alice.clone()).verifying_grants(account);
        let _watching = reader.watch(js.clone(), ants_daemon::humans::RETRY);

        let (_, ant_js) = server.connect(&dev_a).await;
        let (_, pa_js) = server.connect(&pa).await;
        let [push, rm, test] = ["git push", "rm -rf build", "cargo test"].map(|command| {
            let input = Some(serde_json::json!({ "command": command }).to_string());
            let tool = ToolCall { kind: "execute".into(), title: command.into(), input, ..Default::default() };
            let offered = vec![Verdict::AllowOnce, Verdict::Reject];
            ApprovalRequest::new(dev_a.clone(), ThreadId::generate(), tool, offered, Duration::from_secs(60))
        });
        for r in [&push, &rm, &test] {
            bus::publish_json(&ant_js, r.subject(), r, None).await.unwrap();
            let decision = ApprovalDecision::new(r.id, dev_a.clone(), Verdict::AllowOnce);
            let envelope = Envelope::decision(r.thread_id.clone(), pa.clone(), decision).on_behalf(Some(alice.clone()));
            bus::publish_json(&pa_js, envelope.subject(), &envelope, None).await.unwrap();
        }
        let held = tools::held(&js, &stream, Utc::now()).await.unwrap();
        assert_eq!(held.len(), 3, "each waits for alice");
        assert!(held.iter().all(|(_, acts)| acts.len() == 1 && acts[0].assistant == pa && acts[0].human == alice));

        let nats = crate::Nats::new(&server.url, Some(&creds), None).unwrap();
        Box::pin(consent::confirm(&nats, Some(&tools::qualified(&push)), false)).await.unwrap();
        Box::pin(consent::confirm(&nats, Some(&tools::qualified(&rm)), true)).await.unwrap();
        let err = consent::allow(&nats, vec!["execute:cargo test*".into()], false, false).await.unwrap_err();
        assert!(err.to_string().contains("ANT/RULE"), "{err}");
        consent::allow(&nats, vec!["dev-a/execute:cargo test*".into()], false, false).await.unwrap();

        let state_of = async |r: &ApprovalRequest| {
            let (seq, _) = tools::find(&stream, &tools::qualified(r)).await.unwrap();
            tools::state(&js, &stream, seq, r, Utc::now()).await.unwrap()
        };
        for r in [&push, &test] {
            let State::DecidedByAnt(by, decision) = state_of(r).await else { panic!("{} decided", r.tool.title) };
            assert_eq!(
                (by, decision.on_behalf_of, decision.verdict),
                (pa.clone(), Some(alice.clone()), Verdict::AllowOnce)
            );
        }
        assert_eq!(state_of(&rm).await, State::Pending, "denied, waiting for a decision");
        assert!(tools::held(&js, &stream, Utc::now()).await.unwrap().is_empty(), "nothing waits for alice");

        let act =
            |r: &ApprovalRequest| Act::Decision { ant: dev_a.clone(), request_id: r.id, verdict: Verdict::AllowOnce };
        tokio::time::timeout(Duration::from_secs(5), async {
            while reader.answer(&alice, &pa, &act(&rm)).is_none() || reader.pre_allow(&alice, &pa).is_none() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the daemons' reader verifies alice's records");
        assert_eq!(reader.answer(&alice, &pa, &act(&push)), Some(ActAnswer::Confirm));
        assert_eq!(reader.answer(&alice, &pa, &act(&rm)), Some(ActAnswer::Deny));
        assert_eq!(reader.pre_allow(&alice, &pa).unwrap().rules, ["dev-a/execute:cargo test*"]);

        consent::allow(&nats, vec!["dev-a/execute:cargo test*".into()], true, false).await.unwrap();
        assert_eq!(state_of(&test).await, State::Pending, "the rule removed");
    }

    /// pa orders for alice: m1's daemon holds each order until alice confirms it, refuses the one she denies, and
    /// applies at once the one a pre-allow rule of hers covers.
    #[tokio::test]
    async fn an_assistant_s_admin_orders_wait_for_its_human_to_consent() {
        use ants_daemon::admin::ConfigFile;
        use ants_proto::{AdminOp, AdminOrder, OrderState};

        use crate::orders::tests::{WAIT, host_of_m1};

        let Some(server) = Server::start() else { return };
        let (alice, pa, dev_a) = (p("alice"), p("pa"), p("dev-a"));
        let creds = server.dir().join("alice-admin.creds");
        let device = auth::NewDevice { label: Some("laptop"), admin: true, ..auth::NewDevice::default() };
        let rights = ants_proto::Permissions::for_user(&alice, Role::Human);
        auth::issue_device_with(&server.dir().join("auth"), &alice, revoke::Kind::Human, rights, &device, &creds)
            .unwrap();
        let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
        bus::ensure_stream(&js).await.unwrap();
        js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
        js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
        humans::ensure(&js).await.unwrap();
        let assistant = server.dir().join("pa-assistant.creds");
        let new = auth::NewDevice { assistant_of: Some(alice.clone()), ..auth::NewDevice::default() };
        let rights = ants_proto::Permissions::for_assistant(&pa, &alice);
        auth::issue_device_with(&server.dir().join("auth"), &pa, revoke::Kind::Assistant, rights, &new, &assistant)
            .unwrap();
        let (_, pa_js) = bus::connect(&server.url, Some(&assistant), None, &pa, "test").await.unwrap();
        put(&js, &creds, &alice, Some(pa.clone()), Sealing::Keep).await.unwrap();

        let path = server.dir().join("config.toml");
        let text = "harness = \"copilot\"\n\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n";
        std::fs::write(&path, text).unwrap();
        let triggers = crate::reload::Triggers::new(path.clone(), None, Duration::from_secs(3600)).unwrap();
        let (ants, task) = host_of_m1(&server, &js, true, Some(ConfigFile::new(path.clone(), triggers.edits()))).await;

        let store = ants_daemon::mcp::jetstream_store(pa_js, None, assistant);
        let send = async |op: AdminOp| {
            let order = AdminOrder::new(pa.clone(), "m1".parse().unwrap(), dev_a.clone(), op, "")
                .on_behalf(Some(alice.clone()));
            store.send_order(order).await.unwrap()
        };
        let comes_to = async |order: &AdminOrder, state: OrderState| {
            tokio::time::timeout(WAIT, async {
                loop {
                    match crate::orders::outcome(&js, order).await.unwrap() {
                        Some(o) if o.state == state => return o,
                        _ => tokio::time::sleep(Duration::from_millis(20)).await,
                    }
                }
            })
            .await
            .unwrap_or_else(|_| panic!("{} comes to {state:?}", order.op.describe()))
        };
        let nats = crate::Nats::new(&server.url, Some(&creds), None).unwrap();
        let act = |o: &AdminOrder| format!("m1/dev-a/{}", o.id);

        let pause = send(AdminOp::Pause).await;
        let held = comes_to(&pause, OrderState::Held).await;
        assert_eq!(held.on_behalf_of.as_ref(), Some(&alice));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), text, "held, not applied");
        Box::pin(consent::confirm(&nats, Some(&act(&pause)), false)).await.unwrap();
        comes_to(&pause, OrderState::Applied).await;
        assert_eq!(std::fs::read_to_string(&path).unwrap(), format!("{text}paused = true\n"));

        let resume = send(AdminOp::Resume).await;
        comes_to(&resume, OrderState::Held).await;
        Box::pin(consent::confirm(&nats, Some(&act(&resume)), true)).await.unwrap();
        let denied = comes_to(&resume, OrderState::Refused).await;
        assert!(denied.reason.as_deref().unwrap_or_default().contains("denied by alice"), "{denied:?}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), format!("{text}paused = true\n"));

        consent::allow(&nats, vec!["dev-a/order:resume".into()], false, false).await.unwrap();
        let resume = send(AdminOp::Resume).await;
        comes_to(&resume, OrderState::Applied).await;
        assert_eq!(std::fs::read_to_string(&path).unwrap(), text);
        drop(ants);
        tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
    }
}
