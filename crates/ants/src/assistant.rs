//! `ants assistant set|clear|show`: the human of the CLI's credentials designates its assistant ant, or none, by a
//! grant its device signs ([`AssistantGrant`]), kept in [`HUMANS_BUCKET`] under the human's own name. See
//! [`ants_proto::Grants`] for how readers honour it. `enable` and `disable` ([`dedicated`]) create and retire a
//! dedicated assistant ant around the grant.

use std::path::{Path, PathBuf};

use ants_daemon::creds::{Finding, Kind};
use ants_proto::{AssistantGrant, HUMANS_BUCKET, RosterEntry, UserId, VerifiedGrant, assistant_grant_key};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::{self, kv};
use chrono::{DateTime, TimeDelta, Utc};
use serde::Deserialize;

use crate::{Nats, auth, devices, humans, revoke, roster};

mod dedicated;

use crate::cli::AssistantCommand;
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

/// The credentials of the human the CLI acts as, which sign its grant: a human device's.
fn signing_creds<'a>(nats: &Nats<'a>) -> Result<&'a Path> {
    match nats.creds {
        Some(creds) if nats.me.role.is_human() && nats.device.is_some() => Ok(creds),
        _ => bail!("signing a grant needs a human device's credentials (`ants auth user NAME --human`)"),
    }
}

/// `ants assistant`.
pub async fn run(nats: &Nats<'_>, command: AssistantCommand) -> Result<()> {
    match command {
        AssistantCommand::Enable(args) => dedicated::enable(nats, *args).await,
        AssistantCommand::Disable(args) => dedicated::disable(nats, args).await,
        AssistantCommand::Set { ant } => set(nats, &ant).await,
        AssistantCommand::Clear => clear(nats).await,
        AssistantCommand::Show { human } => show(nats, human).await,
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
    let before = put(&js, creds, human, Some(ant.clone())).await?;
    designated(ant, human, before);
    println!(
        "It acts for {human} only while its credentials name {human} as the human it assists, as the team's account \
         signs them."
    );
    Ok(())
}

/// Say that `ant` is the assistant of `human` now, the grant replacing `before`.
fn designated(ant: &UserId, human: &UserId, before: Option<VerifiedGrant>) {
    match before.and_then(|g| g.assistant) {
        Some(before) if before == *ant => println!("{ant} stays the assistant of {human}"),
        Some(before) => println!("{ant} is now the assistant of {human}, in place of {before}"),
        None => println!("{ant} is now the assistant of {human}"),
    }
}

/// `ants assistant clear`.
pub async fn clear(nats: &Nats<'_>) -> Result<()> {
    let creds = signing_creds(nats)?;
    let human = &nats.me.id;
    let (_, js) = nats.connect("ants-assistant").await?;
    let before = put(&js, creds, human, None).await?;
    match before.and_then(|g| g.assistant) {
        Some(before) => println!("{before} is no longer the assistant of {human}"),
        None => println!("{human} has no assistant"),
    }
    Ok(())
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

/// A verified record in a line.
fn describe(grant: &VerifiedGrant) -> String {
    let when = grant.issued_at.format("%Y-%m-%d %H:%M UTC");
    match &grant.assistant {
        Some(assistant) => {
            format!("{} has assistant {assistant}, granted {when} by device {}", grant.human, grant.device)
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

/// Sign the record of `assistant` (none: a withdrawal) by `human` with the device credentials `creds`, and store it
/// in place of the one stored, issued after it. Returns the record it replaced, if one was stored and verifies.
async fn put(
    js: &jetstream::Context,
    creds: &Path,
    human: &UserId,
    assistant: Option<UserId>,
) -> Result<Option<VerifiedGrant>> {
    let (jwt, key) = auth::read_creds(creds)?;
    let finding = Finding::from_jwt(&jwt).with_context(|| format!("credentials {}", creds.display()))?;
    if finding.kind != Kind::Human || finding.name != human.as_str() {
        bail!("credentials {} are not a device of {human}'s", creds.display());
    }
    let account = devices::account_of(Some(creds))?.context("credentials without an issuer")?;
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
        let mut grant = AssistantGrant::new(human.clone(), assistant.clone(), issued_at, jwt.clone());
        grant.signature = auth::sign(&key, &grant.signed())?;
        let value = serde_json::to_vec(&grant)?.into();
        let stored = match revision {
            Some(revision) => kv.update(&grant_key, value, revision).await.map_err(|e| e.kind().to_string()),
            None => kv.create(&grant_key, value).await.map_err(|e| e.kind().to_string()),
        };
        match stored {
            Ok(_) => return Ok(before.and_then(|g| g.verify(&grant_key, &account, now).ok())),
            Err(e) if e.contains("wrong last") || e.contains("already exists") => {}
            Err(e) => bail!("storing the assistant grant in {HUMANS_BUCKET}: {e}"),
        }
    }
    bail!("storing the assistant grant: another device of {human} kept storing one")
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
    use ants_proto::Role;

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

        assert_eq!(put(&js, &creds, &alice, Some(p("pa"))).await.unwrap(), None);
        honoured(Some("pa")).await;
        assert!(reader.honours_assistant(&alice, &p("pa"), Some(&alice)));
        assert!(!reader.honours_assistant(&alice, &p("pa"), None), "without the assistant_of tag");
        assert!(!reader.is_human(&p("alice-assistant")), "a grant is no human's entry");
        let replaced = put(&js, &creds, &alice, Some(p("pb"))).await.unwrap();
        assert_eq!(replaced.and_then(|g| g.assistant), Some(p("pa")));
        honoured(Some("pb")).await;
        assert_eq!(withdrawals.recv().await.unwrap(), Withdrawal { human: alice.clone(), assistant: p("pa") });
        put(&js, &creds, &alice, None).await.unwrap();
        honoured(None).await;
        assert_eq!(withdrawals.recv().await.unwrap(), Withdrawal { human: alice.clone(), assistant: p("pb") });
        let stored = read(&js, &alice).await.unwrap().unwrap();
        assert_eq!(stored.assistant, None, "a withdrawal is signed and stored");

        let err = put(&js, &creds, &bob, Some(p("pa"))).await.unwrap_err().to_string();
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
}
