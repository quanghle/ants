//! The ants a manager ant adds with its `request_ant` tool: checked against the template of the config file first,
//! keyed on this machine (the seed never leaves it), signed by `ants signer` where the account key is
//! ([`signer::ask`]), then listed in the config file, whose change the daemon picks up within seconds
//! ([`crate::reload`]) to serve the new ant. Its `retire_ant` tool retires one of them ([`crate::retire`]).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ants_daemon::config;
use ants_daemon::creds;
use ants_daemon::keystore;
use ants_daemon::mcp::{AntAsk, AntRequester, ManagerAsk, RetireAsk};
use ants_daemon::template::{AntTemplate, Request};
use ants_proto::{ENROLL_REPEAT, JsDomain, UserId};
use anyhow::{Context as _, Result, bail};
use chrono::Utc;
use nkeys::KeyPair;

use crate::{add, auth, retire, signer};

/// Where and how a manager ant's daemon adds ants.
#[derive(Debug, Clone)]
pub struct Manager {
    /// The config file the daemon serves, whose templates bound the requests and which lists the ants added.
    pub config: PathBuf,
    pub nats: String,
    /// The manager's credentials, which sign its requests.
    pub creds: Option<PathBuf>,
    pub js_domain: Option<JsDomain>,
    /// Where the added ants' credentials go, as `<name>.creds` (`ants add` puts them there too).
    pub keys: PathBuf,
    /// The configuration of the machine's leaf node, when it has one there.
    pub leaf_config: Option<PathBuf>,
}

/// How long `retire_ant` waits for the signer's answer.
const RETIRE_WAIT: std::time::Duration = std::time::Duration::from_mins(2);

/// One request at a time, so two cannot both take a template's last place in the file.
static BUSY: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

impl Manager {
    pub fn new(config: PathBuf, nats: String, creds: Option<PathBuf>, js_domain: Option<JsDomain>) -> Result<Self> {
        let keys = auth::default_dir()?;
        let leaf_config = Some(keys.with_file_name("leaf.conf"));
        Ok(Self { config, nats, creds, js_domain, keys, leaf_config })
    }

    /// The requester of `request_ant` and `retire_ant`, refusing with the reason as its text.
    pub fn requester(self) -> AntRequester {
        let manager = Arc::new(self);
        Arc::new(move |ask| {
            let manager = manager.clone();
            Box::pin(async move {
                let done = match ask {
                    ManagerAsk::Add(ask) => manager.add(*ask).await,
                    ManagerAsk::Retire(ask) => manager.retire(ask).await,
                };
                done.map_err(|e| format!("{e:#}"))
            })
        })
    }

    /// Add the ant `ask` asks for, and say what was done.
    pub async fn add(&self, ask: AntAsk) -> Result<String> {
        let _busy = BUSY.lock().await;
        let AntAsk { manager, thread, name, template, request, reason, wait } = ask;
        let loaded = config::load(&self.config)?;
        let ant = loaded
            .check_request(&manager, &name, &template, &request)
            .context("refused by the template of this machine's config file; nothing was asked")?;
        // The signer checks a workdir against its own copy of the template: send it resolved, as this machine has it.
        let request = Request { workdir: request.workdir.as_ref().map(|_| ant.workdir.clone()), ..request };
        let creds = self.creds.as_deref().context("the manager has no credentials (`creds`) to sign a request")?;
        let out = self.keys.join(format!("{name}.creds"));
        if out.exists() {
            bail!("{} exists already; ask for another name, or a human removes the file", out.display());
        }
        let user = waiting_key(&add::seed_path(&out))?;
        let (jwt, key) = creds::load(creds, keystore::os())?;
        let (client, _) =
            ants_daemon::bus::connect(&self.nats, Some(creds), self.js_domain.as_ref(), &manager, "request_ant")
                .await?;
        let leaf = match &self.leaf_config {
            Some(path) if path.exists() => add::leaf_node(Some(path))?,
            _ => None,
        };
        let settings = serde_json::to_string(&request)?;
        let until = Utc::now() + wait;
        let signed = signer::ask(
            &client,
            &jwt,
            &key,
            &template,
            &settings,
            &reason,
            &thread,
            &name,
            &user,
            leaf.as_ref().map(|(_, leaf)| leaf),
            ENROLL_REPEAT,
            until,
        )
        .await?;
        auth::write_creds(&out, &signed.jwt, &user)?;
        let seed = add::seed_path(&out);
        std::fs::remove_file(&seed).with_context(|| format!("removing {}", seed.display()))?;
        let mut said = format!("Added {name} from template {template}, with its credentials in {}.", out.display());
        if let (Some((conf, leaf)), Some(renewed)) = (leaf, signed.leaf_jwt.clone()) {
            let machine = &leaf.machine;
            // Before listing the ant, so its first connection finds the uplink carrying it.
            let renew = move || add::renew_and_reconnect(&conf, &renewed);
            let uplink = if let Some(said) = tokio::task::spawn_blocking(renew).await?? {
                format!(" The uplink credentials of the leaf node {machine} were renewed to carry {name}: {said}.")
            } else {
                format!(
                    " The uplink credentials of {machine} changed meanwhile and were left as they are: until a human \
                     runs `ants auth leaf {machine} --add-ant {name}` where the account key is, {name} reaches only \
                     the ants of this machine."
                )
            };
            said.push_str(&uplink);
        }
        let entry = config::template_entry(&name, &template, &manager, Some(&out), &request)?;
        config::append(&self.config, &entry)
            .with_context(|| format!("listing {name}, whose credentials are in {}", out.display()))?;
        Ok(said)
    }
}

impl Manager {
    /// Retire the ant `ask` names, one the manager added: unlist it, wait up to [`retire::STOP_WAIT`] for the daemon
    /// to stop it, have the signer revoke its credentials, and remove their file; say what was done. An earlier
    /// retirement cut off after the ant was unlisted carries on from its credentials file.
    pub async fn retire(&self, ask: RetireAsk) -> Result<String> {
        let _busy = BUSY.lock().await;
        let RetireAsk { manager, thread, name } = ask;
        let creds = self.creds.as_deref().context("the manager has no credentials (`creds`) to sign a request")?;
        let left = self.keys.join(format!("{name}.creds"));
        let listed = config::load(&self.config)?.ants.iter().any(|a| a.name == name);
        let (template, path, key, home) = match listed {
            true => {
                let unlisted = retire::unlist(&self.config, &name, Some(&manager))?;
                let path = unlisted.creds.unwrap_or(left);
                (Some(unlisted.template), path, unlisted.key, Some(unlisted.home))
            }
            false if left.exists() => (None, left.clone(), Some(retire::key_of(&left)?), None),
            false => bail!("the config file lists no ant {name}"),
        };
        let key = key.with_context(|| {
            format!(
                "{name} is unlisted, but its credentials {} do not read: a human retires it with `ants retire {name}`",
                path.display()
            )
        })?;
        let (jwt, signing) = creds::load(creds, keystore::os())?;
        let (client, js) =
            ants_daemon::bus::connect(&self.nats, Some(creds), self.js_domain.as_ref(), &manager, "retire_ant").await?;
        let stopped = retire::stopped(&js, &name, retire::STOP_WAIT).await;
        let until = Utc::now() + RETIRE_WAIT;
        let template_name = template.as_deref().unwrap_or_default();
        let leaf = match &self.leaf_config {
            Some(path) if path.exists() => add::leaf_node(Some(path))?,
            _ => None,
        };
        let retired = signer::retire(
            &client,
            &jwt,
            &signing,
            template_name,
            &thread,
            &name,
            &key,
            leaf.as_ref().map(|(_, leaf)| leaf),
            ENROLL_REPEAT,
            until,
        )
        .await
        .with_context(|| format!("{name} is unlisted, its credentials not revoked yet: retire_ant {name} again"))?;
        let mut said = vec![
            format!("Retired {name}: unlisted from {}.", self.config.display()),
            retire::stop_note(&name, stopped),
            match retired.revoked.len() {
                0 => "Its credentials were revoked already.".to_owned(),
                n => format!("The signer revoked {n} key(s) of it."),
            },
        ];
        if let Some(e) = &retired.push_error {
            said.push(format!(
                "Pushing the revocation to the hub failed ({e}): the hub refuses the credentials once a human runs \
                 `ants auth push` where the account key is, or it reloads."
            ));
        }
        if let Some((conf, leaf)) = leaf {
            said.extend(shrunk(conf, &leaf.machine, &name, retired).await?);
        }
        said.push(match retire::forget(&path) {
            Ok(()) => format!("{} removed.", path.display()),
            Err(e) => {
                format!("{} stays ({e:#}): request_ant of {name} needs it gone; a human removes it.", path.display())
            }
        });
        if let (Some(template), Some(home)) = (&template, &home) {
            said.push(retire::home_note(&name, template, home));
        }
        Ok(said.join(" "))
    }
}

/// Have the leaf node configured at `conf`, of `machine`, use the uplink credentials the signer renewed without `name`
/// on `retired`, and say what came of it: nothing when they did not carry it.
async fn shrunk(
    conf: PathBuf,
    machine: &ants_proto::Machine,
    name: &UserId,
    retired: signer::Retired,
) -> Result<Option<String>> {
    let remedy = format!(
        "until a human runs `ants auth leaf {machine} --remove-ant {name}` and `ants leaf outbox {machine} \
         --remove-ant {name}`, they keep {name}'s rights"
    );
    let Some(shrunk) = retired.leaf_jwt else {
        return Ok(retired.leaf_error.map(|e| {
            format!("The uplink credentials of the leaf node {machine} were left as they are ({e}): {remedy}.")
        }));
    };
    let asked = retired.leaf_asked;
    let renew = move || add::shrink_and_reconnect(&conf, asked.as_deref(), &shrunk);
    Ok(Some(match tokio::task::spawn_blocking(renew).await? {
        Ok(Some(said)) => {
            format!("The uplink credentials of the leaf node {machine} were renewed without {name}: {said}.")
        }
        Ok(None) => {
            format!("The uplink credentials of {machine} changed meanwhile and were left as they are: {remedy}.")
        }
        Err(e) => format!("Renewing the uplink credentials of {machine} without {name} failed ({e:#}): {remedy}."),
    }))
}

/// The key waiting at `seed` for its credentials, from an earlier request cut off, or a new one waiting there.
fn waiting_key(seed: &Path) -> Result<KeyPair> {
    if seed.exists() {
        return auth::read_seed(seed);
    }
    if let Some(dir) = seed.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let user = KeyPair::new_user();
    auth::write_seed(seed, &user)?;
    Ok(user)
}

/// Whether `ant`, listed in a config file with `templates`, gets `request_ant`: a manager of one, not itself added.
pub fn manages(templates: &[AntTemplate], ant: &UserId, added: bool) -> bool {
    !added && templates.iter().any(|t| t.managers.contains(ant))
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::time::Duration;

    use ants_daemon::{bus, hub};
    use ants_proto::{Role, ThreadId, Verdict};
    use tokio::time::timeout;

    use super::*;
    use crate::test_support::Server;
    use crate::{revoke, serve, tools};

    const WAIT: Duration = Duration::from_secs(30);

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    fn ask(name: &str, template: &str, request: Request) -> AntAsk {
        let (manager, thread, name) = (p("lead"), ThreadId::generate(), p(name));
        let (template, reason, wait) = (template.to_owned(), "more hands".to_owned(), Duration::from_secs(20));
        AntAsk { manager, thread, name, template, request, reason, wait }
    }

    /// A manager's daemon adds ants from its config file's templates, signed by `ants signer` within the policy of
    /// the signer's own copy of them or on a human's approval, and serves them once its file reloads.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn a_manager_adds_ants_that_its_daemon_serves_after_the_reload() {
        let Some(server) = Server::start() else { return };
        let dir = server.dir().to_owned();
        let src = dir.join("src");
        std::fs::create_dir_all(&src).unwrap();
        let (_, js) = server.connect_as(&p("alice"), Role::Human).await;
        let rights = crate::humans::Rights::from_creds(Some(&server.creds_as(&p("alice"), Role::Human))).unwrap();
        hub::init(&js, |s| rights.may_publish(s)).await.unwrap();
        let stream = bus::ensure_stream(&js).await.unwrap();
        let lead = server.creds_as(&p("lead"), Role::Ant);

        // The signer's copy lets lead add from worker only; this machine's file lets it add from helper too.
        let template = |name: &str, managers: &str| {
            format!(
                "[[ant_template]]\nname = \"{name}\"\nmanagers = [{managers}]\nmax_ants = 1\nworkdirs = [{src:?}]\n"
            )
        };
        let signer_config = dir.join("signer.toml");
        let text = template("worker", "\"lead\"") + &template("helper", "\"boss\"");
        std::fs::write(&signer_config, text).unwrap();
        let config = dir.join("config.toml");
        let text = format!(
            "nats = {:?}\nmessenger = false\nharness = \"copilot\"\n{}{}[[ant]]\nname = \"lead\"\nworkdir = {src:?}\n\
             creds = {lead:?}\n",
            server.url,
            template("worker", "\"lead\""),
            template("helper", "\"lead\"")
        );
        std::fs::write(&config, text).unwrap();
        let manager = Manager {
            config: config.clone(),
            nats: server.url.clone(),
            creds: Some(lead),
            js_domain: None,
            keys: dir.join("keys"),
            leaf_config: None,
        };
        let served = || {
            let loaded = config::load(&config)?;
            let mut configs = serve::configs(loaded.ants, &loaded.harnesses, &server.url, None, None, None)?;
            for c in &mut configs {
                c.home = dir.join("homes").join(c.ant.as_str());
            }
            Ok(serve::Served { configs, schedules: Vec::new(), metrics: None, host: None, shared: None, machine: None })
        };
        let triggers = crate::reload::Triggers::new(config.clone(), None, Duration::from_millis(200)).unwrap();
        let daemon = Box::pin(serve::run(served().unwrap(), served, triggers, pending()));
        let options = signer::Options {
            config: &signer_config,
            approval_timeout: WAIT,
            reconnect: WAIT * 4,
            tick: Duration::from_millis(200),
        };
        let team = dir.join("auth");
        let signer = Box::pin(signer::serve(&server.url, &team, None, &options, |_, _| {}, pending()));
        let lookup = ants_daemon::mcp::jetstream_lookup(js.clone());
        let listed = async |name: &str| {
            loop {
                if lookup(p(name)).await.unwrap().is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        };

        let testing = async {
            // Out of the template's bounds: refused here, nothing signed.
            let request = Request { model: Some("gpt-9".to_owned()), ..Request::default() };
            let err = format!("{:#}", manager.add(ask("lead-a", "worker", request)).await.unwrap_err());
            assert!(err.contains("refused by the template of this machine's config file"), "{err}");
            assert!(!revoke::issued(&team).unwrap().iter().any(|i| i.name == "lead-a"));

            // Within the signer's policy: signed at once, listed, and served after the reload.
            let said = manager.add(ask("lead-a", "worker", Request::default())).await.unwrap();
            assert!(said.starts_with("Added lead-a from template worker"), "{said}");
            let text = std::fs::read_to_string(&config).unwrap();
            let entry = text.split("name = \"lead-a\"").nth(1).unwrap();
            assert!(entry.contains("template = \"worker\"\nadded_by = \"lead\"\ncreds = "), "{text}");
            assert!(!dir.join("keys/lead-a.nk").exists(), "the seed is in the credentials now");
            timeout(WAIT, listed("lead-a")).await.expect("lead-a served after the reload");

            // Outside the signer's policy: signed once a human approves.
            let approving = async {
                let asked = loop {
                    let mut pending = Vec::new();
                    for (_, request) in tools::requests(&stream).await.unwrap() {
                        if tools::decision(&stream, &request).await.unwrap().is_none() {
                            pending.push(request);
                        }
                    }
                    if let Some(request) = pending.pop() {
                        break request;
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                };
                assert!(asked.tool.title.contains("lead is not among the managers of template helper"));
                tools::answer(&js, &stream, &asked.id.to_string(), Verdict::AllowOnce, chrono::Utc::now())
                    .await
                    .unwrap();
            };
            let (added, ()) = tokio::join!(manager.add(ask("lead-h", "helper", Request::default())), approving);
            assert!(added.unwrap().starts_with("Added lead-h from template helper"));
            timeout(WAIT, listed("lead-h")).await.expect("lead-h served after the reload");

            // Only the manager that added an ant retires it: refused here, and by the signer.
            let retire_ask = |manager: &str, name: &str| RetireAsk {
                manager: p(manager),
                thread: ThreadId::generate(),
                name: p(name),
            };
            let err = format!("{:#}", manager.retire(retire_ask("boss", "lead-a")).await.unwrap_err());
            assert!(err.contains("a manager retires only the ants it added"), "{err}");
            let creds_a = dir.join("keys/lead-a.creds");
            let key_a = retire::key_of(&creds_a).unwrap();
            assert!(signer::revoke_added(&team, &p("lead-a"), Some(&p("boss")), Some(&key_a)).is_err());
            assert!(revoke::revoked(&team).unwrap().is_empty());

            // Retired: unlisted, stopped, its credentials revoked and removed, its home kept.
            let home_a = dir.join("homes/lead-a");
            std::fs::create_dir_all(&home_a).unwrap();
            std::fs::write(home_a.join("kept"), "memory").unwrap();
            let old_a = dir.join("lead-a-retired.creds");
            std::fs::copy(&creds_a, &old_a).unwrap();
            let said = manager.retire(retire_ask("lead", "lead-a")).await.unwrap();
            assert!(said.starts_with("Retired lead-a"), "{said}");
            assert!(said.contains("lead-a stopped") && said.contains("revoked 1 key(s)"), "{said}");
            assert!(!said.contains("failed"), "{said}");
            assert!(config::load(&config).unwrap().ants.iter().all(|a| a.name != p("lead-a")));
            assert_eq!(lookup(p("lead-a")).await.unwrap(), None);
            assert!(revoke::revoked(&team).unwrap().iter().any(|r| r.key == key_a));
            let refused = async_nats::ConnectOptions::with_credentials_file(&old_a).await.unwrap();
            assert!(refused.connect(&server.url).await.is_err(), "the retired credentials still connect");
            assert!(!creds_a.exists());
            assert!(home_a.join("kept").exists());

            // Added again from the same template, whose max_ants it no longer counts toward, with the same home.
            let said = manager.add(ask("lead-a", "worker", Request::default())).await.unwrap();
            assert!(said.starts_with("Added lead-a from template worker"), "{said}");
            timeout(WAIT, listed("lead-a")).await.expect("lead-a served again");
            assert_ne!(retire::key_of(&creds_a).unwrap(), key_a);
            assert!(home_a.join("kept").exists());

            // A human retires any ant added from a template, but not the others.
            let creds_h = dir.join("keys/lead-h.creds");
            let key_h = retire::key_of(&creds_h).unwrap();
            let args = |name: &str| retire::RetireArgs {
                name: p(name),
                config: Some(config.clone()),
                dir: Some(team.clone()),
                no_push: false,
            };
            let err =
                format!("{:#}", retire::retire(&args("lead"), Some(&js), WAIT, &server.url, None).await.unwrap_err());
            assert!(err.contains("lead was not added from a template"), "{err}");
            retire::retire(&args("lead-h"), Some(&js), WAIT, &server.url, None).await.unwrap();
            assert!(config::load(&config).unwrap().ants.iter().all(|a| a.name != p("lead-h")));
            assert_eq!(lookup(p("lead-h")).await.unwrap(), None);
            assert!(revoke::revoked(&team).unwrap().iter().any(|r| r.key == key_h));
            assert!(!creds_h.exists());
            assert!(lookup(p("lead-a")).await.unwrap().is_some(), "lead-a is still served");
        };
        Box::pin(timeout(WAIT * 4, async {
            tokio::select! {
                r = daemon => panic!("the daemon stopped: {r:?}"),
                r = signer => panic!("the signer stopped: {r:?}"),
                () = testing => {}
            }
        }))
        .await
        .unwrap();
    }
}
