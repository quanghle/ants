//! The ants a manager ant adds with its `request_ant` tool: checked against the template of the config file first,
//! keyed on this machine (the seed never leaves it), signed by `ants signer` where the account key is
//! ([`signer::ask`]), then listed in the config file, whose change the daemon picks up within seconds
//! ([`crate::reload`]) to serve the new ant.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ants_daemon::config;
use ants_daemon::creds;
use ants_daemon::keystore;
use ants_daemon::mcp::{AntAsk, AntRequester};
use ants_daemon::template::{AntTemplate, Request};
use ants_proto::{ENROLL_REPEAT, JsDomain, UserId};
use anyhow::{Context as _, Result, bail};
use chrono::Utc;
use nkeys::KeyPair;

use crate::service::{Action, Service};
use crate::{add, auth, signer};

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

/// One request at a time, so two cannot both take a template's last place in the file.
static BUSY: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

impl Manager {
    pub fn new(config: PathBuf, nats: String, creds: Option<PathBuf>, js_domain: Option<JsDomain>) -> Result<Self> {
        let keys = auth::default_dir()?;
        let leaf_config = Some(keys.with_file_name("leaf.conf"));
        Ok(Self { config, nats, creds, js_domain, keys, leaf_config })
    }

    /// The requester of `request_ant`, refusing with the reason as its text.
    pub fn requester(self) -> AntRequester {
        let manager = Arc::new(self);
        Arc::new(move |ask| {
            let manager = manager.clone();
            Box::pin(async move { manager.add(ask).await.map_err(|e| format!("{e:#}")) })
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
            Some(path) if path.exists() => add::leaf_node(Some(path))?.map(|(_, leaf)| leaf),
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
            leaf.as_ref(),
            ENROLL_REPEAT,
            until,
        )
        .await?;
        auth::write_creds(&out, &signed.jwt, &user)?;
        let seed = add::seed_path(&out);
        std::fs::remove_file(&seed).with_context(|| format!("removing {}", seed.display()))?;
        let mut said = format!("Added {name} from template {template}, with its credentials in {}.", out.display());
        if let (Some(leaf), Some(renewed)) = (&leaf, &signed.leaf_jwt) {
            let machine = &leaf.machine;
            let uplink = if add::renew_uplink(leaf, renewed)? {
                let restart = crate::service::manual(Service::Nats, Action::Restart);
                format!(
                    " The uplink credentials of the leaf node {machine} were renewed to carry {name}: until a human \
                     restarts the leaf node ({restart}), {name} reaches only the ants of this machine."
                )
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
    use crate::auth::tests::Server;
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
            "nats = {:?}\nmessenger = false\n{}{}[[ant]]\nname = \"lead\"\nworkdir = {src:?}\ncreds = {lead:?}\n",
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
            let mut configs = serve::configs(loaded.ants, &loaded.harnesses, &server.url, None, None)?;
            for c in &mut configs {
                c.home = dir.join("homes").join(c.ant.as_str());
            }
            Ok(serve::Served { configs, schedules: Vec::new(), metrics: None })
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
        };
        Box::pin(timeout(WAIT * 2, async {
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
