//! Retiring an ant added from a template, by the manager that added it (its `retire_ant` tool, [`crate::manager`]) or
//! by a human (`ants retire`): its `[[ant]]` entry leaves the config file, the daemon stops it as on any reload (its
//! prompts in flight end first), its credentials are revoked where the account key is and their file removed. Its
//! home stays, so a later `request_ant` of the same name from the same template brings it back with its memory,
//! task database and worktrees.

use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_daemon::config;
use ants_daemon::secrets::creds::Finding;
use ants_daemon::secrets::keystore;
use ants_proto::{LeafRole, Machine, UserId};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::Context;

use crate::revoke::{self, Kind};
use crate::service::{Action, Outcome};
use crate::{add, auth, enroll, members, natsd, outbox};

/// How long a retirement waits for the daemon to stop the ant, its prompts in flight ending, before revoking its
/// credentials, which cuts off what still runs.
pub const STOP_WAIT: Duration = Duration::from_mins(5);
const STOP_POLL: Duration = Duration::from_secs(1);

/// An ant whose entry left the config file.
#[derive(Debug)]
pub struct Unlisted {
    pub template: String,
    /// Its credentials file, as the entry named it.
    pub creds: Option<PathBuf>,
    /// The public key of those credentials, when the file reads.
    pub key: Option<String>,
    /// Where its memory, task database and worktrees stay.
    pub home: PathBuf,
}

/// Remove the `[[ant]]` entry of `name` from the config file at `path`, after checking that it was added from a
/// template, by `manager` when given. The daemon serving the file stops the ant on its next reload.
pub fn unlist(path: &Path, name: &UserId, manager: Option<&UserId>) -> Result<Unlisted> {
    let loaded = config::load(path)?;
    let ant = loaded
        .ants
        .iter()
        .find(|a| a.name == *name)
        .with_context(|| format!("config {} lists no ant {name}", path.display()))?;
    let template = match (&ant.template, &ant.added_by, manager) {
        (Some(template), Some(by), Some(manager)) if by == manager => template.clone(),
        (Some(_), _, Some(manager)) => {
            bail!("{name} was not added by {manager}; a manager retires only the ants it added")
        }
        (Some(template), _, None) => template.clone(),
        (None, _, Some(manager)) => {
            bail!("{name} was not added by {manager}; a manager retires only the ants it added")
        }
        (None, _, None) => bail!(
            "{name} was not added from a template: remove its [[ant]] entry from {} by hand and revoke its \
             credentials with `ants auth revoke {name}` where the account key is",
            path.display()
        ),
    };
    let key = ant.creds.as_deref().and_then(|creds| key_of(creds).ok());
    let home = match &ant.home {
        Some(home) => home.clone(),
        None => std::env::home_dir().context("no home directory")?.join(".ants").join(name.as_str()),
    };
    let unlisted = Unlisted { template, creds: ant.creds.clone(), key, home };
    config::write(path, &config::without_ant(path, name)?)?;
    Ok(unlisted)
}

/// The public key of the credentials at `path`.
pub fn key_of(path: &Path) -> Result<String> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))?;
    Ok(Finding::from_creds(&text).with_context(|| format!("credentials {}", path.display()))?.key)
}

/// Wait up to `wait` for `name` to leave the colony, as the daemon stopping it removes its entry; whether it did.
pub async fn stopped(js: &async_nats::jetstream::Context, name: &UserId, wait: Duration) -> bool {
    let lookup = ants_daemon::mcp::jetstream_lookup(js.clone());
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        match lookup(name.clone()).await {
            Ok(None) => return true,
            Ok(Some(_)) => {}
            Err(e) => tracing::warn!(ant = %name, error = format!("{e:#}"), "reading the colony"),
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(STOP_POLL).await;
    }
}

/// Delete the credentials file `path` of a retired ant, and the seed it keeps in the OS credential store.
pub fn forget(path: &Path) -> Result<()> {
    if path.exists() {
        crate::keys::remove(path, keystore::os())?;
    }
    Ok(())
}

/// What `ants retire` and `retire_ant` say of the ant's stop.
pub fn stop_note(name: &UserId, stopped: bool) -> String {
    if stopped {
        format!("{name} stopped, its prompts in flight ended.")
    } else {
        format!(
            "{name} was still in the colony after {}m, so its prompts in flight, if any, are cut off by the \
             revocation.",
            STOP_WAIT.as_secs() / 60
        )
    }
}

/// What `ants retire` and `retire_ant` say of the home left.
pub fn home_note(name: &UserId, template: &str, home: &Path) -> String {
    format!(
        "Its home {} stays: a request_ant of {name} from template {template} brings it back with its memory, task \
         database and worktrees.",
        home.display()
    )
}

/// Drop `ant`, whose keys `ants retire` or `ants auth revoke` revoked in the team directory `dir`, from the leaf nodes
/// whose members the registry keeps it among ([`members::behind`]), as a retire order does. For this machine's leaf
/// node (~/.config/ants/leaf.conf): renew its uplink credentials for the same key without what they allowed only for
/// the ant ([`auth::shrink_leaf`]), have the node reconnect its uplink with them ([`add::shrink_and_reconnect`]), and
/// once they are written drop the ant from the leaf's members ([`members::retired`]) and from what the hub, `hub` when
/// connected, takes from the machine's outbox ([`outbox::drop_ant`]). For a leaf node whose uplink credentials are on
/// another machine, it names what to run. What came of it, a line each; none when the ant is behind no leaf node.
pub async fn leave_leaves(dir: &Path, ant: &UserId, hub: Option<&Context>) -> Vec<String> {
    let apply = |conf: &Path, action| natsd::apply(conf, action);
    leave_leaves_with(dir, ant, hub, || add::leaf_node(None), apply).await
}

/// [`leave_leaves`], `local` reading this machine's leaf node configuration, and `apply` reloading the node.
pub(crate) async fn leave_leaves_with(
    dir: &Path,
    ant: &UserId,
    hub: Option<&Context>,
    local: impl FnOnce() -> Result<Option<(PathBuf, enroll::Leaf)>>,
    apply: impl FnOnce(&Path, Action) -> Outcome + Send + 'static,
) -> Vec<String> {
    let machines = match members::behind(dir, ant) {
        Ok(machines) => machines,
        Err(e) => {
            return vec![format!(
                "reading the leaf nodes {ant} is behind from {} failed ({e:#}): `ants auth leaf M --remove-ant {ant}` \
                 and `ants leaf outbox M --remove-ant {ant}` drop it from the leaf node of M",
                dir.display()
            )];
        }
    };
    if machines.is_empty() {
        return Vec::new();
    }
    let mut said = Vec::new();
    let local = local().unwrap_or_else(|e| {
        said.push(format!("reading this machine's leaf node configuration failed: {e:#}"));
        None
    });
    let remedy = |machine: &Machine| {
        format!(
            "until a human runs `ants auth leaf {machine} --remove-ant {ant}` where the account key is and `ants leaf \
             outbox {machine} --remove-ant {ant}`, they keep {ant}'s rights"
        )
    };
    let here = local.filter(|(_, leaf)| machines.contains(&leaf.machine));
    for machine in machines.iter().filter(|m| here.as_ref().is_none_or(|(_, leaf)| leaf.machine != **m)) {
        let remedy = remedy(machine);
        said.push(format!(
            "{ant} is behind the leaf node {machine}, whose uplink credentials are on another machine: {remedy}"
        ));
    }
    if let Some((conf, leaf)) = here {
        let remedy = remedy(&leaf.machine);
        said.push(leave_here(dir, ant, hub, &conf, &leaf, apply, &remedy).await);
    }
    said
}

/// [`leave_leaves`] for `leaf`, this machine's leaf node, configured at `conf`.
async fn leave_here(
    dir: &Path,
    ant: &UserId,
    hub: Option<&Context>,
    conf: &Path,
    leaf: &enroll::Leaf,
    apply: impl FnOnce(&Path, Action) -> Outcome + Send + 'static,
    remedy: &str,
) -> String {
    let machine = &leaf.machine;
    let left = |e: anyhow::Error| {
        format!("the uplink credentials of the leaf node {machine} were left as they are ({e:#}): {remedy}")
    };
    let jwt = match auth::read_creds(&leaf.creds) {
        Ok((jwt, _)) => jwt,
        Err(e) => return left(e),
    };
    let assistant_of = match revoke::issued(dir) {
        Ok(issued) => issued
            .into_iter()
            .rfind(|i| i.name == ant.as_str() && matches!(i.kind, Kind::Ant | Kind::Assistant))
            .and_then(|i| i.assistant_of),
        Err(e) => return left(e),
    };
    let role = assistant_of.as_ref().map_or(LeafRole::Ant, LeafRole::AssistantOf);
    let renewed = match auth::shrink_leaf(dir, &jwt, machine, ant, role) {
        Ok(None) => format!("the uplink credentials of the leaf node {machine} allowed nothing only for {ant}"),
        Ok(Some(shrunk)) => {
            let conf = conf.to_owned();
            let write = move || add::shrink_and_apply(&conf, Some(&jwt), &shrunk, |action| apply(&conf, action));
            match tokio::task::spawn_blocking(write).await.map_err(anyhow::Error::from).and_then(|r| r) {
                Ok(Some(reconnected)) => {
                    format!("the uplink credentials of the leaf node {machine} renewed without {ant}: {reconnected}")
                }
                Ok(None) => {
                    return format!(
                        "the uplink credentials of {machine} changed meanwhile and were left as they are: {remedy}"
                    );
                }
                Err(e) => {
                    return format!(
                        "renewing the uplink credentials of {machine} without {ant} failed ({e:#}): {remedy}"
                    );
                }
            }
        }
        Err(e) => return left(e),
    };
    let mut said = vec![renewed];
    if let Err(e) = members::retired(dir, machine, ant) {
        said.push(format!(
            "dropping {ant} from the members of {machine} in {} failed ({e:#}): a later `ants auth leaf {machine}` \
             without `--remove-ant {ant}` gives the uplink {ant}'s rights back",
            dir.display()
        ));
    }
    let outbox = match hub {
        Some(hub) => outbox::drop_ant(hub, machine, ant).await,
        None => Err(anyhow::anyhow!("not connected to the hub")),
    };
    match outbox {
        Ok(true) => said.push(format!("the hub no longer takes {ant}'s messages from the outbox of {machine}")),
        Ok(false) => {}
        Err(e) => said.push(format!(
            "dropping {ant} from what the hub takes from the outbox of {machine} failed ({e:#}): `ants leaf outbox \
             {machine}` on {machine} drops it, if the hub takes from that outbox"
        )),
    }
    said.join("; ")
}

/// `ants retire`.
#[derive(Debug, clap::Args)]
pub struct RetireArgs {
    /// The ant to retire.
    pub name: UserId,
    /// Config file listing the ant (default: ~/.ants/config.toml).
    #[arg(long, env = "ANTS_CONFIG")]
    pub config: Option<PathBuf>,
    /// Team directory holding the account key, where the ant's credentials are revoked (default:
    /// ~/.config/ants/auth). Without the account key there, the revocation to run where it is gets printed.
    #[arg(long, env = "ANTS_AUTH_DIR")]
    pub dir: Option<PathBuf>,
    /// Only record the revocation and write server.conf; push later with `ants auth push`.
    #[arg(long)]
    pub no_push: bool,
}

/// Retire the ant `args` names, as a human: any ant added from a template, whichever manager added it. `colony`, the
/// connection to read the colony through, if any, waits up to `wait` for the daemon to stop the ant; `url` is where to
/// push the revocation, and `js_domain` the hub's JetStream domain, if it has one.
pub async fn retire(
    args: &RetireArgs,
    colony: Option<&async_nats::jetstream::Context>,
    wait: Duration,
    url: &str,
    js_domain: Option<&ants_proto::JsDomain>,
) -> Result<()> {
    let name = &args.name;
    let path = match &args.config {
        Some(path) => path.clone(),
        None => config::default_path()?,
    };
    let left = crate::auth::default_dir()?.join(format!("{name}.creds"));
    let listed = config::load(&path)?.ants.iter().any(|a| a.name == *name);
    let (unlisted, creds) = match listed {
        true => {
            let unlisted = unlist(&path, name, None)?;
            println!(
                "unlisted {name} from {}: the daemon serving it stops it, its prompts in flight ending first",
                path.display()
            );
            let creds = unlisted.creds.clone().unwrap_or(left);
            (Some(unlisted), creds)
        }
        false if left.exists() => (None, left),
        false => bail!("config {} lists no ant {name}", path.display()),
    };
    let key = match unlisted.as_ref().and_then(|u| u.key.clone()) {
        Some(key) => Some(key),
        None => key_of(&creds).ok(),
    };
    if let Some(js) = colony {
        println!("waiting up to {}m for {name} to stop", wait.as_secs().div_ceil(60));
        println!("{}", stop_note(name, stopped(js, name, wait).await));
    }
    let dir = crate::auth::team_dir(args.dir.clone())?;
    let revoked = match crate::auth::account_key(&dir) {
        Ok(_) => {
            let revoked = crate::signer::revoke_added(&dir, name, None, key.as_deref())?;
            for r in &revoked {
                println!("revoked {} ({name})", r.key);
            }
            if revoked.is_empty() {
                println!("no live credentials of {name} to revoke");
            }
            for line in leave_leaves(&dir, name, colony).await {
                println!("{line}");
            }
            Some(revoked)
        }
        Err(_) if unlisted.is_none() => bail!(
            "config {} lists no ant {name}, and with no account key in {} its credentials {} cannot be checked as \
             those of an added ant",
            path.display(),
            dir.display(),
            creds.display()
        ),
        Err(_) => {
            let revoke = match &key {
                Some(key) => format!("ants auth revoke --key {key} --earlier"),
                None => format!("ants auth revoke {name}"),
            };
            println!(
                "no account key in {}: revoke the credentials of {name} where it is, with `{revoke}`, which also \
                 drops {name} from a leaf node's uplink once no key of {name} is live",
                dir.display()
            );
            None
        }
    };
    forget(&creds)?;
    println!("{} removed", creds.display());
    if let Some(u) = &unlisted {
        println!("{}", home_note(name, &u.template, &u.home));
    }
    match revoked {
        Some(revoked) if !revoked.is_empty() && !args.no_push => crate::cmd::auth::push(url, js_domain, &dir).await,
        Some(revoked) if !revoked.is_empty() => {
            println!("push the revocation to the hub with: ants auth push --nats URL");
            Ok(())
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::{Permissions, Role};

    use super::*;
    use crate::members::EnrollApprover;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    fn leaf_conf(path: &Path, machine: &Machine, creds: &Path) {
        std::fs::write(path, format!("server_name: \"{machine}\"\ncredentials: {creds:?}\n")).unwrap();
    }

    /// An ant retired behind this machine's leaf node leaves its uplink, renewed for the same key and reconnected, and
    /// its members; for another machine's leaf node, or when the uplink credentials do not read, what to run is said
    /// and the members stay.
    #[tokio::test]
    async fn a_retired_ant_leaves_the_uplink_of_this_machine_s_leaf_node() {
        let dir = ants_test_support::scratch_dir("leave-leaves");
        let team = dir.join("auth");
        auth::init(&team).unwrap();
        let (m1, m2): (Machine, Machine) = ("m1".parse().unwrap(), "m2".parse().unwrap());
        for ant in [p("dev-a"), p("dev-c")] {
            let permissions = Permissions::for_user(&ant, Role::Ant);
            auth::issue_device(&team, &ant, Role::Ant.into(), permissions, None, &dir.join(format!("{ant}.creds")))
                .unwrap();
        }
        let (m1_uplink, m2_uplink) = (dir.join("leaf-m1.creds"), dir.join("leaf-m2.creds"));
        let on_m1 = auth::members(&[p("dev-a"), p("dev-c")], &[]);
        auth::issue_leaf(&team, &m1, &on_m1, &[], EnrollApprover::Without, None, &m1_uplink).unwrap();
        let on_m2 = auth::members(&[p("dev-a")], &[]);
        auth::issue_leaf(&team, &m2, &on_m2, &[], EnrollApprover::Without, None, &m2_uplink).unwrap();
        let conf = dir.join("leaf.conf");
        leaf_conf(&conf, &m1, &m1_uplink);
        let local = |conf: &Path, machine: &Machine, creds: &Path| {
            let (conf, leaf) = (conf.to_owned(), enroll::Leaf { machine: machine.clone(), creds: creds.to_owned() });
            move || Ok(Some((conf, leaf)))
        };
        let reload = |_: &Path, action| {
            assert_eq!(action, Action::Reload);
            Outcome::Done("kill -HUP".to_owned())
        };
        let (old, key) = auth::read_creds(&m1_uplink).unwrap();
        revoke::revoke(&team, Some("dev-c"), &[]).unwrap();

        let said = leave_leaves_with(&team, &p("dev-c"), None, local(&conf, &m1, &m1_uplink), reload).await;
        let [said] = said.as_slice() else { panic!("{said:?}") };
        assert!(said.contains("renewed without dev-c: the leaf node reloaded"), "{said}");
        assert!(said.contains("not connected to the hub): `ants leaf outbox m1` on m1 drops it"), "{said}");
        let (shrunk, shrunk_key) = auth::read_creds(&m1_uplink).unwrap();
        assert_eq!(shrunk_key.public_key(), key.public_key());
        assert!(auth::allows_all(&old, &shrunk).unwrap() && !auth::allows_all(&shrunk, &old).unwrap());
        assert_eq!(members::uplink(&conf, &m1).unwrap().ants, [p("dev-a")]);
        assert_eq!(members::issued(&team, &m1).unwrap().unwrap().ants, [p("dev-a")]);
        let again = leave_leaves_with(&team, &p("dev-c"), None, local(&conf, &m1, &m1_uplink), reload).await;
        assert!(again.is_empty(), "behind no leaf node any more: {again:?}");

        let missing = dir.join("missing.creds");
        let said = leave_leaves_with(&team, &p("dev-a"), None, local(&conf, &m2, &missing), reload).await;
        let [elsewhere, here] = said.as_slice() else { panic!("{said:?}") };
        assert!(elsewhere.starts_with("dev-a is behind the leaf node m1, whose uplink credentials are on another"));
        assert!(elsewhere.contains("`ants auth leaf m1 --remove-ant dev-a`"), "{elsewhere}");
        assert!(here.starts_with("the uplink credentials of the leaf node m2 were left as they are"), "{here}");
        assert!(here.contains("`ants leaf outbox m2 --remove-ant dev-a`"), "{here}");
        assert_eq!(members::issued(&team, &m1).unwrap().unwrap().ants, [p("dev-a")]);
        assert_eq!(members::issued(&team, &m2).unwrap().unwrap().ants, [p("dev-a")]);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
