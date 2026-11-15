//! Retiring an ant added from a template, by the manager that added it (its `retire_ant` tool, [`crate::manager`]) or
//! by a human (`ants retire`): its `[[ant]]` entry leaves the config file, the daemon stops it as on any reload (its
//! prompts in flight end first), its credentials are revoked where the account key is and their file removed. Its
//! home stays, so a later `request_ant` of the same name from the same template brings it back with its memory,
//! task database and worktrees.

use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_daemon::config;
use ants_daemon::creds::Finding;
use ants_daemon::keystore;
use ants_proto::UserId;
use anyhow::{Context as _, Result, bail};

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

/// Wait up to `wait` for `name` to leave the roster, as the daemon stopping it removes its entry; whether it did.
pub async fn stopped(js: &async_nats::jetstream::Context, name: &UserId, wait: Duration) -> bool {
    let lookup = ants_daemon::mcp::jetstream_lookup(js.clone());
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        match lookup(name.clone()).await {
            Ok(None) => return true,
            Ok(Some(_)) => {}
            Err(e) => tracing::warn!(ant = %name, error = format!("{e:#}"), "reading the roster"),
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
            "{name} was still in the roster after {}m, so its prompts in flight, if any, are cut off by the \
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

/// Retire the ant `args` names, as a human: any ant added from a template, whichever manager added it. `roster`, the
/// connection to read the roster through, if any, waits up to `wait` for the daemon to stop the ant; `url` is where to
/// push the revocation, and `js_domain` the hub's JetStream domain, if it has one.
pub async fn retire(
    args: &RetireArgs,
    roster: Option<&async_nats::jetstream::Context>,
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
    if let Some(js) = roster {
        println!("waiting up to {}m for {name} to stop", wait.as_secs().div_ceil(60));
        println!("{}", stop_note(name, stopped(js, name, wait).await));
    }
    let dir = crate::team_dir(args.dir.clone())?;
    let revoked = match crate::auth::account_key(&dir) {
        Ok(_) => {
            let revoked = crate::signer::revoke_added(&dir, name, None, key.as_deref())?;
            for r in &revoked {
                println!("revoked {} ({name})", r.key);
            }
            if revoked.is_empty() {
                println!("no live credentials of {name} to revoke");
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
                Some(key) => format!("ants auth revoke --key {key}"),
                None => format!("ants auth revoke {name}"),
            };
            println!(
                "no account key in {}: revoke the credentials of {name} where it is, with `{revoke}`",
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
