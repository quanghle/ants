//! `ants auth`: the team's keys, and the credentials they sign.

use std::path::{Path, PathBuf};

use ants_proto::{ChannelId, Destination, JsDomain, Machine, Permissions, UserId};
use anyhow::{Context as _, Result, bail};
use clap::Subcommand;

use crate::members::EnrollApprover;
use crate::{Connection, auth, backup, binding, channel, keys, members, revoke, team_dir};

#[derive(Subcommand, Debug)]
pub(crate) enum AuthCommand {
    /// Create the team operator and account, and the server.conf that makes nats-server trust them.
    Init {
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// Issue credentials for an ant, allowing only the subjects of its role, with --human for a human, a CLI user,
    /// with --webhook for a webhook of `ants webhook serve`, or with --observer for a hook observing the team's
    /// events. `human` always gets a human's. Give the hub's --js-domain, if it has one, so they work with it.
    #[command(group = clap::ArgGroup::new("targets").args(["to", "channels"]).multiple(true))]
    User {
        /// The ant, with --human the human, with --webhook the webhook, or with --observer the observer.
        user: UserId,
        /// Issue a human's credentials, with the same rights as every other human. The human joins `ants_humans`,
        /// the team's humans, on its first CLI connection.
        #[arg(long, conflicts_with = "webhook")]
        human: bool,
        /// Issue an observer's read-only credentials, for hooks (`ants hook run`, docs/hooks.md): it reads approval
        /// requests and decisions, dead letters, every ant's status (escalations among it) and the roster, and
        /// publishes nothing.
        #[arg(long, conflicts_with_all = ["human", "webhook", "label"])]
        observer: bool,
        /// Issue a webhook's credentials: it may only message the ants of --to and post to the channels of
        /// --channel, as itself, and record the events it delivered.
        #[arg(long, requires = "targets")]
        webhook: bool,
        /// With --webhook: an ant the webhook may message (repeatable).
        #[arg(long, value_name = "ANT", requires = "webhook")]
        to: Vec<UserId>,
        /// With --webhook: a channel the webhook may post to (repeatable).
        #[arg(long = "channel", value_name = "CHANNEL", value_parser = channel::parse_channel, requires = "webhook")]
        channels: Vec<ChannelId>,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
        /// Where to write the credentials (default: <dir>/<user>.creds), or with --public-key the JWT (default:
        /// print it).
        #[arg(long)]
        out: Option<PathBuf>,
        /// Sign this public user NKey (U...), from `ants add` on the ant's machine, instead of generating a key:
        /// the JWT, given to `ants add --jwt` there, completes the credentials.
        #[arg(long, value_name = "KEY")]
        public_key: Option<String>,
        /// With --human (or for `human`): a label for the device the credentials are for, such as "Quang's laptop",
        /// shown by `ants auth devices`. Each human's credentials are for a new device with a generated id.
        #[arg(long, conflicts_with = "webhook")]
        label: Option<String>,
        /// Keep the NKey seed in the OS credential store (macOS keychain, Linux Secret Service, Windows Credential
        /// Manager), never in a file, for credentials used on this machine: the file names the stored key, written
        /// only once the seed reads back and authenticates at --nats, unless no server answers there.
        #[arg(long, conflicts_with = "public_key")]
        store_key: bool,
        /// With --human: bind the human's name to this GitHub user's numeric id, looked up through the GitHub API, so
        /// that the user may register more devices with `ants login`. Refused for a name or user bound otherwise.
        #[arg(long, value_name = "LOGIN", requires = "human")]
        github: Option<String>,
        /// With --github: the signer's config file, whose [registration] table names the GitHub API and the token to
        /// look the user up with (default: ~/.ants/config.toml; github.com, with no token, without the table).
        #[arg(long, env = "ANTS_CONFIG")]
        config: Option<PathBuf>,
    },
    /// Issue enrollment credentials for a new ant: they may only ask for the ant's credentials, with
    /// `ants add NAME --enroll FILE` on its machine, and hear the answer `ants enroll approve` or `reject`
    /// gives here. They expire after --ttl, or never with --ttl 0.
    Enroll {
        ant: UserId,
        /// How long the enrollment credentials stay valid (a duration such as 90s, 10m or 1h; 0 for no expiry).
        #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse, default_value = "1h")]
        ttl: std::time::Duration,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
        /// Where to write the enrollment credentials (default: <dir>/enroll-<ant>.creds).
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Issue the team's public bootstrap credentials, which may only ask `ants signer` to register a human's new
    /// device (`ants login`) and hear its answers: no JetStream, messages of at most 4 KiB. The file is meant to be
    /// shared, and readable by all; revoke them with `ants auth revoke bootstrap` and issue new ones here.
    Bootstrap {
        /// How long the bootstrap credentials stay valid (a duration such as 30d or 1h; 0 for no expiry).
        #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse, default_value = "0")]
        ttl: std::time::Duration,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
        /// Where to write the bootstrap credentials (default: <dir>/bootstrap.creds).
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Issue the credentials of the channel manager, `ants channel serve`: it hears channel management requests and
    /// answers each once, reads the roster and the humans, writes the channels and posts in them.
    ChannelManager {
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
        /// Where to write the credentials (default: <dir>/channel-manager.creds).
        #[arg(long)]
        out: Option<PathBuf>,
        /// Keep the NKey seed in the OS credential store, never in a file, as `ants auth user --store-key` does.
        #[arg(long)]
        store_key: bool,
    },
    /// Issue the uplink credentials of a machine's leaf node (leafnodes.remotes[].credentials), allowing what the
    /// ants and humans on the machine may do and usable only by a leaf node connection, and the system account
    /// credentials of its second uplink, which only hears the account updates `ants auth push` sends the hub.
    /// Without --ant, --human or --webhook, it reissues them for the members of the last run, changed by --add-* and
    /// --remove-*.
    Leaf {
        /// Machine name, for the credentials' name and file name, and the leaf's JetStream domain: letters, digits,
        /// '-' and '_'.
        machine: Machine,
        #[command(flatten)]
        members: members::UplinkArgs,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
        /// Where to write the credentials (default: <dir>/leaf-<machine>.creds); the system account credentials of
        /// the leaf's second uplink go beside them, as <stem>.system.creds.
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Revoke credentials before they expire: every key issued to NAME (an ant, human, webhook or observer,
    /// `leaf-<machine>`, `channel-manager` or `enroll-<ant>`) and each --key. The team's account in server.conf
    /// refuses them from then on, and the account is pushed to the server of --nats, which closes their connections,
    /// as do the leaf nodes connected to it through a system account uplink.
    Revoke {
        /// Name the credentials carry; every key issued to it is revoked.
        #[arg(required_unless_present = "keys")]
        name: Option<String>,
        /// Public user NKey to revoke (repeatable), as `ants auth revoked` or the credentials' JWT shows it.
        #[arg(long = "key", value_name = "KEY")]
        keys: Vec<String>,
        /// Revoke only this device of the human NAME (repeatable): its id, its label when no other device of NAME
        /// has it, or its key, as `ants auth devices NAME` lists them.
        #[arg(long = "device", value_name = "DEVICE", requires = "name")]
        devices: Vec<String>,
        /// Only record the revocation and write server.conf; push later with `ants auth push`.
        #[arg(long)]
        no_push: bool,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// List the devices of a human, one per credentials issued to it with `ants auth user NAME --human`: id, label,
    /// key, when issued, and whether revoked.
    Devices {
        human: UserId,
        /// One JSON object per line.
        #[arg(long)]
        json: bool,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// Label a device of a human anew. Labels need not be unique; an id tells devices with the same label apart.
    Rename {
        human: UserId,
        /// The device: its id, its label when no other device of the human has it, or its key.
        device: String,
        /// The new label.
        label: String,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// List the revoked keys: key, name, when revoked and when their credentials expire. Revocations of expired
    /// credentials are dropped on the next revoke or push.
    Revoked {
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// List the keys issued here: key, name, kind, when issued, expiry, permissions version, and whether revoked or
    /// reissued since. Credentials in use that are outdated or expire within 7 days get a line naming the command that
    /// renews them.
    Issued {
        /// One JSON object per line.
        #[arg(long)]
        json: bool,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// Push the team's account, with its revocations, to the server of --nats (the hub) and through it to the leaf
    /// nodes connected with a system account uplink, as `ants auth revoke` does.
    Push {
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// Back up the team keys (operator, account and system account seeds), server.conf, issued.jsonl and
    /// revoked.json into FILE, encrypted with a passphrase in the age format (`age -d` decrypts it too) and readable
    /// by its owner only. Without them no credentials can be issued, renewed or revoked again. The passphrase comes
    /// from --passphrase-file, else ANTS_BACKUP_PASSPHRASE, else the terminal; `ants doctor` warns until a backup
    /// holds every key issued.
    Backup {
        /// The backup file to write, replacing any there.
        file: PathBuf,
        /// Read the passphrase from the first line of this file.
        #[arg(long, value_name = "FILE")]
        passphrase_file: Option<PathBuf>,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// Restore the team keys and files of a backup of `ants auth backup` into the team directory, as files there.
    /// Keys issued and revocations made after the backup are not restored.
    Restore {
        /// The backup file.
        file: PathBuf,
        /// Read the passphrase from the first line of this file (else ANTS_BACKUP_PASSPHRASE, else the terminal).
        #[arg(long, value_name = "FILE")]
        passphrase_file: Option<PathBuf>,
        /// Replace the team files already in the directory, removing those the backup has none of.
        #[arg(long)]
        force: bool,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
}

pub(crate) async fn run(command: AuthCommand, conn: &Connection<'_>) -> Result<()> {
    let domain = conn.js_domain;
    match command {
        AuthCommand::Init { dir } => init(dir),
        AuthCommand::User {
            user,
            human,
            observer,
            webhook,
            to,
            channels,
            dir,
            out,
            public_key,
            label,
            store_key,
            github,
            config,
        } => {
            let (kind, permissions) = user_rights(&user, human, observer, webhook, to, channels, domain)?;
            let dir = team_dir(dir)?;
            let github = match github {
                Some(login) => Some(github_binding(&dir, &user, &login, config).await?),
                None => None,
            };
            let new = auth::NewDevice { label: label.as_deref(), github, ..auth::NewDevice::default() };
            if store_key {
                let out = out.unwrap_or_else(|| dir.join(format!("{user}.creds")));
                let key = nkeys::KeyPair::new_user();
                let (jwt, device) = auth::sign_device_with(&dir, &user, kind, permissions, &new, &key.public_key())?;
                keys::issue_stored(&out, &jwt, &key, conn.nats).await?;
                println!("{}", out.display());
                if let Some(device) = device {
                    eprintln!("device {device}");
                }
                return Ok(());
            }
            let Some(key) = public_key else {
                let out = out.unwrap_or_else(|| dir.join(format!("{user}.creds")));
                let device = auth::issue_device_with(&dir, &user, kind, permissions, &new, &out)?;
                println!("{}", out.display());
                if let Some(device) = device {
                    eprintln!("device {device}");
                }
                return Ok(());
            };
            let (jwt, device) = auth::sign_device_with(&dir, &user, kind, permissions, &new, &key)?;
            if let Some(device) = device {
                eprintln!("device {device}");
            }
            match out {
                Some(out) => {
                    std::fs::write(&out, format!("{jwt}\n")).with_context(|| format!("writing {}", out.display()))?;
                    println!("{}", out.display());
                }
                None => println!("{jwt}"),
            }
            Ok(())
        }
        AuthCommand::Enroll { ant, ttl, dir, out } => enrollment(&ant, ttl, dir, out),
        AuthCommand::Bootstrap { ttl, dir, out } => bootstrap(ttl, dir, out),
        AuthCommand::ChannelManager { dir, out, store_key } => {
            let dir = team_dir(dir)?;
            let out = out.unwrap_or_else(|| dir.join(format!("{}.creds", UserId::channel_manager())));
            if store_key {
                let key = nkeys::KeyPair::new_user();
                let jwt = auth::sign_channel_manager(&dir, domain, &key.public_key())?;
                keys::issue_stored(&out, &jwt, &key, conn.nats).await?;
            } else {
                auth::issue_channel_manager(&dir, domain, &out)?;
            }
            println!("{}\nrun: ants --creds {} channel serve", out.display(), out.display());
            Ok(())
        }
        AuthCommand::Leaf { machine, members, dir, out } => leaf_creds(machine, members, dir, out, domain),
        AuthCommand::Revoke { name, keys, devices, no_push, dir } => {
            revoke_credentials(name, keys, devices, no_push, dir, conn.nats).await
        }
        AuthCommand::Revoked { dir } => revoke::list(&team_dir(dir)?),
        AuthCommand::Issued { json, dir } => revoke::list_issued(&team_dir(dir)?, json),
        AuthCommand::Devices { human, json, dir } => revoke::list_devices(&team_dir(dir)?, human.as_str(), json),
        AuthCommand::Rename { human, device, label, dir } => {
            let renamed = revoke::rename(&team_dir(dir)?, human.as_str(), &device, &label)?;
            println!("{} of {human} is now labelled {label:?}", renamed.device);
            Ok(())
        }
        AuthCommand::Push { dir } => push(conn.nats, &team_dir(dir)?).await,
        AuthCommand::Backup { file, passphrase_file, dir } => {
            backup::run_backup(&team_dir(dir)?, &file, passphrase_file.as_deref())
        }
        AuthCommand::Restore { file, passphrase_file, force, dir } => {
            backup::run_restore(&team_dir(dir)?, &file, passphrase_file.as_deref(), force)
        }
    }
}

/// `ants auth init`.
fn init(dir: Option<PathBuf>) -> Result<()> {
    let dir = team_dir(dir)?;
    auth::init(&dir)?;
    println!(
        "team keys in {0}\ncopy {0}/{1} next to the hub's nats-server configuration and include \"{1}\" from it \
         (include paths are relative), followed by `resolver {{ type: full, dir: \"/ABSOLUTE/DIR\" }}` so the \
         hub keeps the accounts `ants auth push` sends it across restarts",
        dir.display(),
        auth::SERVER_CONF
    );
    Ok(())
}

/// The kind of credentials `ants auth user` issues, and their rights.
fn user_rights(
    user: &UserId,
    human: bool,
    observer: bool,
    webhook: bool,
    to: Vec<UserId>,
    channels: Vec<ChannelId>,
    domain: Option<&JsDomain>,
) -> Result<(revoke::Kind, Permissions)> {
    let kind = match (webhook, observer) {
        (true, _) => revoke::Kind::Webhook,
        (_, true) => revoke::Kind::Observer,
        _ => auth::role(user, human).into(),
    };
    let permissions = match (webhook, observer) {
        (_, true) => {
            if user.is_human() {
                bail!("{user} is the default human, not an observer");
            }
            Permissions::for_observer(user)
        }
        (true, _) => {
            if user.is_human() {
                bail!("{user} is the default human, not a webhook");
            }
            let to = to.into_iter().map(Destination::Ant);
            let destinations: Vec<Destination> = to.chain(channels.into_iter().map(Destination::Channel)).collect();
            Permissions::for_webhook(user, &destinations)
        }
        _ => Permissions::for_user(user, auth::role(user, human)),
    }
    .with_js_domain(domain);
    Ok((kind, permissions))
}

/// `ants auth enroll`.
fn enrollment(ant: &UserId, ttl: std::time::Duration, dir: Option<PathBuf>, out: Option<PathBuf>) -> Result<()> {
    if ant.is_human() {
        bail!("{ant} is the default human, not an ant");
    }
    let dir = team_dir(dir)?;
    let out = out.unwrap_or_else(|| dir.join(format!("{}.creds", ants_proto::enrollment_user(ant))));
    let expires = match auth::issue_enrollment(&dir, ant, ttl, &out)? {
        Some(exp) => {
            let exp = chrono::DateTime::from_timestamp(i64::try_from(exp)?, 0).context("expiry")?;
            format!("valid until {exp}")
        }
        None => "valid with no expiry".to_owned(),
    };
    println!(
        "{}\n{expires}; on the ant's machine: ants add {ant} --enroll FILE ...\n\
         then here: ants enroll approve {ant}",
        out.display()
    );
    Ok(())
}

/// The GitHub user `login`, looked up through the GitHub API of the `[registration]` table of `config` if any, once
/// [`binding::check`] allows binding `user` to it.
async fn github_binding(
    dir: &Path,
    user: &UserId,
    login: &str,
    config: Option<PathBuf>,
) -> Result<ants_daemon::config::registration::GithubAccount> {
    let config = match config {
        Some(config) => Some(config),
        None => Some(ants_daemon::config::default_path()?).filter(|path| path.exists()),
    };
    let registration = match config {
        Some(config) => ants_daemon::config::registration::signer_registration(&config)?,
        None => None,
    };
    let account = crate::github::Github::new(registration.as_ref())?.user_by_login(login).await?;
    if binding::check_in(dir, user, &account, binding::Binder::Operator)? {
        eprintln!("{user} is bound to GitHub user {} ({}): adding a device", account.login, account.id);
    } else {
        eprintln!("binding {user} to GitHub user {} ({})", account.login, account.id);
    }
    Ok(account)
}

/// `ants auth bootstrap`.
fn bootstrap(ttl: std::time::Duration, dir: Option<PathBuf>, out: Option<PathBuf>) -> Result<()> {
    let dir = team_dir(dir)?;
    let out = out.unwrap_or_else(|| dir.join(format!("{}.creds", ants_proto::BOOTSTRAP_USER)));
    let (key, exp) = auth::issue_bootstrap(&dir, ttl, &out)?;
    let expires = match exp {
        Some(exp) => {
            format!("valid until {}", chrono::DateTime::from_timestamp(i64::try_from(exp)?, 0).context("expiry")?)
        }
        None => "valid with no expiry".to_owned(),
    };
    println!(
        "{}\n{key}, {expires}; share the file with the humans who may register: ants login NAME --creds FILE \
         --nats HUB, served by ants signer with a [registration] table",
        out.display()
    );
    Ok(())
}

/// `ants auth leaf`.
fn leaf_creds(
    machine: Machine,
    members: members::UplinkArgs,
    dir: Option<PathBuf>,
    out: Option<PathBuf>,
    domain: Option<&JsDomain>,
) -> Result<()> {
    let dir = team_dir(dir)?;
    let out = out.unwrap_or_else(|| dir.join(format!("leaf-{machine}.creds")));
    let kept = members::issued(&dir, &machine)?;
    let set = members::Change::from(members).apply(&machine, kept, || {
        anyhow::anyhow!("{} keeps no members of {machine}: name them with --ant, --human or --webhook", dir.display())
    })?;
    let approver = EnrollApprover::from(set.enroll_approver);
    auth::issue_leaf(&dir, &machine, &set.roles(), &set.webhooks, approver, domain, &out)?;
    println!("{}\n{}\nmembers of {machine}: {set}", out.display(), auth::leaf_system_creds(&out).display());
    Ok(())
}

/// `ants auth revoke`.
async fn revoke_credentials(
    name: Option<String>,
    mut keys: Vec<String>,
    devices: Vec<String>,
    no_push: bool,
    dir: Option<PathBuf>,
    url: &str,
) -> Result<()> {
    let dir = team_dir(dir)?;
    let now = auth::now()?;
    let mut name = name;
    if let (Some(human), false) = (&name, devices.is_empty()) {
        keys.extend(revoke::device_keys(&dir, human, &devices)?);
        name = None;
    }
    for r in revoke::revoke(&dir, name.as_deref(), &keys)? {
        let expired = if r.expires_at.is_some_and(|exp| exp <= now) { " (already expired)" } else { "" };
        println!("revoked {} ({}){expired}", r.key, r.name.as_deref().unwrap_or("name unknown"));
    }
    println!("{} carries the revocations", dir.join(auth::SERVER_CONF).display());
    if no_push {
        println!(
            "push them to the hub with: ants auth push --nats URL (leaf nodes with a system account uplink get \
             them with the push and keep them)"
        );
        return Ok(());
    }
    push(url, &dir).await
}

/// Push the team's account to the server at `url` ([`revoke::push`]), saying how to apply it otherwise on failure.
pub(crate) async fn push(url: &str, dir: &Path) -> Result<()> {
    match revoke::push(url, dir).await {
        Ok(pushed) => {
            println!(
                "account pushed to server {} ({url}); connections of revoked credentials are closed",
                pushed.server
            );
            let applied: Vec<&str> =
                pushed.others.iter().filter(|(_, error)| error.is_none()).map(|(name, _)| name.as_str()).collect();
            if !applied.is_empty() {
                println!("also applied by: {}", applied.join(", "));
            }
            for (name, error) in &pushed.others {
                if let Some(error) = error {
                    println!("not applied by {name}: {error}");
                }
            }
            let conf = dir.join(auth::SERVER_CONF);
            match pushed.kept {
                Some(true) => println!(
                    "server {} keeps it across restarts (full resolver): copy {} to it only for changes besides the \
                     account's (operator, system account, max_control_line)",
                    pushed.server,
                    conf.display()
                ),
                Some(false) => println!(
                    "server {0} keeps it in memory only: a restart brings back the account of its server.conf, so \
                     copy {1} to it and reload it (SIGHUP); for it to keep pushed accounts, add `resolver {{ type: \
                     full, dir: \"/ABSOLUTE/DIR\" }}` after its `include \"{2}\"` and restart it",
                    pushed.server,
                    conf.display(),
                    auth::SERVER_CONF
                ),
                None => println!(
                    "server {} did not tell whether it keeps pushed accounts across restarts (`ants doctor` on this \
                     machine checks it); without a full resolver, copy {} to it and reload it (SIGHUP)",
                    pushed.server,
                    conf.display()
                ),
            }
            println!(
                "leaf nodes not listed (offline, or with no system account uplink) did not apply it: run `ants auth \
                 push` again once they are connected, unless the hub keeps it (they then catch up within 10 s of \
                 reconnecting)"
            );
            Ok(())
        }
        Err(e) => Err(e.context(format!(
            "the account was not pushed to {url}; the revocations stay in {}: push them with `ants auth push \
             --nats URL` once the hub is in reach, or copy server.conf to the hub and reload it (SIGHUP)",
            dir.display()
        ))),
    }
}
