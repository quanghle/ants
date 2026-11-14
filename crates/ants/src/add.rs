//! `ants add`: list an ant in the machine's config file and give it credentials, its user NKey generated
//! here and its seed kept here, in a file or, with `--store-key`, in the OS credential store only ([`keys`]). The
//! account key signs the public key elsewhere: out of band with `ants auth user NAME --public-key` and `--jwt`, or
//! over NATS with `--enroll` and `ants enroll approve`.

use std::io::Read as _;
use std::path::{Path, PathBuf};

use ants_daemon::config::{self, Ant, Summary};
use ants_daemon::creds::{self, Seed};
use ants_daemon::keystore::{self, Store};
use ants_proto::{ENROLL_REPEAT, JsDomain, UserId};
use anyhow::{Context as _, Result, bail};
use nkeys::KeyPair;

use crate::auth;
use crate::enroll;
use crate::keys;
use crate::leaf;
use crate::serve::AntOptions;
use crate::service::{Action, Outcome, Service, Services, failure};

/// The server `--nats` defaults to.
const DEFAULT_NATS: &str = "nats://127.0.0.1:4222";

#[derive(clap::Args, Debug)]
pub struct AddArgs {
    /// The ant: lowercase letters, digits and '-'.
    pub name: UserId,
    /// Config file to list the ant in (default: ~/.ants/config.toml).
    #[arg(long, env = "ANTS_CONFIG")]
    pub config: Option<PathBuf>,
    /// Where to write the ant's credentials (default: the `creds` of its entry, or
    /// ~/.config/ants/auth/<ant>.creds). The seed waits beside them, as <name>.nk, until they are complete.
    #[arg(long)]
    pub out: Option<PathBuf>,
    /// Keep the ant's NKey seed in the OS credential store (macOS keychain, Linux Secret Service), never in a file:
    /// while it waits for its JWT, <name>.nk holds only its public key, and the credentials, once complete, name the
    /// stored key. They are written only once the seed reads back and authenticates at the server, unless none
    /// answers. A seed already waiting in <name>.nk moves into the store.
    #[arg(long)]
    pub store_key: bool,
    /// Complete the credentials with the user JWT `ants auth user NAME --public-key ...` printed: the JWT, a file
    /// holding it, or `-` to read it from stdin.
    #[arg(long, value_name = "JWT", conflicts_with = "enroll")]
    pub jwt: Option<String>,
    /// Ask for the credentials over NATS with these enrollment credentials (`ants auth enroll NAME`), and wait for
    /// `ants enroll approve NAME` on the machine holding the account key.
    #[arg(long, value_name = "CREDS")]
    pub enroll: Option<PathBuf>,
    /// Server to send the enrollment request to (default: --nats, the machine's leaf node or the hub).
    #[arg(long, value_name = "URL", requires = "enroll")]
    pub hub: Option<String>,
    /// Configuration of the machine's leaf node, from `ants leaf config` (default: ~/.config/ants/leaf.conf, when it
    /// exists): the approval also renews its uplink credentials to carry the ant.
    #[arg(long, value_name = "FILE", requires = "enroll")]
    pub leaf_config: Option<PathBuf>,
    #[command(flatten)]
    pub options: AntOptions,
}

/// Run `ants add` with the global `--nats` and `--js-domain`.
pub async fn add(args: AddArgs, nats: &str, js_domain: Option<&JsDomain>) -> Result<()> {
    add_with(args, nats, js_domain, keystore::os()).await
}

/// [`add`], keeping a seed of the OS credential store in `store`.
async fn add_with(args: AddArgs, nats: &str, js_domain: Option<&JsDomain>, store: &dyn Store) -> Result<()> {
    let name = &args.name;
    if name.is_human() {
        bail!("{name} is the default human, not an ant");
    }
    crate::auth::check_not_reserved(name)?;
    let path = match &args.config {
        Some(path) => path.clone(),
        None => config::default_path()?,
    };
    let summary = Summary::load(&path)?;
    let listed = summary.creds_of(name);
    if listed.is_some() && args.options.sets_any() {
        bail!("{name} is already listed in {}; change its settings there", path.display());
    }
    let out = match (&args.out, listed) {
        (Some(out), _) => std::path::absolute(out)?,
        (None, Some(Some(creds))) => creds.to_owned(),
        (None, Some(None)) => {
            bail!("the entry of {name} in {} names no creds; give --out or add `creds` to it", path.display())
        }
        (None, None) => auth::default_dir()?.join(format!("{name}.creds")),
    };
    let entry = match listed {
        Some(_) => None,
        None => Some(entry(&args, &summary, &out, nats, js_domain)?),
    };

    let (user, stored) = user_key(&out, store, args.store_key)?;
    let public_key = user.public_key();
    let mut uplink = None;
    let mut url = nats;
    let jwt = match (&args.jwt, &args.enroll) {
        (Some(jwt), _) => {
            let jwt = read_jwt(jwt)?;
            check_jwt(&jwt, name, &public_key)?;
            Some(jwt)
        }
        (None, Some(enrollment)) => {
            url = args.hub.as_deref().unwrap_or(nats);
            let leaf = leaf_node(args.leaf_config.as_deref())?;
            let through =
                leaf.as_ref().map(|(_, l)| format!(" through the leaf node of {}", l.machine)).unwrap_or_default();
            eprintln!(
                "asking for the credentials of {name} ({public_key}){through}; approve with `ants enroll approve {name}`"
            );
            let stop = async {
                let _ = tokio::signal::ctrl_c().await;
            };
            let approval =
                enroll::request(url, enrollment, name, &user, leaf.as_ref().map(|(_, l)| l), ENROLL_REPEAT, stop)
                    .await?;
            if let (Some((conf, leaf)), Some(renewed)) = (leaf, approval.leaf_jwt) {
                uplink = Some((renew_uplink(&leaf, &renewed)?, leaf, conf));
            }
            Some(approval.jwt)
        }
        (None, None) => None,
    };
    if let Some(jwt) = &jwt {
        if stored {
            match keys::write_new_stored(&out, jwt, &user, store, url).await {
                Ok(keys::Proven::Authenticated) => {}
                Ok(keys::Proven::Unreached) => eprintln!(
                    "warning: no server answered at {url}, so the stored key of {name} has not authenticated yet"
                ),
                Err(e) => {
                    if args.enroll.is_some() {
                        eprintln!(
                            "the approved user JWT of {name}, to complete the credentials with `ants add {name} --jwt \
                             JWT` once the failure below is fixed: {jwt}"
                        );
                    }
                    return Err(e);
                }
            }
        } else {
            auth::write_creds(&out, jwt, &user)?;
        }
        let seed = seed_path(&out);
        if seed.exists() {
            std::fs::remove_file(&seed).with_context(|| format!("removing {}", seed.display()))?;
        }
    }
    if let Some(entry) = &entry {
        config::append(&path, entry)?;
        println!("{name} listed in {}", path.display());
    }
    match jwt {
        Some(_) => {
            println!("credentials of {name} in {}", out.display());
            match &uplink {
                Some((true, leaf, _)) => println!(
                    "uplink credentials of {} renewed in {} to carry {name}, which later `ants auth leaf {}` and \
                     `ants leaf outbox {}` runs keep",
                    leaf.machine,
                    leaf.creds.display(),
                    leaf.machine,
                    leaf.machine
                ),
                Some((false, leaf, _)) => println!(
                    "{} changed while waiting and were left as they are: add {name} to the uplink with \
                     `ants auth leaf {} --add-ant {name}` on the machine holding the account key",
                    leaf.creds.display(),
                    leaf.machine
                ),
                None => println!(
                    "behind a leaf node the request did not name (--leaf-config), add {name} to the uplink: \
                     `ants auth leaf MACHINE --add-ant {name}` on the machine holding the account key, then \
                     `ants leaf outbox MACHINE --add-ant {name}`"
                ),
            }
            let mut services = Services::local();
            match services.apply(Service::Daemon, Action::Reload, &path) {
                Outcome::Done(command) => println!("daemon reloaded to serve {name} ({command})"),
                Outcome::NotRunning => println!("the daemon service is not running; it serves {name} once started"),
                outcome => println!(
                    "{}serve {name}: reload `ants daemon` with SIGHUP, or its service ({}); the ants it serves carry on",
                    failure(outcome),
                    crate::service::manual(Service::Daemon, Action::Reload)
                ),
            }
            // Last, as the restart cuts the connections through the leaf node.
            if let Some((true, _, conf)) = &uplink {
                match services.apply(Service::Nats, Action::Restart, conf) {
                    Outcome::Done(command) => {
                        println!("leaf node restarted to use the renewed uplink credentials ({command})")
                    }
                    Outcome::NotRunning => {
                        println!(
                            "the leaf node service is not running; it uses the renewed uplink credentials once started"
                        )
                    }
                    outcome => println!(
                        "{}restart the leaf node to use the renewed uplink credentials ({}); SIGHUP does not read them \
                         again",
                        failure(outcome),
                        crate::service::manual(Service::Nats, Action::Restart)
                    ),
                }
            }
        }
        None => println!(
            "public key of {name}: {public_key}\n\
             complete its credentials, either:\n  \
             out of band: run `ants auth user {name} --public-key {public_key}` where the account key is, then \
             `ants add {name} --jwt <JWT>` here\n  \
             over NATS: run `ants auth enroll {name}` where the account key is, copy the file here, run \
             `ants add {name} --enroll <FILE>` and approve with `ants enroll approve {name}` there"
        ),
    }
    Ok(())
}

/// The `[[ant]]` entry of an ant new to the file: the flags' settings, its credentials at `out`, and the server
/// and JetStream domain where they differ from the file's.
fn entry(args: &AddArgs, summary: &Summary, out: &Path, nats: &str, js_domain: Option<&JsDomain>) -> Result<String> {
    let mut p = args.options.apply(Ant::new(args.name.clone(), std::env::current_dir()?));
    p.workdir = p.workdir.canonicalize().with_context(|| format!("workdir {}", p.workdir.display()))?;
    if let Some(home) = &p.home {
        p.home = Some(std::path::absolute(home)?);
    }
    p.creds = Some(out.to_owned());
    if nats != summary.nats.as_deref().unwrap_or(DEFAULT_NATS) {
        p.nats = Some(nats.to_owned());
    }
    if js_domain.is_some() && js_domain != summary.js_domain.as_ref() {
        p.js_domain = js_domain.cloned();
    }
    p.clone().into_config(nats, &summary.harnesses)?;
    config::entry(&p)
}

/// The configuration of the machine's leaf node, at `path`, or at ~/.config/ants/leaf.conf when it exists, and the
/// node.
fn leaf_node(path: Option<&Path>) -> Result<Option<(PathBuf, enroll::Leaf)>> {
    let path = match path {
        Some(path) => path.to_owned(),
        None => {
            let default = auth::default_dir()?.with_file_name("leaf.conf");
            if !default.exists() {
                return Ok(None);
            }
            default
        }
    };
    let (machine, creds) = leaf::read_uplink(&path)?;
    Ok(Some((path, enroll::Leaf { machine, creds })))
}

/// Replace the JWT of the uplink credentials of `leaf` by `renewed`, unless it would drop something they allow (they
/// changed while the request waited). Returns whether it replaced it.
fn renew_uplink(leaf: &enroll::Leaf, renewed: &str) -> Result<bool> {
    let (current, key) = auth::read_creds(&leaf.creds)?;
    if !auth::allows_all(renewed, &current)? {
        return Ok(false);
    }
    auth::write_creds(&leaf.creds, renewed, &key)?;
    Ok(true)
}

/// Where the seed waits for its JWT: beside the credentials, as `<name>.nk`.
fn seed_path(creds: &Path) -> PathBuf {
    creds.with_extension("nk")
}

/// The ant's user key: the one of its credentials at `out`, or the one waiting beside them, or a new one, waiting
/// there. With `store_key`, a new or waiting seed goes into `store`, the file beside keeping only its public key.
/// Returns whether its seed is in `store`, or is to be once the credentials are complete.
fn user_key(out: &Path, store: &dyn Store, store_key: bool) -> Result<(KeyPair, bool)> {
    if out.exists() {
        let text = std::fs::read_to_string(out).with_context(|| format!("reading {}", out.display()))?;
        let stored = matches!(creds::seed_of(&text), Some(Seed::Stored(_)));
        return creds::load(out, store).map(|(_, user)| (user, stored || store_key));
    }
    let seed = seed_path(out);
    if seed.exists() {
        let (user, stored) = read_waiting(&seed, store)?;
        if store_key && !stored {
            wait_stored(&seed, &user, store, auth::replace_secret)?;
        }
        return Ok((user, stored || store_key));
    }
    if let Some(dir) = seed.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let user = KeyPair::new_user();
    match store_key {
        true => wait_stored(&seed, &user, store, auth::write_secret)?,
        false => auth::write_seed(&seed, &user)?,
    }
    Ok((user, store_key))
}

/// Store the seed of `user` in `store`, then `write` its public key alone to `path`, where it waits for its JWT;
/// a failure to write deletes what was stored.
fn wait_stored(path: &Path, user: &KeyPair, store: &dyn Store, write: fn(&Path, &str) -> Result<()>) -> Result<()> {
    let key = user.public_key();
    let created = keys::store_new(user, store)?;
    write(path, &format!("{key}\n")).inspect_err(|_| {
        if created && let Err(e) = store.delete(&key) {
            eprintln!("warning: the seed of {key} stays in the OS credential store: {e:#}");
        }
    })
}

/// The key waiting at `path`: its seed, or, from `ants add --store-key`, the public key of a seed in `store`. Returns
/// whether its seed is in `store`.
fn read_waiting(path: &Path, store: &dyn Store) -> Result<(KeyPair, bool)> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let key = text.trim();
    if !key.starts_with('U') {
        return auth::read_seed(path).map(|user| (user, false));
    }
    let seed = store
        .get(key)
        .with_context(|| {
            format!("reading the seed of {key}, which {} names, from the OS credential store", path.display())
        })?
        .with_context(|| {
            format!(
                "the OS credential store holds no seed for {key}, which {} names: delete that file and run `ants add` \
                 again for a new key",
                path.display()
            )
        })?;
    let user = KeyPair::from_seed(seed.trim()).map_err(|e| anyhow::anyhow!("the seed stored for {key}: {e}"))?;
    if user.public_key() != key {
        bail!("the OS credential store holds the seed of another key under {key} ({})", path.display());
    }
    Ok((user, true))
}

/// The JWT given to `--jwt`: itself, `-` for stdin, or a file holding it alone or in a `.creds` file.
fn read_jwt(arg: &str) -> Result<String> {
    let text = if arg == "-" {
        let mut text = String::new();
        std::io::stdin().read_to_string(&mut text).context("reading the JWT from stdin")?;
        text
    } else if arg.starts_with("eyJ") {
        arg.to_owned()
    } else {
        std::fs::read_to_string(arg).with_context(|| format!("reading {arg}"))?
    };
    Ok(auth::creds_jwt(&text).unwrap_or(text.trim()).to_owned())
}

/// Check that `jwt` is the user JWT of `ant` for `public_key`.
fn check_jwt(jwt: &str, ant: &UserId, public_key: &str) -> Result<()> {
    let claims = auth::verified(jwt).context("--jwt")?;
    if claims.sub != public_key {
        bail!("the JWT is for the key {}, not {public_key}, the key of {ant} here", claims.sub);
    }
    if claims.name != ant.as_str() {
        bail!("the JWT names {}, not {ant}", claims.name);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_seed_stays_until_the_jwt_completes_the_credentials() {
        let dir = ants_test_support::scratch_dir("ant-key").canonicalize().unwrap();
        let out = dir.join("auth/dev-a.creds");
        let store = keystore::Memory::default();
        let (user, _) = user_key(&out, &store, false).unwrap();
        assert_eq!(
            user_key(&out, &store, false).unwrap().0.public_key(),
            user.public_key(),
            "the waiting seed is reused"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(seed_path(&out)).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }

        let team = dir.join("team");
        auth::init(&team).unwrap();
        let dev_a: UserId = "dev-a".parse().unwrap();
        let jwt = auth::sign_user(&team, &dev_a, ants_proto::Role::Ant, None, &user.public_key()).unwrap();
        check_jwt(&jwt, &dev_a, &user.public_key()).unwrap();
        let account = KeyPair::new_account().public_key();
        let err = auth::sign_user(&team, &dev_a, ants_proto::Role::Ant, None, &account).unwrap_err().to_string();
        assert!(err.contains("not the public key of a user"), "{err}");
        let other = KeyPair::new_user().public_key();
        assert!(check_jwt(&jwt, &dev_a, &other).unwrap_err().to_string().contains("not"));
        assert!(check_jwt(&jwt, &"dev-b".parse().unwrap(), &user.public_key()).is_err());
        let mut forged = jwt.clone();
        forged.insert(jwt.find('.').unwrap() + 3, 'x');
        assert!(check_jwt(&forged, &dev_a, &user.public_key()).is_err());

        auth::write_creds(&out, &jwt, &user).unwrap();
        assert_eq!(read_jwt(out.to_str().unwrap()).unwrap(), jwt, "a .creds file will do");
        assert_eq!(read_jwt(&jwt).unwrap(), jwt);
        assert_eq!(
            user_key(&out, &store, false).unwrap().0.public_key(),
            user.public_key(),
            "the credentials keep the key"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[derive(clap::Parser)]
    struct Cmd {
        #[command(flatten)]
        args: AddArgs,
    }

    async fn run(args: &[&str]) -> Result<()> {
        run_with(args, DEFAULT_NATS, keystore::os()).await
    }

    async fn run_with(args: &[&str], nats: &str, store: &dyn Store) -> Result<()> {
        let args = <Cmd as clap::Parser>::try_parse_from(std::iter::once("add").chain(args.iter().copied()))?.args;
        add_with(args, nats, None, store).await
    }

    #[tokio::test]
    async fn a_stored_key_never_touches_a_file() {
        let Some(server) = crate::auth::tests::Server::start() else { return };
        let dir = ants_test_support::scratch_dir("ant-add-stored").canonicalize().unwrap();
        let config = dir.join("config.toml");
        let (config_arg, workdir) = (config.to_str().unwrap(), dir.to_str().unwrap());
        let out = dir.join("keys/dev-a.creds");
        let out_arg = out.to_str().unwrap();
        let add = ["dev-a", "--config", config_arg, "--out", out_arg, "--workdir", workdir, "--store-key"];
        let store = keystore::Memory::default();

        store.lock(true);
        let err = format!("{:#}", run_with(&add, &server.url, &store).await.unwrap_err());
        assert!(err.contains("locked"), "{err}");
        assert!(!seed_path(&out).exists() && !config.exists(), "nothing half-done");
        store.lock(false);

        run_with(&add, &server.url, &store).await.unwrap();
        let key = std::fs::read_to_string(seed_path(&out)).unwrap().trim().to_owned();
        assert_eq!(store.keys(), std::slice::from_ref(&key), "only the public key waits in the file");
        let team = server.dir().join("auth");
        let dev_a: UserId = "dev-a".parse().unwrap();
        let jwt = auth::sign_user(&team, &dev_a, ants_proto::Role::Ant, None, &key).unwrap();
        let complete = ["dev-a", "--config", config_arg, "--jwt", &jwt];
        run_with(&complete, &server.url, &store).await.unwrap();
        let text = std::fs::read_to_string(&out).unwrap();
        assert_eq!((auth::creds_jwt(&text), creds::seed_of(&text)), (Some(jwt.as_str()), Some(Seed::Stored(&key))));
        assert!(!seed_path(&out).exists());
        ants_daemon::bus::connect_with(&server.url, Some(&out), &store, None, &dev_a, "test").await.unwrap();

        // A seed already waiting in its file moves into the store.
        let out = dir.join("keys/dev-b.creds");
        let out_arg = out.to_str().unwrap();
        run_with(&["dev-b", "--config", config_arg, "--out", out_arg, "--workdir", workdir], &server.url, &store)
            .await
            .unwrap();
        let user = auth::read_seed(&seed_path(&out)).unwrap();
        let dev_b: UserId = "dev-b".parse().unwrap();
        let jwt = auth::sign_user(&team, &dev_b, ants_proto::Role::Ant, None, &user.public_key()).unwrap();
        run_with(&["dev-b", "--config", config_arg, "--jwt", &jwt, "--store-key"], &server.url, &store).await.unwrap();
        let text = std::fs::read_to_string(&out).unwrap();
        assert_eq!(creds::seed_of(&text), Some(Seed::Stored(&user.public_key())));
        assert!(!seed_path(&out).exists());
        assert_eq!(store.keys().len(), 2);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn a_listed_ant_gets_its_credentials_later() {
        let dir = ants_test_support::scratch_dir("ant-add").canonicalize().unwrap();
        let config = dir.join("config.toml");
        std::fs::write(&config, "# mine\nharness = \"copilot\"\n").unwrap();
        let (config_arg, out) = (config.to_str().unwrap(), dir.join("keys/dev-a.creds"));
        let workdir = dir.to_str().unwrap();
        let out_arg = out.to_str().unwrap();
        run(&[
            "dev-a",
            "--config",
            config_arg,
            "--out",
            out_arg,
            "--workdir",
            workdir,
            "--model",
            "m",
            "--idle-timeout",
            "90s",
        ])
        .await
        .unwrap();
        let listed = std::fs::read_to_string(&config).unwrap();
        assert!(listed.starts_with("# mine\nharness = \"copilot\"\n\n[[ant]]\nname = \"dev-a\"\n"), "{listed}");
        assert!(listed.contains("\nidle_timeout = \"1m30s\"\n"), "{listed}");
        let dev_a = config::load(&config).unwrap().ants.into_iter().find(|p| p.name.as_str() == "dev-a").unwrap();
        assert_eq!(
            (dev_a.creds.as_deref(), dev_a.model.as_deref(), dev_a.nats, dev_a.idle_timeout),
            (Some(out.as_path()), Some("m"), None, std::time::Duration::from_secs(90))
        );
        assert!(!out.exists() && seed_path(&out).exists());

        let err = run(&["dev-a", "--config", config_arg, "--model", "n"]).await.unwrap_err().to_string();
        assert!(err.contains("already listed"), "{err}");
        let team = dir.join("team");
        auth::init(&team).unwrap();
        let user = auth::read_seed(&seed_path(&out)).unwrap();
        let dev_a: UserId = "dev-a".parse().unwrap();
        let jwt = auth::sign_user(&team, &dev_a, ants_proto::Role::Ant, None, &user.public_key()).unwrap();
        run(&["dev-a", "--config", config_arg, "--jwt", &jwt]).await.unwrap();
        assert_eq!(std::fs::read_to_string(&config).unwrap(), listed, "listed once");
        let creds = std::fs::read_to_string(&out).unwrap();
        assert_eq!(auth::creds_jwt(&creds), Some(jwt.as_str()));
        assert!(!seed_path(&out).exists(), "the seed lives in the credentials now");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
