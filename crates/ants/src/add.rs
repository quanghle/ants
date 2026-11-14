//! `ants add`: list an ant in the machine's config file and give it credentials, its user NKey generated
//! here and its seed kept here. The account key signs the public key elsewhere: out of band with
//! `ants auth user NAME --public-key` and `--jwt`, or over NATS with `--enroll` and `ants enroll approve`.

use std::io::Read as _;
use std::path::{Path, PathBuf};

use ants_daemon::config::{self, Ant, Summary};
use ants_proto::{ENROLL_REPEAT, JsDomain, UserId};
use anyhow::{Context as _, Result, bail};
use nkeys::KeyPair;

use crate::auth;
use crate::enroll;
use crate::leaf;
use crate::serve::AntOptions;

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

    let user = user_key(&out)?;
    let public_key = user.public_key();
    let mut uplink = None;
    let jwt = match (&args.jwt, &args.enroll) {
        (Some(jwt), _) => {
            let jwt = read_jwt(jwt)?;
            check_jwt(&jwt, name, &public_key)?;
            Some(jwt)
        }
        (None, Some(enrollment)) => {
            let url = args.hub.as_deref().unwrap_or(nats);
            let leaf = leaf_node(args.leaf_config.as_deref())?;
            let through = leaf.as_ref().map(|l| format!(" through the leaf node of {}", l.machine)).unwrap_or_default();
            eprintln!(
                "asking for the credentials of {name} ({public_key}){through}; approve with `ants enroll approve {name}`"
            );
            let stop = async {
                let _ = tokio::signal::ctrl_c().await;
            };
            let approval = enroll::request(url, enrollment, name, &user, leaf.as_ref(), ENROLL_REPEAT, stop).await?;
            if let (Some(leaf), Some(renewed)) = (leaf, approval.leaf_jwt) {
                uplink = Some((renew_uplink(&leaf, &renewed)?, leaf));
            }
            Some(approval.jwt)
        }
        (None, None) => None,
    };
    if let Some(jwt) = &jwt {
        auth::write_creds(&out, jwt, &user)?;
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
                Some((true, leaf)) => println!(
                    "uplink credentials of {} renewed in {} to carry {name}: restart the leaf node to use them \
                     (macOS: launchctl kickstart -k gui/$(id -u)/ants.nats, Linux: systemctl --user restart \
                     ants-nats; SIGHUP does not read them again). List {name} too when running `ants auth leaf {}` \
                     or `ants leaf outbox {}` again",
                    leaf.machine,
                    leaf.creds.display(),
                    leaf.machine,
                    leaf.machine
                ),
                Some((false, leaf)) => println!(
                    "{} changed while waiting and were left as they are: add {name} to the uplink with \
                     `ants auth leaf {} --ant ...` on the machine holding the account key",
                    leaf.creds.display(),
                    leaf.machine
                ),
                None => println!(
                    "behind a leaf node the request did not name (--leaf-config), add {name} to the uplink: \
                     `ants auth leaf MACHINE --ant ...` on the machine holding the account key, then \
                     `ants leaf outbox MACHINE --ant ...`"
                ),
            }
            println!(
                "serve it: reload `ants daemon` with SIGHUP, or its service (macOS: launchctl kill SIGHUP \
                 gui/$(id -u)/ants.daemon, Linux: systemctl --user reload ants-daemon); the ants it serves carry on"
            );
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

/// The machine's leaf node, from the configuration at `path`, or at ~/.config/ants/leaf.conf when it exists.
fn leaf_node(path: Option<&Path>) -> Result<Option<enroll::Leaf>> {
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
    Ok(Some(enroll::Leaf { machine, creds }))
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

/// The ant's user key: the one of its credentials at `out`, or the one waiting beside them, or a new one, written
/// there.
fn user_key(out: &Path) -> Result<KeyPair> {
    if let Ok(text) = std::fs::read_to_string(out) {
        let seed = auth::creds_seed(&text).with_context(|| format!("no seed in {}", out.display()))?;
        return KeyPair::from_seed(seed).map_err(|e| anyhow::anyhow!("{}: {e}", out.display()));
    }
    let seed = seed_path(out);
    if seed.exists() {
        return auth::read_seed(&seed);
    }
    if let Some(dir) = seed.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let user = KeyPair::new_user();
    auth::write_seed(&seed, &user)?;
    Ok(user)
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
        let user = user_key(&out).unwrap();
        assert_eq!(user_key(&out).unwrap().public_key(), user.public_key(), "the waiting seed is reused");
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
        assert_eq!(user_key(&out).unwrap().public_key(), user.public_key(), "the credentials keep the key");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[derive(clap::Parser)]
    struct Cmd {
        #[command(flatten)]
        args: AddArgs,
    }

    async fn run(args: &[&str]) -> Result<()> {
        let args = <Cmd as clap::Parser>::try_parse_from(std::iter::once("add").chain(args.iter().copied()))?.args;
        add(args, DEFAULT_NATS, None).await
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
        let dev_a = config::load(&config).unwrap().0.into_iter().find(|p| p.name.as_str() == "dev-a").unwrap();
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
