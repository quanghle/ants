//! `ants webhook add`: list a webhook in the config file of `ants webhook serve` in one step: its credentials, issued
//! here or asked for over NATS, a new secret (or one given, such as GitLab's signing token) in a 0600 file beside the
//! config file, and its `[[webhook]]` entry, set up for the sender a [`Preset`] names.

use std::io::Read as _;
use std::path::{Path, PathBuf};

use ants_daemon::config::toml_string;
use ants_proto::{ChannelId, Destination, ENROLL_REPEAT, JsDomain, Permissions, UserId};
use anyhow::{Context as _, Result, bail};
use data_encoding::HEXLOWER;
use nkeys::KeyPair;

use crate::auth;
use crate::credentials::{enroll, issued};
use crate::service::{Action, Outcome, Service, Services, failure};
use crate::webhook::{self, Config, Listing};

/// The senders `ants webhook add --preset` sets a webhook up for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Preset {
    /// GitHub: `X-Hub-Signature-256` (HMAC-SHA256), one event per `X-GitHub-Delivery`.
    Github,
    /// GitLab: the secret token in `X-Gitlab-Token`, one event per `Idempotency-Key`; with `--signing-token`, the
    /// signing token's Standard Webhooks signatures, one event per `webhook-id`.
    Gitlab,
    /// Grafana alerting: `X-Grafana-Alerting-Signature` (HMAC-SHA256), the alert's title and message.
    Grafana,
}

/// How a webhook takes its sender's requests: the `auth` of its entry, its header setting if any, and its `id` and
/// `text`, if not the defaults.
struct Settings {
    auth: &'static str,
    header: Option<(&'static str, &'static str)>,
    id: Option<&'static str>,
    text: Option<&'static str>,
}

impl Settings {
    fn of(preset: Option<Preset>, signing_token: bool) -> Self {
        match preset {
            Some(Preset::Gitlab) if signing_token => Settings {
                auth: "standard-webhooks",
                header: None,
                id: Some("header:webhook-id"),
                text: Some("GitLab {header:X-Gitlab-Event} on {json:/project/path_with_namespace}\n\n{body}"),
            },
            None => Settings { auth: "bearer", header: None, id: None, text: None },
            Some(Preset::Github) => Settings {
                auth: "hmac-sha256",
                header: Some(("signature_header", "X-Hub-Signature-256")),
                id: Some("header:X-GitHub-Delivery"),
                text: Some("GitHub {header:X-GitHub-Event} on {json:/repository/full_name}\n\n{body}"),
            },
            Some(Preset::Gitlab) => Settings {
                auth: "token",
                header: Some(("token_header", "X-Gitlab-Token")),
                id: Some("header:Idempotency-Key"),
                text: Some("GitLab {header:X-Gitlab-Event} on {json:/project/path_with_namespace}\n\n{body}"),
            },
            // Each notification is its own event: the group key is the same for firing and resolved ones.
            Some(Preset::Grafana) => Settings {
                auth: "hmac-sha256",
                header: Some(("signature_header", "X-Grafana-Alerting-Signature")),
                id: None,
                text: Some("Grafana: {json:/title}\n\n{json:/message}"),
            },
        }
    }
}

#[derive(clap::Args, Debug)]
#[command(group = clap::ArgGroup::new("destination").args(["to", "channel"]).required(true))]
pub struct AddArgs {
    /// The webhook: lowercase letters, digits and '-'. Requests go to `<path>/<name>`.
    pub name: UserId,
    /// The ant the webhook's events go to.
    #[arg(long, value_name = "ANT")]
    pub to: Option<UserId>,
    /// The channel the webhook's events are posted to.
    #[arg(long, value_name = "CHANNEL", value_parser = crate::channel::parse_channel)]
    pub channel: Option<ChannelId>,
    /// Set the webhook up for this sender: its signature or token header, event id and text. Without it, requests
    /// carry `Authorization: Bearer <secret>`.
    #[arg(long, value_enum)]
    pub preset: Option<Preset>,
    /// With --preset gitlab: check the Standard Webhooks signatures of GitLab's signing token (webhook-signature)
    /// rather than a secret token in X-Gitlab-Token. GitLab generates the token: give it with --secret-file or
    /// --secret-env, or paste it when asked.
    #[arg(long)]
    pub signing_token: bool,
    /// Take the webhook's secret, such as GitLab's signing token, from the first line of FILE ('-' for standard
    /// input) rather than making a new one; it is copied to <name>.secret.
    #[arg(long, value_name = "FILE", conflicts_with = "secret_env")]
    pub secret_file: Option<PathBuf>,
    /// Take the webhook's secret, such as GitLab's signing token, from the environment variable VAR rather than
    /// making a new one; it is copied to <name>.secret.
    #[arg(long, value_name = "VAR")]
    pub secret_env: Option<String>,
    /// The webhooks' config file (default: ~/.ants/webhooks.toml).
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// Auth directory holding the signing keys (default: ~/.config/ants/auth).
    #[arg(long, env = "ANTS_AUTH_DIR")]
    pub dir: Option<PathBuf>,
    /// Where to write the webhook's credentials (default: ~/.config/ants/auth/<name>.creds, or <dir>/<name>.creds).
    #[arg(long)]
    pub out: Option<PathBuf>,
    /// Ask for the credentials over NATS with these enrollment credentials (`ants auth enroll NAME`), and wait for
    /// `ants enroll approve NAME --webhook --to ANT` (or `--channel CHANNEL`) on the machine holding the account key,
    /// instead of issuing them with the account key here.
    #[arg(long, value_name = "CREDS", conflicts_with = "dir")]
    pub enroll: Option<PathBuf>,
    /// Server to send the enrollment request to (default: --nats).
    #[arg(long, value_name = "URL", requires = "enroll")]
    pub hub: Option<String>,
}

/// Run `ants webhook add` with the global `--nats` and `--js-domain`.
pub async fn add(args: AddArgs, nats: &str, js_domain: Option<&JsDomain>) -> Result<()> {
    let name = &args.name;
    if name.is_human() {
        bail!("{name} is the default human, not a webhook");
    }
    auth::check_not_reserved(name)?;
    let destination = match (&args.to, &args.channel) {
        (Some(ant), None) => Destination::Ant(ant.clone()),
        (None, Some(channel)) => Destination::Channel(channel.clone()),
        _ => bail!("give one of --to and --channel"),
    };
    let path = std::path::absolute(match &args.config {
        Some(path) => path.clone(),
        None => webhook::default_path()?,
    })?;
    let old = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e).with_context(|| format!("reading webhook config {}", path.display())),
    };
    let listing = Listing::parse(&old).with_context(|| format!("parsing webhook config {}", path.display()))?;
    if listing.webhooks.contains(name) {
        bail!("webhook {name} is already listed in {}", path.display());
    }
    if args.signing_token && args.preset != Some(Preset::Gitlab) {
        bail!("--signing-token goes with --preset gitlab");
    }
    let url = url(&listing, name);
    let given = given_secret(&args, &url)?;
    let dir = path.parent().context("the config file has no directory")?;
    let team = match &args.dir {
        Some(dir) => dir.clone(),
        None => auth::default_dir()?,
    };
    let out = std::path::absolute(args.out.clone().unwrap_or_else(|| team.join(format!("{name}.creds"))))?;

    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let secret_file = dir.join(format!("{name}.secret"));
    let secret = given.clone().unwrap_or_else(new_secret);
    auth::write_secret(&secret_file, &format!("{secret}\n"))?;
    let settings = Settings::of(args.preset, args.signing_token);
    let entry = entry(name, &out, &format!("{name}.secret"), &destination, &settings)?;
    let text = appended(&old, &entry);
    check(&text, &entry, &path)?;

    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }
    let domain = js_domain.or(listing.js_domain.as_ref());
    credentials(&args, args.hub.as_deref().unwrap_or(nats), domain, &team, &destination, &out).await?;
    webhook::check_creds("webhook", name, Some(&out), &destination)?;
    ants_daemon::config::write(&path, &text)?;
    println!("webhook {name} listed in {}", path.display());
    println!("credentials of {name} in {}", out.display());
    println!("secret of {name} in {}", secret_file.display());

    reload(name, &path);
    println!();
    if listing.public_url.is_none() {
        let listen = listing.listen.as_deref().unwrap_or(webhook::DEFAULT_LISTEN);
        println!(
            "the ingress listens on http://{listen}{}/{name}; give the sender the address the reverse proxy maps to it",
            listing.path
        );
    }
    let shown = if given.is_some() { "<the secret given>" } else { &secret };
    println!("{}", sender(args.preset, args.signing_token, &url, shown));
    Ok(())
}

/// The credentials of the webhook `args` adds, written to `out`: issued with the account key in `team`, for the
/// JetStream domain `domain` if any, or with --enroll asked for over the server at `url` and awaited.
async fn credentials(
    args: &AddArgs,
    url: &str,
    domain: Option<&JsDomain>,
    team: &Path,
    destination: &Destination,
    out: &Path,
) -> Result<()> {
    let name = &args.name;
    match &args.enroll {
        None => {
            let permissions = Permissions::for_webhook(name, std::slice::from_ref(destination)).with_js_domain(domain);
            auth::Issue::new(team, name, issued::Kind::Webhook, permissions).into_file(out)?;
        }
        Some(enrollment) => {
            let user = KeyPair::new_user();
            eprintln!(
                "asking for the credentials of {name} ({}); approve with `ants enroll approve {name} --webhook {}`",
                user.public_key(),
                destination_flag(destination)
            );
            let stop = async {
                let _ = tokio::signal::ctrl_c().await;
            };
            let approval =
                enroll::request(url, enroll::Enrollment::new(enrollment, name, &user), ENROLL_REPEAT, stop).await?;
            auth::write_creds(out, &approval.jwt, &user)?;
        }
    }
    Ok(())
}

/// Reload the webhook service to serve `name`, listed in the config file `path`, and print how that went.
fn reload(name: &UserId, path: &Path) {
    match Services::local().apply(Service::Webhook, Action::Reload, path) {
        Outcome::Done(command) => println!("ants webhook serve reloaded to serve {name} ({command})"),
        Outcome::NotRunning => println!("the webhook service is not running; it serves {name} once started"),
        outcome => println!(
            "{}a running `ants webhook serve --config {}` serves {name} within seconds, or at once on SIGHUP ({})",
            failure(outcome),
            path.display(),
            crate::service::manual(Service::Webhook, Action::Reload)
        ),
    }
}

/// The URL the sender posts the events of webhook `name` to: under `public_url`, else behind the reverse proxy.
fn url(listing: &Listing, name: &UserId) -> String {
    match &listing.public_url {
        Some(base) => format!("{}/{name}", base.trim_end_matches('/')),
        None => format!("https://<reverse proxy>{}/{name}", listing.path),
    }
}

/// The secret given for the webhook, trimmed: from `--secret-file`, `--secret-env`, or, for a signing token, the
/// terminal, after saying where to get it for the webhook at `url`; None to make a new one.
fn given_secret(args: &AddArgs, url: &str) -> Result<Option<String>> {
    let given = match (&args.secret_file, &args.secret_env) {
        (Some(file), _) => {
            let mut text = String::new();
            if file == Path::new("-") {
                std::io::stdin().read_to_string(&mut text).context("reading the secret from standard input")?;
            } else {
                text = std::fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
            }
            text.lines().next().unwrap_or_default().to_owned()
        }
        (None, Some(var)) => std::env::var(var).with_context(|| format!("environment variable {var} is not set"))?,
        (None, None) if args.signing_token => {
            eprintln!(
                "in GitLab (Settings > Webhooks > Add new webhook), set URL {url} and select Generate signing token"
            );
            crate::credentials::backup::read_hidden(
                "GitLab's signing token",
                "signing token",
                "give --secret-file FILE ('-' for standard input) or --secret-env VAR",
            )?
        }
        (None, None) => return Ok(None),
    };
    let given = given.trim();
    if given.is_empty() {
        bail!("the secret given is empty");
    }
    if args.signing_token {
        webhook::signing_key(given).context("the signing token given")?;
    }
    Ok(Some(given.to_owned()))
}

/// A new secret: 32 random bytes in hex.
fn new_secret() -> String {
    let mut bytes = [0u8; 32];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut bytes).expect("system randomness");
    HEXLOWER.encode(&bytes)
}

/// The `[[webhook]]` entry of webhook `name`, with its credentials at `creds` and its secret in `secret_file`.
fn entry(
    name: &UserId,
    creds: &Path,
    secret_file: &str,
    destination: &Destination,
    settings: &Settings,
) -> Result<String> {
    let creds = creds.to_str().with_context(|| format!("{} is no UTF-8", creds.display()))?;
    let mut lines = vec![
        "[[webhook]]".to_owned(),
        format!("name = {}", toml_string(name.as_str())),
        format!("creds = {}", toml_string(creds)),
        format!("auth = {}", toml_string(settings.auth)),
        format!("secret_file = {}", toml_string(secret_file)),
    ];
    if let Some((key, header)) = settings.header {
        lines.push(format!("{key} = {}", toml_string(header)));
    }
    if let Some(id) = settings.id {
        lines.push(format!("id = {}", toml_string(id)));
    }
    lines.push(match destination {
        Destination::Ant(ant) => format!("ant = {}", toml_string(ant.as_str())),
        Destination::Channel(channel) => format!("channel = {}", toml_string(channel.as_str())),
    });
    if let Some(text) = settings.text {
        lines.push(format!("text = {}", toml_string(text)));
    }
    Ok(lines.join("\n") + "\n")
}

/// `old`, the text of a config file, with `entry` at its end, a blank line between.
fn appended(old: &str, entry: &str) -> String {
    let mut text = old.to_owned();
    if !text.is_empty() {
        if !text.ends_with('\n') {
            text.push('\n');
        }
        text.push('\n');
    }
    text.push_str(entry);
    text
}

/// Fail unless `text`, the config file at `path` with `entry` added, parses, and `entry` alone loads: its secret
/// read, its id and template understood.
fn check(text: &str, entry: &str, path: &Path) -> Result<()> {
    Listing::parse(text).with_context(|| format!("webhook config {} with the new entry", path.display()))?;
    let home = std::env::home_dir().context("no home directory")?;
    Config::parse(entry, path, &home, |_| None).map(drop)
}

/// The flag of `destination`, as `ants auth user --webhook` takes it.
fn destination_flag(destination: &Destination) -> String {
    match destination {
        Destination::Ant(ant) => format!("--to {ant}"),
        Destination::Channel(channel) => format!("--channel {channel}"),
    }
}

/// What to set at the sender of `preset` (GitLab with its `signing_token`) for the webhook at `url` with `secret`.
fn sender(preset: Option<Preset>, signing_token: bool, url: &str, secret: &str) -> String {
    match preset {
        Some(Preset::Gitlab) if signing_token => format!(
            "GitLab (Settings > Webhooks > Add new webhook):\n  URL: {url}\n  Signing token: the one given (Generate \
             signing token), with no secret token"
        ),
        None => format!("send: POST {url}\n  header: Authorization: Bearer {secret}"),
        Some(Preset::Github) => format!(
            "GitHub (Settings > Webhooks > Add webhook):\n  Payload URL: {url}\n  Content type: application/json\n  \
             Secret: {secret}"
        ),
        Some(Preset::Gitlab) => {
            format!("GitLab (Settings > Webhooks > Add new webhook):\n  URL: {url}\n  Secret token: {secret}")
        }
        Some(Preset::Grafana) => format!(
            "Grafana (Alerting > Contact points > Webhook):\n  URL: {url}\n  HMAC signature secret: {secret}\n  \
             HMAC signature header: X-Grafana-Alerting-Signature (the default), with no timestamp header"
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use ants_test_support::scratch_dir;
    use hyper::http::HeaderMap;

    use super::*;

    fn parsed(entry: &str, dir: &Path) -> Config {
        let secret = dir.join("hook.secret");
        auth::write_secret(&secret, "s3cret\n").unwrap();
        Config::parse(entry, &dir.join("webhooks.toml"), dir, |_| None).unwrap()
    }

    #[test]
    fn each_preset_makes_an_entry_that_loads_and_checks_its_senders_requests() {
        let dir = scratch_dir("webhook-add-presets");
        let name: UserId = "hook".parse().unwrap();
        let creds = dir.join("hook.creds");
        let ant = Destination::Ant("dev-a".parse().unwrap());
        let body =
            br#"{"repository":{"full_name":"o/r"},"project":{"path_with_namespace":"g/p"},"title":"T","message":"M"}"#;
        let signature = {
            let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"s3cret");
            HEXLOWER.encode(ring::hmac::sign(&key, body).as_ref())
        };
        let headers = |pairs: &[(&str, String)]| {
            let mut map = HeaderMap::new();
            for (k, v) in pairs {
                map.insert(hyper::http::HeaderName::from_bytes(k.as_bytes()).unwrap(), v.parse().unwrap());
            }
            map
        };

        let github = parsed(
            &entry(&name, &creds, "hook.secret", &ant, &Settings::of(Some(Preset::Github), false)).unwrap(),
            &dir,
        );
        let hook = &github.webhooks[0];
        let signed = headers(&[
            ("x-hub-signature-256", format!("sha256={signature}")),
            ("x-github-delivery", "d-1".into()),
            ("x-github-event", "push".into()),
        ]);
        assert!(hook.accepts(&signed, body));
        assert!(!hook.accepts(&headers(&[("x-hub-signature-256", "sha256=00".into())]), body));
        let (id, text) = hook.event_of(&signed, body);
        assert_eq!(text, format!("GitHub push on o/r\n\n{}", std::str::from_utf8(body).unwrap()));
        assert_eq!(hook.event_of(&signed, b"{}").0, id, "the delivery identifies the event");

        let gitlab = parsed(
            &entry(&name, &creds, "hook.secret", &ant, &Settings::of(Some(Preset::Gitlab), false)).unwrap(),
            &dir,
        );
        let hook = &gitlab.webhooks[0];
        let sent = headers(&[
            ("x-gitlab-token", "s3cret".into()),
            ("idempotency-key", "k-1".into()),
            ("x-gitlab-event", "Push Hook".into()),
        ]);
        assert!(hook.accepts(&sent, body));
        assert!(!hook.accepts(&headers(&[("x-gitlab-token", "wrong".into())]), body));
        assert!(!hook.accepts(&headers(&[("authorization", "Bearer s3cret".into())]), body));
        assert_eq!(hook.event_of(&sent, b"{}").0, hook.event_of(&sent, body).0, "the key identifies the event");
        assert!(hook.event_of(&sent, body).1.starts_with("GitLab Push Hook on g/p\n\n"));

        auth::write_secret(&dir.join("hook.secret"), "whsec_c2lnbmluZy1rZXk=\n").unwrap();
        let settings = Settings::of(Some(Preset::Gitlab), true);
        let signed = entry(&name, &creds, "hook.secret", &ant, &settings).unwrap();
        let signed = Config::parse(&signed, &dir.join("webhooks.toml"), &dir, |_| None).unwrap();
        let hook = &signed.webhooks[0];
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs().to_string();
        let signed_with_token = {
            let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"signing-key");
            let signed = [b"m-1.", now.as_bytes(), b".", body].concat();
            data_encoding::BASE64.encode(ring::hmac::sign(&key, &signed).as_ref())
        };
        let sent = headers(&[
            ("webhook-id", "m-1".into()),
            ("webhook-timestamp", now),
            ("webhook-signature", format!("v1,{signed_with_token}")),
            ("x-gitlab-event", "Push Hook".into()),
        ]);
        assert!(hook.accepts(&sent, body));
        assert!(!hook.accepts(&headers(&[("x-gitlab-token", "whsec_c2lnbmluZy1rZXk=".into())]), body));
        assert_eq!(hook.event_of(&sent, b"{}").0, hook.event_of(&sent, body).0, "the webhook-id identifies the event");
        assert!(hook.event_of(&sent, body).1.starts_with("GitLab Push Hook on g/p\n\n"));

        let grafana = parsed(
            &entry(&name, &creds, "hook.secret", &ant, &Settings::of(Some(Preset::Grafana), false)).unwrap(),
            &dir,
        );
        let hook = &grafana.webhooks[0];
        let sent = headers(&[("x-grafana-alerting-signature", signature)]);
        assert!(hook.accepts(&sent, body));
        assert!(!hook.accepts(&headers(&[("x-grafana-alerting-signature", "00".into())]), body));
        assert_eq!(hook.event_of(&sent, body).1, "Grafana: T\n\nM");

        let channel = Destination::Channel("ops".parse().unwrap());
        let bearer = parsed(&entry(&name, &creds, "hook.secret", &channel, &Settings::of(None, false)).unwrap(), &dir);
        let hook = &bearer.webhooks[0];
        assert_eq!(hook.destination, channel);
        assert!(hook.accepts(&headers(&[("authorization", "Bearer s3cret".into())]), body));
        assert!(!hook.accepts(&headers(&[("authorization", "Bearer other".into())]), body));
    }

    fn args(name: &str, config: &Path, team: &Path) -> AddArgs {
        AddArgs {
            name: name.parse().unwrap(),
            to: Some("dev-a".parse().unwrap()),
            channel: None,
            preset: Some(Preset::Github),
            signing_token: false,
            secret_file: None,
            secret_env: None,
            config: Some(config.to_owned()),
            dir: Some(team.to_owned()),
            out: None,
            enroll: None,
            hub: None,
        }
    }

    #[tokio::test]
    async fn add_lists_a_webhook_that_loads_keeping_the_file_and_refuses_one_listed() {
        let dir = scratch_dir("webhook-add");
        let team = dir.join("auth");
        auth::init(&team).unwrap();
        let config = dir.join("hooks/webhooks.toml");
        add(args("github", &config, &team), "nats://127.0.0.1:4222", None).await.unwrap();
        let first = std::fs::read_to_string(&config).unwrap();
        assert!(first.starts_with("[[webhook]]\nname = \"github\"\n"), "{first}");
        let edited = format!("# the hub's webhooks\n{first}");
        std::fs::write(&config, &edited).unwrap();

        let mut other = args("alerts", &config, &team);
        (other.to, other.channel, other.preset) = (None, Some("ops".parse().unwrap()), None);
        add(other, "nats://127.0.0.1:4222", None).await.unwrap();
        let text = std::fs::read_to_string(&config).unwrap();
        assert!(text.starts_with(&format!("{edited}\n[[webhook]]\nname = \"alerts\"\n")), "{text}");
        let loaded = Config::load(&config).unwrap();
        let names: Vec<&str> = loaded.webhooks.iter().map(|w| w.name.as_str()).collect();
        assert_eq!(names, ["github", "alerts"]);
        assert_eq!(loaded.webhooks[0].creds.as_deref(), Some(team.join("github.creds").as_path()));
        assert_eq!(loaded.webhooks[1].destination, Destination::Channel("ops".parse().unwrap()));

        let err = add(args("github", &config, &team), "nats://127.0.0.1:4222", None).await.unwrap_err();
        assert!(err.to_string().contains("already listed"), "{err:#}");
        assert_eq!(std::fs::read_to_string(&config).unwrap(), text);
    }

    #[tokio::test]
    async fn a_gitlab_signing_token_given_is_kept_and_checked_and_goes_with_gitlab_only() {
        let dir = scratch_dir("webhook-add-signing-token");
        let team = dir.join("auth");
        auth::init(&team).unwrap();
        let config = dir.join("webhooks.toml");
        let token = dir.join("token");
        std::fs::write(&token, "whsec_c2lnbmluZy1rZXk=\n").unwrap();
        let mut github = args("github", &config, &team);
        (github.signing_token, github.secret_file) = (true, Some(token.clone()));
        let err = add(github, "nats://127.0.0.1:4222", None).await.unwrap_err();
        assert!(err.to_string().contains("--signing-token goes with --preset gitlab"), "{err:#}");

        let mut gitlab = args("gitlab", &config, &team);
        (gitlab.preset, gitlab.signing_token, gitlab.secret_file) = (Some(Preset::Gitlab), true, Some(dir.join("bad")));
        std::fs::write(dir.join("bad"), "not base64!\n").unwrap();
        let err = add(gitlab, "nats://127.0.0.1:4222", None).await.unwrap_err();
        assert!(format!("{err:#}").contains("no Standard Webhooks signing secret"), "{err:#}");
        assert!(!config.exists() && !dir.join("gitlab.secret").exists(), "nothing written");

        let mut gitlab = args("gitlab", &config, &team);
        (gitlab.preset, gitlab.signing_token, gitlab.secret_file) = (Some(Preset::Gitlab), true, Some(token));
        add(gitlab, "nats://127.0.0.1:4222", None).await.unwrap();
        assert_eq!(std::fs::read_to_string(dir.join("gitlab.secret")).unwrap(), "whsec_c2lnbmluZy1rZXk=\n");
        let text = std::fs::read_to_string(&config).unwrap();
        assert!(text.contains("auth = \"standard-webhooks\"\n") && text.contains("id = \"header:webhook-id\"\n"));
        Config::load(&config).unwrap();
    }

    #[test]
    fn an_entry_is_appended_after_the_text_there_comments_included() {
        let entry = "[[webhook]]\nname = \"b\"\n";
        assert_eq!(appended("", entry), entry);
        assert_eq!(appended("# hooks\nlisten = \"x\"", entry), format!("# hooks\nlisten = \"x\"\n\n{entry}"));
        assert_eq!(appended("# hooks\n", entry), format!("# hooks\n\n{entry}"));
    }
}
