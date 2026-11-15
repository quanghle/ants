//! `ants login`: a human registers a device with a GitHub sign-in, asking `ants signer` on the hub with the team's
//! public bootstrap credentials ([`LoginRequest`]). The device's NKey is generated here and its seed never leaves the
//! machine: it stays in memory while the request waits, and is written, to the `.creds` file or the OS credential
//! store, only once the signed user JWT arrives, so a refusal, an expiry or Ctrl-C leaves neither key nor file.
//!
//! The signer seals each answer to the request's one-off X25519 key ([`LoginReply`]): anyone holding the bootstrap
//! credentials hears it, and only this machine opens it. A sign-in page is shown only when it is GitHub's device login
//! page, and a JWT is taken only when the account of the bootstrap credentials issued it to the device's key, for a
//! human of the name asked for.

use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_daemon::creds::{self, Finding, Kind, Seed};
use ants_daemon::keystore::{self, Store};
use ants_daemon::{bus, config};
use ants_proto::{BOOTSTRAP_USER, LOGIN_REPEAT, LoginAnswer, LoginReply, LoginRequest, UserId, subject};
use anyhow::{Context as _, Result, anyhow, bail};
use chrono::Utc;
use data_encoding::BASE64URL_NOPAD;
use futures::StreamExt as _;
use nkeys::{KeyPair, XKey};

use crate::keys::{self, Proven};
use crate::{auth, defaults, revoke};

/// GitHub's device login page: the only sign-in page `ants login` shows.
pub const GITHUB_DEVICE_PAGE: &str = "https://github.com/login/device";

#[derive(clap::Args, Debug)]
pub struct LoginArgs {
    /// The hub's NATS server, where `ants signer` answers registrations (nats://, tls://, ws:// or wss://):
    /// registration never goes through a leaf node.
    #[arg(long, value_name = "URL")]
    pub hub: String,
    /// The human name to register the device for. A name already registered with the same GitHub user gets one more
    /// device; a name bound to another GitHub user is refused.
    #[arg(long)]
    pub name: UserId,
    /// A label for the device, such as "Quang's laptop", shown by `ants auth devices`.
    #[arg(long)]
    pub label: Option<String>,
    /// Keep the seeds in the OS credential store (macOS keychain, Linux Secret Service, Windows Credential Manager),
    /// never in a file: the credentials file names the stored keys, written once they read back and authenticate at
    /// the hub.
    #[arg(long)]
    pub store_key: bool,
    /// The team's public bootstrap credentials, from `ants auth bootstrap` (default:
    /// ~/.config/ants/auth/bootstrap.creds).
    #[arg(long, value_name = "FILE")]
    pub bootstrap: Option<PathBuf>,
}

/// A registration to ask for, and where its credentials go.
pub struct Login<'a> {
    pub hub: &'a str,
    pub name: &'a UserId,
    pub label: Option<&'a str>,
    pub store_key: bool,
    pub bootstrap: &'a Path,
    /// Where the credentials are written.
    pub out: &'a Path,
    /// The config file whose `[cli]` gets the credentials and the hub when it names no credentials.
    pub config: &'a Path,
    /// The only sign-in page shown: [`GITHUB_DEVICE_PAGE`].
    pub device_page: &'a str,
    /// How often the request is repeated: [`LOGIN_REPEAT`].
    pub repeat: Duration,
}

/// Run `ants login`: register a device of `--name`, writing `<name>.creds` in `~/.config/ants/auth` and pointing the
/// config file's `[cli]` at them and at the hub when it names no credentials.
pub async fn run(args: LoginArgs) -> Result<()> {
    let dir = auth::default_dir()?;
    let bootstrap = args.bootstrap.unwrap_or_else(|| dir.join("bootstrap.creds"));
    let out = dir.join(format!("{}.creds", args.name));
    let config = defaults::path()?;
    let login = Login {
        hub: &args.hub,
        name: &args.name,
        label: args.label.as_deref(),
        store_key: args.store_key,
        bootstrap: &bootstrap,
        out: &out,
        config: &config,
        device_page: GITHUB_DEVICE_PAGE,
        repeat: LOGIN_REPEAT,
    };
    let stop = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    register(&login, keystore::os(), stop).await
}

/// Register a device of `login.name`: ask until the signer answers with its credentials, a refusal, the request
/// expires or `stop` completes; then write the credentials, their seed in `store` with `login.store_key`, and set the
/// config file's `[cli]`.
pub async fn register(login: &Login<'_>, store: &dyn Store, stop: impl Future<Output = ()>) -> Result<()> {
    if let Some(label) = login.label {
        revoke::check_label(label)?;
    }
    bus::check_url(login.hub)?;
    let account = bootstrap_account(login.bootstrap)?;
    let key = KeyPair::new_user();
    let reply_key = XKey::new();
    let mut request = LoginRequest::new(login.name.clone(), key.public_key(), reply_key.public_key());
    request.label = login.label.map(str::to_owned);
    request.signature = auth::sign(&key, &request.signed())?;
    let bootstrap_user: UserId = BOOTSTRAP_USER.parse()?;
    let (client, _) =
        bus::connect_with(login.hub, Some(login.bootstrap), store, None, &bootstrap_user, "ants-login").await?;
    eprintln!(
        "asking ants signer at {} to register a device of {} (key {}); waiting (Ctrl-C stops)",
        login.hub,
        login.name,
        key.public_key()
    );
    let jwt = ask(&client, &request, &reply_key, &account, login, stop).await?;
    drop(client);
    write(login, &jwt, &key, store).await?;
    let finding = Finding::from_jwt(&jwt)?;
    println!("{}", login.out.display());
    if let Some(device) = &finding.device {
        eprintln!("device {device} of {}", login.name);
    }
    use_in_config(login)
}

/// The account of the bootstrap credentials at `path`: the issuer of the JWTs `ants login` takes.
fn bootstrap_account(path: &Path) -> Result<String> {
    let get = || {
        format!(
            "reading the bootstrap credentials {}: get the team's bootstrap.creds (`ants auth bootstrap` on the \
             machine holding the team keys) and give --bootstrap FILE",
            path.display()
        )
    };
    let (jwt, _) = auth::read_creds(path).with_context(get)?;
    let finding = Finding::from_jwt(&jwt).with_context(get)?;
    if finding.kind != Kind::Bootstrap {
        bail!("{} are the credentials of {}, not the team's bootstrap credentials", path.display(), finding.holder());
    }
    Ok(auth::verified(&jwt).with_context(get)?.iss)
}

/// Repeat `request` every `login.repeat` until an answer opened with `reply_key` settles it, showing what it waits
/// for; fail at a refusal, a sign-in page other than `login.device_page`, the request's expiry or `stop`. Returns the
/// user JWT of the answer, checked to be `account`'s for the request's key ([`check_signed`]).
async fn ask(
    client: &async_nats::Client,
    request: &LoginRequest,
    reply_key: &XKey,
    account: &str,
    login: &Login<'_>,
    stop: impl Future<Output = ()>,
) -> Result<String> {
    let mut replies = client.subscribe(subject::login_reply(&request.name)).await?;
    let payload = serde_json::to_vec(&request)?;
    let left = (request.expires_at - Utc::now()).to_std().unwrap_or_default();
    let expiry = tokio::time::sleep(left);
    let mut repeats = tokio::time::interval(login.repeat);
    let mut shown: Option<LoginAnswer> = None;
    tokio::pin!(expiry, stop);
    loop {
        tokio::select! {
            _ = repeats.tick() => {
                client.publish(request.subject(), payload.clone().into()).await?;
                client.flush().await?;
            }
            msg = replies.next() => {
                let msg = msg.context("the NATS connection closed")?;
                let answer = match open(&msg.payload, request, reply_key) {
                    Ok(Some(answer)) => answer,
                    Ok(None) => continue,
                    Err(e) => {
                        tracing::warn!("ignoring a registration answer: {e:#}");
                        continue;
                    }
                };
                let line = match &answer {
                    LoginAnswer::Signed { jwt } => match check_signed(jwt, request, account) {
                        Ok(()) => return Ok(jwt.clone()),
                        Err(e) => {
                            tracing::warn!("ignoring a registration answer: {e:#}");
                            continue;
                        }
                    },
                    LoginAnswer::Refused { reason } => bail!("the registration of {} was refused: {reason}", request.name),
                    LoginAnswer::Verify { url, .. } if url != login.device_page => bail!(
                        "the signer sent a sign-in page other than GitHub's device login page {}: not shown",
                        login.device_page
                    ),
                    _ if shown.as_ref() == Some(&answer) => continue,
                    LoginAnswer::Verify { url, user_code, expires_at } => {
                        let until = expires_at.with_timezone(&chrono::Local).format("%H:%M");
                        format!("sign in to GitHub at {url} and enter the code {user_code} (before {until})")
                    }
                    LoginAnswer::Waiting { login, reason } => {
                        format!("signed in to GitHub as {login}: {reason}; waiting for a human of the team to approve")
                    }
                };
                eprintln!("{line}");
                shown = Some(answer);
            }
            () = &mut expiry => bail!("the registration request of {} expired without an answer", request.name),
            () = &mut stop => bail!("stopped before an answer came"),
        }
    }
}

/// The answer `payload` carries for `request`, opened with `reply_key` and the sender's key it names; None for an
/// answer to another request.
fn open(payload: &[u8], request: &LoginRequest, reply_key: &XKey) -> Result<Option<LoginAnswer>> {
    let reply: LoginReply = serde_json::from_slice(payload).context("a malformed answer")?;
    if reply.name != request.name || !reply.answers(&request.public_key) {
        return Ok(None);
    }
    let sender = XKey::from_public_key(&reply.sender).map_err(|e| anyhow!("the sender's key {}: {e}", reply.sender))?;
    let sealed = BASE64URL_NOPAD.decode(reply.sealed.as_bytes()).context("a sealed answer that is not base64url")?;
    let opened = reply_key.open(&sealed, &sender).map_err(|e| anyhow!("an answer that does not open: {e}"))?;
    Ok(Some(serde_json::from_slice(&opened).context("a malformed sealed answer")?))
}

/// Check that `jwt` is a human's, issued by `account` to `request`'s key for its name.
fn check_signed(jwt: &str, request: &LoginRequest, account: &str) -> Result<()> {
    let claims = auth::verified(jwt)?;
    if claims.iss != account {
        bail!("a JWT signed by {}, not by the account {account} of the bootstrap credentials", claims.iss);
    }
    if claims.sub != request.public_key || claims.name != request.name.as_str() {
        bail!("a JWT issued to {} ({}), not to {} ({})", claims.name, claims.sub, request.name, request.public_key);
    }
    let finding = Finding::from_jwt(jwt)?;
    if finding.kind != Kind::Human {
        bail!("a JWT of {}, not of a human", finding.holder());
    }
    Ok(())
}

/// Write the credentials of `jwt` for `key` to `login.out`, replacing any there; with `login.store_key` the seed goes
/// to `store`, proven at the hub. Seeds the earlier credentials kept in `store` are deleted.
async fn write(login: &Login<'_>, jwt: &str, key: &KeyPair, store: &dyn Store) -> Result<()> {
    let out = login.out;
    if let Some(dir) = out.parent().filter(|dir| !dir.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let earlier = std::fs::read_to_string(out).ok();
    if login.store_key {
        if keys::write_new_stored(out, jwt, key, None, store, login.hub).await? == Proven::Unreached {
            eprintln!(
                "warning: the hub did not answer at {}, so the stored key {} has not authenticated yet",
                login.hub,
                key.public_key()
            );
        }
    } else {
        auth::replace_creds(out, jwt, key, None)?;
    }
    let Some(earlier) = earlier else { return Ok(()) };
    for seed in [creds::seed_of(&earlier), creds::xkey_of(&earlier)] {
        if let Some(Seed::Stored(stored)) = seed
            && let Err(e) = store.delete(stored)
        {
            eprintln!("warning: the earlier seed of {stored} stays in the OS credential store: {e:#}");
        }
    }
    if let Ok(finding) = Finding::from_creds(&earlier) {
        eprintln!(
            "replaced the earlier credentials of {} (key {}) in {}: they stay valid until they expire, or \
             `ants auth revoke --key {}` on the machine holding the team keys",
            finding.holder(),
            finding.key,
            out.display(),
            finding.key
        );
    }
    Ok(())
}

/// Point the config file's `[cli]` at the new credentials, and at the hub unless it names a server, when it names no
/// credentials ([`config::with_cli_login`]).
fn use_in_config(login: &Login<'_>) -> Result<()> {
    let out = std::path::absolute(login.out)?;
    let path = login.config;
    match config::with_cli_login(path, login.hub, &out)? {
        Some((text, nats)) => {
            if let Some(dir) = path.parent().filter(|dir| !dir.as_os_str().is_empty()) {
                std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
            }
            config::write(path, &text)?;
            let what = if nats { "creds and nats" } else { "creds" };
            eprintln!("set {what} of [cli] in {}: ants commands now act as {}", path.display(), login.name);
        }
        None => eprintln!(
            "[cli] in {} names other credentials, which stay the CLI's: use these with `ants --creds {} --nats {}`",
            path.display(),
            out.display(),
            login.hub
        ),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::{Arc, Mutex};

    use ants_daemon::keystore::Memory;
    use ants_proto::AntRequest;
    use tokio::time::timeout;

    use super::*;
    use crate::auth::tests::Server;
    use crate::github::tests::fake;
    use crate::signer;

    const WAIT: Duration = Duration::from_secs(20);
    const REPEAT: Duration = Duration::from_millis(200);
    const PAGE: &str = "https://github.example/login/device";

    /// A fake GitHub where the nth device flow is that of the nth of `users` (id, login), every one a member of acme:
    /// signed in at the second poll, or never for None.
    async fn github(users: &'static [Option<(u64, &'static str)>]) -> String {
        let flows = Arc::new(Mutex::new(0_usize));
        fake(Arc::new(move |what, auth, body| {
            let token = auth.and_then(|a| a.strip_prefix("Bearer tok-")).and_then(|n| n.parse::<usize>().ok());
            let user = token.and_then(|n| users.get(n).copied().flatten());
            match (what, user) {
                ("POST /login/device/code", _) => {
                    let mut flows = flows.lock().unwrap();
                    let n = *flows;
                    *flows += 1;
                    let interval = u8::from(users.get(n).copied().flatten().is_none());
                    (200, format!(r#"{{"device_code":"dc-{n}","user_code":"CODE-{n}","verification_uri":"{PAGE}","expires_in":900,"interval":{interval}}}"#))
                }
                ("POST /login/oauth/access_token", _) => {
                    let n = body.split('&').find_map(|kv| kv.strip_prefix("device_code=dc-")).unwrap();
                    match users.get(n.parse::<usize>().unwrap()).copied().flatten() {
                        Some(_) => (200, format!(r#"{{"access_token":"tok-{n}","token_type":"bearer","scope":"read:org"}}"#)),
                        None => (200, r#"{"error":"authorization_pending"}"#.to_owned()),
                    }
                }
                ("GET /user", Some((id, login))) => (200, format!(r#"{{"id":{id},"login":"{login}"}}"#)),
                ("GET /user/emails", Some(_)) => (200, "[]".to_owned()),
                ("GET /user/memberships/orgs/acme", Some(_)) => (200, r#"{"state":"active"}"#.to_owned()),
                _ => (404, r#"{"message":"Not Found"}"#.to_owned()),
            }
        }))
        .await
    }

    #[tokio::test]
    async fn a_device_registers_with_the_signer_and_keeps_no_key_when_refused() {
        let Some(server) = Server::start() else { return };
        let team = server.dir().join("auth");
        let url = github(&[Some((1, "octocat")), Some((1, "octocat")), Some((1, "octocat")), None, None]).await;
        let signer_config = server.dir().join("signer.toml");
        std::fs::write(
            &signer_config,
            format!("[registration]\nclient_id = \"Iv1.test\"\norgs = [\"acme\"]\ngithub_url = \"{url}\"\napi_url = \"{url}\"\n"),
        )
        .unwrap();
        let bootstrap = server.dir().join("bootstrap.creds");
        auth::issue_bootstrap(&team, Duration::ZERO, &bootstrap).unwrap();
        let account = auth::account_key(&team).unwrap();
        let device = server.dir().join("device");
        let config = device.join("config.toml");
        let store = Memory::default();
        let alice: UserId = "alice".parse().unwrap();
        let bob: UserId = "bob".parse().unwrap();
        let (alice_out, again_out, bob_out) =
            (device.join("alice.creds"), device.join("alice-2.creds"), device.join("bob.creds"));
        let first = Login {
            hub: &server.url,
            name: &alice,
            label: Some("laptop"),
            store_key: false,
            bootstrap: &bootstrap,
            out: &alice_out,
            config: &config,
            device_page: PAGE,
            repeat: REPEAT,
        };

        let options =
            signer::Options { config: &signer_config, approval_timeout: WAIT, reconnect: WAIT * 3, tick: REPEAT };
        let serving = Box::pin(signer::serve(
            &server.url,
            &team,
            None,
            &options,
            |_: &AntRequest, _: &signer::Event| {},
            pending(),
        ));
        let asking = Box::pin(async {
            register(&first, &store, pending()).await.unwrap();
            let text = std::fs::read_to_string(first.out).unwrap();
            let finding = Finding::from_creds(&text).unwrap();
            assert_eq!((&finding.kind, finding.name.as_str()), (&Kind::Human, "alice"));
            let (jwt, key) = auth::read_creds(first.out).unwrap();
            assert_eq!(auth::verified(&jwt).unwrap().iss, account);
            assert_eq!(finding.key, key.public_key());
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                assert_eq!(std::fs::metadata(first.out).unwrap().permissions().mode() & 0o777, 0o600);
            }
            let cli = config::CliDefaults::load(&config).unwrap();
            assert_eq!((cli.nats.as_deref(), cli.creds.as_deref()), (Some(server.url.as_str()), Some(first.out)));
            let client = async_nats::ConnectOptions::with_credentials_file(first.out).await.unwrap();
            client.connect(&server.url).await.expect("the credentials authenticate at the hub");

            let mut forged = LoginRequest::new(alice.clone(), KeyPair::new_user().public_key(), "X");
            assert!(check_signed(&jwt, &forged, &account).unwrap_err().to_string().contains("not to alice"));
            forged.public_key = key.public_key();
            assert!(check_signed(&jwt, &forged, "AOTHER").unwrap_err().to_string().contains("not by the account"));
            check_signed(&jwt, &forged, &account).unwrap();

            let again = Login { store_key: true, out: &again_out, ..first };
            register(&again, &store, pending()).await.unwrap();
            let text = std::fs::read_to_string(again.out).unwrap();
            let Some(Seed::Stored(stored)) = creds::seed_of(&text) else { panic!("no stored seed: {text}") };
            assert!(store.get(stored).unwrap().is_some(), "the seed is in the store");
            assert_eq!(config::CliDefaults::load(&config).unwrap().creds.as_deref(), Some(first.out), "the CLI's stay");
            let devices = revoke::issued(&team).unwrap().into_iter().filter(|i| i.name == "alice").count();
            assert_eq!(devices, 2, "a second device of alice");

            let refused = Login { name: &bob, out: &bob_out, ..first };
            let err = format!("{:#}", register(&refused, &store, pending()).await.unwrap_err());
            assert!(err.contains("refused") && err.contains("octocat is bound to alice"), "{err}");
            assert!(!refused.out.exists());

            let phished = Login { device_page: GITHUB_DEVICE_PAGE, ..refused };
            let err = format!("{:#}", register(&phished, &store, pending()).await.unwrap_err());
            assert!(err.contains("other than GitHub's device login page"), "{err}");
            assert!(!phished.out.exists());

            let stopped = &refused;
            let err = register(stopped, &store, tokio::time::sleep(REPEAT * 5)).await.unwrap_err().to_string();
            assert!(err.contains("stopped"), "{err}");
            assert!(!stopped.out.exists());
            assert_eq!(std::fs::read_dir(&device).unwrap().count(), 3, "alice's two files and the config");
        });
        timeout(WAIT * 4, async {
            tokio::select! {
                r = serving => panic!("the signer stopped: {r:?}"),
                () = asking => {}
            }
        })
        .await
        .unwrap();
    }
}
