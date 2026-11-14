//! Revoking issued credentials before they expire.
//!
//! Every user key the team directory issues or signs is recorded in [`ISSUED`], one JSON object per line, so that
//! `ants auth revoke NAME` finds all the keys of a name: a human may hold credentials on several devices, and
//! enrollment and re-issued credentials add more. Revocations live in [`REVOKED`]; the account JWT in `server.conf`
//! carries them as NATS revocations (a user key and a time: JWTs of that key issued at or before it are refused),
//! and [`push`] sends that account JWT to a running server through the system account, which closes the revoked
//! users' connections at once.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write as _;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};

use crate::auth;

/// The registry of issued user keys in the team directory.
pub const ISSUED: &str = "issued.jsonl";
/// The revocations in the team directory.
pub const REVOKED: &str = "revoked.json";

const PUSH_TIMEOUT: Duration = Duration::from_secs(10);

/// What credentials an issued key is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Kind {
    Ant,
    Human,
    Webhook,
    Leaf,
    ChannelManager,
    Enrollment,
}

impl From<ants_proto::Role> for Kind {
    fn from(role: ants_proto::Role) -> Self {
        match role {
            ants_proto::Role::Human => Self::Human,
            ants_proto::Role::Ant => Self::Ant,
        }
    }
}

/// One user key the team issued, a line of [`ISSUED`]. Fields added later are optional, so older lines still read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Issued {
    /// The user's public NKey, `U...`.
    pub key: String,
    /// The name its JWT carries: the ant, human or webhook, `leaf-<machine>`, `channel-manager` or `enroll-<ant>`.
    pub name: String,
    pub kind: Kind,
    /// Seconds since the epoch.
    pub issued_at: u64,
    /// Seconds since the epoch; none for credentials that never expire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
}

impl Issued {
    /// A key issued now.
    pub fn new(key: &str, name: &str, kind: Kind, expires_at: Option<u64>) -> Result<Self> {
        Ok(Self { key: key.to_owned(), name: name.to_owned(), kind, issued_at: auth::now()?, expires_at })
    }
}

/// One revoked user key, an entry of [`REVOKED`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Revoked {
    pub key: String,
    /// Seconds since the epoch: JWTs of the key issued at or before it are refused.
    pub revoked_at: u64,
    /// The name its credentials carry, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// When the latest credentials known for the key expire; none when they never do, or are unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
}

impl Revoked {
    /// Whether every known credential of the key has expired at `now`, so the revocation is no longer needed.
    fn expired(&self, now: u64) -> bool {
        self.expires_at.is_some_and(|exp| exp <= now)
    }
}

/// Record `issued` in the registry of `dir`.
pub fn record(dir: &Path, issued: &Issued) -> Result<()> {
    let path = dir.join(ISSUED);
    let mut line = serde_json::to_string(issued)?;
    line.push('\n');
    // One append of a whole line, so concurrent issuers do not interleave.
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("opening {}", path.display()))?;
    file.write_all(line.as_bytes()).with_context(|| format!("writing {}", path.display()))
}

/// The keys recorded in the registry of `dir`, oldest first.
pub fn issued(dir: &Path) -> Result<Vec<Issued>> {
    let path = dir.join(ISSUED);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let lines = text.lines().enumerate().filter(|(_, line)| !line.trim().is_empty());
    lines.map(|(n, line)| serde_json::from_str(line).with_context(|| format!("{}:{}", path.display(), n + 1))).collect()
}

/// The revocations of `dir`, oldest first.
pub fn revoked(dir: &Path) -> Result<Vec<Revoked>> {
    let path = dir.join(REVOKED);
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes).with_context(|| format!("reading {}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

fn write_revoked(dir: &Path, revoked: &[Revoked]) -> Result<()> {
    let path = dir.join(REVOKED);
    let mut text = serde_json::to_string_pretty(revoked)?;
    text.push('\n');
    fs::write(&path, text).with_context(|| format!("writing {}", path.display()))
}

/// Revoke every key known for `name`, in the registry or in a `.creds` file of `dir` the team's account signed, and
/// each of `keys`, then write `server.conf` again ([`apply`]). Returns the keys revoked, some perhaps already expired;
/// fails without a change when `name` has no known key.
pub fn revoke(dir: &Path, name: Option<&str>, keys: &[String]) -> Result<Vec<Revoked>> {
    let now = auth::now()?;
    let issued = issued(dir)?;
    let mut targets: Vec<Revoked> = Vec::new();
    let add = |targets: &mut Vec<Revoked>, key: &str, name: Option<&str>, expires_at: Option<u64>| {
        match targets.iter_mut().find(|t| t.key == key) {
            // Of several credentials of one key, the revocation lasts until the last of them expires.
            Some(known) => known.expires_at = known.expires_at.zip(expires_at).map(|(a, b)| a.max(b)),
            None => targets.push(Revoked {
                key: key.to_owned(),
                revoked_at: now,
                name: name.map(str::to_owned),
                expires_at,
            }),
        }
    };
    if let Some(name) = name {
        for one in issued.iter().filter(|i| i.name == name) {
            add(&mut targets, &one.key, Some(name), one.expires_at);
        }
        for claims in team_creds(dir)?.into_iter().filter(|c| c.name == name) {
            add(&mut targets, &claims.sub, Some(name), claims.exp);
        }
        if targets.is_empty() {
            bail!(
                "no credentials of {name} known in {} ({ISSUED}, or a .creds file there); revoke them by public key \
                 with --key U...",
                dir.display()
            );
        }
    }
    for key in keys {
        auth::check_user_key(key)?;
        let known: Vec<&Issued> = issued.iter().filter(|i| i.key == *key).collect();
        match known.as_slice() {
            [] => add(&mut targets, key, None, None),
            known => {
                for one in known {
                    add(&mut targets, key, Some(&one.name), one.expires_at);
                }
            }
        }
    }
    let mut all = revoked(dir)?;
    all.retain(|r| !targets.iter().any(|t| t.key == r.key));
    all.extend(targets.iter().cloned());
    write_revoked(dir, &all)?;
    apply(dir)?;
    Ok(targets)
}

/// Drop the revocations whose credentials have all expired, and write the `server.conf` of `dir` with the rest.
/// Returns the account's public key and JWT.
pub fn apply(dir: &Path) -> Result<(String, String)> {
    let now = auth::now()?;
    let mut all = revoked(dir)?;
    let before = all.len();
    all.retain(|r| !r.expired(now));
    if all.len() != before {
        write_revoked(dir, &all)?;
    }
    let revocations: BTreeMap<String, u64> = all.into_iter().map(|r| (r.key, r.revoked_at)).collect();
    auth::write_server_conf(dir, &revocations)
}

/// The claims of the `.creds` files in `dir` that the team's account signed.
fn team_creds(dir: &Path) -> Result<Vec<auth::Claims>> {
    let account = auth::account_key(dir)?;
    let entries = fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))?;
    let mut found = Vec::new();
    for entry in entries {
        let path = entry?.path();
        if path.extension().is_none_or(|ext| ext != "creds") {
            continue;
        }
        let Ok(text) = fs::read_to_string(&path) else { continue };
        let claims = auth::creds_jwt(&text).and_then(|jwt| auth::verified(jwt).ok());
        found.extend(claims.filter(|c| c.iss == account));
    }
    Ok(found)
}

/// Print the revocations of `dir`: key, name if known, when revoked and until when it matters.
pub fn list(dir: &Path) -> Result<()> {
    let revoked = revoked(dir)?;
    if revoked.is_empty() {
        println!("no credentials revoked");
    }
    let now = auth::now()?;
    for r in revoked {
        let until = match r.expires_at {
            Some(exp) if exp <= now => format!("expired {}", time(exp)),
            Some(exp) => format!("expires {}", time(exp)),
            None => "no expiry".to_owned(),
        };
        println!("{}  {}  revoked {}  {until}", r.key, r.name.as_deref().unwrap_or("-"), time(r.revoked_at));
    }
    Ok(())
}

fn time(secs: u64) -> String {
    i64::try_from(secs)
        .ok()
        .and_then(|secs| chrono::DateTime::from_timestamp(secs, 0))
        .map_or_else(|| secs.to_string(), |t| t.to_rfc3339())
}

/// Send the team's account JWT, with its revocations, to the server at `url`, as a user of the system account signed
/// with its key in `dir`; the server closes the connections of users revoked since. Returns the server's name.
pub async fn push(url: &str, dir: &Path) -> Result<String> {
    ants_daemon::bus::check_url(url)?;
    let (account, jwt) = apply(dir)?;
    let creds = auth::system_creds(dir)?;
    let update = async {
        let client = async_nats::ConnectOptions::with_credentials(&creds)?
            .name("ants-auth")
            .connection_timeout(PUSH_TIMEOUT)
            .connect(url)
            .await
            .context("connecting to NATS")?;
        let answer = client
            .request(format!("$SYS.REQ.ACCOUNT.{account}.CLAIMS.UPDATE"), jwt.into())
            .await
            .context("asking the server to update the account")?;
        anyhow::Ok(answer.payload)
    };
    let answer = tokio::time::timeout(PUSH_TIMEOUT, update).await.context("no answer from the server")??;
    let answer: serde_json::Value = serde_json::from_slice(&answer).context("the server's answer")?;
    let server = answer["server"]["name"].as_str().unwrap_or("?").to_owned();
    if let Some(description) = answer["error"]["description"].as_str() {
        bail!("server {server}: {description}");
    }
    match answer["data"]["message"].as_str() {
        Some("jwt updated") => Ok(server),
        Some(message) => bail!("server {server}: {message} (the account is not loaded there)"),
        None => bail!("server {server}: unexpected answer {answer}"),
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use super::*;
    use crate::auth::tests::Server;

    const WAIT: Duration = Duration::from_secs(5);

    #[test]
    fn revoking_a_name_takes_all_its_keys_and_drops_revocations_once_expired() {
        let dir = ants_test_support::scratch_dir("revoke");
        let team = dir.join("auth");
        auth::init(&team).unwrap();
        let alice: ants_proto::UserId = "alice".parse().unwrap();
        let human = |n: &str| {
            let out = team.join(format!("alice-{n}.creds"));
            auth::issue(&team, &alice, ants_proto::Role::Human, None, &out).unwrap();
            auth::verified(auth::creds_jwt(&fs::read_to_string(out).unwrap()).unwrap()).unwrap().sub
        };
        let (laptop, phone) = (human("laptop"), human("phone"));
        // Credentials issued before the registry are found through their .creds file.
        let old = nkeys::KeyPair::new_user();
        let jwt = auth::sign_user(&team, &alice, ants_proto::Role::Human, None, &old.public_key()).unwrap();
        auth::write_creds(&team.join("alice-old.creds"), &jwt, &old).unwrap();
        let registry: Vec<Issued> = issued(&team).unwrap().into_iter().filter(|i| i.key != old.public_key()).collect();
        let lines: String = registry.iter().map(|i| serde_json::to_string(i).unwrap() + "\n").collect();
        fs::write(team.join(ISSUED), lines).unwrap();

        let err = revoke(&team, Some("bob"), &[]).unwrap_err().to_string();
        assert!(err.contains("no credentials of bob"), "{err}");
        assert!(revoke(&team, None, &["not-a-key".to_owned()]).is_err());
        let mut keys: Vec<String> = revoke(&team, Some("alice"), &[]).unwrap().into_iter().map(|r| r.key).collect();
        keys.sort();
        let mut expected = vec![laptop.clone(), phone, old.public_key()];
        expected.sort();
        assert_eq!(keys, expected);
        let conf = fs::read_to_string(team.join(auth::SERVER_CONF)).unwrap();
        let account =
            conf.lines().find_map(|l| l.trim().strip_prefix(&format!("{}: ", auth::account_key(&team).unwrap())));
        let body = account.unwrap().split('.').nth(1).unwrap();
        let claims: serde_json::Value =
            serde_json::from_slice(&data_encoding::BASE64URL_NOPAD.decode(body.as_bytes()).unwrap()).unwrap();
        assert_eq!(claims["nats"]["revocations"].as_object().unwrap().len(), 3, "{claims}");

        // A key of credentials that expired needs no revocation: it is dropped on the next change.
        let enrollment = team.join("enroll.creds");
        auth::issue_enrollment(&team, &"dev-c".parse().unwrap(), Duration::from_secs(1), &enrollment).unwrap();
        let revoked_now = revoke(&team, Some("enroll-dev-c"), std::slice::from_ref(&laptop)).unwrap();
        assert_eq!(revoked_now.len(), 2);
        assert_eq!(revoked(&team).unwrap().len(), 4, "laptop's revocation is replaced, not repeated");
        std::thread::sleep(Duration::from_secs(2));
        apply(&team).unwrap();
        let left: Vec<Option<String>> = revoked(&team).unwrap().into_iter().map(|r| r.name).collect();
        assert_eq!(left, vec![Some("alice".to_owned()); 3]);
        fs::remove_dir_all(dir).unwrap();
    }

    /// A connection with `creds` to `url` that reports when the server closes it.
    async fn watched(url: &str, creds: &Path) -> Result<(async_nats::Client, mpsc::UnboundedReceiver<()>)> {
        let (tx, rx) = mpsc::unbounded_channel();
        let client = async_nats::ConnectOptions::with_credentials_file(creds)
            .await?
            .event_callback(move |event| {
                let tx = tx.clone();
                async move {
                    if let async_nats::Event::Disconnected = event {
                        let _ = tx.send(());
                    }
                }
            })
            .connect(url)
            .await?;
        Ok((client, rx))
    }

    #[tokio::test]
    async fn revoked_credentials_lose_their_connection_and_are_refused_after_a_push_or_a_reload() {
        let Some(server) = Server::start() else { return };
        let team = server.dir().join("auth");
        let issue = |user: &str, file: &str| {
            let out = team.join(file);
            auth::issue(&team, &user.parse().unwrap(), ants_proto::Role::Ant, None, &out).unwrap();
            out
        };
        let (a, b) = (issue("dev-a", "dev-a.creds"), issue("dev-b", "dev-b.creds"));
        let (a_client, mut a_closed) = watched(&server.url, &a).await.unwrap();
        let (b_client, mut b_closed) = watched(&server.url, &b).await.unwrap();
        for client in [&a_client, &b_client] {
            client.flush().await.unwrap();
        }

        // Pushed: dev-a's connection is closed at once and its credentials refused; dev-b's are untouched.
        revoke(&team, Some("dev-a"), &[]).unwrap();
        push(&server.url, &team).await.unwrap();
        timeout(WAIT, a_closed.recv()).await.expect("dev-a's connection stays open").unwrap();
        assert!(watched(&server.url, &a).await.is_err(), "dev-a's credentials still work");
        b_client.flush().await.unwrap();
        assert!(b_closed.try_recv().is_err(), "dev-b's connection was closed");
        // New credentials of the name are not revoked.
        let (renewed, _) = watched(&server.url, &issue("dev-a", "dev-a-new.creds")).await.unwrap();

        // Without a push, the server reloading server.conf applies the revocation too.
        revoke(&team, Some("dev-b"), &[]).unwrap();
        let reload = std::process::Command::new("kill").arg("-HUP").arg(server.pid().to_string()).status();
        assert!(reload.unwrap().success());
        timeout(WAIT, b_closed.recv()).await.expect("dev-b's connection stays open").unwrap();
        assert!(watched(&server.url, &b).await.is_err(), "dev-b's credentials still work");
        assert!(watched(&server.url, &a).await.is_err(), "dev-a's revocation stays after the reload");
        renewed.flush().await.unwrap();

        // An unreachable server leaves the revocations stored.
        let port = ants_test_support::free_port();
        assert!(push(&format!("nats://127.0.0.1:{port}"), &team).await.is_err());
        assert_eq!(revoked(&team).unwrap().len(), 2);
    }
}
