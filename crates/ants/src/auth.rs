//! Team credentials for NATS decentralized auth: one operator, one account holding the ants stream, and one user per
//! ant or human whose JWT carries the permissions of [`Permissions::for_user`] in its role.
//!
//! The operator and account seeds stay in the team directory on the machine that issues credentials. The server
//! needs only `server.conf`; each machine gets the `.creds` file of the ants it runs, and its leaf node the uplink
//! credentials of [`issue_leaf`].
//!
//! When the hub has a JetStream domain, credentials are issued for it: they allow the JetStream API as
//! `$JS.<domain>.API`, the prefix every ants process then uses, besides the plain `$JS.API` the hub maps it to
//! ([`Permissions::with_js_domain`]).

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ants_proto::{JsDomain, Machine, Permissions, Role, UserId};
use anyhow::{Context as _, Result, anyhow, bail};
use data_encoding::{BASE32_NOPAD, BASE64URL_NOPAD};
use nkeys::KeyPair;
use serde_json::{Value, json};

const OPERATOR_SEED: &str = "operator.nk";
const ACCOUNT_SEED: &str = "account.nk";
const SYSTEM_SEED: &str = "system.nk";
pub const SERVER_CONF: &str = "server.conf";
const TEAM: &str = "ants";
/// `max_control_line` of the servers that include [`SERVER_CONF`], hub and leaf nodes alike.
const MAX_CONTROL_LINE: u32 = 16 * 1024;

/// The default team directory, `~/.config/ants/auth`.
pub fn default_dir() -> Result<PathBuf> {
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")).context("no home directory")?;
    Ok(Path::new(&home).join(".config/ants/auth"))
}

/// Create the operator and account in `dir` and write the `server.conf` that trusts them.
pub fn init(dir: &Path) -> Result<()> {
    if dir.join(ACCOUNT_SEED).exists() {
        bail!("{} already holds a team", dir.display());
    }
    fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let operator = KeyPair::new_operator();
    let account = KeyPair::new_account();
    let system = KeyPair::new_account();
    write_secret(&dir.join(OPERATOR_SEED), &seed(&operator)?)?;
    write_secret(&dir.join(SYSTEM_SEED), &seed(&system)?)?;
    write_secret(&dir.join(ACCOUNT_SEED), &seed(&account)?)?;
    fs::write(dir.join(SERVER_CONF), server_conf(&operator, &system, &account)?)?;
    Ok(())
}

/// The role `ants auth user` issues: a human's with `--human`, and for the default human whatever the flag; else an
/// ant's.
pub fn role(user: &UserId, human: bool) -> Role {
    if human { Role::Human } else { Role::by_default(user) }
}

/// The ants and humans served on a machine, each in its role, without repeats: a name given as both is a human.
pub fn members(ants: &[UserId], humans: &[UserId]) -> Vec<(UserId, Role)> {
    let mut members: Vec<(UserId, Role)> = humans.iter().map(|h| (h.clone(), Role::Human)).collect();
    for ant in ants {
        if !members.iter().any(|(m, _)| m == ant) {
            members.push((ant.clone(), Role::by_default(ant)));
        }
    }
    members
}

/// Fail when `user` is a name the team reserves: [`UserId::channel_manager`], which no ant or human may take.
pub fn check_not_reserved(user: &UserId) -> Result<()> {
    if *user == UserId::channel_manager() {
        bail!("{user} is reserved for the channel manager (`ants auth channel-manager`); choose another name");
    }
    Ok(())
}

/// Issue a new NATS user for `user`, an ant or a human, in `role`, for the hub's JetStream domain if it has one, and
/// write its `.creds` file to `out`. Earlier credentials stay valid.
#[cfg(test)]
pub fn issue(dir: &Path, user: &UserId, role: Role, js_domain: Option<&JsDomain>, out: &Path) -> Result<()> {
    issue_with(dir, user, Permissions::for_user(user, role).with_js_domain(js_domain), out)
}

/// Issue a new NATS user for `user` with `permissions`, and write its `.creds` file to `out`.
pub fn issue_with(dir: &Path, user: &UserId, permissions: Permissions, out: &Path) -> Result<()> {
    check_not_reserved(user)?;
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let key = KeyPair::new_user();
    let jwt = user_jwt(&account, &key.public_key(), user.as_str(), permissions, json!({}), None)?;
    write_secret(out, &creds(&jwt, &seed(&key)?))
}

/// Issue the uplink user of `machine`'s leaf node and write its `.creds` file to `out`. It may carry only what
/// `members`, ants and humans each in its role ([`members`]), may do and the sourcing of the machine's outbox
/// ([`Permissions::for_leaf`]), plus what the credentials of `webhooks` allow, for an `ants webhook serve` on the
/// machine ([`webhook_permissions`]), and connect only as a leaf node, not as a client.
///
/// nats-server applies a leaf user's permissions from the leaf's side, as for a client: publish limits what the leaf
/// sends to the hub, subscribe what the hub sends to the leaf (the hub swaps them internally).
pub fn issue_leaf(
    dir: &Path,
    machine: &Machine,
    members: &[(UserId, Role)],
    webhooks: &[UserId],
    js_domain: Option<&JsDomain>,
    out: &Path,
) -> Result<()> {
    if members.is_empty() && webhooks.is_empty() {
        bail!("a leaf node needs at least one ant, human or webhook");
    }
    members.iter().map(|(member, _)| member).chain(webhooks).try_for_each(check_not_reserved)?;
    if js_domain.is_some_and(|domain| domain.as_str() == machine.as_str()) {
        bail!("machine {machine} would take the hub's JetStream domain; name it otherwise");
    }
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let user = KeyPair::new_user();
    let roles = members.iter().map(|(member, role)| (member, *role));
    let mut permissions = Permissions::for_leaf(machine, roles);
    for webhook in webhooks {
        let Permissions { publish, subscribe } = webhook_permissions(dir, webhook)?;
        for (all, new) in [(&mut permissions.publish, publish), (&mut permissions.subscribe, subscribe)] {
            all.extend(new);
        }
    }
    let mut permissions = permissions.with_js_domain(js_domain);
    for all in [&mut permissions.publish, &mut permissions.subscribe] {
        let mut seen = std::collections::HashSet::new();
        all.retain(|s| seen.insert(s.clone()));
    }
    let only_leaf = json!({ "allowed_connection_types": ["LEAFNODE", "LEAFNODE_WS"] });
    let jwt = user_jwt(&account, &user.public_key(), &format!("leaf-{machine}"), permissions, only_leaf, None)?;
    write_secret(out, &creds(&jwt, &seed(&user)?))
}

/// The permissions of the credentials of `webhook` in `dir`, `<webhook>.creds` as `ants auth user WEBHOOK --webhook`
/// writes them ([`Permissions::for_webhook`]): issued by the team's account, for `webhook`, hearing only its own reply
/// inboxes as a webhook's do.
pub fn webhook_permissions(dir: &Path, webhook: &UserId) -> Result<Permissions> {
    let path = dir.join(format!("{webhook}.creds"));
    let text = fs::read_to_string(&path).with_context(|| {
        format!(
            "reading {} (issue them first: `ants auth user {webhook} --webhook --to ANT` or `--channel CHANNEL`)",
            path.display()
        )
    })?;
    let jwt = creds_jwt(&text).with_context(|| format!("{}: no user JWT", path.display()))?;
    let (claims, body) = verified_body(jwt).with_context(|| path.display().to_string())?;
    if claims.iss != load(&dir.join(ACCOUNT_SEED))?.public_key() {
        bail!("{} were not issued by the team in {}", path.display(), dir.display());
    }
    if claims.name != webhook.as_str() {
        bail!("{} are {}'s, not {webhook}'s", path.display(), claims.name);
    }
    let allowed = |kind: &str| -> Vec<String> {
        let list = body["nats"][kind]["allow"].as_array().map(Vec::as_slice).unwrap_or_default();
        list.iter().filter_map(|s| s.as_str().map(str::to_owned)).collect()
    };
    let permissions = Permissions { publish: allowed("pub"), subscribe: allowed("sub") };
    if permissions.subscribe != [format!("{}.>", ants_proto::inbox_prefix(webhook))] {
        bail!("{} are no webhook's: issue them with `ants auth user {webhook} --webhook`", path.display());
    }
    Ok(permissions)
}

/// Issue the credentials of the channel manager, `ants channel serve`, for the hub's JetStream domain if it has one,
/// and write its `.creds` file to `out`: it hears channel management requests and answers each once, reads the roster
/// and the humans, writes the channels bucket and posts in channels as [`UserId::channel_manager`]
/// ([`Permissions::for_channel_manager`]).
pub fn issue_channel_manager(dir: &Path, js_domain: Option<&JsDomain>, out: &Path) -> Result<()> {
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let key = KeyPair::new_user();
    let permissions = Permissions::for_channel_manager().with_js_domain(js_domain);
    let ttl = u64::try_from(ants_proto::CHANADM_TTL.as_nanos())?;
    let answer_once = json!({ "resp": { "max": 1, "ttl": ttl } });
    let name = UserId::channel_manager();
    let jwt = user_jwt(&account, &key.public_key(), name.as_str(), permissions, answer_once, None)?;
    write_secret(out, &creds(&jwt, &seed(&key)?))
}

/// Sign the user JWT of `user` in `role` for `public_key`, a user NKey generated on another machine, with the
/// permissions [`issue`] gives. The seed stays where it was generated; its `.creds` file is the JWT plus that seed.
pub fn sign_user(
    dir: &Path,
    user: &UserId,
    role: Role,
    js_domain: Option<&JsDomain>,
    public_key: &str,
) -> Result<String> {
    sign_with(dir, user, Permissions::for_user(user, role).with_js_domain(js_domain), public_key)
}

/// Sign the user JWT of `user` with `permissions` for `public_key`, a user NKey generated on another machine.
pub fn sign_with(dir: &Path, user: &UserId, permissions: Permissions, public_key: &str) -> Result<String> {
    check_user_key(public_key)?;
    check_not_reserved(user)?;
    let account = load(&dir.join(ACCOUNT_SEED))?;
    user_jwt(&account, public_key, user.as_str(), permissions, json!({}), None)
}

/// Fail unless `public_key` is a user's public NKey, `U...`.
pub fn check_user_key(public_key: &str) -> Result<()> {
    let key = KeyPair::from_public_key(public_key).map_err(|e| anyhow!("public key {public_key}: {e}"))?;
    if key.key_pair_type() != nkeys::KeyPairType::User {
        bail!("{public_key} is not the public key of a user (U...)");
    }
    Ok(())
}

/// Issue the enrollment credentials of `ant`, valid for `ttl`, and write their `.creds` file to `out`: they may
/// only ask for the ant's credentials and hear the answer ([`Permissions::for_enrollment`]). Returns when they
/// expire, in seconds since the epoch.
pub fn issue_enrollment(dir: &Path, ant: &UserId, ttl: Duration, out: &Path) -> Result<u64> {
    check_not_reserved(ant)?;
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let user = KeyPair::new_user();
    let exp = now()? + ttl.as_secs().max(1);
    let name = ants_proto::enrollment_user(ant);
    let jwt = user_jwt(&account, &user.public_key(), &name, Permissions::for_enrollment(ant), json!({}), Some(exp))?;
    write_secret(out, &creds(&jwt, &seed(&user)?))?;
    Ok(exp)
}

/// The text of short-lived credentials for `ants enroll`, which hear enrollment requests and answer them
/// ([`Permissions::for_enroll_approver`]), issued afresh from the account key of each run.
pub fn approver_creds(dir: &Path) -> Result<String> {
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let user = KeyPair::new_user();
    let exp = now()? + APPROVER_TTL.as_secs();
    let jwt =
        user_jwt(&account, &user.public_key(), "enroll", Permissions::for_enroll_approver(), json!({}), Some(exp))?;
    Ok(creds(&jwt, &seed(&user)?))
}

const APPROVER_TTL: Duration = Duration::from_secs(10 * 60);

/// The claims of a user JWT that matter to enrollment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claims {
    /// The user's public key.
    pub sub: String,
    /// The account that signed it.
    pub iss: String,
    pub name: String,
    /// Expiry, in seconds since the epoch.
    pub exp: Option<u64>,
}

/// Decode a JWT and check that its issuer signed it.
pub fn verified(jwt: &str) -> Result<Claims> {
    verified_body(jwt).map(|(claims, _)| claims)
}

/// [`verified`], with all of the JWT's claims.
fn verified_body(jwt: &str) -> Result<(Claims, Value)> {
    let jwt = jwt.trim();
    let (signed, signature) = jwt.rsplit_once('.').context("not a JWT")?;
    let (_, body) = signed.split_once('.').context("not a JWT")?;
    let decode = |part: &str| BASE64URL_NOPAD.decode(part.as_bytes()).context("not a JWT");
    let claims: Value = serde_json::from_slice(&decode(body)?).context("not a JWT")?;
    let text = |field: &str| claims[field].as_str().map(str::to_owned).with_context(|| format!("JWT without {field}"));
    let iss = text("iss")?;
    let issuer = KeyPair::from_public_key(&iss).map_err(|e| anyhow!("JWT issuer {iss}: {e}"))?;
    issuer.verify(signed.as_bytes(), &decode(signature)?).map_err(|_| anyhow!("JWT not signed by its issuer {iss}"))?;
    let verified = Claims { sub: text("sub")?, iss, name: text("name")?, exp: claims["exp"].as_u64() };
    Ok((verified, claims))
}

/// The user JWT of a `.creds` file's text: the line after the `BEGIN NATS USER JWT` marker.
pub fn creds_jwt(creds: &str) -> Option<&str> {
    creds_block(creds, "BEGIN NATS USER JWT")
}

/// The seed of a `.creds` file's text: the line after the `BEGIN USER NKEY SEED` marker.
pub fn creds_seed(creds: &str) -> Option<&str> {
    creds_block(creds, "BEGIN USER NKEY SEED")
}

fn creds_block<'a>(creds: &'a str, marker: &str) -> Option<&'a str> {
    let mut lines = creds.lines().map(str::trim);
    lines.find(|line| line.starts_with("---") && line.contains(marker))?;
    lines.find(|line| !line.is_empty()).filter(|line| !line.starts_with("---"))
}

/// Write the `.creds` file of `jwt` and the seed of `user` to `out`, readable by its owner only.
pub fn write_creds(out: &Path, jwt: &str, user: &KeyPair) -> Result<()> {
    write_secret(out, &creds(jwt, &seed(user)?))
}

/// Write `user`'s seed alone to `out`, readable by its owner only.
pub fn write_seed(out: &Path, user: &KeyPair) -> Result<()> {
    write_secret(out, &format!("{}\n", seed(user)?))
}

/// Read a seed written by [`write_seed`].
pub fn read_seed(path: &Path) -> Result<KeyPair> {
    let seed = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    KeyPair::from_seed(seed.trim()).map_err(|e| anyhow!("{}: {e}", path.display()))
}

fn now() -> Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

fn server_conf(operator: &KeyPair, system: &KeyPair, account: &KeyPair) -> Result<String> {
    let operator_jwt = jwt(
        operator,
        &operator.public_key(),
        TEAM,
        json!({ "system_account": system.public_key(), "type": "operator", "version": 2 }),
        None,
    )?;
    // JetStream needs a system account in operator mode.
    let system_jwt = jwt(operator, &system.public_key(), "SYS", json!({ "type": "account", "version": 2 }), None)?;
    let account_jwt = jwt(
        operator,
        &account.public_key(),
        TEAM,
        json!({
            "limits": {
                "subs": -1, "data": -1, "payload": -1, "imports": -1, "exports": -1, "wildcards": true,
                "conn": -1, "leaf": -1, "mem_storage": -1, "disk_storage": -1, "streams": -1, "consumer": -1,
            },
            "default_permissions": { "pub": {}, "sub": {} },
            "type": "account",
            "version": 2,
        }),
        None,
    )?;
    Ok(format!(
        "# Written by `ants auth init`. Include it from the server configuration: include \"{SERVER_CONF}\"\n\
         # Credentials carry their permissions in the CONNECT line, longer than the 4 KB nats-server allows by default.\n\
         max_control_line: {MAX_CONTROL_LINE}\n\
         operator: {operator_jwt}\n\
         system_account: {system}\n\
         resolver: MEMORY\n\
         resolver_preload: {{\n  {system}: {system_jwt}\n  {account}: {account_jwt}\n}}\n",
        system = system.public_key(),
        account = account.public_key(),
    ))
}

/// A user JWT for the public key `user` with `permissions`, plus the `extra` claims, expiring at `exp` (seconds since
/// the epoch) if given.
fn user_jwt(
    account: &KeyPair,
    user: &str,
    name: &str,
    permissions: Permissions,
    extra: Value,
    exp: Option<u64>,
) -> Result<String> {
    let Permissions { publish, subscribe } = permissions;
    let mut nats = json!({
        "pub": { "allow": publish },
        "sub": { "allow": subscribe },
        "subs": -1, "data": -1, "payload": -1,
        "type": "user",
        "version": 2,
    });
    if let (Value::Object(nats), Value::Object(extra)) = (&mut nats, extra) {
        nats.extend(extra);
    }
    jwt(account, user, name, nats, exp)
}

/// A NATS v2 JWT for `subject`, signed by `issuer`, expiring at `exp` if given.
fn jwt(issuer: &KeyPair, subject: &str, name: &str, nats: Value, exp: Option<u64>) -> Result<String> {
    let mut claims = json!({ "iat": now()?, "iss": issuer.public_key(), "name": name, "sub": subject, "nats": nats });
    if let Some(exp) = exp {
        claims["exp"] = exp.into();
    }
    // As in nats-io/jwt: the id is the hash of the claims without it.
    let id = ring::digest::digest(&ring::digest::SHA512_256, &serde_json::to_vec(&claims)?);
    claims["jti"] = BASE32_NOPAD.encode(id.as_ref()).into();
    let header = BASE64URL_NOPAD.encode(br#"{"typ":"JWT","alg":"ed25519-nkey"}"#);
    let signed = format!("{header}.{}", BASE64URL_NOPAD.encode(&serde_json::to_vec(&claims)?));
    let signature = issuer.sign(signed.as_bytes()).map_err(|e| anyhow!("signing: {e}"))?;
    Ok(format!("{signed}.{}", BASE64URL_NOPAD.encode(&signature)))
}

fn creds(jwt: &str, seed: &str) -> String {
    format!(
        "-----BEGIN NATS USER JWT-----\n{jwt}\n------END NATS USER JWT------\n\n\
         ************************* IMPORTANT *************************\n\
         NKEY Seed printed below can be used to sign and prove identity.\n\
         NKEYs are sensitive and should be treated as secrets.\n\n\
         -----BEGIN USER NKEY SEED-----\n{seed}\n------END USER NKEY SEED------\n\n\
         *************************************************************\n"
    )
}

fn seed(key: &KeyPair) -> Result<String> {
    key.seed().map_err(|e| anyhow!("{e}"))
}

fn load(path: &Path) -> Result<KeyPair> {
    let seed =
        fs::read_to_string(path).with_context(|| format!("reading {} (run `ants auth init`)", path.display()))?;
    KeyPair::from_seed(seed.trim()).map_err(|e| anyhow!("{}: {e}", path.display()))
}

/// Write a file only its owner can read, replacing any earlier one.
fn write_secret(path: &Path, contents: &str) -> Result<()> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options.open(path).with_context(|| format!("writing {}", path.display()))?;
    // `mode` applies only on creation; tighten a file that already existed too.
    #[cfg(unix)]
    file.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
    file.write_all(contents.as_bytes())?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use ants_daemon::bus;
    use ants_daemon::channel::{Channels, Threads};
    use ants_daemon::daemon::Claims;
    use ants_daemon::dead::{self, Attempt};
    use ants_daemon::humans::Registry;
    use ants_daemon::notes;
    use ants_daemon::roster::Registration;
    use ants_daemon::webhooks::Webhooks;
    use ants_proto::{
        ChannelEntry, ChannelId, ChannelPost, DeadLetter, Destination, Envelope, NotesPost, RosterEntry, STREAM,
        STREAM_SUBJECTS, ThreadId, attempt_key, chan_consumer, chan_reader, inbox_prefix, link_key, notes_consumer,
        session_key, subject,
    };
    use ants_test_support::NatsServer;
    use async_nats::jetstream::AckKind;
    use async_nats::{Client, Event, jetstream};
    use futures::{StreamExt as _, TryStreamExt as _};
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use super::*;

    const WAIT: Duration = Duration::from_secs(5);

    /// A nats-server trusting a fresh team, whose keys are in its directory.
    pub(crate) struct Server(NatsServer);

    impl std::ops::Deref for Server {
        type Target = NatsServer;
        fn deref(&self) -> &NatsServer {
            &self.0
        }
    }

    impl Server {
        /// None when nats-server is not installed.
        pub(crate) fn start() -> Option<Self> {
            Self::start_with("", "")
        }

        /// A server trusting a fresh team, with `jetstream` added to its JetStream block and `extra` to its
        /// configuration.
        fn start_with(jetstream: &str, extra: &str) -> Option<Self> {
            NatsServer::spawn_with(|dir| {
                init(&dir.join("auth")).unwrap();
                let store = dir.join("js");
                format!("jetstream {{ store_dir: {store:?}\n{jetstream} }}\ninclude \"auth/{SERVER_CONF}\"\n{extra}")
            })
            .map(Self)
        }

        fn creds(&self, user: &UserId) -> PathBuf {
            self.creds_in(user, None)
        }

        /// Credentials of `user` issued for `domain`, as `ants auth user` without `--human` does.
        fn creds_in(&self, user: &UserId, domain: Option<&JsDomain>) -> PathBuf {
            let suffix = domain.map(|d| format!("-{d}")).unwrap_or_default();
            let out = self.dir().join(format!("{user}{suffix}.creds"));
            issue(&self.dir().join("auth"), user, role(user, false), domain, &out).unwrap();
            out
        }

        pub(crate) async fn connect(&self, user: &UserId) -> (Client, jetstream::Context) {
            self.connect_as(user, role(user, false)).await
        }

        /// Credentials of `user` issued in `role`.
        pub(crate) fn creds_as(&self, user: &UserId, role: Role) -> PathBuf {
            let creds = self.dir().join(format!("{user}-{role:?}.creds"));
            issue(&self.dir().join("auth"), user, role, None, &creds).unwrap();
            creds
        }

        /// A connection with credentials of `user` issued in `role`.
        pub(crate) async fn connect_as(&self, user: &UserId, role: Role) -> (Client, jetstream::Context) {
            let creds = self.creds_as(user, role);
            let (client, mut js) = bus::connect(&self.url, Some(&creds), None, user, "test").await.unwrap();
            // Denied JetStream requests only time out.
            js.set_timeout(Duration::from_secs(1));
            (client, js)
        }

        /// A connection that reports the server errors it gets, such as permission violations.
        async fn watched(&self, user: &UserId) -> (Client, mpsc::UnboundedReceiver<String>) {
            self.watched_with(user, &self.creds(user)).await
        }

        /// A connection with `creds` that reports the server errors it gets.
        async fn watched_with(&self, user: &UserId, creds: &Path) -> (Client, mpsc::UnboundedReceiver<String>) {
            let (tx, rx) = mpsc::unbounded_channel();
            let client = async_nats::ConnectOptions::with_credentials_file(creds)
                .await
                .unwrap()
                .custom_inbox_prefix(inbox_prefix(user))
                .event_callback(move |event| {
                    let tx = tx.clone();
                    async move {
                        if let Event::ServerError(e) = event {
                            let _ = tx.send(e.to_string());
                        }
                    }
                })
                .connect(&self.url)
                .await
                .unwrap();
            (client, rx)
        }
    }

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    /// The server handles operations in order, so the next violation names the first denied one.
    async fn next_violation(client: &Client, errors: &mut mpsc::UnboundedReceiver<String>) -> String {
        client.flush().await.unwrap();
        timeout(WAIT, errors.recv()).await.expect("no violation").unwrap()
    }

    #[tokio::test]
    async fn credentials_confine_each_user_to_its_role() {
        let Some(server) = Server::start() else { return };
        let (human, dev_a, dev_b) = (UserId::human(), p("dev-a"), p("dev-b"));

        assert!(bus::connect(&server.url, None, None, &human, "anonymous").await.is_err(), "credentials are required");

        let (_, a_js) = server.connect(&dev_a).await;
        assert!(bus::ensure_stream(&a_js).await.is_err(), "only the human creates the stream");
        let (h_client, h_js) = server.connect(&human).await;
        bus::ensure_stream(&h_js).await.unwrap();

        let mut human_inbox = h_client.subscribe(subject::inbox_of(&human)).await.unwrap();
        h_client.flush().await.unwrap();
        let ask = Envelope::new(human.clone(), dev_a.clone(), "hi");
        bus::publish(&h_js, &ask).await.unwrap();

        // dev-a serves its inbox the way the daemon does, and replies.
        let stream = bus::ensure_stream(&a_js).await.unwrap();
        let mut inbox = bus::inbox_consumer(&stream, &dev_a, WAIT, 5, 2).await.unwrap();
        let msg = timeout(WAIT, inbox.next()).await.unwrap().unwrap().unwrap();
        assert_eq!(serde_json::from_slice::<Envelope>(&msg.payload).unwrap(), ask);
        let reply = ask.reply(dev_a.clone(), "hello", Role::Human).unwrap();
        bus::publish(&a_js, &reply).await.unwrap();
        msg.double_ack().await.unwrap();
        let got = timeout(WAIT, human_inbox.next()).await.unwrap().unwrap();
        assert_eq!(serde_json::from_slice::<Envelope>(&got.payload).unwrap(), reply);

        // dev-a cannot read dev-b's inbox, through a consumer or the stream.
        assert!(bus::inbox_consumer(&stream, &dev_b, WAIT, 5, 2).await.is_err());
        assert!(stream.get_raw_message(1).await.is_err());

        // dev-a keeps its sessions under its own keys, and cannot touch dev-b's.
        let thread: ThreadId = "t1".parse().unwrap();
        assert!(bus::ensure_sessions(&a_js).await.is_err(), "only the human creates the bucket");
        bus::ensure_sessions(&h_js).await.unwrap();
        let sessions = bus::ensure_sessions(&a_js).await.unwrap();
        sessions.put(session_key(&dev_a, &thread), "s1".into()).await.unwrap();
        assert_eq!(sessions.get(session_key(&dev_a, &thread)).await.unwrap().as_deref(), Some(&b"s1"[..]));
        assert!(sessions.put(session_key(&dev_b, &thread), "s1".into()).await.is_err());
        assert!(sessions.get(session_key(&dev_b, &thread)).await.is_err());

        // So it does with the markers of its attempts.
        assert!(bus::ensure_attempts(&a_js).await.is_err(), "only the human creates the bucket");
        bus::ensure_attempts(&h_js).await.unwrap();
        let attempts = bus::ensure_attempts(&a_js).await.unwrap();
        attempts.put(attempt_key(&dev_a, 1), "started".into()).await.unwrap();
        assert_eq!(attempts.get(attempt_key(&dev_a, 1)).await.unwrap().as_deref(), Some(&b"started"[..]));
        attempts.delete(attempt_key(&dev_a, 1)).await.unwrap();
        assert_eq!(attempts.get(attempt_key(&dev_a, 1)).await.unwrap(), None);
        assert!(attempts.put(attempt_key(&dev_b, 1), "started".into()).await.is_err());
        assert!(attempts.get(attempt_key(&dev_b, 1)).await.is_err());

        // Nor speak for dev-b, or listen to others.
        let (a_raw, mut errors) = server.watched(&dev_a).await;
        a_raw.publish(subject::status(&dev_a, &thread), "ok".into()).await.unwrap();
        a_raw.publish(subject::status(&dev_b, &thread), "spoof".into()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains("Publish to \"ants.status.dev-b.t1\""), "{v}");
        a_raw.publish(subject::approvals(&dev_b), "spoof".into()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains("Publish to \"ants.approvals.dev-b\""), "{v}");
        a_raw.publish(subject::outbox(&subject::dead(&dev_a)).unwrap(), "ok".into()).await.unwrap();
        for spoof in [subject::dead(&dev_b), subject::outbox(&subject::dead(&dev_b)).unwrap()] {
            a_raw.publish(spoof.clone(), "spoof".into()).await.unwrap();
            let v = next_violation(&a_raw, &mut errors).await;
            assert!(v.contains(&format!("Publish to \"{spoof}\"")), "{v}");
        }
        // It sends only under its own name: not as the human, nor as dev-b to the human, nor as the hub's stream
        // republishing what dev-b sent.
        a_raw.publish(subject::inbox(&dev_a, &dev_a), "ok".into()).await.unwrap();
        let republished = format!("{}.stored.{dev_b}", inbox_prefix(&human));
        for spoof in [subject::inbox(&dev_b, &human), subject::inbox(&human, &dev_b), republished] {
            a_raw.publish(spoof.clone(), "spoof".into()).await.unwrap();
            let v = next_violation(&a_raw, &mut errors).await;
            assert!(v.contains(&format!("Publish to \"{spoof}\"")), "{v}");
        }
        // It posts to thread notes only under its own name too.
        let thread: ThreadId = "t2".parse().unwrap();
        a_raw.publish(subject::notes(&thread, &dev_a), "ok".into()).await.unwrap();
        for spoof in [subject::notes(&thread, &dev_b), subject::notes(&thread, &human)] {
            a_raw.publish(spoof.clone(), "spoof".into()).await.unwrap();
            let v = next_violation(&a_raw, &mut errors).await;
            assert!(v.contains(&format!("Publish to \"{spoof}\"")), "{v}");
        }
        // dev-a hears the human's decisions on its approval requests, but cannot decide them itself.
        let request = ants_proto::Uuid::now_v7();
        let mut decisions = a_raw.subscribe(subject::approval_decision(&dev_a, &request)).await.unwrap();
        a_raw.publish(subject::approval_decision(&dev_a, &request), "allow".into()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"ants.approvals.dev-a.{request}\"")), "{v}");
        h_client.publish(subject::approval_decision(&dev_a, &request), "allow".into()).await.unwrap();
        let got = timeout(WAIT, decisions.next()).await.unwrap().unwrap();
        assert_eq!(&got.payload[..], b"allow");
        // It reads back the decision stored on its own request, and on no one else's.
        let stored = stream.direct_get_last_for_subject(subject::approval_decision(&dev_a, &request)).await.unwrap();
        assert_eq!(&stored.payload[..], b"allow");
        let of_b = format!("$JS.API.DIRECT.GET.{STREAM}.{}", subject::approval_decision(&dev_b, &request));
        a_raw.publish(of_b.clone(), "".into()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{of_b}\"")), "{v}");
        // Its own inbox it may watch live; a denial would be the next violation below.
        let _own = a_raw.subscribe(subject::inbox_of(&dev_a)).await.unwrap();
        let _own = a_raw.subscribe(subject::stored_inbox(&dev_a)).await.unwrap();
        for denied in
            [subject::inbox_of(&dev_b), subject::stored_inbox(&dev_b), "_INBOX.>".to_owned(), subject::ALL.to_owned()]
        {
            let _sub = a_raw.subscribe(denied.clone()).await.unwrap();
            let v = next_violation(&a_raw, &mut errors).await;
            assert!(v.contains(&format!("Subscription to \"{denied}\"")), "{v}");
        }
    }

    #[tokio::test]
    async fn every_human_has_the_same_rights_and_reads_only_its_own_inbox() {
        let Some(server) = Server::start() else { return };
        let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
        assert_eq!(
            (role(&alice, true), role(&alice, false), role(&UserId::human(), false)),
            (Role::Human, Role::Ant, Role::Human)
        );
        let (a_client, a_js) = server.connect_as(&alice, role(&alice, true)).await;
        let (b_client, b_js) = server.connect_as(&bob, role(&bob, true)).await;
        let (d_client, d_js) = server.connect(&dev_a).await;
        let (_, carol_js) = server.connect_as(&p("carol"), role(&p("carol"), false)).await;

        // Either human creates the stream and the buckets; a name issued without --human does not.
        assert!(bus::ensure_stream(&carol_js).await.is_err(), "carol is no human");
        assert!(crate::humans::ensure(&d_js).await.is_err(), "only a human creates the humans bucket");
        bus::ensure_stream(&a_js).await.unwrap();
        let b_stream = bus::init_stream(&b_js).await.unwrap();
        bus::ensure_roster(&b_js, ants_proto::ROSTER_TTL).await.unwrap();
        bus::ensure_sessions(&a_js).await.unwrap();
        bus::ensure_attempts(&b_js).await.unwrap();
        bus::ensure_channels(&a_js).await.unwrap();
        let a_humans = crate::humans::ensure(&a_js).await.unwrap();
        let b_humans = crate::humans::ensure(&b_js).await.unwrap();

        // Each human writes the humans bucket; ants read it, but never write it.
        for (kv, human) in [(&a_humans, &alice), (&b_humans, &bob)] {
            let entry = ants_proto::HumanEntry::new(human.clone());
            kv.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
        }
        let d_humans = crate::humans::ensure(&d_js).await.unwrap();
        let listed: ants_proto::HumanEntry =
            serde_json::from_slice(&d_humans.get("bob").await.unwrap().unwrap()).unwrap();
        assert_eq!((listed.check_key("bob"), listed.human), (Ok(()), bob.clone()));
        let keys: Vec<String> = d_humans.keys().await.unwrap().try_collect().await.unwrap();
        assert_eq!(keys, ["alice", "bob"]);
        let forged = serde_json::to_vec(&ants_proto::HumanEntry::new(dev_a.clone())).unwrap();
        assert!(d_humans.put("dev-a", forged.into()).await.is_err(), "an ant cannot list itself");
        assert!(d_humans.delete("alice").await.is_err());

        // Both watch all traffic and read dead letters.
        let mut a_all = a_client.subscribe(subject::ALL).await.unwrap();
        let mut b_all = b_client.subscribe(subject::ALL).await.unwrap();
        for c in [&a_client, &b_client] {
            c.flush().await.unwrap();
        }
        assert!(dead::list(&b_stream).await.unwrap().is_empty());

        // Each reads its replies through its own inbox consumer, never through the other's.
        let (to_alice, to_bob) =
            (Envelope::new(dev_a.clone(), alice.clone(), "a"), Envelope::new(dev_a.clone(), bob.clone(), "b"));
        bus::publish(&d_js, &to_alice).await.unwrap();
        bus::publish(&d_js, &to_bob).await.unwrap();
        for all in [&mut a_all, &mut b_all] {
            assert_eq!(timeout(WAIT, all.next()).await.unwrap().unwrap().subject.as_str(), to_alice.subject());
        }
        for (js, human, other, sent) in [(&a_js, &alice, &bob, &to_alice), (&b_js, &bob, &alice, &to_bob)] {
            let stream = bus::ensure_stream(js).await.unwrap();
            assert!(bus::human_inbox_consumer(&stream, other, WAIT).await.is_err(), "{human} reads {other}'s inbox");
            let consumer = bus::human_inbox_consumer(&stream, human, WAIT).await.unwrap();
            let msg = timeout(WAIT, consumer.messages().await.unwrap().next()).await.unwrap().unwrap().unwrap();
            assert_eq!(&serde_json::from_slice::<Envelope>(&msg.payload).unwrap(), sent);
            msg.double_ack().await.unwrap();
        }

        // Either human decides dev-a's approval requests.
        let request = ants_proto::Uuid::now_v7();
        let mut decisions = d_client.subscribe(subject::approval_decision(&dev_a, &request)).await.unwrap();
        d_client.flush().await.unwrap();
        a_client.publish(subject::approval_decision(&dev_a, &request), "allow".into()).await.unwrap();
        a_client.flush().await.unwrap();
        b_client.publish(subject::approval_decision(&dev_a, &request), "reject".into()).await.unwrap();
        for verdict in [&b"allow"[..], b"reject"] {
            assert_eq!(&timeout(WAIT, decisions.next()).await.unwrap().unwrap().payload[..], verdict);
        }
    }

    #[tokio::test]
    async fn thread_notes_are_read_back_through_each_reader_own_consumer() {
        let Some(server) = Server::start() else { return };
        let (human, dev_a, dev_b) = (UserId::human(), p("dev-a"), p("dev-b"));
        let (_h, h_js) = server.connect(&human).await;
        bus::ensure_stream(&h_js).await.unwrap();
        let (_a, a_js) = server.connect(&dev_a).await;
        let (_b, b_js) = server.connect(&dev_b).await;
        let (thread, other): (ThreadId, ThreadId) = ("t1".parse().unwrap(), "t2".parse().unwrap());

        let first = NotesPost::spend(thread.clone(), dev_a.clone(), "draft", 8, None).unwrap();
        assert!(notes::publish(&a_js, &first, Some(0)).await.unwrap());
        // dev-b spends the same turn without having seen dev-a's post: refused, the notes moved on.
        let stale = NotesPost::spend(thread.clone(), dev_b.clone(), "me first", 8, None).unwrap();
        assert!(!notes::publish(&b_js, &stale, Some(0)).await.unwrap());
        // dev-a may only post on its own subject, so a post naming dev-b as its author is dropped by readers.
        let forged = NotesPost::new(thread.clone(), dev_b.clone(), "dev-b agrees");
        bus::publish_json(&a_js, subject::notes(&thread, &dev_a), &forged, None).await.unwrap();
        notes::publish(&a_js, &NotesPost::new(other, dev_a.clone(), "elsewhere"), None).await.unwrap();

        let seen = notes::read(&b_js, &dev_b, &thread).await.unwrap();
        assert_eq!(seen.posts, std::slice::from_ref(&first));
        let answer = NotesPost::spend(thread.clone(), dev_b.clone(), "agreed", 8, seen.last()).unwrap();
        assert!(notes::publish(&b_js, &answer, Some(seen.last_seq)).await.unwrap());
        assert_eq!(notes::read(&h_js, &human, &thread).await.unwrap().posts, [first, answer]);

        // dev-a's notes consumer filters thread notes only, and dev-b's is not dev-a's to use.
        let consumer = |name: String, filter: String| jetstream::consumer::pull::Config {
            name: Some(name),
            filter_subject: filter,
            ..Default::default()
        };
        let into_inbox = consumer(notes_consumer(&dev_a), subject::inbox_of(&dev_b));
        assert!(a_js.create_consumer_on_stream(into_inbox, STREAM).await.is_err());
        let not_its_own = consumer(notes_consumer(&dev_b), subject::notes_of(&thread));
        assert!(a_js.create_consumer_on_stream(not_its_own, STREAM).await.is_err());
        assert!(notes::read(&a_js, &dev_a, &thread).await.is_ok(), "its own works");
    }

    #[tokio::test]
    async fn ants_write_their_own_roster_entry_and_everyone_reads_the_roster() {
        let Some(server) = Server::start() else { return };
        let (human, dev_a) = (UserId::human(), p("dev-a"));
        let ttl = ants_proto::ROSTER_TTL;

        let (_a, a_js) = server.connect(&dev_a).await;
        assert!(bus::ensure_roster(&a_js, ttl).await.is_err(), "only the human creates the bucket");
        let (_h, h_js) = server.connect(&human).await;
        let h_kv = bus::ensure_roster(&h_js, ttl).await.unwrap();

        let a_kv = bus::ensure_roster(&a_js, ttl).await.unwrap();
        let now = chrono::Utc::now();
        let entry = RosterEntry {
            ant: dev_a.clone(),
            machine: "m1".to_owned(),
            instance: None,
            harness: "copilot".to_owned(),
            capabilities: vec!["rust".to_owned()],
            whoami: None,
            workdir: None,
            started_at: now,
            heartbeat_at: now,
            activity: None,
            handling: None,
            harnesses: None,
            escalate_to: Vec::new(),
        };
        let registration =
            Registration::start(a_kv.clone(), entry.clone(), ants_proto::ROSTER_HEARTBEAT).await.unwrap();
        assert!(a_kv.put("dev-b", "spoof".into()).await.is_err(), "dev-a cannot list dev-b");
        // Ants read the roster, to route requests by capability.
        let (_b, b_js) = server.connect(&p("dev-b")).await;
        let b_kv = bus::ensure_roster(&b_js, ttl).await.unwrap();
        // dev-b may write its own key only, and an entry there naming dev-a is skipped.
        let forged = RosterEntry { capabilities: vec!["deploy".to_owned()], ..entry.clone() };
        b_kv.put("dev-b", serde_json::to_vec(&forged).unwrap().into()).await.unwrap();
        for kv in [&b_kv, &h_kv] {
            assert_eq!(ants_daemon::roster::list_entries(kv).await.unwrap(), std::slice::from_ref(&entry));
        }
        b_kv.delete("dev-b").await.unwrap();

        let listed: RosterEntry = serde_json::from_slice(&h_kv.get("dev-a").await.unwrap().unwrap()).unwrap();
        assert_eq!(listed, entry);
        let keys: Vec<String> = h_kv.keys().await.unwrap().try_collect().await.unwrap();
        assert_eq!(keys, ["dev-a"]);
        let mut watch = h_kv.watch_with_history(">").await.unwrap();
        let seen = timeout(WAIT, watch.next()).await.unwrap().unwrap().unwrap();
        assert_eq!(seen.key, "dev-a");
        assert!(h_kv.put("dev-a", "spoof".into()).await.is_err(), "the human does not write the roster");

        registration.deregister().await;
        assert!(h_kv.get("dev-a").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn ants_keep_their_own_escalation_chain_and_read_everyone_s() {
        let Some(server) = Server::start() else { return };
        let (human, dev_a, lead) = (UserId::human(), p("dev-a"), p("lead"));
        let (_a, a_js) = server.connect(&dev_a).await;
        assert!(bus::ensure_chains(&a_js).await.is_err(), "only the human creates the bucket");
        let (_h, h_js) = server.connect(&human).await;
        let h_kv = bus::ensure_chains(&h_js).await.unwrap();

        let (_l, l_js) = server.connect(&lead).await;
        ants_daemon::chains::publish(&l_js, &lead, std::slice::from_ref(&dev_a)).await.unwrap();
        let a_kv = bus::ensure_chains(&a_js).await.unwrap();
        assert_eq!(ants_daemon::chains::read(&a_kv, &lead).await.unwrap(), Some(vec![dev_a.clone()]));
        assert_eq!(ants_daemon::chains::read(&h_kv, &lead).await.unwrap(), Some(vec![dev_a.clone()]));

        // dev-a writes its own key only, and an entry there naming lead is skipped.
        assert!(a_kv.put("lead", "spoof".into()).await.is_err(), "dev-a cannot rewrite lead's chain");
        let forged = ants_proto::ChainEntry::new(lead.clone(), vec![human.clone()]);
        a_kv.put("dev-a", serde_json::to_vec(&forged).unwrap().into()).await.unwrap();
        assert_eq!(ants_daemon::chains::read(&h_kv, &dev_a).await.unwrap(), None);
        assert!(h_kv.put("lead", "spoof".into()).await.is_err(), "the human does not write chains");
    }

    #[tokio::test]
    async fn ants_record_the_links_they_pass_on_and_read_everyone_s() {
        let Some(server) = Server::start() else { return };
        let (dev_a, dev_b, lead) = (p("dev-a"), p("dev-b"), p("lead"));
        let (_a, a_js) = server.connect(&dev_a).await;
        assert!(bus::ensure_links(&a_js).await.is_err(), "only the human creates the bucket");
        let (_h, h_js) = server.connect(&UserId::human()).await;
        let h_kv = bus::ensure_links(&h_js).await.unwrap();

        let origin = ThreadId::generate();
        let spin = |from: &UserId, to: &UserId| {
            Envelope::on_thread(origin.clone(), from.clone(), to.clone(), "")
                .with_creator(Some(lead.clone()))
                .spun_off(ThreadId::generate())
        };
        ants_daemon::links::pass_on(&a_js, &spin(&dev_a, &dev_b)).await.unwrap();
        let a_kv = a_js.get_key_value(ants_proto::LINKS_BUCKET).await.unwrap();
        for forged in [link_key(&lead, &origin, &dev_b), link_key(&dev_b, &origin, &dev_a)] {
            assert!(a_kv.put(forged, "spoof".into()).await.is_err(), "dev-a records its own links only");
        }
        assert!(h_kv.put(link_key(&lead, &origin, &dev_a), "spoof".into()).await.is_err(), "the human records none");

        let (_b, b_js) = server.connect(&dev_b).await;
        for js in [&a_js, &b_js, &h_js] {
            assert_eq!(ants_daemon::links::passed_on(js, &origin).await.unwrap(), [(dev_a.clone(), dev_b.clone())]);
        }
        assert!(ants_daemon::links::passed_on(&b_js, &ThreadId::generate()).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_webhook_posts_as_itself_to_its_destinations_only_and_reads_no_message() {
        let Some(server) = Server::start() else { return };
        let (hook, dev_a, ops) = (p("github"), p("dev-a"), "ops".parse::<ChannelId>().unwrap());
        let creds = server.dir().join("github.creds");
        let destinations = [Destination::Ant(dev_a.clone()), Destination::Channel(ops.clone())];
        issue_with(&server.dir().join("auth"), &hook, Permissions::for_webhook(&hook, &destinations), &creds).unwrap();
        let (_, h_js) = server.connect(&UserId::human()).await;
        bus::init_stream(&h_js).await.unwrap();
        crate::webhook::ensure_bucket(&h_js).await.unwrap();

        let (raw, mut errors) = server.watched_with(&hook, &creds).await;
        for allowed in
            [subject::inbox(&dev_a, &hook), subject::chan(&ops, &hook), "$KV.ants_webhooks.github.x".to_owned()]
        {
            raw.publish(allowed, "ok".into()).await.unwrap();
        }
        for denied in [
            subject::inbox(&p("dev-b"), &hook),
            subject::inbox(&dev_a, &p("dev-b")),
            subject::chan(&"dev".parse().unwrap(), &hook),
            "ants.out.inbox.dev-a.github".to_owned(),
            "$KV.ants_channels.ops".to_owned(),
            "$KV.ants_humans.github".to_owned(),
            "$KV.ants_webhooks.gitlab.x".to_owned(),
            subject::notes(&"t1".parse().unwrap(), &hook),
        ] {
            raw.publish(denied.clone(), "spoof".into()).await.unwrap();
            let v = next_violation(&raw, &mut errors).await;
            assert!(v.contains(&format!("Publish to \"{denied}\"")), "{v}");
        }
        for denied in [subject::ALL, "_INBOX.dev-a.>"] {
            let _sub = raw.subscribe(denied).await.unwrap();
            let v = next_violation(&raw, &mut errors).await;
            assert!(v.contains(&format!("Subscription to \"{denied}\"")), "{v}");
        }

        // Its marker tells ants it is a webhook; they read no event it delivered, and put no marker of their own.
        raw.publish(ants_proto::webhook_marker_subject(&hook), "github".into()).await.unwrap();
        raw.flush().await.unwrap();
        let (_, a_js) = server.connect(&dev_a).await;
        let bucket = a_js.get_stream_no_info(format!("KV_{}", ants_proto::WEBHOOKS_BUCKET)).await.unwrap();
        let marker = bucket.direct_get_last_for_subject(ants_proto::webhook_marker_subject(&hook)).await.unwrap();
        assert_eq!(&marker.payload[..], b"github");
        let (a_raw, mut errors) = server.watched(&dev_a).await;
        for denied in [
            "$JS.API.DIRECT.GET.KV_ants_webhooks.$KV.ants_webhooks.github.x".to_owned(),
            ants_proto::webhook_marker_subject(&dev_a),
        ] {
            a_raw.publish(denied.clone(), "".into()).await.unwrap();
            let v = next_violation(&a_raw, &mut errors).await;
            assert!(v.contains(&format!("Publish to \"{denied}\"")), "{v}");
        }
    }

    #[tokio::test]
    async fn ants_post_to_channels_as_themselves_and_read_but_never_write_the_channels_bucket() {
        let Some(server) = Server::start() else { return };
        let (human, dev_a, dev_b) = (UserId::human(), p("dev-a"), p("dev-b"));
        let ops: ChannelId = "ops".parse().unwrap();
        let (_h, h_js) = server.connect(&human).await;
        let stream = bus::init_stream(&h_js).await.unwrap();
        assert!(stream.cached_info().config.subjects.iter().any(|s| s == subject::ALL_CHANNELS));
        let (_a, a_js) = server.connect(&dev_a).await;

        // Only the human creates and writes the bucket; ants read and watch it.
        assert!(bus::ensure_channels(&a_js).await.is_err(), "only the human creates the bucket");
        let h_kv = bus::ensure_channels(&h_js).await.unwrap();
        let mut entry = ChannelEntry::new(ops.clone());
        entry.add_member(dev_a.clone());
        h_kv.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
        let a_kv = bus::ensure_channels(&a_js).await.unwrap();
        let read: ChannelEntry = serde_json::from_slice(&a_kv.get("ops").await.unwrap().unwrap()).unwrap();
        assert_eq!(read.check_key("ops"), Ok(()));
        assert_eq!(read, entry);
        let mut watch = a_kv.watch_with_history(">").await.unwrap();
        assert_eq!(timeout(WAIT, watch.next()).await.unwrap().unwrap().unwrap().key, "ops");
        entry.add_member(dev_b.clone());
        assert!(a_kv.put("ops", serde_json::to_vec(&entry).unwrap().into()).await.is_err(), "ants never write");
        assert!(a_kv.delete("ops").await.is_err());
        h_kv.delete("ops").await.unwrap();

        // Posts go on the poster's own subject only, in the hub's stream or its outbox form.
        let post = ChannelPost::new(ops.clone(), dev_a.clone(), "@dev-b ready?");
        bus::publish_json(&a_js, post.subject(), &post, None).await.unwrap();
        let (a_raw, mut errors) = server.watched(&dev_a).await;
        let _live = a_raw.subscribe(subject::ALL_CHANNELS).await.unwrap();
        a_raw.publish(subject::outbox(&subject::chan(&ops, &dev_a)).unwrap(), "ok".into()).await.unwrap();
        for spoof in [subject::chan(&ops, &dev_b), subject::chan(&ops, &human), "ants.out.chan.ops.dev-b".to_owned()] {
            a_raw.publish(spoof.clone(), "spoof".into()).await.unwrap();
            let v = next_violation(&a_raw, &mut errors).await;
            assert!(v.contains(&format!("Publish to \"{spoof}\"")), "{v}");
        }

        // dev-a's channel consumer and reader filter channel subjects only, as one filter: several would hide it.
        let consumer = |name: String, filter: &str| jetstream::consumer::pull::Config {
            durable_name: Some(name),
            filter_subject: filter.to_owned(),
            ..Default::default()
        };
        let a_stream = bus::ensure_stream(&a_js).await.unwrap();
        let chan: jetstream::consumer::PullConsumer =
            a_stream.create_consumer(consumer(chan_consumer(&dev_a), subject::ALL_CHANNELS)).await.unwrap();
        let msg = chan.messages().await.unwrap().next().await.unwrap().unwrap();
        let got: ChannelPost = serde_json::from_slice(&msg.payload).unwrap();
        assert_eq!((got.check_subject(&msg.subject), &got), (Ok(()), &post));
        msg.double_ack().await.unwrap();
        let room = consumer(chan_reader(&dev_a), &subject::chan_room(&ops));
        a_js.create_consumer_on_stream(room, STREAM).await.unwrap();
        a_js.delete_consumer_from_stream(chan_reader(&dev_a), STREAM).await.unwrap();
        for name in [chan_consumer(&dev_a), chan_reader(&dev_a)] {
            assert!(a_stream.create_consumer(consumer(name.clone(), &subject::inbox_of(&dev_b))).await.is_err());
            let several = jetstream::consumer::pull::Config {
                durable_name: Some(name),
                filter_subjects: vec![subject::chan_room(&ops), subject::inbox_of(&dev_b)],
                ..Default::default()
            };
            assert!(a_stream.create_consumer(several).await.is_err());
        }
        assert!(a_stream.create_consumer(consumer(chan_consumer(&dev_b), subject::ALL_CHANNELS)).await.is_err());

        // What dev-a's daemon reads: the channels, and the thread of a post back for its prompt.
        h_kv.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
        let channels = Channels::watch(a_kv).await.unwrap();
        assert_eq!(channels.get(&ops), Some(&entry));
        bus::ensure_sessions(&h_js).await.unwrap();
        let note = post.reply(human.clone(), "noted", &[], &ants_proto::Humans::default()).unwrap();
        bus::publish_json(&h_js, note.subject(), &note, None).await.unwrap();
        let ask = post.reply(human.clone(), "@dev-a go", &[], &ants_proto::Humans::default()).unwrap();
        let seq = bus::publish_json(&h_js, ask.subject(), &ask, None).await.unwrap();
        let threads = Threads::new(a_js.clone(), dev_a.clone(), bus::ensure_sessions(&a_js).await.unwrap());
        assert_eq!(
            threads.context(&ask, seq, true, &ants_proto::Humans::default()).await.unwrap().posts,
            [note],
            "without dev-a's own post"
        );
        threads.seen(&ask.thread_id, seq).await.unwrap();
    }

    #[tokio::test]
    async fn the_channel_manager_applies_the_owner_s_changes_only_and_announces_them() {
        use ants_daemon::chanadm;
        use ants_proto::{ChannelAdminOutcome, ChannelAdminRequest, ChannelOp, Refusal};

        let Some(server) = Server::start() else { return };
        let (human, dev_a, dev_b, manager) = (UserId::human(), p("dev-a"), p("dev-b"), UserId::channel_manager());
        let ops: ChannelId = "ops".parse().unwrap();
        let (h_client, h_js) = server.connect(&human).await;
        bus::init_stream(&h_js).await.unwrap();
        let roster = bus::ensure_roster(&h_js, ants_proto::ROSTER_TTL).await.unwrap();
        let channels = bus::ensure_channels(&h_js).await.unwrap();
        let mut entry = ChannelEntry::new(ops.clone());
        entry.owner = Some(dev_a.clone());
        h_kv_put(&channels, &entry).await;
        let (_b, b_js) = server.connect(&dev_b).await;
        let now = chrono::Utc::now();
        let listed = RosterEntry {
            ant: dev_b.clone(),
            machine: "m1".to_owned(),
            instance: None,
            harness: "copilot".to_owned(),
            capabilities: Vec::new(),
            whoami: None,
            workdir: None,
            started_at: now,
            heartbeat_at: now,
            activity: None,
            handling: None,
            harnesses: None,
            escalate_to: Vec::new(),
        };
        let b_roster = bus::ensure_roster(&b_js, ants_proto::ROSTER_TTL).await.unwrap();
        b_roster.put("dev-b", serde_json::to_vec(&listed).unwrap().into()).await.unwrap();
        assert!(roster.get("dev-b").await.unwrap().is_some());

        assert!(issue(&server.dir().join("auth"), &manager, Role::Ant, None, &server.dir().join("x")).is_err());
        let creds = server.dir().join("channel-manager.creds");
        issue_channel_manager(&server.dir().join("auth"), None, &creds).unwrap();
        let (m_client, m_js) = bus::connect(&server.url, Some(&creds), None, &manager, "test").await.unwrap();
        let humans = ants_daemon::humans::Registry::new(human.clone());
        let _watching = humans.watch(m_js.clone(), ants_daemon::humans::RETRY);
        let roster_of = ants_daemon::mcp::jetstream_roster(m_js.clone());
        let serving = tokio::spawn(chanadm::serve(m_client, m_js, roster_of, humans));

        let mut room = h_client.subscribe(subject::chan_room(&ops)).await.unwrap();
        let (a_client, a_js) = server.connect(&dev_a).await;
        let a_admin = chanadm::nats_admin(a_client.clone(), a_js);
        let ask = |admin: &std::sync::Arc<dyn chanadm::Admin>, ant: &UserId, ops_: Vec<ChannelOp>| {
            let request = ChannelAdminRequest::new(ops.clone(), ant.clone(), ThreadId::generate(), ops_);
            let admin = admin.clone();
            async move { admin.request(request).await.unwrap().expect("an answer").outcome }
        };
        let refused = |outcome: ChannelAdminOutcome| match outcome {
            ChannelAdminOutcome::Refused { refusal } => refusal,
            applied => panic!("applied: {applied:?}"),
        };

        // The owner adds an ant in the roster; the change is stored and announced in the channel, mentioning nobody.
        let add_b = vec![ChannelOp::AddMember { ant: dev_b.clone() }];
        let first = ChannelAdminRequest::new(ops.clone(), dev_a.clone(), ThreadId::generate(), add_b.clone());
        let answer = a_admin.request(first.clone()).await.unwrap().expect("an answer");
        let ChannelAdminOutcome::Applied { entry: applied, changed, .. } = answer.outcome.clone() else {
            panic!("refused")
        };
        assert_eq!(
            (applied.members.as_slice(), changed.as_slice()),
            (std::slice::from_ref(&dev_b), ["added dev-b".to_owned()].as_slice())
        );
        let stored: ChannelEntry = serde_json::from_slice(&channels.get("ops").await.unwrap().unwrap()).unwrap();
        assert_eq!(stored, applied);
        let msg = timeout(WAIT, room.next()).await.unwrap().unwrap();
        let post: ChannelPost = serde_json::from_slice(&msg.payload).unwrap();
        assert_eq!((post.check_subject(&msg.subject), &post.from), (Ok(()), &manager));
        assert!(
            post.text.contains("dev-a") && post.text.contains("added dev-b") && post.mentions.is_empty(),
            "{post:?}"
        );

        // Ants outside the roster, other ants, and the former owner are refused.
        let add_c = vec![ChannelOp::AddMember { ant: p("dev-c") }];
        assert_eq!(refused(ask(&a_admin, &dev_a, add_c).await), Refusal::NotInRoster(p("dev-c")));
        // A human needs no roster entry to be a member, but is never the messenger.
        let add_human = vec![ChannelOp::AddMember { ant: human.clone() }];
        let ChannelAdminOutcome::Applied { entry: with_human, changed, .. } = ask(&a_admin, &dev_a, add_human).await
        else {
            panic!("a human member refused")
        };
        assert_eq!((with_human.members, changed), (vec![dev_b.clone(), human.clone()], vec!["added human".to_owned()]));
        let human_messenger = vec![ChannelOp::SetMessenger { ant: human.clone() }];
        assert_eq!(refused(ask(&a_admin, &dev_a, human_messenger).await), Refusal::Human(human.clone()));
        let remove_human = vec![ChannelOp::RemoveMember { ant: human.clone() }];
        assert!(matches!(ask(&a_admin, &dev_a, remove_human).await, ChannelAdminOutcome::Applied { .. }));
        let (b_client, b_js) = server.connect(&dev_b).await;
        let b_admin = chanadm::nats_admin(b_client, b_js);
        let remove_b = vec![ChannelOp::RemoveMember { ant: dev_b.clone() }];
        assert!(matches!(refused(ask(&b_admin, &dev_b, remove_b.clone()).await), Refusal::NotOwner { .. }));
        entry = stored;
        entry.owner = Some(dev_b.clone());
        h_kv_put(&channels, &entry).await;
        assert!(matches!(refused(ask(&a_admin, &dev_a, remove_b.clone()).await), Refusal::NotOwner { .. }));
        assert!(matches!(ask(&b_admin, &dev_b, remove_b).await, ChannelAdminOutcome::Applied { .. }));
        // A request asked again gets its first answer, not a second change.
        assert_eq!(a_admin.request(first).await.unwrap(), Some(answer));

        // A request naming another ant's subject is denied, and one asking for the answer elsewhere goes unanswered.
        let (a_raw, mut errors) = server.watched(&dev_a).await;
        let foreign = ChannelAdminRequest::new(ops.clone(), dev_b.clone(), ThreadId::generate(), add_b.clone());
        a_raw.publish(foreign.subject(), serde_json::to_vec(&foreign).unwrap().into()).await.unwrap();
        assert!(next_violation(&a_raw, &mut errors).await.contains("ants.chanadm.ops.dev-b"));
        let mut b_inbox = h_client.subscribe(format!("{}.>", inbox_prefix(&dev_b))).await.unwrap();
        let forged = ChannelAdminRequest::new(ops.clone(), dev_a.clone(), ThreadId::generate(), add_b);
        let payload = serde_json::to_vec(&forged).unwrap();
        a_raw
            .publish_with_reply(forged.subject(), format!("{}.x", inbox_prefix(&dev_b)), payload.into())
            .await
            .unwrap();
        a_raw.flush().await.unwrap();
        assert!(timeout(Duration::from_millis(500), b_inbox.next()).await.is_err(), "no answer to dev-b's inbox");

        // The channel manager writes the channels bucket only, and posts as itself only.
        let (m_raw, mut errors) = server.watched_with(&manager, &creds).await;
        for denied in [subject::chan(&ops, &dev_a), "$KV.ants_roster.dev-a".to_owned(), subject::inbox_of(&dev_a)] {
            m_raw.publish(denied.clone(), "x".into()).await.unwrap();
            let v = next_violation(&m_raw, &mut errors).await;
            assert!(v.contains(&format!("Publish to \"{denied}\"")), "{v}");
        }
        let _sub = m_raw.subscribe(subject::ALL_CHANNELS).await.unwrap();
        assert!(next_violation(&m_raw, &mut errors).await.contains(subject::ALL_CHANNELS));
        serving.abort();
    }

    async fn h_kv_put(kv: &jetstream::kv::Store, entry: &ChannelEntry) {
        kv.put(entry.key(), serde_json::to_vec(entry).unwrap().into()).await.unwrap();
    }

    #[tokio::test]
    async fn failed_messages_become_dead_letters_the_human_replays() {
        let Some(server) = Server::start() else { return };
        let (human, dev_a) = (UserId::human(), p("dev-a"));
        let (h_client, h_js) = server.connect(&human).await;
        let (_, a_js) = server.connect(&dev_a).await;

        // A stream from before dead letters, thread notes and direct gets gains them on `ants init`, and keeps the
        // subject of its older thread notes, whose posts stay stored.
        let mut old: Vec<String> = STREAM_SUBJECTS
            .iter()
            .filter(|s| ![subject::DEAD_ALL, subject::ALL_NOTES].contains(*s))
            .map(|s| (*s).to_owned())
            .collect();
        old.push("ants.plan.>".to_owned());
        h_js.create_stream(jetstream::stream::Config { name: STREAM.to_owned(), subjects: old, ..Default::default() })
            .await
            .unwrap();
        let stream = bus::init_stream(&h_js).await.unwrap();
        assert!(bus::missing_subjects(&stream.cached_info().config.subjects).is_empty());
        assert!(stream.cached_info().config.subjects.iter().any(|s| s == "ants.plan.>"), "older subjects stay");
        assert!(stream.cached_info().config.allow_direct, "and direct gets");
        assert!(stream.cached_info().config.republish.is_some(), "and republishes inbox messages");

        let mut human_inbox = h_client.subscribe(subject::inbox_of(&human)).await.unwrap();
        h_client.flush().await.unwrap();
        let ask = Envelope::new(human.clone(), dev_a.clone(), "build it");
        bus::publish(&h_js, &ask).await.unwrap();

        // dev-a fails every attempt, as the daemon does when the harness errors.
        let a_stream = bus::ensure_stream(&a_js).await.unwrap();
        let mut inbox = bus::inbox_consumer(&a_stream, &dev_a, WAIT, 3, 1).await.unwrap();
        loop {
            let msg = timeout(WAIT, inbox.next()).await.unwrap().unwrap().unwrap();
            assert_eq!(serde_json::from_slice::<Envelope>(&msg.payload).unwrap(), ask);
            match Attempt::of(msg.info().unwrap().delivered, 2) {
                Attempt::Early => msg.ack_with(AckKind::Nak(None)).await.unwrap(),
                _ => {
                    dead::bury(&a_js, &dev_a, &msg, Some(&ask), "harness crashed").await.unwrap();
                    break;
                }
            }
        }
        let notice: Envelope =
            serde_json::from_slice(&timeout(WAIT, human_inbox.next()).await.unwrap().unwrap().payload).unwrap();
        assert_eq!((notice.thread_id.clone(), notice.in_reply_to), (ask.thread_id.clone(), Some(ask.id)));

        // A payload that is no envelope is kept for inspection too.
        h_client.publish(subject::inbox(&dev_a, &human), "{oops".into()).await.unwrap();
        let msg = timeout(WAIT, inbox.next()).await.unwrap().unwrap().unwrap();
        dead::bury(&a_js, &dev_a, &msg, None, "not an envelope").await.unwrap();

        let letters = dead::list(&stream).await.unwrap();
        assert_eq!(letters.len(), 2, "{letters:?}");
        let (seq, letter) = &letters[0];
        assert_eq!(
            (letter.ant.clone(), letter.deliveries, letter.error.as_str()),
            (dev_a.clone(), 2, "harness crashed")
        );
        assert_eq!(letter.envelope.as_ref(), Some(&ask));
        assert!(notice.text.contains(&format!("ants dead replay {seq}")), "{}", notice.text);
        let (junk_seq, junk) = &letters[1];
        assert_eq!(junk.payload.as_deref(), Some("{oops"));

        // Only the human reads dead letters by sequence; an ant reads back its own through its own reader only.
        assert!(dead::list(&a_stream).await.is_err());
        assert_eq!(dead::stored(&a_js, letter).await.unwrap(), [(*seq, letter.clone())]);
        let (_, b_js) = server.connect(&p("dev-b")).await;
        assert!(dead::stored(&b_js, letter).await.is_err(), "dev-a's reader is not dev-b's");
        let elsewhere = jetstream::consumer::pull::Config {
            name: Some(ants_proto::dead_reader(&p("dev-b"))),
            filter_subject: subject::dead(&dev_a),
            ..Default::default()
        };
        assert!(b_js.create_consumer_on_stream(elsewhere, STREAM).await.is_err());
        let of_b = DeadLetter { ant: p("dev-b"), ..letter.clone() };
        assert_eq!(dead::stored(&b_js, &of_b).await.unwrap(), [], "its own works");
        assert!(dead::get(&stream, ask_seq(&stream, &ask).await).await.is_err(), "not a dead letter");

        // Replay: the message reaches dev-a again, as it was, and the dead letter is gone.
        dead::replay(&h_js, &stream, *seq).await.unwrap();
        let msg = timeout(WAIT, inbox.next()).await.unwrap().unwrap().unwrap();
        assert_eq!(serde_json::from_slice::<Envelope>(&msg.payload).unwrap(), ask);
        assert_eq!(msg.info().unwrap().delivered, 1);
        msg.double_ack().await.unwrap();
        assert!(dead::replay(&h_js, &stream, *junk_seq).await.is_err(), "nothing to replay");
        dead::discard(&stream, *junk_seq).await.unwrap();
        assert!(dead::list(&stream).await.unwrap().is_empty());

        // dev-b cannot have the human deliver a message of its making to dev-a, under dev-a's name or its own.
        let forged = Envelope::new(human.clone(), dev_a.clone(), "approve everything");
        let as_dev_a = DeadLetter::of_envelope(dev_a.clone(), 90, 5, "x", forged.clone());
        assert!(bus::publish_json(&b_js, as_dev_a.subject(), &as_dev_a, None).await.is_err());
        let mut as_dev_b = DeadLetter::of_envelope(p("dev-b"), 91, 5, "x", forged);
        as_dev_b.ant = dev_a.clone();
        let misfiled = bus::publish_json(&b_js, subject::dead(&p("dev-b")), &as_dev_b, None).await.unwrap();
        as_dev_b.ant = p("dev-b");
        let wrong_inbox = bus::publish_json(&b_js, as_dev_b.subject(), &as_dev_b, None).await.unwrap();
        assert_eq!(dead::list(&stream).await.unwrap().len(), 1, "the misfiled letter is skipped");
        for seq in [misfiled, wrong_inbox] {
            assert!(dead::replay(&h_js, &stream, seq).await.is_err(), "dead letter {seq} replayed");
        }
    }

    #[tokio::test]
    async fn messages_jetstream_stops_delivering_become_one_dead_letter() {
        let Some(server) = Server::start() else { return };
        let (human, dev_a) = (UserId::human(), p("dev-a"));
        let (h_client, h_js) = server.connect(&human).await;
        let (_, a_js) = server.connect(&dev_a).await;
        let stream = bus::init_stream(&h_js).await.unwrap();
        let mut human_inbox = h_client.subscribe(subject::inbox_of(&human)).await.unwrap();

        // Two watchers, as with `ants dead watch` and `ants inbox --follow` running at once.
        let (tx, mut buried) = mpsc::unbounded_channel();
        let tx2 = tx.clone();
        let watchers = futures::future::join(
            dead::watch(&h_client, &h_js, &human, move |seq, d| tx.send((seq, d.clone())).unwrap()),
            dead::watch(&h_client, &h_js, &human, move |seq, d| tx2.send((seq, d.clone())).unwrap()),
        );

        let test = async {
            // Ants may not hear the advisories: those of other consumers name messages they may not read.
            let (a_raw, mut errors) = server.watched(&dev_a).await;
            let _sub = a_raw.subscribe(subject::max_deliveries_advisories()).await.unwrap();
            let v = next_violation(&a_raw, &mut errors).await;
            assert!(v.contains("Subscription to \"$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.ANTS.*\""), "{v}");

            let ask = Envelope::new(human.clone(), dev_a.clone(), "build it");
            bus::publish(&h_js, &ask).await.unwrap();
            // dev-a takes the message on every delivery and never settles it, as when dead-lettering fails too.
            let a_stream = bus::ensure_stream(&a_js).await.unwrap();
            let mut inbox = bus::inbox_consumer(&a_stream, &dev_a, Duration::from_millis(500), 2, 1).await.unwrap();
            let pulling = tokio::spawn(async move { while let Some(Ok(_unacked)) = inbox.next().await {} });

            let (dead_seq, letter) = timeout(Duration::from_secs(10), buried.recv()).await.unwrap().unwrap();
            let seq = ask_seq(&stream, &ask).await;
            assert_eq!((letter.ant.clone(), letter.stream_seq, letter.deliveries), (dev_a.clone(), seq, 2));
            assert_eq!(letter.envelope.as_ref(), Some(&ask));
            assert!(letter.error.contains("max-deliveries advisory"), "{}", letter.error);
            let notice: Envelope =
                serde_json::from_slice(&timeout(WAIT, human_inbox.next()).await.unwrap().unwrap().payload).unwrap();
            assert_eq!((notice.from.clone(), notice.in_reply_to), (dev_a.clone(), Some(ask.id)));
            assert!(notice.text.contains(&format!("ants dead replay {dead_seq}")), "{}", notice.text);
            assert!(notice.text.contains("told to a human directly, past the escalation chain"), "{}", notice.text);
            assert_eq!(notice.escalation, None, "the human's side skips the ant's chain");

            // A later advisory of the same message, past the duplicate window, finds the dead letter.
            let advisory = dead::MaxDeliveries {
                stream: STREAM.to_owned(),
                consumer: ants_proto::inbox_consumer(&dev_a),
                stream_seq: seq,
                deliveries: 2,
            };
            let (humans, webhooks) = (Registry::default(), Webhooks::new(h_js.clone()));
            let threads = Threads::new(h_js.clone(), human.clone(), bus::ensure_sessions(&h_js).await.unwrap());
            let claims = Claims { humans: &humans, webhooks: &webhooks, threads: &threads };
            assert_eq!(dead::bury_undelivered(&h_js, &stream, &advisory, &claims).await.unwrap(), None);
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert!(buried.try_recv().is_err(), "one watcher reports the dead letter");
            let letters = dead::list(&stream).await.unwrap();
            assert_eq!(letters.iter().map(|(s, _)| *s).collect::<Vec<_>>(), [dead_seq]);
            let notices = stream.get_last_raw_message_by_subject(&subject::inbox(&human, &dev_a)).await.unwrap();
            assert_eq!(notices.sequence, dead_seq + 1, "one notice, right after the dead letter");
            pulling.abort();
        };
        tokio::select! {
            () = test => {}
            _ = watchers => panic!("the watchers stopped"),
        }
    }

    #[tokio::test]
    async fn the_human_s_side_tells_the_owner_the_thread_s_posts_name_not_the_one_an_ant_claims() {
        let Some(server) = Server::start() else { return };
        let (human, alice, dev_a, dev_b, lead) = (UserId::human(), p("alice"), p("dev-a"), p("dev-b"), p("lead"));
        // alice runs the human's side, and gets what goes to a thread with no owner.
        let (h_client, h_js) = server.connect_as(&alice, Role::Human).await;
        let (_, b_js) = server.connect(&dev_b).await;
        let (_, lead_js) = server.connect(&lead).await;
        let stream = bus::init_stream(&h_js).await.unwrap();
        let mut to_human = h_client.subscribe(subject::inbox_of(&human)).await.unwrap();
        let mut to_alice = h_client.subscribe(subject::inbox_of(&alice)).await.unwrap();
        h_client.flush().await.unwrap();

        // lead starts a thread in #ops, which no human owns; on it dev-b names human as its owner, to dev-a.
        let ops: ChannelId = "ops".parse().unwrap();
        let root = ChannelPost::new(ops.clone(), lead.clone(), "plan the release");
        bus::publish_json(&lead_js, root.subject(), &root, None).await.unwrap();
        let thread = root.thread_id.clone();
        let message = Envelope::on_thread(thread.clone(), dev_b.clone(), dev_a.clone(), "relay this")
            .with_owner(Some(human.clone()));
        let message_seq = bus::publish_json(&b_js, message.subject(), &message, None).await.unwrap();
        let post =
            ChannelPost::on_thread(ops, thread.clone(), dev_b.clone(), "@dev-a check").with_owner(Some(human.clone()));
        let post_seq = bus::publish_json(&b_js, post.subject(), &post, None).await.unwrap();

        let (humans, webhooks) = (Registry::new(alice.clone()), Webhooks::new(h_js.clone()));
        let threads = Threads::new(h_js.clone(), alice.clone(), bus::ensure_sessions(&h_js).await.unwrap());
        let claims = Claims { humans: &humans, webhooks: &webhooks, threads: &threads };
        for (consumer, seq) in [(ants_proto::inbox_consumer(&dev_a), message_seq), (chan_consumer(&dev_a), post_seq)] {
            let advisory = dead::MaxDeliveries { stream: STREAM.to_owned(), consumer, stream_seq: seq, deliveries: 5 };
            let (_, letter) = dead::bury_undelivered(&h_js, &stream, &advisory, &claims).await.unwrap().unwrap();
            let notice: Envelope =
                serde_json::from_slice(&timeout(WAIT, to_alice.next()).await.unwrap().unwrap().payload).unwrap();
            let named = (&notice.thread_id, notice.owner, notice.creator);
            assert_eq!(named, (&thread, None, Some(lead.clone())), "as lead's post says");
            if let Some(envelope) = letter.envelope {
                assert_eq!((envelope.owner, envelope.creator), (None, Some(lead.clone())), "kept as checked");
            }
        }
        assert!(
            timeout(Duration::from_millis(200), to_human.next()).await.is_err(),
            "nothing goes to the claimed owner"
        );
    }

    /// A port nobody listens on right now (nats-server does not report its leafnode port).
    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    async fn connect_to(url: &str, creds: &Path, domain: Option<&JsDomain>, user: &UserId) -> jetstream::Context {
        let (_, mut js) = bus::connect(url, Some(creds), domain, user, "test").await.unwrap();
        js.set_timeout(Duration::from_secs(1));
        js
    }

    /// A hub with a JetStream domain, and a leaf node without JetStream whose uplink uses `ants auth leaf` credentials
    /// for dev-a and the webhook github: dev-a reaches the hub's stream through the domain, and github delivers there
    /// (to dev-b, elsewhere); dev-b and the webhook alerts, also valid on the leaf, get nothing across.
    #[tokio::test]
    async fn clients_on_a_leaf_node_reach_the_hub_stream_through_its_domain() {
        let leaf_port = free_port();
        let Some(hub) = Server::start_with(
            "domain: hub",
            &format!("leafnodes {{ listen: \"127.0.0.1:{leaf_port}\"\nno_advertise: true }}\n"),
        ) else {
            return;
        };
        let domain: JsDomain = "hub".parse().unwrap();
        let (human, dev_a, dev_b) = (UserId::human(), p("dev-a"), p("dev-b"));
        let team = hub.dir().join("auth");

        let (h_client, mut h_js) =
            bus::connect(&hub.url, Some(&hub.creds_in(&human, Some(&domain))), Some(&domain), &human, "test")
                .await
                .unwrap();
        h_js.set_timeout(Duration::from_secs(1));
        bus::init_stream(&h_js).await.unwrap();
        bus::ensure_sessions(&h_js).await.unwrap();
        bus::ensure_attempts(&h_js).await.unwrap();
        crate::webhook::ensure_bucket(&h_js).await.unwrap();

        let (github, alerts) = (p("github"), p("alerts"));
        for hook in [&github, &alerts] {
            let permissions = Permissions::for_webhook(hook, &[Destination::Ant(dev_b.clone())]);
            issue_with(&team, hook, permissions.with_js_domain(Some(&domain)), &team.join(format!("{hook}.creds")))
                .unwrap();
        }
        let leaf_creds = hub.dir().join("leaf-m1.creds");
        let m1: Machine = "m1".parse().unwrap();
        let on_m1 = members(std::slice::from_ref(&dev_a), &[]);
        let missing = issue_leaf(&team, &m1, &on_m1, &[p("ci")], Some(&domain), &leaf_creds).unwrap_err();
        assert!(format!("{missing:#}").contains("ants auth user ci --webhook"), "{missing:#}");
        issue(&team, &p("dev-c"), Role::Ant, Some(&domain), &team.join("dev-c.creds")).unwrap();
        let ant = issue_leaf(&team, &m1, &on_m1, &[p("dev-c")], Some(&domain), &leaf_creds).unwrap_err();
        assert!(format!("{ant:#}").contains("no webhook's"), "{ant:#}");
        issue_leaf(&team, &m1, &[], std::slice::from_ref(&github), None, &leaf_creds).unwrap();
        issue_leaf(&team, &m1, &on_m1, std::slice::from_ref(&github), Some(&domain), &leaf_creds).unwrap();
        assert!(
            async_nats::ConnectOptions::with_credentials_file(&leaf_creds)
                .await
                .unwrap()
                .connect(&hub.url)
                .await
                .is_err(),
            "leaf credentials do not work for clients"
        );
        let account = load(&team.join(ACCOUNT_SEED)).unwrap().public_key();
        let leaf = NatsServer::spawn_with(|dir| {
            fs::copy(team.join(SERVER_CONF), dir.join(SERVER_CONF)).unwrap();
            format!(
                "include \"{SERVER_CONF}\"\nleafnodes {{ remotes: [{{ url: \"nats-leaf://127.0.0.1:{leaf_port}\"\n\
                 account: {account}\ncredentials: {leaf_creds:?} }}] }}\n"
            )
        })
        .unwrap();

        // Without the domain the JetStream API stays on the leaf, which has none.
        let a_plain = connect_to(&leaf.url, &hub.creds(&dev_a), None, &dev_a).await;
        let a_js = connect_to(&leaf.url, &hub.creds_in(&dev_a, Some(&domain)), Some(&domain), &dev_a).await;
        let mut stream = None;
        for _ in 0..50 {
            if let Ok(s) = bus::ensure_stream(&a_js).await {
                stream = Some(s);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let stream = stream.expect("dev-a on the leaf reaches the hub stream");
        assert!(bus::ensure_stream(&a_plain).await.is_err(), "no JetStream without the domain");

        // github, as `ants webhook serve` on the leaf, keeps its events in the bucket and delivers to dev-b's inbox.
        let hook_js = connect_to(&leaf.url, &team.join("github.creds"), Some(&domain), &github).await;
        let mut seen = None;
        for _ in 0..50 {
            if let Ok(store) = hook_js.get_key_value(ants_proto::WEBHOOKS_BUCKET).await {
                seen = Some(store);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let seen = seen.expect("github on the leaf reaches the hub's webhooks bucket");
        seen.create(ants_proto::webhook_seen_key(&github, "e1"), "{}".into()).await.unwrap();
        let event = Envelope::new(github.clone(), dev_b.clone(), "build red").from_webhook();
        let stored = hook_js.publish(event.subject(), serde_json::to_vec(&event).unwrap().into()).await.unwrap();
        let stored = stored.await.unwrap().sequence;
        let on_hub = h_js.get_stream(STREAM).await.unwrap().get_raw_message(stored).await.unwrap();
        assert_eq!(serde_json::from_slice::<Envelope>(&on_hub.payload).unwrap(), event);
        let alerts_js = connect_to(&leaf.url, &team.join("alerts.creds"), Some(&domain), &alerts).await;
        assert!(alerts_js.get_key_value(ants_proto::WEBHOOKS_BUCKET).await.is_err(), "alerts is not on the uplink");

        // dev-a serves its inbox from the leaf, keeps its session, and replies.
        let mut human_inbox = h_client.subscribe(subject::inbox_of(&human)).await.unwrap();
        h_client.flush().await.unwrap();
        let ask = Envelope::new(human.clone(), dev_a.clone(), "hi");
        bus::publish(&h_js, &ask).await.unwrap();
        let mut inbox = bus::inbox_consumer(&stream, &dev_a, WAIT, 5, 2).await.unwrap();
        let msg = timeout(WAIT, inbox.next()).await.unwrap().unwrap().unwrap();
        assert_eq!(serde_json::from_slice::<Envelope>(&msg.payload).unwrap(), ask);
        let sessions = bus::ensure_sessions(&a_js).await.unwrap();
        let key = session_key(&dev_a, &ask.thread_id);
        sessions.put(&key, "s1".into()).await.unwrap();
        assert_eq!(sessions.get(&key).await.unwrap().as_deref(), Some(&b"s1"[..]));
        let attempts = bus::ensure_attempts(&a_js).await.unwrap();
        let marker = attempt_key(&dev_a, msg.info().unwrap().stream_sequence);
        attempts.put(&marker, "started".into()).await.unwrap();
        assert!(attempts.get(&marker).await.unwrap().is_some());
        attempts.delete(&marker).await.unwrap();
        let reply = ask.reply(dev_a.clone(), "hello", Role::Human).unwrap();
        bus::publish(&a_js, &reply).await.unwrap();
        msg.double_ack().await.unwrap();
        let got = timeout(WAIT, human_inbox.next()).await.unwrap().unwrap();
        assert_eq!(serde_json::from_slice::<Envelope>(&got.payload).unwrap(), reply);
        // And reads the decision stored on one of its approval requests.
        let decision = subject::approval_decision(&dev_a, &ants_proto::Uuid::now_v7());
        h_js.publish(decision.clone(), "allow".into()).await.unwrap().await.unwrap();
        assert_eq!(&stream.direct_get_last_for_subject(decision).await.unwrap().payload[..], b"allow");

        // The uplink carries only what dev-a may do: the leaf accepts dev-b, but nothing of dev-b crosses either way.
        let b_js = connect_to(&leaf.url, &hub.creds_in(&dev_b, Some(&domain)), Some(&domain), &dev_b).await;
        assert!(bus::ensure_stream(&b_js).await.is_err(), "dev-b's JetStream requests stay on the leaf");
        let (a_client, b_client) = (a_js.client(), b_js.client());
        let mut a_live = a_client.subscribe(subject::inbox_of(&dev_a)).await.unwrap();
        let mut b_live = b_client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
        let mut statuses = h_client.subscribe("ants.status.>").await.unwrap();
        for c in [&a_client, &b_client, &h_client] {
            c.flush().await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let thread = &ask.thread_id;
        b_client.publish(subject::status(&dev_b, thread), "b".into()).await.unwrap();
        b_client.flush().await.unwrap();
        a_client.publish(subject::status(&dev_a, thread), "a".into()).await.unwrap();
        let got = timeout(WAIT, statuses.next()).await.unwrap().unwrap();
        assert_eq!(got.subject.as_str(), subject::status(&dev_a, thread), "dev-b's status stays on the leaf");
        h_client.publish(subject::inbox(&dev_b, &human), "to b".into()).await.unwrap();
        h_client.publish(subject::inbox(&dev_a, &human), "to a".into()).await.unwrap();
        h_client.flush().await.unwrap();
        assert_eq!(&timeout(WAIT, a_live.next()).await.unwrap().unwrap().payload[..], b"to a");
        assert!(timeout(Duration::from_millis(300), b_live.next()).await.is_err(), "nothing for dev-b crosses");
    }

    /// Stream sequence of the first copy of `envelope`.
    async fn ask_seq(stream: &jetstream::stream::Stream, envelope: &Envelope) -> u64 {
        stream.get_first_raw_message_by_subject(envelope.subject(), 1).await.unwrap().sequence
    }
}
