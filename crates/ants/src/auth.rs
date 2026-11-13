//! Team credentials for NATS decentralized auth: one operator, one account holding the ants stream, and one user per
//! persona or human whose JWT carries the permissions of [`Permissions::for_persona`] in its role.
//!
//! The operator and account seeds stay in the team directory on the machine that issues credentials. The server
//! needs only `server.conf`; each machine gets the `.creds` file of the personas it runs, and its leaf node the uplink
//! credentials of [`issue_leaf`].
//!
//! When the hub has a JetStream domain, credentials are issued for it: they allow the JetStream API as
//! `$JS.<domain>.API`, the prefix every ants process then uses, besides the plain `$JS.API` the hub maps it to
//! ([`Permissions::with_js_domain`]).

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use ants_proto::{JsDomain, Machine, Permissions, PersonaId, Role};
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

/// The role `ants auth user` issues: a human's with `--human`, and for the default human whatever the flag; else a
/// persona's.
pub fn role(persona: &PersonaId, human: bool) -> Role {
    if human { Role::Human } else { Role::by_default(persona) }
}

/// The personas and humans served on a machine, each in its role, without repeats: a name given as both is a human.
pub fn members(personas: &[PersonaId], humans: &[PersonaId]) -> Vec<(PersonaId, Role)> {
    let mut members: Vec<(PersonaId, Role)> = humans.iter().map(|h| (h.clone(), Role::Human)).collect();
    for persona in personas {
        if !members.iter().any(|(m, _)| m == persona) {
            members.push((persona.clone(), Role::by_default(persona)));
        }
    }
    members
}

/// Issue a new user for `persona` in `role`, for the hub's JetStream domain if it has one, and write its `.creds` file
/// to `out`. Earlier credentials stay valid.
pub fn issue(dir: &Path, persona: &PersonaId, role: Role, js_domain: Option<&JsDomain>, out: &Path) -> Result<()> {
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let user = KeyPair::new_user();
    let permissions = Permissions::for_persona(persona, role).with_js_domain(js_domain);
    write_secret(out, &creds(&user_jwt(&account, &user, persona.as_str(), permissions, json!({}))?, &seed(&user)?))
}

/// Issue the uplink user of `machine`'s leaf node and write its `.creds` file to `out`. It may carry only what
/// `members`, personas and humans each in its role ([`members`]), may do and the sourcing of the machine's outbox
/// ([`Permissions::for_leaf`]), and connect only as a leaf node, not as a client.
///
/// nats-server applies a leaf user's permissions from the leaf's side, as for a client: publish limits what the leaf
/// sends to the hub, subscribe what the hub sends to the leaf (the hub swaps them internally).
pub fn issue_leaf(
    dir: &Path,
    machine: &Machine,
    members: &[(PersonaId, Role)],
    js_domain: Option<&JsDomain>,
    out: &Path,
) -> Result<()> {
    if members.is_empty() {
        bail!("a leaf node needs at least one persona or human");
    }
    if js_domain.is_some_and(|domain| domain.as_str() == machine.as_str()) {
        bail!("machine {machine} would take the hub's JetStream domain; name it otherwise");
    }
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let user = KeyPair::new_user();
    let roles = members.iter().map(|(member, role)| (member, *role));
    let permissions = Permissions::for_leaf(machine, roles).with_js_domain(js_domain);
    let only_leaf = json!({ "allowed_connection_types": ["LEAFNODE", "LEAFNODE_WS"] });
    let jwt = user_jwt(&account, &user, &format!("leaf-{machine}"), permissions, only_leaf)?;
    write_secret(out, &creds(&jwt, &seed(&user)?))
}

fn server_conf(operator: &KeyPair, system: &KeyPair, account: &KeyPair) -> Result<String> {
    let operator_jwt = jwt(
        operator,
        &operator.public_key(),
        TEAM,
        json!({ "system_account": system.public_key(), "type": "operator", "version": 2 }),
    )?;
    // JetStream needs a system account in operator mode.
    let system_jwt = jwt(operator, &system.public_key(), "SYS", json!({ "type": "account", "version": 2 }))?;
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

/// A user JWT with `permissions`, plus the `extra` claims.
fn user_jwt(account: &KeyPair, user: &KeyPair, name: &str, permissions: Permissions, extra: Value) -> Result<String> {
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
    jwt(account, &user.public_key(), name, nats)
}

/// A NATS v2 JWT for `subject`, signed by `issuer`.
fn jwt(issuer: &KeyPair, subject: &str, name: &str, nats: Value) -> Result<String> {
    let iat = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let mut claims = json!({ "iat": iat, "iss": issuer.public_key(), "name": name, "sub": subject, "nats": nats });
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
    use std::time::Duration;

    use ants_daemon::bus;
    use ants_daemon::channel::{Channels, Threads};
    use ants_daemon::dead::{self, Attempt};
    use ants_daemon::plan;
    use ants_daemon::roster::Registration;
    use ants_proto::{
        ChannelEntry, ChannelId, ChannelPost, DeadLetter, Envelope, PlanPost, RosterEntry, STREAM, STREAM_SUBJECTS,
        ThreadId, attempt_key, chan_consumer, chan_reader, inbox_prefix, plan_consumer, session_key, subject,
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

        fn creds(&self, persona: &PersonaId) -> PathBuf {
            self.creds_in(persona, None)
        }

        /// Credentials of `persona` issued for `domain`, as `ants auth user` without `--human` does.
        fn creds_in(&self, persona: &PersonaId, domain: Option<&JsDomain>) -> PathBuf {
            let suffix = domain.map(|d| format!("-{d}")).unwrap_or_default();
            let out = self.dir().join(format!("{persona}{suffix}.creds"));
            issue(&self.dir().join("auth"), persona, role(persona, false), domain, &out).unwrap();
            out
        }

        pub(crate) async fn connect(&self, persona: &PersonaId) -> (Client, jetstream::Context) {
            self.connect_as(persona, role(persona, false)).await
        }

        /// Credentials of `persona` issued in `role`.
        pub(crate) fn creds_as(&self, persona: &PersonaId, role: Role) -> PathBuf {
            let creds = self.dir().join(format!("{persona}-{role:?}.creds"));
            issue(&self.dir().join("auth"), persona, role, None, &creds).unwrap();
            creds
        }

        /// A connection with credentials of `persona` issued in `role`.
        pub(crate) async fn connect_as(&self, persona: &PersonaId, role: Role) -> (Client, jetstream::Context) {
            let creds = self.creds_as(persona, role);
            let (client, mut js) = bus::connect(&self.url, Some(&creds), None, persona, "test").await.unwrap();
            // Denied JetStream requests only time out.
            js.set_timeout(Duration::from_secs(1));
            (client, js)
        }

        /// A connection that reports the server errors it gets, such as permission violations.
        async fn watched(&self, persona: &PersonaId) -> (Client, mpsc::UnboundedReceiver<String>) {
            let (tx, rx) = mpsc::unbounded_channel();
            let client = async_nats::ConnectOptions::with_credentials_file(self.creds(persona))
                .await
                .unwrap()
                .custom_inbox_prefix(inbox_prefix(persona))
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

    fn p(s: &str) -> PersonaId {
        s.parse().unwrap()
    }

    /// The server handles operations in order, so the next violation names the first denied one.
    async fn next_violation(client: &Client, errors: &mut mpsc::UnboundedReceiver<String>) -> String {
        client.flush().await.unwrap();
        timeout(WAIT, errors.recv()).await.expect("no violation").unwrap()
    }

    #[tokio::test]
    async fn credentials_confine_each_persona_to_its_role() {
        let Some(server) = Server::start() else { return };
        let (human, dev_a, dev_b) = (PersonaId::human(), p("dev-a"), p("dev-b"));

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
        a_raw.publish(subject::dead(&dev_b), "spoof".into()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains("Publish to \"ants.dead.dev-b\""), "{v}");
        // It sends only under its own name: not as the human, nor as dev-b to the human.
        a_raw.publish(subject::inbox(&dev_a, &dev_a), "ok".into()).await.unwrap();
        for spoof in [subject::inbox(&dev_b, &human), subject::inbox(&human, &dev_b)] {
            a_raw.publish(spoof.clone(), "spoof".into()).await.unwrap();
            let v = next_violation(&a_raw, &mut errors).await;
            assert!(v.contains(&format!("Publish to \"{spoof}\"")), "{v}");
        }
        // It posts to plan rooms only under its own name too.
        let room: ThreadId = "t2".parse().unwrap();
        a_raw.publish(subject::plan(&room, &dev_a), "ok".into()).await.unwrap();
        for spoof in [subject::plan(&room, &dev_b), subject::plan(&room, &human)] {
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
        for denied in [subject::inbox_of(&dev_b), "_INBOX.>".to_owned(), subject::ALL.to_owned()] {
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
            (role(&alice, true), role(&alice, false), role(&PersonaId::human(), false)),
            (Role::Human, Role::Persona, Role::Human)
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

        // Each human writes the humans bucket; personas read it, but never write it.
        for (kv, human) in [(&a_humans, &alice), (&b_humans, &bob)] {
            let entry = ants_proto::HumanEntry::new(human.clone());
            kv.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
        }
        let d_humans = crate::humans::ensure(&d_js).await.unwrap();
        let listed: ants_proto::HumanEntry =
            serde_json::from_slice(&d_humans.get("bob").await.unwrap().unwrap()).unwrap();
        assert_eq!((listed.check_key("bob"), listed.persona), (Ok(()), bob.clone()));
        let keys: Vec<String> = d_humans.keys().await.unwrap().try_collect().await.unwrap();
        assert_eq!(keys, ["alice", "bob"]);
        let forged = serde_json::to_vec(&ants_proto::HumanEntry::new(dev_a.clone())).unwrap();
        assert!(d_humans.put("dev-a", forged.into()).await.is_err(), "a persona cannot list itself");
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
    async fn plan_rooms_are_read_back_through_each_reader_own_consumer() {
        let Some(server) = Server::start() else { return };
        let (human, dev_a, dev_b) = (PersonaId::human(), p("dev-a"), p("dev-b"));
        let (_h, h_js) = server.connect(&human).await;
        bus::ensure_stream(&h_js).await.unwrap();
        let (_a, a_js) = server.connect(&dev_a).await;
        let (_b, b_js) = server.connect(&dev_b).await;
        let (thread, other): (ThreadId, ThreadId) = ("t1".parse().unwrap(), "t2".parse().unwrap());

        let first = PlanPost::spend(thread.clone(), dev_a.clone(), "draft", 8, None).unwrap();
        assert!(plan::publish(&a_js, &first, Some(0)).await.unwrap());
        // dev-b spends the same turn without having seen dev-a's post: refused, the room moved on.
        let stale = PlanPost::spend(thread.clone(), dev_b.clone(), "me first", 8, None).unwrap();
        assert!(!plan::publish(&b_js, &stale, Some(0)).await.unwrap());
        // dev-a may only post on its own subject, so a post naming dev-b as its author is dropped by readers.
        let forged = PlanPost::new(thread.clone(), dev_b.clone(), "dev-b agrees");
        bus::publish_json(&a_js, subject::plan(&thread, &dev_a), &forged, None).await.unwrap();
        plan::publish(&a_js, &PlanPost::new(other, dev_a.clone(), "elsewhere"), None).await.unwrap();

        let room = plan::read(&b_js, &dev_b, &thread).await.unwrap();
        assert_eq!(room.posts, std::slice::from_ref(&first));
        let answer = PlanPost::spend(thread.clone(), dev_b.clone(), "agreed", 8, room.last()).unwrap();
        assert!(plan::publish(&b_js, &answer, Some(room.last_seq)).await.unwrap());
        assert_eq!(plan::read(&h_js, &human, &thread).await.unwrap().posts, [first, answer]);

        // dev-a's plan consumer filters plan rooms only, and dev-b's is not dev-a's to use.
        let consumer = |name: String, filter: String| jetstream::consumer::pull::Config {
            name: Some(name),
            filter_subject: filter,
            ..Default::default()
        };
        let into_inbox = consumer(plan_consumer(&dev_a), subject::inbox_of(&dev_b));
        assert!(a_js.create_consumer_on_stream(into_inbox, STREAM).await.is_err());
        let not_its_own = consumer(plan_consumer(&dev_b), subject::plan_room(&thread));
        assert!(a_js.create_consumer_on_stream(not_its_own, STREAM).await.is_err());
        assert!(plan::read(&a_js, &dev_a, &thread).await.is_ok(), "its own works");
    }

    #[tokio::test]
    async fn personas_write_their_own_roster_entry_and_everyone_reads_the_roster() {
        let Some(server) = Server::start() else { return };
        let (human, dev_a) = (PersonaId::human(), p("dev-a"));
        let ttl = ants_proto::ROSTER_TTL;

        let (_a, a_js) = server.connect(&dev_a).await;
        assert!(bus::ensure_roster(&a_js, ttl).await.is_err(), "only the human creates the bucket");
        let (_h, h_js) = server.connect(&human).await;
        let h_kv = bus::ensure_roster(&h_js, ttl).await.unwrap();

        let a_kv = bus::ensure_roster(&a_js, ttl).await.unwrap();
        let now = chrono::Utc::now();
        let entry = RosterEntry {
            persona: dev_a.clone(),
            machine: "m1".to_owned(),
            harness: "copilot".to_owned(),
            capabilities: vec!["rust".to_owned()],
            whoami: None,
            workdir: None,
            started_at: now,
            heartbeat_at: now,
            activity: None,
            handling: None,
            harnesses: None,
        };
        let registration =
            Registration::start(a_kv.clone(), entry.clone(), ants_proto::ROSTER_HEARTBEAT).await.unwrap();
        assert!(a_kv.put("dev-b", "spoof".into()).await.is_err(), "dev-a cannot list dev-b");
        // Personas read the roster, to route requests by capability.
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
    async fn personas_post_to_channels_as_themselves_and_read_but_never_write_the_channels_bucket() {
        let Some(server) = Server::start() else { return };
        let (human, dev_a, dev_b) = (PersonaId::human(), p("dev-a"), p("dev-b"));
        let ops: ChannelId = "ops".parse().unwrap();
        let (_h, h_js) = server.connect(&human).await;
        let stream = bus::init_stream(&h_js).await.unwrap();
        assert!(stream.cached_info().config.subjects.iter().any(|s| s == subject::ALL_CHANNELS));
        let (_a, a_js) = server.connect(&dev_a).await;

        // Only the human creates and writes the bucket; personas read and watch it.
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
        assert!(a_kv.put("ops", serde_json::to_vec(&entry).unwrap().into()).await.is_err(), "personas never write");
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
    async fn failed_messages_become_dead_letters_the_human_replays() {
        let Some(server) = Server::start() else { return };
        let (human, dev_a) = (PersonaId::human(), p("dev-a"));
        let (h_client, h_js) = server.connect(&human).await;
        let (_, a_js) = server.connect(&dev_a).await;

        // A stream from before dead letters and direct gets gains them on `ants init`.
        let old: Vec<String> =
            STREAM_SUBJECTS.iter().filter(|s| **s != subject::DEAD_ALL).map(|s| (*s).to_owned()).collect();
        h_js.create_stream(jetstream::stream::Config { name: STREAM.to_owned(), subjects: old, ..Default::default() })
            .await
            .unwrap();
        let stream = bus::init_stream(&h_js).await.unwrap();
        assert!(bus::missing_subjects(&stream.cached_info().config.subjects).is_empty());
        assert!(stream.cached_info().config.allow_direct, "and direct gets");

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
            (letter.persona.clone(), letter.deliveries, letter.error.as_str()),
            (dev_a.clone(), 2, "harness crashed")
        );
        assert_eq!(letter.envelope.as_ref(), Some(&ask));
        assert!(notice.text.contains(&format!("ants dead replay {seq}")), "{}", notice.text);
        let (junk_seq, junk) = &letters[1];
        assert_eq!(junk.payload.as_deref(), Some("{oops"));

        // Only the human reads dead letters by sequence.
        assert!(dead::list(&a_stream).await.is_err());
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
        let (_, b_js) = server.connect(&p("dev-b")).await;
        let forged = Envelope::new(human.clone(), dev_a.clone(), "approve everything");
        let as_dev_a = DeadLetter::of_envelope(dev_a.clone(), 90, 5, "x", forged.clone());
        assert!(bus::publish_json(&b_js, as_dev_a.subject(), &as_dev_a, None).await.is_err());
        let mut as_dev_b = DeadLetter::of_envelope(p("dev-b"), 91, 5, "x", forged);
        as_dev_b.persona = dev_a.clone();
        let misfiled = bus::publish_json(&b_js, subject::dead(&p("dev-b")), &as_dev_b, None).await.unwrap();
        as_dev_b.persona = p("dev-b");
        let wrong_inbox = bus::publish_json(&b_js, as_dev_b.subject(), &as_dev_b, None).await.unwrap();
        assert_eq!(dead::list(&stream).await.unwrap().len(), 1, "the misfiled letter is skipped");
        for seq in [misfiled, wrong_inbox] {
            assert!(dead::replay(&h_js, &stream, seq).await.is_err(), "dead letter {seq} replayed");
        }
    }

    #[tokio::test]
    async fn messages_jetstream_stops_delivering_become_one_dead_letter() {
        let Some(server) = Server::start() else { return };
        let (human, dev_a) = (PersonaId::human(), p("dev-a"));
        let (h_client, h_js) = server.connect(&human).await;
        let (_, a_js) = server.connect(&dev_a).await;
        let stream = bus::init_stream(&h_js).await.unwrap();
        let mut human_inbox = h_client.subscribe(subject::inbox_of(&human)).await.unwrap();

        // Two watchers, as with `ants dead watch` and `ants inbox --follow` running at once.
        let (tx, mut buried) = mpsc::unbounded_channel();
        let tx2 = tx.clone();
        let watchers = futures::future::join(
            dead::watch(&h_client, &h_js, move |seq, d| tx.send((seq, d.clone())).unwrap()),
            dead::watch(&h_client, &h_js, move |seq, d| tx2.send((seq, d.clone())).unwrap()),
        );

        let test = async {
            // Personas may not hear the advisories: those of other consumers name messages they may not read.
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
            assert_eq!((letter.persona.clone(), letter.stream_seq, letter.deliveries), (dev_a.clone(), seq, 2));
            assert_eq!(letter.envelope.as_ref(), Some(&ask));
            assert!(letter.error.contains("max-deliveries advisory"), "{}", letter.error);
            let notice: Envelope =
                serde_json::from_slice(&timeout(WAIT, human_inbox.next()).await.unwrap().unwrap().payload).unwrap();
            assert_eq!((notice.from.clone(), notice.in_reply_to), (dev_a.clone(), Some(ask.id)));
            assert!(notice.text.contains(&format!("ants dead replay {dead_seq}")), "{}", notice.text);

            // A later advisory of the same message, past the duplicate window, finds the dead letter.
            let advisory = dead::MaxDeliveries {
                stream: STREAM.to_owned(),
                consumer: ants_proto::inbox_consumer(&dev_a),
                stream_seq: seq,
                deliveries: 2,
            };
            assert_eq!(dead::bury_undelivered(&h_js, &stream, &advisory, &Default::default()).await.unwrap(), None);
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

    /// A port nobody listens on right now (nats-server does not report its leafnode port).
    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    async fn connect_to(url: &str, creds: &Path, domain: Option<&JsDomain>, persona: &PersonaId) -> jetstream::Context {
        let (_, mut js) = bus::connect(url, Some(creds), domain, persona, "test").await.unwrap();
        js.set_timeout(Duration::from_secs(1));
        js
    }

    /// A hub with a JetStream domain, and a leaf node without JetStream whose uplink uses `ants auth leaf` credentials
    /// for dev-a: dev-a reaches the hub's stream through the domain; dev-b, also valid on the leaf, gets nothing across.
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
        let (human, dev_a, dev_b) = (PersonaId::human(), p("dev-a"), p("dev-b"));
        let team = hub.dir().join("auth");

        let (h_client, mut h_js) =
            bus::connect(&hub.url, Some(&hub.creds_in(&human, Some(&domain))), Some(&domain), &human, "test")
                .await
                .unwrap();
        h_js.set_timeout(Duration::from_secs(1));
        bus::init_stream(&h_js).await.unwrap();
        bus::ensure_sessions(&h_js).await.unwrap();
        bus::ensure_attempts(&h_js).await.unwrap();

        let leaf_creds = hub.dir().join("leaf-m1.creds");
        let on_m1 = members(std::slice::from_ref(&dev_a), &[]);
        issue_leaf(&team, &"m1".parse().unwrap(), &on_m1, Some(&domain), &leaf_creds).unwrap();
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
