//! Team credentials for NATS decentralized auth: one operator, one account holding the ants stream, and one user per
//! persona whose JWT carries the permissions of [`Permissions::for_persona`].
//!
//! The operator and account seeds stay in the team directory on the machine that issues credentials. The server
//! needs only `server.conf`; each machine gets the `.creds` file of the personas it runs.

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use ants_proto::{Permissions, PersonaId};
use anyhow::{Context as _, Result, anyhow, bail};
use data_encoding::{BASE32_NOPAD, BASE64URL_NOPAD};
use nkeys::KeyPair;
use serde_json::{Value, json};

const OPERATOR_SEED: &str = "operator.nk";
const ACCOUNT_SEED: &str = "account.nk";
const SYSTEM_SEED: &str = "system.nk";
pub const SERVER_CONF: &str = "server.conf";
const TEAM: &str = "ants";

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

/// Issue a new user for `persona` and write its `.creds` file to `out`. Earlier credentials stay valid.
pub fn issue(dir: &Path, persona: &PersonaId, out: &Path) -> Result<()> {
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let user = KeyPair::new_user();
    write_secret(out, &creds(&user_jwt(&account, &user, persona)?, &seed(&user)?))
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
         operator: {operator_jwt}\n\
         system_account: {system}\n\
         resolver: MEMORY\n\
         resolver_preload: {{\n  {system}: {system_jwt}\n  {account}: {account_jwt}\n}}\n",
        system = system.public_key(),
        account = account.public_key(),
    ))
}

fn user_jwt(account: &KeyPair, user: &KeyPair, persona: &PersonaId) -> Result<String> {
    let Permissions { publish, subscribe } = Permissions::for_persona(persona);
    jwt(
        account,
        &user.public_key(),
        persona.as_str(),
        json!({
            "pub": { "allow": publish },
            "sub": { "allow": subscribe },
            "subs": -1, "data": -1, "payload": -1,
            "type": "user",
            "version": 2,
        }),
    )
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
    use ants_daemon::dead::{self, Attempt};
    use ants_daemon::roster::Registration;
    use ants_proto::{Envelope, RosterEntry, STREAM, STREAM_SUBJECTS, ThreadId, inbox_prefix, session_key, subject};
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
            NatsServer::start_with(|dir| {
                init(&dir.join("auth")).unwrap();
                format!("include \"auth/{SERVER_CONF}\"\n")
            })
            .map(Self)
        }

        fn creds(&self, persona: &PersonaId) -> PathBuf {
            let out = self.dir().join(format!("{persona}.creds"));
            issue(&self.dir().join("auth"), persona, &out).unwrap();
            out
        }

        pub(crate) async fn connect(&self, persona: &PersonaId) -> (Client, jetstream::Context) {
            let (client, mut js) = bus::connect(&self.url, Some(&self.creds(persona)), persona, "test").await.unwrap();
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

        assert!(bus::connect(&server.url, None, &human, "anonymous").await.is_err(), "credentials are required");

        let (_, a_js) = server.connect(&dev_a).await;
        assert!(bus::ensure_stream(&a_js).await.is_err(), "only the human creates the stream");
        let (h_client, h_js) = server.connect(&human).await;
        bus::ensure_stream(&h_js).await.unwrap();

        let mut human_inbox = h_client.subscribe(subject::inbox(&human)).await.unwrap();
        h_client.flush().await.unwrap();
        let ask = Envelope::new(human.clone(), dev_a.clone(), "hi");
        bus::publish(&h_js, &ask).await.unwrap();

        // dev-a serves its inbox the way the daemon does, and replies.
        let stream = bus::ensure_stream(&a_js).await.unwrap();
        let mut inbox = bus::inbox_consumer(&stream, &dev_a, WAIT, 5, 2).await.unwrap();
        let msg = timeout(WAIT, inbox.next()).await.unwrap().unwrap().unwrap();
        assert_eq!(serde_json::from_slice::<Envelope>(&msg.payload).unwrap(), ask);
        let reply = ask.reply(dev_a.clone(), "hello").unwrap();
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
        // dev-a hears the human's decisions on its approval requests, but cannot decide them itself.
        let request = ants_proto::Uuid::now_v7();
        let mut decisions = a_raw.subscribe(subject::approval_decision(&dev_a, &request)).await.unwrap();
        a_raw.publish(subject::approval_decision(&dev_a, &request), "allow".into()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"ants.approvals.dev-a.{request}\"")), "{v}");
        h_client.publish(subject::approval_decision(&dev_a, &request), "allow".into()).await.unwrap();
        let got = timeout(WAIT, decisions.next()).await.unwrap().unwrap();
        assert_eq!(&got.payload[..], b"allow");
        // Its own inbox it may watch live; a denial would be the next violation below.
        let _own = a_raw.subscribe(subject::inbox(&dev_a)).await.unwrap();
        for denied in [subject::inbox(&dev_b), "_INBOX.>".to_owned(), subject::ALL.to_owned()] {
            let _sub = a_raw.subscribe(denied.clone()).await.unwrap();
            let v = next_violation(&a_raw, &mut errors).await;
            assert!(v.contains(&format!("Subscription to \"{denied}\"")), "{v}");
        }
    }

    #[tokio::test]
    async fn personas_write_their_own_roster_entry_and_the_human_reads_the_roster() {
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
            workdir: None,
            started_at: now,
            heartbeat_at: now,
        };
        let registration =
            Registration::start(a_kv.clone(), entry.clone(), ants_proto::ROSTER_HEARTBEAT).await.unwrap();
        assert!(a_kv.put("dev-b", "spoof".into()).await.is_err(), "dev-a cannot list dev-b");
        assert!(a_kv.get("dev-a").await.is_err(), "personas do not read the roster");

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
    async fn failed_messages_become_dead_letters_the_human_replays() {
        let Some(server) = Server::start() else { return };
        let (human, dev_a) = (PersonaId::human(), p("dev-a"));
        let (h_client, h_js) = server.connect(&human).await;
        let (_, a_js) = server.connect(&dev_a).await;

        // A stream from before dead letters gains their subject on `ants init`.
        let old: Vec<String> =
            STREAM_SUBJECTS.iter().filter(|s| **s != subject::DEAD_ALL).map(|s| (*s).to_owned()).collect();
        h_js.create_stream(jetstream::stream::Config { name: STREAM.to_owned(), subjects: old, ..Default::default() })
            .await
            .unwrap();
        let stream = bus::init_stream(&h_js).await.unwrap();
        assert!(bus::missing_subjects(&stream.cached_info().config.subjects).is_empty());

        let mut human_inbox = h_client.subscribe(subject::inbox(&human)).await.unwrap();
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
        h_client.publish(subject::inbox(&dev_a), "{oops".into()).await.unwrap();
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
    }

    /// Stream sequence of the first copy of `envelope`.
    async fn ask_seq(stream: &jetstream::stream::Stream, envelope: &Envelope) -> u64 {
        stream.get_first_raw_message_by_subject(envelope.subject(), 1).await.unwrap().sequence
    }
}
