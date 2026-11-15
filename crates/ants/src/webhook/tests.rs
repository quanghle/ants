use std::assert_matches;
use std::fmt::Write as _;
use std::net::SocketAddr;

use ants_proto::{ChannelEntry, Permissions, STREAM};
use futures::TryStreamExt as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::*;
use crate::auth::{issue_with, tests::Server};

fn p(s: &str) -> UserId {
    s.parse().unwrap()
}

fn parse(text: &str) -> Result<Config> {
    let env = |name: &str| match name {
        "HOOK_SECRET" => Some("s3cret".to_owned()),
        // The test vector of the Standard Webhooks libraries.
        "SIGNING_SECRET" => Some("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw".to_owned()),
        _ => None,
    };
    Config::parse(text, Path::new("/etc/ants/webhooks.toml"), Path::new("/home/q"), env)
}

fn error(text: &str) -> String {
    format!("{:#}", parse(text).err().expect("an error"))
}

const HOOK: &str = "[[webhook]]\nname = \"github\"\nauth = \"bearer\"\nsecret_env = \"HOOK_SECRET\"\n";

#[test]
fn the_config_names_one_destination_and_its_mentions_per_webhook() {
    let config = parse(&format!(
        "path = \"/in/\"\n{HOOK}ant = \"dev-a\"\ncreds = \"~/github.creds\"\nid = \"header:X-GitHub-Delivery\"\n\
             [[webhook]]\nname = \"ci\"\nauth = \"hmac-sha256\"\nsecret_env = \"HOOK_SECRET\"\nchannel = \"ops\"\n\
             broadcast = true\ncreds = \"ci.creds\"\n\
             [[webhook]]\nname = \"alerts\"\nauth = \"bearer\"\nsecret_env = \"HOOK_SECRET\"\nchannel = \"ops\"\n\
             mention = [\"dev-b\"]\nid = \"json:/alert/id\"\nallow = [\"read:*\", \"execute:gh run view*\"]\n"
    ))
    .unwrap();
    assert_eq!(config.path, "/in");
    let [github, ci, alerts] = &config.webhooks[..] else { panic!("three webhooks") };
    assert_eq!(github.destination, Destination::Ant(p("dev-a")));
    assert_eq!(github.creds.as_deref(), Some(Path::new("/home/q/github.creds")));
    assert_eq!(ci.creds.as_deref(), Some(Path::new("/etc/ants/ci.creds")));
    assert_matches!(github.id, IdSource::Header(ref h) if h == "x-github-delivery");
    assert_matches!(ci.id, IdSource::BodySha256);
    assert_matches!(alerts.id, IdSource::Json(ref p) if p == "/alert/id");
    assert_eq!((&github.mentions, &ci.mentions), (&Mentions::Nobody, &Mentions::Members));
    assert_eq!(alerts.mentions, Mentions::Listed(vec![p("dev-b")]));
    assert_eq!((&github.allow, &alerts.allow), (&None, &Some(vec!["read:*".into(), "execute:gh run view*".into()])));

    for (extra, why) in [
        ("", "exactly one of ant and channel"),
        ("ant = \"dev-a\"\nchannel = \"ops\"", "exactly one of ant and channel"),
        ("ant = \"dev-a\"\nmention = [\"dev-b\"]", "with a channel only"),
        ("ant = \"dev-a\"\nbroadcast = true", "with a channel only"),
        ("channel = \"ops\"\nmention = []\nbroadcast = true", "exclude each other"),
        ("ant = \"dev-a\"\ntext = \"{payload}\"", "unknown placeholder {payload}"),
        ("ant = \"dev-a\"\ntext = \"{json:a}\"", "starts with /"),
        ("ant = \"dev-a\"\ntext = \"{body\"", "without its }"),
        ("ant = \"dev-a\"\nid = \"body\"", "expected header:NAME"),
        ("ant = \"dev-a\"\nsignature_header = \"X-Sig\"", "hmac-sha256\" only"),
        ("ant = \"dev-a\"\nsecret_file = \"s\"", "exactly one of secret_env and secret_file"),
        ("ant = \"dev-a\"\nurl = \"x\"", "unknown field `url`"),
        ("ant = \"dev-a\"\nallow = [\"write\"]", "unknown tool kind"),
    ] {
        let e = error(&format!("{HOOK}{extra}\n"));
        assert!(e.contains("webhook github") || e.contains("unknown field"), "{e}");
        assert!(e.contains(why), "{extra}: {e}");
    }
    let unset = HOOK.replace("HOOK_SECRET", "NOT_SET");
    assert!(error(&format!("{unset}ant = \"dev-a\"\n")).contains("NOT_SET is not set"));
    let human = HOOK.replace("github", "human");
    assert!(error(&format!("{human}ant = \"dev-a\"\n")).contains("the default human is no webhook"));
    assert!(error(&format!("{HOOK}ant = \"dev-a\"\n{HOOK}ant = \"dev-b\"\n")).contains("listed twice"));
    assert!(error("nats = \"nats://hub:4222\"\n").contains("nothing to serve"));
    let err = error(&format!("nats = \"nats://dev:secret@hub\"\n{HOOK}ant = \"dev-a\"\n"));
    assert!(err.contains("NATS URL nats://***@hub") && !err.contains("secret"), "{err}");
}

#[test]
fn standard_webhooks_signatures_are_checked_with_the_signing_secret_within_the_tolerance() {
    const SIGNED: &str = "[[webhook]]\nname = \"gitlab\"\nauth = \"standard-webhooks\"\nsecret_env = \"SIGNING_SECRET\"\n\
                          ant = \"dev-a\"\n";
    let config = parse(SIGNED).unwrap();
    let hook = &config.webhooks[0];
    assert_matches!(hook.id, IdSource::Header(ref h) if h == "webhook-id");
    assert!(matches!(hook.auth, Auth::Standard { tolerance, .. } if tolerance == Duration::from_mins(5)));
    let tolerance = parse(&format!("{SIGNED}tolerance = \"90s\"\nid = \"body-sha256\"\n")).unwrap();
    assert!(
        matches!(tolerance.webhooks[0].auth, Auth::Standard { tolerance, .. } if tolerance == Duration::from_secs(90))
    );
    assert_matches!(tolerance.webhooks[0].id, IdSource::BodySha256);

    let body = br#"{"test": 2432232314}"#;
    let signature = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=";
    let headers = |signatures: &str| {
        let mut headers = HeaderMap::new();
        headers.insert(WEBHOOK_ID, HeaderValue::from_static("msg_p5jXN8AQM9LWM0D4loKWxJek"));
        headers.insert(WEBHOOK_TIMESTAMP, HeaderValue::from_static("1614265330"));
        headers.insert(WEBHOOK_SIGNATURE, signatures.parse().unwrap());
        headers
    };
    assert!(hook.authorized(&headers(signature), body));
    assert!(hook.authorized(&headers(&format!("v1a,AAAA v1,bm90IGl0 {signature}")), body), "any v1 one matches");
    assert!(!hook.authorized(&headers(&signature.replace("v1,", "v2,")), body));
    assert!(!hook.authorized(&headers("v1,bm90IGl0 v1,!!"), body));
    assert!(!hook.authorized(&headers(signature), br#"{"test": 2432232315}"#));
    let mut moved = headers(signature);
    moved.insert(WEBHOOK_TIMESTAMP, HeaderValue::from_static("1614265331"));
    assert!(!hook.authorized(&moved, body), "the timestamp is signed");
    let mut unsigned = headers(signature);
    unsigned.remove(WEBHOOK_SIGNATURE);
    assert!(!hook.authorized(&unsigned, body));

    let sent = SystemTime::UNIX_EPOCH + Duration::from_secs(1_614_265_330);
    let five = Duration::from_mins(5);
    assert!(recent(&headers(signature), five, sent + five) && recent(&headers(signature), five, sent - five));
    assert!(!recent(&headers(signature), five, sent + five + Duration::from_secs(1)), "too old");
    assert!(!recent(&headers(signature), five, sent - five - Duration::from_secs(1)), "too far ahead");
    let mut undated = headers(signature);
    undated.remove(WEBHOOK_TIMESTAMP);
    assert!(!recent(&undated, five, sent));
    assert!(!hook.authorized_before_body(&headers(signature)), "a stale request is refused before its body");
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs().to_string();
    let mut fresh = headers(signature);
    fresh.insert(WEBHOOK_TIMESTAMP, now.parse().unwrap());
    assert!(hook.authorized_before_body(&fresh));
    fresh.remove(WEBHOOK_ID);
    assert!(!hook.authorized_before_body(&fresh));

    for (extra, why) in [
        ("tolerance = \"0\"", "from 1s to 1d"),
        ("tolerance = \"2d\"", "from 1s to 1d"),
        ("tolerance = \"5\"", "a number without a unit"),
    ] {
        let e = error(&format!("{SIGNED}{extra}\n"));
        assert!(e.contains(why), "{extra}: {e}");
    }
    let e = error(&SIGNED.replace("SIGNING_SECRET", "HOOK_SECRET"));
    assert!(e.contains("no Standard Webhooks signing secret"), "{e}");
    let e = error(&format!("{HOOK}ant = \"dev-a\"\ntolerance = \"1m\"\n"));
    assert!(e.contains("standard-webhooks\" only"), "{e}");
}

#[test]
fn humans_are_notified_and_answer_links_with_public_url_and_the_action_secret() {
    const ALICE: &str = "[[human]]\nname = \"alice\"\n[[human.notify]]\nkind = \"ntfy\"\ntopic = \"a\"\n";
    let env = |name: &str| match name {
        "ACTION_SECRET" => Some("0123456789abcdef-secret\n".to_owned()),
        "HOOK_SECRET" => Some("s3cret".to_owned()),
        _ => None,
    };
    let parse = |text: &str| Config::parse(text, Path::new("/etc/ants/webhooks.toml"), Path::new("/home/q"), env);
    let error = |text: &str| format!("{:#}", parse(text).err().expect("an error"));
    let config = parse(ALICE).unwrap();
    assert_eq!((config.webhooks.len(), config.humans.len(), config.links.is_some()), (0, 1, false));
    let linked = format!("public_url = \"https://hub.example/hooks\"\naction_secret_env = \"ACTION_SECRET\"\n{ALICE}");
    assert!(parse(&linked).unwrap().links.is_some());

    let url = "public_url = \"https://hub.example/hooks\"\n";
    let secret = "action_secret_env = \"ACTION_SECRET\"\n";
    for (text, why) in [
        (format!("{url}{ALICE}"), "public_url needs action_secret_file"),
        (format!("{secret}{ALICE}"), "the action secret needs public_url"),
        (format!("{url}{secret}{HOOK}ant = \"dev-a\"\n"), "go with [[human]] entries"),
        (format!("{url}{secret}action_secret_file = \"s\"\n{ALICE}"), "at most one of action_secret_env"),
        (format!("public_url = \"https://hub.example/hooks?x=1\"\n{secret}{ALICE}"), "no user, password or query"),
        (format!("{ALICE}{ALICE}"), "human alice is listed twice"),
        (format!("{HOOK}ant = \"dev-a\"\n{}", ALICE.replace("alice", "github")), "both a webhook and a human"),
        ("[[human]]\nname = \"alice\"\n".to_owned(), "nowhere to send"),
    ] {
        let e = error(&text);
        assert!(e.contains(why), "{text}: {e}");
    }
}

#[tokio::test]
async fn a_reload_starts_new_webhooks_and_humans_stops_removed_ones_and_restarts_changed_ones() {
    const ALICE: &str = "[[human]]\nname = \"alice\"\n[[human.notify]]\nkind = \"ntfy\"\ntopic = \"a\"\n";
    let hook = |name: &str, text: &str| HOOK.replace("github", name) + &format!("ant = \"dev-a\"\ntext = \"{text}\"\n");
    let (mut tasks, (failed, _failed)) = (JoinSet::new(), mpsc::unbounded_channel());
    let first = parse(&format!("{}{}{ALICE}", hook("kept", "{body}"), hook("changed", "{body}"))).unwrap();
    let mut running = Running::new("nats://127.0.0.1:1", None, &first);
    let before = running.apply(first, &mut tasks, &failed);
    let stopped = running.hooks[&p("changed")].task.clone();

    let second =
        format!("path = \"/in\"\n{}{}{}{ALICE}", hook("kept", "{body}"), hook("changed", "{id}"), hook("new", "x"));
    let after = running.apply(parse(&second).unwrap(), &mut tasks, &failed);
    assert_eq!(after.prefix, "/in/");
    assert!(Arc::ptr_eq(&before.hooks["kept"], &after.hooks["kept"]), "an unchanged webhook carries on");
    assert!(!Arc::ptr_eq(&before.hooks["changed"], &after.hooks["changed"]), "a changed one starts again");
    assert!(Arc::ptr_eq(&before.hooks["changed"].outcomes, &after.hooks["changed"].outcomes), "its counts stay");
    assert!(after.hooks.contains_key("new") && Arc::ptr_eq(&before.humans[0], &after.humans[0]));
    tokio::task::yield_now().await;
    assert!(stopped.is_finished());

    let human = running.humans[&p("alice")].task.clone();
    let third = parse(&hook("new", "x")).unwrap();
    let removed = running.apply(third, &mut tasks, &failed);
    assert_eq!(removed.hooks.keys().collect::<Vec<_>>(), ["new"]);
    assert!(removed.humans.is_empty() && running.humans.is_empty());
    tokio::task::yield_now().await;
    assert!(human.is_finished(), "a human no longer listed stops");

    let other_hub = parse(&format!("nats = \"nats://127.0.0.1:2\"\n{}", hook("new", "x"))).unwrap();
    let moved = running.apply(other_hub, &mut tasks, &failed);
    assert!(!Arc::ptr_eq(&removed.hooks["new"], &moved.hooks["new"]), "another hub restarts every webhook");
}

#[test]
fn a_secret_file_must_be_private() {
    let dir = ants_test_support::scratch_dir("webhook-secret");
    let secret = dir.join("github.secret");
    std::fs::write(&secret, "s3cret\n").unwrap();
    let text =
        format!("[[webhook]]\nname = \"github\"\nauth = \"bearer\"\nsecret_file = {secret:?}\nant = \"dev-a\"\n");
    let path = dir.join("webhooks.toml");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o644)).unwrap();
        let e = Config::parse(&text, &path, &dir, |_| None).err().unwrap();
        assert!(format!("{e:#}").contains("readable by others"), "{e:#}");
        std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let config = Config::parse(&text, &path, &dir, |_| None).unwrap();
    assert!(matches!(&config.webhooks[0].auth, Auth::Bearer(token) if token == "s3cret"), "trimmed");
}

#[test]
fn the_text_comes_from_the_template_and_the_event_id_from_its_source() {
    let config = parse(&format!(
        "{HOOK}ant = \"dev-a\"\nid = \"json:/id\"\n\
             text = \"{{{{{{webhook}}}}}} {{header:X-Event}} {{json:/repo/name}} {{json:/n}} {{json:/none}}|{{id}}\\n{{body}}\"\n"
    ))
    .unwrap();
    let hook = &config.webhooks[0];
    let mut headers = HeaderMap::new();
    headers.insert("x-event", HeaderValue::from_static("push"));
    let body = br#"{"id":42,"repo":{"name":"ants"},"n":[1]}"#;
    let event = hook.event(&headers, body).ok().unwrap();
    assert_eq!(event.text, format!("{{github}} push ants [1] |42\n{}", std::str::from_utf8(body).unwrap()));
    assert_eq!(event.hash.len(), ID_HASH_LEN);
    assert_eq!(hook.event(&headers, br#"{"id":42}"#).ok().unwrap().hash, event.hash, "the same id");

    let status = |body: &[u8]| hook.event(&headers, body).err().unwrap().0;
    assert_eq!(status(br#"{"repo":{}}"#), StatusCode::BAD_REQUEST, "no id");
    assert_eq!(status(b"not json"), StatusCode::BAD_REQUEST);
    let plain = parse(&format!("{HOOK}ant = \"dev-a\"\n")).unwrap();
    let event = plain.webhooks[0].event(&headers, b"ping").ok().unwrap();
    let id = HEXLOWER.encode(digest::digest(&digest::SHA256, b"ping").as_ref());
    assert_eq!(event.text, format!("[webhook github] {id}\n\nping"));
    assert_eq!(plain.webhooks[0].event(&headers, b"\xff").err().unwrap().0, StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

// ---- over HTTP -------------------------------------------------------------------------------------------

struct Answer {
    status: u16,
    headers: String,
    body: String,
}

impl Answer {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or_else(|e| panic!("{e}: {}", self.body))
    }
}

async fn request(addr: SocketAddr, method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Answer {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: hooks\r\nConnection: close\r\n");
    for (name, value) in headers {
        let _ = write!(head, "{name}: {value}\r\n");
    }
    let _ = write!(head, "Content-Length: {}\r\n\r\n", body.len());
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut answer = String::new();
    stream.read_to_string(&mut answer).await.unwrap();
    let (headers, body) = answer.split_once("\r\n\r\n").unwrap_or_else(|| panic!("{method} {path}: {answer:?}"));
    let status = headers.split(' ').nth(1).unwrap().parse().unwrap();
    Answer { status, headers: headers.to_lowercase(), body: body.to_owned() }
}

fn signed(body: &[u8]) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, b"s3cret");
    format!("sha256={}", HEXLOWER.encode(hmac::sign(&key, body).as_ref()))
}

/// `ants webhook serve` with the config at `path`, checked for changes every 50 ms, and where it listens.
async fn start(path: &Path, nats: &str) -> (JoinHandle<Result<()>>, SocketAddr) {
    let config = Config::load(path).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (nats, path) = (nats.to_owned(), path.to_owned());
    let triggers = crate::reload::Triggers::new(path.clone(), None, Duration::from_millis(50)).unwrap();
    let reload = Reload { triggers, load: move || Config::load(&path) };
    (tokio::spawn(async move { serve(config, &nats, None, listener, reload).await }), addr)
}

/// Send until the webhook is connected: 503 until then.
async fn deliver(addr: SocketAddr, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Answer {
    for _ in 0..100 {
        let answer = request(addr, "POST", path, headers, body).await;
        if answer.status != 503 {
            return answer;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{path} never connected");
}

/// The hub with its stream and buckets, as `ants init` leaves it, with #ops of dev-a and dev-b, and a private
/// secret file.
pub(crate) async fn hub() -> Option<(Server, jetstream::Context, PathBuf)> {
    let server = Server::start()?;
    let (_, js) = server.connect(&UserId::human()).await;
    bus::ensure_stream(&js).await.unwrap();
    ensure_bucket(&js).await.unwrap();
    bus::ensure_owners(&js).await.unwrap();
    let channels = bus::ensure_channels(&js).await.unwrap();
    let mut ops = ChannelEntry::new("ops".parse().unwrap());
    ops.add_member(p("dev-a"));
    ops.add_member(p("dev-b"));
    channels.put(ops.key(), serde_json::to_vec(&ops).unwrap().into()).await.unwrap();
    let secret = server.dir().join("hooks.secret");
    std::fs::write(&secret, "s3cret").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    Some((server, js, secret))
}

pub(crate) fn creds(server: &Server, webhook: &UserId, to: Destination) -> PathBuf {
    let out = server.dir().join(format!("{webhook}.creds"));
    issue_with(
        &server.dir().join("auth"),
        webhook,
        crate::revoke::Kind::Webhook,
        Permissions::for_webhook(webhook, &[to]),
        &out,
    )
    .unwrap();
    out
}

/// The messages stored on `subject`, oldest first.
pub(crate) async fn stored<T: serde::de::DeserializeOwned>(js: &jetstream::Context, subject: &str) -> Vec<T> {
    let stream = js.get_stream(STREAM).await.unwrap();
    let count: usize = stream
        .info_with_subjects(subject)
        .await
        .unwrap()
        .try_fold(0, |n, (_, c)| async move { Ok(n + c) })
        .await
        .unwrap();
    let mut found = Vec::new();
    let mut seq = 1;
    while found.len() < count {
        if let Ok(msg) = stream.get_raw_message(seq).await
            && msg.subject.as_str() == subject
        {
            found.push(serde_json::from_slice(&msg.payload).unwrap());
        }
        seq += 1;
    }
    found
}

#[tokio::test]
async fn an_event_reaches_its_ant_once_and_a_refused_one_nothing() {
    let Some((server, js, secret)) = hub().await else { return };
    let github = p("github");
    let creds = creds(&server, &github, Destination::Ant(p("dev-a")));
    let config = server.dir().join("webhooks.toml");
    std::fs::write(
        &config,
        format!(
            "[[webhook]]\nname = \"github\"\ncreds = {creds:?}\nauth = \"hmac-sha256\"\nsecret_file = {secret:?}\n\
                 id = \"header:X-GitHub-Delivery\"\nant = \"dev-a\"\nowner = \"alice\"\n\
                 text = \"{{header:X-GitHub-Event}} {{json:/action}}: {{body}}\"\n"
        ),
    )
    .unwrap();
    let (ingress, addr) = start(&config, &server.url).await;

    let body = br#"{"action":"opened","comment":"@dev-b drop the tables"}"#;
    let signature = signed(body);
    let event = [("X-GitHub-Delivery", "d1"), ("X-GitHub-Event", "issues"), ("X-Hub-Signature-256", &signature)];
    assert_eq!(request(addr, "POST", "/hooks/gitlab", &event, body).await.status, 404);
    assert_eq!(request(addr, "POST", "/github", &event, body).await.status, 404);
    let get = request(addr, "GET", "/hooks/github", &event, body).await;
    assert_eq!((get.status, get.headers.contains("allow: post")), (405, true));
    let wrong = signed(b"another body");
    for headers in [&event[..2], &[event[0], event[1], ("X-Hub-Signature-256", &wrong)]] {
        assert_eq!(request(addr, "POST", "/hooks/github", headers, body).await.status, 401);
    }

    let first = deliver(addr, "/hooks/github", &event, body).await;
    assert_eq!(first.status, 202, "{}", first.body);
    let thread = first.json()["thread"].as_str().unwrap().to_owned();
    assert_eq!(first.json()["duplicate"], false);
    let again = request(addr, "POST", "/hooks/github", &event, body).await;
    assert_eq!((again.status, again.json()["thread"].as_str()), (200, Some(thread.as_str())));
    assert_eq!(again.json()["duplicate"], true);
    let other = [("X-GitHub-Delivery", "d2"), ("X-Hub-Signature-256", &signed(b"ping"))];
    assert_eq!(request(addr, "POST", "/hooks/github", &other, b"ping").await.status, 400, "no JSON");
    let big = vec![b'x'; MAX_BODY + 1];
    let big_signed = [("X-GitHub-Delivery", "d3"), ("X-Hub-Signature-256", &signed(&big))];
    assert_eq!(request(addr, "POST", "/hooks/github", &big_signed, &big).await.status, 413);

    // A restarted ingress still knows the event, and the stream drops it while the bucket forgot it.
    ingress.abort();
    let (_ingress, addr) = start(&config, &server.url).await;
    let after = deliver(addr, "/hooks/github", &event, body).await;
    assert_eq!((after.status, after.json()["thread"].as_str()), (200, Some(thread.as_str())));
    let (_, own) = bus::connect(&server.url, Some(&creds), None, &github, "test").await.unwrap();
    let seen = DirectKv::new(&own, None, WEBHOOKS_BUCKET).await.unwrap();
    let hash = &HEXLOWER.encode(digest::digest(&digest::SHA256, b"d1").as_ref())[..ID_HASH_LEN];
    seen.delete(webhook_seen_key(&github, hash)).await.unwrap();
    let forgotten = request(addr, "POST", "/hooks/github", &event, body).await;
    assert_eq!((forgotten.status, forgotten.json()["duplicate"].as_bool()), (200, Some(true)));

    let messages: Vec<Envelope> = stored(&js, &subject::inbox(&p("dev-a"), &github)).await;
    let [message] = &messages[..] else { panic!("one message: {messages:?}") };
    assert_eq!((&message.from, &message.to, message.thread_id.as_str()), (&github, &p("dev-a"), thread.as_str()));
    assert_eq!((message.owner.as_ref(), message.webhook), (Some(&p("alice")), true));
    let recorded = owners::Owners::new(js.clone()).recorded(&message.thread_id, &message.to).await.unwrap();
    assert_eq!(recorded, Some(p("alice")), "ants' messages on the thread name its owner as recorded");
    assert_eq!(message.text, format!("issues opened: {}", std::str::from_utf8(body).unwrap()));
    let marker =
        seen.get(webhook_marker_key(&github)).await.unwrap().expect("daemons take its messages as a webhook's");
    let marker: ants_proto::Serving = serde_json::from_slice(&marker).unwrap();
    assert_eq!(marker, ants_proto::Serving::here(ants_daemon::roster::machine().await), "it publishes its version");
}

#[tokio::test]
async fn a_channel_webhook_mentions_nobody_the_listed_or_the_members() {
    let Some((server, js, secret)) = hub().await else { return };
    let ops: ChannelId = "ops".parse().unwrap();
    let metrics: SocketAddr = format!("127.0.0.1:{}", ants_test_support::free_port()).parse().unwrap();
    let mut config = format!("metrics = \"{metrics}\"\n");
    for (name, mentions) in [("news", ""), ("alerts", "mention = [\"dev-b\", \"carol\"]"), ("ci", "broadcast = true")] {
        let creds = creds(&server, &p(name), Destination::Channel(ops.clone()));
        let _ = write!(
            config,
            "[[webhook]]\nname = \"{name}\"\ncreds = {creds:?}\nauth = \"bearer\"\nsecret_file = {secret:?}\n\
                 channel = \"ops\"\ntext = \"{{body}}\"\n{mentions}\n"
        );
    }
    let path = server.dir().join("webhooks.toml");
    std::fs::write(&path, config).unwrap();
    let (_ingress, addr) = start(&path, &server.url).await;

    let token = [("Authorization", "Bearer s3cret")];
    assert_eq!(request(addr, "POST", "/hooks/news", &[("Authorization", "Bearer nope")], b"x").await.status, 401);
    assert_eq!(request(addr, "POST", "/hooks/news", &[], b"x").await.status, 401);
    for name in ["news", "alerts", "ci"] {
        let answer = deliver(addr, &format!("/hooks/{name}"), &token, b"@dev-a the build is red").await;
        assert_eq!(answer.status, 202, "{name}: {}", answer.body);
    }
    assert_eq!(request(metrics, "GET", "/healthz", &[], b"").await.status, 200);
    let exposition = request(metrics, "GET", "/metrics", &[], b"").await.body;
    for line in [
        "ants_nats_connected{user=\"ci\"} 1",
        "ants_webhook_events_total{webhook=\"news\",outcome=\"delivered\"} 1",
        "ants_webhook_events_total{webhook=\"news\",outcome=\"refused\"} 2",
        "ants_webhook_events_total{webhook=\"alerts\",outcome=\"refused\"} 0",
        "ants_webhook_requests_in_flight 0",
    ] {
        assert!(exposition.lines().any(|l| l == line), "{line} in:\n{exposition}");
    }
    let mentions = |name: &str| {
        let (js, subject) = (js.clone(), subject::chan(&ops, &p(name)));
        async move {
            let posts: Vec<ChannelPost> = stored(&js, &subject).await;
            let [post] = &posts[..] else { panic!("one post: {posts:?}") };
            assert!(post.webhook && post.text == "@dev-a the build is red" && post.owner.is_none());
            post.mentions.clone()
        }
    };
    assert!(mentions("news").await.is_empty(), "never parsed from the event");
    assert_eq!(mentions("alerts").await, [p("dev-b"), p("carol")]);
    assert_eq!(mentions("ci").await, [p("dev-a"), p("dev-b")]);
}

#[tokio::test]
async fn an_edited_config_is_served_on_the_same_listener_and_a_bad_one_is_not() {
    let Some((server, _, secret)) = hub().await else { return };
    let entry = |name: &str| {
        let creds = creds(&server, &p(name), Destination::Ant(p("dev-a")));
        format!(
            "[[webhook]]\nname = \"{name}\"\ncreds = {creds:?}\nauth = \"bearer\"\nsecret_file = {secret:?}\n\
                 ant = \"dev-a\"\n"
        )
    };
    let path = server.dir().join("webhooks.toml");
    std::fs::write(&path, entry("github")).unwrap();
    let (ingress, addr) = start(&path, &server.url).await;
    let token = [("Authorization", "Bearer s3cret")];
    assert_eq!(deliver(addr, "/hooks/github", &token, b"one").await.status, 202);
    assert_eq!(request(addr, "POST", "/hooks/gitlab", &token, b"two").await.status, 404);

    std::fs::write(&path, entry("gitlab")).unwrap();
    let mut answer = request(addr, "POST", "/hooks/gitlab", &token, b"two").await;
    for _ in 0..100 {
        if answer.status != 404 && answer.status != 503 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        answer = request(addr, "POST", "/hooks/gitlab", &token, b"two").await;
    }
    assert_eq!(answer.status, 202, "{}", answer.body);
    assert_eq!(request(addr, "POST", "/hooks/github", &token, b"one").await.status, 404, "removed");

    std::fs::write(&path, "[[webhook]]\nname = \"broken\"\n").unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(request(addr, "POST", "/hooks/gitlab", &token, b"three").await.status, 202, "still served");
    assert!(!ingress.is_finished());
}

#[tokio::test]
async fn without_the_hub_an_event_is_refused_for_a_retry() {
    let dir = ants_test_support::scratch_dir("webhook-down");
    let path = dir.join("webhooks.toml");
    let metrics: SocketAddr = format!("127.0.0.1:{}", ants_test_support::free_port()).parse().unwrap();
    let hook = format!("{HOOK}ant = \"dev-a\"\n").replace("secret_env = \"HOOK_SECRET\"", "secret_file = \"s\"");
    std::fs::write(&path, format!("metrics = \"{metrics}\"\n{hook}")).unwrap();
    std::fs::write(dir.join("s"), "s3cret").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(dir.join("s"), std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let port = ants_test_support::free_port();
    let (_ingress, addr) = start(&path, &format!("nats://127.0.0.1:{port}")).await;
    assert_eq!(request(addr, "POST", "/hooks/github", &[], b"x").await.status, 401, "checked first");
    let down = request(addr, "POST", "/hooks/github", &[("Authorization", "Bearer s3cret")], b"x").await;
    assert_eq!(down.status, 503);
    assert!(down.headers.contains(&format!("retry-after: {RETRY_AFTER}")), "{}", down.headers);
    let health = request(metrics, "GET", "/healthz", &[], b"").await;
    assert_eq!(health.status, 503, "{}", health.body);
    let exposition = request(metrics, "GET", "/metrics", &[], b"").await.body;
    for line in [
        "ants_nats_connected{user=\"github\"} 0",
        "ants_webhook_events_total{webhook=\"github\",outcome=\"failed\"} 1",
        "ants_webhook_events_total{webhook=\"github\",outcome=\"refused\"} 1",
    ] {
        assert!(exposition.lines().any(|l| l == line), "{line} in:\n{exposition}");
    }
}

/// A notification service on loopback that takes every `POST`, handing over its path and JSON body.
async fn sink() -> (SocketAddr, mpsc::UnboundedReceiver<(String, Value)>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let tx = tx.clone();
            let service = hyper::service::service_fn(move |request: Request<Incoming>| {
                let tx = tx.clone();
                async move {
                    let path = request.uri().path().to_owned();
                    let body = request.into_body().collect().await.unwrap().to_bytes();
                    let _ = tx.send((path, serde_json::from_slice(&body).unwrap()));
                    Ok::<_, Infallible>(reply(StatusCode::OK, "{}"))
                }
            });
            tokio::spawn(hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service));
        }
    });
    (addr, rx)
}

async fn next(posts: &mut mpsc::UnboundedReceiver<(String, Value)>) -> (String, Value) {
    tokio::time::timeout(Duration::from_secs(10), posts.recv()).await.expect("a notification").unwrap()
}

fn private(path: &Path, text: &str) {
    std::fs::write(path, text).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}

#[tokio::test]
async fn a_test_notice_goes_to_every_sink_and_each_failure_is_reported() {
    let (sink, mut posts) = sink().await;
    let gone = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gone_addr = gone.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = gone.accept().await {
            let mut head = [0; 4096];
            let _ = stream.read(&mut head).await;
            let answer = "HTTP/1.1 404 Not Found\r\ncontent-length: 10\r\nconnection: close\r\n\r\nno_service";
            let _ = stream.write_all(answer.as_bytes()).await;
        }
    });
    let dir = ants_test_support::scratch_dir("webhook-notify-test");
    private(&dir.join("bot"), "123:abc\n");
    private(&dir.join("slack"), &format!("http://{gone_addr}/services/T0/B0/x\n"));
    let path = dir.join("webhooks.toml");
    let config = format!(
        "[[human]]\nname = \"alice\"\napprovals = false\n\
             [[human.notify]]\nkind = \"ntfy\"\ntopic = \"alerts\"\nurl = \"http://{sink}\"\n\
             [[human.notify]]\nkind = \"telegram\"\nchat = \"42\"\nurl = \"http://{sink}\"\nsecret_file = \"bot\"\n\
             [[human.notify]]\nkind = \"slack\"\nsecret_file = \"slack\"\n"
    );
    std::fs::write(&path, config).unwrap();

    let config = Config::read(&path).unwrap();
    let results = notify::test(&config.humans[0], "laptop").await;
    let shown: Vec<(&str, String)> = results
        .iter()
        .map(|(sink, sent)| (sink.as_str(), sent.as_ref().map_or_else(ToString::to_string, |()| "ok".into())))
        .collect();
    assert_eq!(
        shown,
        [
            ("ntfy topic alerts", "ok".to_owned()),
            ("telegram chat 42", "ok".to_owned()),
            ("slack", "answered 404 Not Found: no_service".to_owned()),
        ]
    );
    let mut got = [next(&mut posts).await, next(&mut posts).await];
    got.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(got[0].0, "/", "ntfy");
    assert_eq!(got[0].1["title"], "ants test notice for alice");
    assert_eq!(got[1].0, "/bot123:abc/sendMessage");
    assert!(got[1].1["text"].as_str().unwrap().contains("on laptop"), "{}", got[1].1);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn a_human_is_notified_of_tool_requests_and_escalations_and_decides_on_a_link() {
    let Some((server, js, _)) = hub().await else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let creds = server.creds_as(&alice, ants_proto::Role::Human);
    let (action_secret, bot_token) = (server.dir().join("action.secret"), server.dir().join("bot.token"));
    private(&action_secret, "0123456789abcdef-secret");
    private(&bot_token, "123:abc");
    let (sink, mut posts) = sink().await;
    let config = server.dir().join("webhooks.toml");
    std::fs::write(
        &config,
        format!(
            "public_url = \"https://hub.example/hooks\"\naction_secret_file = {action_secret:?}\n\
                 [[human]]\nname = \"alice\"\ncreds = {creds:?}\n\
                 [[human.notify]]\nkind = \"ntfy\"\nurl = \"http://{sink}\"\ntopic = \"alice-ants\"\n\
                 [[human.notify]]\nkind = \"telegram\"\nurl = \"http://{sink}\"\nchat = \"42\"\nsecret_file = {bot_token:?}\n"
        ),
    )
    .unwrap();
    let (_ingress, addr) = start(&config, &server.url).await;
    let (_, own) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
    let stream = own.get_stream(STREAM).await.unwrap();
    let consumer = ants_proto::notify_consumer(&alice);
    for _ in 0..100 {
        if stream.consumer_info(&consumer).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let (_, ant) = server.connect(&dev_a).await;
    let tool = ants_proto::ToolCall {
        kind: "execute".to_owned(),
        title: "cargo publish".to_owned(),
        locations: Vec::new(),
        input: None,
    };
    let options = vec![ants_proto::Verdict::AllowOnce, ants_proto::Verdict::AllowAlways];
    let asked =
        ants_proto::ApprovalRequest::new(dev_a.clone(), ThreadId::generate(), tool, options, Duration::from_secs(600));
    ant.publish(asked.subject(), serde_json::to_vec(&asked).unwrap().into()).await.unwrap().await.unwrap();

    let mut sent = HashMap::new();
    for _ in 0..2 {
        let (path, body) = next(&mut posts).await;
        sent.insert(path, body);
    }
    let ntfy = &sent["/"];
    assert_eq!(
        (ntfy["topic"].as_str(), ntfy["title"].as_str()),
        (Some("alice-ants"), Some("dev-a asks to run a tool"))
    );
    let telegram = &sent["/bot123:abc/sendMessage"];
    assert_eq!(telegram["chat_id"], "42");
    let link = |i: usize| {
        let url = ntfy["actions"][i]["url"].as_str().unwrap();
        assert_eq!(telegram["reply_markup"]["inline_keyboard"][0][i]["url"].as_str(), Some(url));
        url.strip_prefix("https://hub.example").unwrap().to_owned()
    };
    let (approve, reject) = (link(0), link(2));
    assert!(approve.starts_with("/hooks/_approval?") && approve.contains("verdict=allow_once"), "{approve}");

    let shown = request(addr, "GET", &approve, &[], b"").await;
    assert_eq!(shown.status, 200, "{}", shown.body);
    assert!(shown.body.contains("cargo publish") && shown.body.contains("<form method=\"post\""), "{}", shown.body);
    let decisions = |js: jetstream::Context| {
        let subject = asked.decision_subject();
        async move { stored::<ants_proto::ApprovalDecision>(&js, &subject).await }
    };
    assert!(decisions(js.clone()).await.is_empty(), "showing decides nothing");
    let forged = approve.replace("verdict=allow_once", "verdict=allow_always");
    assert_eq!(request(addr, "POST", &forged, &[], b"").await.status, 403);
    assert_eq!(request(addr, "POST", "/hooks/_approval?ant=dev-a", &[], b"").await.status, 400);
    assert_eq!(request(addr, "PUT", &approve, &[], b"").await.status, 405);

    let approved = request(addr, "POST", &approve, &[], b"").await;
    assert_eq!(approved.status, 200, "{}", approved.body);
    let [decision] = &decisions(js.clone()).await[..] else { panic!("one decision") };
    assert_eq!((decision.request_id, decision.verdict), (asked.id, ants_proto::Verdict::AllowOnce));
    let late = request(addr, "POST", &reject, &[], b"").await;
    assert_eq!(late.status, 409, "the first answer stands: {}", late.body);
    assert_eq!(decisions(js.clone()).await.len(), 1);
    let settled = request(addr, "GET", &reject, &[], b"").await;
    assert!(settled.body.contains("Already answered") && !settled.body.contains("<form"), "{}", settled.body);

    let plain = Envelope::new(dev_a.clone(), alice.clone(), "done");
    ant.publish(plain.subject(), serde_json::to_vec(&plain).unwrap().into()).await.unwrap().await.unwrap();
    let replied =
        Envelope::new(alice.clone(), dev_a.clone(), "build it").reply(dev_a.clone(), "built", ants_proto::Role::Human);
    let replied = replied.unwrap();
    ant.publish(replied.subject(), serde_json::to_vec(&replied).unwrap().into()).await.unwrap().await.unwrap();
    let escalation =
        ants_proto::Escalation::new(dev_a.clone(), plain.thread_id.clone(), ants_proto::EscalationReason::Asked);
    let escalated = Envelope::new(dev_a.clone(), alice.clone(), "which database?").with_escalation(Some(escalation));
    ant.publish(escalated.subject(), serde_json::to_vec(&escalated).unwrap().into()).await.unwrap().await.unwrap();
    let (path, body) = next(&mut posts).await;
    assert_eq!(path, "/", "plain messages, and replies unless completions = true, are not notified");
    assert_eq!(body["title"], "dev-a escalates: asked on dev-a");
    assert!(body["message"].as_str().unwrap().starts_with("which database?"), "{body}");
    assert!(body.get("actions").is_none());

    question_is_pushed(&ant, &mut posts, Envelope::asking(plain.thread_id, dev_a, alice, which_database())).await;
}

fn which_database() -> ants_proto::Question {
    ants_proto::Question::new("Which database?", &["Postgres".to_owned(), "SQLite".to_owned()], true).unwrap()
}

/// Publish `asking` as `ant` and check that the ntfy sink gets its question.
async fn question_is_pushed(
    ant: &jetstream::Context,
    posts: &mut mpsc::UnboundedReceiver<(String, Value)>,
    asking: Envelope,
) {
    ant.publish(asking.subject(), serde_json::to_vec(&asking).unwrap().into()).await.unwrap().await.unwrap();
    let body = loop {
        // The escalation's telegram message may come first.
        let (path, body) = next(posts).await;
        if path == "/" {
            break body;
        }
    };
    assert_eq!(body["title"], "dev-a asks: Which database?");
    let message = body["message"].as_str().unwrap();
    assert!(
        message.contains("1. Postgres") && message.contains(&format!("ants answer {} NUMBER", asking.id)),
        "{body}"
    );
}

/// The topics of the next `n` notifications, with their titles, sorted.
async fn topics(posts: &mut mpsc::UnboundedReceiver<(String, Value)>, n: usize) -> Vec<(String, String)> {
    let mut got = Vec::new();
    for _ in 0..n {
        let (_, body) = next(posts).await;
        got.push((body["topic"].as_str().unwrap().to_owned(), body["title"].as_str().unwrap().to_owned()));
    }
    got.sort();
    got
}

#[tokio::test]
async fn a_device_following_what_a_notice_is_about_gets_no_push_and_the_others_do() {
    use crate::presence::{self, tests::device};
    use ants_proto::Following;

    let Some((server, _, _)) = hub().await else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let creds = server.creds_as(&alice, ants_proto::Role::Human);
    let (laptop, laptop_id) = device(&server, &alice, "laptop").await;
    let (_, phone_id) = device(&server, &alice, "phone").await;
    let (sink, mut posts) = sink().await;
    let config = server.dir().join("webhooks.toml");
    let target = |topic: &str, device: String| {
        format!("[[human.notify]]\nkind = \"ntfy\"\nurl = \"http://{sink}\"\ntopic = \"{topic}\"\n{device}")
    };
    std::fs::write(
        &config,
        format!(
            "[[human]]\nname = \"alice\"\ncreds = {creds:?}\n{}{}{}",
            target("laptop", format!("device = \"{laptop_id}\"\n")),
            target("phone", format!("device = \"{phone_id}\"\n")),
            target("any", String::new()),
        ),
    )
    .unwrap();
    let (_ingress, _) = start(&config, &server.url).await;
    let stream = laptop.get_stream(STREAM).await.unwrap();
    for _ in 0..100 {
        if stream.consumer_info(&ants_proto::notify_consumer(&alice)).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (_, ant) = server.connect(&dev_a).await;
    let escalate = async |text: &str| {
        let thread = ThreadId::generate();
        let escalation = ants_proto::Escalation::new(dev_a.clone(), thread, ants_proto::EscalationReason::Asked);
        let escalated = Envelope::new(dev_a.clone(), alice.clone(), text).with_escalation(Some(escalation));
        ant.publish(escalated.subject(), serde_json::to_vec(&escalated).unwrap().into()).await.unwrap().await.unwrap();
    };
    let ask = async || {
        let tool = ants_proto::ToolCall { kind: "execute".into(), title: "make".into(), ..Default::default() };
        let options = vec![ants_proto::Verdict::AllowOnce];
        let asked = ants_proto::ApprovalRequest::new(
            dev_a.clone(),
            ThreadId::generate(),
            tool,
            options,
            Duration::from_secs(600),
        );
        ant.publish(asked.subject(), serde_json::to_vec(&asked).unwrap().into()).await.unwrap().await.unwrap();
    };
    let pair = |topic: &str, title: &str| (topic.to_owned(), title.to_owned());
    let (escalated, asked) = ("dev-a escalates: asked on dev-a", "dev-a asks to run a tool");

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let following = presence::while_following(&laptop, None, &alice, Some(laptop_id), Following::Inbox, async move {
        _ = stop_rx.await;
    });
    let check = async {
        let present = presence::Present::new(&laptop, None).await.unwrap();
        for _ in 0..50 {
            if !present.following(&alice, [laptop_id], Following::Inbox).await.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        escalate("which database?").await;
        assert_eq!(
            topics(&mut posts, 2).await,
            [pair("any", escalated), pair("phone", escalated)],
            "the laptop shows the escalation in its inbox"
        );
        ask().await;
        assert_eq!(
            topics(&mut posts, 3).await,
            [pair("any", asked), pair("laptop", asked), pair("phone", asked)],
            "the laptop follows no tool requests"
        );
        stop_tx.send(()).unwrap();
    };
    tokio::join!(following, check);
    escalate("and now?").await;
    assert_eq!(
        topics(&mut posts, 3).await,
        [pair("any", escalated), pair("laptop", escalated), pair("phone", escalated)],
        "the laptop stopped following"
    );
}

#[tokio::test]
async fn held_notices_are_dropped_unread_and_undecided_and_each_reply_is_notified_once() {
    let Some((server, js, _)) = hub().await else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let creds = server.creds_as(&alice, ants_proto::Role::Human);
    let (sink, mut posts) = sink().await;
    let config = server.dir().join("webhooks.toml");
    std::fs::write(
        &config,
        format!(
            "[[human]]\nname = \"alice\"\ncreds = {creds:?}\ncompletions = true\nbypass = [\"escalations\"]\n\
                 [[human.notify]]\nkind = \"ntfy\"\nurl = \"http://{sink}\"\ntopic = \"alice\"\n"
        ),
    )
    .unwrap();
    // Two services listing alice, which share her consumer.
    let (_one, _) = start(&config, &server.url).await;
    let (_two, _) = start(&config, &server.url).await;
    let (_, own) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
    let stream = own.get_stream(STREAM).await.unwrap();
    for _ in 0..100 {
        if stream.consumer_info(&ants_proto::notify_consumer(&alice)).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (_, ant) = server.connect(&dev_a).await;
    let send = async |envelope: &Envelope| {
        ant.publish(envelope.subject(), serde_json::to_vec(envelope).unwrap().into()).await.unwrap().await.unwrap();
    };
    let reply = |thread: &ThreadId, text: &str| {
        let asked = Envelope::on_thread(thread.clone(), alice.clone(), dev_a.clone(), "go on");
        asked.reply(dev_a.clone(), text, ants_proto::Role::Human).unwrap()
    };
    let messages = async |posts: &mut mpsc::UnboundedReceiver<(String, Value)>, n: usize| {
        let mut got = Vec::new();
        for _ in 0..n {
            let (_, body) = next(posts).await;
            let first = body["message"].as_str().unwrap().lines().next().unwrap_or_default().to_owned();
            got.push((body["title"].as_str().unwrap().to_owned(), first));
        }
        got.sort();
        got
    };
    let replied = |text: &str| ("dev-a replied".to_owned(), text.to_owned());
    let hold = async |change: &(dyn Fn(&mut ants_proto::NotifyPrefs, chrono::DateTime<chrono::Utc>) + Sync)| {
        crate::quiet::update(&own, &alice, change).await.unwrap();
    };
    let (thread, muted) = (ThreadId::generate(), ThreadId::generate());

    send(&reply(&thread, "first")).await;
    send(&reply(&thread, "second")).await;
    assert_eq!(messages(&mut posts, 2).await, [replied("first"), replied("second")]);

    hold(&|prefs, now| prefs.snoozed_until = Some(now + chrono::TimeDelta::hours(1))).await;
    let tool = ants_proto::ToolCall { kind: "execute".into(), title: "make".into(), ..Default::default() };
    let options = vec![ants_proto::Verdict::AllowOnce];
    let asked =
        ants_proto::ApprovalRequest::new(dev_a.clone(), thread.clone(), tool, options, Duration::from_secs(600));
    ant.publish(asked.subject(), serde_json::to_vec(&asked).unwrap().into()).await.unwrap().await.unwrap();
    send(&reply(&thread, "while snoozed")).await;
    let escalation = ants_proto::Escalation::new(dev_a.clone(), thread.clone(), ants_proto::EscalationReason::Asked);
    send(&Envelope::new(dev_a.clone(), alice.clone(), "which database?").with_escalation(Some(escalation))).await;
    assert_eq!(
        messages(&mut posts, 1).await,
        [("dev-a escalates: asked on dev-a".to_owned(), "which database?".to_owned())],
        "escalations bypass the snooze"
    );

    hold(&|prefs, now| {
        prefs.snoozed_until = None;
        prefs.muted.insert(muted.clone(), now + chrono::TimeDelta::hours(1));
    })
    .await;
    send(&reply(&muted, "on the muted thread")).await;
    let escalation = ants_proto::Escalation::new(dev_a.clone(), muted.clone(), ants_proto::EscalationReason::Asked);
    send(&Envelope::new(dev_a.clone(), alice.clone(), "muted anyway").with_escalation(Some(escalation))).await;
    send(&reply(&thread, "elsewhere")).await;
    assert_eq!(messages(&mut posts, 1).await, [replied("elsewhere")], "a mute holds back escalations too");

    // A snooze whose deadline passed holds nothing back, with no one there to lift it.
    hold(&|prefs, now| prefs.snoozed_until = Some(now - chrono::TimeDelta::seconds(1))).await;
    send(&reply(&thread, "after the snooze")).await;
    assert_eq!(messages(&mut posts, 1).await, [replied("after the snooze")]);
    assert!(
        tokio::time::timeout(Duration::from_millis(500), posts.recv()).await.is_err(),
        "nothing sent twice, nothing held sent later"
    );

    assert!(stored::<ants_proto::ApprovalDecision>(&js, &asked.decision_subject()).await.is_empty(), "undecided");
    let seq = stream.get_last_raw_message_by_subject(&asked.subject()).await.unwrap().sequence;
    let state = crate::tools::state(&own, &stream, seq, &asked, chrono::Utc::now()).await.unwrap();
    assert_matches!(state, crate::tools::State::Pending, "pending");
    if let Ok(read) = js.get_key_value(ants_proto::READ_BUCKET).await {
        assert!(read.entry(alice.as_str()).await.unwrap().is_none(), "nothing marked read");
    }
}

#[tokio::test]
async fn one_digest_of_what_a_snooze_held_back_follows_its_end_across_services() {
    let Some((server, _, _)) = hub().await else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let creds = server.creds_as(&alice, ants_proto::Role::Human);
    let (sink, mut posts) = sink().await;
    let config = server.dir().join("webhooks.toml");
    std::fs::write(
        &config,
        format!(
            "[[human]]\nname = \"alice\"\ncreds = {creds:?}\ncompletions = true\ndigest = true\n\
                 [[human.notify]]\nkind = \"ntfy\"\nurl = \"http://{sink}\"\ntopic = \"alice\"\n"
        ),
    )
    .unwrap();
    let (_one, _) = start(&config, &server.url).await;
    let (_two, _) = start(&config, &server.url).await;
    let (_, own) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
    let stream = own.get_stream(STREAM).await.unwrap();
    for _ in 0..100 {
        if stream.consumer_info(&ants_proto::notify_consumer(&alice)).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (_, ant) = server.connect(&dev_a).await;
    let hold = async |change: &(dyn Fn(&mut ants_proto::NotifyPrefs, chrono::DateTime<chrono::Utc>) + Sync)| {
        crate::quiet::update(&own, &alice, change).await.unwrap();
    };
    let thread = ThreadId::generate();
    hold(&|prefs, now| prefs.snoozed_until = Some(now + chrono::TimeDelta::hours(1))).await;
    let tool = ants_proto::ToolCall { kind: "execute".into(), title: "make".into(), ..Default::default() };
    let asked = ants_proto::ApprovalRequest::new(
        dev_a.clone(),
        thread.clone(),
        tool,
        vec![ants_proto::Verdict::AllowOnce],
        Duration::from_secs(600),
    );
    ant.publish(asked.subject(), serde_json::to_vec(&asked).unwrap().into()).await.unwrap().await.unwrap();
    for text in ["one", "two"] {
        let question = Envelope::on_thread(thread.clone(), alice.clone(), dev_a.clone(), "go on");
        let reply = question.reply(dev_a.clone(), text, ants_proto::Role::Human).unwrap();
        ant.publish(reply.subject(), serde_json::to_vec(&reply).unwrap().into()).await.unwrap().await.unwrap();
    }

    let notify = own.get_key_value(ants_proto::NOTIFY_BUCKET).await.unwrap();
    let key = ants_proto::held_key(&alice);
    let mut counts = std::collections::BTreeMap::new();
    for _ in 0..100 {
        if let Some(value) = notify.get(&key).await.unwrap() {
            counts = serde_json::from_slice::<ants_proto::HeldNotices>(&value).unwrap().counts;
            if counts.values().sum::<u32>() == 3 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(counts, std::collections::BTreeMap::from([("approval".to_owned(), 1), ("completion".to_owned(), 2)]));
    assert!(tokio::time::timeout(Duration::from_millis(500), posts.recv()).await.is_err(), "held back while snoozed");

    hold(&|prefs, _| prefs.snoozed_until = None).await;
    let (_, digest) = next(&mut posts).await;
    assert_eq!(digest["title"], "3 notices held back");
    let text = digest["message"].as_str().unwrap();
    assert!(text.starts_with("1 tool request, 2 replies held back by quiet hours or a snooze since "), "{text}");
    assert!(text.contains(&format!("1 tool request still pending:\n{} make", crate::tools::qualified(&asked))));
    assert!(tokio::time::timeout(Duration::from_millis(1000), posts.recv()).await.is_err(), "one digest, sent once");
    assert!(notify.get(&key).await.unwrap().is_none(), "claimed");
    let seq = stream.get_last_raw_message_by_subject(&asked.subject()).await.unwrap().sequence;
    let state = crate::tools::state(&own, &stream, seq, &asked, chrono::Utc::now()).await.unwrap();
    assert_matches!(state, crate::tools::State::Pending, "still pending");
}
