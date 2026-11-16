#![allow(clippy::too_many_lines)]

use std::future::pending;

use ants_proto::{Machine, UserId};
use async_nats::Event;
use tokio::sync::mpsc;
use tokio::time::timeout;

use super::*;
use crate::members::EnrollApprover;
use crate::test_support::Server;

const WAIT: Duration = Duration::from_secs(5);
const REPEAT: Duration = Duration::from_millis(100);
const LISTEN: Duration = Duration::from_millis(400);

fn p(s: &str) -> UserId {
    s.parse().unwrap()
}

fn enrollment(server: &Server, ant: &UserId, ttl: Duration) -> std::path::PathBuf {
    let out = server.dir().join(format!("enroll-{ant}.creds"));
    auth::issue_enrollment(&server.dir().join("auth"), ant, ttl, &out).unwrap();
    out
}

fn enrollment_in(team: &Path, dir: &Path, ant: &UserId) -> std::path::PathBuf {
    let out = dir.join(format!("enroll-{ant}.creds"));
    auth::issue_enrollment(team, ant, Duration::from_secs(60), &out).unwrap();
    out
}

#[tokio::test]
async fn enrollment_credentials_ask_only_for_their_ant() {
    let Some(server) = Server::start() else { return };
    let creds = enrollment(&server, &p("dev-a"), Duration::from_secs(60));
    let (tx, mut errors) = mpsc::unbounded_channel();
    let client = async_nats::ConnectOptions::with_credentials_file(&creds)
        .await
        .unwrap()
        .event_callback(move |event| {
            let tx = tx.clone();
            async move {
                if let Event::ServerError(e) = event {
                    let _ = tx.send(e.to_string());
                }
            }
        })
        .connect(&server.url)
        .await
        .unwrap();
    let mut violation = async |what: &str| {
        client.flush().await.unwrap();
        let error = timeout(WAIT, errors.recv()).await.expect(what).unwrap();
        assert!(error.to_lowercase().contains("permissions violation"), "{what}: {error}");
    };
    client.publish("ants.enroll.dev-b", "{}".into()).await.unwrap();
    violation("asking for another ant").await;
    let _all = client.subscribe(subject::ALL_ENROLL_REPLIES).await.unwrap();
    violation("hearing the answers to others").await;
    let _inbox = client.subscribe("ants.inbox.dev-a").await.unwrap();
    violation("reading the ant's inbox").await;

    let other = enrollment(&server, &p("dev-b"), Duration::from_secs(60));
    let user = KeyPair::new_user();
    let err = request(&server.url, &other, &p("dev-a"), &user, None, REPEAT, pending()).await.unwrap_err();
    assert!(err.to_string().contains("not enroll-dev-a"), "{err:#}");
}

#[tokio::test]
async fn an_approval_completes_the_credentials_of_the_asking_key() {
    let Some(server) = Server::start() else { return };
    let (team, dev_a) = (server.dir().join("auth"), p("dev-a"));
    // With no expiry, as `--ttl 0` issues them.
    let creds = enrollment(&server, &dev_a, Duration::ZERO);
    let user = KeyPair::new_user();
    let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();

    let asking = request(&server.url, &creds, &dev_a, &user, None, REPEAT, pending());
    let approving = async {
        let heard = approver.pending(None).await.unwrap();
        assert_eq!(heard.len(), 1, "{heard:?}");
        assert_eq!((&heard[0].ant, &heard[0].public_key), (&dev_a, &user.public_key()));
        approver.approve(&team, &dev_a, None).await.unwrap()
    };
    let (jwt, approved) = Box::pin(timeout(WAIT * 2, async { tokio::join!(asking, approving) })).await.unwrap();
    assert_eq!(approved.public_key, user.public_key());
    assert!(approved.leaf.is_none());
    let approval = jwt.unwrap();
    assert_eq!(approval.leaf_jwt, None);
    let jwt = approval.jwt;

    let out = server.dir().join("dev-a.creds");
    auth::write_creds(&out, &jwt, &user).unwrap();
    let (client, _) = bus::connect(&server.url, Some(&out), None, &dev_a, "test").await.unwrap();
    client.flush().await.unwrap();
}

#[tokio::test]
async fn a_webhook_approval_gives_a_webhooks_credentials_for_its_destination() {
    let Some(server) = Server::start() else { return };
    let (team, hook) = (server.dir().join("auth"), p("github"));
    let creds = enrollment(&server, &hook, Duration::from_secs(60));
    let user = KeyPair::new_user();
    let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();
    let to = Destination::Ant(p("dev-a"));

    let asking = request(&server.url, &creds, &hook, &user, None, REPEAT, pending());
    let approving = approver.approve_webhook(&team, &hook, std::slice::from_ref(&to), None);
    let (jwt, key) = Box::pin(timeout(WAIT * 2, async { tokio::join!(asking, approving) })).await.unwrap();
    assert_eq!(key.unwrap(), user.public_key());
    let out = server.dir().join("github.creds");
    auth::write_creds(&out, &jwt.unwrap().jwt, &user).unwrap();
    crate::webhook::check_creds("webhook", &hook, Some(&out), &to).unwrap();
    let other = Destination::Ant(p("dev-b"));
    assert!(crate::webhook::check_creds("webhook", &hook, Some(&out), &other).is_err());
    let issued = revoke::issued(&team).unwrap();
    let record = issued.iter().find(|i| i.key == user.public_key()).unwrap();
    assert_eq!(record.kind, revoke::Kind::Webhook);
}

/// Publish `request` for dev-a with the enrollment credentials at `creds`, as a machine would, signed as given.
async fn forge(server: &Server, creds: &Path, request: &EnrollRequest) {
    let client = async_nats::ConnectOptions::with_credentials_file(creds).await.unwrap().connect(&server.url).await;
    let client = client.unwrap();
    for _ in 0..8 {
        client.publish(request.subject(), serde_json::to_vec(request).unwrap().into()).await.unwrap();
        client.flush().await.unwrap();
        tokio::time::sleep(REPEAT).await;
    }
}

#[tokio::test]
async fn requests_must_prove_their_enrollment_and_leaf_credentials() {
    let Some(server) = Server::start() else { return };
    let (team, dev_a) = (server.dir().join("auth"), p("dev-a"));
    let creds = enrollment(&server, &dev_a, Duration::from_secs(60));
    let (enrollment_jwt, enrollment_key) = auth::read_creds(&creds).unwrap();
    let m1: Machine = "m1".parse().unwrap();
    let leaf_creds = server.dir().join("leaf-m1.creds");
    auth::issue_leaf(&team, &m1, &[(p("dev-b"), Role::Ant)], &[], EnrollApprover::Without, None, &leaf_creds).unwrap();
    let (leaf_jwt, leaf_key) = auth::read_creds(&leaf_creds).unwrap();
    // An ant named like a leaf's uplink user, which may connect as a client only.
    let posing = server.dir().join("leaf-m1-ant.creds");
    auth::issue(&team, &p("leaf-m1"), Role::Ant, None, &posing).unwrap();
    let (posing_jwt, posing_key) = auth::read_creds(&posing).unwrap();
    let revoked = server.dir().join("leaf-m1-revoked.creds");
    auth::issue_leaf(&team, &m1, &[(p("dev-b"), Role::Ant)], &[], EnrollApprover::Without, None, &revoked).unwrap();
    let (revoked_jwt, revoked_key) = auth::read_creds(&revoked).unwrap();
    revoke::revoke(&team, None, &[revoked_key.public_key()]).unwrap();
    let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();

    let signed = |leaf: Option<(&str, &KeyPair)>, enrollment: &KeyPair| {
        let mut request = EnrollRequest::new(dev_a.clone(), KeyPair::new_user().public_key(), "host");
        request.enrollment = enrollment_jwt.clone();
        if let Some((jwt, leaf_key)) = leaf {
            request.leaf = Some(EnrollLeaf { machine: m1.clone(), jwt: jwt.to_owned(), signature: String::new() });
            let signature = auth::sign(leaf_key, &request.signed()).unwrap();
            request.leaf.as_mut().unwrap().signature = signature;
        }
        request.signature = auth::sign(enrollment, &request.signed()).unwrap();
        request
    };
    let stranger = KeyPair::new_user();
    let good = signed(Some((&leaf_jwt, &leaf_key)), &enrollment_key);
    let mut unsigned = good.clone();
    unsigned.signature = String::new();
    let mut moved = good.clone();
    moved.leaf.as_mut().unwrap().machine = "m2".parse().unwrap();
    let with_host = |signer: &KeyPair| {
        let mut request = good.clone();
        let public_key = KeyPair::new_user().public_key();
        request.host_creds = Some(EnrollHost { machine: m1.clone(), public_key, signature: String::new() });
        request.host_creds.as_mut().unwrap().signature = auth::sign(signer, &request.host_signed().unwrap()).unwrap();
        request
    };
    let host_by_stranger = with_host(&stranger);
    let mut host_moved = with_host(&enrollment_key);
    host_moved.host_creds.as_mut().unwrap().machine = "m2".parse().unwrap();
    let forged = [
        signed(None, &stranger),
        unsigned,
        moved,
        signed(Some((&leaf_jwt, &stranger)), &enrollment_key),
        signed(Some((&posing_jwt, &posing_key)), &enrollment_key),
        signed(Some((&revoked_jwt, &revoked_key)), &enrollment_key),
        host_by_stranger,
        host_moved,
    ];
    let sending = async {
        for request in forged.iter().chain([&good]) {
            forge(&server, &creds, request).await;
        }
    };
    let listening = async {
        let mut heard = Vec::new();
        while !heard.contains(&good) {
            heard.extend(approver.pending(Some(&dev_a)).await.unwrap());
        }
        heard
    };
    let ((), heard) = Box::pin(timeout(WAIT * 4, async { tokio::join!(sending, listening) })).await.unwrap();
    assert!(heard.contains(&good), "the proven request is heard");
    assert!(heard.iter().all(|r| *r == good), "{heard:#?}");

    let approving = approver.approve(&team, &dev_a, None);
    let ((), approved) =
        Box::pin(timeout(WAIT * 2, async { tokio::join!(forge(&server, &creds, &good), approving) })).await.unwrap();
    let approved = approved.unwrap_or_else(|e| panic!("{e:#}"));
    let leaf = approved.leaf.unwrap();
    assert_eq!(leaf.machine, m1);
    assert!(!leaf.outbox, "no outbox of m1 to admit dev-a from");
}

/// A machine asking for its host credentials with an ant gets them once a human approves, unless they would be another
/// machine's: the team holding live ones of the machine for another key, the machine running a leaf node the request did
/// not come through, or the request coming through another machine's leaf node. A refusal still approves the ant.
#[tokio::test]
async fn an_approval_grants_the_host_credentials_of_the_asking_machine_only() {
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();
    let m = |s: &str| s.parse::<Machine>().unwrap();
    let m3_uplink = server.dir().join("leaf-m3.creds");
    let members = [(p("dev-z"), Role::Ant)];
    auth::issue_leaf(&team, &m("m3"), &members, &[], EnrollApprover::Without, None, &m3_uplink).unwrap();
    let ask = async |ant: &str, machine: &str, leaf: Option<&Leaf>, grant: bool| {
        let ant = p(ant);
        let creds = enrollment(&server, &ant, Duration::from_secs(60));
        let (user, host) = (KeyPair::new_user(), HostAsk { machine: m(machine), key: KeyPair::new_user() });
        let asking = request_with(&server.url, &creds, &ant, &user, leaf, Some(&host), REPEAT, pending());
        let approving = approver.approve_with(&team, &ant, None, grant);
        let (approval, approved) =
            Box::pin(timeout(WAIT * 2, async { tokio::join!(asking, approving) })).await.unwrap();
        let (approval, approved) = (approval.unwrap(), approved.unwrap_or_else(|e| panic!("{e:#}")));
        assert_eq!(approved.public_key, user.public_key(), "the ant is approved");
        (approval.host_jwt, approved.host.unwrap(), host.key.public_key())
    };
    let refused = |host: HostApproved, machine: &str| match host {
        HostApproved::Refused(m, why) if m.as_str() == machine => why,
        other => panic!("{other:?}"),
    };

    let (jwt, host, key) = ask("dev-a", "m1", None, true).await;
    assert_eq!(host, HostApproved::Granted(m("m1")));
    let claims = auth::verified(&jwt.unwrap()).unwrap();
    assert_eq!((claims.name.as_str(), claims.sub.as_str()), ("host-m1", key.as_str()));
    let issued = revoke::issued(&team).unwrap();
    assert!(issued.iter().any(|i| i.key == key && i.kind == Kind::Host && i.name == "host-m1"), "{issued:?}");

    let (jwt, host, _) = ask("dev-b", "m1", None, true).await;
    let why = refused(host, "m1");
    assert!(jwt.is_none() && why.contains(&format!("live host credentials of m1 (key {key})")), "{why}");
    let (jwt, host, _) = ask("dev-c", "m2", None, false).await;
    assert!(jwt.is_none() && refused(host, "m2").contains("--no-host"));
    let (jwt, host, _) = ask("dev-d", "m3", None, true).await;
    let why = refused(host, "m3");
    assert!(jwt.is_none() && why.contains("m3 runs a leaf node"), "{why}");
    let m3 = Leaf { machine: m("m3"), creds: m3_uplink };
    let (jwt, host, _) = ask("dev-e", "m4", Some(&m3), true).await;
    let why = refused(host, "m4");
    assert!(jwt.is_none() && why.contains("those of m4 through the leaf node of m3"), "{why}");
    let (jwt, host, key) = ask("dev-f", "m3", Some(&m3), true).await;
    assert_eq!(host, HostApproved::Granted(m("m3")), "through its own leaf node");
    assert_eq!(auth::verified(&jwt.unwrap()).unwrap().sub, key);
}

/// A real hub and two leaf nodes: an ant new to m1 asks through m1's uplink, `ants enroll` approves it through
/// m2's (issued with `--enroll-approver`), and once m1's leaf restarts with the renewed uplink credentials, the
/// ant reaches the hub and what it keeps in m1's outbox reaches the hub's stream.
#[tokio::test]
async fn an_ant_new_to_a_leaf_enrolls_through_it_and_reaches_the_hub() {
    use ants_proto::{JsDomain, STREAM, ThreadId};
    use ants_test_support::{NatsServer, free_port};

    use crate::leaf;
    use crate::test_support::{eventually, spawn_hub};

    let Some(hub) = spawn_hub(free_port(), "feature_flags { js_api_reply_v2: true }") else { return };
    let leaf_port = {
        let conf = std::fs::read_to_string(hub.dir().join("nats.conf")).unwrap();
        let at = conf.find("leafnodes { listen: \"127.0.0.1:").unwrap() + "leafnodes { listen: \"127.0.0.1:".len();
        conf[at..].split('"').next().unwrap().to_owned()
    };
    let team = hub.dir().join("auth");
    let domain: JsDomain = "hub".parse().unwrap();
    let (human, dev_a, dev_c) = (UserId::human(), p("dev-a"), p("dev-c"));
    let creds = |user: &UserId| {
        let out = hub.dir().join(format!("{user}.creds"));
        auth::issue(&team, user, Role::by_default(user), Some(&domain), &out).unwrap();
        out
    };
    let (h, h_js) = bus::connect(&hub.url, Some(&creds(&human)), Some(&domain), &human, "test").await.unwrap();
    bus::ensure_stream(&h_js).await.unwrap();
    bus::ensure_links(&h_js).await.unwrap();

    let spawn_leaf = |machine: &Machine, members: &[(UserId, Role)], approver: EnrollApprover| {
        // Its configuration, outbox and uplink credentials outlive each of its runs.
        let dir = hub.dir().join(machine.as_str());
        std::fs::create_dir_all(&dir).unwrap();
        let leaf_creds = dir.join("uplink.creds");
        if !leaf_creds.exists() {
            auth::issue_leaf(&team, machine, members, &[], approver, Some(&domain), &leaf_creds).unwrap();
        }
        let uplinks = [format!("nats-leaf://127.0.0.1:{leaf_port}")];
        let listen = format!("127.0.0.1:{}", free_port());
        let config = leaf::Leaf {
            machine,
            hub_domain: Some(&domain),
            uplinks: &uplinks,
            creds: &leaf_creds,
            system_creds: None,
            server_conf: &team.join(auth::SERVER_CONF),
            ca_file: None,
            proxy: None,
            listen: &listen,
        };
        leaf::write(&config, &dir.join("leaf.conf")).unwrap();
        let hub_dir = hub.dir().file_name().unwrap().to_string_lossy().into_owned();
        let node = NatsServer::spawn_with(|_| format!("include \"../{hub_dir}/{machine}/leaf.conf\"\n")).unwrap();
        (node, dir.join("leaf.conf"))
    };
    let (m1, m2): (Machine, Machine) = ("m1".parse().unwrap(), "m2".parse().unwrap());
    let on_m1 = [(dev_a.clone(), Role::Ant), (human.clone(), Role::Human)];
    let (m1_node, m1_conf) = spawn_leaf(&m1, &on_m1, EnrollApprover::Without);
    let (m2_node, _) = spawn_leaf(&m2, &[(human.clone(), Role::Human)], EnrollApprover::With);
    let (_a, a_js) = bus::connect(&m1_node.url, Some(&creds(&dev_a)), Some(&domain), &dev_a, "test").await.unwrap();
    eventually("m1 reaches the hub", async || bus::open_stream(&a_js, &dev_a).await.is_ok()).await;
    let setup = crate::outbox::connect(&hub.url, Some(&creds(&human)), &human, &m1).await.unwrap();
    crate::outbox::admit(&setup, &h_js, &m1, &on_m1).await.unwrap();

    let enroll_creds = enrollment_in(&team, hub.dir(), &dev_c);
    let (machine, uplink_creds) = leaf::read_uplink(&m1_conf).unwrap();
    let m1_leaf = Leaf { machine, creds: uplink_creds.clone() };
    let user = KeyPair::new_user();
    let approver = Approver::connect(&m2_node.url, &team, Some(&domain), Duration::from_millis(1500)).await.unwrap();
    let asking = request(&m1_node.url, &enroll_creds, &dev_c, &user, Some(&m1_leaf), REPEAT, pending());
    let approving = async {
        loop {
            match approver.approve(&team, &dev_c, None).await {
                Ok(approved) => break approved,
                Err(e) if e.to_string().contains("no enrollment request") => continue,
                Err(e) => panic!("{e:#}"),
            }
        }
    };
    let (approval, approved) = Box::pin(timeout(WAIT * 6, async { tokio::join!(asking, approving) })).await.unwrap();
    let approval = approval.unwrap();
    let leaf_approved = approved.leaf.unwrap();
    assert_eq!(leaf_approved.machine, m1);
    assert!(leaf_approved.outbox, "the hub sources m1's outbox");
    let kept = members::issued(&team, &m1).unwrap().unwrap();
    assert_eq!(kept.ants, [dev_a.clone(), dev_c.clone()], "a later `ants auth leaf m1` keeps dev-c");

    let (old, key) = auth::read_creds(&uplink_creds).unwrap();
    let renewed = approval.leaf_jwt.unwrap();
    assert!(auth::allows_all(&renewed, &old).unwrap() && !auth::allows_all(&old, &renewed).unwrap());
    let c_creds = hub.dir().join("dev-c-enrolled.creds");
    auth::write_creds(&c_creds, &approval.jwt, &user).unwrap();
    let (_c, c_js) = bus::connect(&m1_node.url, Some(&c_creds), Some(&domain), &dev_c, "test").await.unwrap();
    assert!(bus::open_stream(&c_js, &dev_c).await.is_err(), "the uplink does not carry dev-c yet");
    auth::write_creds(&uplink_creds, &renewed, &key).unwrap();
    let carried = members::uplink(&m1_conf, &m1).unwrap();
    assert!(carried.ants.contains(&dev_c), "a later `ants leaf outbox m1` admits dev-c: {carried}");
    drop((_a, _c));
    drop(m1_node);
    let (m1_node, _) = spawn_leaf(&m1, &on_m1, EnrollApprover::Without);

    let (_c, c_js) = bus::connect(&m1_node.url, Some(&c_creds), Some(&domain), &dev_c, "test").await.unwrap();
    eventually("dev-c reaches the hub", async || bus::open_stream(&c_js, &dev_c).await.is_ok()).await;
    let direct = ants_proto::Envelope::new(dev_c.clone(), human.clone(), "hello");
    assert_eq!(bus::publish(&c_js, &direct).await.unwrap(), bus::Stored::Hub);
    // Its notes kept in m1's outbox reach the hub's stream: only its own admission takes them.
    let thread: ThreadId = "t1".parse().unwrap();
    let post = ants_proto::NotesPost::new(thread.clone(), dev_c.clone(), "kept");
    let kept = subject::outbox(&post.subject()).unwrap();
    c_js.publish(kept, serde_json::to_vec(&post).unwrap().into()).await.unwrap().await.unwrap();
    let notes = subject::notes(&thread, &dev_c);
    eventually("the outbox's notes reach the hub", async || {
        let stream = h_js.get_stream(STREAM).await.unwrap();
        stream.get_last_raw_message_by_subject(&notes).await.is_ok()
    })
    .await;
    drop(h);
}

#[tokio::test]
async fn a_rejection_or_the_expiry_ends_the_request() {
    let Some(server) = Server::start() else { return };
    let (team, dev_a) = (server.dir().join("auth"), p("dev-a"));
    let creds = enrollment(&server, &dev_a, Duration::from_secs(60));
    let (first, second) = (KeyPair::new_user(), KeyPair::new_user());
    let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();

    let both = async {
        let first = request(&server.url, &creds, &dev_a, &first, None, REPEAT, pending());
        let second = request(&server.url, &creds, &dev_a, &second, None, REPEAT, pending());
        tokio::join!(first, second)
    };
    let answering = async {
        let err = approver.approve(&team, &dev_a, None).await.unwrap_err().to_string();
        assert!(err.contains("several machines ask for dev-a"), "{err}");
        approver.reject(&dev_a, None, Some("unknown machine")).await.unwrap()
    };
    let ((first_err, second_err), rejected) =
        Box::pin(timeout(WAIT * 2, async { tokio::join!(both, answering) })).await.unwrap();
    assert_eq!(rejected.len(), 2);
    for err in [first_err.unwrap_err(), second_err.unwrap_err()] {
        assert!(err.to_string().contains("rejected: unknown machine"), "{err:#}");
    }

    let err = approver.approve(&team, &dev_a, None).await.unwrap_err().to_string();
    assert!(err.contains("no enrollment request for dev-a"), "{err}");
    let short = enrollment(&server, &dev_a, Duration::from_secs(1));
    let asking = request(&server.url, &short, &dev_a, &first, None, REPEAT, pending());
    let err = timeout(WAIT, asking).await.expect("the request outlives its enrollment credentials").unwrap_err();
    assert!(format!("{err:#}").contains("expire"), "{err:#}");
}

/// With the server gone, a flush waits forever for a reconnect: the expiry and `stop` still end a request.
#[tokio::test]
async fn the_expiry_or_a_stop_ends_a_request_whose_server_is_gone() {
    let Some(server) = Server::start() else { return };
    let (team, url, dev_a, dev_b) = (server.dir().join("auth"), server.url.clone(), p("dev-a"), p("dev-b"));
    let expiring = enrollment(&server, &dev_a, Duration::from_secs(3));
    let lasting = enrollment(&server, &dev_b, Duration::ZERO);
    let approver = Approver::connect(&url, &team, None, LISTEN).await.unwrap();
    let (a, b) = (KeyPair::new_user(), KeyPair::new_user());
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();

    let first = request(&url, &expiring, &dev_a, &a, None, REPEAT, pending());
    let second = request(&url, &lasting, &dev_b, &b, None, REPEAT, async {
        let _ = stop_rx.await;
    });
    let stopping = async {
        while approver.pending(None).await.unwrap().len() < 2 {}
        drop(server);
        // Long enough for both requests to repeat into a flush that waits.
        tokio::time::sleep(REPEAT * 3).await;
        stop_tx.send(()).unwrap();
    };
    let (first, second, ()) =
        Box::pin(timeout(WAIT, async { tokio::join!(first, second, stopping) })).await.expect("a request hangs");
    let (first, second) = (format!("{:#}", first.unwrap_err()), format!("{:#}", second.unwrap_err()));
    assert!(first.contains("expired without an answer"), "{first}");
    assert!(second.contains("stopped before an answer came"), "{second}");
}

/// The server ending the connection of revoked enrollment credentials and refusing every reconnect ends a request.
#[tokio::test]
async fn a_refusal_of_the_enrollment_credentials_ends_the_request() {
    let Some(server) = Server::start() else { return };
    let (team, dev_a) = (server.dir().join("auth"), p("dev-a"));
    let creds = enrollment(&server, &dev_a, Duration::from_secs(60));
    let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();
    let user = KeyPair::new_user();

    let asking = request(&server.url, &creds, &dev_a, &user, None, REPEAT, pending());
    let revoking = async {
        while approver.pending(None).await.unwrap().is_empty() {}
        let (_, key) = auth::read_creds(&creds).unwrap();
        revoke::revoke(&team, None, &[key.public_key()]).unwrap();
        let reload = std::process::Command::new("kill").arg("-HUP").arg(server.pid().to_string()).status();
        assert!(reload.unwrap().success());
    };
    let (refused, ()) =
        Box::pin(timeout(WAIT, async { tokio::join!(asking, revoking) })).await.expect("the request goes on");
    let err = format!("{:#}", refused.unwrap_err());
    assert!(err.contains("the NATS server refused the enrollment credentials"), "{err}");
}

/// Behind a leaf node whose uplink the hub would refuse with one more ant, an approval fails with the reason before
/// anything is signed, recorded or answered.
#[tokio::test]
async fn an_ant_the_uplink_has_no_room_for_is_neither_signed_nor_recorded() {
    let Some(server) = Server::start() else { return };
    // As long a name as that of the first ant the uplink had no room for.
    let (team, dev_x) = (server.dir().join("auth"), p("dev-xx"));
    let creds = enrollment(&server, &dev_x, Duration::from_secs(60));
    let m1: Machine = "m1".parse().unwrap();
    let uplink = server.dir().join("leaf-m1.creds");
    crate::test_support::uplink_for_most_ants(&team, &m1, None, &uplink);
    let before = std::fs::read_to_string(&uplink).unwrap();
    let (members_before, issued_before) = (members::issued(&team, &m1).unwrap(), revoke::issued(&team).unwrap().len());
    let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();
    let (leaf, user) = (Leaf { machine: m1.clone(), creds: uplink.clone() }, KeyPair::new_user());
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();

    let asking = request(&server.url, &creds, &dev_x, &user, Some(&leaf), REPEAT, async {
        let _ = stop_rx.await;
    });
    let approving = async {
        while approver.pending(None).await.unwrap().is_empty() {}
        let refused = approver.approve(&team, &dev_x, None).await;
        // Long enough for an answer to arrive, had one gone out.
        tokio::time::sleep(REPEAT * 3).await;
        stop_tx.send(()).unwrap();
        refused
    };
    let (asked, refused) = Box::pin(timeout(WAIT * 2, async { tokio::join!(asking, approving) })).await.unwrap();
    let err = format!("{:#}", refused.unwrap_err());
    assert!(err.contains("the uplink of the leaf node m1 cannot carry dev-xx"), "{err}");
    assert!(err.contains("would take") && err.contains("another leaf node"), "{err}");
    let asked = format!("{:#}", asked.unwrap_err());
    assert!(asked.contains("stopped before an answer came"), "no answer: {asked}");
    let issued = revoke::issued(&team).unwrap();
    assert!(!issued.iter().any(|i| i.name == "dev-xx"), "dev-xx is not signed");
    assert_eq!(issued.len(), issued_before);
    assert_eq!(members::issued(&team, &m1).unwrap(), members_before, "dev-xx is not among m1's members");
    assert_eq!(std::fs::read_to_string(&uplink).unwrap(), before);
}
