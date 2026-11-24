//! A leaf node's uplink credentials: the rights they carry, their size, and what clients on the leaf node reach
//! with them.

use ants_test_support::{NatsServer, free_port};

use super::*;
use crate::test_support::uplink_for_most_ants;

#[test]
fn a_renewed_uplink_keeps_its_key_and_rights_and_a_revoked_one_is_not_renewed() {
    let dir = ants_test_support::scratch_dir("extend-leaf");
    let (team, out, m1) = (dir.join("auth"), dir.join("leaf-m1.creds"), "m1".parse::<Machine>().unwrap());
    init(&team).unwrap();
    issue_leaf(&team, &m1, &[(p("dev-a"), Role::Ant)], &[], EnrollApprover::Without, None, &out).unwrap();
    let (jwt, key) = read_creds(&out).unwrap();
    let renewed = extend_leaf(&team, &jwt, &m1, Permissions::for_leaf(&m1, [(&p("dev-c"), Role::Ant)])).unwrap();
    assert!(allows_all(&renewed, &jwt).unwrap() && !allows_all(&jwt, &renewed).unwrap());
    assert_eq!(check_leaf(&account_key(&team).unwrap(), &renewed, &m1).unwrap().sub, key.public_key());
    let none = || Permissions { publish: vec![], subscribe: vec![] };
    let ant = sign_user(&team, &p("leaf-m1"), Role::Ant, None, &KeyPair::new_user().public_key()).unwrap();
    assert!(extend_leaf(&team, &ant, &m1, none()).is_err(), "an ant named leaf-m1 is no leaf");
    revoke::revoke(&team, Some("leaf-m1"), &[]).unwrap();
    let err = extend_leaf(&team, &jwt, &m1, none()).unwrap_err().to_string();
    assert!(err.contains("revoked"), "{err}");
    std::fs::remove_dir_all(dir).unwrap();
}

/// An ant whose credentials make it a human's assistant takes the assistant's rights into a leaf node's uplink, issued
/// or renewed, and an ant's again once those credentials are revoked.
#[test]
fn a_leaf_uplink_carries_an_assistant_s_rights() {
    use ants_daemon::secrets::creds;
    let dir = ants_test_support::scratch_dir("leaf-assistant");
    let (team, out, m1) = (dir.join("auth"), dir.join("leaf-m1.creds"), "m1".parse::<Machine>().unwrap());
    init(&team).unwrap();
    let (alice, pa, dev_a) = (p("alice"), p("pa"), p("dev-a"));
    issue(&team, &alice, Role::Human, None, &dir.join("alice.creds")).unwrap();
    let new = Registered { assistant_of: Some(alice.clone()), ..Registered::default() };
    let rights = Permissions::for_assistant(&pa, &alice);
    Issue::new(&team, &pa, Kind::Assistant, rights).with(&new).into_file(&dir.join("pa.creds")).unwrap();
    issue_leaf(
        &team,
        &m1,
        &[(pa.clone(), Role::Ant), (dev_a.clone(), Role::Ant)],
        &[],
        EnrollApprover::Without,
        None,
        &out,
    )
    .unwrap();
    let sorted = |mut rights: Permissions| {
        for all in [&mut rights.publish, &mut rights.subscribe] {
            all.sort();
            all.dedup();
        }
        rights
    };
    let carried = |jwt: &str| sorted(allowed(&verified_body(jwt).unwrap().1));
    let as_assistant = [(&pa, LeafRole::AssistantOf(&alice)), (&dev_a, LeafRole::Ant)];
    let (jwt, _) = read_creds(&out).unwrap();
    assert_eq!(carried(&jwt), sorted(Permissions::for_leaf(&m1, as_assistant)));
    let marker = "$JS.API.DIRECT.GET.KV_ants_read.$KV.ants_read.alice.chan";
    assert!(carried(&jwt).publish.iter().any(|s| s == marker), "pa reads alice's read markers through the leaf");
    let finding = creds::Finding::from_jwt(&jwt).unwrap();
    let creds::Kind::Leaf { ants, humans, .. } = finding.kind else { panic!("{:?}", finding.kind) };
    assert_eq!((ants, humans), (vec![pa.clone(), dev_a.clone()], vec![]), "an assistant is an ant of the leaf");

    let renewed = renew(&team, &jwt, &KeyPair::new_user().public_key(), None).unwrap();
    assert_eq!(carried(&renewed.jwt), carried(&jwt));
    revoke::revoke(&team, Some("pa"), &[]).unwrap();
    let as_ant = renew(&team, &renewed.jwt, &KeyPair::new_user().public_key(), None).unwrap();
    assert_eq!(carried(&as_ant.jwt), sorted(Permissions::for_leaf(&m1, [(&pa, Role::Ant), (&dev_a, Role::Ant)])));
    std::fs::remove_dir_all(dir).unwrap();
}

/// A retired ant's rights leave the uplink, renewed for the same key, while those its other members, webhooks and
/// enrollment approver share with it stay.
#[test]
fn a_shrunk_uplink_drops_only_what_the_retired_ant_needed() {
    let dir = ants_test_support::scratch_dir("shrink-leaf");
    let (team, out, m1) = (dir.join("auth"), dir.join("leaf-m1.creds"), "m1".parse::<Machine>().unwrap());
    init(&team).unwrap();
    let hub: JsDomain = "hub".parse().unwrap();
    let hook = Permissions::for_webhook(&p("hook"), &[Destination::Ant(p("dev-a"))]);
    issue_with(&team, &p("hook"), Kind::Webhook, hook, &team.join("hook.creds")).unwrap();
    let on_m1 = members(&[p("dev-a")], &[p("alice")]);
    issue_leaf(&team, &m1, &on_m1, &[p("hook")], EnrollApprover::With, Some(&hub), &out).unwrap();
    let (jwt, key) = read_creds(&out).unwrap();
    let more = Permissions::for_leaf(&m1, [(&p("dev-c"), Role::Ant)]).with_uplink_js_domain(Some(&hub));
    let with_c = extend_leaf(&team, &jwt, &m1, more).unwrap();

    let without_c = shrink_leaf(&team, &with_c, &m1, &p("dev-c"), LeafRole::Ant).unwrap().unwrap();
    assert!(allows_all(&without_c, &jwt).unwrap() && allows_all(&jwt, &without_c).unwrap(), "as issued again");
    assert_eq!(check_leaf(&account_key(&team).unwrap(), &without_c, &m1).unwrap().sub, key.public_key());
    assert_eq!(shrink_leaf(&team, &without_c, &m1, &p("dev-c"), LeafRole::Ant).unwrap(), None, "nothing left");

    let without_a = shrink_leaf(&team, &jwt, &m1, &p("dev-a"), LeafRole::Ant).unwrap().unwrap();
    let finding = ants_daemon::secrets::creds::Finding::from_jwt(&without_a).unwrap();
    let ants_daemon::secrets::creds::Kind::Leaf { ants, humans, webhooks, enroll_approver, .. } = finding.kind else {
        panic!("{finding:?}")
    };
    assert!(ants.is_empty(), "{ants:?}");
    assert!(humans.contains(&p("alice")) && webhooks == [p("hook")] && enroll_approver, "{humans:?} {webhooks:?}");
    let (_, body) = verified_body(&without_a).unwrap();
    let publish = allowed(&body).publish;
    assert!(publish.iter().any(|s| s.starts_with("ants.inbox.dev-a.")), "the webhook's: {publish:?}");
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn uplink_credentials_a_hub_would_refuse_are_neither_issued_nor_renewed() {
    let dir = ants_test_support::scratch_dir("uplink-size");
    let (team, out, m1) = (dir.join("auth"), dir.join("leaf-m1.creds"), "m1".parse::<Machine>().unwrap());
    init(&team).unwrap();
    let domain: JsDomain = "hub".parse().unwrap();
    let (ants, err) = uplink_for_most_ants(&team, &m1, Some(&domain), &out);
    eprintln!("{} ants fit the uplink of m1 (domain {domain})", ants.len());
    assert!(ants.len() >= 44, "only {} ants fit", ants.len());
    assert!(err.contains("max_control_line 65536") && err.contains("another leaf node"), "{err}");
    let (jwt, _) = read_creds(&out).unwrap();
    assert!(jwt.len() <= MAX_UPLINK_JWT, "the refused credentials are not written");
    // As long a name as that of the ant refused.
    let more = Permissions::for_leaf(&m1, [(&p("dev-xx"), Role::Ant)]).with_uplink_js_domain(Some(&domain));
    let err = extend_leaf(&team, &jwt, &m1, more).unwrap_err().to_string();
    assert!(err.contains("the uplink credentials of m1 would take"), "{err}");
    std::fs::remove_dir_all(dir).unwrap();
}

/// Each ant added behind a leaf node (`request_ant`, enrollment) grows its uplink JWT by the same amount, about
/// 5.3 KB for a six-character name, 5.7 KB with the hub's JetStream domain, whose plain JetStream API and KV forms it
/// leaves out: no permission is shared between two ants, and none is a pattern matching an ant not yet added.
#[test]
fn each_ant_added_grows_the_uplink_by_the_same_measure() {
    let dir = ants_test_support::scratch_dir("uplink-growth");
    let (team, out, m1) = (dir.join("auth"), dir.join("leaf-m1.creds"), "m1".parse::<Machine>().unwrap());
    init(&team).unwrap();
    let hub: JsDomain = "hub".parse().unwrap();
    for (domain, least, most) in [(None, 4_800, 5_400), (Some(&hub), 5_200, 5_800)] {
        issue_leaf(&team, &m1, &members(&[p("dev-00")], &[]), &[], EnrollApprover::Without, domain, &out).unwrap();
        let (mut jwt, _) = read_creds(&out).unwrap();
        let mut growth = Vec::new();
        for i in 1..=5 {
            let more =
                Permissions::for_leaf(&m1, [(&p(&format!("dev-{i:02}")), Role::Ant)]).with_uplink_js_domain(domain);
            let renewed = extend_leaf(&team, &jwt, &m1, more).unwrap();
            growth.push(renewed.len() - jwt.len());
            jwt = renewed;
        }
        if domain.is_some() {
            let publish = allowed(&verified_body(&jwt).unwrap().1).publish;
            let plain = publish.iter().find(|s| s.starts_with("$JS.API.") || s.starts_with("$KV."));
            assert_eq!(plain, None, "the hub denies the plain forms on a leaf connection of another domain");
        }
        let (min, max) = (*growth.iter().min().unwrap(), *growth.iter().max().unwrap());
        eprintln!("uplink of m1 (domain {domain:?}): {} bytes, each ant adds {growth:?}", jwt.len());
        assert!(max - min <= 4, "ants of names of one length grow it alike: {growth:?}");
        assert!((least..=most).contains(&max), "{max} bytes per ant, domain {domain:?}");
        let room = (MAX_UPLINK_JWT - jwt.len()) / max + 6;
        assert!(room >= if domain.is_some() { 44 } else { 47 }, "{room} ants fit");
    }
    std::fs::remove_dir_all(dir).unwrap();
}

/// Each assistant added behind a leaf node grows its uplink JWT by the same amount, about 3.8 KB for six-character
/// names of it and its human, 4.2 KB with the hub's JetStream domain: less than an ant, as it holds none of an ant's
/// reader consumers. The first one adds about 2.4 KB more (2.7 KB), the human reads every assistant shares.
#[test]
fn each_assistant_added_grows_the_uplink_by_the_same_measure() {
    let dir = ants_test_support::scratch_dir("uplink-assistant-growth");
    let (team, out, m1) = (dir.join("auth"), dir.join("leaf-m1.creds"), "m1".parse::<Machine>().unwrap());
    init(&team).unwrap();
    let hub: JsDomain = "hub".parse().unwrap();
    for (domain, each, shared) in [(None, 3_600..=4_000, 2_200..=2_600), (Some(&hub), 4_000..=4_400, 2_500..=2_900)] {
        issue_leaf(&team, &m1, &members(&[p("dev-00")], &[]), &[], EnrollApprover::Without, domain, &out).unwrap();
        let (mut jwt, _) = read_creds(&out).unwrap();
        let mut growth = Vec::new();
        for i in 1..=5 {
            let (assistant, human) = (p(&format!("pa-{i:03}")), p(&format!("hu-{i:03}")));
            let more = Permissions::for_leaf(&m1, [(&assistant, LeafRole::AssistantOf(&human))]);
            let renewed = extend_leaf(&team, &jwt, &m1, more.with_uplink_js_domain(domain)).unwrap();
            growth.push(renewed.len() - jwt.len());
            jwt = renewed;
        }
        eprintln!("uplink of m1 (domain {domain:?}): {} bytes, each assistant adds {growth:?}", jwt.len());
        let (min, max) = (*growth[1..].iter().min().unwrap(), *growth[1..].iter().max().unwrap());
        assert!(max - min <= 4, "assistants of names of one length grow it alike: {growth:?}");
        assert!(each.contains(&max), "{max} bytes per assistant, domain {domain:?}");
        assert!(shared.contains(&(growth[0] - max)), "{} bytes more for the first, domain {domain:?}", growth[0]);
    }
    std::fs::remove_dir_all(dir).unwrap();
}

/// The longest uplink credentials `ants auth leaf` issues, far over the hub's max_control_line, still connect.
#[tokio::test]
async fn the_longest_uplink_credentials_connect() {
    let leaf_port = free_port();
    let Some(hub) = Server::start_with(
        "domain: hub",
        &format!("leafnodes {{ listen: \"127.0.0.1:{leaf_port}\"\nno_advertise: true }}\n"),
    ) else {
        return;
    };
    let (domain, human, m1) = ("hub".parse::<JsDomain>().unwrap(), UserId::human(), "m1".parse().unwrap());
    let team = hub.dir().join("auth");
    let (_h_client, mut h_js) =
        bus::connect(&hub.url, Some(&hub.creds_in(&human, Some(&domain))), Some(&domain), &human, "test")
            .await
            .unwrap();
    h_js.set_timeout(Duration::from_secs(1));
    bus::ensure_stream(&h_js).await.unwrap();
    let leaf_creds = hub.dir().join("leaf-m1.creds");
    let (ants, _) = uplink_for_most_ants(&team, &m1, Some(&domain), &leaf_creds);
    assert!(read_creds(&leaf_creds).unwrap().0.len() > MAX_UPLINK_JWT - 9 * 1024);
    let account = load(&team.join(ACCOUNT_SEED)).unwrap().public_key();
    let leaf = NatsServer::spawn_with(|dir| {
        fs::copy(team.join(SERVER_CONF), dir.join(SERVER_CONF)).unwrap();
        format!(
            "include \"{SERVER_CONF}\"\nleafnodes {{ remotes: [{{ url: \"nats-leaf://127.0.0.1:{leaf_port}\"\n\
                 account: {account}\ncredentials: {leaf_creds:?} }}] }}\n"
        )
    })
    .unwrap();
    let last = ants.last().unwrap();
    let js = connect_to(&leaf.url, &hub.creds_in(last, Some(&domain)), Some(&domain), last).await;
    let mut reached = false;
    for _ in 0..50 {
        if bus::open_stream(&js, last).await.is_ok() {
            reached = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(reached, "{last} on the leaf reaches the hub stream");
}

/// A hub with a JetStream domain, and a leaf node without JetStream whose uplink uses `ants auth leaf` credentials
/// for dev-a and the webhook github: dev-a reaches the hub's stream through the domain, and github delivers there
/// (to dev-b, elsewhere); dev-b and the webhook alerts, also valid on the leaf, get nothing across.
#[tokio::test]
#[expect(clippy::too_many_lines, reason = "a hub and its leaf node set up, then each client checked across")]
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
    bus::ensure_stream(&h_js).await.unwrap();
    bus::init_own_buckets(&h_js).await.unwrap();
    crate::test_support::webhook::ensure_bucket(&h_js).await.unwrap();

    let (github, alerts) = (p("github"), p("alerts"));
    for hook in [&github, &alerts] {
        let permissions = Permissions::for_webhook(hook, &[Destination::Ant(dev_b.clone())]);
        issue_with(
            &team,
            hook,
            crate::credentials::issued::Kind::Webhook,
            permissions.with_js_domain(Some(&domain)),
            &team.join(format!("{hook}.creds")),
        )
        .unwrap();
    }
    let leaf_creds = hub.dir().join("leaf-m1.creds");
    let m1: Machine = "m1".parse().unwrap();
    let on_m1 = members(std::slice::from_ref(&dev_a), &[]);
    let missing =
        issue_leaf(&team, &m1, &on_m1, &[p("ci")], EnrollApprover::Without, Some(&domain), &leaf_creds).unwrap_err();
    assert!(format!("{missing:#}").contains("ants auth user ci --webhook"), "{missing:#}");
    issue(&team, &p("dev-c"), Role::Ant, Some(&domain), &team.join("dev-c.creds")).unwrap();
    let ant =
        issue_leaf(&team, &m1, &on_m1, &[p("dev-c")], EnrollApprover::Without, Some(&domain), &leaf_creds).unwrap_err();
    assert!(format!("{ant:#}").contains("no webhook's"), "{ant:#}");
    issue_leaf(&team, &m1, &[], std::slice::from_ref(&github), EnrollApprover::Without, None, &leaf_creds).unwrap();
    issue_leaf(&team, &m1, &on_m1, std::slice::from_ref(&github), EnrollApprover::Without, Some(&domain), &leaf_creds)
        .unwrap();
    assert!(
        async_nats::ConnectOptions::with_credentials_file(&leaf_creds).await.unwrap().connect(&hub.url).await.is_err(),
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
        if let Ok(s) = bus::open_stream(&a_js, &dev_a).await {
            stream = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let stream = stream.expect("dev-a on the leaf reaches the hub stream");
    assert!(bus::open_stream(&a_plain, &dev_a).await.is_err(), "no JetStream without the domain");
    let usages = ants_daemon::monitoring::storage::measure(&a_js, Some(&domain), &Default::default()).await.unwrap();
    assert!(usages.iter().all(|u| !u.leaf && u.domain.as_deref() == Some("hub")), "{usages:?}");

    // github, as `ants webhook serve` on the leaf, keeps its events in the bucket and delivers to dev-b's inbox.
    let hook_js = connect_to(&leaf.url, &team.join("github.creds"), Some(&domain), &github).await;
    let seen = DirectKv::new(&hook_js, Some(&domain), ants_proto::WEBHOOKS_BUCKET).await.unwrap();
    let mut found = false;
    for _ in 0..50 {
        if seen.exists(&github).await.unwrap_or(false) {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(found, "github on the leaf reaches the hub's webhooks bucket");
    assert!(seen.create(ants_proto::webhook_seen_key(&github, "e1"), "{}").await.unwrap());
    let event = Envelope::new(github.clone(), dev_b.clone(), "build red").from_webhook();
    let stored = hook_js.publish(event.subject(), serde_json::to_vec(&event).unwrap().into()).await.unwrap();
    let stored = stored.await.unwrap().sequence;
    let on_hub = h_js.get_stream(STREAM).await.unwrap().get_raw_message(stored).await.unwrap();
    assert_eq!(serde_json::from_slice::<Envelope>(&on_hub.payload).unwrap(), event);
    let alerts_js = connect_to(&leaf.url, &team.join("alerts.creds"), Some(&domain), &alerts).await;
    let alerts_seen = DirectKv::new(&alerts_js, Some(&domain), ants_proto::WEBHOOKS_BUCKET).await.unwrap();
    assert!(!alerts_seen.exists(&alerts).await.unwrap_or(false), "alerts is not on the uplink");

    // dev-a serves its inbox from the leaf, keeps its session, and replies.
    let mut human_inbox = h_client.subscribe(subject::inbox_of(&human)).await.unwrap();
    h_client.flush().await.unwrap();
    let ask = Envelope::new(human.clone(), dev_a.clone(), "hi");
    bus::publish(&h_js, &ask).await.unwrap();
    let mut inbox = bus::inbox_consumer(&stream, &dev_a, WAIT, 5, 2).await.unwrap();
    let msg = timeout(WAIT, inbox.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(serde_json::from_slice::<Envelope>(&msg.payload).unwrap(), ask);
    let sessions = bus::ensure_sessions(&a_js, Some(&domain), &dev_a).await.unwrap();
    let key = session_key(&dev_a, &ask.thread_id);
    sessions.put(&key, "s1").await.unwrap();
    assert_eq!(sessions.get(&key).await.unwrap().as_deref(), Some(&b"s1"[..]));
    let attempts = bus::ensure_attempts(&a_js, Some(&domain), &dev_a).await.unwrap();
    let marker = attempt_key(&dev_a, msg.info().unwrap().stream_sequence);
    attempts.put(&marker, "started").await.unwrap();
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
    assert!(bus::open_stream(&b_js, &dev_b).await.is_err(), "dev-b's JetStream requests stay on the leaf");
    let (a_client, b_client) = (a_js.client(), b_js.client());
    let mut a_live = a_client.subscribe(subject::inbox_of(&dev_a)).await.unwrap();
    let mut b_live = b_client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
    let mut statuses = h_client.subscribe("ants.status.>").await.unwrap();
    // dev-a's round trip goes up the uplink behind its subscription and comes back behind the human's, so the hub and
    // the leaf know both before anyone publishes; dev-b's, which is to stay on the leaf, gets time to cross anyway.
    subscribed(&h_js, &human).await;
    subscribed(&a_js, &dev_a).await;
    b_client.flush().await.unwrap();
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

/// A human whose CLI goes through a leaf node decides approval requests, the signer's included, on which nothing on
/// the leaf subscribes: the hub's stream stores the decision, also once the hub took an update of the bus's account,
/// as a revocation pushes.
#[tokio::test]
async fn a_human_behind_a_leaf_node_stores_decisions_on_approval_requests() {
    let leaf_port = free_port();
    let Some(hub) = NatsServer::spawn_with(|dir| {
        init(&dir.join("auth")).unwrap();
        format!(
            "jetstream {{ store_dir: {:?}, domain: hub }}\ninclude \"auth/{SERVER_CONF}\"\n\
             resolver {{ type: full, dir: {:?} }}\nleafnodes {{ listen: \"127.0.0.1:{leaf_port}\", no_advertise: true }}\n",
            dir.join("js"),
            dir.join("jwt"),
        )
    }) else {
        return;
    };
    let (domain, alice, bob, m1) =
        ("hub".parse::<JsDomain>().unwrap(), p("alice"), p("bob"), "m1".parse::<Machine>().unwrap());
    let team = hub.dir().join("auth");
    let creds = |user: &UserId| {
        let out = hub.dir().join(format!("{user}.creds"));
        issue(&team, user, Role::Human, Some(&domain), &out).unwrap();
        out
    };
    let h_js = connect_to(&hub.url, &creds(&bob), Some(&domain), &bob).await;
    bus::ensure_stream(&h_js).await.unwrap();
    let leaf_creds = hub.dir().join("leaf-m1.creds");
    let on_m1 = members(&[], std::slice::from_ref(&alice));
    issue_leaf(&team, &m1, &on_m1, &[], EnrollApprover::Without, Some(&domain), &leaf_creds).unwrap();
    let (server_conf, listen) = (team.join(SERVER_CONF), format!("127.0.0.1:{}", free_port()));
    let system_creds = leaf_system_creds(&leaf_creds);
    let leaf = NatsServer::spawn_with(|dir| {
        let uplinks = [format!("nats-leaf://127.0.0.1:{leaf_port}")];
        let config = crate::node::leaf::Leaf {
            machine: &m1,
            hub_domain: Some(&domain),
            uplinks: &uplinks,
            creds: &leaf_creds,
            system_creds: Some(&system_creds),
            server_conf: &server_conf,
            ca_file: None,
            proxy: None,
            listen: &listen,
        };
        crate::node::leaf::write(&config, &dir.join("leaf.conf")).unwrap();
        "include \"leaf.conf\"\n".to_owned()
    })
    .unwrap();
    let js = connect_to(&leaf.url, &creds(&alice), Some(&domain), &alice).await;
    crate::test_support::eventually("alice on the leaf reaches the hub stream", async || {
        bus::open_stream(&js, &alice).await.is_ok()
    })
    .await;
    let stream = h_js.get_stream(STREAM).await.unwrap();
    let decide = async |asker: &UserId, when: &str| {
        let decision = subject::approval_decision(asker, &ants_proto::Uuid::now_v7());
        let stored = js.publish(decision.clone(), "deny".into()).await.unwrap().await;
        assert!(stored.is_ok(), "{when}, the hub's stream stores alice's decision on {decision}: {stored:?}");
        assert_eq!(&stream.get_last_raw_message_by_subject(&decision).await.unwrap().payload[..], b"deny");
    };
    decide(&UserId::signer(), "at first").await;
    creds(&p("carol"));
    crate::credentials::revoke::revoke(&team, Some("carol"), &[]).unwrap();
    crate::credentials::push::push(&hub.url, &team).await.unwrap();
    // The hub withdraws the interest and sends it again, once the uplink allows it.
    let probe = subject::approval_decision(&UserId::signer(), &ants_proto::Uuid::now_v7());
    ants_test_support::eventually_within(Duration::from_secs(5), "the interest crosses the uplink again", async || {
        js.publish(probe.clone(), "deny".into()).await.unwrap().await.is_ok()
    })
    .await;
    for asker in [UserId::signer(), p("dev-a")] {
        decide(&asker, "after an update of the account").await;
    }
}
