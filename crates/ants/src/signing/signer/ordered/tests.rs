//! Tests of enrollment requests carrying an order: the signer signs for the machine an order names only, and on a
//! leaf's machine renews the uplink to carry the ant.

use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_proto::{
    AdminOp, AdminOrder, EnrollHost, EnrollLeaf, EnrollReply, EnrollRequest, Machine, Permissions, Role, subject,
};
use ants_test_support::user as p;
use futures::StreamExt as _;
use nkeys::KeyPair;
use serde_json::json;

use crate::auth::{self, Issue, Registered, issue_host};
use crate::credentials::issued::Kind;
use crate::node::members::{self, EnrollApprover};
use crate::signing::signer::{Options, serve};
use crate::test_support::Server;

const WAIT: Duration = Duration::from_secs(10);
/// How long a request the signer ignores goes unanswered before the test stops waiting.
const IGNORED: Duration = Duration::from_millis(1500);

/// `op` about `ant` on `machine`, signed with the credentials `creds` of `admin`.
fn order(admin: &str, creds: &Path, machine: &str, ant: &str, op: AdminOp) -> AdminOrder {
    let (jwt, key) = auth::read_creds(creds).unwrap();
    let mut order = AdminOrder::new(p(admin), machine.parse().unwrap(), p(ant), op, jwt);
    order.signature = auth::sign(&key, &order.signed()).unwrap();
    order
}

fn add() -> AdminOp {
    AdminOp::Add { settings: serde_json::Map::new(), assistant_of: None }
}

/// What the signer answers the enrollment request of `ant` for `key` carrying `order`, signed with the credentials
/// `creds`, asked again every 200ms until `wait` ends: none when it ignores the request.
async fn ask(
    client: &async_nats::Client,
    creds: &Path,
    ant: &str,
    key: &KeyPair,
    order: &AdminOrder,
    wait: Duration,
) -> Option<EnrollReply> {
    ask_full(client, creds, ant, key, order, Alongside::default(), wait).await
}

/// [`ask`], asking for the host credentials of the machine `host` names with its key too.
async fn ask_with(
    client: &async_nats::Client,
    creds: &Path,
    ant: &str,
    key: &KeyPair,
    order: &AdminOrder,
    host: Option<(&str, &KeyPair)>,
    wait: Duration,
) -> Option<EnrollReply> {
    ask_full(client, creds, ant, key, order, Alongside { leaf: None, host }, wait).await
}

/// [`ask`], the request carrying the uplink credentials `leaf` of the leaf node of the machine they name, signed with
/// their key too.
async fn ask_through(
    client: &async_nats::Client,
    creds: &Path,
    leaf: Option<(&str, &Path)>,
    ant: &str,
    key: &KeyPair,
    order: &AdminOrder,
    wait: Duration,
) -> Option<EnrollReply> {
    ask_full(client, creds, ant, key, order, Alongside { leaf, host: None }, wait).await
}

/// What an ask goes along with: through the leaf node `leaf`, a machine and its uplink credentials, and asking for
/// the host credentials `host`, a machine and its key, either as given.
#[derive(Clone, Copy, Default)]
struct Alongside<'a> {
    leaf: Option<(&'a str, &'a Path)>,
    host: Option<(&'a str, &'a KeyPair)>,
}

/// [`ask`] with what goes `alongside` it.
async fn ask_full(
    client: &async_nats::Client,
    creds: &Path,
    ant: &str,
    key: &KeyPair,
    order: &AdminOrder,
    Alongside { leaf, host }: Alongside<'_>,
    wait: Duration,
) -> Option<EnrollReply> {
    let (jwt, signer) = auth::read_creds(creds).unwrap();
    let mut replies = client.subscribe(subject::enroll_reply(&p(ant))).await.unwrap();
    let mut request = EnrollRequest::new(p(ant), key.public_key(), "m1");
    request.enrollment = jwt;
    request.order = Some(Box::new(order.clone()));
    let leaf = leaf.map(|(machine, creds)| {
        let (jwt, key) = auth::read_creds(creds).unwrap();
        request.leaf = Some(EnrollLeaf { machine: machine.parse().unwrap(), jwt, signature: String::new() });
        key
    });
    let asking = async {
        loop {
            request.sent_at = chrono::Utc::now();
            if let Some((machine, host_key)) = host {
                let public_key = host_key.public_key();
                request.host_creds = Some(EnrollHost { machine: machine.parse().unwrap(), public_key });
            }
            request.signature = auth::sign(&signer, &request.signed()).unwrap();
            if let Some(key) = &leaf {
                let signature = auth::sign(key, &request.signed()).unwrap();
                request.leaf.as_mut().unwrap().signature = signature;
            }
            client.publish(request.subject(), serde_json::to_vec(&request).unwrap().into()).await.unwrap();
            let reply = tokio::time::timeout(Duration::from_millis(200), replies.next()).await;
            if let Ok(Some(msg)) = reply {
                let reply: EnrollReply = serde_json::from_slice(&msg.payload).unwrap();
                if reply.answers(&key.public_key()) {
                    return reply;
                }
            }
        }
    };
    tokio::time::timeout(wait, asking).await.ok()
}

fn refusal(reply: Option<EnrollReply>) -> String {
    match reply {
        Some(EnrollReply::Rejected { reason: Some(reason), .. }) => reason,
        other => panic!("not refused: {other:?}"),
    }
}

/// The signer signs a key on an admin's order to add the ant, or clone one as it, on the machine asking, with its live
/// host credentials, and that one key only; it refuses an order no admin signed, one altered after signing, one for another machine or
/// ant, and ignores a request not from a machine.
#[tokio::test]
async fn the_signer_signs_on_an_admin_s_order_for_the_machine_asking_only() {
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let (alice, bob) = (p("alice"), p("bob"));
    let admin_creds = server.dir().join("alice-admin.creds");
    let admin = Registered { label: Some("laptop"), admin: true, ..Registered::default() };
    let rights = Permissions::for_user(&alice, Role::Human);
    Issue::new(&team, &alice, Kind::Human, rights).with(&admin).into_file(&admin_creds).unwrap();
    let bob_creds = server.creds_as(&bob, Role::Human);
    let host_creds: PathBuf = server.dir().join("host-m1.creds");
    let m1: Machine = "m1".parse().unwrap();
    issue_host(&team, &m1, None, &host_creds).unwrap();
    let client = async_nats::ConnectOptions::with_credentials_file(&host_creds)
        .await
        .unwrap()
        .custom_inbox_prefix(ants_proto::host_inbox_prefix(&m1))
        .connect(&server.url)
        .await
        .unwrap();
    let config = server.dir().join("signer.toml");
    std::fs::write(&config, "").unwrap();
    let options =
        Options { config: &config, approval_timeout: WAIT, reconnect: WAIT * 3, tick: Duration::from_millis(200) };
    let serving = serve(&server.url, &team, None, &options, |_, _| {}, std::future::pending());

    let checks = async {
        let ok = order("alice", &admin_creds, "m1", "dev-c", add());
        let (first, second) = (KeyPair::new_user(), KeyPair::new_user());
        let Some(EnrollReply::Approved { jwt, .. }) = ask(&client, &host_creds, "dev-c", &first, &ok, WAIT).await
        else {
            panic!("an admin's order is signed")
        };
        let (claims, _) = ants_proto::verified_jwt(&jwt).unwrap();
        assert_eq!((claims.sub, claims.name.as_str()), (first.public_key(), "dev-c"));
        let reused = refusal(ask(&client, &host_creds, "dev-c", &second, &ok, WAIT).await);
        assert!(reused.contains("added dev-c with the key"), "{reused}");

        let no_admin = order("bob", &bob_creds, "m1", "dev-d", add());
        let refused = refusal(ask(&client, &host_creds, "dev-d", &second, &no_admin, WAIT).await);
        assert!(refused.contains("admin"), "{refused}");
        let mut forged = order("alice", &admin_creds, "m1", "dev-d", add());
        forged.op =
            AdminOp::Add { settings: json!({ "model": "o3" }).as_object().unwrap().clone(), assistant_of: None };
        let refused = refusal(ask(&client, &host_creds, "dev-d", &second, &forged, WAIT).await);
        assert!(refused.contains("not signed by"), "{refused}");
        let elsewhere = order("alice", &admin_creds, "m2", "dev-d", add());
        let refused = refusal(ask(&client, &host_creds, "dev-d", &second, &elsewhere, WAIT).await);
        assert!(refused.contains("for m2, not m1"), "{refused}");
        let other_ant = order("alice", &admin_creds, "m1", "dev-e", add());
        let refused = refusal(ask(&client, &host_creds, "dev-d", &second, &other_ant, WAIT).await);
        assert!(refused.contains("about dev-e, not dev-d"), "{refused}");
        let clone = AdminOp::Clone { new: p("dev-f"), settings: serde_json::Map::new() };
        let clone = order("alice", &admin_creds, "m1", "dev-c", clone);
        let refused = refusal(ask(&client, &host_creds, "dev-c", &second, &clone, WAIT).await);
        assert!(
            refused.contains("about dev-f, not dev-c"),
            "a clone order signs no key of the ant it clones: {refused}"
        );
        let third = KeyPair::new_user();
        let Some(EnrollReply::Approved { jwt, .. }) = ask(&client, &host_creds, "dev-f", &third, &clone, WAIT).await
        else {
            panic!("a clone order is signed for the clone")
        };
        assert_eq!(ants_proto::verified_jwt(&jwt).unwrap().0.name, "dev-f");
        let (host_too, host_key) = (order("alice", &admin_creds, "m1", "dev-d", add()), KeyPair::new_user());
        let with_host = ask_with(&client, &host_creds, "dev-d", &second, &host_too, Some(("m1", &host_key)), WAIT);
        let refused = refusal(with_host.await);
        assert!(refused.contains("grants no host credentials"), "{refused}");
        let unasked = order("alice", &admin_creds, "m1", "dev-d", AdminOp::Pause);
        let refused = refusal(ask(&client, &host_creds, "dev-d", &second, &unasked, WAIT).await);
        assert!(refused.contains("asks nothing of the signer"), "{refused}");

        let by_human = order("alice", &admin_creds, "m1", "dev-d", add());
        assert_eq!(ask(&client, &bob_creds, "dev-d", &second, &by_human, IGNORED).await, None, "no machine asks");
    };
    tokio::select! {
        r = serving => panic!("the signer stopped: {r:?}"),
        () = checks => {}
    }
}

/// On a machine running a leaf node, the signer signs an add order's key only for a request carrying the leaf's
/// uplink credentials, signed with their key too, and renews them to carry the ant, which it keeps among the leaf's
/// members; it refuses a request carrying none, or another machine's. A retire order's request revokes the ant's key
/// all the same without them, and with them has the signer renew them without the ant, which leaves the members.
#[tokio::test]
async fn on_a_leaf_node_s_machine_the_signer_renews_the_uplink_to_carry_the_ant_an_order_adds() {
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let alice = p("alice");
    let admin_creds = server.dir().join("alice-admin.creds");
    let admin = Registered { label: Some("laptop"), admin: true, ..Registered::default() };
    let rights = Permissions::for_user(&alice, Role::Human);
    Issue::new(&team, &alice, Kind::Human, rights).with(&admin).into_file(&admin_creds).unwrap();
    let host_creds: PathBuf = server.dir().join("host-m1.creds");
    let (m1, m2): (Machine, Machine) = ("m1".parse().unwrap(), "m2".parse().unwrap());
    issue_host(&team, &m1, None, &host_creds).unwrap();
    let on_leaf = auth::members(&[p("dev-a")], std::slice::from_ref(&alice));
    let (m1_uplink, m2_uplink) = (server.dir().join("leaf-m1.creds"), server.dir().join("leaf-m2.creds"));
    auth::issue_leaf(&team, &m1, &on_leaf, &[], EnrollApprover::Without, None, &m1_uplink).unwrap();
    auth::issue_leaf(&team, &m2, &on_leaf, &[], EnrollApprover::Without, None, &m2_uplink).unwrap();
    let client = async_nats::ConnectOptions::with_credentials_file(&host_creds)
        .await
        .unwrap()
        .custom_inbox_prefix(ants_proto::host_inbox_prefix(&m1))
        .connect(&server.url)
        .await
        .unwrap();
    let config = server.dir().join("signer.toml");
    std::fs::write(&config, "").unwrap();
    let options =
        Options { config: &config, approval_timeout: WAIT, reconnect: WAIT * 3, tick: Duration::from_millis(200) };
    let serving = serve(&server.url, &team, None, &options, |_, _| {}, std::future::pending());

    let checks = async {
        let key = KeyPair::new_user();
        let bare = order("alice", &admin_creds, "m1", "dev-c", add());
        let refused = refusal(ask(&client, &host_creds, "dev-c", &key, &bare, WAIT).await);
        assert!(refused.contains("m1 runs a leaf node"), "{refused}");
        let other = order("alice", &admin_creds, "m1", "dev-c", add());
        let through_m2 = Some(("m2", m2_uplink.as_path()));
        let refused = refusal(ask_through(&client, &host_creds, through_m2, "dev-c", &key, &other, WAIT).await);
        assert!(refused.contains("of m2, not of m1"), "{refused}");

        let ok = order("alice", &admin_creds, "m1", "dev-c", add());
        let through_m1 = Some(("m1", m1_uplink.as_path()));
        let reply = ask_through(&client, &host_creds, through_m1, "dev-c", &key, &ok, WAIT).await;
        let Some(EnrollReply::Approved { jwt, leaf_jwt: Some(renewed), .. }) = reply else {
            panic!("signed with the uplink renewed: {reply:?}")
        };
        let (claims, _) = ants_proto::verified_jwt(&jwt).unwrap();
        assert_eq!((claims.sub, claims.name.as_str()), (key.public_key(), "dev-c"));
        let (old, _) = auth::read_creds(&m1_uplink).unwrap();
        assert!(auth::allows_all(&renewed, &old).unwrap() && !auth::allows_all(&old, &renewed).unwrap());
        let kept = members::issued(&team, &m1).unwrap().unwrap();
        assert_eq!(kept.ants, [p("dev-a"), p("dev-c")], "a later `ants auth leaf m1` keeps dev-c");
        assert_eq!(members::issued(&team, &m2).unwrap().unwrap().ants, [p("dev-a")]);

        let (_, uplink_key) = auth::read_creds(&m1_uplink).unwrap();
        let seed = uplink_key.seed().unwrap();
        std::fs::write(&m1_uplink, ants_daemon::secrets::creds::text(&renewed, &seed)).unwrap();
        let bare = order("alice", &admin_creds, "m1", "dev-c", AdminOp::Retire);
        let reply = ask(&client, &host_creds, "dev-c", &key, &bare, WAIT).await;
        let Some(EnrollReply::Retired { revoked, leaf_jwt: None, leaf_error: Some(why), .. }) = reply else {
            panic!("retired, the uplink left as it is: {reply:?}")
        };
        assert_eq!(revoked, [key.public_key()]);
        assert!(why.contains("m1 runs a leaf node"), "{why}");
        assert_eq!(members::issued(&team, &m1).unwrap().unwrap().ants, [p("dev-a"), p("dev-c")]);

        let retire = order("alice", &admin_creds, "m1", "dev-c", AdminOp::Retire);
        // Skips late answers to the repeated requests before.
        let mut reply = None;
        for _ in 0..10 {
            reply = ask_through(&client, &host_creds, through_m1, "dev-c", &key, &retire, WAIT).await;
            if !matches!(reply, Some(EnrollReply::Retired { leaf_jwt: None, .. })) {
                break;
            }
        }
        let Some(EnrollReply::Retired { revoked, leaf_jwt: Some(shrunk), leaf_error: None, .. }) = reply else {
            panic!("retired again, the uplink renewed without dev-c: {reply:?}")
        };
        assert!(revoked.is_empty(), "the key was already revoked");
        assert_eq!(ants_proto::verified_jwt(&shrunk).unwrap().0.sub, uplink_key.public_key());
        assert!(auth::allows_all(&shrunk, &old).unwrap() && auth::allows_all(&old, &shrunk).unwrap());
        assert_eq!(members::issued(&team, &m1).unwrap().unwrap().ants, [p("dev-a")]);
    };
    tokio::select! {
        r = serving => panic!("the signer stopped: {r:?}"),
        () = checks => {}
    }
}
