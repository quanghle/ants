use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_proto::{AdminOp, AdminOrder, EnrollReply, EnrollRequest, Machine, Permissions, Role, UserId, subject};
use futures::StreamExt as _;
use nkeys::KeyPair;
use serde_json::json;

use crate::auth::tests::Server;
use crate::auth::{self, NewDevice, issue_device_with, issue_host};
use crate::revoke::Kind;
use crate::signer::{Options, serve};

const WAIT: Duration = Duration::from_secs(10);
/// How long a request the signer ignores goes unanswered before the test stops waiting.
const IGNORED: Duration = Duration::from_millis(1500);

fn p(s: &str) -> UserId {
    s.parse().unwrap()
}

/// `op` about `ant` on `machine`, signed with the credentials `creds` of `admin`.
fn order(admin: &str, creds: &Path, machine: &str, ant: &str, op: AdminOp) -> AdminOrder {
    let (jwt, key) = auth::read_creds(creds).unwrap();
    let mut order = AdminOrder::new(p(admin), machine.parse().unwrap(), p(ant), op, jwt);
    order.signature = auth::sign(&key, &order.signed()).unwrap();
    order
}

fn add() -> AdminOp {
    AdminOp::Add { settings: serde_json::Map::new() }
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
    let (jwt, signer) = auth::read_creds(creds).unwrap();
    let mut replies = client.subscribe(subject::enroll_reply(&p(ant))).await.unwrap();
    let mut request = EnrollRequest::new(p(ant), key.public_key(), "m1");
    request.enrollment = jwt;
    request.order = Some(Box::new(order.clone()));
    let asking = async {
        loop {
            request.sent_at = chrono::Utc::now();
            request.signature = auth::sign(&signer, &request.signed()).unwrap();
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

/// The signer signs a key on an admin's order to add the ant on the machine asking, with its live host credentials,
/// and that one key only; it refuses an order no admin signed, one altered after signing, one for another machine or
/// ant, and ignores a request not from a machine.
#[tokio::test]
async fn the_signer_signs_on_an_admin_s_order_for_the_machine_asking_only() {
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let (alice, bob) = (p("alice"), p("bob"));
    let admin_creds = server.dir().join("alice-admin.creds");
    let admin = NewDevice { label: Some("laptop"), admin: true, ..NewDevice::default() };
    let rights = Permissions::for_user(&alice, Role::Human);
    issue_device_with(&team, &alice, Kind::Human, rights, &admin, &admin_creds).unwrap();
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
        forged.op = AdminOp::Add { settings: json!({ "model": "o3" }).as_object().unwrap().clone() };
        let refused = refusal(ask(&client, &host_creds, "dev-d", &second, &forged, WAIT).await);
        assert!(refused.contains("not signed by"), "{refused}");
        let elsewhere = order("alice", &admin_creds, "m2", "dev-d", add());
        let refused = refusal(ask(&client, &host_creds, "dev-d", &second, &elsewhere, WAIT).await);
        assert!(refused.contains("for m2, not m1"), "{refused}");
        let other_ant = order("alice", &admin_creds, "m1", "dev-e", add());
        let refused = refusal(ask(&client, &host_creds, "dev-d", &second, &other_ant, WAIT).await);
        assert!(refused.contains("about dev-e, not dev-d"), "{refused}");
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
