//! Tests of renewal over NATS: a request approved, automatically or one by one, gives working credentials of the
//! same kind, keeps the seed where it was, and proves the credentials it renews.

use std::future::pending;

use ants_daemon::secrets::keystore;
use ants_proto::Role;
use ants_test_support::user as p;
use tokio::time::timeout;

use super::*;
use crate::credentials::approver::Approver;
use crate::credentials::approver::renewals::auto;
use crate::credentials::issued;
use crate::credentials::revoke;
use crate::node::members::EnrollApprover;
use crate::test_support::Server;

const WAIT: Duration = Duration::from_secs(5);
const REPEAT: Duration = Duration::from_millis(100);
const LISTEN: Duration = Duration::from_millis(400);

#[tokio::test]
async fn an_approved_renewal_gives_working_credentials_of_the_same_kind() {
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let creds = server.creds_as(&p("dev-a"), Role::Ant);
    let (old_jwt, old_key) = auth::read_creds(&creds).unwrap();
    let before = Finding::from_jwt(&old_jwt).unwrap();
    let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();
    let user = KeyPair::new_user();

    let renewing = Renewal { creds: &creds, user: &user, xkey: None };
    let asking = request(&server.url, &creds, renewing, keystore::os(), REPEAT, pending());
    let approving = async {
        let heard = approver.renewals(None).await.unwrap();
        assert_eq!(heard.len(), 1, "{heard:?}");
        assert_eq!((heard[0].name.as_str(), heard[0].public_key.as_str()), ("dev-a", user.public_key().as_str()));
        approver.renew(&team, "dev-a", None).await.unwrap().1
    };
    let (jwt, renewed) = timeout(WAIT * 2, Box::pin(async { tokio::join!(asking, approving) })).await.unwrap();
    let jwt = jwt.unwrap();
    assert_eq!(jwt, renewed.jwt);
    assert_eq!(renewed.previous, old_key.public_key());
    assert!(before.same_credentials(&renewed.finding));

    auth::replace_creds(&creds, &jwt, &user, None).unwrap();
    let (renewed_jwt, key) = auth::read_creds(&creds).unwrap();
    assert_eq!((renewed_jwt, key.public_key()), (jwt, user.public_key()));
    let (client, _) = bus::connect(&server.url, Some(&creds), None, &p("dev-a"), "test").await.unwrap();
    client.flush().await.unwrap();
    let line = issued::issued(&team).unwrap().pop().unwrap();
    assert_eq!((line.key, line.renewed_from), (user.public_key(), Some(old_key.public_key())));
}

#[tokio::test]
async fn renewing_stored_credentials_keeps_the_new_seed_in_the_store() {
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let store = keystore::Memory::default();
    let creds = server.creds_as(&p("dev-a"), Role::Ant);
    let Ok(keys::Moved::Now(old_key)) = keys::store_seed(&creds, &store, &server.url).await else { panic!() };
    let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();
    let renewal = async |user: &KeyPair| {
        let renewing = Renewal { creds: &creds, user, xkey: None };
        let asking = request(&server.url, &creds, renewing, &store, REPEAT, pending());
        let approving = async {
            while approver.renewals(None).await.unwrap().is_empty() {}
            approver.renew(&team, "dev-a", None).await.unwrap()
        };
        let (jwt, _) = timeout(WAIT * 2, Box::pin(async { tokio::join!(asking, approving) })).await.unwrap();
        jwt.unwrap()
    };

    let refused = KeyPair::new_user();
    let jwt = renewal(&refused).await;
    let before = std::fs::read(&creds).unwrap();
    store.refuse_writes();
    let err = format!("{:#}", keys::replace(&creds, &jwt, &refused, None, &store, &server.url).await.unwrap_err());
    assert!(err.contains("stay as they were") && err.contains("denied"), "{err}");
    assert_eq!((std::fs::read(&creds).unwrap(), store.keys()), (before, vec![old_key.clone()]));
    store.allow_writes();

    let user = KeyPair::new_user();
    let jwt = renewal(&user).await;
    keys::replace(&creds, &jwt, &user, None, &store, &server.url).await.unwrap();
    let text = std::fs::read_to_string(&creds).unwrap();
    assert_eq!(creds::seed_of(&text), Some(creds::Seed::Stored(&user.public_key())));
    assert_eq!(store.keys(), [user.public_key()], "the earlier seed {old_key} goes");
    let (client, _) = bus::connect_with(&server.url, Some(&creds), &store, None, &p("dev-a"), "test").await.unwrap();
    client.flush().await.unwrap();
}

#[tokio::test]
async fn renewing_a_human_device_without_an_encryption_key_binds_a_new_one_and_keeps_it() {
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let store = keystore::Memory::default();
    let creds = server.dir().join("alice.creds");
    let first = KeyPair::new_user();
    let rights = ants_proto::Permissions::for_user(&p("alice"), Role::Human);
    let human = issued::Kind::Human;
    let (jwt, _) = auth::Issue::new(&team, &p("alice"), human, rights).for_key(&first.public_key(), None).unwrap();
    auth::replace_creds(&creds, &jwt, &first, None).unwrap();
    let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();
    let renewal = async |user: &KeyPair, xkey: Option<&DeviceKey>| {
        let renewing = Renewal { creds: &creds, user, xkey };
        let asking = request(&server.url, &creds, renewing, &store, REPEAT, pending());
        let approving = async {
            while approver.renewals(None).await.unwrap().is_empty() {}
            approver.renew(&team, "alice", None).await.unwrap()
        };
        let (jwt, _) = timeout(WAIT * 2, Box::pin(async { tokio::join!(asking, approving) })).await.unwrap();
        jwt.unwrap()
    };

    let finding = Finding::from_jwt(&jwt).unwrap();
    let xkey = device_xkey(&creds, &finding, &store).unwrap().expect("a new key for a human's without one");
    let asked = auth::device_key(&xkey).unwrap();
    let user = KeyPair::new_user();
    let jwt = renewal(&user, Some(&asked)).await;
    let renewed = Finding::from_jwt(&jwt).unwrap();
    assert_eq!(renewed.xkey.as_ref(), Some(&asked));
    let current = auth::verified(&auth::read_creds(&creds).unwrap().0).unwrap();
    assert!(check_renewed(&jwt, &current, &finding, &user.public_key(), None).is_err(), "an unasked key");
    keys::replace(&creds, &jwt, &user, Some(&xkey), &store, &server.url).await.unwrap();
    assert_eq!(creds::load_xkey(&creds, &store).unwrap().map(|x| x.public_key()), Some(xkey.public_key()));

    let kept = device_xkey(&creds, &renewed, &store).unwrap().expect("the key the file holds");
    assert_eq!(kept.public_key(), xkey.public_key());
    let again = KeyPair::new_user();
    let jwt = renewal(&again, None).await;
    assert_eq!(Finding::from_jwt(&jwt).unwrap().xkey, Some(asked));
    keys::replace(&creds, &jwt, &again, Some(&kept), &store, &server.url).await.unwrap();
    assert_eq!(creds::load_xkey(&creds, &store).unwrap().map(|x| x.public_key()), Some(xkey.public_key()));
    let (client, _) = bus::connect(&server.url, Some(&creds), None, &p("alice"), "test").await.unwrap();
    client.flush().await.unwrap();
}

#[tokio::test]
async fn auto_approval_renews_asking_credentials_and_answers_a_repeat_alike() {
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let creds = server.creds_as(&p("alice"), Role::Human);
    let user = KeyPair::new_user();
    let (done_tx, mut done) = tokio::sync::mpsc::unbounded_channel();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let approving = auto(
        async || Approver::connect(&server.url, &team, None, LISTEN).await,
        Duration::from_millis(300),
        move |request, renewed| done_tx.send((request.name.clone(), renewed.jwt.clone())).unwrap(),
        async move {
            let _ = stop_rx.await;
        },
    );
    let asking = async {
        let renewing = Renewal { creds: &creds, user: &user, xkey: None };
        let first = request(&server.url, &creds, renewing, keystore::os(), REPEAT, pending()).await.unwrap();
        // Asking again with the same key, as a holder that missed the answer, gets the same, signed once.
        let renewing = Renewal { creds: &creds, user: &user, xkey: None };
        let again = request(&server.url, &creds, renewing, keystore::os(), REPEAT, pending()).await.unwrap();
        stop_tx.send(()).unwrap();
        (first, again)
    };
    let (approved, (first, again)) =
        timeout(WAIT * 2, Box::pin(async { tokio::join!(approving, asking) })).await.unwrap();
    approved.unwrap();
    assert_eq!(first, again);
    assert_eq!(done.recv().await.unwrap(), ("alice".to_owned(), first.clone()));
    assert!(done.try_recv().is_err(), "renewed once");
    auth::replace_creds(&creds, &first, &user, None).unwrap();
    let (client, _) = bus::connect(&server.url, Some(&creds), None, &p("alice"), "test").await.unwrap();
    client.flush().await.unwrap();
}

/// Publish `request` with the credentials at `creds`, as a holder would, a few times.
async fn forge(server: &Server, creds: &Path, request: &RenewRequest) {
    let client = async_nats::ConnectOptions::with_credentials_file(creds).await.unwrap().connect(&server.url).await;
    let client = client.unwrap();
    for _ in 0..6 {
        client.publish(request.subject(), serde_json::to_vec(request).unwrap().into()).await.unwrap();
        client.flush().await.unwrap();
        tokio::time::sleep(REPEAT).await;
    }
}

#[tokio::test]
async fn requests_must_prove_valid_unrevoked_credentials_of_their_name() {
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let alice = server.creds_as(&p("alice"), Role::Human);
    let (alice_jwt, alice_key) = auth::read_creds(&alice).unwrap();
    let dev_a = server.creds_as(&p("dev-a"), Role::Ant);
    let (dev_a_jwt, _) = auth::read_creds(&dev_a).unwrap();
    let revoked = server.dir().join("dev-b.creds");
    crate::test_support::issue(&team, &p("dev-b"), Role::Ant, None, &revoked).unwrap();
    let (revoked_jwt, revoked_key) = auth::read_creds(&revoked).unwrap();
    revoke::revoke(&team, Some("dev-b"), &[]).unwrap();
    let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();

    let signed = |name: &str, current: &str, key: &KeyPair| {
        let mut request = RenewRequest::new(name, KeyPair::new_user().public_key(), "host");
        request.current = current.to_owned();
        request.signature = auth::sign(key, &request.signed()).unwrap();
        request
    };
    let stranger = KeyPair::new_user();
    let good = signed("alice", &alice_jwt, &alice_key);
    let mut tampered = good.clone();
    tampered.public_key = KeyPair::new_user().public_key();
    let forged = [
        // Another's credentials, signed by a key that is not theirs.
        signed("dev-a", &dev_a_jwt, &stranger),
        // Alice's credentials renamed: they are dev-a's no more than the key signing them is.
        signed("dev-a", &alice_jwt, &alice_key),
        tampered,
        signed("dev-b", &revoked_jwt, &revoked_key),
    ];
    let sending = async {
        for request in forged.iter().chain([&good]) {
            forge(&server, &alice, request).await;
        }
    };
    let listening = async {
        let mut heard = Vec::new();
        while !heard.contains(&good) {
            heard.extend(approver.renewals(None).await.unwrap());
        }
        heard
    };
    let ((), heard) = timeout(WAIT * 4, Box::pin(async { tokio::join!(sending, listening) })).await.unwrap();
    assert!(!heard.is_empty() && heard.iter().all(|r| *r == good), "{heard:?}");
    // The revoked credentials are refused by the signing too, whatever hears them.
    let err = auth::renew(&team, &revoked_jwt, &KeyPair::new_user().public_key(), None).unwrap_err();
    assert!(err.to_string().contains("revoked"), "{err:#}");
}

#[tokio::test]
async fn the_holder_ignores_an_answer_changing_what_its_credentials_are() {
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let creds = server.creds_as(&p("dev-a"), Role::Ant);
    let user = KeyPair::new_user();
    // The approver's connection, which may answer.
    let client = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap().client;
    let renewing = Renewal { creds: &creds, user: &user, xkey: None };
    let asking = request(&server.url, &creds, renewing, keystore::os(), REPEAT, pending());
    let answering = async {
        tokio::time::sleep(REPEAT * 3).await;
        let human = auth::sign_user(&team, &p("dev-a"), Role::Human, None, &user.public_key()).unwrap();
        let other = auth::sign_user(&team, &p("dev-b"), Role::Ant, None, &user.public_key()).unwrap();
        for jwt in [human, other] {
            let reply = RenewReply::signed("dev-a", &user.public_key(), jwt);
            client.publish(reply.subject(), serde_json::to_vec(&reply).unwrap().into()).await.unwrap();
        }
        client.flush().await.unwrap();
        tokio::time::sleep(REPEAT * 3).await;
    };
    tokio::select! {
        jwt = asking => panic!("took an answer changing the credentials: {jwt:?}"),
        () = answering => {}
    }
}

#[tokio::test]
async fn a_leaf_node_s_uplink_is_renewed_through_a_human_s_credentials() {
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let m1: ants_proto::Machine = "m1".parse().unwrap();
    let uplink = server.dir().join("leaf-m1.creds");
    let members = auth::members(&[p("dev-a")], &[p("alice")]);
    auth::issue_leaf(&team, &m1, &members, &[], EnrollApprover::With, None, &uplink).unwrap();
    let before = Finding::from_jwt(&auth::read_creds(&uplink).unwrap().0).unwrap();
    let alice = server.creds_as(&p("alice"), Role::Human);
    let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();
    let user = KeyPair::new_user();
    let renewing = Renewal { creds: &uplink, user: &user, xkey: None };
    let asking = request(&server.url, &alice, renewing, keystore::os(), REPEAT, pending());
    let approving = approver.renew(&team, "leaf-m1", None);
    let (jwt, renewed) = timeout(WAIT * 2, Box::pin(async { tokio::join!(asking, approving) })).await.unwrap();
    let (_, renewed) = renewed.unwrap();
    assert_eq!(jwt.unwrap(), renewed.jwt);
    assert!(before.same_credentials(&renewed.finding), "{:?}", renewed.finding.kind);
    auth::check_leaf(&auth::account_key(&team).unwrap(), &renewed.jwt, &m1).unwrap();
}

#[test]
fn expired_and_enrollment_credentials_name_their_renewal_on_the_team_key_machine() {
    let dir = ants_test_support::scratch_dir("renewable");
    let team = dir.join("auth");
    auth::init(&team).unwrap();
    let enrollment = dir.join("enroll-dev-a.creds");
    auth::issue_enrollment(&team, &p("dev-a"), Duration::from_secs(60), &enrollment).unwrap();
    let (jwt, _) = auth::read_creds(&enrollment).unwrap();
    let err = renewable(&enrollment, &jwt).unwrap_err().to_string();
    assert!(err.contains("are not renewed over NATS") && err.contains("`ants auth enroll dev-a`"), "{err}");
    let ant = dir.join("dev-a.creds");
    crate::test_support::issue(&team, &p("dev-a"), Role::Ant, None, &ant).unwrap();
    let (jwt, _) = auth::read_creds(&ant).unwrap();
    assert!(renewable(&ant, &jwt).is_ok());
    std::fs::remove_dir_all(dir).unwrap();
}
