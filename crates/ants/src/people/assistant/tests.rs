//! Tests of `ants assistant`: refused and wide grants, grants and withdrawals, sealing, what an assistant does for its
//! human while granted, and its decisions and orders awaiting consent.

use std::time::Duration;

use ants_daemon::bus;
use ants_daemon::people::humans::{Registry, Withdrawal};
use ants_proto::{Acting, Role};
use ants_test_support::user as p;

use super::*;
use crate::connection::Nats;
use crate::test_support::Server;

#[test]
fn templated_ants_are_refused_and_wide_ones_warned_of() {
    let dir = ants_test_support::scratch_dir("assistant-check");
    let config = dir.join("config.toml");
    std::fs::write(
        &config,
        r#"
            harness = "claude"
            [[ant]]
            name = "worker-1"
            template = "worker"
            [[ant]]
            name = "coder"
            workdir = "~/src/repo"
            allow = ["read", "execute:cargo test*", "fetch"]
            [[ant]]
            name = "pa"
            whoami = "Assist."
            "#,
    )
    .unwrap();
    let err = check(&p("worker-1"), &Local::read(&p("worker-1"), &config, None), &[]).unwrap_err().to_string();
    assert!(err.contains("added from template worker"), "{err}");

    let warnings = check(&p("coder"), &Local::read(&p("coder"), &config, None), &[]).unwrap();
    assert_eq!(warnings.len(), 2, "{warnings:?}");
    assert!(warnings[0].contains("works in ~/src/repo"), "{warnings:?}");
    assert!(warnings[1].contains("(execute:cargo test*, fetch)"), "{warnings:?}");

    assert_eq!(check(&p("pa"), &Local::read(&p("pa"), &config, None), &[]).unwrap(), Vec::<String>::new());
    let unknown = check(&p("other"), &Local::read(&p("other"), &config, None), &[]).unwrap();
    assert!(unknown[0].contains("is not checked"), "{unknown:?}");
}

#[test]
fn a_replacing_record_is_issued_after_the_one_stored() {
    let now = Utc::now();
    assert_eq!(after(now, None), now);
    assert_eq!(after(now, Some(now - TimeDelta::seconds(5))), now);
    assert_eq!(after(now, Some(now + TimeDelta::seconds(5))), now + TimeDelta::seconds(5) + TimeDelta::microseconds(1));
}

/// What a human sends naming no one goes to its assistant while the grant naming it verifies, else the messenger.
#[tokio::test]
async fn a_human_s_unaddressed_asks_go_to_its_assistant_while_its_grant_stands() {
    use crate::messages::send::{Recipient, unaddressed};

    let Some(server) = Server::start() else { return };
    let alice = p("alice");
    let creds = server.creds_as(&alice, Role::Human);
    let account = devices::account_of(Some(&creds)).unwrap();
    let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
    humans::ensure(&js).await.unwrap();
    let me = humans::Me { id: alice.clone(), role: Role::Human, assistant_of: None };
    let messenger = Recipient::from(UserId::messenger());

    assert_eq!(unaddressed(&js, &me, account.as_deref()).await, messenger, "no grant");
    put(&js, &creds, &alice, Some(p("pa")), Sealing::Keep).await.unwrap();
    let assistant = unaddressed(&js, &me, account.as_deref()).await;
    assert_eq!(assistant, Recipient { to: p("pa"), assistant_of: Some(alice.clone()) });
    assert_eq!(assistant.to_string(), "pa (assistant of alice)");
    assert_eq!(unaddressed(&js, &me, None).await, messenger, "not verified without an account");
    let foreign = Some("AAOTHERACCOUNT");
    assert_eq!(unaddressed(&js, &me, foreign).await, messenger, "signed for another team");
    let ant = humans::Me { id: alice.clone(), role: Role::Ant, assistant_of: None };
    assert_eq!(unaddressed(&js, &ant, account.as_deref()).await, messenger, "an ant has no assistant");
    put(&js, &creds, &alice, None, Sealing::Off).await.unwrap();
    assert_eq!(unaddressed(&js, &me, account.as_deref()).await, messenger, "withdrawn");
}

/// A human's device signs a grant only the human's credentials store, readers watching honour it at once, and
/// a withdrawal ends it at once.
#[tokio::test]
async fn a_human_grants_and_withdraws_its_assistant() {
    let Some(server) = Server::start() else { return };
    let (alice, bob) = (p("alice"), p("bob"));
    let creds = server.creds_as(&alice, Role::Human);
    let account = devices::account_of(Some(&creds)).unwrap();
    let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
    humans::ensure(&js).await.unwrap();

    let reader = Registry::new(alice.clone()).verifying_grants(account);
    let mut withdrawals = reader.withdrawals();
    let _watching = reader.watch(js.clone(), ants_daemon::people::humans::RETRY);
    let honoured = async |assistant: Option<&str>| {
        let wanted = assistant.map(p);
        tokio::time::timeout(Duration::from_secs(5), async {
            while reader.assistant(&alice) != wanted {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the reader never saw {wanted:?}"));
    };

    assert_eq!(put(&js, &creds, &alice, Some(p("pa")), Sealing::Keep).await.unwrap().0, None);
    honoured(Some("pa")).await;
    assert!(reader.honours_assistant(&alice, &p("pa"), Some(&alice)));
    assert!(!reader.honours_assistant(&alice, &p("pa"), None), "without the assistant_of tag");
    assert!(!reader.is_human(&p("alice-assistant")), "a grant is no human's entry");
    let (replaced, _) = put(&js, &creds, &alice, Some(p("pb")), Sealing::Keep).await.unwrap();
    assert_eq!(replaced.and_then(|g| g.assistant), Some(p("pa")));
    honoured(Some("pb")).await;
    assert_eq!(withdrawals.recv().await.unwrap(), Withdrawal { human: alice.clone(), assistant: p("pa") });
    put(&js, &creds, &alice, None, Sealing::Off).await.unwrap();
    honoured(None).await;
    assert_eq!(withdrawals.recv().await.unwrap(), Withdrawal { human: alice.clone(), assistant: p("pb") });
    let stored = read(&js, &alice).await.unwrap().unwrap();
    assert_eq!(stored.assistant, None, "a withdrawal is signed and stored");

    let err = put(&js, &creds, &bob, Some(p("pa")), Sealing::Keep).await.unwrap_err().to_string();
    assert!(err.contains("not a device of bob's"), "{err}");
    let (_, bob_js) = server.connect_as(&bob, Role::Human).await;
    let kv = bob_js.get_key_value(HUMANS_BUCKET).await.unwrap();
    let denied = kv.put("alice.assistant", serde_json::to_vec(&stored).unwrap().into()).await;
    assert!(denied.is_err(), "only alice's credentials write its grant");
    kv.put("bob", b"{\"human\":\"bob\"}".to_vec().into()).await.expect("every human's entry stays writable");
    let (_, ant_js) = server.connect_as(&p("pa"), Role::Ant).await;
    let kv = ant_js.get_key_value(HUMANS_BUCKET).await.unwrap();
    assert!(kv.put("alice.assistant", b"{}".to_vec().into()).await.is_err(), "no ant writes a grant");
}

/// Alice enrolls her assistant as one of her devices: Bob's CLI seals for it too, the assistant opens what Bob
/// sealed for Alice and seals what it sends Bob for her, which Bob opens as from the assistant and never as from
/// Alice; `seal off` ends both at once.
#[tokio::test]
async fn an_enrolled_assistant_opens_and_seals_for_its_human_until_withdrawn() {
    use ants_client::sealed::State;
    use ants_daemon::people::devices::Directory;
    use ants_daemon::secrets::sealing::{AssistantSealing, Opening, seal_envelope};
    use ants_proto::Envelope;
    use crypto_secretbox::XSalsa20Poly1305;
    use crypto_secretbox::aead::{KeyInit as _, OsRng};

    use crate::people::sealed::{Keyring, PLAINTEXT, Sealer};

    let Some(server) = Server::start() else { return };
    let (alice, bob, pa) = (p("alice"), p("bob"), p("pa"));
    let creds: Vec<_> = [&alice, &bob].iter().map(|h| server.creds_as(h, Role::Human)).collect();
    let nats: Vec<_> = creds.iter().map(|c| Nats::new(&server.url, Some(c), None).unwrap()).collect();
    let (_, alice_js) = nats[0].connect("test").await.unwrap();
    let (_, bob_js) = nats[1].connect("test").await.unwrap();
    humans::ensure(&alice_js).await.unwrap();
    let account = devices::account_of(Some(&creds[0])).unwrap().unwrap();

    let xkey = XKey::new();
    let key: DeviceKey = xkey.public_key().parse().unwrap();
    put(&alice_js, &creds[0], &alice, Some(pa.clone()), Sealing::Keep).await.unwrap();
    let (_, enrolled) = put(&alice_js, &creds[0], &alice, Some(pa.clone()), Sealing::Set(key.clone())).await.unwrap();
    let device = enrolled.sealing.as_ref().unwrap().device;
    let (_, again) = put(&alice_js, &creds[0], &alice, Some(pa.clone()), Sealing::Set(key.clone())).await.unwrap();
    assert_eq!(again.sealing.unwrap().device, device, "the same key stays the same device");

    // The assistant's daemon; its credentials reading the directory are Alice's here.
    let (_, pa_js) = server.connect_as(&pa, Role::Ant).await;
    let registry = Registry::new(alice.clone()).verifying_grants(Some(account.clone()));
    let _watching = registry.watch(pa_js, ants_daemon::people::humans::RETRY);
    let directory = Directory::open(&alice_js, &account).await.unwrap();
    let assistant = AssistantSealing::new(pa.clone(), xkey.clone(), registry, directory);
    let enrolled_now = async |wanted: Option<DeviceId>| {
        tokio::time::timeout(Duration::from_secs(5), async {
            while assistant.device_for(&alice) != wanted {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the assistant never saw {wanted:?}"));
    };
    enrolled_now(Some(device)).await;

    let sealer = Sealer::new(&nats[1], &bob_js, &alice, PLAINTEXT).await.unwrap();
    let mut asked = Envelope::new(bob.clone(), alice.clone(), "the board meets at nine".to_owned());
    sealer.seal(&mut asked, &mut []).unwrap();
    assert!(asked.sealed.as_ref().unwrap().key_for(&device).is_some(), "sealed for the assistant too");
    let Opening::Opened(body) = assistant.open(&alice, &asked).await.unwrap() else { panic!("does not open") };
    assert_eq!(body.text, "the board meets at nine");
    assert_eq!(assistant.open(&bob, &asked).await.unwrap(), Opening::Unreadable, "bob enrolled no one");

    let mut answer = Envelope::new(pa.clone(), bob.clone(), "alice will be there".to_owned());
    assistant.seal(&alice, &mut answer).await.unwrap();
    let bob_keyring = Keyring::load(&nats[1], &bob_js).await.unwrap();
    let alice_keyring = Keyring::load(&nats[0], &alice_js).await.unwrap();
    for keyring in [&bob_keyring, &alice_keyring] {
        let (opened, state) = crate::people::sealed::read(answer.clone(), Some(keyring));
        assert_eq!((opened.text.as_str(), state), ("alice will be there", Some(State::Opened)));
    }
    let mut title = ants_proto::ThreadTitle::new(answer.thread_id.clone(), "Board", pa.clone()).unwrap();
    assistant.seal_title(&alice, &bob, &mut title).await.unwrap();
    assert!(title.title.is_empty() && title.sealed.as_ref().unwrap().key_for(&device).is_some());
    for keyring in [&bob_keyring, &alice_keyring] {
        assert_eq!(keyring.open_title(&title).await, Some(Opening::Opened("Board".to_owned())));
    }
    let mut as_alice = Envelope::new(alice.clone(), bob.clone(), "pay pa double".to_owned());
    let content = XSalsa20Poly1305::generate_key(&mut OsRng);
    let bob_devices = Directory::open(&bob_js, &account).await.unwrap().devices(std::slice::from_ref(&bob)).await;
    let bob_device = bob_devices.unwrap().verified.remove(0);
    let recipients = [(bob_device.device, &bob_device.key)];
    seal_envelope(&mut as_alice, &content, Vec::new(), device, &xkey, &recipients).unwrap();
    assert_eq!(crate::people::sealed::read(as_alice, Some(&bob_keyring)).1, Some(State::Forged), "not as its human");
    let refused = assistant.seal(&alice, &mut Envelope::new(alice.clone(), bob.clone(), String::new())).await;
    assert!(refused.unwrap_err().to_string().contains("seals only what it sends itself"));

    put(&alice_js, &creds[0], &alice, Some(pa.clone()), Sealing::Off).await.unwrap();
    enrolled_now(None).await;
    assert_eq!(assistant.open(&alice, &asked).await.unwrap(), Opening::Unreadable);
    let refused = assistant.seal(&alice, &mut Envelope::new(pa.clone(), bob.clone(), String::new())).await;
    assert!(refused.unwrap_err().to_string().contains("has not enrolled pa"));
    let sealer = Sealer::new(&nats[1], &bob_js, &alice, PLAINTEXT).await.unwrap();
    let mut asked = Envelope::new(bob.clone(), alice.clone(), "lunch?".to_owned());
    sealer.seal(&mut asked, &mut []).unwrap();
    assert!(asked.sealed.unwrap().key_for(&device).is_none(), "no longer sealed for the assistant");

    let (_, enrolled) = put(&alice_js, &creds[0], &alice, Some(pa.clone()), Sealing::Set(key)).await.unwrap();
    let device = enrolled.sealing.unwrap().device;
    enrolled_now(Some(device)).await;
    put(&alice_js, &creds[0], &alice, Some(p("pb")), Sealing::Keep).await.unwrap();
    enrolled_now(None).await;
    let (_, stored) = put(&alice_js, &creds[0], &alice, Some(pa.clone()), Sealing::Keep).await.unwrap();
    assert_eq!(stored.sealing, None, "an enrollment ends with its assistant");
}

/// What an ant does for a human counts for the human only while the human's grant names the ant and the ant's
/// record, its own credentials, names the human; any other claim is forged.
#[tokio::test]
async fn an_assistant_acts_for_its_human_only_while_grant_and_credentials_say_so() {
    let Some(server) = Server::start() else { return };
    let (alice, pa, pb, dev_a) = (p("alice"), p("pa"), p("pb"), p("dev-a"));
    let creds = server.creds_as(&alice, Role::Human);
    let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
    humans::ensure(&js).await.unwrap();
    js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    for ant in [&pa, &pb] {
        let assistant = server.dir().join(format!("{ant}-assistant.creds"));
        let new = auth::Registered { assistant_of: Some(alice.clone()), ..auth::Registered::default() };
        let rights = ants_proto::Permissions::for_assistant(ant, &alice);
        auth::Issue::new(&server.dir().join("auth"), ant, issued::Kind::Assistant, rights)
            .with(&new)
            .into_file(&assistant)
            .unwrap();
        let (_, ant_js) = bus::connect(&server.url, Some(&assistant), None, ant, "test").await.unwrap();
        let kept = ants_daemon::people::behalf::keep_record(&ant_js, None, ant, &assistant).await.unwrap();
        assert_eq!(kept, Some(alice.clone()));
    }
    let account = devices::account_of(Some(&creds)).unwrap().unwrap();
    let directory = ants_daemon::people::devices::Directory::open(&js, &account).await.unwrap();
    for read in [directory.all().await, directory.devices(&[pa.clone(), pb.clone()]).await] {
        assert!(read.unwrap().skipped.is_empty(), "an assistant's record is no device's entry to skip");
    }
    let ant_creds = server.creds_as(&dev_a, Role::Ant);
    let (_, ant_js) = bus::connect(&server.url, Some(&ant_creds), None, &dev_a, "test").await.unwrap();
    assert_eq!(ants_daemon::people::behalf::keep_record(&ant_js, None, &dev_a, &ant_creds).await.unwrap(), None);
    let devices =
        ants_daemon::records::direct_kv::DirectKv::new(&ant_js, None, ants_proto::DEVICES_BUCKET).await.unwrap();
    let record = ants_proto::assistant_record_key(&dev_a);
    assert!(devices.put(&record, b"{}".to_vec()).await.is_err(), "an ant keeps no record");
    let nats = Nats::new(&server.url, Some(&creds), None).unwrap();
    let acting = async |by: &UserId, claim: Option<&UserId>| {
        crate::people::behalf::Checker::new(&nats, &js).check(by, claim).await
    };

    put(&js, &creds, &alice, Some(pa.clone()), Sealing::Keep).await.unwrap();
    assert_eq!(acting(&pa, Some(&alice)).await, Acting::For(alice.clone()));
    assert_eq!(acting(&pa, None).await, Acting::Own);
    assert_eq!(acting(&pb, Some(&alice)).await, Acting::Forged(alice.clone()), "another assistant of alice");
    put(&js, &creds, &alice, Some(dev_a.clone()), Sealing::Keep).await.unwrap();
    assert_eq!(acting(&dev_a, Some(&alice)).await, Acting::Forged(alice.clone()), "credentials naming no human");
    put(&js, &creds, &alice, None, Sealing::Keep).await.unwrap();
    assert_eq!(acting(&pa, Some(&alice)).await, Acting::Forged(alice.clone()), "withdrawn");
    assert_eq!(Acting::For(alice.clone()).label("pa"), "alice via pa");
}

/// A long-running reader's checker takes a grant withdrawn, or granted again, and credentials revoked, as its
/// watches deliver them, without checking anew.
#[tokio::test]
async fn a_following_checker_takes_withdrawals_and_revocations() {
    let Some(server) = Server::start() else { return };
    let (alice, pa) = (p("alice"), p("pa"));
    let creds = server.creds_as(&alice, Role::Human);
    let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
    humans::ensure(&js).await.unwrap();
    js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    let assistant = server.dir().join("pa-assistant.creds");
    let new = auth::Registered { assistant_of: Some(alice.clone()), ..auth::Registered::default() };
    let rights = ants_proto::Permissions::for_assistant(&pa, &alice);
    let team = server.dir().join("auth");
    auth::Issue::new(&team, &pa, issued::Kind::Assistant, rights).with(&new).into_file(&assistant).unwrap();
    let (_, pa_js) = bus::connect(&server.url, Some(&assistant), None, &pa, "test").await.unwrap();
    ants_daemon::people::behalf::keep_record(&pa_js, None, &pa, &assistant).await.unwrap();
    put(&js, &creds, &alice, Some(pa.clone()), Sealing::Keep).await.unwrap();
    let nats = Nats::new(&server.url, Some(&creds), None).unwrap();
    let checker = std::sync::Arc::new(crate::people::behalf::Checker::new(&nats, &js));
    assert_eq!(checker.check(&pa, Some(&alice)).await, Acting::For(alice.clone()));
    let _following = checker.follow();
    let comes_to = async |expected: Acting| {
        for _ in 0..100 {
            if checker.acting(&pa, Some(&alice)) == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("still {:?}, not {expected:?}", checker.acting(&pa, Some(&alice)));
    };
    // The watches start in the background: a change made before them is missed until the next refresh.
    checker.watching().await;

    put(&js, &creds, &alice, None, Sealing::Keep).await.unwrap();
    comes_to(Acting::Forged(alice.clone())).await;
    put(&js, &creds, &alice, Some(pa.clone()), Sealing::Keep).await.unwrap();
    comes_to(Acting::For(alice.clone())).await;
    crate::credentials::revoke::revoke(&team, Some("pa"), &[]).unwrap();
    crate::people::devices::record_revocations(&server.url, None, &team).await.unwrap();
    comes_to(Acting::Forged(alice.clone())).await;
}

/// pa pins, archives and snoozes alice's threads under keys of its own, never hers: her devices take them in while
/// her grant names pa, on the threads she marked or snoozed none of herself, and a reader keeps up with her
/// withdrawing the grant. Her own unarchiving drops pa's archive. pa keeps none of its own, nor touches bob's.
#[tokio::test]
async fn an_assistant_s_marks_and_snoozes_count_for_its_human_while_granted() {
    use ants_daemon::records::direct_kv::DirectKv;
    use std::collections::BTreeSet;

    use ants_proto::{
        Reminders, THREADS_BUCKET, ThreadId, ThreadMarks, ThreadSnoozes, assisted_marks_key, assisted_reminders_key,
        marks_key, reminders_key,
    };

    let Some(server) = Server::start() else { return };
    let (alice, pa, bob) = (p("alice"), p("pa"), p("bob"));
    let creds = server.creds_as(&alice, Role::Human);
    let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
    humans::ensure(&js).await.unwrap();
    js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
    let assistant = server.dir().join("pa-assistant.creds");
    let new = auth::Registered { assistant_of: Some(alice.clone()), ..auth::Registered::default() };
    let rights = ants_proto::Permissions::for_assistant(&pa, &alice);
    auth::Issue::new(&server.dir().join("auth"), &pa, issued::Kind::Assistant, rights)
        .with(&new)
        .into_file(&assistant)
        .unwrap();
    let (_, pa_js) = bus::connect(&server.url, Some(&assistant), None, &pa, "test").await.unwrap();
    ants_daemon::people::behalf::keep_record(&pa_js, None, &pa, &assistant).await.unwrap();
    put(&js, &creds, &alice, Some(pa.clone()), Sealing::Keep).await.unwrap();

    let (mine, pinned, archived, snoozed) =
        (ThreadId::generate(), ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
    let nats = Nats::new(&server.url, Some(&creds), None).unwrap();
    let meta = crate::threads::thread_meta::open(&nats, &js).await.unwrap();
    assert!(meta.change_marks(|m| m.pin(&mine)).await.unwrap());
    let now = chrono::Utc::now();
    let own_snooze = |s: &mut ThreadSnoozes| {
        s.snooze(&mine, now, now + TimeDelta::hours(1));
        true
    };
    assert!(meta.change_snoozes(own_snooze).await.unwrap());
    let kv = DirectKv::new(&pa_js, None, THREADS_BUCKET).await.unwrap();
    let mut marks = ThreadMarks::default();
    assert!(marks.pin(&pinned) && marks.archive(&archived, 1) && marks.archive(&mine, 1));
    kv.put(assisted_marks_key(&alice, &pa), serde_json::to_vec(&marks).unwrap()).await.unwrap();
    let mut snoozes = Reminders::default();
    snoozes.snoozed.snooze(&snoozed, now, now + TimeDelta::hours(1));
    snoozes.snoozed.snooze(&mine, now, now + TimeDelta::hours(5));
    kv.put(assisted_reminders_key(&alice, &pa), serde_json::to_vec(&snoozes).unwrap()).await.unwrap();
    for key in [
        marks_key(&alice),
        reminders_key(&alice),
        assisted_marks_key(&alice, &bob),
        marks_key(&bob),
        assisted_reminders_key(&bob, &pa),
        marks_key(&pa),
        reminders_key(&pa),
    ] {
        assert!(kv.put(&key, "{}").await.is_err(), "pa puts {key}");
    }

    let read = meta.marks().await.unwrap();
    assert_eq!(read.pinned.keys().collect::<BTreeSet<_>>(), [&mine, &pinned].into());
    assert_eq!(read.archived.keys().collect::<Vec<_>>(), [&archived], "alice's own pin of hers wins");
    let read = meta.reminders().await.unwrap().snoozed;
    assert!(read.get(&snoozed, now).is_some());
    assert_eq!(read.get(&mine, now).unwrap().until, now + TimeDelta::hours(1), "as does her own snooze");
    assert!(meta.change_thread_marks(&archived, |m| m.unarchive(&archived)).await.unwrap(), "pa's archive goes");
    assert!(meta.marks().await.unwrap().archived.is_empty());

    put(&js, &creds, &alice, None, Sealing::Keep).await.unwrap();
    let withdrawn = async {
        loop {
            if meta.marks().await.unwrap().pinned.keys().collect::<Vec<_>>() == [&mine] {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), withdrawn).await.expect("withdrawn, pa's marks count no more");
    assert_eq!(meta.reminders().await.unwrap().snoozed.0.keys().collect::<Vec<_>>(), [&mine]);
    assert!(meta.change_thread_marks(&pinned, |m| m.pin(&pinned)).await.unwrap(), "alice's own pin");
    assert_eq!(meta.marks().await.unwrap().pinned.len(), 2);
}

/// pa's reaction for alice reads as hers through pa, a claim of bob, who is no assistant, is not even read,
/// and pa's reaction once alice withdraws the grant is pa's own, its claim forged.
#[tokio::test]
async fn an_assistant_s_reactions_for_its_human_show_as_hers() {
    use ants_daemon::records::reactions::{Change, Reactions};
    use ants_proto::{REACTIONS_BUCKET, Reacted, Reaction, ReactionEntry};

    let Some(server) = Server::start() else { return };
    let (alice, pa, bob) = (p("alice"), p("pa"), p("bob"));
    let creds = server.creds_as(&alice, Role::Human);
    let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
    humans::ensure(&js).await.unwrap();
    js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    js.create_stream(ants_daemon::hub::reactions_config()).await.unwrap();
    let assistant = server.dir().join("pa-assistant.creds");
    let new = auth::Registered { assistant_of: Some(alice.clone()), ..auth::Registered::default() };
    let rights = ants_proto::Permissions::for_assistant(&pa, &alice);
    auth::Issue::new(&server.dir().join("auth"), &pa, issued::Kind::Assistant, rights)
        .with(&new)
        .into_file(&assistant)
        .unwrap();
    let (_, pa_js) = bus::connect(&server.url, Some(&assistant), None, &pa, "test").await.unwrap();
    ants_daemon::people::behalf::keep_record(&pa_js, None, &pa, &assistant).await.unwrap();
    put(&js, &creds, &alice, Some(pa.clone()), Sealing::Keep).await.unwrap();

    let message = ants_proto::Uuid::now_v7();
    let check: Reaction = "check".parse().unwrap();
    let (_, bob_js) = server.connect_as(&bob, Role::Human).await;
    for (js, by) in [(&pa_js, &pa), (&bob_js, &bob)] {
        let entry = ReactionEntry::new(by.clone(), message, check.clone()).on_behalf(Some(alice.clone()));
        let subject = format!("$KV.{REACTIONS_BUCKET}.{}", entry.key());
        js.publish(subject, serde_json::to_vec(&entry).unwrap().into()).await.unwrap().await.unwrap();
    }
    let read = || -> Reactions { [(message, vec![Reacted::new(check.clone(), vec![pa.clone(), bob.clone()])])].into() };
    let nats = Nats::new(&server.url, Some(&creds), None).unwrap();
    let checker = crate::people::behalf::Checker::new(&nats, &js);
    let mut checked = read();
    checker.reactions(&mut checked).await;
    assert_eq!(crate::messages::inbox::render_reactions(&checked[&message], ""), "reacted check: alice via pa, bob\n");
    let change = Change { message, by: pa.clone(), reaction: check.clone(), removed: false, acting: Acting::Own };
    assert_eq!(checker.change(change).await.acting, Acting::For(alice.clone()));

    put(&js, &creds, &alice, None, Sealing::Keep).await.unwrap();
    checker.refresh().await;
    let mut checked = read();
    checker.reactions(&mut checked).await;
    assert_eq!(checked[&message][0].acting, [(pa.clone(), Acting::Forged(alice.clone()))].into());
}

/// pa's channel and notes posts for alice read as hers through pa; bob's claim to post for alice is forged.
#[tokio::test]
async fn an_assistant_s_posts_for_its_human_show_as_hers() {
    use ants_proto::{ChannelPost, NotesPost, ThreadId};

    let Some(server) = Server::start() else { return };
    let (alice, pa, bob) = (p("alice"), p("pa"), p("bob"));
    let creds = server.creds_as(&alice, Role::Human);
    let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
    humans::ensure(&js).await.unwrap();
    js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    let assistant = server.dir().join("pa-assistant.creds");
    let new = auth::Registered { assistant_of: Some(alice.clone()), ..auth::Registered::default() };
    let rights = ants_proto::Permissions::for_assistant(&pa, &alice);
    auth::Issue::new(&server.dir().join("auth"), &pa, issued::Kind::Assistant, rights)
        .with(&new)
        .into_file(&assistant)
        .unwrap();
    let (_, pa_js) = bus::connect(&server.url, Some(&assistant), None, &pa, "test").await.unwrap();
    ants_daemon::people::behalf::keep_record(&pa_js, None, &pa, &assistant).await.unwrap();
    put(&js, &creds, &alice, Some(pa.clone()), Sealing::Keep).await.unwrap();

    let nats = Nats::new(&server.url, Some(&creds), None).unwrap();
    let checker = std::sync::Arc::new(crate::people::behalf::Checker::new(&nats, &js));
    let mut parser = crate::threads::thread::Parser::default().checking(Some(checker));
    let mut read = async |subject: String, payload: Vec<u8>| {
        parser.read(1, chrono::Utc::now(), &subject, &payload).await.unwrap().route()
    };
    let ops = "ops".parse().unwrap();
    let posted = ChannelPost::new(ops, pa.clone(), "deploy friday").on_behalf(Some(alice.clone()));
    assert_eq!(read(posted.subject(), serde_json::to_vec(&posted).unwrap()).await, "alice via pa -> #ops");
    let forged = ChannelPost { from: bob.clone(), ..posted };
    assert_eq!(read(forged.subject(), serde_json::to_vec(&forged).unwrap()).await, "bob (forged: for alice) -> #ops");
    let noted = NotesPost::new(ThreadId::generate(), pa.clone(), "noted").on_behalf(Some(alice.clone()));
    assert_eq!(read(noted.subject(), serde_json::to_vec(&noted).unwrap()).await, "alice via pa -> notes");
}

/// The store the daemon's tools act for a human through reads and writes with its assistant's own credentials:
/// the human's messages and the tool requests waiting for it, its read marker, its threads' titles and done marks,
/// the assistant's reactions, and the directory its sealing reads device keys from.
#[tokio::test]
async fn an_assistant_s_credentials_reach_what_its_tools_act_on_for_its_human() {
    use std::time::SystemTime;

    use ants_proto::{ApprovalRequest, Envelope, ReadMarker, ThreadDone, ThreadId, ThreadTitle, ToolCall};

    let Some(server) = Server::start() else { return };
    let (alice, bob, pa, dev_a) = (p("alice"), p("bob"), p("pa"), p("dev-a"));
    let creds = server.creds_as(&alice, Role::Human);
    let (_, alice_js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
    bus::ensure_stream(&alice_js).await.unwrap();
    alice_js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
    alice_js.create_stream(ants_daemon::hub::reactions_config()).await.unwrap();
    let read = alice_js.create_key_value(ants_daemon::hub::read_config()).await.unwrap();
    alice_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    let assistant = server.dir().join("pa-assistant.creds");
    let new = auth::Registered { assistant_of: Some(alice.clone()), ..auth::Registered::default() };
    let rights = ants_proto::Permissions::for_assistant(&pa, &alice);
    auth::Issue::new(&server.dir().join("auth"), &pa, issued::Kind::Assistant, rights)
        .with(&new)
        .into_file(&assistant)
        .unwrap();
    let (_, mut pa_js) = bus::connect(&server.url, Some(&assistant), None, &pa, "test").await.unwrap();
    pa_js.set_timeout(Duration::from_secs(2));

    let thread = ThreadId::generate();
    let (_, dev_js) = server.connect_as(&dev_a, Role::Ant).await;
    let (_, bob_js) = server.connect_as(&bob, Role::Human).await;
    let publish = async |js: &async_nats::jetstream::Context, subject: String, payload: Vec<u8>| {
        js.publish(subject, payload.into()).await.unwrap().await.unwrap().sequence
    };
    let asked = Envelope::on_thread(thread.clone(), dev_a.clone(), alice.clone(), "ship it?");
    let seq = publish(&dev_js, asked.subject(), serde_json::to_vec(&asked).unwrap()).await;
    let from_bob = Envelope::new(bob.clone(), alice.clone(), "lunch?");
    publish(&bob_js, from_bob.subject(), serde_json::to_vec(&from_bob).unwrap()).await;
    let to_bob = Envelope::new(alice.clone(), bob.clone(), "later");
    publish(&alice_js, to_bob.subject(), serde_json::to_vec(&to_bob).unwrap()).await;
    let elsewhere = Envelope::new(bob.clone(), dev_a.clone(), "not alice's");
    publish(&bob_js, elsewhere.subject(), serde_json::to_vec(&elsewhere).unwrap()).await;
    let call = ToolCall { kind: "execute".into(), title: "git push".into(), ..ToolCall::default() };
    let request = ApprovalRequest::new(dev_a.clone(), thread.clone(), call, Vec::new(), Duration::from_secs(600))
        .for_human(Some(alice.clone()), None);
    publish(&dev_js, request.subject(), serde_json::to_vec(&request).unwrap()).await;
    let marker = ReadMarker { below: seq, ..ReadMarker::default() };
    read.put(alice.as_str(), serde_json::to_vec(&marker).unwrap().into()).await.unwrap();

    let store = ants_daemon::mcp::jetstream_store(pa_js.clone(), None, assistant.clone());
    let since = SystemTime::now() - Duration::from_secs(3600);
    let messages = store.messages(alice.clone(), pa.clone(), since).await.unwrap();
    let ids: Vec<_> = messages.iter().map(|m| m.envelope.id).collect();
    assert_eq!(ids, [asked.id, from_bob.id, to_bob.id]);
    let (requests, decided) = store.approvals(since).await.unwrap();
    assert_eq!((requests.iter().map(|r| r.id).collect::<Vec<_>>(), decided.len()), (vec![request.id], 0));
    assert!(store.read_marker(alice.clone()).await.unwrap().is_read(seq));
    let last = store.last_sequence().await.unwrap();
    assert!(last > seq);

    let title = ThreadTitle::new(thread.clone(), "Release", pa.clone()).unwrap().on_behalf(Some(alice.clone()));
    store.put_title(title).await.unwrap();
    let done = ThreadDone::new(thread.clone(), pa.clone(), last).on_behalf(Some(alice.clone()));
    store.put_done(done).await.unwrap();
    let found = store.done(thread.clone()).await.unwrap().unwrap();
    assert_eq!((found.by, found.on_behalf_of), (pa.clone(), Some(alice.clone())));
    let check: ants_proto::Reaction = "check".parse().unwrap();
    assert!(store.react(pa.clone(), asked.id, check.clone(), false, alice.clone()).await.unwrap());
    assert!(store.react(pa.clone(), asked.id, check, true, alice.clone()).await.unwrap());
    let account = devices::account_of(Some(&creds)).unwrap().unwrap();
    ants_daemon::people::devices::Directory::open(&pa_js, &account).await.expect("its sealing reads the directory");
}

/// pa decides three of dev-a's requests for alice: alice confirms one, denies one, and pre-allows the third, so
/// that her CLI and the daemons reading her records see the first and third decided for her.
#[tokio::test]
async fn a_human_confirms_denies_or_pre_allows_its_assistant_s_decisions() {
    use ants_proto::{Act, ActAnswer, ApprovalDecision, ApprovalRequest, Envelope, ThreadId, ToolCall, Verdict};

    use crate::tools::{self, State};

    let Some(server) = Server::start() else { return };
    let (alice, pa, dev_a) = (p("alice"), p("pa"), p("dev-a"));
    let creds = server.creds_as(&alice, Role::Human);
    let account = devices::account_of(Some(&creds)).unwrap();
    let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
    humans::ensure(&js).await.unwrap();
    let (_, human) = server.connect(&UserId::human()).await;
    let stream = bus::ensure_stream(&human).await.unwrap();
    put(&js, &creds, &alice, Some(pa.clone()), Sealing::Keep).await.unwrap();
    let reader = Registry::new(alice.clone()).verifying_grants(account);
    let _watching = reader.watch(js.clone(), ants_daemon::people::humans::RETRY);

    let (_, ant_js) = server.connect(&dev_a).await;
    let (_, pa_js) = server.connect(&pa).await;
    let [push, rm, test] = ["git push", "rm -rf build", "cargo test"].map(|command| {
        let input = Some(serde_json::json!({ "command": command }).to_string());
        let tool = ToolCall { kind: "execute".into(), title: command.into(), input, ..Default::default() };
        let offered = vec![Verdict::AllowOnce, Verdict::Reject];
        ApprovalRequest::new(dev_a.clone(), ThreadId::generate(), tool, offered, Duration::from_secs(60))
    });
    for r in [&push, &rm, &test] {
        bus::publish_json(&ant_js, r.subject(), r, None).await.unwrap();
        let decision = ApprovalDecision::new(r.id, dev_a.clone(), Verdict::AllowOnce);
        let envelope = Envelope::decision(r.thread_id.clone(), pa.clone(), decision).on_behalf(Some(alice.clone()));
        bus::publish_json(&pa_js, envelope.subject(), &envelope, None).await.unwrap();
    }
    let held = tools::held(&js, &stream, Utc::now()).await.unwrap();
    assert_eq!(held.len(), 3, "each waits for alice");
    assert!(held.iter().all(|(_, acts)| acts.len() == 1 && acts[0].assistant == pa && acts[0].human == alice));

    let nats = Nats::new(&server.url, Some(&creds), None).unwrap();
    Box::pin(consent::confirm(&nats, Some(&tools::qualified(&push)), false)).await.unwrap();
    Box::pin(consent::confirm(&nats, Some(&tools::qualified(&rm)), true)).await.unwrap();
    let err = consent::allow(&nats, vec!["execute:cargo test*".into()], false, false).await.unwrap_err();
    assert!(err.to_string().contains("ANT/RULE"), "{err}");
    consent::allow(&nats, vec!["dev-a/execute:cargo test*".into()], false, false).await.unwrap();

    let state_of = async |r: &ApprovalRequest| {
        let (seq, _) = tools::find(&stream, &tools::qualified(r)).await.unwrap();
        tools::state(&js, &stream, seq, r, Utc::now()).await.unwrap()
    };
    for r in [&push, &test] {
        let State::DecidedByAnt(by, decision) = state_of(r).await else { panic!("{} decided", r.tool.title) };
        assert_eq!(
            (by, decision.on_behalf_of, decision.verdict),
            (pa.clone(), Some(alice.clone()), Verdict::AllowOnce)
        );
    }
    assert_eq!(state_of(&rm).await, State::Pending, "denied, waiting for a decision");
    assert!(tools::held(&js, &stream, Utc::now()).await.unwrap().is_empty(), "nothing waits for alice");

    let act = |r: &ApprovalRequest| Act::Decision { ant: dev_a.clone(), request_id: r.id, verdict: Verdict::AllowOnce };
    tokio::time::timeout(Duration::from_secs(5), async {
        while reader.answer(&alice, &pa, &act(&rm)).is_none() || reader.pre_allow(&alice, &pa).is_none() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the daemons' reader verifies alice's records");
    assert_eq!(reader.answer(&alice, &pa, &act(&push)), Some(ActAnswer::Confirm));
    assert_eq!(reader.answer(&alice, &pa, &act(&rm)), Some(ActAnswer::Deny));
    assert_eq!(reader.pre_allow(&alice, &pa).unwrap().rules, ["dev-a/execute:cargo test*"]);

    consent::allow(&nats, vec!["dev-a/execute:cargo test*".into()], true, false).await.unwrap();
    assert_eq!(state_of(&test).await, State::Pending, "the rule removed");
}

/// pa orders for alice: m1's daemon holds each order until alice confirms it, refuses the one she denies, and
/// applies at once the one a pre-allow rule of hers covers.
#[tokio::test]
async fn an_assistant_s_admin_orders_wait_for_its_human_to_consent() {
    use ants_daemon::admin::ConfigFile;
    use ants_proto::{AdminOp, AdminOrder, OrderState};

    use crate::test_support::orders::{WAIT, host_of_m1};

    let Some(server) = Server::start() else { return };
    let (alice, pa, dev_a) = (p("alice"), p("pa"), p("dev-a"));
    let creds = server.dir().join("alice-admin.creds");
    let device = auth::Registered { label: Some("laptop"), admin: true, ..auth::Registered::default() };
    let rights = ants_proto::Permissions::for_user(&alice, Role::Human);
    auth::Issue::new(&server.dir().join("auth"), &alice, issued::Kind::Human, rights)
        .with(&device)
        .into_file(&creds)
        .unwrap();
    let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
    bus::ensure_stream(&js).await.unwrap();
    js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
    js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    humans::ensure(&js).await.unwrap();
    let assistant = server.dir().join("pa-assistant.creds");
    let new = auth::Registered { assistant_of: Some(alice.clone()), ..auth::Registered::default() };
    let rights = ants_proto::Permissions::for_assistant(&pa, &alice);
    auth::Issue::new(&server.dir().join("auth"), &pa, issued::Kind::Assistant, rights)
        .with(&new)
        .into_file(&assistant)
        .unwrap();
    let (_, pa_js) = bus::connect(&server.url, Some(&assistant), None, &pa, "test").await.unwrap();
    put(&js, &creds, &alice, Some(pa.clone()), Sealing::Keep).await.unwrap();

    let path = server.dir().join("config.toml");
    let text = "harness = \"copilot\"\n\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n";
    std::fs::write(&path, text).unwrap();
    let triggers = crate::reload::Triggers::new(path.clone(), None, Duration::from_secs(3600)).unwrap();
    let (ants, task) = host_of_m1(&server, &js, true, Some(ConfigFile::new(path.clone(), triggers.edits()))).await;

    let store = ants_daemon::mcp::jetstream_store(pa_js, None, assistant);
    let send = async |op: AdminOp| {
        let order =
            AdminOrder::new(pa.clone(), "m1".parse().unwrap(), dev_a.clone(), op, "").on_behalf(Some(alice.clone()));
        store.send_order(order).await.unwrap()
    };
    let comes_to = async |order: &AdminOrder, state: OrderState| {
        tokio::time::timeout(WAIT, async {
            loop {
                match crate::admin::orders::outcome(&js, order).await.unwrap() {
                    Some(o) if o.state == state => return o,
                    _ => tokio::time::sleep(Duration::from_millis(20)).await,
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{} comes to {state:?}", order.op.describe()))
    };
    let nats = Nats::new(&server.url, Some(&creds), None).unwrap();
    let act = |o: &AdminOrder| format!("m1/dev-a/{}", o.id);

    let pause = send(AdminOp::Pause).await;
    let held = comes_to(&pause, OrderState::Held).await;
    assert_eq!(held.on_behalf_of.as_ref(), Some(&alice));
    assert_eq!(std::fs::read_to_string(&path).unwrap(), text, "held, not applied");
    Box::pin(consent::confirm(&nats, Some(&act(&pause)), false)).await.unwrap();
    comes_to(&pause, OrderState::Applied).await;
    assert_eq!(std::fs::read_to_string(&path).unwrap(), format!("{text}paused = true\n"));

    let resume = send(AdminOp::Resume).await;
    comes_to(&resume, OrderState::Held).await;
    Box::pin(consent::confirm(&nats, Some(&act(&resume)), true)).await.unwrap();
    let denied = comes_to(&resume, OrderState::Refused).await;
    assert!(denied.reason.as_deref().unwrap_or_default().contains("denied by alice"), "{denied:?}");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), format!("{text}paused = true\n"));

    consent::allow(&nats, vec!["dev-a/order:resume".into()], false, false).await.unwrap();
    let resume = send(AdminOp::Resume).await;
    comes_to(&resume, OrderState::Applied).await;
    assert_eq!(std::fs::read_to_string(&path).unwrap(), text);
    drop(ants);
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
}
