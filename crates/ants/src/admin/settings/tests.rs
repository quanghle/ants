use ants_daemon::admin::ConfigFile;
use ants_proto::{FieldChange, Hidden, Permissions, Role};
use serde_json::json;

use super::*;
use crate::test_support::Server;
use ants_daemon::Harnesses;

use crate::auth::{NewDevice, issue_device_with, issue_host};
use crate::credentials::revoke::Kind;
use crate::test_support::orders::{WAIT, host_of_m1};

fn a(text: &str) -> Assignment {
    text.parse().unwrap()
}

fn field(text: &str) -> Vec<String> {
    text.split('.').map(str::to_owned).collect()
}

/// The shared settings of `ant` as the CLI reads them, which must verify.
async fn verified_of(nats: &Nats<'_>, js: &jetstream::Context, ant: &UserId) -> Option<SharedSettings> {
    shared_of(nats, js, ant).await.unwrap().map(|found| match found {
        SharedRecord::Verified(record) => record,
        SharedRecord::Unverified(_, why) => panic!("not verified: {why}"),
    })
}

/// A connection with host credentials of `machine`, as a host daemon has.
async fn host_js(server: &Server, machine: &Machine) -> jetstream::Context {
    let creds = server.dir().join(format!("host-{machine}.creds"));
    issue_host(&server.dir().join("auth"), machine, None, &creds).unwrap();
    let client = async_nats::ConnectOptions::with_credentials_file(&creds)
        .await
        .unwrap()
        .custom_inbox_prefix(ants_proto::host_inbox_prefix(machine))
        .connect(&server.url)
        .await
        .unwrap();
    jetstream::new(client)
}

#[test]
fn an_assignment_reads_its_value_as_toml_else_as_a_string_and_removes_with_none() {
    assert_eq!(a("model=gpt-5"), Assignment { field: field("model"), value: Some(json!("gpt-5")) });
    assert_eq!(a("caps.daily = 2.5").value, Some(json!(2.5)));
    assert_eq!(a("allow=[\"read\", \"search\"]").value, Some(json!(["read", "search"])));
    assert_eq!(a("env = { RUST_LOG = \"info\" }").value, Some(json!({ "RUST_LOG": "info" })));
    assert_eq!(a("workdir=\"~/src/a=b\"").value, Some(json!("~/src/a=b")));
    assert_eq!(a("harnesses.copilot.command=copilot").field, field("harnesses.copilot.command"));
    let quoted = a("env.\"A=B\"=1");
    assert_eq!((quoted.field, quoted.value), (vec!["env".to_owned(), "A=B".to_owned()], Some(json!(1))));
    assert_eq!(a("model=").value, None);
    assert!("model".parse::<Assignment>().is_err());
    assert!("=x".parse::<Assignment>().is_err());
    assert!("allow=[\"read\"".parse::<Assignment>().is_err(), "a broken array is no string");
}

#[test]
fn the_order_expects_each_field_as_read_and_a_secret_by_its_digest() {
    let mut fields = serde_json::Map::new();
    fields.insert("model".to_owned(), json!("gpt-5"));
    fields.insert("env".to_owned(), json!({ "API_TOKEN": "[redacted]" }));
    let hidden = vec![Hidden { field: field("env.API_TOKEN"), digest: "d1".to_owned() }];
    let read = Settings { fields, hidden };
    let AdminOp::Set { changes } = order(&read, None, &[a("model=o3"), a("env.API_TOKEN=new"), a("workdir=/w")]) else {
        panic!("a set")
    };
    assert_eq!(
        changes,
        [
            FieldChange {
                field: field("model"),
                expected: Some(json!("gpt-5")),
                expected_digest: None,
                value: Some(json!("o3"))
            },
            FieldChange {
                field: field("env.API_TOKEN"),
                expected: None,
                expected_digest: Some("d1".to_owned()),
                value: Some(json!("new"))
            },
            FieldChange { field: field("workdir"), expected: None, expected_digest: None, value: Some(json!("/w")) },
        ]
    );
}

const FILE: &str = r#"harness = "copilot"

[harnesses.custom]
command = "custom-acp"

[[ant]]
name = "dev-a"
workdir = "."
model = "gpt-5"  # fast enough
env = { RUST_LOG = "info", API_TOKEN = "s3cr3t-value-42" }
"#;

/// An admin reads an ant's settings, secrets hidden, and changes them, a secret and a harness's command
/// included, expecting the values read: a field changed meanwhile has the order refused, naming it. A human
/// without the admin role sees them as last read. While the machine's daemon is away, a read is sent once and
/// waits, as does a change, both pending, until the daemon returns and applies them.
#[tokio::test]
async fn an_admin_reads_the_settings_and_changes_them_expecting_what_was_read() {
    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a) = ("alice".parse().unwrap(), "bob".parse().unwrap(), "dev-a".parse().unwrap());
    let (_, h_js) = server.connect_as(&bob, Role::Human).await;
    ants_daemon::bus::ensure_stream(&h_js).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    let admin_creds = server.dir().join("alice-admin.creds");
    let device = NewDevice { label: Some("laptop"), admin: true, ..NewDevice::default() };
    let rights = Permissions::for_user(&alice, Role::Human);
    issue_device_with(&server.dir().join("auth"), &alice, Kind::Human, rights, &device, &admin_creds).unwrap();
    let admin = Nats::new(&server.url, Some(&admin_creds), None).unwrap();
    assert!(may_order(&admin));
    let (_, js) = admin.connect("test").await.unwrap();
    let path = server.dir().join("config.toml");
    std::fs::write(&path, FILE).unwrap();
    let triggers = crate::reload::Triggers::new(path.clone(), None, Duration::from_secs(3600)).unwrap();
    let config = || Some(ConfigFile::new(path.clone(), triggers.edits()));
    let (ants, task) = host_of_m1(&server, &js, true, config()).await;

    let reading = read(&admin, &js, &dev_a, WAIT).await.unwrap();
    assert_eq!(reading.stale, None);
    let first = reading.read.expect("read afresh");
    assert_eq!(first.machine.as_str(), "m1");
    let lines = first.lines();
    assert!(lines.contains(&"model = \"gpt-5\"".to_owned()), "{lines:?}");
    assert!(lines.contains(&"env.API_TOKEN = \"[redacted]\"  (secret)".to_owned()), "{lines:?}");
    assert!(lines.contains(&"harnesses.custom.command = \"custom-acp\"".to_owned()), "{lines:?}");
    let recorded = serde_json::to_string(&reading.orders).unwrap();
    assert!(!recorded.contains("s3cr3t"), "the outcome hides the secret: {recorded}");

    let changes: Vec<Assignment> = ["model=o3", "env.API_TOKEN=n3w-s3cr3t-value", "harnesses.custom.command=other"]
        .iter()
        .map(|a| a.parse().unwrap())
        .collect();
    let op = order(&first.settings, None, &changes);
    let (_, applied) = orders::send(&admin, &js, Some(first.machine.clone()), &dev_a, op, WAIT).await.unwrap();
    assert_eq!(applied.map(|o| (o.state, o.reason)), Some((OrderState::Applied, None)));
    let edited = std::fs::read_to_string(&path).unwrap();
    assert!(edited.contains("model = \"o3\"  # fast enough"), "{edited}");
    assert!(edited.contains("API_TOKEN = \"n3w-s3cr3t-value\"") && edited.contains("command = \"other\""));

    std::fs::write(&path, edited.replace("\"o3\"", "\"o4\"")).unwrap();
    let stale = order(&first.settings, None, &["model=o5".parse().unwrap()]);
    let (_, refused) = orders::send(&admin, &js, None, &dev_a, stale, WAIT).await.unwrap();
    let refused = refused.expect("decided at once");
    assert_eq!(refused.state, OrderState::Refused);
    assert!(refused.reason.unwrap().starts_with("model changed since the order was sent"));

    let fresh = read(&admin, &js, &dev_a, WAIT).await.unwrap();
    assert_eq!(fresh.read.unwrap().settings.get(&["model".to_owned()]), Some(&json!("o4")));
    let bob_creds = server.creds_as(&bob, Role::Human);
    let bob_nats = Nats::new(&server.url, Some(&bob_creds), None).unwrap();
    let as_bob = read(&bob_nats, &js, &dev_a, WAIT).await.unwrap();
    assert!(as_bob.stale.unwrap().starts_with("as last read"));
    let last = as_bob.read.expect("as last read");
    assert_eq!(last.settings.get(&["model".to_owned()]), Some(&json!("o4")), "the last read");

    drop(ants);
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
    let away = read(&admin, &js, &dev_a, Duration::ZERO).await.unwrap();
    assert_eq!(away.stale.as_deref(), Some("the read waits for the daemon of m1"));
    assert_eq!(away.read.as_ref().map(|r| r.order), Some(last.order), "the last read meanwhile");
    let again = read(&admin, &js, &dev_a, Duration::ZERO).await.unwrap();
    assert!(again.stale.unwrap().contains("waits for the daemon of m1"));
    let reads = again.orders.iter().filter(|o| o.op == Some(AdminOp::ReadSettings)).count();
    assert_eq!(reads, away.orders.iter().filter(|o| o.op == Some(AdminOp::ReadSettings)).count(), "sent once");
    let workdir = server.dir().display().to_string();
    let moved = Assignment { field: vec!["workdir".to_owned()], value: Some(json!(workdir)) };
    let op = order(&last.settings, None, &[moved]);
    let (pending, none) = orders::send(&admin, &js, None, &dev_a, op, Duration::ZERO).await.unwrap();
    assert_eq!(none, None);
    let listed = orders::orders_of(&js, &dev_a).await.unwrap();
    assert_eq!(listed.iter().find(|o| o.id == pending.id).map(|o| o.state), Some(OrderState::Pending));

    let (ants, task) = host_of_m1(&server, &js, true, config()).await;
    let decided = tokio::time::timeout(WAIT, async {
        loop {
            if let Some(outcome) = orders::outcome(&js, &pending).await.unwrap() {
                return outcome;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    let decided = decided.await.expect("decided once back");
    assert_eq!(decided.state, OrderState::Applied, "{:?}", decided.reason);
    let back = read(&admin, &js, &dev_a, WAIT).await.unwrap();
    assert_eq!(back.read.unwrap().settings.get(&["workdir".to_owned()]), Some(&json!(workdir)));
    drop(ants);
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
}

/// The first daemon of an ant to start creates its shared settings from its config; an admin's change of one
/// lands in them, and in the config where it sets the field, and the daemon serves it, core memories included. A
/// standby machine serves the shared settings over its own config's, telling which fields drift, as does the
/// first's record of its machine once its config is edited otherwise.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one scenario: the settings created, changed by orders, then served everywhere"
)]
async fn the_shared_settings_are_created_once_changed_by_orders_and_served_on_every_machine() {
    let Some(server) = Server::start() else { return };
    let (alice, dev_a) = ("alice".parse().unwrap(), "dev-a".parse::<UserId>().unwrap());
    let (_, h_js) = server.connect_as(&alice, Role::Human).await;
    ants_daemon::bus::ensure_stream(&h_js).await.unwrap();
    for config in [ants_daemon::hub::machines_config(), ants_daemon::hub::devices_config()] {
        h_js.create_key_value(config).await.unwrap();
    }
    ants_daemon::hub::create_identities(&h_js).await.unwrap();
    let admin_creds = server.dir().join("alice-admin.creds");
    let device = NewDevice { label: Some("laptop"), admin: true, ..NewDevice::default() };
    let rights = Permissions::for_user(&alice, Role::Human);
    issue_device_with(&server.dir().join("auth"), &alice, Kind::Human, rights, &device, &admin_creds).unwrap();
    let admin = Nats::new(&server.url, Some(&admin_creds), None).unwrap();
    let (_, js) = admin.connect("test").await.unwrap();
    let path = server.dir().join("config.toml");
    let a_creds = server.creds_as(&dev_a, Role::Ant);
    let table = format!("workdir = \".\"\nwhoami = \"a tester\"\ncreds = {:?}", a_creds.display().to_string());
    std::fs::write(&path, FILE.replace("workdir = \".\"", &table)).unwrap();
    let triggers = crate::reload::Triggers::new(path.clone(), None, Duration::from_secs(3600)).unwrap();
    let config = ConfigFile::new(path.clone(), triggers.edits());
    let shared = config.shared.clone();
    let mut changes = shared.subscribe();
    shared.listed(&ants_daemon::config::load(&path).unwrap().ants);
    let (ants, task) = host_of_m1(&server, &js, true, Some(config)).await;
    assert!(shared.wait_polled(WAIT).await, "the host reads the shared settings");

    let created = verified_of(&admin, &js, &dev_a).await.expect("created from the config");
    assert_eq!(serde_json::Value::Object(created.fields.clone()), json!({ "model": "gpt-5", "whoami": "a tester" }));
    assert_eq!((created.machine.as_str(), created.by.as_ref()), ("m1", None));
    let accounts = vec![ants_daemon::secrets::creds::account(&a_creds).unwrap()];

    let reading = read(&admin, &js, &dev_a, WAIT).await.unwrap();
    assert_eq!(reading.verified_shared(), Some(&created));
    let lines = sections(&dev_a, &reading, Utc::now());
    assert!(lines.contains(&"  model = \"gpt-5\"  (ignored: shared)".to_owned()), "{lines:?}");
    let settings = &reading.read.as_ref().unwrap().settings;
    let op = order(settings, reading.verified_shared(), &[a("model=o3"), a("memory=notes")]);
    changes.mark_unchanged();
    let (_, applied) = orders::send(&admin, &js, None, &dev_a, op, WAIT).await.unwrap();
    assert_eq!(applied.map(|o| (o.state, o.reason)), Some((OrderState::Applied, None)));
    let changed = verified_of(&admin, &js, &dev_a).await.unwrap();
    assert_eq!(
        serde_json::Value::Object(changed.fields.clone()),
        json!({ "model": "o3", "whoami": "a tester", "memory": "notes" })
    );
    assert_eq!(changed.by, Some(alice.clone()));
    assert!(changes.has_changed().unwrap(), "the daemon reloads with the change");
    let edited = std::fs::read_to_string(&path).unwrap();
    assert!(edited.contains("model = \"o3\"  # fast enough"), "the config sets the model too: {edited}");
    assert!(!edited.contains("memory"), "a field the config leaves out stays out: {edited}");

    let loaded = ants_daemon::config::load(&path).unwrap();
    let served = crate::serve::configs(loaded.ants, &loaded.harnesses, &server.url, None, Some(&shared));
    let served = served.unwrap().remove(0);
    assert_eq!(
        (served.harness.model_settings.model.as_deref(), served.drift.get()),
        (Some("o3"), Vec::<String>::new())
    );
    let (_, read_memories) =
        orders::send(&admin, &js, None, &dev_a, AdminOp::ReadMemories { identity: None }, WAIT).await.unwrap();
    let memories: ants_proto::Memories = serde_json::from_value(read_memories.unwrap().result.unwrap()).unwrap();
    assert_eq!(
        (memories.core.whoami.as_deref(), memories.core.memory.as_deref()),
        (Some("a tester"), Some("notes")),
        "the core memories are the shared ones"
    );

    let standby = ants_daemon::profile::shared::Shared::default();
    let mut own = ants_daemon::config::Ant::new(dev_a.clone(), server.dir().to_path_buf());
    (own.model, own.harness) = (Some("gpt-4".to_owned()), Some("copilot".to_owned()));
    standby.listed(std::slice::from_ref(&own));
    let m2: Machine = "m2".parse().unwrap();
    let m2_js = host_js(&server, &m2).await;
    let m2_kv = DirectKv::new(&m2_js, None, IDENTITIES_BUCKET).await.unwrap();
    let m2_devices = DirectKv::new(&m2_js, None, DEVICES_BUCKET).await.unwrap();
    let m2_bucket = Bucket { identities: &m2_kv, devices: &m2_devices, accounts: &accounts };
    ants_daemon::profile::shared::refresh(m2_bucket, Some(&m2), &standby, std::slice::from_ref(&dev_a)).await;
    assert_eq!(verified_of(&admin, &js, &dev_a).await, Some(changed), "the first record stays");
    let taken = crate::serve::configs(vec![own], &Harnesses::builtin(), &server.url, None, Some(&standby));
    let taken = taken.unwrap().remove(0);
    assert_eq!(
        (taken.harness.model_settings.model.as_deref(), taken.drift.get()),
        (Some("o3"), vec!["model".to_owned()])
    );
    assert_eq!(ants_daemon::colony::entry(&taken).await.drift, ["model"], "the colony tells the drift");

    std::fs::write(&path, edited.replace("\"o3\"", "\"o4\"")).unwrap();
    shared.listed(&ants_daemon::config::load(&path).unwrap().ants);
    ants.send_replace(vec![dev_a.clone()]);
    tokio::time::timeout(WAIT, async {
        loop {
            let records = crate::machines::list(&js).await.unwrap();
            if records.iter().any(|r| r.drift.get(&dev_a).is_some_and(|d| d == &["model"])) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the machine's record tells the drift");

    // m2's host credentials may write dev-a's record, but not sign it as dev-a: m1 writes its config's over it.
    let forged =
        SharedSettings::new(dev_a.clone(), serde_json::Map::from_iter([("model".into(), json!("evil"))]), m2, None);
    m2_kv.put(forged.key(), serde_json::to_vec(&forged).unwrap()).await.unwrap();
    ants_daemon::profile::shared::refresh(m2_bucket, None, &standby, std::slice::from_ref(&dev_a)).await;
    // m1 may have mended the record already; the forged one is never applied.
    let applied = standby.record(&dev_a);
    assert!(applied.is_none_or(|r| r.fields.get("model") != Some(&json!("evil"))), "not applied");
    ants.send_replace(vec![dev_a.clone()]);
    let mended = tokio::time::timeout(WAIT, async {
        loop {
            if let Some(record) = shared.record(&dev_a).filter(|r| r.fields.get("model") == Some(&json!("o4"))) {
                return record;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    let mended = mended.await.expect("m1 writes its config's over the forged record");
    assert_eq!(mended.machine.as_str(), "m1");
    assert_eq!(verified_of(&admin, &js, &dev_a).await, Some(mended.clone()));

    // A daemon without host credentials reads the record as the ant, and writes none.
    let follower = ants_daemon::profile::shared::Shared::default();
    let mut own = ants_daemon::config::Ant::new(dev_a.clone(), server.dir().to_path_buf());
    own.creds = Some(a_creds.clone());
    follower.listed(std::slice::from_ref(&own));
    let via = ants_daemon::profile::shared::Via {
        nats: server.url.clone(),
        creds: Some(a_creds.clone()),
        js_domain: None,
        user: dev_a.clone(),
        ants: vec![dev_a.clone()],
    };
    let (vias, watched) = tokio::sync::watch::channel(vec![via]);
    let following = tokio::spawn(ants_daemon::profile::shared::follow(follower.clone(), watched, None));
    assert!(follower.wait_polled(WAIT).await, "the follower reads the shared settings");
    assert_eq!(follower.record(&dev_a), Some(mended));
    drop(vias);
    tokio::time::timeout(WAIT, following).await.unwrap().unwrap();
    drop(ants);
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
}

/// A record the ant signed, put back after it was replaced, however soon, and one signed with revoked credentials
/// are not verified, by a reader that never read the record replacing it too: `ants ant show` marks the record,
/// `ants ant set` expects the config's values over it, and the daemon holding the ant's credentials writes back the
/// settings it kept, or none with revoked credentials.
#[tokio::test]
async fn shared_settings_put_back_or_signed_with_revoked_credentials_are_not_verified() {
    let Some(server) = Server::start() else { return };
    let dev_a = "dev-a".parse::<UserId>().unwrap();
    let alice = "alice".parse::<UserId>().unwrap();
    let (_, h_js) = server.connect_as(&alice, Role::Human).await;
    h_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    ants_daemon::hub::create_identities(&h_js).await.unwrap();
    let alice_creds = server.creds_as(&alice, Role::Human);
    let human = Nats::new(&server.url, Some(&alice_creds), None).unwrap();
    let (_, js) = human.connect("test").await.unwrap();
    let a_creds = server.creds_as(&dev_a, Role::Ant);
    let accounts = vec![ants_daemon::secrets::creds::account(&a_creds).unwrap()];
    let m1: Machine = "m1".parse().unwrap();
    let m1_js = host_js(&server, &m1).await;
    let identities = DirectKv::new(&m1_js, None, IDENTITIES_BUCKET).await.unwrap();
    let devices = DirectKv::new(&m1_js, None, DEVICES_BUCKET).await.unwrap();
    let bucket = Bucket { identities: &identities, devices: &devices, accounts: &accounts };
    let writer = ants_daemon::profile::shared::Shared::default();
    let mut own = ants_daemon::config::Ant::new(dev_a.clone(), server.dir().to_path_buf());
    (own.model, own.creds) = (Some("gpt-5".to_owned()), Some(a_creds.clone()));
    writer.listed(std::slice::from_ref(&own));
    let refresh = || ants_daemon::profile::shared::refresh(bucket, Some(&m1), &writer, std::slice::from_ref(&dev_a));
    let put = |record: &SharedSettings| identities.put(record.key(), serde_json::to_vec(record).unwrap());
    let model_of = |record: &SharedSettings| record.fields.get("model").cloned();

    refresh().await;
    let first = verified_of(&human, &js, &dev_a).await.expect("created from the config");
    let fields = serde_json::Map::from_iter([("model".into(), json!("o3"))]);
    let mut second = SharedSettings::new(dev_a.clone(), fields, m1.clone(), Some(alice.clone()));
    let signer = ants_daemon::profile::shared::Signer::load(&dev_a, Some(&a_creds), &accounts).unwrap();
    assert!(ants_daemon::profile::shared::write(&identities, &mut second, &signer, None).await.unwrap());
    assert_eq!(verified_of(&human, &js, &dev_a).await.as_ref(), Some(&second));
    refresh().await;
    assert_eq!(writer.record(&dev_a), Some(second.clone()));

    put(&first).await.unwrap();
    let Some(SharedRecord::Unverified(record, why)) = shared_of(&human, &js, &dev_a).await.unwrap() else {
        panic!("a reader that never read the record replacing it takes one put back a moment after its signing")
    };
    assert_eq!(record, first);
    assert!(why.contains("put back"), "{why}");
    let starting = ants_daemon::profile::shared::Shared::default();
    ants_daemon::profile::shared::refresh(bucket, None, &starting, std::slice::from_ref(&dev_a)).await;
    assert_eq!(starting.record(&dev_a), None, "a daemon starting does not apply it either");
    let shared = Some(Box::new(SharedRecord::Unverified(record, why)));
    let reading = Reading { read: None, stale: None, orders: Vec::new(), shared };
    assert_eq!(reading.verified_shared(), None, "an order expects the config's values");
    let lines = sections(&dev_a, &reading, Utc::now());
    assert!(
        lines.iter().any(|l| l.starts_with("  shared, NOT VERIFIED (") && l.contains("daemons ignore it")),
        "{lines:?}"
    );
    assert!(lines.contains(&"  model = \"gpt-5\"".to_owned()), "{lines:?}");
    refresh().await;
    let restored = writer.record(&dev_a).expect("the settings kept, written back");
    assert_eq!((model_of(&restored), restored.by.as_ref()), (Some(json!("o3")), Some(&alice)));
    assert_ne!(restored, second, "signed anew");
    assert_eq!(verified_of(&human, &js, &dev_a).await, Some(restored.clone()));

    let team = server.dir().join("auth");
    crate::credentials::revoke::revoke(&team, Some("dev-a"), &[]).unwrap();
    crate::people::devices::record_revocations(&server.url, None, &team).await.unwrap();
    let Some(SharedRecord::Unverified(_, why)) = shared_of(&human, &js, &dev_a).await.unwrap() else {
        panic!("a record signed with revoked credentials verifies")
    };
    assert!(why.contains("revoked"), "{why}");
    let before = identities.lookup_stored(SharedSettings::key_of(&dev_a)).await.unwrap().1;
    refresh().await;
    assert_eq!(writer.record(&dev_a), None, "not applied");
    let after = identities.lookup_stored(SharedSettings::key_of(&dev_a)).await.unwrap().1;
    assert_eq!(after, before, "nothing written with the revoked credentials");
}

/// In a bucket without an incarnation marker, as `nats kv add` creates it, no record is signed or verified; `ants
/// init` marks it. After `ants init --identities-restored`, no record signed before verifies, and the daemon
/// holding the ant's credentials writes back the settings it kept.
#[tokio::test]
async fn shared_settings_of_a_bucket_without_a_marker_or_of_an_earlier_incarnation_are_not_verified() {
    let Some(server) = Server::start() else { return };
    let dev_a = "dev-a".parse::<UserId>().unwrap();
    let alice = "alice".parse::<UserId>().unwrap();
    let (_, h_js) = server.connect_as(&alice, Role::Human).await;
    h_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::identities_config()).await.unwrap();
    let alice_creds = server.creds_as(&alice, Role::Human);
    let human = Nats::new(&server.url, Some(&alice_creds), None).unwrap();
    let (_, js) = human.connect("test").await.unwrap();
    let a_creds = server.creds_as(&dev_a, Role::Ant);
    let accounts = vec![ants_daemon::secrets::creds::account(&a_creds).unwrap()];
    let m1: Machine = "m1".parse().unwrap();
    let m1_js = host_js(&server, &m1).await;
    let identities = DirectKv::new(&m1_js, None, IDENTITIES_BUCKET).await.unwrap();
    let devices = DirectKv::new(&m1_js, None, DEVICES_BUCKET).await.unwrap();
    let bucket = Bucket { identities: &identities, devices: &devices, accounts: &accounts };
    let writer = ants_daemon::profile::shared::Shared::default();
    let mut own = ants_daemon::config::Ant::new(dev_a.clone(), server.dir().to_path_buf());
    (own.model, own.creds) = (Some("gpt-5".to_owned()), Some(a_creds.clone()));
    writer.listed(std::slice::from_ref(&own));
    let refresh = || ants_daemon::profile::shared::refresh(bucket, Some(&m1), &writer, std::slice::from_ref(&dev_a));
    let signer = ants_daemon::profile::shared::Signer::load(&dev_a, Some(&a_creds), &accounts).unwrap();
    let sequence = || async { identities.lookup_stored(SharedSettings::key_of(&dev_a)).await.unwrap().1 };

    refresh().await;
    assert_eq!((writer.record(&dev_a), sequence().await), (None, 0), "none written");
    let fields = serde_json::Map::from_iter([("model".into(), json!("o3"))]);
    let mut set = SharedSettings::new(dev_a.clone(), fields, m1.clone(), Some(alice.clone()));
    let unmarked = ants_daemon::profile::shared::write(&identities, &mut set, &signer, None).await.unwrap_err();
    assert!(format!("{unmarked:#}").contains("ants init"), "{unmarked:#}");

    crate::cmd::init::hub(&human, false).await.unwrap();
    refresh().await;
    assert_eq!(
        verified_of(&human, &js, &dev_a).await.and_then(|r| r.fields.get("model").cloned()),
        Some(json!("gpt-5"))
    );
    assert!(ants_daemon::profile::shared::write(&identities, &mut set, &signer, None).await.unwrap());
    refresh().await;
    assert_eq!(writer.record(&dev_a), Some(set.clone()));

    let lines = crate::cmd::init::hub(&human, true).await.unwrap();
    assert!(lines.last().unwrap().contains("new incarnation"), "{lines:?}");
    let Some(SharedRecord::Unverified(record, why)) = shared_of(&human, &js, &dev_a).await.unwrap() else {
        panic!("a record of the bucket's earlier incarnation verifies")
    };
    assert_eq!(record, set);
    assert!(why.contains("incarnation"), "{why}");
    refresh().await;
    let restored = writer.record(&dev_a).expect("the settings kept, written back");
    assert_eq!((restored.fields.get("model"), restored.by.as_ref()), (Some(&json!("o3")), Some(&alice)));
    assert_eq!(verified_of(&human, &js, &dev_a).await, Some(restored));

    let store = DirectKv::whole(&h_js, None, IDENTITIES_BUCKET).await.unwrap();
    store.delete(ants_proto::INCARNATION_KEY).await.unwrap();
    let Some(SharedRecord::Unverified(_, why)) = shared_of(&human, &js, &dev_a).await.unwrap() else {
        panic!("a record of a bucket without a marker verifies")
    };
    assert!(why.contains("ants init"), "{why}");
    let before = sequence().await;
    refresh().await;
    assert_eq!((writer.record(&dev_a), sequence().await), (None, before), "none applied or written");
}
