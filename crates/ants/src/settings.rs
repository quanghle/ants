//! An ant's settings, as the config file of the machine serving it has them: read with an admin order
//! ([`AdminOp::ReadSettings`]) whose outcome every human reads, else as last read; and changed with one
//! ([`AdminOp::Set`]) expecting the values read, so the daemon refuses it should a field have changed meanwhile
//! (`ants ant show`, `ants ant set`, the settings pane of the ant view of `ants chat`). The ant's shared settings
//! ([`SHARED_FIELDS`]), the same on every machine, are read from their record on the bus ([`shared_of`]), and a change
//! of one expects the record's value.

use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use ants_daemon::config::edit;
#[cfg(doc)]
use ants_proto::SHARED_FIELDS;
use ants_proto::{
    ADMIN_TAG, AdminOp, IDENTITIES_BUCKET, Machine, OrderState, Settings, SharedSettings, UserId, Uuid, is_shared_field,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

use crate::orders::{self, Listed};
use crate::roster::{short, since};
use crate::{Nats, auth};

/// How long a read of the settings waits for the daemon of the ant's machine before taking the last read.
pub const READ_WAIT: Duration = Duration::from_secs(5);

/// How many of the latest admin orders to an ant `ants ant show` lists.
const ORDERS_SHOWN: usize = 10;

/// How long `ants ant set` waits for the outcome of its order.
pub const SET_WAIT: Duration = Duration::from_secs(10);

/// A change asked for as `KEY=VALUE`: the field `KEY` names, dotted as a TOML key (`model`, `caps.daily`,
/// `env.RUST_LOG`, `harnesses.copilot.command`), set to `VALUE` read as a TOML value, else as a string; or, with
/// nothing after `=`, removed.
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub field: Vec<String>,
    /// None to remove the field.
    pub value: Option<Value>,
}

impl FromStr for Assignment {
    type Err = anyhow::Error;

    fn from_str(text: &str) -> Result<Self> {
        let at = equals(text).with_context(|| format!("{text:?} sets nothing: KEY=VALUE, or KEY= to remove"))?;
        let (key, value) = (text[..at].trim(), text[at + 1..].trim());
        let field = edit::parse_field(key).with_context(|| format!("{key:?} is no setting's key"))?;
        let value = match value {
            "" => None,
            value => Some(match edit::parse_value(value) {
                Ok(value) => value,
                // A bare word reads as a string: `model=gpt-5`.
                Err(_) if !value.starts_with(['"', '\'', '[', '{']) => Value::String(value.to_owned()),
                Err(e) => return Err(e).with_context(|| format!("{value} is no TOML value")),
            }),
        };
        Ok(Self { field, value })
    }
}

/// Where the first `=` outside quotes is.
fn equals(text: &str) -> Option<usize> {
    let mut quote = None;
    for (at, c) in text.char_indices() {
        match (quote, c) {
            (None, '"' | '\'') => quote = Some(c),
            (Some(q), _) if c == q => quote = None,
            (None, '=') => return Some(at),
            _ => {}
        }
    }
    None
}

/// The order making `assignments` to the settings `read` has, each expecting the value read: that of the ant's
/// `shared` settings for one of those, when it has a record of them. The one place changes asked for become an order.
pub fn order(read: &Settings, shared: Option<&SharedSettings>, assignments: &[Assignment]) -> AdminOp {
    let changes = assignments
        .iter()
        .map(|a| match (a.field.as_slice(), shared) {
            ([key], Some(shared)) if is_shared_field(key) => ants_proto::FieldChange {
                field: a.field.clone(),
                expected: shared.fields.get(key).cloned(),
                expected_digest: None,
                value: a.value.clone(),
            },
            _ => read.change(a.field.clone(), a.value.clone()),
        })
        .collect();
    AdminOp::Set { changes }
}

/// The shared settings of `ant`, read with a human's credentials; none while it has no record of them.
pub async fn shared_of(js: &jetstream::Context, ant: &UserId) -> Result<Option<SharedSettings>> {
    let key = SharedSettings::key_of(ant);
    let filter = format!("$KV.{IDENTITIES_BUCKET}.{key}");
    let found = crate::machines::latest_in(js, IDENTITIES_BUCKET, &filter).await?;
    let Some((_, payload)) = found.into_iter().find(|(k, _)| *k == key) else { return Ok(None) };
    let record: SharedSettings = serde_json::from_slice(&payload).context("reading the ant's shared settings")?;
    record.check_key(&key)?;
    Ok(Some(record))
}

/// One `KEY = VALUE` line per field of `shared`.
pub fn shared_lines(shared: &SharedSettings) -> Vec<String> {
    shared.fields.iter().map(|(key, value)| format!("{key} = {}", edit::toml_text(value))).collect()
}

/// The settings as a read order's outcome carries them.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Read {
    /// The machine whose config file has them.
    pub machine: Machine,
    /// The read order.
    pub order: Uuid,
    pub read_at: DateTime<Utc>,
    pub settings: Settings,
}

impl Read {
    /// The last read of the settings among `orders`, oldest first, that the daemon applied.
    pub fn last(orders: &[Listed]) -> Option<Self> {
        orders.iter().rev().find_map(|o| {
            if o.op != Some(AdminOp::ReadSettings) || o.state != OrderState::Applied {
                return None;
            }
            let settings = serde_json::from_value(o.result.clone()?).ok()?;
            Some(Self { machine: o.machine.clone(), order: o.id, read_at: o.decided_at?, settings })
        })
    }

    /// One `KEY = VALUE` line per field, as TOML has them, `(secret)` after those hiding one.
    pub fn lines(&self) -> Vec<String> {
        self.settings
            .leaves()
            .into_iter()
            .map(|(field, value)| {
                let secret = if self.settings.digest(&field).is_some() { "  (secret)" } else { "" };
                format!("{} = {}{secret}", edit::field_text(&field), edit::toml_text(value))
            })
            .collect()
    }
}

/// The settings read and the orders to the ant.
#[derive(Debug, Clone, Default)]
pub struct Reading {
    /// Afresh, or with `stale` set, as last read; none when never read.
    pub read: Option<Read>,
    /// Why the settings are not read afresh, if they are not.
    pub stale: Option<String>,
    /// The orders to the ant, oldest first.
    pub orders: Vec<Listed>,
    /// The ant's shared settings; none while it has none, or they could not be read.
    pub shared: Option<Box<SharedSettings>>,
}

/// The settings and orders sections of `ants ant show`, as lines.
pub fn sections(ant: &UserId, reading: &Reading, now: DateTime<Utc>) -> Vec<String> {
    let mut out = vec![String::new(), format!("settings (`ants ant set {ant} KEY=VALUE...`):")];
    if let Some(shared) = &reading.shared {
        let ago = short(since(shared.updated_at, now));
        let by = shared.by.as_ref().map_or_else(String::new, |by| format!(" by {by}"));
        out.push(format!("  shared, the ant's on every machine: set{by} on {} {ago} ago", shared.machine));
        out.extend(shared_lines(shared).into_iter().map(|line| format!("  {line}")));
    }
    match &reading.read {
        Some(read) => {
            let ago = short(since(read.read_at, now));
            out.push(format!("  as config.toml on {} has them, read {ago} ago", read.machine));
            let shared = reading.shared.is_some();
            for line in read.lines() {
                let key = line.split([' ', '.']).next().unwrap_or_default();
                let ignored = if shared && is_shared_field(key) { "  (ignored: shared)" } else { "" };
                out.push(format!("  {line}{ignored}"));
            }
        }
        None => out.push("  never read".to_owned()),
    }
    out.extend(reading.stale.iter().map(|stale| format!("  {stale}")));
    out.extend([String::new(), format!("admin orders (`ants ant orders {ant}`):")]);
    let shown = &reading.orders[reading.orders.len().saturating_sub(ORDERS_SHOWN)..];
    if shown.is_empty() {
        out.push("  none".to_owned());
    } else {
        out.extend(orders::table(shown, now).lines().map(|line| format!("  {line}")));
    }
    if reading.orders.len() > shown.len() {
        out.push(format!("  and {} earlier", reading.orders.len() - shown.len()));
    }
    out
}

/// Whether the credentials of `nats` sign admin orders: a human's device's, carrying the admin role.
pub fn may_order(nats: &Nats<'_>) -> bool {
    match nats.creds {
        Some(creds) if nats.me.role.is_human() && nats.device.is_some() => admin_tagged(creds),
        _ => false,
    }
}

fn admin_tagged(creds: &Path) -> bool {
    let Ok((jwt, _)) = auth::read_creds(creds) else { return false };
    let Ok((_, claims)) = ants_proto::verified_jwt(&jwt) else { return false };
    claims["nats"]["tags"].as_array().is_some_and(|tags| tags.iter().any(|tag| tag.as_str() == Some(ADMIN_TAG)))
}

/// Read `ant`'s settings: with an admin's credentials, afresh, waiting up to `wait` for the daemon of its machine;
/// else, or when that daemon does not answer in time, refuses or is away, as last read. A read still pending is not
/// sent again.
pub async fn read(nats: &Nats<'_>, js: &jetstream::Context, ant: &UserId, wait: Duration) -> Result<Reading> {
    let mut orders = orders::orders_of(js, ant).await?;
    let now: DateTime<Utc> = SystemTime::now().into();
    let waiting = orders.iter().rev().find(|o| o.state == OrderState::Pending && o.op == Some(AdminOp::ReadSettings));
    let stale = if !may_order(nats) {
        Some("as last read: an admin's credentials read them afresh".to_owned())
    } else if let Some(waiting) = waiting {
        let ago = waiting.sent_at.map_or_else(String::new, |at| format!(" sent {} ago", short(since(at, now))));
        Some(format!("a read{ago} waits for the daemon of {}", waiting.machine))
    } else {
        match orders::send(nats, js, None, ant, AdminOp::ReadSettings, wait).await {
            Ok((sent, outcome)) => {
                orders = orders::orders_of(js, ant).await?;
                match outcome {
                    Some(o) if o.state == OrderState::Applied => None,
                    Some(o) => Some(format!(
                        "the daemon of {} refused the read: {}",
                        o.machine,
                        o.reason.as_deref().unwrap_or("no reason given")
                    )),
                    None => Some(format!("the read waits for the daemon of {}", sent.machine)),
                }
            }
            Err(e) => Some(format!("{e:#}")),
        }
    };
    let shared = shared_of(js, ant)
        .await
        .inspect_err(|e| tracing::debug!("no shared settings: {e:#}"))
        .ok()
        .flatten()
        .map(Box::new);
    Ok(Reading { read: Read::last(&orders), stale, orders, shared })
}

/// `ants ant set ANT KEY=VALUE...`: read `ant`'s settings, send the order of `assignments` expecting the values read,
/// and wait up to `wait` for its outcome.
pub async fn set(nats: &Nats<'_>, ant: &UserId, assignments: &[Assignment], wait: Duration) -> Result<()> {
    if !may_order(nats) {
        bail!("`ants ant set` needs a human device's admin credentials (`ants auth user NAME --human --admin`)");
    }
    let (_, js) = nats.connect("ants-ant-set").await?;
    let reading = read(nats, &js, ant, READ_WAIT).await?;
    let Some(read) = reading.read else {
        let why = reading.stale.unwrap_or_default();
        bail!("{ant}'s settings were never read, so no change can expect their values: {why}");
    };
    if let Some(stale) = &reading.stale {
        let ago = short(since(read.read_at, SystemTime::now().into()));
        eprintln!("expecting the settings as read {ago} ago: {stale}");
    }
    let op = order(&read.settings, reading.shared.as_deref(), assignments);
    let (sent, outcome) = orders::send(nats, &js, Some(read.machine.clone()), ant, op, wait).await?;
    println!("order {} sent to the daemon of {}", sent.id, read.machine);
    match outcome {
        Some(o) if o.state == OrderState::Applied => println!("applied"),
        Some(o) => bail!("refused: {}", o.reason.as_deref().unwrap_or("no reason given")),
        None if wait.is_zero() => {}
        None => println!("pending: the daemon of {} has not taken it yet; `ants ant orders {ant}` tells", read.machine),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use ants_daemon::admin::ConfigFile;
    use ants_proto::{FieldChange, Hidden, Permissions, Role, SettingsError};
    use serde_json::json;

    use super::*;
    use crate::auth::tests::Server;
    use ants_daemon::Harnesses;
    use ants_daemon::direct_kv::DirectKv;
    use ants_daemon::shared::{Bucket, Found};

    use crate::auth::{NewDevice, issue_device_with, issue_host};
    use crate::orders::tests::{WAIT, host_of_m1};
    use crate::revoke::Kind;

    fn a(text: &str) -> Assignment {
        text.parse().unwrap()
    }

    fn field(text: &str) -> Vec<String> {
        text.split('.').map(str::to_owned).collect()
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
        let AdminOp::Set { changes } = order(&read, None, &[a("model=o3"), a("env.API_TOKEN=new"), a("workdir=/w")])
        else {
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
                FieldChange {
                    field: field("workdir"),
                    expected: None,
                    expected_digest: None,
                    value: Some(json!("/w"))
                },
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
    #[allow(clippy::too_many_lines)]
    async fn the_shared_settings_are_created_once_changed_by_orders_and_served_on_every_machine() {
        let Some(server) = Server::start() else { return };
        let (alice, dev_a) = ("alice".parse().unwrap(), "dev-a".parse::<UserId>().unwrap());
        let (_, h_js) = server.connect_as(&alice, Role::Human).await;
        ants_daemon::bus::ensure_stream(&h_js).await.unwrap();
        for config in [
            ants_daemon::hub::machines_config(),
            ants_daemon::hub::devices_config(),
            ants_daemon::hub::identities_config(),
        ] {
            h_js.create_key_value(config).await.unwrap();
        }
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

        let created = shared_of(&js, &dev_a).await.unwrap().expect("created from the config");
        assert_eq!(
            serde_json::Value::Object(created.fields.clone()),
            json!({ "model": "gpt-5", "whoami": "a tester" })
        );
        assert_eq!((created.machine.as_str(), created.by.as_ref()), ("m1", None));
        let accounts = vec![ants_daemon::creds::account(&a_creds).unwrap()];
        created
            .verify(accounts.iter().map(String::as_str), |_| false, Utc::now())
            .expect("signed with dev-a's credentials");

        let reading = read(&admin, &js, &dev_a, WAIT).await.unwrap();
        assert_eq!(reading.shared.as_deref(), Some(&created));
        let lines = sections(&dev_a, &reading, Utc::now());
        assert!(lines.contains(&"  model = \"gpt-5\"  (ignored: shared)".to_owned()), "{lines:?}");
        let op = order(&reading.read.unwrap().settings, reading.shared.as_deref(), &[a("model=o3"), a("memory=notes")]);
        changes.mark_unchanged();
        let (_, applied) = orders::send(&admin, &js, None, &dev_a, op, WAIT).await.unwrap();
        assert_eq!(applied.map(|o| (o.state, o.reason)), Some((OrderState::Applied, None)));
        let changed = shared_of(&js, &dev_a).await.unwrap().unwrap();
        assert_eq!(
            serde_json::Value::Object(changed.fields.clone()),
            json!({ "model": "o3", "whoami": "a tester", "memory": "notes" })
        );
        assert_eq!(changed.by, Some(alice.clone()));
        changed
            .verify(accounts.iter().map(String::as_str), |_| false, Utc::now())
            .expect("signed with dev-a's credentials");
        assert!(changes.has_changed().unwrap(), "the daemon reloads with the change");
        let edited = std::fs::read_to_string(&path).unwrap();
        assert!(edited.contains("model = \"o3\"  # fast enough"), "the config sets the model too: {edited}");
        assert!(!edited.contains("memory"), "a field the config leaves out stays out: {edited}");

        let loaded = ants_daemon::config::load(&path).unwrap();
        let served = crate::serve::configs(loaded.ants, &loaded.harnesses, &server.url, None, None, Some(&shared));
        let served = served.unwrap().remove(0);
        assert_eq!((served.model_settings.model.as_deref(), served.drift.get()), (Some("o3"), Vec::<String>::new()));
        let (_, read_memories) = orders::send(&admin, &js, None, &dev_a, AdminOp::ReadMemories, WAIT).await.unwrap();
        let memories: ants_proto::Memories = serde_json::from_value(read_memories.unwrap().result.unwrap()).unwrap();
        assert_eq!(
            (memories.core.whoami.as_deref(), memories.core.memory.as_deref()),
            (Some("a tester"), Some("notes")),
            "the core memories are the shared ones"
        );

        let standby = ants_daemon::shared::Shared::default();
        let mut own = ants_daemon::config::Ant::new(dev_a.clone(), server.dir().to_path_buf());
        (own.model, own.harness) = (Some("gpt-4".to_owned()), Some("copilot".to_owned()));
        standby.listed(std::slice::from_ref(&own));
        let m2: Machine = "m2".parse().unwrap();
        let m2_js = host_js(&server, &m2).await;
        let m2_kv = DirectKv::new(&m2_js, None, IDENTITIES_BUCKET).await.unwrap();
        let m2_devices = DirectKv::new(&m2_js, None, ants_proto::DEVICES_BUCKET).await.unwrap();
        let m2_bucket = Bucket { identities: &m2_kv, devices: &m2_devices, accounts: &accounts };
        ants_daemon::shared::refresh(m2_bucket, Some(&m2), &standby, std::slice::from_ref(&dev_a)).await;
        assert_eq!(shared_of(&js, &dev_a).await.unwrap(), Some(changed), "the first record stays");
        let taken = crate::serve::configs(vec![own], &Harnesses::builtin(), &server.url, None, None, Some(&standby));
        let taken = taken.unwrap().remove(0);
        assert_eq!((taken.model_settings.model.as_deref(), taken.drift.get()), (Some("o3"), vec!["model".to_owned()]));
        assert_eq!(ants_daemon::roster::entry(&taken).await.drift, ["model"], "the roster tells the drift");

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
        ants_daemon::shared::refresh(m2_bucket, None, &standby, std::slice::from_ref(&dev_a)).await;
        assert_eq!(standby.record(&dev_a), None, "not applied");
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
        assert_eq!(shared_of(&js, &dev_a).await.unwrap(), Some(mended.clone()));

        // A daemon without host credentials reads the record as the ant, and writes none.
        let follower = ants_daemon::shared::Shared::default();
        let mut own = ants_daemon::config::Ant::new(dev_a.clone(), server.dir().to_path_buf());
        own.creds = Some(a_creds.clone());
        follower.listed(std::slice::from_ref(&own));
        let via = ants_daemon::shared::Via {
            nats: server.url.clone(),
            creds: Some(a_creds.clone()),
            js_domain: None,
            user: dev_a.clone(),
            ants: vec![dev_a.clone()],
        };
        let (vias, watched) = tokio::sync::watch::channel(vec![via]);
        let following = tokio::spawn(ants_daemon::shared::follow(follower.clone(), watched));
        assert!(follower.wait_polled(WAIT).await, "the follower reads the shared settings");
        assert_eq!(follower.record(&dev_a), Some(mended));
        drop(vias);
        tokio::time::timeout(WAIT, following).await.unwrap().unwrap();
        drop(ants);
        tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
    }

    /// A record the ant signed and the stream stored long after, one put back after the daemon read the record
    /// replacing it, and one signed with revoked credentials are not verified: the daemon holding the ant's
    /// credentials writes back the settings it kept, or none with revoked credentials.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn shared_settings_put_back_or_signed_with_revoked_credentials_are_not_verified() {
        let Some(server) = Server::start() else { return };
        let dev_a = "dev-a".parse::<UserId>().unwrap();
        let alice = "alice".parse::<UserId>().unwrap();
        let (_, h_js) = server.connect_as(&alice, Role::Human).await;
        for config in [ants_daemon::hub::devices_config(), ants_daemon::hub::identities_config()] {
            h_js.create_key_value(config).await.unwrap();
        }
        let a_creds = server.creds_as(&dev_a, Role::Ant);
        let accounts = vec![ants_daemon::creds::account(&a_creds).unwrap()];
        let m1: Machine = "m1".parse().unwrap();
        let m1_js = host_js(&server, &m1).await;
        let identities = DirectKv::new(&m1_js, None, IDENTITIES_BUCKET).await.unwrap();
        let devices = DirectKv::new(&m1_js, None, ants_proto::DEVICES_BUCKET).await.unwrap();
        let bucket = Bucket { identities: &identities, devices: &devices, accounts: &accounts };
        let writer = ants_daemon::shared::Shared::default();
        let mut own = ants_daemon::config::Ant::new(dev_a.clone(), server.dir().to_path_buf());
        (own.model, own.creds) = (Some("gpt-5".to_owned()), Some(a_creds.clone()));
        writer.listed(std::slice::from_ref(&own));
        let refresh = || ants_daemon::shared::refresh(bucket, Some(&m1), &writer, std::slice::from_ref(&dev_a));
        let signed = |model: &str, ago: chrono::TimeDelta| {
            let fields = serde_json::Map::from_iter([("model".into(), json!(model))]);
            let mut record = SharedSettings::new(dev_a.clone(), fields, m1.clone(), Some(alice.clone()));
            record.updated_at -= ago;
            ants_daemon::shared::sign_as(&mut record, Some(&a_creds), &accounts).unwrap();
            record
        };
        let put = |record: &SharedSettings| identities.put(record.key(), serde_json::to_vec(record).unwrap());
        let model_of = |record: &SharedSettings| record.fields.get("model").cloned();
        let stored = || async {
            match ants_daemon::shared::read(bucket, &dev_a).await.unwrap() {
                Found::Record(record, _) => Ok(record),
                Found::Unverified(record, why, _) => Err((*record, why)),
                _ => panic!("no record"),
            }
        };

        refresh().await;
        let first = stored().await.expect("created from the config");
        let second = signed("o3", chrono::TimeDelta::zero());
        put(&second).await.unwrap();
        refresh().await;
        assert_eq!(writer.record(&dev_a), Some(second.clone()));

        put(&first).await.unwrap();
        assert_eq!(
            stored().await,
            Ok(first.clone()),
            "a reader that never read the record replacing it takes one put back within minutes of its signing"
        );
        let replayed = writer.check(Found::Record(first.clone(), 0));
        assert!(matches!(replayed, Found::Unverified(_, SettingsError::Replayed, _)), "the writer read the newer one");
        refresh().await;
        let restored = writer.record(&dev_a).expect("the settings kept, written back");
        assert_eq!((model_of(&restored), restored.by.as_ref()), (Some(json!("o3")), Some(&alice)));
        assert_ne!(restored, second, "signed anew");
        assert_eq!(stored().await, Ok(restored.clone()));

        let old = signed("evil", chrono::TimeDelta::hours(1));
        put(&old).await.unwrap();
        let (record, why) = stored().await.expect_err("a record stored an hour after its signing verifies");
        assert_eq!((record, matches!(why, SettingsError::Stale { .. })), (old, true));
        refresh().await;
        assert_eq!(model_of(&stored().await.unwrap()), Some(json!("o3")), "written back");

        let team = server.dir().join("auth");
        crate::revoke::revoke(&team, Some("dev-a"), &[]).unwrap();
        crate::devices::record_revocations(&server.url, None, &team).await.unwrap();
        let (_, why) = stored().await.expect_err("a record signed with revoked credentials verifies");
        assert!(matches!(why, SettingsError::Revoked(_)), "{why}");
        let before = identities.lookup_stored(SharedSettings::key_of(&dev_a)).await.unwrap().1;
        refresh().await;
        assert_eq!(writer.record(&dev_a), None, "not applied");
        let after = identities.lookup_stored(SharedSettings::key_of(&dev_a)).await.unwrap().1;
        assert_eq!(after, before, "nothing written with the revoked credentials");
    }
}
