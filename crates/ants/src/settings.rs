//! An ant's settings, as the config file of the machine serving it has them: read with an admin order
//! ([`AdminOp::ReadSettings`]) whose outcome every human reads, else as last read; and changed with one
//! ([`AdminOp::Set`]) expecting the values read, so the daemon refuses it should a field have changed meanwhile
//! (`ants ant show`, `ants ant set`, the settings pane of the ant view of `ants chat`).

use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use ants_daemon::config::edit;
use ants_proto::{ADMIN_TAG, AdminOp, Machine, OrderState, Settings, UserId, Uuid};
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

/// The order making `assignments` to the settings `read` has, each expecting the value read. The one place changes
/// asked for become an order.
pub fn order(read: &Settings, assignments: &[Assignment]) -> AdminOp {
    let changes = assignments.iter().map(|a| read.change(a.field.clone(), a.value.clone())).collect();
    AdminOp::Set { changes }
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
}

/// The settings and orders sections of `ants ant show`, as lines.
pub fn sections(ant: &UserId, reading: &Reading, now: DateTime<Utc>) -> Vec<String> {
    let mut out = vec![String::new(), format!("settings (`ants ant set {ant} KEY=VALUE...`):")];
    match &reading.read {
        Some(read) => {
            let ago = short(since(read.read_at, now));
            out.push(format!("  as config.toml on {} has them, read {ago} ago", read.machine));
            out.extend(read.lines().into_iter().map(|line| format!("  {line}")));
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
    Ok(Reading { read: Read::last(&orders), stale, orders })
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
    let op = order(&read.settings, assignments);
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
    use ants_proto::{FieldChange, Hidden, Permissions, Role};
    use serde_json::json;

    use super::*;
    use crate::auth::tests::Server;
    use crate::auth::{NewDevice, issue_device_with};
    use crate::orders::tests::{WAIT, host_of_m1};
    use crate::revoke::Kind;

    fn a(text: &str) -> Assignment {
        text.parse().unwrap()
    }

    fn field(text: &str) -> Vec<String> {
        text.split('.').map(str::to_owned).collect()
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
        let AdminOp::Set { changes } = order(&read, &[a("model=o3"), a("env.API_TOKEN=new"), a("workdir=/w")]) else {
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
        let config = || Some(ConfigFile { path: path.clone(), edited: triggers.edits() });
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
        let op = order(&first.settings, &changes);
        let (_, applied) = orders::send(&admin, &js, Some(first.machine.clone()), &dev_a, op, WAIT).await.unwrap();
        assert_eq!(applied.map(|o| (o.state, o.reason)), Some((OrderState::Applied, None)));
        let edited = std::fs::read_to_string(&path).unwrap();
        assert!(edited.contains("model = \"o3\"  # fast enough"), "{edited}");
        assert!(edited.contains("API_TOKEN = \"n3w-s3cr3t-value\"") && edited.contains("command = \"other\""));

        std::fs::write(&path, edited.replace("\"o3\"", "\"o4\"")).unwrap();
        let stale = order(&first.settings, &["model=o5".parse().unwrap()]);
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
        let op = order(&last.settings, &[moved]);
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
}
