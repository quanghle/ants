//! The admin orders to an ant ([`AdminOrder`]): sending one, signed with an admin's device credentials, and waiting
//! for its outcome, which the daemon of the ant's machine records ([`AdminOutcome`]); and listing the orders to an ant
//! with where each stands (`ants ant orders`).

use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::path::Path;
use std::time::{Duration, SystemTime};

use ants_daemon::bus;
use ants_daemon::reading::{self, Reading};
use ants_proto::{
    ADMIN_TAG, AdminOp, AdminOrder, AdminOutcome, MACHINES_BUCKET, Machine, OrderState, UserId, Uuid, admin_outcome_of,
    subject,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::{self, consumer::DeliverPolicy};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

use crate::roster::{short, since};
use crate::{Nats, auth, machines};

/// How often [`send`] looks for the outcome of the order it sent.
const POLL: Duration = Duration::from_millis(250);

/// The credentials that sign an order: a human's device's, carrying the admin role.
fn admin_creds<'a>(nats: &Nats<'a>) -> Result<&'a Path> {
    match nats.creds {
        Some(creds) if nats.me.role.is_human() && nats.device.is_some() => Ok(creds),
        _ => bail!("an admin order needs a human device's admin credentials (`ants auth user NAME --human --admin`)"),
    }
}

/// Send `op` about `ant` to the daemon of `machine`, else of the machine that last recorded serving it, signed with
/// the admin credentials of `nats`, once the hub's stream has stored it; then wait up to `wait` for its outcome. None
/// when the order is still pending then, as it stays until the machine's daemon takes it.
#[cfg_attr(not(test), expect(dead_code, reason = "the admin commands of ants-5ggma.6 send orders"))]
pub async fn send(
    nats: &Nats<'_>,
    js: &jetstream::Context,
    machine: Option<Machine>,
    ant: &UserId,
    op: AdminOp,
    wait: Duration,
) -> Result<(AdminOrder, Option<AdminOutcome>)> {
    let creds = admin_creds(nats)?;
    let (jwt, key) = auth::read_creds(creds)?;
    let (_, claims) = ants_proto::verified_jwt(&jwt).with_context(|| format!("credentials {}", creds.display()))?;
    let tags = claims["nats"]["tags"].as_array().map_or(&[][..], Vec::as_slice);
    if !tags.iter().any(|tag| tag.as_str() == Some(ADMIN_TAG)) {
        bail!(
            "the credentials of {} carry no admin role: `ants auth user NAME --human --admin` issues them",
            nats.me.id
        );
    }
    let machine = match machine {
        Some(machine) => machine,
        None => machines::machines_of(js, ant).await?.into_iter().next().with_context(|| {
            format!("no machine recorded serving {ant}: its daemon records it once it runs with host credentials")
        })?,
    };
    let mut order = AdminOrder::new(nats.me.id.clone(), machine, ant.clone(), op, jwt);
    order.signature = auth::sign(&key, &order.signed())?;
    publish(js, &order).await?;
    let started = tokio::time::Instant::now();
    loop {
        if let Some(outcome) = outcome(js, &order).await? {
            return Ok((order, Some(outcome)));
        }
        if started.elapsed() >= wait {
            return Ok((order, None));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Publish `order` to the hub's stream, as message id its id, and wait until the stream stored it.
async fn publish(js: &jetstream::Context, order: &AdminOrder) -> Result<()> {
    let subject = order.subject();
    let message = jetstream::message::PublishMessage::build()
        .payload(serde_json::to_vec(order)?.into())
        .message_id(order.id.to_string());
    let stored = async { js.send_publish(subject.clone(), message).await?.await };
    match stored.await {
        Ok(_) => Ok(()),
        Err(e) => Err(bus::explain(js, &subject, e).await).context("sending the admin order"),
    }
}

/// The outcome the daemon of `order`'s machine recorded, if any yet.
pub async fn outcome(js: &jetstream::Context, order: &AdminOrder) -> Result<Option<AdminOutcome>> {
    let filter = format!("$KV.{MACHINES_BUCKET}.{}", order.outcome_key());
    let found = machines::latest(js, &filter).await?.into_iter().next();
    found.map(|(_, payload)| serde_json::from_slice(&payload).context("reading the order's outcome")).transpose()
}

/// An order to an ant and where it stands.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Listed {
    pub id: Uuid,
    pub machine: Machine,
    pub ant: UserId,
    /// The admin the order names; none when it could not be read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub admin: Option<UserId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub op: Option<AdminOp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sent_at: Option<DateTime<Utc>>,
    pub state: OrderState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    /// When the machine's daemon decided it; none while pending.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decided_at: Option<DateTime<Utc>>,
}

impl Listed {
    fn pending(order: AdminOrder, machine: Machine, ant: UserId) -> Self {
        Self {
            id: order.id,
            machine,
            ant,
            admin: Some(order.admin),
            op: Some(order.op),
            sent_at: Some(order.sent_at),
            state: OrderState::Pending,
            reason: None,
            result: None,
            decided_at: None,
        }
    }
}

impl From<AdminOutcome> for Listed {
    fn from(o: AdminOutcome) -> Self {
        Self {
            id: o.id,
            machine: o.machine,
            ant: o.ant,
            admin: o.admin,
            op: o.op,
            sent_at: o.sent_at,
            state: o.state,
            reason: o.reason,
            result: o.result,
            decided_at: Some(o.decided_at),
        }
    }
}

/// The orders to `ant`, oldest first, read with a human's credentials: those its machines' daemons decided, as they
/// recorded them, and those still waiting in the hub's stream for their machine, pending.
pub async fn orders_of(js: &jetstream::Context, ant: &UserId) -> Result<Vec<Listed>> {
    let mut listed = BTreeMap::new();
    for (key, payload) in machines::latest(js, &subject::admin_outcomes_of(ant)).await? {
        let Some((machine, _, id)) = admin_outcome_of(&key) else { continue };
        match serde_json::from_slice::<AdminOutcome>(&payload) {
            Ok(outcome) => _ = listed.insert((id, machine), Listed::from(outcome)),
            Err(e) => tracing::warn!(key, error = %e, "skipping a malformed order outcome"),
        }
    }
    let mut sent = Vec::new();
    reading::read_back(
        js,
        "",
        Reading::Gets,
        &subject::admin_orders_to(ant),
        DeliverPolicy::All,
        |_, _, subject, payload| {
            sent.push((subject.to_owned(), payload.to_vec()));
            ControlFlow::Continue(())
        },
    )
    .await
    .context("reading the admin orders in the hub's stream")?;
    for (subject, payload) in sent {
        let (Some((machine, to)), Ok(order)) =
            (subject::parse_admin_order(&subject), serde_json::from_slice::<AdminOrder>(&payload))
        else {
            continue;
        };
        listed.entry((order.id, machine.clone())).or_insert_with(|| Listed::pending(order, machine, to));
    }
    Ok(listed.into_values().collect())
}

/// `ants ant orders ANT`: the orders to `ant`, a table or with `json` one per line.
pub async fn show(nats: &Nats<'_>, ant: &UserId, json: bool) -> Result<()> {
    if !nats.me.role.is_human() {
        bail!("the credentials of {} are an ant's; `ants ant orders` reads what a human reads", nats.me.id);
    }
    let (_, js) = nats.connect("ants-orders").await?;
    let orders = orders_of(&js, ant).await?;
    if json {
        for order in &orders {
            println!("{}", serde_json::to_string(order)?);
        }
    } else if orders.is_empty() {
        println!("no admin orders to {ant}");
    } else {
        println!("{}", table(&orders, SystemTime::now().into()));
    }
    Ok(())
}

fn table(orders: &[Listed], now: DateTime<Utc>) -> String {
    let header = ["SENT", "ADMIN", "MACHINE", "ORDER", "STATE"].map(str::to_owned);
    let rows: Vec<[String; 5]> = std::iter::once(header)
        .chain(orders.iter().map(|o| {
            let state = match &o.reason {
                Some(reason) => format!("{}: {reason}", o.state.as_str()),
                None => o.state.as_str().to_owned(),
            };
            [
                o.sent_at.map_or_else(|| "-".to_owned(), |at| format!("{} ago", short(since(at, now)))),
                o.admin.as_ref().map_or_else(|| "-".to_owned(), ToString::to_string),
                o.machine.to_string(),
                o.op.as_ref().map_or_else(|| "-".to_owned(), AdminOp::describe),
                state,
            ]
        }))
        .collect();
    let mut widths = [0; 5];
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let lines: Vec<String> = rows
        .iter()
        .map(|row| {
            let cells: Vec<String> = row.iter().zip(widths).map(|(cell, width)| format!("{cell:width$}")).collect();
            cells.join("  ").trim_end().to_owned()
        })
        .collect();
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use ants_daemon::admin::ConfigFile;
    use ants_daemon::direct_kv::DirectKv;
    use ants_daemon::host::{self, Host};
    use ants_proto::{FieldChange, MACHINES_BUCKET, Permissions, Role};

    use super::*;
    use crate::auth::tests::Server;
    use crate::auth::{NewDevice, issue_device_with, issue_host};
    use crate::revoke::Kind;

    const WAIT: Duration = Duration::from_secs(5);

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    /// The host connection of m1 serving dev-a, as a daemon whose config sets `[admin] remote` to `remote` runs it,
    /// serving the file `config`, once it recorded its ants; dropping the sender stops it.
    async fn host_of_m1(
        server: &Server,
        js: &jetstream::Context,
        remote: bool,
        config: Option<ConfigFile>,
    ) -> (tokio::sync::watch::Sender<Vec<UserId>>, tokio::task::JoinHandle<Result<()>>) {
        let creds = server.dir().join("host-m1.creds");
        issue_host(&server.dir().join("auth"), &"m1".parse().unwrap(), None, &creds).unwrap();
        let (url, creds) = (server.url.clone(), Some(creds));
        let host = Host { machine: None, nats: url, creds, js_domain: None, remote_admin: remote, config };
        let (ants, watched) = tokio::sync::watch::channel(vec![p("dev-a")]);
        let task = tokio::spawn(host::run(host, watched));
        tokio::time::timeout(WAIT, async {
            while machines::machines_of(js, &p("dev-a")).await.unwrap_or_default().is_empty() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the host records its ants");
        (ants, task)
    }

    /// An admin's order reaches the daemon of the ant's machine, which records its outcome; one a human without the
    /// role signed, it refuses with the reason, and every order while its config says `[admin] remote = false`. An
    /// order to a machine whose daemon is away waits, pending. Only humans send orders, and only the machine's host
    /// credentials record their outcomes, and only of orders to the machine.
    #[tokio::test]
    async fn a_machine_s_daemon_decides_each_order_to_its_ants_once_and_records_why() {
        let Some(server) = Server::start() else { return };
        let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
        let (_, h_js) = server.connect_as(&bob, Role::Human).await;
        ants_daemon::bus::ensure_stream(&h_js).await.unwrap();
        h_js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
        h_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
        let admin_creds = server.dir().join("alice-admin.creds");
        let admin = NewDevice { label: Some("laptop"), admin: true, ..NewDevice::default() };
        let rights = Permissions::for_user(&alice, Role::Human);
        issue_device_with(&server.dir().join("auth"), &alice, Kind::Human, rights, &admin, &admin_creds).unwrap();
        let admin = crate::Nats::new(&server.url, Some(&admin_creds), None).unwrap();
        let (_, js) = admin.connect("test").await.unwrap();
        let (ants, task) = host_of_m1(&server, &js, true, None).await;

        let (pause, paused) = send(&admin, &js, None, &dev_a, AdminOp::Pause, WAIT).await.unwrap();
        let paused = paused.expect("the daemon decides at once");
        assert_eq!(
            (paused.state, paused.reason.as_deref()),
            (OrderState::Refused, Some(ants_daemon::admin::NOT_SUPPORTED))
        );
        assert_eq!(
            (paused.id, paused.admin.as_ref(), paused.op.as_ref()),
            (pause.id, Some(&alice), Some(&AdminOp::Pause))
        );

        let bob_creds = server.creds_as(&bob, Role::Human);
        let bob_nats = crate::Nats::new(&server.url, Some(&bob_creds), None).unwrap();
        let refused = send(&bob_nats, &js, None, &dev_a, AdminOp::Resume, WAIT).await.unwrap_err();
        assert!(refused.to_string().contains("no admin role"), "{refused:#}");
        let (jwt, key) = auth::read_creds(&bob_creds).unwrap();
        let mut forged = AdminOrder::new(bob.clone(), "m1".parse().unwrap(), dev_a.clone(), AdminOp::Retire, jwt);
        forged.signature = auth::sign(&key, &forged.signed()).unwrap();
        publish(&js, &forged).await.unwrap();
        let away = send(&admin, &js, Some("m2".parse().unwrap()), &dev_a, AdminOp::Resume, Duration::ZERO).await;
        let (away, none) = away.unwrap();
        assert_eq!(none, None, "no daemon of m2 takes it");

        let decided = tokio::time::timeout(WAIT, async {
            loop {
                if let Some(outcome) = outcome(&js, &forged).await.unwrap() {
                    return outcome;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
        let decided = decided.await.expect("refused");
        assert_eq!(decided.state, OrderState::Refused);
        assert!(decided.reason.as_deref().unwrap().contains("admin"), "{decided:?}");
        let listed = orders_of(&js, &dev_a).await.unwrap();
        let states: Vec<_> = listed.iter().map(|o| (o.id, o.machine.as_str(), o.state)).collect();
        assert_eq!(
            states,
            [
                (pause.id, "m1", OrderState::Refused),
                (forged.id, "m1", OrderState::Refused),
                (away.id, "m2", OrderState::Pending)
            ]
        );

        // Only humans send orders; only m1's host credentials record outcomes, of m1's orders.
        let (_, a_js) = server.connect_as(&dev_a, Role::Ant).await;
        let by_ant = AdminOrder { id: Uuid::now_v7(), ..pause.clone() };
        assert!(publish(&a_js, &by_ant).await.is_err(), "an ant sends no order");
        let as_human = DirectKv::new(&h_js, None, MACHINES_BUCKET).await.unwrap();
        assert!(as_human.put(by_ant.outcome_key(), b"{}".to_vec()).await.is_err(), "a human records no outcome");
        drop(ants);
        tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();

        let creds = server.dir().join("host-m1.creds");
        let mut m_js = jetstream::new(
            async_nats::ConnectOptions::with_credentials_file(&creds)
                .await
                .unwrap()
                .custom_inbox_prefix(ants_proto::host_inbox_prefix(&"m1".parse().unwrap()))
                .connect(&server.url)
                .await
                .unwrap(),
        );
        m_js.set_timeout(Duration::from_secs(1));
        let as_host = DirectKv::new(&m_js, None, MACHINES_BUCKET).await.unwrap();
        assert!(as_host.put(away.outcome_key(), b"{}".to_vec()).await.is_err(), "m1 records no outcome of m2's");

        let (ants, task) = host_of_m1(&server, &js, false, None).await;
        let (_, off) = send(&admin, &js, None, &dev_a, AdminOp::Pause, WAIT).await.unwrap();
        let off = off.expect("the daemon decides at once");
        assert_eq!((off.state, off.reason.as_deref()), (OrderState::Refused, Some(ants_daemon::admin::REMOTE_OFF)));
        drop(ants);
        tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
    }

    /// A set order edits the ant's table in the machine's config file in place, comments kept, and has the daemon
    /// reload it at once; one expecting a value the field no longer holds is refused naming the field, and one the
    /// config loader rejects is refused too, both leaving the file as it was.
    #[tokio::test]
    async fn a_set_order_edits_the_config_in_place_unless_a_field_changed_or_the_file_would_not_load() {
        let Some(server) = Server::start() else { return };
        let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
        let (_, h_js) = server.connect_as(&bob, Role::Human).await;
        ants_daemon::bus::ensure_stream(&h_js).await.unwrap();
        h_js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
        h_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
        let admin_creds = server.dir().join("alice-admin.creds");
        let admin = NewDevice { label: Some("laptop"), admin: true, ..NewDevice::default() };
        let rights = Permissions::for_user(&alice, Role::Human);
        issue_device_with(&server.dir().join("auth"), &alice, Kind::Human, rights, &admin, &admin_creds).unwrap();
        let admin = crate::Nats::new(&server.url, Some(&admin_creds), None).unwrap();
        let (_, js) = admin.connect("test").await.unwrap();

        let path = server.dir().join("config.toml");
        let text = "harness = \"copilot\"\n\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nmodel = \"gpt-5\"  # fast \
                    enough\n\n# Keep dev-b on its own.\n[[ant]]\nname = \"dev-b\"\nworkdir = \".\"\n";
        std::fs::write(&path, text).unwrap();
        let mut triggers = crate::reload::Triggers::new(path.clone(), None, Duration::from_secs(3600)).unwrap();
        let config = ConfigFile { path: path.clone(), edited: triggers.edits() };
        let (ants, task) = host_of_m1(&server, &js, true, Some(config)).await;
        let set = |field: &str, expected: Option<Value>, value: Option<Value>| AdminOp::Set {
            changes: vec![FieldChange { field: vec![field.to_owned()], expected, value }],
        };

        let model = set("model", Some("gpt-5".into()), Some("o3".into()));
        let (_, applied) = send(&admin, &js, None, &dev_a, model, WAIT).await.unwrap();
        let applied = applied.expect("the daemon decides at once");
        assert_eq!((applied.state, applied.reason), (OrderState::Applied, None));
        let edited = std::fs::read_to_string(&path).unwrap();
        assert_eq!(edited, text.replace("\"gpt-5\"", "\"o3\""), "only the value changes, comments kept");
        let reload = tokio::time::timeout(WAIT, triggers.next()).await.expect("a reload at once");
        assert_eq!(reload, crate::reload::Trigger::Edited);

        let stale = set("model", Some("gpt-5".into()), Some("o4".into()));
        let (_, conflict) = send(&admin, &js, None, &dev_a, stale, WAIT).await.unwrap();
        let conflict = conflict.expect("the daemon decides at once");
        assert_eq!(conflict.state, OrderState::Refused);
        let reason = conflict.reason.unwrap();
        assert!(reason.starts_with("model changed since the order was sent"), "{reason}");

        let invalid = set("max_harnesses", None, Some("many".into()));
        let (_, invalid) = send(&admin, &js, None, &dev_a, invalid, WAIT).await.unwrap();
        let invalid = invalid.expect("the daemon decides at once");
        assert_eq!(invalid.state, OrderState::Refused);
        assert!(invalid.reason.as_deref().unwrap().contains("max_harnesses"), "{invalid:?}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), edited, "refusals leave the file alone");
        let quiet = tokio::time::timeout(Duration::from_millis(200), triggers.next()).await;
        assert!(quiet.is_err(), "no reload without an edit");
        drop(ants);
        tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
    }

    #[test]
    fn the_table_lists_each_order_with_where_it_stands() {
        let now = Utc::now();
        let mut order = AdminOrder::new(
            "alice".parse().unwrap(),
            "box".parse().unwrap(),
            "dev-a".parse().unwrap(),
            AdminOp::Restart { now: true },
            "",
        );
        order.sent_at = now - chrono::Duration::minutes(5);
        let pending = Listed::pending(order.clone(), order.machine.clone(), order.ant.clone());
        let outcome = AdminOutcome::of(&order, OrderState::Refused, Some("not supported yet".into()), None);
        let table = table(&[pending, outcome.into()], now);
        let lines: Vec<&str> = table.lines().collect();
        assert_eq!(lines[1], "5m ago  alice  box      restart now  pending", "{table}");
        assert_eq!(lines[2], "5m ago  alice  box      restart now  refused: not supported yet", "{table}");
    }
}
