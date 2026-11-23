//! The admin orders to an ant ([`AdminOrder`]): sending one, signed with an admin's device credentials, and waiting
//! for its outcome, which the daemon of the ant's machine records ([`AdminOutcome`]); and listing the orders to an ant
//! with where each stands (`ants ant orders`).

pub mod add;
pub mod clone;
pub mod retire;

use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use ants_daemon::bus;
use ants_daemon::stream::reading::{self, Reading};
use ants_proto::{
    ADMIN_TAG, AdminOp, AdminOrder, AdminOutcome, MACHINES_BUCKET, Machine, OrderState, UserId, Uuid, admin_outcome_of,
    subject,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::context::PublishErrorKind;
use async_nats::jetstream::{self, consumer::DeliverPolicy};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

use crate::colony::{short, since};
use crate::connection::Nats;
use crate::{auth, machines};

/// How often [`send`] looks for the outcome of the order it sent.
pub(crate) const POLL: Duration = Duration::from_millis(250);

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
pub async fn send(
    nats: &Nats<'_>,
    js: &jetstream::Context,
    machine: Option<Machine>,
    ant: &UserId,
    op: AdminOp,
    wait: Duration,
) -> Result<(AdminOrder, Option<AdminOutcome>)> {
    Admin::of(nats)?.send(js, machine, ant, op, wait).await
}

/// The admin whose device credentials sign the orders it sends, apart from the connection that sends them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admin {
    pub id: UserId,
    creds: PathBuf,
}

impl Admin {
    /// The human of `nats`, if its credentials are a device's.
    pub fn of(nats: &Nats<'_>) -> Result<Self> {
        Ok(Self { id: nats.me.id.clone(), creds: admin_creds(nats)?.to_owned() })
    }

    /// [`send`] with these credentials.
    pub async fn send(
        &self,
        js: &jetstream::Context,
        machine: Option<Machine>,
        ant: &UserId,
        op: AdminOp,
        wait: Duration,
    ) -> Result<(AdminOrder, Option<AdminOutcome>)> {
        let creds = &self.creds;
        let (jwt, key) = auth::read_creds(creds)?;
        let (_, claims) = ants_proto::verified_jwt(&jwt).with_context(|| format!("credentials {}", creds.display()))?;
        let tags = claims["nats"]["tags"].as_array().map_or(&[][..], Vec::as_slice);
        if !tags.iter().any(|tag| tag.as_str() == Some(ADMIN_TAG)) {
            bail!(
                "the credentials of {} carry no admin role: `ants auth user NAME --human --admin` issues them",
                self.id
            );
        }
        let machine = match machine {
            Some(machine) => machine,
            None => serving(js, ant).await?,
        };
        let mut order = AdminOrder::new(self.id.clone(), machine, ant.clone(), op, jwt);
        order.signature = auth::sign(&key, &order.signed())?;
        sent(js, order, wait).await
    }
}

/// The machine that last recorded serving `ant`, or why none: no machine lists it, or no machine recorded itself.
async fn serving(js: &jetstream::Context, ant: &UserId) -> Result<Machine> {
    let records = machines::list(js).await?;
    if let Some(record) = ants_proto::machines_of(&records, ant).into_iter().next() {
        return Ok(record.machine.clone());
    }
    if records.is_empty() {
        bail!(
            "no machine recorded serving {ant}: a machine's daemon records the ants it serves once it runs with host credentials"
        );
    }
    bail!(
        "no machine lists {ant}: it was retired, never added or its name is misspelt; `ants machines` shows the ants \
         each machine serves"
    )
}

/// Publish the signed `order` and wait up to `wait` for its outcome.
async fn sent(
    js: &jetstream::Context,
    order: AdminOrder,
    wait: Duration,
) -> Result<(AdminOrder, Option<AdminOutcome>)> {
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

/// What a command says of `order` still pending when it stopped waiting.
pub(crate) fn pending_note(order: &AdminOrder) -> String {
    format!(
        "order {} sent; the daemon of {} has not decided it yet: `ants ant orders {}` shows its outcome",
        order.id, order.machine, order.ant
    )
}

/// Why the daemon refused an order, as its outcome says.
pub(crate) fn reason(outcome: &AdminOutcome) -> &str {
    outcome.reason.as_deref().unwrap_or("no reason given")
}

/// Publish `order` to the hub's stream, as message id its id, and wait until the stream stored it. No outbox keeps an
/// order: while the hub is out of reach, as when the uplink of the machine's leaf node is down, sending it fails.
async fn publish(js: &jetstream::Context, order: &AdminOrder) -> Result<()> {
    let subject = order.subject();
    let message = jetstream::message::PublishMessage::build()
        .payload(serde_json::to_vec(order)?.into())
        .message_id(order.id.to_string());
    let stored = async { js.send_publish(subject.clone(), message).await?.await };
    let e = match stored.await {
        Ok(_) => return Ok(()),
        Err(e) => e,
    };
    let uncaptured = e.kind() == PublishErrorKind::StreamNotFound && bus::uncaptured(js).await;
    let unsent = match e.kind() {
        PublishErrorKind::StreamNotFound if !uncaptured => format!(
            "the hub is out of reach, as when the uplink of this machine's leaf node is down: order {} was not stored, \
             and no outbox keeps an admin order to send later; send it again once the hub is in reach",
            order.id
        ),
        PublishErrorKind::TimedOut => format!(
            "the hub's stream did not say in time whether it stored order {}: `ants ant orders {}` lists it if it did",
            order.id, order.ant
        ),
        _ => "sending the admin order".to_owned(),
    };
    Err(bus::explained(e, &subject, uncaptured)).context(unsent)
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
    /// The human the admin, an assistant, sent the order for.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_behalf_of: Option<UserId>,
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
    /// Who sent it: the admin, or `<human> via <assistant>`.
    pub(crate) fn by(&self) -> String {
        match (&self.admin, &self.on_behalf_of) {
            (Some(admin), Some(human)) => format!("{human} via {admin}"),
            (Some(admin), None) => admin.to_string(),
            (None, _) => "-".to_owned(),
        }
    }

    fn pending(order: AdminOrder, machine: Machine, ant: UserId) -> Self {
        Self {
            id: order.id,
            machine,
            ant,
            admin: Some(order.admin),
            on_behalf_of: order.on_behalf_of,
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
            on_behalf_of: o.on_behalf_of,
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

pub(crate) fn table(orders: &[Listed], now: DateTime<Utc>) -> String {
    let header = ["SENT", "ADMIN", "MACHINE", "ORDER", "STATE"].map(str::to_owned);
    let rows: Vec<[String; 5]> = std::iter::once(header)
        .chain(orders.iter().map(|o| {
            let state = match &o.reason {
                Some(reason) => format!("{}: {reason}", o.state.as_str()),
                None => o.state.as_str().to_owned(),
            };
            [
                o.sent_at.map_or_else(|| "-".to_owned(), |at| format!("{} ago", short(since(at, now)))),
                o.by(),
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
mod tests;
