//! `ants machines`: the machines whose daemons recorded the ants they serve ([`MACHINES_BUCKET`]), which stay there
//! after an ant's roster entry expired, so that an ant that is offline still has a machine.

use std::time::Duration;

use ants_proto::{MACHINES_BUCKET, Machine, MachineRecord, UserId, subject};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::{StatusCode, jetstream};
use chrono::Utc;
use futures::StreamExt as _;
use serde_json::json;

use crate::roster::{short, since};

/// Records asked for at once.
const BATCH: usize = 256;

/// The machines' records, read with a human's credentials by batched direct gets of their keys
/// ([`subject::machine_records`]), sorted by machine.
pub async fn list(js: &jetstream::Context) -> Result<Vec<MachineRecord>> {
    let mut records = Vec::new();
    for (key, payload) in latest(js, &subject::machine_records()).await? {
        match serde_json::from_slice::<MachineRecord>(&payload) {
            Ok(record) if record.key() == key => records.push(record),
            Ok(_) => tracing::warn!(key, "skipping the record of another machine"),
            Err(e) => tracing::warn!(key, error = %e, "skipping a malformed machine record"),
        }
    }
    Ok(sorted(records))
}

/// The key and value of each live entry of [`MACHINES_BUCKET`] on `filter` (a subject under `$KV.ants_machines.`), by
/// batched direct gets of the bucket's stream, which keeps one message per key, in the order they were last put.
pub(crate) async fn latest(js: &jetstream::Context, filter: &str) -> Result<Vec<(String, Vec<u8>)>> {
    latest_in(js, MACHINES_BUCKET, filter).await
}

/// [`latest`] of `bucket`, a KV bucket of history 1, on `filter`, a subject under `$KV.<bucket>.`.
pub(crate) async fn latest_in(js: &jetstream::Context, bucket: &str, filter: &str) -> Result<Vec<(String, Vec<u8>)>> {
    let stream = format!("KV_{bucket}");
    let client = js.client();
    let wait = client.timeout().unwrap_or(Duration::from_secs(10));
    let inbox = client.new_inbox();
    let mut replies = client.subscribe(inbox.clone()).await?;
    let prefix = format!("$KV.{bucket}.");
    let (mut entries, mut seq) = (Vec::new(), 1);
    loop {
        let request = json!({ "seq": seq, "next_by_subj": filter, "batch": BATCH });
        let request = async_nats::Request::new().inbox(inbox.clone()).payload(serde_json::to_vec(&request)?.into());
        js.send_request(format!("DIRECT.GET.{stream}"), request)
            .await
            .with_context(|| format!("reading {bucket} (`ants init` creates it)"))?;
        let mut got = 0;
        loop {
            let Ok(Some(msg)) = tokio::time::timeout(wait, replies.next()).await else {
                bail!("no answer reading {bucket} (`ants init` creates it)");
            };
            match msg.status {
                None => {}
                Some(StatusCode::NOT_FOUND) => return Ok(entries),
                Some(StatusCode::NO_RESPONDERS) => bail!("no {bucket} bucket: `ants init` creates it"),
                Some(status) if status.as_u16() == 204 => {
                    let pending = msg.headers.as_ref().and_then(|h| h.get("Nats-Num-Pending"));
                    if got == 0 || pending.is_some_and(|n| n.as_str() == "0") {
                        return Ok(entries);
                    }
                    break;
                }
                Some(status) => bail!("reading {bucket}: {status} {:?}", msg.description),
            }
            got += 1;
            let msg = jetstream::message::StreamMessage::try_from(msg).map_err(|e| anyhow!(e))?;
            seq = msg.sequence + 1;
            let deleted = msg.headers.get("KV-Operation").is_some();
            if let Some(key) = msg.subject.strip_prefix(prefix.as_str()).filter(|_| !deleted) {
                entries.push((key.to_owned(), msg.payload.to_vec()));
            }
        }
    }
}

fn sorted(mut records: Vec<MachineRecord>) -> Vec<MachineRecord> {
    records.sort_by(|a, b| a.machine.cmp(&b.machine));
    records
}

/// The machines whose daemons last recorded serving `ant`, latest first: one, unless it moved and the machine it left
/// has not recorded since.
pub(crate) async fn machines_of(js: &jetstream::Context, ant: &UserId) -> Result<Vec<Machine>> {
    let records = list(js).await?;
    Ok(ants_proto::machines_of(&records, ant).into_iter().map(|r| r.machine.clone()).collect())
}

/// Print the machines and the ants each serves, a table or with `json` one record per line; or with `ant` the
/// machines of that ant, one per line or with `json` as one object.
pub async fn show(nats: &crate::Nats<'_>, ant: Option<&UserId>, json: bool) -> Result<()> {
    let (_, js) = nats.connect("ants-machines").await?;
    if let Some(ant) = ant {
        let machines = machines_of(&js, ant).await?;
        if json {
            println!("{}", serde_json::json!({ "ant": ant, "machines": machines }));
        } else if machines.is_empty() {
            println!("no machine recorded {ant}");
        }
        for machine in machines.iter().filter(|_| !json) {
            println!("{machine}");
        }
        return Ok(());
    }
    let records = list(&js).await?;
    if json {
        for record in &records {
            println!("{}", serde_json::to_string(record)?);
        }
    } else if records.is_empty() {
        println!("no machines recorded");
    } else {
        println!("{}", table(&records, Utc::now()));
    }
    Ok(())
}

fn table(records: &[MachineRecord], now: chrono::DateTime<Utc>) -> String {
    let header = ["MACHINE", "HOSTNAME", "VERSION", "RECORDED", "ANTS"].map(str::to_owned);
    let rows: Vec<[String; 5]> = std::iter::once(header)
        .chain(records.iter().map(|r| {
            let ants: Vec<&str> = r.ants.iter().map(UserId::as_str).collect();
            [
                r.machine.to_string(),
                if r.hostname.is_empty() { "-".to_owned() } else { r.hostname.clone() },
                if r.version.is_empty() { "-".to_owned() } else { r.version.clone() },
                format!("{} ago", short(since(r.updated_at, now))),
                if ants.is_empty() { "-".to_owned() } else { ants.join(",") },
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
    use super::*;

    #[test]
    fn the_table_lists_each_machine_with_its_ants() {
        let now = Utc::now();
        let record = MachineRecord {
            machine: "box".parse().unwrap(),
            ants: vec!["dev-a".parse().unwrap(), "dev-b".parse().unwrap()],
            hostname: "box.local".into(),
            version: String::new(),
            updated_at: now - chrono::Duration::minutes(5),
            drift: std::collections::BTreeMap::new(),
        };
        let table = table(&[record], now);
        assert_eq!(table.lines().nth(1), Some("box      box.local  -        5m ago    dev-a,dev-b"), "{table}");
    }
}
