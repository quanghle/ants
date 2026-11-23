//! `ants machines`: the machines whose daemons recorded the ants they serve ([`MACHINES_BUCKET`]), which stay there
//! after an ant's colony entry expired, so that an ant that is offline still has a machine.

use ants_proto::{ACCOUNT_HOLDER_KEY, AccountHolder, MACHINES_BUCKET, Machine, MachineRecord, UserId, subject};
use anyhow::{Context as _, Result};
use async_nats::jetstream;
use chrono::Utc;

use crate::text::{ago, short, since};

/// The machines' records, read with a human's credentials by batched direct gets of their keys
/// ([`subject::machine_records`]), sorted by machine.
pub async fn list(js: &jetstream::Context) -> Result<Vec<MachineRecord>> {
    let mut records = Vec::new();
    for (key, payload) in latest(js, &subject::machine_records()).await? {
        if key == ACCOUNT_HOLDER_KEY {
            continue;
        }
        match serde_json::from_slice::<MachineRecord>(&payload) {
            Ok(record) if record.key() == key => records.push(record),
            Ok(_) => tracing::warn!(key, "skipping the record of another machine"),
            Err(e) => tracing::warn!(key, error = %e, "skipping a malformed machine record"),
        }
    }
    Ok(sorted(records))
}

/// The machine holding the team's account key, as `ants signer` there last recorded it; None when no signer did.
pub async fn holder(js: &jetstream::Context) -> Result<Option<AccountHolder>> {
    let Some((_, payload)) = latest(js, &subject::account_holder()).await?.pop() else { return Ok(None) };
    Ok(Some(serde_json::from_slice(&payload).context("the record of the machine holding the account key")?))
}

/// Where the team's account key is, in words, from `holder` as of `now`: the machine, its host and how long ago
/// `ants signer` there recorded it.
pub fn held_on(holder: &AccountHolder, now: chrono::DateTime<Utc>) -> String {
    let host = if holder.hostname.is_empty() { String::new() } else { format!(" (host {})", holder.hostname) };
    let ago = short(since(holder.updated_at, now));
    format!("machine {}{host}, as `ants signer` there recorded {ago} ago", holder.machine)
}

/// The key and value of each live entry of [`MACHINES_BUCKET`] on `filter` (a subject under `$KV.ants_machines.`), in
/// the order they were last put ([`crate::buckets::latest_in`]).
pub(crate) async fn latest(js: &jetstream::Context, filter: &str) -> Result<Vec<(String, Vec<u8>)>> {
    crate::buckets::latest_in(js, MACHINES_BUCKET, filter).await
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
pub async fn show(nats: &crate::connection::Nats<'_>, ant: Option<UserId>, json: bool) -> Result<()> {
    let (_, js) = nats.connect("ants-machines").await?;
    if let Some(ant) = ant {
        let machines = machines_of(&js, &ant).await?;
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
    if !json && let Ok(Some(holder)) = holder(&js).await {
        println!("the team's account key is on {}", held_on(&holder, Utc::now()));
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
                ago(r.updated_at, now),
                if ants.is_empty() { "-".to_owned() } else { ants.join(",") },
            ]
        }))
        .collect();
    crate::text::columns(&rows)
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
