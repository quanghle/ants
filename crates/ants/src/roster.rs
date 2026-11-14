//! `ants roster`: the personas being served, and whether their daemons still beat.

use std::time::Duration;

use ants_daemon::{bus, roster};
use ants_proto::{PersonaId, ROSTER_HEARTBEAT, ROSTER_TTL, RosterEntry};
use anyhow::Result;
use async_nats::jetstream;
use chrono::{DateTime, Utc};

/// An entry not refreshed for this long has missed a heartbeat: its daemon may be gone, and the entry expires once
/// [`ROSTER_TTL`] has passed.
const STALE_AFTER: Duration = ROSTER_HEARTBEAT.saturating_mul(2);

/// How fresh an entry is, judged by its last heartbeat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Live,
    Stale,
}

impl State {
    pub fn of(entry: &RosterEntry, now: DateTime<Utc>) -> Self {
        if since(entry.heartbeat_at, now) <= STALE_AFTER { Self::Live } else { Self::Stale }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Stale => "stale",
        }
    }
}

/// Time from `then` to `now`, zero when a clock is ahead.
fn since(then: DateTime<Utc>, now: DateTime<Utc>) -> Duration {
    (now - then).to_std().unwrap_or_default()
}

/// `duration` in its largest whole unit: `7s`, `3m`, `2h`, `5d`.
fn short(duration: Duration) -> String {
    let secs = duration.as_secs();
    match secs {
        0..60 => format!("{secs}s"),
        60..3600 => format!("{}m", secs / 60),
        3600..86_400 => format!("{}h", secs / 3600),
        _ => format!("{}d", secs / 86_400),
    }
}

/// The persona's escalation chain, leader first, or `-` for none.
fn chain(entry: &RosterEntry) -> String {
    if entry.escalate_to.is_empty() {
        return "-".to_owned();
    }
    entry.escalate_to.iter().map(PersonaId::as_str).collect::<Vec<_>>().join(",")
}

/// The roster as a table: one row per persona under a header, columns padded to line up.
pub fn table(entries: &[RosterEntry], now: DateTime<Utc>) -> String {
    let header = [
        "PERSONA",
        "STATE",
        "ACTIVITY",
        "HARNESSES",
        "HARNESS",
        "MACHINE",
        "HEARTBEAT",
        "UP",
        "CAPABILITIES",
        "ESCALATE_TO",
        "WORKDIR",
    ]
    .map(str::to_owned);
    let rows: Vec<[String; 11]> = std::iter::once(header)
        .chain(entries.iter().map(|e| {
            [
                e.persona.to_string(),
                State::of(e, now).as_str().to_owned(),
                e.activity.map_or("-", |a| a.as_str()).to_owned(),
                e.harnesses.map_or_else(|| "-".to_owned(), |n| n.to_string()),
                e.harness.clone(),
                e.machine.clone(),
                format!("{} ago", short(since(e.heartbeat_at, now))),
                short(since(e.started_at, now)),
                if e.capabilities.is_empty() { "-".to_owned() } else { e.capabilities.join(",") },
                chain(e),
                e.workdir.clone().unwrap_or_else(|| "-".to_owned()),
            ]
        }))
        .collect();
    let mut widths = [0; 11];
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

/// An entry as one JSON object, with its `state` added.
pub fn json(entry: &RosterEntry, now: DateTime<Utc>) -> Result<String> {
    let mut value = serde_json::to_value(entry)?;
    if let Some(object) = value.as_object_mut() {
        object.insert("state".to_owned(), State::of(entry, now).as_str().into());
    }
    Ok(value.to_string())
}

/// The entries in the roster, read with a human's credentials.
pub async fn list(js: &jetstream::Context) -> Result<Vec<RosterEntry>> {
    let kv = bus::ensure_roster(js, ROSTER_TTL).await?;
    roster::list_entries(&kv).await
}

/// Print the roster: a table, or with `json` one entry per line.
pub async fn show(nats: &crate::Nats<'_>, json: bool) -> Result<()> {
    let (_, js) = nats.connect("ants-roster").await?;
    let entries = list(&js).await?;
    let now = Utc::now();
    if json {
        for entry in &entries {
            println!("{}", self::json(entry, now)?);
        }
    } else if entries.is_empty() {
        println!("no personas registered");
    } else {
        println!("{}", table(&entries, now));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use ants_daemon::roster::Registration;
    use ants_proto::Activity;

    use super::*;
    use crate::auth::tests::Server;

    fn entry(persona: &str, started: i64, beat: i64, now: DateTime<Utc>) -> RosterEntry {
        RosterEntry {
            persona: persona.parse().unwrap(),
            machine: "m1".to_owned(),
            instance: None,
            harness: "copilot".to_owned(),
            capabilities: vec!["rust".to_owned(), "review".to_owned()],
            whoami: None,
            workdir: Some("/src/app".to_owned()),
            started_at: now - chrono::Duration::seconds(started),
            heartbeat_at: now - chrono::Duration::seconds(beat),
            activity: None,
            handling: None,
            harnesses: None,
            escalate_to: Vec::new(),
        }
    }

    #[test]
    fn rows_show_state_and_ages() {
        let now = Utc::now();
        let mut quiet = entry("dev-b", 7300, 25, now);
        quiet.capabilities.clear();
        quiet.workdir = None;
        let mut busy = entry("dev-a", 190, 4, now);
        (busy.activity, busy.handling, busy.harnesses) = (Some(Activity::Working), Some(1), Some(2));
        busy.escalate_to = vec!["lead".parse().unwrap(), "dev-b".parse().unwrap()];
        let table = table(&[busy, quiet], now);
        let rows: Vec<Vec<&str>> = table.lines().map(|l| l.split_whitespace().collect()).collect();
        assert_eq!(
            rows[0],
            [
                "PERSONA",
                "STATE",
                "ACTIVITY",
                "HARNESSES",
                "HARNESS",
                "MACHINE",
                "HEARTBEAT",
                "UP",
                "CAPABILITIES",
                "ESCALATE_TO",
                "WORKDIR"
            ]
        );
        assert_eq!(
            rows[1],
            [
                "dev-a",
                "live",
                "working",
                "2",
                "copilot",
                "m1",
                "4s",
                "ago",
                "3m",
                "rust,review",
                "lead,dev-b",
                "/src/app"
            ]
        );
        assert_eq!(rows[2], ["dev-b", "stale", "-", "-", "copilot", "m1", "25s", "ago", "2h", "-", "-", "-"]);
        let columns: Vec<Option<usize>> = table.lines().map(|l| l.find("copilot").or(l.find("HARNESS "))).collect();
        assert!(columns.iter().all(|c| c.is_some() && *c == columns[0]), "{table}");

        let mut led = entry("dev-a", 0, 30, now);
        led.escalate_to = vec!["lead".parse().unwrap()];
        let json: serde_json::Value = serde_json::from_str(&super::json(&led, now).unwrap()).unwrap();
        assert_eq!((json["persona"].as_str(), json["state"].as_str()), (Some("dev-a"), Some("stale")));
        assert_eq!(json["escalate_to"], serde_json::json!(["lead"]));
    }

    /// With the human's credentials, so it also checks that they read the roster.
    #[tokio::test]
    async fn the_human_lists_registered_personas() {
        let Some(server) = Server::start() else { return };
        let (_, human) = server.connect(&PersonaId::human()).await;
        assert!(list(&human).await.unwrap().is_empty(), "an empty roster, created on first use");

        let mut registrations = Vec::new();
        let now = Utc::now();
        for persona in ["dev-b", "dev-a"] {
            let (_, js) = server.connect(&persona.parse().unwrap()).await;
            let kv = bus::ensure_roster(&js, ROSTER_TTL).await.unwrap();
            let entry = entry(persona, 60, 0, now);
            registrations.push(Registration::start(kv, entry, ROSTER_HEARTBEAT).await.unwrap());
        }
        let listed = list(&human).await.unwrap();
        let personas: Vec<&str> = listed.iter().map(|e| e.persona.as_str()).collect();
        assert_eq!(personas, ["dev-a", "dev-b"]);
        assert_eq!(listed[0], entry("dev-a", 60, 0, now));

        registrations.remove(0).deregister().await;
        let listed = list(&human).await.unwrap();
        assert_eq!(listed.iter().map(|e| e.persona.as_str()).collect::<Vec<_>>(), ["dev-a"]);
    }
}
