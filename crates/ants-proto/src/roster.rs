use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[cfg(doc)]
use crate::{ANTS_VERSION, PERMISSIONS_VERSION, ROSTER_BUCKET, ROSTER_HEARTBEAT, ROSTER_TTL};
use crate::{Error, PermissionsVersion, UserId};

/// An ant's entry in the [`ROSTER_BUCKET`], under the key of its id.
///
/// The serving daemon puts it on start, re-puts it every [`ROSTER_HEARTBEAT`] and deletes it on a clean stop. The
/// bucket's max age is [`ROSTER_TTL`], so the entry of a daemon that died expires on its own. A daemon that defers to
/// others, as the built-in messenger's does, puts it only over its own entry or none, and stops serving once another
/// daemon puts it. Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RosterEntry {
    pub ant: UserId,
    /// Host name of the machine the daemon runs on.
    pub machine: String,
    /// The daemon run that put the entry, unique to it, so two daemons on one machine tell their entries apart.
    /// Missing from entries of older daemons.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
    /// Agent harness behind the ant, such as `copilot`.
    pub harness: String,
    /// The agent the harness runs, as the ACP handshake of its latest start named it. Missing until a harness of the
    /// daemon started, when the agent does not name itself, and from entries of older daemons.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<HarnessAgent>,
    /// Free-form tags the ant is good for (`rust`, `frontend`, `billing`), for routing requests to it.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Role instructions advertised to other ants.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub whoami: Option<String>,
    /// Directory the harness works in, on `machine`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    /// When the daemon started serving the ant.
    pub started_at: DateTime<Utc>,
    /// When the daemon last refreshed the entry.
    pub heartbeat_at: DateTime<Utc>,
    /// Whether the ant was handling a message at the last refresh. Missing from entries of older daemons.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity: Option<Activity>,
    /// Messages being handled at the last refresh.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handling: Option<u32>,
    /// Live harness processes (sessions) at the last refresh.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harnesses: Option<u32>,
    /// The ant's escalation chain, in order: what it cannot settle goes to these ants before a human. The first
    /// leads. Published so any daemon can follow the chain while the ant is offline.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub escalate_to: Vec<UserId>,
    /// The version of ants the daemon runs ([`ANTS_VERSION`]). Missing from entries of older daemons.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// The permissions version of the ants the daemon runs ([`PERMISSIONS_VERSION`]). Missing from entries of older
    /// daemons.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub perms: Option<PermissionsVersion>,
    /// Set while the ant's harness is signed out: messages to the ant wait, unanswered, until a check finds the
    /// harness signed in again. Missing while it is signed in, and from entries of older daemons.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signed_out: Option<SignInNeeded>,
    /// Set while the ant's harness is held back by a provider limit: messages to the ant wait, spending no attempts,
    /// until `until`. Missing otherwise, and from entries of older daemons.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limited: Option<HarnessLimit>,
    /// Set while the ant's harness keeps failing to start: messages to the ant wait, spending no attempts, until a
    /// check starts it. Missing otherwise, and from entries of older daemons.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failing: Option<HarnessFailing>,
    /// Set while the system memory available on `machine` is low: the daemon stops the ant's idle harnesses at once
    /// and runs fewer of them. Missing otherwise, and from entries of older daemons.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_low: Option<MemoryLow>,
}

/// Low memory on an ant's machine, as its roster entry reports it ([`RosterEntry::memory_low`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryLow {
    /// When the available memory fell below the daemon's threshold.
    pub since: DateTime<Utc>,
    /// Bytes of memory available at the latest check.
    pub available: u64,
    /// The most harnesses the ant runs at once until memory recovers.
    pub max_harnesses: u32,
}

/// An ant's harness that keeps failing to start, as its roster entry reports it ([`RosterEntry::failing`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessFailing {
    /// When the failures held the messages first since a message last got past the harness's start.
    pub since: DateTime<Utc>,
    /// The harness's last error.
    pub error: String,
}

/// A provider limit an ant's harness ran into, as its roster entry reports it ([`RosterEntry::limited`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessLimit {
    pub kind: LimitKind,
    /// When the harness first reported a limit since it last answered a message.
    pub since: DateTime<Utc>,
    /// When the held messages are tried again.
    pub until: DateTime<Utc>,
}

/// Which provider limit a harness ran into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitKind {
    /// A rate limit or an overloaded provider, passing within minutes.
    RateLimit,
    /// An exhausted quota or usage allowance, which may take hours or a plan change.
    Quota,
    /// A value from a newer daemon.
    #[serde(other)]
    Unknown,
}

impl LimitKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RateLimit => "rate-limited",
            Self::Quota => "out-of-quota",
            Self::Unknown => "limited",
        }
    }
}

/// The agent behind an ant's harness, as it names itself in ACP `initialize` ([`RosterEntry::agent`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessAgent {
    pub name: String,
    pub version: String,
}

impl fmt::Display for HarnessAgent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.version)
    }
}

/// An ant's harness that is signed out, as its roster entry reports it ([`RosterEntry::signed_out`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignInNeeded {
    /// When the harness first said it is signed out.
    pub since: DateTime<Utc>,
    /// The command that signs the harness in, run on the entry's machine as the user its daemon runs as, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sign_in: Option<String>,
}

/// What an ant is doing, as its roster entry reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Activity {
    /// No message in hand.
    Idle,
    /// Handling at least one message.
    Working,
    /// A value from a newer daemon.
    #[serde(other)]
    Unknown,
}

impl Activity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Working => "working",
            Self::Unknown => "unknown",
        }
    }
}

impl RosterEntry {
    /// The KV key of the entry.
    pub fn key(&self) -> &str {
        self.ant.as_str()
    }

    /// Check that the entry was read from its own key. Credentials pin each ant to its own key, while the body is
    /// whatever the writer put there, so an entry naming another ant is forged.
    pub fn check_key(&self, key: &str) -> Result<(), Error> {
        if key == self.key() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "roster entry", user: self.ant.clone(), at: format!("key {key}") })
    }

    /// The leader of the ant's escalation chain, if it has one.
    pub fn leader(&self) -> Option<&UserId> {
        self.escalate_to.first()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{p, subject};

    #[test]
    #[allow(clippy::too_many_lines)]
    fn roster_entry_round_trips_and_tolerates_additions() {
        let entry = RosterEntry {
            ant: p("dev-a"),
            machine: "build-1".to_owned(),
            instance: None,
            harness: "copilot".to_owned(),
            agent: None,
            capabilities: vec!["rust".to_owned(), "nats".to_owned()],
            whoami: Some("Maintain the Rust backend.\nReview NATS changes.".to_owned()),
            workdir: None,
            started_at: Utc::now(),
            heartbeat_at: Utc::now(),
            activity: None,
            handling: None,
            harnesses: None,
            escalate_to: Vec::new(),
            version: None,
            perms: None,
            signed_out: None,
            limited: None,
            failing: None,
            memory_low: None,
        };
        let json = serde_json::to_string(&entry).unwrap();
        assert!(!json.contains("workdir") && !json.contains("activity") && !json.contains("harnesses"), "{json}");
        assert!(!json.contains("escalate_to") && !json.contains("instance"), "{json}");
        assert!(!json.contains("version") && !json.contains("perms"), "{json}");
        assert!(!json.contains("signed_out") && !json.contains("limited") && !json.contains("failing"), "{json}");
        assert!(!json.contains("agent") && !json.contains("memory_low"), "{json}");
        assert_eq!(serde_json::from_str::<RosterEntry>(&json).unwrap(), entry);
        assert_eq!(entry.leader(), None);
        let agent = HarnessAgent { name: "copilot-agent".to_owned(), version: "1.0.3".to_owned() };
        let named = RosterEntry { agent: Some(agent.clone()), ..entry.clone() };
        let json = serde_json::to_string(&named).unwrap();
        assert!(json.contains(r#""agent":{"name":"copilot-agent","version":"1.0.3"}"#), "{json}");
        assert_eq!(serde_json::from_str::<RosterEntry>(&json).unwrap(), named);
        assert_eq!(agent.to_string(), "copilot-agent 1.0.3");
        let versioned =
            RosterEntry { version: Some("0.3.1".to_owned()), perms: Some(PermissionsVersion(2)), ..entry.clone() };
        let json = serde_json::to_string(&versioned).unwrap();
        assert!(json.contains(r#""version":"0.3.1","perms":2"#), "{json}");
        assert_eq!(serde_json::from_str::<RosterEntry>(&json).unwrap(), versioned);
        let chained = RosterEntry { escalate_to: vec![p("lead"), p("dev-b")], ..entry.clone() };
        let json = serde_json::to_string(&chained).unwrap();
        assert!(json.contains(r#""escalate_to":["lead","dev-b"]"#), "{json}");
        assert_eq!(serde_json::from_str::<RosterEntry>(&json).unwrap(), chained);
        assert_eq!(chained.leader(), Some(&p("lead")));
        let busy =
            RosterEntry { activity: Some(Activity::Working), handling: Some(2), harnesses: Some(1), ..entry.clone() };
        let json = serde_json::to_string(&busy).unwrap();
        assert!(json.contains(r#""activity":"working","handling":2,"harnesses":1"#), "{json}");
        assert_eq!(serde_json::from_str::<RosterEntry>(&json).unwrap(), busy);
        let since = "2026-01-01T00:00:00Z".parse().unwrap();
        let out = RosterEntry {
            signed_out: Some(SignInNeeded { since, sign_in: Some("copilot login".to_owned()) }),
            ..entry.clone()
        };
        let json = serde_json::to_string(&out).unwrap();
        assert!(json.contains(r#""signed_out":{"since":"2026-01-01T00:00:00Z","sign_in":"copilot login"}"#), "{json}");
        assert_eq!(serde_json::from_str::<RosterEntry>(&json).unwrap(), out);
        let unknown = RosterEntry { signed_out: Some(SignInNeeded { since, sign_in: None }), ..entry.clone() };
        let json = serde_json::to_string(&unknown).unwrap();
        assert!(json.contains(r#""signed_out":{"since":"2026-01-01T00:00:00Z"}"#), "{json}");
        assert_eq!(serde_json::from_str::<RosterEntry>(&json).unwrap(), unknown);
        let until = "2026-01-01T00:15:00Z".parse().unwrap();
        let limited =
            RosterEntry { limited: Some(HarnessLimit { kind: LimitKind::Quota, since, until }), ..entry.clone() };
        let json = serde_json::to_string(&limited).unwrap();
        assert!(
            json.contains(
                r#""limited":{"kind":"quota","since":"2026-01-01T00:00:00Z","until":"2026-01-01T00:15:00Z"}"#
            ),
            "{json}"
        );
        assert_eq!(serde_json::from_str::<RosterEntry>(&json).unwrap(), limited);
        let failing = RosterEntry {
            failing: Some(HarnessFailing { since, error: "copilot: not found".to_owned() }),
            ..entry.clone()
        };
        let json = serde_json::to_string(&failing).unwrap();
        assert!(json.contains(r#""failing":{"since":"2026-01-01T00:00:00Z","error":"copilot: not found"}"#), "{json}");
        assert_eq!(serde_json::from_str::<RosterEntry>(&json).unwrap(), failing);
        let low = RosterEntry {
            memory_low: Some(MemoryLow { since, available: 512 << 20, max_harnesses: 1 }),
            ..entry.clone()
        };
        let json = serde_json::to_string(&low).unwrap();
        assert!(
            json.contains(r#""memory_low":{"since":"2026-01-01T00:00:00Z","available":536870912,"max_harnesses":1}"#),
            "{json}"
        );
        assert_eq!(serde_json::from_str::<RosterEntry>(&json).unwrap(), low);
        assert_eq!(entry.key(), "dev-a");
        assert_eq!(entry.check_key("dev-a"), Ok(()));
        assert_eq!(entry.check_key("dev-b").unwrap_err().to_string(), "roster entry of dev-a found at key dev-b");
        assert_eq!(subject::roster(&entry.ant), "$KV.ants_roster.dev-a");

        let minimal = r#"{"ant":"dev-b","machine":"m","harness":"copilot","state":"idle",
            "started_at":"2026-01-01T00:00:00Z","heartbeat_at":"2026-01-01T00:00:10Z"}"#;
        let entry: RosterEntry = serde_json::from_str(minimal).unwrap();
        assert!(entry.capabilities.is_empty() && entry.escalate_to.is_empty());
        assert_eq!((entry.whoami, entry.instance), (None, None));
        assert_eq!((entry.version, entry.perms), (None, None), "older daemons publish no version");
        assert_eq!((entry.activity, entry.handling, entry.harnesses), (None, None, None));
        assert_eq!(entry.signed_out, None, "an older daemon's entry");
        assert_eq!(entry.limited, None, "an older daemon's entry");
        assert_eq!(entry.failing, None, "an older daemon's entry");
        assert_eq!(entry.memory_low, None, "an older daemon's entry");

        let newer = r#"{"ant":"dev-b","machine":"m","harness":"copilot","activity":"draining","harnesses":3,
            "signed_out":{"since":"2026-01-01T00:00:05Z","sign_in":"x login","reason":"expired"},
            "limited":{"kind":"daily_cap","since":"2026-01-01T00:00:05Z","until":"2026-01-01T01:00:00Z"},
            "started_at":"2026-01-01T00:00:00Z","heartbeat_at":"2026-01-01T00:00:10Z"}"#;
        let entry: RosterEntry = serde_json::from_str(newer).unwrap();
        assert_eq!((entry.activity, entry.harnesses), (Some(Activity::Unknown), Some(3)));
        assert_eq!(entry.signed_out.and_then(|s| s.sign_in).as_deref(), Some("x login"));
        assert_eq!(entry.limited.map(|l| l.kind), Some(LimitKind::Unknown));
    }
}
