//! The colony: the [`ColonyEntry`] of each ant served, in [`crate::COLONY_BUCKET`], which the serving daemon keeps
//! alive with heartbeats: its machine, harness, version, activity and what holds it back (a sign-in, a provider limit,
//! a failing harness, low memory, a pause). `ants colony` lists it, and daemons find escalation chains in it.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[cfg(doc)]
use crate::{ANTS_VERSION, COLONY_BUCKET, COLONY_HEARTBEAT, COLONY_TTL, PERMISSIONS_VERSION};
use crate::{Error, PermissionsVersion, UserId};

/// An ant's entry in the [`COLONY_BUCKET`], under the key of its id.
///
/// The serving daemon puts it on start, re-puts it every [`COLONY_HEARTBEAT`] and deletes it on a clean stop. The
/// bucket's max age is [`COLONY_TTL`], so the entry of a daemon that died expires on its own. A daemon that defers to
/// others, as the built-in messenger's does, puts it only over its own entry or none, and stops serving once another
/// daemon puts it. Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColonyEntry {
    pub ant: UserId,
    /// The machine the daemon runs on: its config file's `machine`, else its host name.
    pub machine: String,
    /// The daemon run that put the entry, unique to it, so two daemons on one machine tell their entries apart.
    pub instance: String,
    /// Agent harness behind the ant, such as `copilot`.
    pub harness: String,
    /// The agent the harness runs, as the ACP handshake of its latest start named it. Missing until a harness of the
    /// daemon started, and when the agent does not name itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<HarnessAgent>,
    /// Free-form tags the ant is good for (`rust`, `frontend`, `billing`), for routing requests to it.
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
    /// Whether the ant was handling a message at the last refresh.
    pub activity: Activity,
    /// Messages being handled at the last refresh.
    pub handling: u32,
    /// Live harness processes (sessions) at the last refresh.
    pub harnesses: u32,
    /// The ant's escalation chain, in order: what it cannot settle goes to these ants before a human. The first
    /// leads. Published so any daemon can follow the chain while the ant is offline.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub escalate_to: Vec<UserId>,
    /// The version of ants the daemon runs ([`ANTS_VERSION`]).
    pub version: String,
    /// The permissions version of the ants the daemon runs ([`PERMISSIONS_VERSION`]).
    pub perms: PermissionsVersion,
    /// Set while the ant's harness is signed out: messages to the ant wait, unanswered, until a check finds the
    /// harness signed in again. Missing while it is signed in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signed_out: Option<SignInNeeded>,
    /// Set while the ant's harness is held back by a provider limit: messages to the ant wait, spending no attempts,
    /// until `until`. Missing otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limited: Option<HarnessLimit>,
    /// Set while the ant's harness keeps failing to start: messages to the ant wait, spending no attempts, until a
    /// check starts it. Missing otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failing: Option<HarnessFailing>,
    /// Set while a fallback of the ant answers its messages, its own harness signed out, out of quota or failing to
    /// start. Missing otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback: Option<ServingFallback>,
    /// Set while the system memory available on `machine` is low: the daemon stops the ant's idle harnesses at once
    /// and runs fewer of them. Missing otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_low: Option<MemoryLow>,
    /// The model, reasoning effort and context the ant's config sets for its harness. Missing when the config sets
    /// none, so the harness's defaults hold.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<AntModel>,
    /// Set while the ant is paused (`paused = true` in its config): its daemon takes no new message and starts no new
    /// prompt, the messages to it waiting queued, until it is resumed. Missing otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused: Option<Paused>,
    /// The ant's settings the serving machine's config sets otherwise than the ant's [`crate::SharedSettings`], which
    /// the daemon runs it with: drift to mend in that config. Missing when none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub drift: Vec<String>,
    /// Whether the ant's harness reports its MCP servers to an [`crate::AdminOp::ReadMcp`] order: protocols claude,
    /// codex and copilot-sdk do. Missing when it does not.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub mcp: bool,
}

/// An ant's pause, as its colony entry reports it ([`ColonyEntry::paused`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Paused {
    /// When the daemon serving the ant found it paused: the order's or the config's change, or the daemon's start.
    pub since: DateTime<Utc>,
    /// The admin whose order paused it; missing when its config was edited on its machine, or since the daemon
    /// restarted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<UserId>,
}

/// The model settings of an ant's config, as its colony entry reports them ([`ColonyEntry::model`]). A missing
/// setting keeps the harness's default.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AntModel {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    /// Provider-native context selection, such as `1m`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
}

impl AntModel {
    /// The settings, or None when none is set.
    pub fn of(model: Option<String>, reasoning_effort: Option<String>, context: Option<String>) -> Option<Self> {
        let settings = Self { model, reasoning_effort, context };
        (settings != Self::default()).then_some(settings)
    }
}

impl fmt::Display for AntModel {
    /// `<model>`, `effort <effort>` and `context <context>`, the ones set, joined by `, `.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts = [
            self.model.clone(),
            self.reasoning_effort.as_ref().map(|e| format!("effort {e}")),
            self.context.as_ref().map(|c| format!("context {c}")),
        ];
        write!(f, "{}", parts.into_iter().flatten().collect::<Vec<_>>().join(", "))
    }
}

/// A fallback answering an ant's messages, as its colony entry reports it ([`ColonyEntry::fallback`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServingFallback {
    /// The fallback as the ant's config names it: `<harness>`, or `<harness>:<model>`.
    pub name: String,
    /// When it first answered since the ant's own harness last could.
    pub since: DateTime<Utc>,
}

/// Low memory on an ant's machine, as its colony entry reports it ([`ColonyEntry::memory_low`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryLow {
    /// When the available memory fell below the daemon's threshold.
    pub since: DateTime<Utc>,
    /// Bytes of memory available at the latest check.
    pub available: u64,
    /// The most harnesses the ant runs at once until memory recovers.
    pub max_harnesses: u32,
}

/// An ant's harness that keeps failing to start, as its colony entry reports it ([`ColonyEntry::failing`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessFailing {
    /// When the failures held the messages first since a message last got past the harness's start.
    pub since: DateTime<Utc>,
    /// The harness's last error.
    pub error: String,
}

/// A provider limit an ant's harness ran into, as its colony entry reports it ([`ColonyEntry::limited`]).
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

/// The agent behind an ant's harness, as it names itself in ACP `initialize` ([`ColonyEntry::agent`]).
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

/// An ant's harness that is signed out, as its colony entry reports it ([`ColonyEntry::signed_out`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignInNeeded {
    /// When the harness first said it is signed out.
    pub since: DateTime<Utc>,
    /// The command that signs the harness in, run on the entry's machine as the user its daemon runs as, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sign_in: Option<String>,
}

/// What an ant is doing, as its colony entry reports it.
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

impl ColonyEntry {
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
        Err(Error::ForeignUser { kind: "colony entry", user: self.ant.clone(), at: format!("key {key}") })
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

    fn dev_a() -> ColonyEntry {
        ColonyEntry {
            ant: p("dev-a"),
            machine: "build-1".to_owned(),
            instance: "run-1".to_owned(),
            harness: "copilot".to_owned(),
            agent: None,
            capabilities: vec!["rust".to_owned(), "nats".to_owned()],
            whoami: Some("Maintain the Rust backend.\nReview NATS changes.".to_owned()),
            workdir: None,
            started_at: Utc::now(),
            heartbeat_at: Utc::now(),
            activity: Activity::Idle,
            handling: 0,
            harnesses: 0,
            escalate_to: Vec::new(),
            version: "0.3.1".to_owned(),
            perms: PermissionsVersion(2),
            signed_out: None,
            limited: None,
            failing: None,
            fallback: None,
            memory_low: None,
            model: None,
            paused: None,
            drift: Vec::new(),
            mcp: false,
        }
    }

    /// Check that `entry` serializes with `part` and reads back the same.
    fn round_trips(entry: &ColonyEntry, part: &str) {
        let json = serde_json::to_string(entry).unwrap();
        assert!(json.contains(part), "{json}");
        assert_eq!(serde_json::from_str::<ColonyEntry>(&json).unwrap(), *entry);
    }

    #[test]
    fn colony_entry_round_trips() {
        let entry = dev_a();
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains(r#""instance":"run-1""#), "{json}");
        assert!(json.contains(r#""activity":"idle","handling":0,"harnesses":0"#), "{json}");
        assert!(json.contains(r#""version":"0.3.1","perms":2"#), "{json}");
        assert!(!json.contains("workdir") && !json.contains("escalate_to"), "{json}");
        assert!(!json.contains("signed_out") && !json.contains("limited") && !json.contains("failing"), "{json}");
        assert!(!json.contains("agent") && !json.contains("fallback") && !json.contains("memory_low"), "{json}");
        assert!(!json.contains("model") && !json.contains("paused"), "{json}");
        assert_eq!(serde_json::from_str::<ColonyEntry>(&json).unwrap(), entry);
        assert_eq!(entry.leader(), None);
        let agent = HarnessAgent { name: "copilot-agent".to_owned(), version: "1.0.3".to_owned() };
        let named = ColonyEntry { agent: Some(agent.clone()), ..entry.clone() };
        round_trips(&named, r#""agent":{"name":"copilot-agent","version":"1.0.3"}"#);
        assert_eq!(agent.to_string(), "copilot-agent 1.0.3");
        let chained = ColonyEntry { escalate_to: vec![p("lead"), p("dev-b")], ..entry.clone() };
        round_trips(&chained, r#""escalate_to":["lead","dev-b"]"#);
        assert_eq!(chained.leader(), Some(&p("lead")));
        let busy = ColonyEntry { activity: Activity::Working, handling: 2, harnesses: 1, ..entry.clone() };
        round_trips(&busy, r#""activity":"working","handling":2,"harnesses":1"#);
        assert_eq!(AntModel::of(None, None, None), None);
        let model = AntModel::of(Some("gpt-5".to_owned()), None, Some("1m".to_owned()));
        assert_eq!(model.as_ref().map(ToString::to_string).as_deref(), Some("gpt-5, context 1m"));
        round_trips(&ColonyEntry { model, ..entry.clone() }, r#""model":{"model":"gpt-5","context":"1m"}"#);
        assert_eq!(entry.key(), "dev-a");
        assert_eq!(entry.check_key("dev-a"), Ok(()));
        assert_eq!(entry.check_key("dev-b").unwrap_err().to_string(), "colony entry of dev-a found at key dev-b");
        assert_eq!(subject::colony(&entry.ant), "$KV.ants_colony.dev-a");
    }

    #[test]
    fn colony_entry_round_trips_the_harness_state() {
        let entry = dev_a();
        let since = "2026-01-01T00:00:00Z".parse().unwrap();
        let out = ColonyEntry {
            signed_out: Some(SignInNeeded { since, sign_in: Some("copilot login".to_owned()) }),
            ..entry.clone()
        };
        round_trips(&out, r#""signed_out":{"since":"2026-01-01T00:00:00Z","sign_in":"copilot login"}"#);
        let unknown = ColonyEntry { signed_out: Some(SignInNeeded { since, sign_in: None }), ..entry.clone() };
        round_trips(&unknown, r#""signed_out":{"since":"2026-01-01T00:00:00Z"}"#);
        let until = "2026-01-01T00:15:00Z".parse().unwrap();
        let limited =
            ColonyEntry { limited: Some(HarnessLimit { kind: LimitKind::Quota, since, until }), ..entry.clone() };
        round_trips(
            &limited,
            r#""limited":{"kind":"quota","since":"2026-01-01T00:00:00Z","until":"2026-01-01T00:15:00Z"}"#,
        );
        let failing = ColonyEntry {
            failing: Some(HarnessFailing { since, error: "copilot: not found".to_owned() }),
            ..entry.clone()
        };
        round_trips(&failing, r#""failing":{"since":"2026-01-01T00:00:00Z","error":"copilot: not found"}"#);
        let fallback = ColonyEntry {
            fallback: Some(ServingFallback { name: "claude:sonnet".to_owned(), since }),
            ..entry.clone()
        };
        round_trips(&fallback, r#""fallback":{"name":"claude:sonnet","since":"2026-01-01T00:00:00Z"}"#);
        let low = ColonyEntry {
            memory_low: Some(MemoryLow { since, available: 512 << 20, max_harnesses: 1 }),
            ..entry.clone()
        };
        round_trips(&low, r#""memory_low":{"since":"2026-01-01T00:00:00Z","available":536870912,"max_harnesses":1}"#);
        let paused = ColonyEntry { paused: Some(Paused { since, by: Some(p("alice")) }), ..entry.clone() };
        round_trips(&paused, r#""paused":{"since":"2026-01-01T00:00:00Z","by":"alice"}"#);
        let by_hand = ColonyEntry { paused: Some(Paused { since, by: None }), ..entry };
        let json = serde_json::to_string(&by_hand).unwrap();
        assert!(json.contains(r#""paused":{"since":"2026-01-01T00:00:00Z"}"#), "{json}");
    }

    #[test]
    fn colony_entry_tolerates_missing_and_added_fields() {
        let minimal = r#"{"ant":"dev-b","machine":"m","instance":"run-2","harness":"copilot","capabilities":[],
            "started_at":"2026-01-01T00:00:00Z","heartbeat_at":"2026-01-01T00:00:10Z",
            "activity":"idle","handling":0,"harnesses":0,"version":"0.3.1","perms":1}"#;
        let entry: ColonyEntry = serde_json::from_str(minimal).unwrap();
        assert!(entry.escalate_to.is_empty());
        assert_eq!((entry.agent, entry.whoami, entry.workdir), (None, None, None));
        assert_eq!((entry.signed_out, entry.limited, entry.failing), (None, None, None));
        assert_eq!((entry.fallback, entry.memory_low, entry.model, entry.paused), (None, None, None, None));

        let newer = r#"{"ant":"dev-b","machine":"m","instance":"run-2","harness":"copilot","capabilities":[],
            "activity":"draining","handling":0,"harnesses":3,"version":"9.0.0","perms":9,"queue":4,
            "signed_out":{"since":"2026-01-01T00:00:05Z","sign_in":"x login","reason":"expired"},
            "limited":{"kind":"daily_cap","since":"2026-01-01T00:00:05Z","until":"2026-01-01T01:00:00Z"},
            "started_at":"2026-01-01T00:00:00Z","heartbeat_at":"2026-01-01T00:00:10Z"}"#;
        let entry: ColonyEntry = serde_json::from_str(newer).unwrap();
        assert_eq!((entry.activity, entry.harnesses), (Activity::Unknown, 3));
        assert_eq!(entry.signed_out.and_then(|s| s.sign_in).as_deref(), Some("x login"));
        assert_eq!(entry.limited.map(|l| l.kind), Some(LimitKind::Unknown));
    }
}
