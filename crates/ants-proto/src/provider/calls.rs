//! The params and results of the protocol's methods past initialize. None carries an ant's messages, prompts or
//! replies: a provider sees its own tool calls, the lifecycle events, and what the ant passes it.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use super::Format;
use crate::{ChannelId, ThreadId, UserId};

/// One tool of a provider. ants serves it under [`super::served_name`], with the [`super::JSON_ARGUMENT`] added to its
/// schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tool {
    /// Its name without the provider's prefix ([`super::valid_tool_name`]).
    pub name: String,
    pub description: String,
    /// The JSON Schema of its arguments, an object.
    pub input_schema: Value,
}

/// The result of [`super::method::TOOLS_LIST`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolList {
    pub tools: Vec<Tool>,
}

/// The params of [`super::method::TOOLS_CALL`]: a call of one of the provider's tools by an ant's session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCall {
    /// The tool's name without the provider's prefix.
    pub name: String,
    /// The arguments the ant passed, without the [`super::JSON_ARGUMENT`].
    #[serde(default)]
    pub arguments: Map<String, Value>,
    pub context: CallContext,
    /// The answer format the ant asked for.
    #[serde(default)]
    pub format: Format,
}

/// Who calls, where and when, as ants knows it: never taken from the ant's text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallContext {
    pub ant: UserId,
    /// The id of the ant's identity record ([`crate::AntIdentity::id`]), which stays across renames.
    pub identity: String,
    pub thread: ThreadId,
    pub origin: Origin,
    /// The harness running the session, by its name in the config.
    pub harness: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// The machine serving the ant, as its colony entry names it.
    pub machine: String,
    /// The thread's repository: the remote of the git repository its session works in (`origin`, else the first
    /// with a URL, [`remote_url`]), as `host/path` ([`normalize_remote`]). None outside a git repository with a remote.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    pub at: DateTime<Utc>,
}

impl CallContext {
    /// Whether what the call writes can be trusted as the ant's own: not when its thread came from a webhook or asks a
    /// review.
    pub fn trusted(&self) -> bool {
        self.origin.trusted()
    }
}

/// `remote`, a git remote in any form git takes (`https://user@Host:443/owner/repo.git`,
/// `git@host:owner/repo.git`, `ssh://…`, `host/owner/repo`), as `host/path`: without scheme, user, port, `.git` or
/// slashes around, the host in lower case. None when it names no host and path.
pub fn normalize_remote(remote: &str) -> Option<String> {
    let remote = remote.trim();
    let (authority, path) = match remote.split_once("://") {
        Some((_, rest)) => rest.split_once('/')?,
        // scp-like: `[user@]host:path`, a colon before any slash.
        None => match (remote.find(':'), remote.find('/')) {
            (Some(colon), slash) if slash.is_none_or(|slash| colon < slash) => (&remote[..colon], &remote[colon + 1..]),
            _ => remote.split_once('/')?,
        },
    };
    let host = authority.rsplit_once('@').map_or(authority, |(_, host)| host);
    let host = match host.rsplit_once(':') {
        Some((host, port)) if port.bytes().all(|b| b.is_ascii_digit()) => host,
        _ => host,
    };
    let path = path.trim_matches('/');
    let path = path.strip_suffix(".git").unwrap_or(path).trim_end_matches('/');
    let valid = |part: &str| !part.is_empty() && !part.contains(|c: char| c.is_whitespace() || c.is_control());
    (valid(host) && valid(path) && !host.contains(['/', '@', ':']))
        .then(|| format!("{}/{path}", host.to_ascii_lowercase()))
}

/// The URL of remote `origin` in `config`, the text of a git config file, else of the first remote with one; includes
/// and `insteadOf` rewrites are not followed.
pub fn remote_url(config: &str) -> Option<&str> {
    let (mut remote, mut first) = (None, None);
    for line in config.lines().map(str::trim) {
        if let Some(header) = line.strip_prefix('[') {
            remote = header.split_once(']').and_then(|(header, _)| {
                let (section, name) = header.split_once(char::is_whitespace)?;
                section.eq_ignore_ascii_case("remote").then(|| name.trim().trim_matches('"'))
            });
            continue;
        }
        let (Some(name), Some((key, value))) = (remote, line.split_once('=')) else { continue };
        let Some(url) = value.split_whitespace().next().map(|url| url.trim_matches('"')) else { continue };
        if !key.trim().eq_ignore_ascii_case("url") || url.is_empty() {
            continue;
        }
        if name == "origin" {
            return Some(url);
        }
        first.get_or_insert(url);
    }
    first
}

/// Where a thread came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Origin {
    pub by: Starter,
    /// The channel whose post started the thread, when one did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<ChannelId>,
}

impl Origin {
    /// Whether the thread's text comes from ants and humans of the bus: not from a webhook, which relays the outside,
    /// nor from an ant asking a review, whose packet carries its threads, webhooks' among them, nor from a starter of a
    /// kind not known.
    pub fn trusted(&self) -> bool {
        matches!(self.by, Starter::Human { .. } | Starter::Ant { .. } | Starter::Provider { .. })
    }
}

/// Who started a thread.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Starter {
    Human {
        user: UserId,
    },
    Ant {
        user: UserId,
    },
    /// A webhook or a schedule, relaying what came from outside the bus.
    Webhook {
        user: UserId,
    },
    /// An ant asking the ant, its sage, to review a dream ([`crate::REVIEW_ALLOW`]): the ant under review.
    Review {
        user: UserId,
    },
    /// A provider's wake request ([`super::method::WAKE`]), by the provider's name.
    Provider {
        name: String,
    },
    /// A kind of a later version of ants, untrusted ([`Origin::trusted`]).
    #[serde(other)]
    Unknown,
}

/// The result of [`super::method::TOOLS_CALL`]: the answer the ant reads, in the format it asked for, and whether the
/// call failed. A call the tool refuses is a result with `is_error`; an error response is for calls that never reached
/// the tool, such as one naming no tool of the provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolResult {
    #[serde(flatten)]
    pub output: Output,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub is_error: bool,
}

/// A tool's answer: `{"text": ...}` or `{"json": ...}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Output {
    Text(String),
    Json(Value),
}

/// The params of [`super::method::BRIEF`]: the ant's brief for a session of a thread.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BriefRequest {
    /// The session's ant and thread, as a tool call of it would carry them.
    pub context: CallContext,
    /// The most bytes of UTF-8 the brief may take ([`super::BRIEF_BUDGET`] unless configured).
    pub budget: u32,
    pub reason: BriefReason,
}

/// Why the session needs a brief.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BriefReason {
    /// A new session.
    Started,
    /// A session resumed from the harness's store.
    Loaded,
    /// A session started again after the last one was lost or overflowed its context.
    Rebuilt,
}

/// The result of [`super::method::BRIEF`]: text ants puts after the `[ants memory]` instructions; empty for none.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Brief {
    pub text: String,
}

/// The params of [`super::method::EVENT`]: something that happened to an ant. Sent in order per ant and at least once,
/// a repeat carrying the same `id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub id: Uuid,
    pub ant: UserId,
    /// The id of the ant's identity record ([`crate::AntIdentity::id`]).
    pub identity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread: Option<ThreadId>,
    pub at: DateTime<Utc>,
    #[serde(flatten)]
    pub kind: EventKind,
}

/// What happened, with what each kind needs to say. A provider acks a kind it does not know.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventKind {
    AntAdded,
    /// The ant took a new name, the event's `ant`.
    AntRenamed {
        from: UserId,
    },
    /// The ant moved to another machine or bus.
    AntMoved,
    /// The ant was made a clone of `from`, whose identity id was `from_identity`.
    AntCloned {
        from: UserId,
        from_identity: String,
    },
    AntRetired,
    AntRevived,
    ThreadStarted {
        origin: Origin,
        /// The repository of the ant's working directory, named as [`CallContext::repo`] is; none outside a git
        /// repository with a remote.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        repo: Option<String>,
    },
    ThreadDone,
    ThreadExpired,
    /// The harness lost the thread's session.
    SessionLost,
    /// The thread's session was started again.
    SessionRebuilt {
        reason: Rebuilt,
    },
    #[serde(other)]
    Unknown,
}

/// Why a session was rebuilt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Rebuilt {
    /// The harness lost it.
    Lost,
    /// It overflowed its context.
    Overflow,
}

/// The result of a call that returns nothing: `{}`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ack {}

/// The params of [`super::method::WAKE`], a provider's request to wake an ant with a message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WakeRequest {
    pub ant: UserId,
    /// The thread to wake the ant on; none for a new one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread: Option<ThreadId>,
    pub text: String,
    /// The request's idempotency id: a repeat within [`super::WAKE_DEDUP`] wakes the ant once.
    pub id: String,
}

/// The result of [`super::method::WAKE`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Woken {
    /// The thread the message went on.
    pub thread: ThreadId,
}

/// An ant, by name and identity id: the params of [`super::method::MEMORIES`] and [`super::method::EXPORT`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AntRef {
    pub ant: UserId,
    pub identity: String,
}

/// The result of [`super::method::MEMORIES`]: what `ants ant memories` shows of the provider's memories of the ant,
/// cut to the caps of a memories read ([`crate::MEMORY_FILE_CAP`], [`crate::MEMORIES_CAP`]).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoriesView {
    pub memories: Vec<MemoryEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryEntry {
    pub key: String,
    pub content: String,
}

/// The result of [`super::method::EXPORT`]: the provider's data of an ant, which `ants ant export` carries as it is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Export {
    /// The provider that made it, the only one that imports it.
    pub provider: String,
    pub data: Value,
}

/// The params of [`super::method::IMPORT`]: an export to take in for an ant, on `ants ant import`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportRequest {
    pub ant: UserId,
    pub identity: String,
    pub export: Export,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn webhook_and_review_threads_are_untrusted() {
        let by = |by| Origin { by, channel: None };
        let user = || "x".parse::<UserId>().unwrap();
        assert!(by(Starter::Human { user: user() }).trusted());
        assert!(by(Starter::Ant { user: user() }).trusted());
        assert!(by(Starter::Provider { name: "sense".to_owned() }).trusted());
        assert!(!by(Starter::Webhook { user: user() }).trusted());
        assert!(!by(Starter::Review { user: user() }).trusted());
        let review = json!({"by": {"kind": "review", "user": "x"}});
        assert_eq!(serde_json::from_value::<Origin>(review).unwrap(), by(Starter::Review { user: user() }));
    }

    #[test]
    fn a_starter_of_a_later_kind_reads_as_unknown_and_untrusted() {
        let later = json!({"by": {"kind": "levitated", "user": "x", "height": 3}, "channel": "ops"});
        let origin: Origin = serde_json::from_value(later).unwrap();
        assert_eq!(origin.by, Starter::Unknown);
        assert!(!origin.trusted());
    }

    #[test]
    fn remotes_normalize_to_host_and_path() {
        for remote in [
            "https://github.com/Owner/Repo.git",
            "https://user:pw@GitHub.com:443/Owner/Repo/",
            "git@github.com:Owner/Repo.git",
            "ssh://git@github.com:22/Owner/Repo",
            "github.com/Owner/Repo",
        ] {
            assert_eq!(normalize_remote(remote).as_deref(), Some("github.com/Owner/Repo"), "{remote}");
        }
        for remote in ["", "github.com", "https://github.com/", "a b/c"] {
            assert_eq!(normalize_remote(remote), None, "{remote}");
        }
    }

    #[test]
    fn the_remote_of_a_git_config_is_origin_else_the_first() {
        let config = "[core]\n\tbare = false\n[remote \"upstream\"]\n\turl = https://github.com/up/repo.git\n\
                      [Remote \"origin\"]\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n\tURL = \"git@GitHub.com:o/r.git\"\n";
        assert_eq!(remote_url(config), Some("git@GitHub.com:o/r.git"));
        assert_eq!(
            remote_url("[remote \"fork\"]\nurl = ssh://git@host:22/f/r # mine\n"),
            Some("ssh://git@host:22/f/r")
        );
        assert_eq!(remote_url("[core]\n\turl = https://h/o/r\n[branch \"main\"]\n\tremote = origin\n"), None);
    }

    #[test]
    fn unknown_event_kinds_read_as_unknown() {
        let event: Event = serde_json::from_value(json!({
            "id": "019a7d2e-0000-7000-8000-000000000001", "ant": "dev-a", "identity": "ab", "at": "2025-01-01T12:00:00Z",
            "kind": "ant_levitated", "height": 3
        }))
        .unwrap();
        assert_eq!(event.kind, EventKind::Unknown);
    }
}
