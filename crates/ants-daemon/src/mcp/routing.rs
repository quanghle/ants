//! Finding ants in the colony: `list_ants`, and `route`, which picks the ant to send a request to by capability, a
//! profile only breaking a tie ([`colony::pick`]). A message to an ant no daemon serves now is checked for first
//! ([`Hub::unserved`]), naming ants to message instead.

use crate::bus::relayed::GuardedKv as _;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use ants_proto::{COLONY_BUCKET, ColonyEntry, UserId};
use anyhow::{Context as _, Result};
use futures::future::BoxFuture;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, schemars, tool, tool_router};
use serde::Deserialize;

use super::Hub;
use super::messages::{ask_wait, shown};
use super::tools::{Tools, text};
use crate::colony;

/// How long a recipient's colony lookup may take before the message goes without it.
pub(super) const LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);
/// Names of the colony a message to an ant no daemon serves lists at most.
const SHOWN_NAMES: usize = 10;

/// Reads the ants in the colony.
pub type Colony = Arc<dyn Fn() -> BoxFuture<'static, Result<Vec<ColonyEntry>>> + Send + Sync>;

/// Reads the colony bucket.
pub fn jetstream_colony(js: async_nats::jetstream::Context) -> Colony {
    Arc::new(move || {
        let js = js.clone();
        Box::pin(async move {
            let kv = js.get_key_value(COLONY_BUCKET).await.context("opening the colony")?;
            colony::list_entries(&kv).await
        })
    })
}

/// Reads one ant's colony entry, None when the colony lists none.
pub type ColonyLookup = Arc<dyn Fn(UserId) -> BoxFuture<'static, Result<Option<ColonyEntry>>> + Send + Sync>;

/// Reads the ant's key in the colony bucket.
pub fn jetstream_lookup(js: async_nats::jetstream::Context) -> ColonyLookup {
    Arc::new(move |ant| {
        let js = js.clone();
        Box::pin(async move {
            let kv = js.get_key_value(COLONY_BUCKET).await.context("opening the colony")?;
            let Some(value) =
                kv.guarded_get(ant.as_str()).await.with_context(|| format!("reading {ant} in the colony"))?
            else {
                return Ok(None);
            };
            let entry: ColonyEntry = serde_json::from_slice(&value).with_context(|| format!("parsing {ant}"))?;
            entry.check_key(ant.as_str())?;
            Ok(Some(entry))
        })
    })
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ListAnts {
    /// Only the ants advertising this capability, such as `rust`.
    #[serde(default)]
    capability: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct Route {
    /// Capability the request needs, as ants advertise it in the colony, such as `rust`.
    capability: String,
    /// The request.
    text: String,
    /// Wait for the reply and return it (the default); false sends the request and returns at once.
    #[serde(default = "default_wait")]
    wait: bool,
    /// How long to wait for the reply, such as `90s` or `10m` (default 5m, at most 30m).
    #[serde(default)]
    timeout: Option<String>,
    /// Files to attach: paths in the working directory, at most 8 files of up to 8 MiB each; secrets in text files are
    /// redacted.
    #[serde(default)]
    attach: Vec<String>,
}

fn default_wait() -> bool {
    true
}

impl Hub {
    /// The colony without this ant, by id.
    pub(super) async fn others(&self) -> Result<Vec<ColonyEntry>, String> {
        let mut entries = (self.colony)().await.map_err(|e| format!("cannot read the colony: {e:#}"))?;
        entries.retain(|e| e.ant != self.ant);
        Ok(entries)
    }

    /// Why no daemon serves the ant `to` now, by its colony entry, with names from the colony `to` may stand for; None
    /// when one does, `to` is a human or the channel manager, or the colony cannot be read, so a lookup never holds a
    /// message back. A provider limit counts only while it holds past `wait` from now.
    pub(super) async fn unserved(&self, to: &UserId, wait: Duration) -> Option<String> {
        let lookup = self.lookup.get()?;
        if self.humans.is_human(to) || *to == UserId::channel_manager() {
            return None;
        }
        let entry = match tokio::time::timeout(LOOKUP_TIMEOUT, lookup(to.clone())).await {
            Ok(Ok(entry)) => entry,
            Ok(Err(e)) => {
                tracing::debug!(%to, error = format!("{e:#}"), "not looking the recipient up");
                return None;
            }
            Err(_) => {
                tracing::debug!(%to, "not looking the recipient up: no answer within {LOOKUP_TIMEOUT:?}");
                return None;
            }
        };
        let now = chrono::Utc::now();
        let why = why_unserved(to, entry.as_ref(), now, wait)?;
        let others = match tokio::time::timeout(LOOKUP_TIMEOUT, self.others()).await {
            Ok(Ok(others)) => others,
            _ => return Some(format!("{why}.")),
        };
        Some(format!("{why}. {}", alternatives(to, entry.as_ref(), &others, now)))
    }
}

#[tool_router(router = routing_router, vis = "pub(super)")]
impl Tools {
    #[tool(description = "List the other ants being served, from the colony, with each ant's advertised \
        whoami field, profile (what the ant says it is good for, in its own words), capabilities (such as `rust` or \
        `billing`), harness, machine and whether it is idle or working, \
        or signed out or paused: messages to an ant whose harness is signed out wait unanswered until a human signs \
        it in, and to a paused ant until a human resumes it. \
        `capability` keeps only those advertising it. Pick whom to send_message or ask_ant by these capabilities.")]
    async fn list_ants(&self, Parameters(args): Parameters<ListAnts>) -> Result<CallToolResult, ErrorData> {
        let result = async {
            let entries = self.hub.others().await?;
            let capability = args.capability.as_deref().map(str::trim).filter(|c| !c.is_empty());
            let shown: Vec<&ColonyEntry> =
                entries.iter().filter(|e| capability.is_none_or(|c| colony::has_capability(e, c))).collect();
            match (capability, shown.is_empty()) {
                (Some(c), true) => Err(nobody_has(c, &entries)),
                (None, true) => Ok("No other ant is in the colony.".to_owned()),
                (_, false) => Ok(describe(&shown)),
            }
        }
        .await;
        Ok(text(result))
    }

    #[tool(description = "Route a request to an ant whose colony capabilities include `capability`, other than \
        this one: an ant not paused before a paused one, then an ant whose harness is signed in before one signed \
        out, then a live ant before a stale one, then \
        an idle ant before a busy one, then the one with the fewest messages in hand, then the one whose profile \
        best matches `text`, words rare among the candidates' profiles weighing most, then the first by name: a \
        profile only breaks a tie. By \
        default it waits for the reply and returns it, like ask_ant; \
        with `wait` false it sends and returns at once, like send_message. Either way it goes on the current thread \
        and follows its turn budget, as those do, and `attach` attaches files as they do.")]
    async fn route(
        &self,
        Parameters(args): Parameters<Route>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        let result = async {
            let wait = ask_wait(args.timeout.as_deref())?;
            let entries = self.hub.others().await?;
            let capability = args.capability.trim();
            let to = colony::pick(&entries, capability, &args.text, &self.hub.ant, chrono::Utc::now())
                .ok_or_else(|| nobody_has(capability, &entries))?
                .ant
                .to_string();
            if !args.wait {
                let sent = self.send_to(&thread, &to, args.text, None, &args.attach).await?;
                return Ok(format!("Routed to {to} for {capability}. {sent}"));
            }
            let reply = self.ask(&thread, &to, args.text, &args.attach, wait, &cx).await?;
            Ok(format!(
                "Routed to {to} for {capability}. {} replied (message {}):\n{}",
                reply.from,
                reply.id,
                shown(&reply)
            ))
        }
        .await;
        Ok(text(result))
    }
}

/// Ant, capabilities, harness and the agent it runs, machine, activity or a pause or signed-out harness, the fallback
/// answering for it, and advertised role instructions.
pub(super) fn describe(entries: &[&ColonyEntry]) -> String {
    let lines: Vec<String> = entries
        .iter()
        .map(|e| {
            let capabilities =
                if e.capabilities.is_empty() { "no capabilities".to_owned() } else { e.capabilities.join(", ") };
            let activity = match (&e.signed_out, &e.failing, &e.limited, e.activity) {
                (Some(_), _, _, _) => ", signed out".to_owned(),
                (None, Some(_), _, _) => ", failing".to_owned(),
                (None, None, Some(limited), _) => format!(", {}", limited.kind.as_str()),
                (None, None, None, activity) => format!(", {}", activity.as_str()),
            };
            let activity = if e.paused.is_some() { format!("{activity}, paused") } else { activity };
            let agent = e.agent.as_ref().map_or_default(|a| format!(" running {a}"));
            let mut text = format!("{}: {capabilities} ({}{agent} on {}{activity})", e.ant, e.harness, e.machine);
            if let Some(paused) = &e.paused {
                let _ = write!(
                    text,
                    "\n  paused since {}: {} takes no new prompt and messages to it wait queued until a human resumes \
                     it",
                    paused.since.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    e.ant
                );
            } else if let Some(fallback) = &e.fallback {
                let _ = write!(
                    text,
                    "\n  fallback {} answers messages to {} since {}, until its own harness can again",
                    fallback.name,
                    e.ant,
                    fallback.since.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                );
            } else if let Some(out) = &e.signed_out {
                let how = out.sign_in.as_ref().map_or_default(|c| format!(" (`{c}` on {})", e.machine));
                let _ = write!(
                    text,
                    "\n  harness signed out since {}: messages to {} wait unanswered until a human signs it in{how}",
                    out.since.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    e.ant
                );
            } else if let Some(failing) = &e.failing {
                let _ = write!(
                    text,
                    "\n  harness failing to start since {} ({}): messages to {} wait unanswered until a human fixes it",
                    failing.since.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    failing.error,
                    e.ant
                );
            } else if let Some(limited) = &e.limited {
                let _ = write!(
                    text,
                    "\n  harness held back by a provider limit ({}): messages to {} wait until about {}",
                    limited.kind.as_str(),
                    e.ant,
                    limited.until.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                );
            }
            if let Some(low) = &e.memory_low {
                let _ = write!(
                    text,
                    "\n  memory low on {} since {}: at most {} {} at once, so messages may wait longer",
                    e.machine,
                    low.since.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    low.max_harnesses,
                    if low.max_harnesses == 1 { "harness runs" } else { "harnesses run" }
                );
            }
            if let Some(whoami) = &e.whoami
                && !whoami.trim().is_empty()
            {
                let _ = write!(text, "\n  whoami (advertised):\n{whoami}");
            }
            if let Some(profile) = &e.profile
                && !profile.trim().is_empty()
            {
                let _ = write!(text, "\n  profile (the ant's own words):\n{profile}");
            }
            text
        })
        .collect();
    lines.join("\n")
}

/// Why nobody matches `capability`, and what there is instead.
fn nobody_has(capability: &str, entries: &[ColonyEntry]) -> String {
    let mut known: Vec<String> =
        entries.iter().flat_map(|e| e.capabilities.iter().map(|c| c.trim().to_lowercase())).collect();
    known.sort();
    known.dedup();
    if known.is_empty() {
        return format!("No other ant in the colony has capability {capability:?}; none advertises any.");
    }
    format!("No other ant in the colony has capability {capability:?}. Capabilities there: {}.", known.join(", "))
}

/// Why no daemon serves `ant` now, given its colony entry, or None when one does: no entry, a stale one (see
/// [`colony::is_live`]), a pause, a harness signed out or failing to start, or a provider limit holding its messages past `wait`
/// from `now`, with no fallback answering for it.
pub(crate) fn why_unserved(
    ant: &UserId,
    entry: Option<&ColonyEntry>,
    now: chrono::DateTime<chrono::Utc>,
    wait: Duration,
) -> Option<String> {
    let Some(entry) = entry else {
        return Some(format!("{ant} is not in the colony: no daemon serves {ant} now"));
    };
    if !colony::is_live(entry, now) {
        let ago = (now - entry.heartbeat_at).to_std().unwrap_or_default();
        return Some(format!(
            "{ant} is stale in the colony: the daemon on {} last beat {} ago and may not serve {ant} now",
            entry.machine,
            ants_proto::duration::format(Duration::from_secs(ago.as_secs()))
        ));
    }
    if let Some(paused) = &entry.paused {
        return Some(format!(
            "{ant} is paused since {}: messages to {ant} wait queued, unanswered until a human resumes it",
            paused.since.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ));
    }
    if entry.fallback.is_some() {
        return None;
    }
    if let Some(failing) = entry.failing.as_ref().filter(|_| entry.signed_out.is_none()) {
        return Some(format!(
            "the harness of {ant} ({} on {}) fails to start since {}: messages to {ant} wait unanswered until a human \
             fixes it",
            entry.harness,
            entry.machine,
            failing.since.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ));
    }
    let Some(out) = entry.signed_out.as_ref() else {
        let limited = entry.limited.as_ref()?;
        let after = chrono::Duration::from_std(wait).unwrap_or(chrono::Duration::MAX);
        if now.checked_add_signed(after).is_some_and(|by| limited.until <= by) {
            return None;
        }
        let what = match limited.kind {
            ants_proto::LimitKind::Quota => "is out of quota",
            ants_proto::LimitKind::RateLimit => "is rate limited",
            ants_proto::LimitKind::Unknown => "is held back by a provider limit",
        };
        return Some(format!(
            "the harness of {ant} ({} on {}) {what}: messages to {ant} wait unanswered until {}",
            entry.harness,
            entry.machine,
            limited.until.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ));
    };
    Some(format!(
        "the harness of {ant} ({} on {}) is signed out since {}: messages to {ant} wait unanswered until a human signs \
         the harness in",
        entry.harness,
        entry.machine,
        out.since.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    ))
}

/// What to message instead of `ant`, which no daemon serves, from the `others` in the colony: for an ant missing
/// from it the names closest to `ant` (the name may be misspelt), else the served ants sharing a capability of its
/// `entry`.
fn alternatives(
    ant: &UserId,
    entry: Option<&ColonyEntry>,
    others: &[ColonyEntry],
    now: chrono::DateTime<chrono::Utc>,
) -> String {
    let names = |entries: &[&ColonyEntry]| entries.iter().map(|e| e.ant.as_str()).collect::<Vec<_>>().join(", ");
    if let Some(entry) = entry {
        let sharing: Vec<&ColonyEntry> = others
            .iter()
            .filter(|e| e.ant != *ant && colony::is_live(e, now) && e.paused.is_none())
            .filter(|e| e.fallback.is_some() || e.signed_out.is_none() && e.failing.is_none() && e.limited.is_none())
            .filter(|e| entry.capabilities.iter().any(|c| colony::has_capability(e, c)))
            .take(SHOWN_NAMES)
            .collect();
        if sharing.is_empty() {
            return format!("No served ant shares a capability of {ant}; list_ants shows the ants in the colony.");
        }
        return format!("Served ants sharing a capability of {ant}: {}.", names(&sharing));
    }
    let wanted = ant.as_str().to_lowercase();
    let most = wanted.chars().count().div_ceil(3).max(1);
    let mut close: Vec<(usize, &ColonyEntry)> = others
        .iter()
        .filter_map(|e| {
            let name = e.ant.as_str().to_lowercase();
            let distance = edit_distance(&wanted, &name);
            (distance <= most || name.starts_with(&wanted) || wanted.starts_with(&name)).then_some((distance, e))
        })
        .collect();
    close.sort_by(|(a, x), (b, y)| a.cmp(b).then_with(|| x.ant.cmp(&y.ant)));
    let close: Vec<&ColonyEntry> = close.into_iter().map(|(_, e)| e).take(3).collect();
    match others.len() {
        _ if !close.is_empty() => format!("Closest names in the colony: {}.", names(&close)),
        0 => "No other ant is in the colony.".to_owned(),
        n if n <= SHOWN_NAMES => format!("In the colony: {}.", names(&others.iter().collect::<Vec<_>>())),
        n => format!("No name in the colony is close; list_ants shows the {n} ants there."),
    }
}

/// The edits (insertions, deletions, substitutions of a character) that turn `a` into `b`.
fn edit_distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut diagonal = row[0];
        row[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let substituted = diagonal + usize::from(ca != *cb);
            diagonal = row[j + 1];
            row[j + 1] = substituted.min(row[j] + 1).min(diagonal + 1);
        }
    }
    row[b.len()]
}
